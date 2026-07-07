from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import asyncpg
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

SERVICE_NAME = os.getenv("SERVICE_NAME", "bot-service-02")
SERVICE_TITLE = os.getenv("SERVICE_TITLE", "Fleet Swarm")
SERVICE_KIND = os.getenv("SERVICE_KIND", "fleet")
POD_NAME = os.getenv("HOSTNAME", SERVICE_NAME)
APP_VERSION = os.getenv("APP_VERSION", "v1.0.0-stable")
APP_FLAVOR = os.getenv("APP_FLAVOR", "stable")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/bot_service_02")
SCENARIO_MAX_ROWS = int(os.getenv("SCENARIO_MAX_ROWS", "10000"))
LOCK_SECONDS = int(os.getenv("LOCK_SECONDS", "35"))
CPU_BURN_SECONDS = int(os.getenv("CPU_BURN_SECONDS", "45"))
KUBE_NAMESPACE = os.getenv("KUBE_NAMESPACE", "sandbox")
DEPLOYMENT_NAME = os.getenv("DEPLOYMENT_NAME", f"{SERVICE_NAME}-api")
BASE_REPLICAS = int(os.getenv("BASE_REPLICAS", "1"))
SURGE_REPLICAS = int(os.getenv("SURGE_REPLICAS", "6"))
HPA_MAX_REPLICAS = int(os.getenv("HPA_MAX_REPLICAS", "10"))
RECEIVER_CAPACITY_PER_POD = int(os.getenv("RECEIVER_CAPACITY_PER_POD", "1200"))
KUBE_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
KUBE_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


def stable_pod_factor(seed: str, low: float, span: float) -> float:
    bucket = int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:8], 16) % 1000
    return low + (bucket / 999) * span


POD_PROCESS_FACTOR = stable_pod_factor(POD_NAME, 0.82, 0.36)

REQUESTS = Counter("bot_service_http_requests_total", "HTTP requests", ["service", "path", "method", "status"])
LATENCY = Histogram("bot_service_http_request_duration_seconds", "HTTP request latency", ["service", "path"])
ERRORS = Counter("bot_service_errors_total", "Intentional and real application errors", ["service", "kind"])
DB_LATENCY = Histogram("bot_service_db_query_duration_seconds", "Database query latency", ["service", "operation"])
SCENARIO_ON = Gauge("bot_service_scenario_enabled", "Scenario state", ["service", "scenario"])
ROWS_TOTAL = Gauge("bot_service_rows_total", "Approximate domain row count", ["service", "table"])

pool: asyncpg.Pool | None = None
latency_samples: deque[float] = deque(maxlen=240)
scenario_state: dict[str, bool] = {
    "load": False,
    "scale_surge": False,
    "db_bulk_insert": False,
    "db_lock": False,
    "db_slow_query": False,
    "error_spike": False,
    "traffic_link": False,
}
background_tasks: set[asyncio.Task[Any]] = set()
last_scenario_sync = 0.0
traffic_lock = asyncio.Lock()
receiver_task: asyncio.Task[Any] | None = None
traffic_state: dict[str, Any] = {
    "role": "receiver",
    "running": False,
    "mode": "manual",
    "target_tps": 0,
    "manual_replicas": BASE_REPLICAS,
    "ready_replicas": BASE_REPLICAS,
    "desired_replicas": BASE_REPLICAS,
    "queue_depth": 0.0,
    "received_total": 0,
    "processed_total": 0,
    "failed_total": 0,
    "received_per_second": 0,
    "processed_per_second": 0,
    "last_tick": time.monotonic(),
    "updated_at": "",
}


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def json_arg(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


def current_pool() -> asyncpg.Pool:
    if pool is None:
        raise HTTPException(status_code=503, detail="database pool is not ready")
    return pool


async def timed_db(operation: str, query: str, *args: Any) -> list[asyncpg.Record]:
    start = time.perf_counter()
    try:
        async with current_pool().acquire() as conn:
            return await conn.fetch(query, *args)
    finally:
        DB_LATENCY.labels(SERVICE_NAME, operation).observe(time.perf_counter() - start)


async def exec_db(operation: str, query: str, *args: Any) -> str:
    start = time.perf_counter()
    try:
        async with current_pool().acquire() as conn:
            return await conn.execute(query, *args)
    finally:
        DB_LATENCY.labels(SERVICE_NAME, operation).observe(time.perf_counter() - start)


async def init_schema() -> None:
    async with current_pool().acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
              id SERIAL PRIMARY KEY,
              name TEXT NOT NULL,
              price_cents INTEGER NOT NULL,
              stock INTEGER NOT NULL DEFAULT 100
            );
            CREATE TABLE IF NOT EXISTS orders (
              id BIGSERIAL PRIMARY KEY,
              product_id INTEGER REFERENCES products(id),
              quantity INTEGER NOT NULL,
              status TEXT NOT NULL,
              total_cents INTEGER NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS payments (
              id BIGSERIAL PRIMARY KEY,
              order_id BIGINT REFERENCES orders(id),
              status TEXT NOT NULL,
              provider_latency_ms INTEGER NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS inventory_items (
              sku TEXT PRIMARY KEY,
              available INTEGER NOT NULL,
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS audit_logs (
              id BIGSERIAL PRIMARY KEY,
              event_type TEXT NOT NULL,
              payload JSONB NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS bots (
              id SERIAL PRIMARY KEY,
              name TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'idle',
              current_load INTEGER NOT NULL DEFAULT 0,
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS jobs (
              id BIGSERIAL PRIMARY KEY,
              bot_id INTEGER REFERENCES bots(id),
              status TEXT NOT NULL,
              payload JSONB NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              completed_at TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS bot_events (
              id BIGSERIAL PRIMARY KEY,
              event_type TEXT NOT NULL,
              payload JSONB NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS telemetry_samples (
              id BIGSERIAL PRIMARY KEY,
              metric TEXT NOT NULL,
              value DOUBLE PRECISION NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS scenario_flags (
              name TEXT PRIMARY KEY,
              enabled BOOLEAN NOT NULL DEFAULT false,
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        await conn.execute(
            """
            INSERT INTO products (name, price_cents, stock)
            SELECT * FROM (VALUES
              ('Jungle Hoodie', 59000, 120),
              ('Latency Mug', 18000, 240),
              ('Rollback Sticker Pack', 7000, 500)
            ) AS seed(name, price_cents, stock)
            WHERE NOT EXISTS (SELECT 1 FROM products);
            INSERT INTO inventory_items (sku, available)
            VALUES ('fleet-main', 500)
            ON CONFLICT (sku) DO NOTHING;
            INSERT INTO bots (name, status, current_load)
            SELECT 'bot-' || n, 'idle', 0
            FROM generate_series(1, 18) n
            WHERE NOT EXISTS (SELECT 1 FROM bots);
            """
        )
        await conn.executemany(
            """
            INSERT INTO scenario_flags(name, enabled)
            VALUES($1, false)
            ON CONFLICT (name) DO NOTHING
            """,
            [(name,) for name in scenario_state],
        )


async def refresh_row_gauges() -> dict[str, int]:
    tables = ["orders", "payments", "audit_logs", "jobs", "bot_events", "telemetry_samples"]
    counts: dict[str, int] = {}
    async with current_pool().acquire() as conn:
        for table in tables:
            count = int(await conn.fetchval(f"SELECT count(*) FROM {table}"))
            counts[table] = count
            ROWS_TOTAL.labels(SERVICE_NAME, table).set(count)
    return counts


async def set_scenario(name: str, enabled: bool) -> None:
    scenario_state[name] = enabled
    SCENARIO_ON.labels(SERVICE_NAME, name).set(1 if enabled else 0)
    await exec_db(
        "scenario_flag",
        """
        INSERT INTO scenario_flags(name, enabled, updated_at)
        VALUES($1, $2, now())
        ON CONFLICT (name)
        DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = now()
        """,
        name,
        enabled,
    )
    await exec_db(
        "scenario_event",
        "INSERT INTO audit_logs(event_type, payload) VALUES($1, $2::jsonb)",
        "scenario.changed",
        json_arg({"name": name, "enabled": enabled, "service": SERVICE_NAME, "at": utc_now()}),
    )


async def sync_scenarios_from_db(force: bool = False) -> dict[str, bool]:
    global last_scenario_sync
    now = time.monotonic()
    if not force and now - last_scenario_sync < 0.8:
        return dict(scenario_state)
    rows = await timed_db("scenario_flags", "SELECT name, enabled FROM scenario_flags")
    for row in rows:
        name = row["name"]
        if name in scenario_state:
            enabled = bool(row["enabled"])
            scenario_state[name] = enabled
            SCENARIO_ON.labels(SERVICE_NAME, name).set(1 if enabled else 0)
    last_scenario_sync = now
    return dict(scenario_state)


def track(task: asyncio.Task[Any]) -> None:
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)


def kube_available() -> bool:
    return bool(os.getenv("KUBERNETES_SERVICE_HOST")) and os.path.exists(KUBE_TOKEN_PATH)


def kube_request(
    method: str,
    path: str,
    payload: dict[str, Any] | list[dict[str, Any]] | None = None,
    content_type: str = "application/merge-patch+json",
) -> dict[str, Any]:
    host = os.getenv("KUBERNETES_SERVICE_HOST")
    port = os.getenv("KUBERNETES_SERVICE_PORT_HTTPS", "443")
    if not host or not os.path.exists(KUBE_TOKEN_PATH):
        raise RuntimeError("Kubernetes service account is not available")

    with open(KUBE_TOKEN_PATH, encoding="utf-8") as token_file:
        token = token_file.read().strip()

    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {token}",
    }
    data = None
    if payload is not None:
        headers["content-type"] = content_type
        data = json.dumps(payload).encode("utf-8")

    cafile = KUBE_CA_PATH if os.path.exists(KUBE_CA_PATH) else None
    context = ssl.create_default_context(cafile=cafile)
    request = urllib.request.Request(
        f"https://{host}:{port}{path}",
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=4, context=context) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Kubernetes API {method} {path} failed with {exc.code}: {detail[:260]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Kubernetes API {method} {path} is unreachable: {exc.reason}") from exc


async def kube_json(
    method: str,
    path: str,
    payload: dict[str, Any] | list[dict[str, Any]] | None = None,
    content_type: str = "application/merge-patch+json",
) -> dict[str, Any]:
    return await asyncio.to_thread(kube_request, method, path, payload, content_type)


def parse_age_seconds(timestamp: str | None) -> int:
    if not timestamp:
        return 0
    try:
        created = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return max(0, int((datetime.now(UTC) - created).total_seconds()))
    except ValueError:
        return 0


def pod_summary(pod: dict[str, Any]) -> dict[str, Any]:
    metadata = pod.get("metadata", {})
    status = pod.get("status", {})
    container_statuses = status.get("containerStatuses") or []
    ready = bool(container_statuses) and all(container.get("ready", False) for container in container_statuses)
    restarts = sum(int(container.get("restartCount", 0)) for container in container_statuses)
    return {
        "name": metadata.get("name", "unknown"),
        "phase": status.get("phase", "Unknown"),
        "ready": ready,
        "restarts": restarts,
        "node": status.get("hostIP", "pending"),
        "pod_ip": status.get("podIP"),
        "age_seconds": parse_age_seconds(metadata.get("creationTimestamp")),
    }


def hpa_summary(hpa: dict[str, Any] | None) -> dict[str, Any]:
    if not hpa:
        return {"available": False}
    spec = hpa.get("spec", {})
    status = hpa.get("status", {})
    target_cpu = None
    for metric in spec.get("metrics", []):
        resource = metric.get("resource", {})
        if resource.get("name") == "cpu":
            target_cpu = resource.get("target", {}).get("averageUtilization")
    current_cpu = None
    for metric in status.get("currentMetrics", []) or []:
        resource = metric.get("resource", {})
        if resource.get("name") == "cpu":
            current_cpu = resource.get("current", {}).get("averageUtilization")
    return {
        "available": True,
        "min_replicas": spec.get("minReplicas"),
        "max_replicas": spec.get("maxReplicas"),
        "target_cpu_utilization": target_cpu,
        "current_cpu_utilization": current_cpu,
        "current_replicas": status.get("currentReplicas"),
        "desired_replicas": status.get("desiredReplicas"),
    }


def container_env_value(deployment: dict[str, Any], name: str) -> str | None:
    containers = deployment.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    if not containers:
        return None
    for item in containers[0].get("env", []):
        if item.get("name") == name:
            return item.get("value")
    return None


async def cluster_snapshot() -> dict[str, Any]:
    base = {
        "available": False,
        "namespace": KUBE_NAMESPACE,
        "deployment": DEPLOYMENT_NAME,
        "desired_replicas": BASE_REPLICAS,
        "ready_replicas": 0,
        "available_replicas": 0,
        "updated_replicas": 0,
        "generation": 0,
        "observed_generation": 0,
        "template_version": "unknown",
        "template_flavor": "unknown",
        "rollout_complete": False,
        "pods": [],
        "hpa": {"available": False},
    }
    if not kube_available():
        return {**base, "reason": "not running inside Kubernetes"}

    selector = urllib.parse.quote(f"app.kubernetes.io/name={SERVICE_NAME},app.kubernetes.io/component=api", safe="")
    try:
        deployment, pods = await asyncio.gather(
            kube_json("GET", f"/apis/apps/v1/namespaces/{KUBE_NAMESPACE}/deployments/{DEPLOYMENT_NAME}"),
            kube_json("GET", f"/api/v1/namespaces/{KUBE_NAMESPACE}/pods?labelSelector={selector}"),
        )
        try:
            hpa = await kube_json("GET", f"/apis/autoscaling/v2/namespaces/{KUBE_NAMESPACE}/horizontalpodautoscalers/{DEPLOYMENT_NAME}")
        except RuntimeError:
            hpa = None
        status = deployment.get("status", {})
        desired = int(deployment.get("spec", {}).get("replicas") or 0)
        updated = int(status.get("updatedReplicas") or 0)
        available = int(status.get("availableReplicas") or 0)
        generation = int(deployment.get("metadata", {}).get("generation") or 0)
        observed_generation = int(status.get("observedGeneration") or 0)
        pod_items = sorted(
            (pod_summary(pod) for pod in pods.get("items", [])),
            key=lambda item: item["name"],
        )
        return {
            **base,
            "available": True,
            "desired_replicas": desired,
            "ready_replicas": int(status.get("readyReplicas") or 0),
            "available_replicas": available,
            "updated_replicas": updated,
            "generation": generation,
            "observed_generation": observed_generation,
            "template_version": container_env_value(deployment, "APP_VERSION") or "unknown",
            "template_flavor": container_env_value(deployment, "APP_FLAVOR") or "unknown",
            "rollout_complete": observed_generation >= generation and updated == desired and available == desired,
            "pods": pod_items,
            "hpa": hpa_summary(hpa),
        }
    except RuntimeError as exc:
        return {**base, "error": str(exc)}


async def scale_api_deployment(replicas: int) -> dict[str, Any]:
    if not kube_available():
        raise RuntimeError("Kubernetes service account is not available")
    return await kube_json(
        "PATCH",
        f"/apis/apps/v1/namespaces/{KUBE_NAMESPACE}/deployments/{DEPLOYMENT_NAME}/scale",
        {"spec": {"replicas": replicas}},
    )


async def patch_hpa_bounds(min_replicas: int, max_replicas: int) -> dict[str, Any] | None:
    if not kube_available():
        raise RuntimeError("Kubernetes service account is not available")
    return await kube_json(
        "PATCH",
        f"/apis/autoscaling/v2/namespaces/{KUBE_NAMESPACE}/horizontalpodautoscalers/{DEPLOYMENT_NAME}",
        {"spec": {"minReplicas": min_replicas, "maxReplicas": max_replicas}},
    )


async def patch_release(version: str, flavor: str) -> dict[str, Any]:
    if not kube_available():
        raise RuntimeError("Kubernetes service account is not available")
    patch = [
        {"op": "replace", "path": "/spec/template/spec/containers/0/env/3/value", "value": version},
        {"op": "replace", "path": "/spec/template/spec/containers/0/env/4/value", "value": flavor},
        {"op": "add", "path": "/spec/template/metadata/annotations/releases.bot-service.io~1updated-at", "value": utc_now()},
        {"op": "add", "path": "/spec/template/metadata/annotations/releases.bot-service.io~1version", "value": version},
    ]
    return await kube_json(
        "PATCH",
        f"/apis/apps/v1/namespaces/{KUBE_NAMESPACE}/deployments/{DEPLOYMENT_NAME}",
        patch,
        "application/json-patch+json",
    )


def live_release_version(label: str) -> str:
    return f"v1.2.{int(time.time()) % 10000}-{label}"


def replicas_for_tps(target_tps: int) -> int:
    return max(BASE_REPLICAS, (max(1, target_tps) + 999) // 1000)


def clamp_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def positive_int(value: Any, default: int = 1000) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(1, parsed)


def parse_traffic_payload(payload: dict[str, Any]) -> tuple[int, str, int]:
    target_tps = positive_int(payload.get("target_tps"), 1000)
    mode = str(payload.get("mode", "manual")).lower()
    if mode not in {"manual", "auto"}:
        mode = "manual"
    manual_replicas = positive_int(payload.get("manual_replicas"), BASE_REPLICAS)
    return target_tps, mode, manual_replicas


def receiver_replicas_for_tps(target_tps: int) -> int:
    return max(BASE_REPLICAS, (max(1, target_tps) + RECEIVER_CAPACITY_PER_POD - 1) // RECEIVER_CAPACITY_PER_POD)


def target_replicas_for_mode(target_tps: int, mode: str, manual_replicas: int) -> int:
    return manual_replicas if mode == "manual" else receiver_replicas_for_tps(target_tps)


async def apply_receiver_replicas(target_tps: int, mode: str, manual_replicas: int) -> dict[str, Any]:
    target_replicas = target_replicas_for_mode(target_tps, mode, manual_replicas)
    hpa_max = target_replicas if mode == "manual" else max(target_replicas, HPA_MAX_REPLICAS)
    await patch_hpa_bounds(target_replicas, hpa_max)
    scale = await scale_api_deployment(target_replicas)
    return {
        "target_replicas": target_replicas,
        "hpa_max_replicas": hpa_max,
        "observed_replicas": scale.get("spec", {}).get("replicas"),
    }


async def traffic_cluster_refresh() -> None:
    cluster = await cluster_snapshot()
    async with traffic_lock:
        traffic_state["ready_replicas"] = max(1, int(cluster.get("ready_replicas") or cluster.get("desired_replicas") or BASE_REPLICAS))
        traffic_state["desired_replicas"] = max(1, int(cluster.get("desired_replicas") or traffic_state["ready_replicas"]))


async def record_receiver_batch(units: int, accepted: int, failed: int) -> None:
    await exec_db(
        "traffic_link_receiver",
        "INSERT INTO bot_events(event_type, payload) VALUES($1, $2::jsonb)",
        "traffic.received" if failed == 0 else "traffic.overloaded",
        json_arg({
            "units": units,
            "accepted": accepted,
            "failed": failed,
            "queue_depth": round(float(traffic_state["queue_depth"])),
            "pod": POD_NAME,
            "service": SERVICE_NAME,
            "at": utc_now(),
        }),
    )
    await exec_db(
        "traffic_link_jobs",
        "INSERT INTO jobs(status, payload) VALUES($1, $2::jsonb)",
        "queued" if failed == 0 else "failed",
        json_arg({"units": units, "accepted": accepted, "failed": failed, "service": SERVICE_NAME}),
    )


async def receiver_loop() -> None:
    last_cluster_refresh = 0.0
    last_metric = 0.0
    while True:
        async with traffic_lock:
            running = bool(traffic_state["running"])
        if not running:
            return

        now = time.monotonic()
        if now - last_cluster_refresh >= 1:
            await traffic_cluster_refresh()
            last_cluster_refresh = now

        async with traffic_lock:
            elapsed = max(0.05, now - float(traffic_state["last_tick"]))
            traffic_state["last_tick"] = now
            process_capacity = RECEIVER_CAPACITY_PER_POD * POD_PROCESS_FACTOR * elapsed
            processed = int(min(float(traffic_state["queue_depth"]), process_capacity))
            traffic_state["queue_depth"] = max(0.0, float(traffic_state["queue_depth"]) - processed)
            traffic_state["processed_total"] = int(traffic_state["processed_total"]) + processed
            traffic_state["processed_per_second"] = processed
            traffic_state["updated_at"] = utc_now()
            queue_depth = round(float(traffic_state["queue_depth"]))

        if processed > 0:
            local_cpu_work(min(0.09, processed / 24000))
            await exec_db(
                "traffic_processed_event",
                "INSERT INTO bot_events(event_type, payload) VALUES($1, $2::jsonb)",
                "traffic.processed",
                json_arg({"processed": processed, "queue_depth": queue_depth, "pod": POD_NAME, "service": SERVICE_NAME, "at": utc_now()}),
            )

        if now - last_metric >= 1:
            last_metric = now
            await exec_db(
                "traffic_link_processed",
                "INSERT INTO telemetry_samples(metric, value) VALUES($1, $2)",
                "traffic.link.processed",
                processed,
            )
        await asyncio.sleep(0.2)


async def traffic_snapshot(cluster: dict[str, Any]) -> dict[str, Any]:
    ready = max(1, int(cluster.get("ready_replicas") or cluster.get("desired_replicas") or BASE_REPLICAS))
    desired = max(1, int(cluster.get("desired_replicas") or ready))
    traffic_state["ready_replicas"] = ready
    traffic_state["desired_replicas"] = desired
    pod_rows = await timed_db(
        "traffic_receiver_pods",
        """
        SELECT
          payload->>'pod' AS pod,
          COALESCE(ROUND(SUM((payload->>'units')::bigint) FILTER (
            WHERE event_type IN ('traffic.received', 'traffic.overloaded')
              AND created_at >= now() - interval '2 seconds'
          ) / 2.0), 0) AS received_per_second,
          COALESCE(ROUND(SUM((payload->>'processed')::bigint) FILTER (
            WHERE event_type = 'traffic.processed'
              AND created_at >= now() - interval '2 seconds'
          ) / 2.0), 0) AS processed_per_second,
          COALESCE(MAX((payload->>'queue_depth')::double precision), 0) AS queue_depth
        FROM bot_events
        WHERE event_type IN ('traffic.received', 'traffic.overloaded', 'traffic.processed')
          AND created_at >= now() - interval '3 seconds'
          AND payload ? 'pod'
        GROUP BY payload->>'pod'
        """,
    )
    pod_stats = {
        row["pod"]: {
            "received_per_second": int(row["received_per_second"] or 0),
            "processed_per_second": int(row["processed_per_second"] or 0),
            "queue_depth": round(float(row["queue_depth"] or 0)),
        }
        for row in pod_rows
        if row["pod"]
    }
    queue_depth = sum(float(item["queue_depth"]) for item in pod_stats.values()) or float(traffic_state["queue_depth"])
    received_per_second = sum(int(item["received_per_second"]) for item in pod_stats.values())
    processed_per_second = sum(int(item["processed_per_second"]) for item in pod_stats.values())
    pressure = min(1.0, queue_depth / max(ready * RECEIVER_CAPACITY_PER_POD * 2, 1))
    return {
        **traffic_state,
        "queue_depth": round(queue_depth),
        "received_per_second": received_per_second,
        "processed_per_second": processed_per_second,
        "pressure": round(pressure, 3),
        "capacity_per_second": ready * RECEIVER_CAPACITY_PER_POD,
        "pod_stats": pod_stats,
    }


async def cpu_burn() -> None:
    await set_scenario("load", True)
    deadline = time.monotonic() + CPU_BURN_SECONDS
    while time.monotonic() < deadline and scenario_state["load"]:
        start = time.perf_counter()
        while time.perf_counter() - start < 0.18:
            _ = sum(i * i for i in range(650))
        await asyncio.sleep(0.02)
    await set_scenario("load", False)


async def bulk_insert() -> None:
    await set_scenario("db_bulk_insert", True)
    rows = 0
    try:
        while rows < SCENARIO_MAX_ROWS and scenario_state["db_bulk_insert"]:
            batch = min(250, SCENARIO_MAX_ROWS - rows)
            async with current_pool().acquire() as conn:
                if SERVICE_KIND == "fleet":
                    await conn.executemany(
                        "INSERT INTO jobs(status, payload) VALUES($1, $2::jsonb)",
                        [("queued", json_arg({"source": "bulk", "n": rows + i, "service": SERVICE_NAME})) for i in range(batch)],
                    )
                    await conn.executemany(
                        "INSERT INTO bot_events(event_type, payload) VALUES($1, $2::jsonb)",
                        [("job.queued", json_arg({"n": rows + i, "service": SERVICE_NAME})) for i in range(batch)],
                    )
                else:
                    await conn.executemany(
                        "INSERT INTO audit_logs(event_type, payload) VALUES($1, $2::jsonb)",
                        [("order.audit", json_arg({"n": rows + i, "service": SERVICE_NAME})) for i in range(batch)],
                    )
                    await conn.executemany(
                        "INSERT INTO orders(product_id, quantity, status, total_cents) VALUES(1, 1, $1, $2)",
                        [("bulk", random.randint(7000, 59000)) for _ in range(batch)],
                    )
            rows += batch
            await asyncio.sleep(0.04)
    finally:
        await set_scenario("db_bulk_insert", False)
        await refresh_row_gauges()


async def hold_inventory_lock() -> None:
    await set_scenario("db_lock", True)
    try:
        async with current_pool().acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT * FROM inventory_items WHERE sku = 'fleet-main' FOR UPDATE")
                await asyncio.sleep(LOCK_SECONDS)
    finally:
        await set_scenario("db_lock", False)


def local_cpu_work(seconds: float) -> int:
    checksum = 0
    deadline = time.perf_counter() + seconds
    while time.perf_counter() < deadline:
        checksum = (checksum + sum(i * i for i in range(900))) % 999_983
    return checksum


@asynccontextmanager
async def lifespan(_: FastAPI):
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10, command_timeout=20)
    await init_schema()
    await refresh_row_gauges()
    await sync_scenarios_from_db(force=True)
    for name, enabled in scenario_state.items():
        SCENARIO_ON.labels(SERVICE_NAME, name).set(1 if enabled else 0)
    yield
    for task in list(background_tasks):
        task.cancel()
    await asyncio.gather(*background_tasks, return_exceptions=True)
    await pool.close()


app = FastAPI(title=SERVICE_TITLE, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def observe_requests(request: Request, call_next):
    start = time.perf_counter()
    status = 500
    path = request.url.path
    try:
        scenarios = dict(scenario_state)
        if path.startswith("/api/"):
            try:
                scenarios = await sync_scenarios_from_db()
            except Exception:
                scenarios = dict(scenario_state)
        if scenarios["error_spike"] and path.startswith("/api/") and random.random() < 0.35:
            ERRORS.labels(SERVICE_NAME, "error_spike").inc()
            status = 503
            return JSONResponse({"detail": "intentional fleet failure spike"}, status_code=503)
        if scenarios["db_slow_query"] and path.startswith("/api/"):
            await timed_db("intentional_slow_query", "SELECT pg_sleep(0.08)")
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        elapsed = time.perf_counter() - start
        latency_samples.append(elapsed)
        REQUESTS.labels(SERVICE_NAME, path, request.method, str(status)).inc()
        LATENCY.labels(SERVICE_NAME, path).observe(elapsed)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"status": "ok", "service": SERVICE_NAME, "version": APP_VERSION}


@app.get("/ready")
async def ready() -> dict[str, Any]:
    if APP_FLAVOR == "bad-crash":
        raise HTTPException(status_code=503, detail="bad rollout readiness failure")
    await timed_db("ready", "SELECT 1")
    return {"status": "ready", "service": SERVICE_NAME, "version": APP_VERSION}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/status")
async def status() -> dict[str, Any]:
    scenarios = await sync_scenarios_from_db(force=True)
    counts, cluster = await asyncio.gather(refresh_row_gauges(), cluster_snapshot())
    traffic = await traffic_snapshot(cluster)
    samples = list(latency_samples)
    p95 = sorted(samples)[int(len(samples) * 0.95) - 1] if samples else 0
    return {
        "service": SERVICE_NAME,
        "title": SERVICE_TITLE,
        "kind": SERVICE_KIND,
        "version": APP_VERSION,
        "flavor": APP_FLAVOR,
        "generated_at": utc_now(),
        "scenarios": scenarios,
        "metrics": {
            "p95_latency_ms": round(p95 * 1000, 1),
            "request_samples": len(samples),
            "background_tasks": len(background_tasks),
        },
        "rows": counts,
        "cluster": cluster,
        "traffic": traffic,
    }


@app.get("/api/products")
async def products() -> dict[str, Any]:
    rows = await timed_db("products", "SELECT id, name, price_cents, stock FROM products ORDER BY id")
    return {"products": [dict(r) for r in rows]}


@app.get("/api/orders")
async def orders() -> dict[str, Any]:
    if APP_FLAVOR == "bad-schema":
        await timed_db("bad_schema", "SELECT missing_fleet_column FROM orders LIMIT 1")
    rows = await timed_db(
        "orders",
        """
        SELECT id, quantity, status, total_cents, created_at
        FROM orders ORDER BY id DESC LIMIT 20
        """,
    )
    return {"orders": [dict(r) for r in rows]}


@app.post("/api/orders")
async def create_order() -> dict[str, Any]:
    if APP_FLAVOR == "bad-schema":
        ERRORS.labels(SERVICE_NAME, "bad_schema").inc()
        await timed_db("bad_schema", "SELECT missing_fleet_column FROM orders LIMIT 1")
    async with current_pool().acquire() as conn:
        async with conn.transaction():
            product = await conn.fetchrow("SELECT id, price_cents FROM products ORDER BY random() LIMIT 1")
            if product is None:
                raise HTTPException(status_code=500, detail="no product seed data")
            order_id = await conn.fetchval(
                "INSERT INTO orders(product_id, quantity, status, total_cents) VALUES($1, 1, 'paid', $2) RETURNING id",
                product["id"],
                product["price_cents"],
            )
            await conn.execute(
                "INSERT INTO payments(order_id, status, provider_latency_ms) VALUES($1, 'approved', $2)",
                order_id,
                random.randint(20, 900),
            )
            await conn.execute("UPDATE inventory_items SET available = available - 1, updated_at = now() WHERE sku = 'fleet-main'")
    return {"order_id": order_id, "status": "paid"}


@app.post("/api/work")
async def work_unit(request: Request) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    bot_id = str(payload.get("bot_id", "bot-unknown"))[:64]
    failure_rate = max(0.0, min(0.8, float(payload.get("failure_rate", 0) or 0)))
    units = max(1, min(1000, int(payload.get("units", 1) or 1)))
    pressure = 0.07 if scenario_state["scale_surge"] or scenario_state["load"] else 0.025
    pressure = min(0.14, pressure + units / 24000)
    checksum = local_cpu_work(pressure)
    failures = sum(1 for _ in range(units) if random.random() < failure_rate)
    successes = units - failures
    failed = failures > 0
    async with current_pool().acquire() as conn:
        if SERVICE_KIND == "fleet":
            job_id = await conn.fetchval(
                "INSERT INTO jobs(status, payload) VALUES($1, $2::jsonb) RETURNING id",
                "failed" if failed else "queued",
                json_arg({
                    "source": "traffic-surge",
                    "bot_id": bot_id,
                    "units": units,
                    "successes": successes,
                    "failures": failures,
                    "checksum": checksum,
                    "service": SERVICE_NAME,
                }),
            )
            await conn.execute(
                "INSERT INTO bot_events(event_type, payload) VALUES($1, $2::jsonb)",
                "traffic.failed" if failed else "traffic.work",
                json_arg({"job_id": job_id, "bot_id": bot_id, "units": units, "successes": successes, "failures": failures}),
            )
            target_bot = random.randint(1, 18)
            await conn.execute("UPDATE bots SET status = 'busy', current_load = $1, updated_at = now() WHERE id = $2", random.randint(35, 99), target_bot)
        else:
            await conn.execute(
                "INSERT INTO audit_logs(event_type, payload) VALUES($1, $2::jsonb)",
                "traffic.failed" if failed else "traffic.work",
                json_arg({
                    "bot_id": bot_id,
                    "units": units,
                    "successes": successes,
                    "failures": failures,
                    "checksum": checksum,
                    "service": SERVICE_NAME,
                    "at": utc_now(),
                }),
            )
            await conn.execute(
                "INSERT INTO telemetry_samples(metric, value) VALUES($1, $2)",
                "checkout.failed" if failed else "checkout.work",
                failures if failed else successes,
            )
    if failed:
        ERRORS.labels(SERVICE_NAME, "bot_work_failed").inc()
    return {
        "ok": not failed,
        "status": "failed" if failed else "accepted",
        "bot_id": bot_id,
        "units": units,
        "success": successes,
        "failure": failures,
        "checksum": checksum,
        "service": SERVICE_NAME,
    }


@app.get("/api/bots")
async def bots() -> dict[str, Any]:
    rows = await timed_db("bots", "SELECT id, name, status, current_load, updated_at FROM bots ORDER BY id LIMIT 80")
    return {"bots": [dict(r) for r in rows]}


@app.post("/api/jobs/enqueue")
async def enqueue_job() -> dict[str, Any]:
    async with current_pool().acquire() as conn:
        job_id = await conn.fetchval(
            "INSERT INTO jobs(status, payload) VALUES('queued', $1::jsonb) RETURNING id",
            json_arg({"source": "manual", "service": SERVICE_NAME, "at": utc_now()}),
        )
        await conn.execute("INSERT INTO bot_events(event_type, payload) VALUES('job.queued', $1::jsonb)", json_arg({"job_id": job_id}))
    return {"job_id": job_id, "status": "queued"}


@app.post("/api/jobs/process")
async def process_job() -> dict[str, Any]:
    async with current_pool().acquire() as conn:
        job = await conn.fetchrow("SELECT id FROM jobs WHERE status = 'queued' ORDER BY id LIMIT 1")
        if job is None:
            return {"processed": 0}
        await conn.execute("UPDATE jobs SET status = 'done', completed_at = now() WHERE id = $1", job["id"])
        await conn.execute("INSERT INTO telemetry_samples(metric, value) VALUES('job.processed', 1)")
    return {"processed": 1, "job_id": job["id"]}


@app.post("/api/scenarios/load/start")
async def start_load() -> dict[str, Any]:
    track(asyncio.create_task(cpu_burn()))
    return {"scenario": "load", "status": "started", "seconds": CPU_BURN_SECONDS}


@app.post("/api/scenarios/load/stop")
async def stop_load() -> dict[str, Any]:
    await set_scenario("load", False)
    return {"scenario": "load", "status": "stopped"}


@app.post("/api/traffic/receiver/start")
async def start_receiver(request: Request) -> dict[str, Any]:
    global receiver_task
    payload: dict[str, Any] = {}
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    target_tps, mode, manual_replicas = parse_traffic_payload(payload)
    source = str(payload.get("source", "")).lower()
    if source == "sender":
        cluster = await cluster_snapshot()
        scale = {
            "target_replicas": int(cluster.get("desired_replicas") or BASE_REPLICAS),
            "hpa_max_replicas": int(cluster.get("hpa", {}).get("max_replicas") or HPA_MAX_REPLICAS),
            "observed_replicas": int(cluster.get("desired_replicas") or BASE_REPLICAS),
        }
        manual_replicas = int(scale["target_replicas"])
    else:
        try:
            scale = await apply_receiver_replicas(target_tps, mode, manual_replicas)
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
    async with traffic_lock:
        traffic_state.update({
            "running": True,
            "mode": mode,
            "target_tps": target_tps,
            "manual_replicas": manual_replicas,
            "queue_depth": 0.0,
            "received_total": 0,
            "processed_total": 0,
            "failed_total": 0,
            "last_tick": time.monotonic(),
            "updated_at": utc_now(),
        })
    await set_scenario("traffic_link", True)
    await set_scenario("scale_surge", mode == "auto")
    if receiver_task is None or receiver_task.done():
        receiver_task = asyncio.create_task(receiver_loop())
        track(receiver_task)
    return {
        "scenario": "traffic_link",
        "status": "receiver_started",
        "mode": mode,
        "target_tps": target_tps,
        **scale,
        "ready_replicas": traffic_state["ready_replicas"],
        "queue_depth": round(float(traffic_state["queue_depth"])),
    }


@app.post("/api/traffic/receiver/scale")
async def scale_receiver(request: Request) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    manual_replicas = positive_int(payload.get("manual_replicas"), BASE_REPLICAS)
    try:
        scale = await apply_receiver_replicas(1, "manual", manual_replicas)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    async with traffic_lock:
        traffic_state["manual_replicas"] = manual_replicas
        traffic_state["desired_replicas"] = manual_replicas
        traffic_state["updated_at"] = utc_now()
    return {"status": "scaled", **scale}


@app.post("/api/traffic/receiver/stop")
async def stop_receiver() -> dict[str, Any]:
    async with traffic_lock:
        traffic_state.update({
            "running": False,
            "target_tps": 0,
            "queue_depth": 0.0,
            "updated_at": utc_now(),
        })
    cluster = await cluster_snapshot()
    await set_scenario("traffic_link", False)
    await set_scenario("scale_surge", False)
    await set_scenario("load", False)
    return {
        "scenario": "traffic_link",
        "status": "receiver_stopped",
        "target_replicas": int(cluster.get("desired_replicas") or BASE_REPLICAS),
        "observed_replicas": int(cluster.get("desired_replicas") or BASE_REPLICAS),
    }


@app.post("/api/traffic/receive")
async def receive_traffic(request: Request) -> dict[str, Any]:
    global receiver_task
    payload: dict[str, Any] = {}
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    units = clamp_int(payload.get("units"), 1, 1, 1000)
    target_tps = positive_int(payload.get("target_tps"), int(traffic_state.get("target_tps") or 1000))
    sender = str(payload.get("sender", "unknown"))[:80]
    should_start_loop = False
    async with traffic_lock:
        if not traffic_state["running"]:
            traffic_state.update({
                "running": True,
                "target_tps": target_tps,
                "last_tick": time.monotonic(),
            })
            should_start_loop = True
        ready = max(1, int(traffic_state["ready_replicas"]))
        queue_limit = RECEIVER_CAPACITY_PER_POD * POD_PROCESS_FACTOR * 6
        overflow = max(0, int(float(traffic_state["queue_depth"]) + units - queue_limit))
        failed = min(units, overflow)
        accepted = units - failed
        traffic_state["queue_depth"] = max(0.0, float(traffic_state["queue_depth"]) + accepted)
        traffic_state["received_total"] = int(traffic_state["received_total"]) + units
        traffic_state["failed_total"] = int(traffic_state["failed_total"]) + failed
        traffic_state["received_per_second"] = units
        traffic_state["target_tps"] = target_tps
        traffic_state["updated_at"] = utc_now()
        queue_depth = round(float(traffic_state["queue_depth"]))
        processed_total = int(traffic_state["processed_total"])
        desired = int(traffic_state["desired_replicas"])
    if should_start_loop:
        await set_scenario("traffic_link", True)
        if receiver_task is None or receiver_task.done():
            receiver_task = asyncio.create_task(receiver_loop())
            track(receiver_task)
    if failed:
        ERRORS.labels(SERVICE_NAME, "receiver_overloaded").inc()
    await record_receiver_batch(units, accepted, failed)
    return {
        "status": "accepted" if failed == 0 else "overloaded",
        "sender": sender,
        "units": units,
        "accepted": accepted,
        "failed": failed,
        "queue_depth": queue_depth,
        "processed_total": processed_total,
        "ready_replicas": ready,
        "desired_replicas": desired,
        "capacity_per_second": ready * RECEIVER_CAPACITY_PER_POD,
        "service": SERVICE_NAME,
    }


@app.post("/api/scenarios/scale-surge/start")
async def start_scale_surge() -> dict[str, Any]:
    await set_scenario("scale_surge", True)
    try:
        await patch_hpa_bounds(SURGE_REPLICAS, HPA_MAX_REPLICAS)
        scale = await scale_api_deployment(SURGE_REPLICAS)
    except RuntimeError as exc:
        await set_scenario("scale_surge", False)
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    track(asyncio.create_task(cpu_burn()))
    track(asyncio.create_task(bulk_insert()))
    return {
        "scenario": "scale_surge",
        "status": "started",
        "from": BASE_REPLICAS,
        "to": SURGE_REPLICAS,
        "observed_replicas": scale.get("spec", {}).get("replicas"),
    }


@app.post("/api/scenarios/scale-surge/stop")
async def stop_scale_surge() -> dict[str, Any]:
    try:
        await patch_hpa_bounds(BASE_REPLICAS, BASE_REPLICAS)
        scale = await scale_api_deployment(BASE_REPLICAS)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    await set_scenario("scale_surge", False)
    await set_scenario("load", False)
    return {
        "scenario": "scale_surge",
        "status": "stopped",
        "to": BASE_REPLICAS,
        "observed_replicas": scale.get("spec", {}).get("replicas"),
    }


@app.post("/api/releases/deploy")
async def deploy_release() -> dict[str, Any]:
    version = live_release_version("live")
    try:
        deployment = await patch_release(version, "stable")
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    await exec_db(
        "release_event",
        "INSERT INTO audit_logs(event_type, payload) VALUES($1, $2::jsonb)",
        "release.deploy",
        json_arg({"version": version, "flavor": "stable", "service": SERVICE_NAME, "at": utc_now()}),
    )
    return {
        "status": "deploying",
        "version": version,
        "flavor": "stable",
        "generation": deployment.get("metadata", {}).get("generation"),
    }


@app.post("/api/releases/faulty")
async def deploy_faulty_release() -> dict[str, Any]:
    version = live_release_version("faulty")
    flavor = "bad-schema" if SERVICE_KIND == "checkout" else "bad-crash"
    try:
        deployment = await patch_release(version, flavor)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    await exec_db(
        "release_event",
        "INSERT INTO audit_logs(event_type, payload) VALUES($1, $2::jsonb)",
        "release.faulty",
        json_arg({"version": version, "flavor": flavor, "service": SERVICE_NAME, "at": utc_now()}),
    )
    return {
        "status": "deploying",
        "version": version,
        "flavor": flavor,
        "generation": deployment.get("metadata", {}).get("generation"),
    }


@app.post("/api/releases/rollback")
async def rollback_release() -> dict[str, Any]:
    version = "v1.0.1-rollback"
    scale_error = None
    try:
        await patch_hpa_bounds(BASE_REPLICAS, BASE_REPLICAS)
        await scale_api_deployment(BASE_REPLICAS)
        deployment = await patch_release(version, "stable")
    except RuntimeError as exc:
        scale_error = str(exc)
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    for name in list(scenario_state):
        await set_scenario(name, False)
    await exec_db(
        "release_event",
        "INSERT INTO audit_logs(event_type, payload) VALUES($1, $2::jsonb)",
        "release.rollback",
        json_arg({"version": version, "flavor": "stable", "service": SERVICE_NAME, "at": utc_now()}),
    )
    return {
        "status": "rolling_back",
        "version": version,
        "flavor": "stable",
        "replicas": BASE_REPLICAS,
        "scale_error": scale_error,
        "generation": deployment.get("metadata", {}).get("generation"),
    }


@app.post("/api/scenarios/db-bulk-insert/start")
async def start_bulk_insert() -> dict[str, Any]:
    track(asyncio.create_task(bulk_insert()))
    return {"scenario": "db_bulk_insert", "status": "started", "max_rows": SCENARIO_MAX_ROWS}


@app.post("/api/scenarios/db-lock/start")
async def start_db_lock() -> dict[str, Any]:
    track(asyncio.create_task(hold_inventory_lock()))
    return {"scenario": "db_lock", "status": "started", "seconds": LOCK_SECONDS}


@app.post("/api/scenarios/db-slow-query/start")
async def start_slow_query() -> dict[str, Any]:
    await set_scenario("db_slow_query", True)
    return {"scenario": "db_slow_query", "status": "started"}


@app.post("/api/scenarios/error-spike/start")
async def start_error_spike() -> dict[str, Any]:
    await set_scenario("error_spike", True)
    return {"scenario": "error_spike", "status": "started"}


@app.post("/api/scenarios/crashloop/start")
async def start_crashloop() -> dict[str, Any]:
    await exec_db(
        "crashloop_event",
        "INSERT INTO audit_logs(event_type, payload) VALUES($1, $2::jsonb)",
        "scenario.crashloop",
        json_arg({"service": SERVICE_NAME, "version": APP_VERSION, "at": utc_now()}),
    )
    os._exit(42)


@app.post("/api/scenarios/recover")
async def recover() -> dict[str, Any]:
    async with traffic_lock:
        traffic_state.update({
            "running": False,
            "target_tps": 0,
            "queue_depth": 0.0,
            "updated_at": utc_now(),
        })
    scale_error = None
    if kube_available():
        try:
            await patch_hpa_bounds(BASE_REPLICAS, BASE_REPLICAS)
            await scale_api_deployment(BASE_REPLICAS)
        except RuntimeError as exc:
            scale_error = str(exc)
    for name in list(scenario_state):
        await set_scenario(name, False)
    return {"status": "recovering", "service": SERVICE_NAME, "replicas": BASE_REPLICAS, "scale_error": scale_error}
