from __future__ import annotations

import asyncio
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
APP_VERSION = os.getenv("APP_VERSION", "v1.0.0-stable")
APP_FLAVOR = os.getenv("APP_FLAVOR", "stable")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/bot_service_02")
SCENARIO_MAX_ROWS = int(os.getenv("SCENARIO_MAX_ROWS", "10000"))
LOCK_SECONDS = int(os.getenv("LOCK_SECONDS", "35"))
CPU_BURN_SECONDS = int(os.getenv("CPU_BURN_SECONDS", "45"))
KUBE_NAMESPACE = os.getenv("KUBE_NAMESPACE", "sandbox")
DEPLOYMENT_NAME = os.getenv("DEPLOYMENT_NAME", f"{SERVICE_NAME}-api")
BASE_REPLICAS = int(os.getenv("BASE_REPLICAS", "2"))
SURGE_REPLICAS = int(os.getenv("SURGE_REPLICAS", "6"))
KUBE_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
KUBE_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

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
}
background_tasks: set[asyncio.Task[Any]] = set()


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
        "scenario_event",
        "INSERT INTO audit_logs(event_type, payload) VALUES($1, $2::jsonb)",
        "scenario.changed",
        json_arg({"name": name, "enabled": enabled, "service": SERVICE_NAME, "at": utc_now()}),
    )


def track(task: asyncio.Task[Any]) -> None:
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)


def kube_available() -> bool:
    return bool(os.getenv("KUBERNETES_SERVICE_HOST")) and os.path.exists(KUBE_TOKEN_PATH)


def kube_request(method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
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
        headers["content-type"] = "application/merge-patch+json"
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


async def kube_json(method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return await asyncio.to_thread(kube_request, method, path, payload)


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


async def cluster_snapshot() -> dict[str, Any]:
    base = {
        "available": False,
        "namespace": KUBE_NAMESPACE,
        "deployment": DEPLOYMENT_NAME,
        "desired_replicas": BASE_REPLICAS,
        "ready_replicas": 0,
        "available_replicas": 0,
        "updated_replicas": 0,
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
        pod_items = sorted(
            (pod_summary(pod) for pod in pods.get("items", [])),
            key=lambda item: item["name"],
        )
        return {
            **base,
            "available": True,
            "desired_replicas": int(deployment.get("spec", {}).get("replicas") or 0),
            "ready_replicas": int(status.get("readyReplicas") or 0),
            "available_replicas": int(status.get("availableReplicas") or 0),
            "updated_replicas": int(status.get("updatedReplicas") or 0),
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


async def patch_hpa_min_replicas(replicas: int) -> dict[str, Any] | None:
    if not kube_available():
        raise RuntimeError("Kubernetes service account is not available")
    return await kube_json(
        "PATCH",
        f"/apis/autoscaling/v2/namespaces/{KUBE_NAMESPACE}/horizontalpodautoscalers/{DEPLOYMENT_NAME}",
        {"spec": {"minReplicas": replicas}},
    )


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
        if scenario_state["error_spike"] and path.startswith("/api/") and random.random() < 0.35:
            ERRORS.labels(SERVICE_NAME, "error_spike").inc()
            status = 503
            return JSONResponse({"detail": "intentional fleet failure spike"}, status_code=503)
        if scenario_state["db_slow_query"] and path.startswith("/api/"):
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
    counts, cluster = await asyncio.gather(refresh_row_gauges(), cluster_snapshot())
    samples = list(latency_samples)
    p95 = sorted(samples)[int(len(samples) * 0.95) - 1] if samples else 0
    return {
        "service": SERVICE_NAME,
        "title": SERVICE_TITLE,
        "kind": SERVICE_KIND,
        "version": APP_VERSION,
        "flavor": APP_FLAVOR,
        "generated_at": utc_now(),
        "scenarios": scenario_state,
        "metrics": {
            "p95_latency_ms": round(p95 * 1000, 1),
            "request_samples": len(samples),
            "background_tasks": len(background_tasks),
        },
        "rows": counts,
        "cluster": cluster,
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
async def work_unit() -> dict[str, Any]:
    pressure = 0.07 if scenario_state["scale_surge"] or scenario_state["load"] else 0.025
    checksum = local_cpu_work(pressure)
    async with current_pool().acquire() as conn:
        if SERVICE_KIND == "fleet":
            job_id = await conn.fetchval(
                "INSERT INTO jobs(status, payload) VALUES('queued', $1::jsonb) RETURNING id",
                json_arg({"source": "traffic-surge", "checksum": checksum, "service": SERVICE_NAME}),
            )
            await conn.execute("INSERT INTO bot_events(event_type, payload) VALUES('traffic.work', $1::jsonb)", json_arg({"job_id": job_id}))
            target_bot = random.randint(1, 18)
            await conn.execute("UPDATE bots SET status = 'busy', current_load = $1, updated_at = now() WHERE id = $2", random.randint(35, 99), target_bot)
        else:
            await conn.execute(
                "INSERT INTO audit_logs(event_type, payload) VALUES('traffic.work', $1::jsonb)",
                json_arg({"checksum": checksum, "service": SERVICE_NAME, "at": utc_now()}),
            )
            await conn.execute("INSERT INTO telemetry_samples(metric, value) VALUES('checkout.work', $1)", random.random() * 100)
    return {"status": "accepted", "checksum": checksum, "service": SERVICE_NAME}


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


@app.post("/api/scenarios/scale-surge/start")
async def start_scale_surge() -> dict[str, Any]:
    await set_scenario("scale_surge", True)
    try:
        await patch_hpa_min_replicas(SURGE_REPLICAS)
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
        await patch_hpa_min_replicas(BASE_REPLICAS)
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
    scale_error = None
    if kube_available():
        try:
            await patch_hpa_min_replicas(BASE_REPLICAS)
            await scale_api_deployment(BASE_REPLICAS)
        except RuntimeError as exc:
            scale_error = str(exc)
    for name in list(scenario_state):
        await set_scenario(name, False)
    return {"status": "recovering", "service": SERVICE_NAME, "replicas": BASE_REPLICAS, "scale_error": scale_error}
