import { useEffect, useMemo, useRef, useState, type CSSProperties } from 'react';

interface Pod {
  name: string;
  ready: boolean;
}

interface Cluster {
  desired_replicas: number;
  ready_replicas: number;
  pods: Pod[];
}

interface Traffic {
  running: boolean;
  mode: 'manual' | 'auto';
  target_tps: number;
  manual_replicas: number;
  queue_depth: number;
  processed_per_second: number;
  pressure: number;
  pod_stats?: Record<string, { processed_per_second?: number; queue_depth?: number }>;
}

interface Status {
  cluster: Cluster;
  traffic: Traffic;
}

interface ScaleResponse {
  mode: 'manual' | 'auto';
  target_tps: number;
  target_replicas: number;
  observed_replicas?: number;
}

interface PodBox {
  name: string;
  ready: boolean;
  rate: number;
  queue: number;
  weight: number;
  x: number;
  y: number;
  w: number;
  h: number;
}

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const url = init?.method && init.method !== 'GET' ? path : `${path}${path.includes('?') ? '&' : '?'}_=${Date.now()}`;
  const res = await fetch(url, { ...init, cache: 'no-store', headers: { 'content-type': 'application/json', 'cache-control': 'no-store', ...(init?.headers ?? {}) } });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function positiveNumber(value: number, fallback = 1) {
  return Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}

function formatCount(value: number) {
  return Math.round(value || 0).toLocaleString('ko-KR');
}

function formatTileRate(value: number) {
  const rounded = Math.round(value || 0);
  if (rounded >= 1000) return `${(rounded / 1000).toFixed(rounded >= 10000 ? 0 : 1)}k`;
  return `${rounded}`;
}

function formatTileQueue(value: number) {
  const rounded = Math.round(value || 0);
  if (rounded >= 1000) return `Q ${(rounded / 1000).toFixed(rounded >= 10000 ? 0 : 1)}k`;
  return `Q ${rounded}`;
}

function splitBoxes(items: Array<Omit<PodBox, 'x' | 'y' | 'w' | 'h'>>, x = 0, y = 0, w = 1, h = 1): PodBox[] {
  if (items.length === 0) return [];
  if (items.length === 1) return [{ ...items[0], x, y, w, h }];
  const total = items.reduce((sum, item) => sum + Math.max(0.05, item.weight), 0);
  const half = total / 2;
  let splitIndex = 1;
  let runningWeight = 0;
  let bestDelta = Number.POSITIVE_INFINITY;
  for (let index = 1; index < items.length; index += 1) {
    runningWeight += Math.max(0.05, items[index - 1].weight);
    const delta = Math.abs(half - runningWeight);
    if (delta < bestDelta) {
      bestDelta = delta;
      splitIndex = index;
    }
  }
  const firstWeight = items.slice(0, splitIndex).reduce((sum, item) => sum + Math.max(0.05, item.weight), 0);
  const ratio = firstWeight / total;
  if (w >= h) {
    const w1 = w * ratio;
    return [
      ...splitBoxes(items.slice(0, splitIndex), x, y, w1, h),
      ...splitBoxes(items.slice(splitIndex), x + w1, y, w - w1, h),
    ];
  }
  const h1 = h * ratio;
  return [
    ...splitBoxes(items.slice(0, splitIndex), x, y, w, h1),
    ...splitBoxes(items.slice(splitIndex), x, y + h1, w, h - h1),
  ];
}

function moveToward(current: number, target: number, maxStep: number) {
  const delta = target - current;
  if (Math.abs(delta) <= maxStep) return target;
  return current + Math.sign(delta) * maxStep;
}

function mixColor(from: [number, number, number], to: [number, number, number], amount: number) {
  return from.map((value, index) => Math.round(value + (to[index] - value) * amount)) as [number, number, number];
}

function tileBackground(pressure: number, ready: boolean) {
  if (!ready) return 'rgba(237, 240, 242, 0.82)';
  const p = Math.max(0, Math.min(1, pressure));
  const low: [number, number, number] = [134, 183, 164];
  const mid: [number, number, number] = [200, 183, 143];
  const high: [number, number, number] = [189, 117, 131];
  const color = p < 0.5 ? mixColor(low, mid, p / 0.5) : mixColor(mid, high, (p - 0.5) / 0.5);
  return `rgba(${color[0]}, ${color[1]}, ${color[2]}, ${(0.58 + p * 0.3).toFixed(2)})`;
}

function tileStyle(box: PodBox, pressure: number) {
  return {
    left: `${box.x * 100}%`,
    top: `${box.y * 100}%`,
    width: `${box.w * 100}%`,
    height: `${box.h * 100}%`,
    backgroundColor: tileBackground(pressure, box.ready),
  } as CSSProperties;
}

function useSmoothNumber(target: number, unitsPerSecond: number, snap = false) {
  const targetRef = useRef(target);
  const snapRef = useRef(snap);
  const [value, setValue] = useState(target);

  useEffect(() => {
    targetRef.current = target;
    snapRef.current = snap;
    if (snap) setValue(target);
  }, [target, snap]);

  useEffect(() => {
    let frame = 0;
    let previous = performance.now();
    const tick = (now: number) => {
      const elapsed = Math.min(0.05, Math.max(0, (now - previous) / 1000));
      previous = now;
      setValue((current) => {
        if (snapRef.current) return targetRef.current;
        return moveToward(current, targetRef.current, unitsPerSecond * elapsed);
      });
      frame = window.requestAnimationFrame(tick);
    };
    frame = window.requestAnimationFrame(tick);
    return () => window.cancelAnimationFrame(frame);
  }, [unitsPerSecond]);

  return value;
}

function useSmoothPods(targets: Array<Omit<PodBox, 'x' | 'y' | 'w' | 'h'>>, snap = false) {
  const targetsRef = useRef(targets);
  const snapRef = useRef(snap);
  const [value, setValue] = useState(targets);

  useEffect(() => {
    targetsRef.current = targets;
    snapRef.current = snap;
    if (snap) setValue(targets);
  }, [targets, snap]);

  useEffect(() => {
    let frame = 0;
    let previous = performance.now();
    const tick = (now: number) => {
      const elapsed = Math.min(0.05, Math.max(0, (now - previous) / 1000));
      previous = now;
      setValue((current) => {
        if (snapRef.current) return targetsRef.current;
        const currentByName = new Map(current.map((item) => [item.name, item]));
        return targetsRef.current.map((target) => {
          const currentItem = currentByName.get(target.name);
          if (!currentItem) {
            return { ...target, rate: 0, queue: 0, weight: 0.06 };
          }
          return {
            ...target,
            rate: moveToward(currentItem.rate, target.rate, 12000 * elapsed),
            queue: moveToward(currentItem.queue, target.queue, 120000 * elapsed),
            weight: moveToward(currentItem.weight, target.weight, 36 * elapsed),
          };
        });
      });
      frame = window.requestAnimationFrame(tick);
    };
    frame = window.requestAnimationFrame(tick);
    return () => window.cancelAnimationFrame(frame);
  }, []);

  return value;
}

export default function App() {
  const [status, setStatus] = useState<Status | null>(null);
  const [manualReplicas, setManualReplicas] = useState(1);
  const [mode, setMode] = useState<'manual' | 'auto'>('manual');
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const podDraftDirtyRef = useRef(false);
  const modeDraftDirtyRef = useRef(false);
  const mountedRef = useRef(true);
  const refreshInFlightRef = useRef(false);
  const refreshQueuedRef = useRef(false);

  const setBusyState = (next: boolean) => {
    busyRef.current = next;
    setBusy(next);
  };

  const setPodDirty = (next: boolean) => {
    podDraftDirtyRef.current = next;
  };

  const setModeDirty = (next: boolean) => {
    modeDraftDirtyRef.current = next;
  };

  const refresh = async () => {
    if (refreshInFlightRef.current) {
      refreshQueuedRef.current = true;
      return;
    }
    refreshInFlightRef.current = true;
    try {
      const next = await api<Status>('/api/status');
      if (!mountedRef.current) return;
      setStatus(next);
      if (!busyRef.current && !podDraftDirtyRef.current) setManualReplicas(next.traffic.manual_replicas || next.cluster.desired_replicas || 1);
      if (!busyRef.current && !modeDraftDirtyRef.current) setMode(next.traffic.mode ?? 'manual');
    } finally {
      refreshInFlightRef.current = false;
      if (refreshQueuedRef.current && mountedRef.current) {
        refreshQueuedRef.current = false;
        void refresh().catch(() => undefined);
      }
    }
  };

  useEffect(() => {
    mountedRef.current = true;
    let timer = 0;
    let cancelled = false;
    const loop = async () => {
      await refresh().catch(() => undefined);
      if (!cancelled) timer = window.setTimeout(loop, 320);
    };
    void loop();
    return () => {
      cancelled = true;
      mountedRef.current = false;
      window.clearTimeout(timer);
    };
  }, []);

  const traffic = status?.traffic;
  const cluster = status?.cluster;
  const ready = cluster?.ready_replicas ?? 0;
  const desired = cluster?.desired_replicas ?? 1;
  const pressure = traffic?.pressure ?? 0;
  const snapStopped = !traffic?.running;
  const smoothTps = useSmoothNumber(traffic?.processed_per_second ?? 0, 36000, snapStopped);
  const smoothQueue = useSmoothNumber(traffic?.queue_depth ?? 0, 120000, snapStopped);

  const rawPods = useMemo(() => {
    const actual = [...(cluster?.pods ?? [])].sort((left, right) => {
      if (left.ready !== right.ready) return left.ready ? -1 : 1;
      return left.name.localeCompare(right.name);
    });
    const stats = traffic?.pod_stats ?? {};
    const actualRates = actual.map((pod) => stats[pod.name]?.processed_per_second ?? 0);
    const activeAverage = Math.max(1, actualRates.reduce((sum, rate) => sum + rate, 0) / Math.max(1, actualRates.filter((rate) => rate > 0).length));
    const count = Math.max(cluster?.desired_replicas ?? 1, actual.length, 1);
    return Array.from({ length: count }, (_, index) => {
      const pod = actual[index];
      const name = pod?.name ?? `pending-${index + 1}`;
      const stat = stats[name];
      const ready = Boolean(pod?.ready);
      const rate = stat?.processed_per_second ?? 0;
      const queue = stat?.queue_depth ?? 0;
      const rateWeight = rate > 0 ? 1 + Math.min(7, rate / activeAverage) : 0.45;
      const queueWeight = Math.min(7, queue / 800);
      return {
        name,
        ready,
        rate,
        queue,
        weight: ready ? rateWeight + queueWeight : 0.12,
      };
    });
  }, [cluster, traffic?.pod_stats]);

  const pods = useSmoothPods(rawPods, snapStopped);
  const boxes = useMemo(() => splitBoxes(pods), [pods]);

  const applyPods = async () => {
    setBusyState(true);
    try {
      const result = await api<ScaleResponse>('/api/traffic/receiver/scale', {
        method: 'POST',
        body: JSON.stringify({ mode, manual_replicas: manualReplicas }),
      });
      const appliedMode = result.mode === 'auto' ? 'auto' : 'manual';
      const targetReplicas = positiveNumber(result.target_replicas, manualReplicas);
      setMode(appliedMode);
      setPodDirty(false);
      setModeDirty(false);
      setStatus((current) => current ? {
        ...current,
        cluster: {
          ...current.cluster,
          desired_replicas: targetReplicas,
          ready_replicas: Math.min(current.cluster.ready_replicas, targetReplicas),
        },
        traffic: {
          ...current.traffic,
          mode: appliedMode,
          target_tps: result.target_tps,
          manual_replicas: manualReplicas,
          running: current.traffic.running,
        },
      } : current);
      void refresh().catch(() => undefined);
    } finally {
      setBusyState(false);
    }
  };

  return (
    <main className="shell">
      <section className="hero">
        <h1>수신</h1>
      </section>

      <section className="control-panel receiver">
        <div className="segmented">
          <button className={mode === 'manual' ? 'active' : ''} onClick={() => { setMode('manual'); setModeDirty(true); }} disabled={busy}>수동</button>
          <button className={mode === 'auto' ? 'active' : ''} onClick={() => { setMode('auto'); setModeDirty(true); }} disabled={busy}>자동</button>
        </div>
        <label>
          <span>Pod</span>
          <input type="number" min={1} value={manualReplicas} onFocus={() => setPodDirty(true)} onChange={(event) => { setPodDirty(true); setManualReplicas(positiveNumber(Number(event.target.value), 1)); }} disabled={busy || mode === 'auto'} />
        </label>
        <button className="primary" onClick={applyPods} disabled={busy}>적용</button>
      </section>

      <section className="metrics">
        <Metric label="TPS" value={formatCount(smoothTps)} />
        <Metric label="대기" value={formatCount(smoothQueue)} tone={pressure > 0.72 ? 'bad' : pressure > 0.35 ? 'warn' : 'ok'} />
        <Metric label="Pod" value={`${ready} / ${desired}`} />
      </section>

      <section className="map-card">
        <div className="treemap">
          {boxes.map((box, index) => {
            const podPressure = box.queue ? Math.min(1, box.queue / 2400) : 0;
            const compact = box.w < 0.07 || box.h < 0.08 || box.w * box.h < 0.012;
            return (
              <article key={box.name} className={`tile ${compact ? 'compact' : ''}`} style={tileStyle(box, podPressure)} title={`${box.name} ${formatTileRate(box.rate)} ${formatTileQueue(box.queue)}`}>
                <b>{String(index + 1).padStart(2, '0')}</b>
                <strong>{formatTileRate(box.rate)}</strong>
                <span>{formatTileQueue(box.queue)}</span>
              </article>
            );
          })}
        </div>
      </section>
    </main>
  );
}

function Metric({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return <div className={`metric ${tone ?? ''}`}><span>{label}</span><strong>{value}</strong></div>;
}
