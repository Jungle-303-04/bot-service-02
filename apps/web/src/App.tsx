import { useEffect, useMemo, useState, type CSSProperties } from 'react';

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
  queue_depth: number;
  processed_per_second: number;
  pressure: number;
  pod_stats?: Record<string, { processed_per_second?: number; queue_depth?: number }>;
}

interface Status {
  cluster: Cluster;
  traffic: Traffic;
}

interface PodBox {
  name: string;
  ready: boolean;
  x: number;
  y: number;
  w: number;
  h: number;
}

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, { ...init, headers: { 'content-type': 'application/json', ...(init?.headers ?? {}) } });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function positiveNumber(value: number, fallback = 1) {
  return Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}

function formatCount(value: number) {
  return Math.round(value || 0).toLocaleString('ko-KR');
}

function splitBoxes(items: Array<{ name: string; ready: boolean }>, x = 0, y = 0, w = 1, h = 1): PodBox[] {
  if (items.length === 0) return [];
  if (items.length === 1) return [{ ...items[0], x, y, w, h }];
  const firstCount = Math.ceil(items.length / 2);
  const ratio = firstCount / items.length;
  if (w >= h) {
    const w1 = w * ratio;
    return [
      ...splitBoxes(items.slice(0, firstCount), x, y, w1, h),
      ...splitBoxes(items.slice(firstCount), x + w1, y, w - w1, h),
    ];
  }
  const h1 = h * ratio;
  return [
    ...splitBoxes(items.slice(0, firstCount), x, y, w, h1),
    ...splitBoxes(items.slice(firstCount), x, y + h1, w, h - h1),
  ];
}

function heatClass(pressure: number, ready: boolean) {
  if (!ready) return 'idle';
  if (pressure >= 0.72) return 'bad';
  if (pressure >= 0.35) return 'warn';
  return 'good';
}

function tileStyle(box: PodBox) {
  return {
    left: `${box.x * 100}%`,
    top: `${box.y * 100}%`,
    width: `${box.w * 100}%`,
    height: `${box.h * 100}%`,
  } as CSSProperties;
}

function useSmoothNumber(target: number) {
  const [value, setValue] = useState(target);

  useEffect(() => {
    let frame = 0;
    const tick = () => {
      setValue((current) => {
        const next = current + (target - current) * 0.22;
        return Math.abs(next - target) < 0.5 ? target : next;
      });
      frame = window.requestAnimationFrame(tick);
    };
    frame = window.requestAnimationFrame(tick);
    return () => window.cancelAnimationFrame(frame);
  }, [target]);

  return value;
}

export default function App() {
  const [status, setStatus] = useState<Status | null>(null);
  const [manualReplicas, setManualReplicas] = useState(1);
  const [busy, setBusy] = useState(false);

  const refresh = async () => {
    const next = await api<Status>('/api/status');
    setStatus(next);
    if (!busy) setManualReplicas(next.cluster.desired_replicas || 1);
  };

  useEffect(() => {
    void refresh();
    const id = window.setInterval(() => { void refresh().catch(() => undefined); }, 500);
    return () => window.clearInterval(id);
  }, []);

  const traffic = status?.traffic;
  const cluster = status?.cluster;
  const ready = cluster?.ready_replicas ?? 0;
  const desired = cluster?.desired_replicas ?? 1;
  const pressure = traffic?.pressure ?? 0;
  const smoothTps = useSmoothNumber(traffic?.processed_per_second ?? 0);
  const smoothQueue = useSmoothNumber(traffic?.queue_depth ?? 0);

  const pods = useMemo(() => {
    const actual = cluster?.pods ?? [];
    const count = Math.max(cluster?.desired_replicas ?? 1, actual.length, 1);
    return Array.from({ length: count }, (_, index) => ({
      name: actual[index]?.name ?? `pending-${index + 1}`,
      ready: Boolean(actual[index]?.ready),
    }));
  }, [cluster]);

  const boxes = useMemo(() => splitBoxes(pods), [pods]);

  const applyPods = async () => {
    setBusy(true);
    try {
      await api('/api/traffic/receiver/scale', {
        method: 'POST',
        body: JSON.stringify({ manual_replicas: manualReplicas }),
      });
      await refresh();
    } finally {
      setBusy(false);
    }
  };

  return (
    <main className="shell">
      <section className="hero">
        <h1>수신</h1>
      </section>

      <section className="control-panel receiver">
        <label>
          <span>Pod</span>
          <input type="number" min={1} value={manualReplicas} onChange={(event) => setManualReplicas(positiveNumber(Number(event.target.value), 1))} />
        </label>
        <button className="primary" onClick={applyPods} disabled={busy}>적용</button>
      </section>

      <section className="metrics">
        <Metric label="TPS" value={`${formatCount(smoothTps)}/s`} />
        <Metric label="큐" value={formatCount(smoothQueue)} tone={pressure > 0.72 ? 'bad' : pressure > 0.35 ? 'warn' : 'ok'} />
        <Metric label="Pod" value={`${ready} / ${desired}`} />
      </section>

      <section className="map-card">
        <div className="treemap">
          {boxes.map((box, index) => {
            const stat = traffic?.pod_stats?.[box.name];
            const podPressure = stat?.queue_depth ? Math.min(1, stat.queue_depth / 2400) : pressure;
            return (
              <article key={box.name} className={`tile ${heatClass(podPressure, box.ready)}`} style={tileStyle(box)}>
                <b>{String(index + 1).padStart(2, '0')}</b>
                <strong>{formatCount(stat?.processed_per_second ?? 0)}/s</strong>
                <span>Q {formatCount(stat?.queue_depth ?? 0)}</span>
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
