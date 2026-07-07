import { useEffect, useMemo, useState, type CSSProperties } from 'react';

interface Pod {
  name: string;
  ready: boolean;
}

interface Cluster {
  desired_replicas: number;
  ready_replicas: number;
  pods: Pod[];
  hpa?: {
    min_replicas?: number;
    max_replicas?: number;
  };
}

interface Traffic {
  running: boolean;
  mode: 'manual' | 'auto';
  target_tps: number;
  manual_replicas: number;
  ready_replicas: number;
  desired_replicas: number;
  queue_depth: number;
  received_total: number;
  processed_total: number;
  failed_total: number;
  pressure: number;
  capacity_per_second: number;
}

interface Status {
  version: string;
  metrics: { p95_latency_ms: number };
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

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value));
}

function formatCount(value: number) {
  return Math.round(value || 0).toLocaleString('ko-KR');
}

function shortPodName(name: string) {
  return name.split('-').slice(-2).join('-');
}

function splitBoxes(items: Array<{ name: string; ready: boolean }>, x = 0, y = 0, w = 1, h = 1): PodBox[] {
  if (items.length === 0) return [];
  if (items.length === 1) return [{ ...items[0], x, y, w, h }];
  const firstCount = Math.ceil(items.length / 2);
  const first = items.slice(0, firstCount);
  const second = items.slice(firstCount);
  const ratio = first.length / items.length;
  if (w >= h) {
    const w1 = w * ratio;
    return [...splitBoxes(first, x, y, w1, h), ...splitBoxes(second, x + w1, y, w - w1, h)];
  }
  const h1 = h * ratio;
  return [...splitBoxes(first, x, y, w, h1), ...splitBoxes(second, x, y + h1, w, h - h1)];
}

function heatClass(pressure: number, ready: boolean) {
  if (!ready) return 'idle';
  if (pressure >= 0.72) return 'bad';
  if (pressure >= 0.35) return 'warn';
  return 'good';
}

function tileStyle(box: PodBox, pressure: number) {
  return {
    left: `${box.x * 100}%`,
    top: `${box.y * 100}%`,
    width: `${box.w * 100}%`,
    height: `${box.h * 100}%`,
    '--heat': `${Math.round(clamp(pressure, 0, 1) * 100)}%`,
  } as CSSProperties;
}

export default function App() {
  const [status, setStatus] = useState<Status | null>(null);
  const [targetTps, setTargetTps] = useState(1000);
  const [manualReplicas, setManualReplicas] = useState(1);
  const [mode, setMode] = useState<'manual' | 'auto'>('manual');
  const [busy, setBusy] = useState('');

  const refresh = async () => {
    const next = await api<Status>('/api/status');
    setStatus(next);
    if (next.traffic.running && !busy) {
      setMode(next.traffic.mode ?? 'manual');
      setManualReplicas(next.traffic.manual_replicas || 1);
      if (next.traffic.target_tps > 0) setTargetTps(next.traffic.target_tps);
    }
  };

  useEffect(() => {
    void refresh();
    const id = window.setInterval(() => { void refresh().catch(() => undefined); }, 1000);
    return () => window.clearInterval(id);
  }, []);

  const traffic = status?.traffic;
  const cluster = status?.cluster;
  const running = Boolean(traffic?.running);
  const ready = cluster?.ready_replicas ?? traffic?.ready_replicas ?? 0;
  const desired = cluster?.desired_replicas ?? traffic?.desired_replicas ?? 0;
  const pressure = traffic?.pressure ?? 0;
  const p95 = status?.metrics.p95_latency_ms ?? 0;

  const pods = useMemo(() => {
    const actual = cluster?.pods ?? [];
    const count = Math.max(cluster?.desired_replicas ?? 1, actual.length, 1);
    return Array.from({ length: count }, (_, index) => {
      const pod = actual[index];
      return { name: pod ? shortPodName(pod.name) : `pending-${index + 1}`, ready: Boolean(pod?.ready) };
    });
  }, [cluster]);

  const boxes = useMemo(() => splitBoxes(pods), [pods]);
  const perPodProcessed = (traffic?.processed_total ?? 0) / Math.max(ready || desired || 1, 1);
  const perPodQueue = (traffic?.queue_depth ?? 0) / Math.max(ready || desired || 1, 1);

  const start = async () => {
    setBusy('start');
    try {
      await api('/api/traffic/receiver/start', {
        method: 'POST',
        body: JSON.stringify({ target_tps: targetTps, mode, manual_replicas: manualReplicas }),
      });
      await refresh();
    } finally {
      setBusy('');
    }
  };

  const stop = async () => {
    setBusy('stop');
    try {
      await api('/api/traffic/receiver/stop', { method: 'POST' });
      await refresh();
    } finally {
      setBusy('');
    }
  };

  return (
    <main className="shell">
      <section className="hero">
        <div>
          <p>bot-service-02 · 수신</p>
          <h1>수신 플릿 맵</h1>
        </div>
        <strong className={`badge ${running ? 'warn' : 'ok'}`}>{running ? '실행 중' : '대기'}</strong>
      </section>

      <section className="control-panel" aria-label="수신 제어">
        <div className="segmented" aria-label="모드">
          <button className={mode === 'manual' ? 'active' : ''} onClick={() => setMode('manual')} disabled={running || !!busy}>수동</button>
          <button className={mode === 'auto' ? 'active' : ''} onClick={() => setMode('auto')} disabled={running || !!busy}>자동</button>
        </div>
        <label>
          <span>목표 TPS</span>
          <input type="number" min={1} max={10000} value={targetTps} onChange={(event) => setTargetTps(clamp(Number(event.target.value), 1, 10000))} disabled={running} />
        </label>
        <label>
          <span>기준 Pod</span>
          <input type="number" min={1} max={10} value={manualReplicas} onChange={(event) => setManualReplicas(clamp(Number(event.target.value), 1, 10))} disabled={running || mode === 'auto'} />
        </label>
        <button className="primary" onClick={start} disabled={running || !!busy}>실행</button>
        <button onClick={stop} disabled={!running || !!busy}>중지</button>
      </section>

      <section className="metrics">
        <Metric label="받은 수" value={formatCount(traffic?.received_total ?? 0)} />
        <Metric label="처리 수" value={formatCount(traffic?.processed_total ?? 0)} />
        <Metric label="수신 큐" value={formatCount(traffic?.queue_depth ?? 0)} tone={pressure > 0.72 ? 'bad' : pressure > 0.35 ? 'warn' : 'ok'} />
        <Metric label="수신 Pod" value={`${ready} / ${desired}`} />
      </section>

      <section className="map-card">
        <div className="map-title">
          <div>
            <strong>Pod 맵</strong>
            <span>p95 {Math.round(p95)}ms · v{status?.version?.replace(/^v/, '') ?? '-'}</span>
          </div>
          <div className="legend">
            <span><i className="good" />정상</span>
            <span><i className="warn" />대기</span>
            <span><i className="bad" />병목</span>
          </div>
        </div>
        <div className="treemap" aria-label="수신 파드 히트맵">
          {boxes.map((box, index) => (
            <article key={`${box.name}-${index}`} className={`tile ${heatClass(pressure, box.ready)}`} style={tileStyle(box, pressure)}>
              <b>{box.name}</b>
              <strong>{box.ready ? '정상' : '대기'}</strong>
              <span>처리 {formatCount(perPodProcessed)}</span>
              <span>큐 {formatCount(perPodQueue)}</span>
            </article>
          ))}
        </div>
      </section>
    </main>
  );
}

function Metric({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return <div className={`metric ${tone ?? ''}`}><span>{label}</span><strong>{value}</strong></div>;
}
