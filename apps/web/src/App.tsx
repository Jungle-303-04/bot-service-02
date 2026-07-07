import { useEffect, useMemo, useRef, useState } from 'react';

interface Pod {
  name: string;
  phase: string;
  ready: boolean;
  restarts: number;
  node: string;
  pod_ip?: string;
  age_seconds: number;
}

interface Cluster {
  available: boolean;
  namespace: string;
  deployment: string;
  desired_replicas: number;
  ready_replicas: number;
  available_replicas: number;
  updated_replicas: number;
  pods: Pod[];
  hpa: {
    available: boolean;
    min_replicas?: number;
    max_replicas?: number;
    target_cpu_utilization?: number | null;
    current_cpu_utilization?: number | null;
    current_replicas?: number;
    desired_replicas?: number;
  };
  error?: string;
}

interface Status {
  service: string;
  title: string;
  kind: string;
  version: string;
  flavor: string;
  generated_at: string;
  scenarios: Record<string, boolean>;
  metrics: { p95_latency_ms: number; request_samples: number; background_tasks: number };
  rows: Record<string, number>;
  cluster: Cluster;
}

interface Bot { id: number; name: string; status: string; current_load: number; updated_at: string }

const scenarios = [
  ['scale-surge/start', 'Scale Swarm', '2 -> 6 API pods'],
  ['load/start', 'CPU Burn', 'worker pressure'],
  ['db-bulk-insert/start', 'Queue Flood', 'job + event rows'],
  ['db-lock/start', 'DB Lock', 'transaction hold'],
  ['db-slow-query/start', 'Slow Query', 'query delay'],
  ['error-spike/start', 'Error Spike', '5xx storm'],
  ['crashloop/start', 'CrashLoop', 'pod restart'],
  ['recover', 'Recover', '6 -> 2 API pods'],
];

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, { ...init, headers: { 'content-type': 'application/json', ...(init?.headers ?? {}) } });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function shortPod(name: string) {
  return name.split('-').slice(-2).join('-');
}

function age(seconds: number) {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  return `${Math.floor(seconds / 3600)}h`;
}

export default function App() {
  const [status, setStatus] = useState<Status | null>(null);
  const [bots, setBots] = useState<Bot[]>([]);
  const [notice, setNotice] = useState('fleet telemetry standby');
  const [busy, setBusy] = useState('');
  const [storming, setStorming] = useState(false);
  const stormRef = useRef<number | null>(null);

  const refresh = async () => {
    const [s, b] = await Promise.all([
      api<Status>('/api/status'),
      api<{ bots: Bot[] }>('/api/bots').catch(() => ({ bots: [] })),
    ]);
    setStatus(s);
    setBots(b.bots);
  };

  const stopStorm = () => {
    if (stormRef.current) {
      window.clearInterval(stormRef.current);
      stormRef.current = null;
    }
    setStorming(false);
  };

  const startStorm = (durationMs: number) => {
    stopStorm();
    setStorming(true);
    const deadline = Date.now() + durationMs;
    stormRef.current = window.setInterval(() => {
      if (Date.now() > deadline) {
        stopStorm();
        return;
      }
      void Promise.allSettled([
        api('/api/work', { method: 'POST' }),
        api('/api/work', { method: 'POST' }),
        api('/api/jobs/process', { method: 'POST' }),
      ]);
    }, 340);
  };

  useEffect(() => {
    void refresh();
    const id = window.setInterval(() => { void refresh(); }, 1700);
    return () => {
      window.clearInterval(id);
      stopStorm();
    };
  }, []);

  const cluster = status?.cluster;
  const desired = cluster?.desired_replicas ?? 2;
  const ready = cluster?.ready_replicas ?? 0;
  const podCount = cluster?.pods.length ?? 0;
  const activeCount = status ? Object.values(status.scenarios).filter(Boolean).length : 0;
  const pressure = useMemo(() => {
    if (!status) return 0;
    const queuePressure = Math.min((status.rows.jobs ?? 0) / 30000, 0.3);
    const replicaPressure = Math.min(Math.max(desired - 2, 0) / 4, 0.34);
    const latencyPressure = Math.min(status.metrics.p95_latency_ms / 1300, 0.3);
    return Math.min(1, queuePressure + replicaPressure + latencyPressure + activeCount * 0.07 + (storming ? 0.14 : 0));
  }, [status, desired, activeCount, storming]);
  const slots = Math.max(6, desired, podCount);
  const readinessPct = desired ? Math.round((ready / desired) * 100) : 0;

  const runScenario = async (name: string, label: string) => {
    setBusy(label);
    try {
      await api(`/api/scenarios/${name}`, { method: 'POST' });
      if (name === 'scale-surge/start') startStorm(65000);
      if (name === 'load/start') startStorm(30000);
      if (name === 'recover') stopStorm();
      setNotice(`${label} dispatched`);
      await refresh();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : 'scenario failed');
    } finally {
      setBusy('');
    }
  };

  const enqueue = async () => {
    setBusy('Enqueue');
    try {
      await api('/api/jobs/enqueue', { method: 'POST' });
      setNotice('job queued');
      await refresh();
    } finally {
      setBusy('');
    }
  };

  const process = async () => {
    setBusy('Process');
    try {
      await api('/api/jobs/process', { method: 'POST' });
      setNotice('job processed');
      await refresh();
    } finally {
      setBusy('');
    }
  };

  return (
    <main className="shell" style={{ ['--pressure' as string]: pressure }}>
      <section className="mast">
        <div>
          <p className="label">bot-service-02 / fleet</p>
          <h1>Fleet Swarm</h1>
        </div>
        <div className="status-card">
          <span>{status?.version ?? 'loading'}</span>
          <strong>{ready}/{desired} ready</strong>
        </div>
      </section>

      <section className="console">
        <section className="swarm-board">
          <div className="swarm-head">
            <strong>Worker Heat Field</strong>
            <span>{storming ? 'surge traffic active' : `${activeCount} active scenario`}</span>
          </div>
          <div className="bot-matrix">
            {bots.map((bot, i) => (
              <article className="bot" key={bot.id} style={{ ['--i' as string]: i }}>
                <b>{bot.name}</b>
                <span>{bot.status}</span>
                <i style={{ height: `${Math.max(8, bot.current_load)}%` }} />
              </article>
            ))}
          </div>
        </section>

        <aside className="panel command">
          <div className="panel-title">
            <strong>Swarm Control</strong>
            <span>{notice}</span>
          </div>
          <div className="scale-buttons">
            <button onClick={() => runScenario('scale-surge/start', 'Scale Swarm')} disabled={!!busy}>
              <b>Scale Swarm</b>
              <span>2 to 6 API pods</span>
            </button>
            <button onClick={() => runScenario('recover', 'Recover')} disabled={!!busy}>
              <b>Recover</b>
              <span>6 to 2 API pods</span>
            </button>
          </div>
          <div className="actions">
            {scenarios.slice(1, -1).map(([path, label, desc]) => (
              <button key={path} onClick={() => runScenario(path, label)} disabled={!!busy} title={desc}>
                <span>{label}</span>
                <small>{desc}</small>
              </button>
            ))}
          </div>
          <div className="duo">
            <button onClick={enqueue} disabled={!!busy}>Queue Job</button>
            <button onClick={process} disabled={!!busy}>Process Job</button>
          </div>
        </aside>

        <section className="panel pods">
          <div className="panel-title">
            <strong>API Pod Columns</strong>
            <span>{cluster?.deployment ?? 'deployment'}</span>
          </div>
          <div className="replica-meter">
            <strong>{ready}/{desired}</strong>
            <span>ready replicas</span>
            <i style={{ width: `${Math.min(100, readinessPct)}%` }} />
          </div>
          <div className="pod-columns">
            {Array.from({ length: slots }, (_, i) => {
              const pod = cluster?.pods[i];
              return (
                <article key={pod?.name ?? i} className={`pod ${pod?.ready ? 'ready' : pod ? 'pending' : 'empty'}`}>
                  <b>{pod ? shortPod(pod.name) : 'standby'}</b>
                  <span>{pod ? `${pod.phase} · ${age(pod.age_seconds)}` : 'slot'}</span>
                  <em>{pod ? `restart ${pod.restarts}` : 'pending'}</em>
                </article>
              );
            })}
          </div>
          <div className="hpa">
            <span>cpu {cluster?.hpa.current_cpu_utilization ?? 0}% / target {cluster?.hpa.target_cpu_utilization ?? 60}%</span>
            <span>pods {podCount}</span>
          </div>
        </section>
      </section>

      <section className="metrics">
        <Metric label="Jobs" value={status?.rows.jobs ?? 0} />
        <Metric label="Bot events" value={status?.rows.bot_events ?? 0} />
        <Metric label="Telemetry" value={status?.rows.telemetry_samples ?? 0} />
        <Metric label="p95 ms" value={status?.metrics.p95_latency_ms ?? 0} />
      </section>
    </main>
  );
}

function Metric({ label, value }: { label: string; value: number }) {
  return <div className="metric"><span>{label}</span><strong>{Math.round(value).toLocaleString()}</strong></div>;
}
