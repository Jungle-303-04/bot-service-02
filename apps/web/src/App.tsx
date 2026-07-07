import { useEffect, useMemo, useState } from 'react';

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
}

interface Bot { id: number; name: string; status: string; current_load: number; updated_at: string }

const scenarios = [
  ['load/start', 'CPU Burn', 'worker 처리 루프에 CPU 압력을 만듭니다.'],
  ['db-bulk-insert/start', 'Queue Flood', 'job과 event row를 대량 생성합니다.'],
  ['db-lock/start', 'DB Lock', '긴 transaction으로 병목 증거를 만듭니다.'],
  ['db-slow-query/start', 'Slow Query', '모든 API에 지연 쿼리를 주입합니다.'],
  ['error-spike/start', 'Error Spike', '관제 API 오류율을 높입니다.'],
  ['crashloop/start', 'CrashLoop', '프로세스를 종료해 pod restart를 만듭니다.'],
  ['recover', 'Recover', '런타임 장애 플래그를 해제합니다.'],
];

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, { ...init, headers: { 'content-type': 'application/json', ...(init?.headers ?? {}) } });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export default function App() {
  const [status, setStatus] = useState<Status | null>(null);
  const [bots, setBots] = useState<Bot[]>([]);
  const [notice, setNotice] = useState('fleet telemetry standby');
  const [busy, setBusy] = useState('');

  const refresh = async () => {
    const [s, b] = await Promise.all([
      api<Status>('/api/status'),
      api<{ bots: Bot[] }>('/api/bots').catch(() => ({ bots: [] })),
    ]);
    setStatus(s);
    setBots(b.bots);
  };

  useEffect(() => {
    void refresh();
    const id = window.setInterval(() => { void refresh(); }, 2200);
    return () => window.clearInterval(id);
  }, []);

  const pressure = useMemo(() => {
    if (!status) return 0;
    const active = Object.values(status.scenarios).filter(Boolean).length;
    const jobPressure = Math.min((status.rows.jobs ?? 0) / 20000, 0.45);
    return Math.min(1, active * 0.18 + jobPressure + Math.min(status.metrics.p95_latency_ms / 1600, 0.35));
  }, [status]);

  const runScenario = async (name: string, label: string) => {
    setBusy(label);
    try {
      await api(`/api/scenarios/${name}`, { method: 'POST' });
      setNotice(`${label} scenario dispatched`);
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
      setNotice('job queued into PostgreSQL');
      await refresh();
    } finally {
      setBusy('');
    }
  };

  const process = async () => {
    setBusy('Process');
    try {
      await api('/api/jobs/process', { method: 'POST' });
      setNotice('one queued job processed');
      await refresh();
    } finally {
      setBusy('');
    }
  };

  return (
    <main className="shell">
      <section className="hero">
        <div>
          <p className="label">bot-service-02</p>
          <h1>Fleet Swarm</h1>
          <p className="copy">bot worker, job queue, DB 저장량, pod restart를 열감처럼 보여주는 장애 관찰용 풀스택 서비스입니다.</p>
        </div>
        <div className="orbital" aria-hidden>
          {Array.from({ length: 14 }, (_, i) => <span key={i} style={{ ['--i' as string]: i }} />)}
        </div>
      </section>

      <section className="topology">
        <div className="swarm" style={{ ['--pressure' as string]: pressure }}>
          {bots.map((bot, i) => (
            <div className="bot" key={bot.id} style={{ animationDelay: `${i * 45}ms` }}>
              <b>{bot.name}</b>
              <span>{bot.status}</span>
            </div>
          ))}
        </div>
        <aside className="panel command">
          <div className="panel-title">
            <strong>Swarm Control</strong>
            <span>{notice}</span>
          </div>
          <div className="actions">
            {scenarios.map(([path, label, desc]) => (
              <button key={path} onClick={() => runScenario(path, label)} disabled={!!busy} title={desc}>
                <span>{label}</span>
                <small>{desc}</small>
              </button>
            ))}
          </div>
          <div className="duo">
            <button onClick={enqueue} disabled={!!busy}>Job 추가</button>
            <button onClick={process} disabled={!!busy}>Job 처리</button>
          </div>
        </aside>
      </section>

      <section className="metrics">
        <Metric label="Jobs" value={status?.rows.jobs ?? 0} />
        <Metric label="Bot events" value={status?.rows.bot_events ?? 0} />
        <Metric label="Telemetry" value={status?.rows.telemetry_samples ?? 0} />
        <Metric label="p95 ms" value={status?.metrics.p95_latency_ms ?? 0} />
      </section>

      <section className="panel version">
        <div>
          <span>release</span>
          <strong>{status?.version ?? 'loading'}</strong>
        </div>
        <div>
          <span>flavor</span>
          <strong>{status?.flavor ?? 'stable'}</strong>
        </div>
        <div>
          <span>active scenarios</span>
          <strong>{status ? Object.values(status.scenarios).filter(Boolean).length : 0}</strong>
        </div>
      </section>
    </main>
  );
}

function Metric({ label, value }: { label: string; value: number }) {
  return <div className="metric"><span>{label}</span><strong>{Math.round(value).toLocaleString()}</strong></div>;
}

