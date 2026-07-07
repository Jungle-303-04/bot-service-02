import { useEffect, useMemo, useRef, useState } from 'react';

interface Pod {
  name: string;
  phase: string;
  ready: boolean;
  restarts: number;
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
  generation: number;
  observed_generation: number;
  template_version: string;
  template_flavor: string;
  rollout_complete: boolean;
  pods: Pod[];
  hpa: {
    min_replicas?: number;
    max_replicas?: number;
    target_cpu_utilization?: number | null;
    current_cpu_utilization?: number | null;
  };
  error?: string;
}

interface Status {
  service: string;
  version: string;
  flavor: string;
  scenarios: Record<string, boolean>;
  metrics: { p95_latency_ms: number; background_tasks: number };
  rows: Record<string, number>;
  cluster: Cluster;
}

interface Bot { id: number; name: string; status: string; current_load: number; updated_at: string }

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, { ...init, headers: { 'content-type': 'application/json', ...(init?.headers ?? {}) } });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function shortName(name: string) {
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
  const [notice, setNotice] = useState('운영 상태 확인 중');
  const [busy, setBusy] = useState('');
  const [loadCells, setLoadCells] = useState(96);
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
    if (stormRef.current) window.clearInterval(stormRef.current);
    stormRef.current = null;
  };

  const startStorm = (durationMs: number) => {
    stopStorm();
    const endAt = Date.now() + durationMs;
    stormRef.current = window.setInterval(() => {
      if (Date.now() > endAt) {
        stopStorm();
        return;
      }
      void Promise.allSettled([
        api('/api/work', { method: 'POST' }),
        api('/api/work', { method: 'POST' }),
        api('/api/jobs/process', { method: 'POST' }),
      ]);
    }, 430);
  };

  useEffect(() => {
    void refresh();
    const id = window.setInterval(() => { void refresh(); }, 1600);
    return () => {
      window.clearInterval(id);
      stopStorm();
    };
  }, []);

  const cluster = status?.cluster;
  const desired = cluster?.desired_replicas ?? 0;
  const ready = cluster?.ready_replicas ?? 0;
  const updated = cluster?.updated_replicas ?? 0;
  const rolloutPct = desired ? Math.round((Math.min(updated, desired) / desired) * 100) : 0;
  const activeIncidents = status ? Object.values(status.scenarios).filter(Boolean).length : 0;
  const releaseTone = !cluster?.rollout_complete ? 'rolling' : cluster.template_flavor !== 'stable' ? 'fault' : 'stable';
  const rolloutLabel = !cluster?.rollout_complete ? '배포 중' : cluster.template_flavor !== 'stable' ? '장애 버전' : '정상';
  const busyBots = useMemo(() => bots.filter(bot => bot.status === 'busy').length, [bots]);
  const loadActive = Boolean(status?.scenarios.scale_surge || status?.scenarios.load || status?.scenarios.db_bulk_insert || status?.scenarios.error_spike);
  const loadPressure = Math.min(1, activeIncidents * 0.18 + Math.max(desired - 2, 0) * 0.1 + (busyBots / Math.max(1, bots.length)) * 0.28 + (cluster?.template_flavor !== 'stable' ? 0.32 : 0));
  const loadTarget = loadActive ? 1000 : desired > 2 ? 360 : 96;

  useEffect(() => {
    const id = window.setInterval(() => {
      setLoadCells((current) => {
        if (current === loadTarget) return current;
        const step = current < loadTarget ? Math.max(12, Math.ceil((loadTarget - current) / 6)) : -Math.max(16, Math.ceil((current - loadTarget) / 5));
        const next = current + step;
        return step > 0 ? Math.min(next, loadTarget) : Math.max(next, loadTarget);
      });
    }, 160);
    return () => window.clearInterval(id);
  }, [loadTarget]);

  const runRelease = async (action: 'deploy' | 'faulty' | 'rollback', label: string) => {
    setBusy(label);
    try {
      const result = await api<{ version: string; flavor: string; status: string }>(`/api/releases/${action}`, { method: 'POST' });
      if (action === 'rollback') stopStorm();
      setNotice(`${label}: ${result.version}`);
      await refresh();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : '릴리스 작업 실패');
    } finally {
      setBusy('');
    }
  };

  const runIncident = async (path: string, label: string) => {
    setBusy(label);
    try {
      await api(`/api/scenarios/${path}`, { method: 'POST' });
      if (path === 'scale-surge/start' || path === 'load/start') startStorm(60000);
      if (path === 'recover') stopStorm();
      setNotice(`${label} 실행됨`);
      await refresh();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : '운영 작업 실패');
    } finally {
      setBusy('');
    }
  };

  const enqueue = async () => {
    setBusy('작업 추가');
    try {
      await api('/api/jobs/enqueue', { method: 'POST' });
      setNotice('작업 추가 완료');
      await refresh();
    } finally {
      setBusy('');
    }
  };

  return (
    <main className="shell">
      <section className="topbar">
        <div>
          <p className="eyebrow">bot-service-02</p>
          <h1>봇 작업 운영</h1>
        </div>
        <span className={`state ${releaseTone}`}>{rolloutLabel}</span>
      </section>

      <section className="summary">
        <Metric label="운영 중 버전" value={status?.version ?? '로딩 중'} tone={status?.flavor !== 'stable' ? 'fault' : undefined} />
        <Metric label="목표 버전" value={cluster?.template_version ?? '로딩 중'} tone={cluster?.template_flavor !== 'stable' ? 'fault' : undefined} />
        <Metric label="파드 준비" value={`${ready}/${desired}`} />
        <Metric label="바쁜 봇" value={`${busyBots}/${bots.length || 18}`} />
      </section>

      <section className="layout">
        <aside className="panel controls">
          <div className="panel-title">
            <strong>릴리스 제어</strong>
            <span>{notice}</span>
          </div>
          <div className="button-stack primary-actions">
            <button onClick={() => runRelease('deploy', '새 버전 배포')} disabled={!!busy}>새 버전 배포</button>
            <button onClick={() => runRelease('faulty', '장애 버전 배포')} disabled={!!busy}>장애 버전 배포</button>
            <button onClick={() => runRelease('rollback', '롤백')} disabled={!!busy}>롤백</button>
          </div>

          <div className="panel-title compact">
            <strong>장애 제어</strong>
            <span>{activeIncidents} active</span>
          </div>
          <div className="button-grid">
            <button onClick={() => runIncident('scale-surge/start', '부하 증가')} disabled={!!busy}>부하 증가</button>
            <button onClick={() => runIncident('db-bulk-insert/start', '대량 저장')} disabled={!!busy}>대량 저장</button>
            <button onClick={() => runIncident('error-spike/start', '오류 증가')} disabled={!!busy}>오류 증가</button>
            <button onClick={() => runIncident('recover', '복구')} disabled={!!busy}>복구</button>
          </div>
          <button className="order-button" onClick={enqueue} disabled={!!busy}>작업 추가</button>
        </aside>

        <section className="panel rollout">
          <div className="panel-title">
            <strong>롤아웃 상태</strong>
            <span>세대 {cluster?.observed_generation ?? 0}/{cluster?.generation ?? 0}</span>
          </div>
          <div className="progress-row">
            <div>
              <span>갱신됨</span>
              <strong>{updated}/{desired}</strong>
            </div>
            <div>
              <span>사용 가능</span>
              <strong>{cluster?.available_replicas ?? 0}/{desired}</strong>
            </div>
            <div>
              <span>HPA</span>
              <strong>{cluster?.hpa.min_replicas ?? 0}/{cluster?.hpa.max_replicas ?? 0}</strong>
            </div>
          </div>
          <div className="progress">
            <i style={{ width: `${rolloutPct}%` }} />
          </div>
          <div className="pod-list">
            {(cluster?.pods ?? []).map((pod) => (
              <article className={`pod ${pod.ready ? 'ready' : 'pending'}`} key={pod.name}>
                <b>{shortName(pod.name)}</b>
                <span>{pod.phase} · {age(pod.age_seconds)}</span>
                <em>restart {pod.restarts}</em>
              </article>
            ))}
          </div>
        </section>
      </section>

      <section className="panel load-panel" style={{ ['--load' as string]: loadPressure }}>
        <div className="panel-title">
          <strong>부하 확산</strong>
          <span>{loadCells.toLocaleString()} 작업 셀 · 실제 파드 {ready}/{desired}</span>
        </div>
        <div className="cell-field">
          {Array.from({ length: loadCells }, (_, index) => <i key={index} />)}
        </div>
      </section>

      <section className="data-grid">
        <div className="panel">
          <div className="panel-title"><strong>작업 큐</strong><span>{status?.rows.jobs?.toLocaleString() ?? 0}</span></div>
          <div className="bot-list">
            {bots.slice(0, 18).map(bot => (
              <span className={bot.status === 'busy' ? 'busy' : ''} key={bot.id}>{bot.name}</span>
            ))}
          </div>
        </div>
        <div className="panel">
          <div className="panel-title"><strong>이벤트 저장</strong><span>{status?.rows.bot_events?.toLocaleString() ?? 0}</span></div>
          <div className="event-copy">PostgreSQL에 작업 이벤트와 처리 상태가 누적됩니다.</div>
        </div>
      </section>
    </main>
  );
}

function Metric({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return <div className={`metric ${tone ?? ''}`}><span>{label}</span><strong>{value}</strong></div>;
}
