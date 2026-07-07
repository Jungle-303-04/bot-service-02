import { useEffect, useMemo, useRef, useState } from 'react';

interface Cluster {
  desired_replicas: number;
  ready_replicas: number;
  template_version: string;
  template_flavor: string;
  rollout_complete: boolean;
  hpa?: {
    min_replicas?: number;
    max_replicas?: number;
    current_cpu_utilization?: number | null;
  };
}

interface Status {
  version: string;
  flavor: string;
  scenarios: Record<string, boolean>;
  metrics: { p95_latency_ms: number };
  rows: Record<string, number>;
  cluster: Cluster;
}

interface Tile {
  x: number;
  y: number;
  w: number;
  h: number;
  heat: number;
  index: number;
}

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, { ...init, headers: { 'content-type': 'application/json', ...(init?.headers ?? {}) } });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value));
}

function splitTreemap(count: number, x = 0, y = 0, w = 100, h = 100, seed = 29, start = 0): Tile[] {
  if (count <= 0) return [];
  if (count === 1) {
    const heat = ((start * 41 + seed * 17) % 100) / 100;
    return [{ x, y, w, h, heat, index: start }];
  }

  const bias = 0.41 + (((seed * 89 + count * 11) % 22) / 100);
  const first = clamp(Math.round(count * bias), 1, count - 1);
  const second = count - first;

  if (w >= h) {
    const w1 = (w * first) / count;
    return [
      ...splitTreemap(first, x, y, w1, h, seed + 5, start),
      ...splitTreemap(second, x + w1, y, w - w1, h, seed + 9, start + first),
    ];
  }

  const h1 = (h * first) / count;
  return [
    ...splitTreemap(first, x, y, w, h1, seed + 7, start),
    ...splitTreemap(second, x, y + h1, w, h - h1, seed + 13, start + first),
  ];
}

function formatCount(value: number) {
  return value.toLocaleString('ko-KR');
}

function loadLabel(count: number) {
  if (count <= 1) return '단일 작업 블록';
  return `${formatCount(count)}개 실제 작업 셀`;
}

export default function App() {
  const [status, setStatus] = useState<Status | null>(null);
  const [targetCells, setTargetCells] = useState(256);
  const [requestRate, setRequestRate] = useState(40);
  const [running, setRunning] = useState(false);
  const [notice, setNotice] = useState('대기 중');
  const [busy, setBusy] = useState('');
  const [baselineRows, setBaselineRows] = useState<number | null>(null);
  const [confirmedWork, setConfirmedWork] = useState(0);
  const workTimer = useRef<number | null>(null);

  const refresh = async () => {
    try {
      setStatus(await api<Status>('/api/status'));
    } catch (error) {
      setNotice(error instanceof Error ? error.message : '상태 조회 실패');
    }
  };

  useEffect(() => {
    void refresh();
    const id = window.setInterval(() => { void refresh(); }, 1400);
    return () => window.clearInterval(id);
  }, []);

  const activeScenarios = status ? Object.values(status.scenarios).filter(Boolean).length : 0;
  const ready = status?.cluster.ready_replicas ?? 0;
  const desired = status?.cluster.desired_replicas ?? 0;
  const hpaMin = status?.cluster.hpa?.min_replicas ?? 0;
  const hpaMax = status?.cluster.hpa?.max_replicas ?? 0;
  const externalLoad = activeScenarios > 0 || desired > 2;
  const storedRows = status?.rows.jobs ?? 0;
  const actualDelta = baselineRows === null ? (externalLoad ? storedRows : 0) : Math.max(0, storedRows - baselineRows);
  const actualWork = Math.max(actualDelta, confirmedWork);
  const visibleCells = running || externalLoad ? clamp(Math.max(actualWork, 1), 1, targetCells) : 1;
  const p95 = status?.metrics.p95_latency_ms ?? 0;
  const podPressure = desired > 2 ? Math.min((desired - 2) / 6, 0.24) : 0;
  const pressure = Math.min(1, (visibleCells / 1000) * 0.56 + (requestRate / 120) * 0.2 + activeScenarios * 0.1 + podPressure + Math.min(p95 / 1300, 0.2));
  const rolloutState = !status?.cluster.rollout_complete ? '배포 중' : status.cluster.template_flavor !== 'stable' ? '장애 버전' : running || externalLoad ? '부하 중' : '정상';

  useEffect(() => {
    if (workTimer.current) window.clearInterval(workTimer.current);
    if (!running) return;

    workTimer.current = window.setInterval(() => {
      const batch = clamp(Math.ceil(requestRate / 2), 1, 80);
      void Promise.allSettled(Array.from({ length: batch }, () => api('/api/work', { method: 'POST' }))).then((results) => {
        const successes = results.filter((result) => result.status === 'fulfilled').length;
        setConfirmedWork((current) => clamp(current + successes, 0, targetCells));
      });
    }, 500);

    return () => {
      if (workTimer.current) window.clearInterval(workTimer.current);
    };
  }, [running, requestRate, targetCells]);

  const setLoad = (next: number) => setTargetCells(clamp(Math.round(next || 1), 1, 1000));
  const setRate = (next: number) => setRequestRate(clamp(Math.round(next || 1), 1, 120));

  const startLoad = async () => {
    setBusy('부하 시작');
    setBaselineRows(status?.rows.jobs ?? 0);
    setConfirmedWork(0);
    setRunning(true);
    setNotice('실제 작업을 저장하며 분할 중');

    try {
      await api('/api/scenarios/scale-surge/start', { method: 'POST' });
      await refresh();
    } catch (error) {
      setRunning(false);
      setNotice(error instanceof Error ? error.message : '부하 시작 실패');
    } finally {
      setBusy('');
    }
  };

  const stopLoad = async () => {
    setBusy('복구');
    setRunning(false);
    setBaselineRows(null);
    setConfirmedWork(0);
    setNotice('복구 중');

    try {
      await api('/api/scenarios/recover', { method: 'POST' });
      setNotice('복구 완료');
      await refresh();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : '복구 실패');
    } finally {
      setBusy('');
    }
  };

  const release = async (action: 'deploy' | 'faulty' | 'rollback', label: string) => {
    setBusy(label);
    setNotice(label);

    try {
      await api(`/api/releases/${action}`, { method: 'POST' });
      if (action === 'rollback') {
        setRunning(false);
        setBaselineRows(null);
        setConfirmedWork(0);
      }
      await refresh();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : `${label} 실패`);
    } finally {
      setBusy('');
    }
  };

  const tiles = useMemo(() => splitTreemap(visibleCells).map((tile) => ({
    ...tile,
    heat: Math.min(1, pressure + tile.heat * 0.24),
  })), [visibleCells, pressure]);

  return (
    <main className="shell">
      <section className="hero">
        <div>
          <p>bot-service-02</p>
          <h1>봇 작업 부하 맵</h1>
        </div>
        <strong className={`badge ${rolloutState === '정상' ? 'ok' : rolloutState === '장애 버전' ? 'bad' : 'warn'}`}>{rolloutState}</strong>
      </section>

      <section className="control-panel" aria-label="부하 제어">
        <label>
          <span>목표 셀</span>
          <input type="number" min={1} max={1000} value={targetCells} onChange={(event) => setLoad(Number(event.target.value))} />
        </label>
        <label>
          <span>요청/초</span>
          <input type="number" min={1} max={120} value={requestRate} onChange={(event) => setRate(Number(event.target.value))} />
        </label>
        <div className="step-buttons">
          <button onClick={() => setLoad(targetCells - 100)} disabled={!!busy}>-100</button>
          <button onClick={() => setLoad(targetCells + 100)} disabled={!!busy}>+100</button>
          <button onClick={() => setLoad(1000)} disabled={!!busy}>1000</button>
        </div>
        <div className="run-buttons">
          <button className="primary" onClick={startLoad} disabled={!!busy || running}>부하 시작</button>
          <button onClick={stopLoad} disabled={!!busy}>중지/복구</button>
        </div>
      </section>

      <section className="metrics">
        <Metric label="실제 작업 셀" value={`${formatCount(visibleCells)} / ${formatCount(targetCells)}`} />
        <Metric label="파드 준비" value={`${ready} / ${desired}`} />
        <Metric label="HPA 범위" value={`${hpaMin} / ${hpaMax}`} />
        <Metric label="p95 지연" value={`${Math.round(p95)}ms`} />
      </section>

      <section className="heat-card" style={{ ['--heat' as string]: pressure }}>
        <div className="heat-title">
          <div>
            <strong>부하 히트맵</strong>
            <span>{loadLabel(visibleCells)}</span>
          </div>
          <span>{notice} · DB {formatCount(storedRows)}건 · 파드 {ready}/{desired}</span>
        </div>
        <div className="heat-map" aria-label="실제 작업 부하 히트맵">
          {tiles.map((tile) => (
            <i
              key={tile.index}
              style={{
                ['--cell' as string]: tile.heat,
                left: `${tile.x}%`,
                top: `${tile.y}%`,
                width: `${tile.w}%`,
                height: `${tile.h}%`,
              }}
            >
              {tile.w * tile.h > 220 && <span>{tile.index + 1}</span>}
            </i>
          ))}
        </div>
      </section>

      <section className="footer-panel">
        <div>
          <span>운영 중 버전</span>
          <strong>{status?.version ?? '로딩 중'}</strong>
        </div>
        <div>
          <span>목표 버전</span>
          <strong>{status?.cluster.template_version ?? '로딩 중'}</strong>
        </div>
        <div className="release-buttons">
          <button onClick={() => release('deploy', '새 버전 배포')} disabled={!!busy}>새 버전</button>
          <button onClick={() => release('faulty', '장애 버전 배포')} disabled={!!busy}>장애 버전</button>
          <button onClick={() => release('rollback', '롤백')} disabled={!!busy}>롤백</button>
        </div>
      </section>
    </main>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return <div className="metric"><span>{label}</span><strong>{value}</strong></div>;
}
