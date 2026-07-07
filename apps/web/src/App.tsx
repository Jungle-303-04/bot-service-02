import { useEffect, useMemo, useRef, useState } from 'react';

interface Pod {
  name: string;
  ready: boolean;
  phase?: string;
}

interface Cluster {
  desired_replicas: number;
  ready_replicas: number;
  template_version: string;
  template_flavor: string;
  rollout_complete: boolean;
  pods: Pod[];
  hpa?: {
    min_replicas?: number;
    max_replicas?: number;
  };
}

interface Status {
  version: string;
  scenarios: Record<string, boolean>;
  metrics: { p95_latency_ms: number };
  rows: Record<string, number>;
  cluster: Cluster;
}

interface BotStat {
  id: number;
  success: number;
  failure: number;
  weight: number;
}

interface PodTile {
  id: string;
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
  return value.toLocaleString('ko-KR');
}

function sleep(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

async function postWithRetry(path: string, attempts = 8) {
  let lastError: unknown;
  for (let i = 0; i < attempts; i += 1) {
    try {
      return await api(path, { method: 'POST' });
    } catch (error) {
      lastError = error;
      await sleep(350 + i * 160);
    }
  }
  throw lastError;
}

function botCountForTarget(target: number) {
  if (target >= 850) return 16;
  if (target >= 500) return 12;
  if (target >= 180) return 8;
  return 4;
}

function createBots(target: number): BotStat[] {
  return Array.from({ length: botCountForTarget(target) }, (_, index) => ({
    id: index + 1,
    success: 0,
    failure: 0,
    weight: 8 + ((index * 7) % 11),
  }));
}

function totalTraffic(bot: BotStat) {
  return bot.success + bot.failure;
}

function shortPodName(name: string) {
  return name.split('-').slice(-2).join('-');
}

function splitPods(pods: Array<{ id: string; name: string; ready: boolean }>, x = 0, y = 0, w = 100, h = 100): PodTile[] {
  if (pods.length === 0) return [];
  if (pods.length === 1) return [{ ...pods[0], x, y, w, h }];

  const firstCount = Math.ceil(pods.length / 2);
  const first = pods.slice(0, firstCount);
  const second = pods.slice(firstCount);
  const ratio = first.length / pods.length;

  if (w >= h) {
    const w1 = w * ratio;
    return [
      ...splitPods(first, x, y, w1, h),
      ...splitPods(second, x + w1, y, w - w1, h),
    ];
  }

  const h1 = h * ratio;
  return [
    ...splitPods(first, x, y, w, h1),
    ...splitPods(second, x, y + h1, w, h - h1),
  ];
}

function pickBotIndex(bots: BotStat[], sequence: number) {
  const totalWeight = bots.reduce((sum, bot) => sum + bot.weight, 0);
  let cursor = (sequence * 13) % totalWeight;
  for (let i = 0; i < bots.length; i += 1) {
    cursor -= bots[i].weight;
    if (cursor < 0) return i;
  }
  return bots.length - 1;
}

export default function App() {
  const [status, setStatus] = useState<Status | null>(null);
  const [targetTraffic, setTargetTraffic] = useState(256);
  const [bots, setBots] = useState<BotStat[]>(() => createBots(256));
  const [running, setRunning] = useState(false);
  const [busy, setBusy] = useState('');
  const [notice, setNotice] = useState('대기 중');
  const timerRef = useRef<number | null>(null);
  const botsRef = useRef(bots);
  const targetRef = useRef(targetTraffic);
  const attemptedRef = useRef(0);

  const refresh = async () => {
    try {
      setStatus(await api<Status>('/api/status'));
    } catch {
      // 부하 중에는 의도된 실패 응답이 섞이므로 상태 조회 오류를 UI 오류로 노출하지 않습니다.
    }
  };

  useEffect(() => {
    void refresh();
    const id = window.setInterval(() => { void refresh(); }, 1400);
    return () => window.clearInterval(id);
  }, []);

  useEffect(() => {
    botsRef.current = bots;
    attemptedRef.current = bots.reduce((sum, bot) => sum + totalTraffic(bot), 0);
  }, [bots]);

  useEffect(() => {
    targetRef.current = targetTraffic;
  }, [targetTraffic]);

  const ready = status?.cluster.ready_replicas ?? 0;
  const desired = status?.cluster.desired_replicas ?? 0;
  const hpaMin = status?.cluster.hpa?.min_replicas ?? 0;
  const hpaMax = status?.cluster.hpa?.max_replicas ?? 0;
  const p95 = status?.metrics.p95_latency_ms ?? 0;
  const attempted = bots.reduce((sum, bot) => sum + totalTraffic(bot), 0);
  const success = bots.reduce((sum, bot) => sum + bot.success, 0);
  const failure = bots.reduce((sum, bot) => sum + bot.failure, 0);
  const failureRate = attempted ? failure / attempted : 0;
  const successPct = attempted ? (success / attempted) * 100 : 100;
  const activeScenarios = status ? Object.values(status.scenarios).filter(Boolean).length : 0;
  const externalLoad = activeScenarios > 0 || desired > 2;
  const botCount = botCountForTarget(targetTraffic);
  const rawPods = status?.cluster.pods ?? [];
  const podCount = Math.max(desired, rawPods.length, 1);
  const podModels = Array.from({ length: podCount }, (_, index) => {
    const pod = rawPods[index];
    return {
      id: pod?.name ?? `pending-${index}`,
      name: pod ? shortPodName(pod.name) : `생성 중 ${index + 1}`,
      ready: Boolean(pod?.ready),
    };
  });
  const podTiles = useMemo(() => splitPods(podModels), [podModels]);
  const badgeState = running ? '부하 설정 중' : externalLoad ? '파드 확장' : failureRate >= 0.25 ? '실패 증가' : '정상';
  const badgeTone = failureRate >= 0.25 ? 'bad' : running || externalLoad || failureRate > 0 ? 'warn' : 'ok';
  const operationText = running
    ? `봇 ${botCount}개가 API 트래픽을 보내는 중`
    : externalLoad
      ? '파드 확장 상태입니다. 리셋하면 파드가 2개로 줄어듭니다'
      : notice;

  useEffect(() => {
    if (timerRef.current) window.clearInterval(timerRef.current);
    if (!running) return;

    timerRef.current = window.setInterval(() => {
      const remaining = targetRef.current - attemptedRef.current;
      if (remaining <= 0) {
        setRunning(false);
        setNotice('목표 트래픽 완료');
        return;
      }

      const batch = clamp(Math.min(remaining, botsRef.current.length * 2), 1, 32);
      const plans = Array.from({ length: batch }, (_, offset) => pickBotIndex(botsRef.current, attemptedRef.current + offset + 1));

      void Promise.allSettled(plans.map(() => api('/api/work', { method: 'POST' }))).then((results) => {
        setBots((current) => {
          const next = current.map((bot) => ({ ...bot }));
          results.forEach((result, index) => {
            const bot = next[plans[index]];
            if (!bot) return;
            if (result.status === 'fulfilled') bot.success += 1;
            else bot.failure += 1;
          });
          attemptedRef.current = next.reduce((sum, bot) => sum + totalTraffic(bot), 0);
          if (attemptedRef.current >= targetRef.current) {
            setRunning(false);
            setNotice('목표 트래픽 완료');
          }
          return next;
        });
      });
    }, 420);

    return () => {
      if (timerRef.current) window.clearInterval(timerRef.current);
    };
  }, [running, botCount]);

  const setTarget = (next: number) => {
    const value = clamp(Math.round(next || 1), 1, 1000);
    setTargetTraffic(value);
    if (!running && attempted === 0) setBots(createBots(value));
  };

  const startLoad = async () => {
    const freshBots = createBots(targetTraffic);
    setBots(freshBots);
    botsRef.current = freshBots;
    attemptedRef.current = 0;
    setRunning(true);
    setBusy('부하 설정');
    setNotice('부하 설정 중');

    try {
      await postWithRetry('/api/scenarios/scale-surge/start', 4);
      await postWithRetry('/api/scenarios/error-spike/start', 4);
      await refresh();
    } catch {
      setNotice('부하 설정 일부 실패: 실제 응답만 집계합니다');
    } finally {
      setBusy('');
    }
  };

  const resetLoad = async () => {
    setBusy('리셋');
    setRunning(false);
    setBots(createBots(targetTraffic));
    attemptedRef.current = 0;
    setNotice('리셋 중');

    try {
      await postWithRetry('/api/releases/rollback', 10);
      setNotice('리셋 완료');
      await refresh();
    } catch {
      setNotice('리셋 재시도 필요');
    } finally {
      setBusy('');
    }
  };

  return (
    <main className="shell">
      <section className="hero">
        <div>
          <p>bot-service-02</p>
          <h1>작업 파드 히트맵</h1>
        </div>
        <strong className={`badge ${badgeTone}`}>{badgeState}</strong>
      </section>

      <section className="control-panel" aria-label="부하 제어">
        <label>
          <span>목표 트래픽 수</span>
          <input type="number" min={1} max={1000} value={targetTraffic} onChange={(event) => setTarget(Number(event.target.value))} />
        </label>
        <button className="primary" onClick={startLoad} disabled={!!busy || running}>부하 설정</button>
        <button onClick={resetLoad} disabled={!!busy}>리셋</button>
      </section>

      <section className="definition">
        <strong>시각화 기준</strong>
        <span>큰 블록 1개는 실제 Kubernetes 파드 1개입니다. 블록 안 색은 전체 API 응답 기준이며 초록은 성공, 빨강은 실패입니다. 봇 수는 목표 트래픽에 맞춰 숫자로 표시됩니다.</span>
      </section>

      <section className="metrics">
        <Metric label="봇 수" value={`${botCount}개`} />
        <Metric label="성공 / 실패" value={`${formatCount(success)} / ${formatCount(failure)}`} tone={failure > 0 ? 'bad' : 'ok'} />
        <Metric label="파드" value={`${ready} / ${desired}`} />
        <Metric label="요청 완료" value={`${formatCount(attempted)} / ${formatCount(targetTraffic)}`} />
      </section>

      <section className="traffic-card">
        <div className="traffic-title">
          <div>
            <strong>파드 히트맵</strong>
            <span>{operationText} · HPA {hpaMin}/{hpaMax} · p95 {Math.round(p95)}ms</span>
          </div>
          <div className="legend" aria-label="색상 기준">
            <span><i className="success" />API 성공 {Math.round(successPct)}%</span>
            <span><i className="failure" />API 실패 {Math.round(failureRate * 100)}%</span>
          </div>
        </div>
        <div className="pod-map" aria-label="실제 파드 증가 감소 히트맵">
          {podTiles.map((tile) => {
            const showText = tile.w * tile.h > 460;
            return (
              <article
                key={tile.id}
                className={tile.ready ? 'ready' : 'pending'}
                style={{
                  left: `${tile.x}%`,
                  top: `${tile.y}%`,
                  width: `${tile.w}%`,
                  height: `${tile.h}%`,
                  ['--success' as string]: `${successPct}%`,
                }}
              >
                {showText && (
                  <>
                    <b>{tile.name}</b>
                    <strong>{tile.ready ? 'Ready' : 'Pending'}</strong>
                    <span>성공 {formatCount(success)} · 실패 {formatCount(failure)}</span>
                  </>
                )}
              </article>
            );
          })}
        </div>
      </section>

      <section className="footer-panel">
        <div>
          <span>운영 중 버전</span>
          <strong>{status?.version ?? '로딩 중'}</strong>
        </div>
        <div>
          <span>리셋 기준</span>
          <strong>안정 버전 · 파드 2개</strong>
        </div>
        <div>
          <span>DB 누적</span>
          <strong>{formatCount(status?.rows.jobs ?? 0)}건</strong>
        </div>
      </section>
    </main>
  );
}

function Metric({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return <div className={`metric ${tone ?? ''}`}><span>{label}</span><strong>{value}</strong></div>;
}
