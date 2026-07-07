import { useEffect, useRef, useState } from 'react';

interface Pod {
  name: string;
  ready: boolean;
}

interface Cluster {
  desired_replicas: number;
  ready_replicas: number;
  template_version: string;
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

interface WorkResult {
  success: number;
  failure: number;
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
  return Math.round(value).toLocaleString('ko-KR');
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

function heatColor(heat: number, pulse: number) {
  const value = clamp(heat + pulse * 0.08, 0, 1);
  if (value < 0.5) {
    const r = value / 0.5;
    return `rgb(${Math.round(99 + 150 * r)}, ${Math.round(205 + 13 * r)}, ${Math.round(170 - 61 * r)})`;
  }
  const r = (value - 0.5) / 0.5;
  return `rgb(${Math.round(249 - 7 * r)}, ${Math.round(218 - 98 * r)}, ${Math.round(109 + 19 * r)})`;
}

function failureRateFor(perPodTps: number) {
  return clamp((perPodTps - 700) / 2200, 0.02, 0.58);
}

export default function App() {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const statusRef = useRef<Status | null>(null);
  const runningRef = useRef(false);
  const targetTpsRef = useRef(1000);
  const trafficTimer = useRef<number | null>(null);

  const [status, setStatus] = useState<Status | null>(null);
  const [targetTps, setTargetTps] = useState(1000);
  const [running, setRunning] = useState(false);
  const [busy, setBusy] = useState('');
  const [success, setSuccess] = useState(0);
  const [failure, setFailure] = useState(0);
  const [notice, setNotice] = useState('대기 중');

  const refresh = async () => {
    try {
      const next = await api<Status>('/api/status');
      statusRef.current = next;
      setStatus(next);
      setNotice((current) => (current === '상태 조회 재시도 중' ? (runningRef.current ? '트래픽 실행 중' : '대기 중') : current));
    } catch {
      setNotice('상태 조회 재시도 중');
    }
  };

  useEffect(() => {
    void refresh();
    const id = window.setInterval(() => { void refresh(); }, 1000);
    return () => window.clearInterval(id);
  }, []);

  useEffect(() => {
    runningRef.current = running;
  }, [running]);

  useEffect(() => {
    targetTpsRef.current = targetTps;
  }, [targetTps]);

  useEffect(() => {
    let raf = 0;

    const draw = (time: number) => {
      const canvas = canvasRef.current;
      if (!canvas) {
        raf = requestAnimationFrame(draw);
        return;
      }

      const rect = canvas.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      const width = Math.max(1, Math.floor(rect.width * dpr));
      const height = Math.max(1, Math.floor(rect.height * dpr));
      if (canvas.width !== width || canvas.height !== height) {
        canvas.width = width;
        canvas.height = height;
      }

      const ctx = canvas.getContext('2d');
      if (!ctx) return;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, rect.width, rect.height);
      ctx.fillStyle = '#f1f6fb';
      ctx.fillRect(0, 0, rect.width, rect.height);

      const current = statusRef.current;
      const pods = current?.cluster.pods ?? [];
      const desired = Math.max(current?.cluster.desired_replicas ?? 1, pods.length, 1);
      const models = Array.from({ length: desired }, (_, index) => {
        const pod = pods[index];
        return { name: pod ? shortPodName(pod.name) : `생성 중 ${index + 1}`, ready: Boolean(pod?.ready) };
      });
      const boxes = splitBoxes(models);
      const perPodTps = runningRef.current ? targetTpsRef.current / Math.max(desired, 1) : 0;
      const baseHeat = clamp(perPodTps / 1000, 0.05, 1);

      boxes.forEach((box, podIndex) => {
        const x = box.x * rect.width + 5;
        const y = box.y * rect.height + 5;
        const w = box.w * rect.width - 10;
        const h = box.h * rect.height - 10;
        const cells = runningRef.current ? clamp(Math.ceil(perPodTps / 10), 8, 1000) : 1;
        const cols = Math.ceil(Math.sqrt(cells * (w / Math.max(h, 1))));
        const rows = Math.ceil(cells / cols);
        const cw = w / cols;
        const ch = h / rows;

        ctx.save();
        ctx.beginPath();
        ctx.roundRect(x, y, w, h, 8);
        ctx.clip();

        for (let i = 0; i < cells; i += 1) {
          const col = i % cols;
          const row = Math.floor(i / cols);
          const wave = Math.sin(time / 200 + i * 0.72 + podIndex) * 0.5 + 0.5;
          const localHeat = clamp(baseHeat * (0.78 + wave * 0.38), 0.03, 1);
          const gx = x + col * cw;
          const gy = y + row * ch;
          const grad = ctx.createLinearGradient(gx, gy, gx + cw, gy + ch);
          grad.addColorStop(0, heatColor(localHeat * 0.8, wave * 0.3));
          grad.addColorStop(1, heatColor(localHeat, wave));
          ctx.fillStyle = box.ready ? grad : 'rgba(249, 181, 98, .66)';
          ctx.fillRect(gx, gy, Math.ceil(cw) + 1, Math.ceil(ch) + 1);
        }

        ctx.fillStyle = 'rgba(255,255,255,.82)';
        ctx.fillRect(x, y, w, 78);
        ctx.strokeStyle = box.ready ? 'rgba(101, 163, 113, .55)' : 'rgba(249, 181, 98, .68)';
        ctx.lineWidth = 2;
        ctx.strokeRect(x + 1, y + 1, w - 2, h - 2);
        const labelSize = clamp(w / 13, 11, 16);
        const stateSize = clamp(w / 8, 16, 28);
        ctx.fillStyle = '#30415f';
        ctx.font = `900 ${labelSize}px system-ui, sans-serif`;
        ctx.fillText(box.name, x + 14, y + 26);
        ctx.font = `900 ${stateSize}px system-ui, sans-serif`;
        ctx.fillText(box.ready ? 'Ready' : 'Pending', x + 14, y + 58);
        ctx.font = '800 12px system-ui, sans-serif';
        ctx.fillStyle = '#7890aa';
        ctx.fillText(`${formatCount(perPodTps)} TPS / pod`, x + 14, y + 74);
        ctx.restore();
      });

      raf = requestAnimationFrame(draw);
    };

    raf = requestAnimationFrame(draw);
    return () => cancelAnimationFrame(raf);
  }, []);

  useEffect(() => {
    if (trafficTimer.current) window.clearInterval(trafficTimer.current);
    if (!running) return;

    trafficTimer.current = window.setInterval(() => {
      const current = statusRef.current;
      const pods = Math.max(current?.cluster.ready_replicas ?? current?.cluster.desired_replicas ?? 1, 1);
      const perPodTps = targetTpsRef.current / pods;
      const unitsPerTick = Math.max(1, Math.round(targetTpsRef.current / 5));
      const chunks = Array.from({ length: Math.ceil(unitsPerTick / 1000) }, (_, index) => Math.min(1000, unitsPerTick - index * 1000));
      const failureRate = failureRateFor(perPodTps);

      void Promise.allSettled(chunks.map((units, index) => api<WorkResult>('/api/work', {
        method: 'POST',
        body: JSON.stringify({ bot_id: `work-bot-${index + 1}`, units, failure_rate: failureRate }),
      }))).then((results) => {
        let ok = 0;
        let bad = 0;
        results.forEach((result) => {
          if (result.status === 'fulfilled') {
            ok += result.value.success;
            bad += result.value.failure;
          } else {
            bad += Math.max(1, Math.round(unitsPerTick / chunks.length));
          }
        });
        setSuccess((value) => value + ok);
        setFailure((value) => value + bad);
      });
    }, 200);

    return () => {
      if (trafficTimer.current) window.clearInterval(trafficTimer.current);
    };
  }, [running]);

  const start = () => {
    setBusy('실행');
    setSuccess(0);
    setFailure(0);
    setRunning(true);
    setNotice('트래픽 실행 중');
    setBusy('');
  };

  const stop = () => {
    setBusy('중지');
    setRunning(false);
    setNotice('중지됨');
    setBusy('');
  };

  const ready = status?.cluster.ready_replicas ?? 0;
  const desired = status?.cluster.desired_replicas ?? 0;
  const p95 = status?.metrics.p95_latency_ms ?? 0;
  const perPodTps = running ? targetTps / Math.max(ready || desired || 1, 1) : 0;
  const failRate = success + failure ? Math.round((failure / (success + failure)) * 100) : 0;

  return (
    <main className="shell">
      <section className="hero">
        <div>
          <p>bot-service-02 · 고정 파드 부하 관찰</p>
          <h1>TPS 파드 히트맵</h1>
        </div>
        <strong className={`badge ${running ? 'warn' : 'ok'}`}>{running ? '실행 중' : '대기'}</strong>
      </section>

      <section className="control-panel" aria-label="트래픽 제어">
        <label>
          <span>목표 TPS</span>
          <input type="number" min={1} max={10000} value={targetTps} onChange={(event) => setTargetTps(clamp(Number(event.target.value), 1, 10000))} />
        </label>
        <button className="primary" onClick={start} disabled={!!busy || running}>실행</button>
        <button onClick={stop} disabled={!!busy || !running}>중지</button>
      </section>

      <section className="definition">
        <strong>기준</strong>
        <span>블록 1개는 실제 Kubernetes 파드 1개입니다. 02는 TPS를 입력해도 파드를 자동으로 늘리지 않습니다. 배포 파일의 파드 기준을 늘리면 같은 TPS에서 열감이 내려갑니다.</span>
      </section>

      <section className="metrics">
        <Metric label="목표 TPS" value={formatCount(targetTps)} />
        <Metric label="파드" value={`${ready} / ${desired}`} />
        <Metric label="파드당 TPS" value={formatCount(perPodTps)} />
        <Metric label="실패율" value={`${failRate}%`} tone={failRate > 12 ? 'bad' : failRate > 0 ? 'warn' : 'ok'} />
      </section>

      <section className="traffic-card">
        <div className="traffic-title">
          <div>
            <strong>60fps 파드 히트맵</strong>
            <span>{notice} · 성공 {formatCount(success)} · 실패 {formatCount(failure)} · p95 {Math.round(p95)}ms · 버전 {status?.version ?? '-'}</span>
          </div>
          <div className="legend">
            <span><i className="low" />낮음</span>
            <span><i className="mid" />주의</span>
            <span><i className="high" />위험</span>
          </div>
        </div>
        <canvas ref={canvasRef} className="pod-canvas" aria-label="TPS 파드 히트맵" />
      </section>
    </main>
  );
}

function Metric({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return <div className={`metric ${tone ?? ''}`}><span>{label}</span><strong>{value}</strong></div>;
}
