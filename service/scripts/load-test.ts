// Open-loop HTTP load test for POST /events.
//
// Sends events at a fixed target rate per phase, without waiting for replies,
// so slow responses show up as latency instead of silently lowering the rate.
// Afterwards it checks that every 2xx event is in Postgres.
//
//   node scripts/load-test.ts [--target http://127.0.0.1:8080] [--devices 5000]
//        [--phases 5000:20,50000:30,5000:20] [--connections 256]
//        [--db postgres://teton:teton@127.0.0.1:5433/teton] [--max-pending 20000]
//
// Latency is measured from the scheduled send time, so client-side queueing
// (all sockets busy) counts too. If "achieved" is well below "target" the
// client machine is the bottleneck, not the service. Events that would push the
// client past --max-pending in-flight requests are not sent and are reported as
// "skipped (client overloaded)" - they are not service failures.

import http from 'node:http';
import { parseArgs } from 'node:util';
import pg from 'pg';

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'http://127.0.0.1:8080' },
    devices: { type: 'string', default: '5000' },
    phases: { type: 'string', default: '5000:20,50000:30,5000:20' },
    connections: { type: 'string', default: '256' },
    timeout: { type: 'string', default: '5000' },
    db: { type: 'string', default: 'postgres://teton:teton@127.0.0.1:5433/teton' },
    'max-pending': { type: 'string', default: '20000' },
  },
});

const target = new URL('/events', args.target);
const deviceCount = Number(args.devices);
const timeoutMs = Number(args.timeout);
const maxPending = Number(args['max-pending']);
const phases = args.phases.split(',').map((p) => {
  const [rate, seconds] = p.split(':').map(Number);
  return { rate, seconds };
});
const agent = new http.Agent({ keepAlive: true, maxSockets: Number(args.connections) });

// Unique prefix so the DB check counts only this run's rows.
// Random suffix: parallel clients can start in the same millisecond.
const runId = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
const devices = Array.from({ length: deviceCount }, (_, i) => ({
  id: `load_${runId}_${String(i).padStart(5, '0')}`,
  room: `load_${runId}_room_${Math.floor(i / 2)}`,
  seq: 0,
}));

// Same mix as event_generator/generate.py.
const WEIGHTS: Array<[string, number]> = [
  ['heartbeat', 50],
  ['motion', 15],
  ['presence', 4],
  ['sleep_state', 3],
  ['net_status', 3],
  ['fall_warn', 1],
];
const TOTAL_WEIGHT = WEIGHTS.reduce((s, [, w]) => s + w, 0);

function makeEvent(device: (typeof devices)[number]): { type: string; body: string } {
  let r = Math.random() * TOTAL_WEIGHT;
  const type = WEIGHTS.find(([, w]) => (r -= w) < 0)?.[0] ?? 'heartbeat';
  device.seq++;
  const e: Record<string, unknown> = {
    device_id: device.id,
    room_id: device.room,
    type,
    ts: new Date().toISOString(),
    seq: device.seq,
  };
  if (type === 'presence') e.in_room = Math.random() < 0.5;
  if (type === 'motion') e.magnitude = Math.round(Math.random() * 100) / 100;
  if (type === 'sleep_state') e.state = ['asleep', 'awake', 'unknown'][Math.floor(Math.random() * 3)];
  if (type === 'fall_warn') e.confidence = 0.7 + Math.round(Math.random() * 29) / 100;
  if (type === 'net_status') e.rssi = -90 + Math.floor(Math.random() * 41);
  return { type, body: JSON.stringify(e) };
}

interface PhaseStats {
  label: string;
  sent: number;
  ok: number;
  duplicate: number;
  rejected: number; // 4xx
  serverError: number; // 5xx
  failed: number; // network error / timeout
  skipped: number; // not sent: client already had max-pending requests in flight
  latency: number[];
  fallLatency: number[];
}

const newStats = (label: string): PhaseStats => ({
  label,
  sent: 0,
  ok: 0,
  duplicate: 0,
  rejected: 0,
  serverError: 0,
  failed: 0,
  skipped: 0,
  latency: [],
  fallLatency: [],
});

let inFlight = 0;
let maxInFlight = 0;

function send(stats: PhaseStats, scheduledAt: number, deviceIdx: number): Promise<void> {
  const { type, body } = makeEvent(devices[deviceIdx]);
  stats.sent++;
  inFlight++;
  maxInFlight = Math.max(maxInFlight, inFlight);

  return new Promise((resolve) => {
    const done = (outcome: keyof PhaseStats) => {
      inFlight--;
      (stats[outcome] as number)++;
      if (outcome === 'ok' || outcome === 'duplicate') {
        const ms = performance.now() - scheduledAt;
        stats.latency.push(ms);
        if (type === 'fall_warn') stats.fallLatency.push(ms);
      }
      resolve();
    };

    const req = http.request(
      target,
      {
        method: 'POST',
        agent,
        timeout: timeoutMs,
        headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) },
      },
      (res) => {
        let text = '';
        res.setEncoding('utf8');
        res.on('data', (chunk: string) => (text += chunk));
        res.on('end', () => {
          const status = res.statusCode ?? 0;
          if (status >= 200 && status < 300) done(text.includes('duplicate') ? 'duplicate' : 'ok');
          else if (status >= 400 && status < 500) done('rejected');
          else done('serverError');
        });
      },
    );
    req.on('timeout', () => req.destroy(new Error('timeout')));
    req.on('error', () => done('failed'));
    req.end(body);
  });
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return NaN;
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

function report(s: PhaseStats, seconds: number, targetRate: number): void {
  const lat = s.latency.sort((a, b) => a - b);
  const fall = s.fallLatency.sort((a, b) => a - b);
  const f = (n: number) => (Number.isNaN(n) ? '-' : n.toFixed(1));
  console.log(
    `${s.label.padEnd(18)} target ${targetRate}/s  achieved ${(s.sent / seconds).toFixed(0)}/s  ` +
      `sent ${s.sent}  ok ${s.ok}  dup ${s.duplicate}  4xx ${s.rejected}  5xx ${s.serverError}  failed ${s.failed}` +
      (s.skipped > 0 ? `  skipped (client overloaded) ${s.skipped}` : ''),
  );
  console.log(
    `${''.padEnd(18)} latency ms  p50 ${f(percentile(lat, 50))}  p95 ${f(percentile(lat, 95))}  ` +
      `p99 ${f(percentile(lat, 99))}  max ${f(lat.at(-1) ?? NaN)}  |  fall_warn p95 ${f(percentile(fall, 95))} (n=${fall.length})`,
  );
}

async function runPhase(index: number, rate: number, seconds: number): Promise<PhaseStats> {
  const stats = newStats(`phase ${index + 1} (${rate}/s)`);
  const pending: Promise<void>[] = [];
  const start = performance.now();
  const end = start + seconds * 1000;
  let scheduled = 0;
  let nextDevice = 0;

  // Every tick, send however many events are due by now at the target rate.
  await new Promise<void>((resolve) => {
    const tick = () => {
      const now = performance.now();
      const due = Math.floor(((Math.min(now, end) - start) / 1000) * rate);
      for (; scheduled < due; scheduled++) {
        const scheduledAt = start + (scheduled / rate) * 1000;
        if (inFlight >= maxPending) {
          stats.skipped++;
          continue;
        }
        pending.push(send(stats, scheduledAt, nextDevice));
        nextDevice = (nextDevice + 1) % deviceCount;
      }
      if (now >= end) resolve();
      else setTimeout(tick, 5);
    };
    tick();
  });

  await Promise.all(pending);
  report(stats, seconds, rate);
  return stats;
}

async function main(): Promise<void> {
  console.log(
    `load test -> ${target.href}  devices ${deviceCount}  connections ${args.connections}  run ${runId}`,
  );
  const all: PhaseStats[] = [];
  for (const [i, { rate, seconds }] of phases.entries()) {
    all.push(await runPhase(i, rate, seconds));
  }
  agent.destroy();

  const sum = (k: 'sent' | 'ok' | 'duplicate' | 'rejected' | 'serverError' | 'failed' | 'skipped') =>
    all.reduce((s, p) => s + p[k], 0);
  console.log(
    `\ntotal sent ${sum('sent')}  2xx ${sum('ok') + sum('duplicate')}  4xx ${sum('rejected')}  ` +
      `5xx ${sum('serverError')}  failed ${sum('failed')}  skipped ${sum('skipped')}  max in-flight ${maxInFlight}`,
  );

  // Every 2xx must be durable: compare with the rows stored for this run.
  const db = new pg.Client({ connectionString: args.db });
  await db.connect();
  const { rows } = await db.query<{ n: number }>(
    'SELECT count(*)::int AS n FROM events WHERE device_id LIKE $1',
    [`load_${runId}_%`],
  );
  await db.end();
  const stored = rows[0].n;
  const acked = sum('ok');
  console.log(`rows in Postgres ${stored}  acked as inserted ${acked}  ${stored === acked ? 'OK' : 'MISMATCH'}`);
  if (stored !== acked || sum('failed') + sum('serverError') + sum('rejected') > 0) process.exitCode = 1;
}

main().catch((err: unknown) => {
  console.error(err);
  process.exitCode = 1;
});
