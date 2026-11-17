import { type ChildProcess, execFileSync, spawn } from 'node:child_process';
import http from 'node:http';
import net from 'node:net';
import pg from 'pg';
import { openStream } from './sse-client.js';

// The built service (dist/main) as a real process tree, with 1, 2 and 4 workers.
// Every request opens a new connection and the cluster hands connections out
// round-robin, so consecutive requests land on different workers: whatever a
// worker writes, every other worker must read, stream and count.

const SERVICE_DIR = new URL('..', import.meta.url).pathname.replace(/^\/(\w:)/, '$1');

interface Service {
  proc: ChildProcess;
  url: string;
  /** pids of the processes that logged "service started", in order. */
  started: number[];
}

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer().listen(0, '127.0.0.1', () => {
      const { port } = server.address() as net.AddressInfo;
      server.close(() => resolve(port));
    });
    server.on('error', reject);
  });
}

async function waitUntil(what: string, cond: () => boolean, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 25));
  }
}

async function startService(workers: number): Promise<Service> {
  const [port, metricsPort] = await Promise.all([freePort(), freePort()]);
  const proc = spawn(process.execPath, ['dist/main.js'], {
    cwd: SERVICE_DIR,
    env: {
      ...process.env,
      PORT: String(port),
      METRICS_PORT: String(metricsPort),
      WORKERS: String(workers),
      NODE_CLUSTER_SCHED_POLICY: 'rr', // the default on Windows is "let the OS pick"
    },
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  const service: Service = { proc, url: `http://127.0.0.1:${port}`, started: [] };
  let buffer = '';
  proc.stdout!.on('data', (chunk: Buffer) => {
    buffer += chunk.toString();
    let end: number;
    while ((end = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, end);
      buffer = buffer.slice(end + 1);
      try {
        const entry = JSON.parse(line) as { msg?: string; pid: number };
        if (entry.msg === 'service started') service.started.push(entry.pid);
      } catch {
        // not a log line
      }
    }
  });
  await waitUntil(`${workers} workers to start`, () => service.started.length === workers);
  return service;
}

async function stopService(service: Service): Promise<void> {
  if (service.proc.exitCode !== null) return;
  const exited = new Promise((r) => service.proc.once('exit', r));
  // On Windows kill() terminates only the primary; take the workers with it.
  if (process.platform === 'win32') {
    execFileSync('taskkill', ['/PID', String(service.proc.pid), '/T', '/F'], { stdio: 'ignore' });
  } else {
    service.proc.kill('SIGTERM');
  }
  await exited;
}

/** One request on a fresh connection (no keep-alive), so the cluster picks a worker each time. */
function request(
  base: string,
  method: string,
  path: string,
  body?: unknown,
): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    const payload = body === undefined ? undefined : JSON.stringify(body);
    const req = http.request(
      new URL(path, base),
      {
        method,
        agent: false,
        headers: payload ? { 'content-type': 'application/json' } : {},
      },
      (res) => {
        let text = '';
        res.setEncoding('utf8');
        res.on('data', (d: string) => (text += d));
        res.on('end', () => resolve({ status: res.statusCode!, body: text }));
      },
    );
    req.on('error', reject);
    req.end(payload);
  });
}

/** Sum of all samples of a counter in Prometheus text (all label sets). */
function counterTotal(metricsText: string, name: string): number {
  return metricsText
    .split('\n')
    .filter((l) => l.startsWith(`${name}{`) || l.startsWith(`${name} `))
    .reduce((sum, l) => sum + Number(l.slice(l.lastIndexOf(' ') + 1)), 0);
}

beforeAll(() => {
  execFileSync(process.execPath, ['node_modules/@nestjs/cli/bin/nest.js', 'build'], {
    cwd: SERVICE_DIR,
    stdio: 'inherit',
  });
}, 120_000);

describe.each([1, 2, 4])('service with WORKERS=%i', (workers) => {
  let db: pg.Pool;
  let service: Service;
  const now = Date.now();
  const post = (event: unknown) => request(service.url, 'POST', '/events', event);
  const get = (path: string) => request(service.url, 'GET', path);

  beforeAll(async () => {
    db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
    await db.query('TRUNCATE events, alarms RESTART IDENTITY');
    service = await startService(workers);
  }, 30_000);
  afterAll(async () => {
    if (service) await stopService(service);
    await db?.end();
  });

  it('starts one process per worker', () => {
    expect(new Set(service.started).size).toBe(workers);
  });

  it('persists every event once across workers and counts duplicates', async () => {
    const events = Array.from({ length: 200 }, (_, i) => ({
      device_id: `dev_${i % 20}`,
      room_id: `room_${i % 10}`,
      type: 'heartbeat',
      ts: new Date(now - 60_000 + i * 100).toISOString(),
      seq: i,
    }));
    // Every event twice, at the same time, so copies race on different workers.
    const results = await Promise.all([...events, ...events].map(post));
    expect(results.every((r) => r.status === 200)).toBe(true);
    const statuses = results.map((r) => (JSON.parse(r.body) as { status: string }).status);
    expect(statuses.filter((s) => s === 'ok')).toHaveLength(200);
    expect(statuses.filter((s) => s === 'duplicate')).toHaveLength(200);

    const { rows } = await db.query<{ n: number }>('SELECT count(*)::int AS n FROM events');
    expect(rows[0].n).toBe(200);
  });

  it('serves the same reads from every worker', async () => {
    await post({
      device_id: 'dev_p',
      room_id: 'room_p',
      type: 'presence',
      ts: new Date(now - 5000).toISOString(),
      seq: 1,
      in_room: true,
    });
    const healths = await Promise.all(
      Array.from({ length: 2 * workers }, () => get('/devices/dev_0/health')),
    );
    const rooms = await Promise.all(
      Array.from({ length: 2 * workers }, () => get('/rooms/room_p/occupancy?window=1m')),
    );
    for (const r of healths) {
      expect(r.status).toBe(200);
      expect(r.body).toBe(healths[0].body);
    }
    // dev_0 sent seq 0, 20, ..., 180: the newest is seq 180.
    expect(JSON.parse(healths[0].body).last_heartbeat_ts).toBe(
      new Date(now - 60_000 + 180 * 100).toISOString(),
    );
    for (const r of rooms) expect(JSON.parse(r.body).in_room).toBe(true);
  });

  it('pushes alarms ingested by any worker to SSE clients on every worker, in order, once', async () => {
    const streams = await Promise.all(
      Array.from({ length: 2 * workers }, () => openStream(service.url)),
    );
    const falls = Array.from({ length: 10 }, (_, i) => ({
      device_id: `dev_fall_${i}`, // one device each, so every fall is its own alarm
      room_id: 'room_fall',
      type: 'fall_warn',
      ts: new Date(now - 2000).toISOString(),
      seq: 1,
      confidence: 0.9,
    }));
    const results = await Promise.all(falls.map(post));
    expect(results.every((r) => r.status === 200)).toBe(true);

    const expected = Array.from({ length: 10 }, (_, i) => String(i + 1));
    for (const stream of streams) {
      const msgs = await stream.waitFor(10, 3000);
      expect(msgs.map((m) => m.id)).toEqual(expected);
    }
    await new Promise((r) => setTimeout(r, 200));
    for (const stream of streams) {
      expect(stream.messages).toHaveLength(10); // no doubles
      stream.close();
    }

    const alarms = JSON.parse((await get('/alarms?since=0')).body) as { alarms: unknown[] };
    expect(alarms.alarms).toHaveLength(10);
  });

  it('reports metrics merged over all workers', async () => {
    // Asked several times so different workers answer: all must report the totals.
    for (let i = 0; i < workers; i++) {
      const res = await get('/metrics');
      expect(res.status).toBe(200);
      // 200 heartbeats + 1 presence + 10 falls; each heartbeat was also sent once more.
      expect(counterTotal(res.body, 'events_ingested_total')).toBe(211);
      expect(counterTotal(res.body, 'events_duplicate_total')).toBe(200);
    }
  });

  it.runIf(workers > 1)('replaces a crashed worker and keeps serving', async () => {
    const victim = service.started[0];
    process.kill(victim, 'SIGKILL');
    await waitUntil('a replacement worker', () => service.started.length === workers + 1);
    expect(service.started.at(-1)).not.toBe(victim);

    const results = await Promise.all(
      Array.from({ length: 2 * workers }, () => get('/alarms?since=0')),
    );
    expect(results.every((r) => r.status === 200)).toBe(true);
  });
});
