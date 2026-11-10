// Hard-restart test: SSE consumers must resume without missing alarms.
//
// 1. Opens --clients SSE connections to /alarms/stream (with workers they land
//    on different processes). Each behaves like a browser EventSource: on
//    disconnect it reconnects with Last-Event-ID = the last id it received.
// 2. Runs scripts/load-test.ts against the service (mixed events incl. falls).
// 3. At --kill-at seconds, SIGKILLs the service container (no shutdown hooks,
//    no drain), waits --down-for seconds, starts it again.
// 4. After the load ends: every client must have received exactly the alarms
//    in Postgres committed since the start, in id order, no gaps, no doubles.
//    load-test.ts separately checks that every 2xx event is in Postgres.
//
//   node scripts/restart-test.ts [--target http://127.0.0.1:8080] [--clients 4]
//        [--rate 2000] [--duration 40] [--kill-at 15] [--down-for 5]
//        [--container careers-challenge-streaming-backend-service-1]
//        [--db postgres://teton:teton@127.0.0.1:5433/teton]
//
// Needs the Docker Compose stack running (`make run` or `docker compose up -d`).

import { execFileSync, spawn } from 'node:child_process';
import { parseArgs } from 'node:util';
import pg from 'pg';

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'http://127.0.0.1:8080' },
    clients: { type: 'string', default: '4' },
    rate: { type: 'string', default: '2000' },
    duration: { type: 'string', default: '40' },
    'kill-at': { type: 'string', default: '15' },
    'down-for': { type: 'string', default: '5' },
    container: { type: 'string', default: 'careers-challenge-streaming-backend-service-1' },
    db: { type: 'string', default: 'postgres://teton:teton@127.0.0.1:5433/teton' },
  },
});

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const docker = (...a: string[]) => execFileSync('docker', a, { encoding: 'utf8' }).trim();

/** An SSE consumer that reconnects like EventSource, sending Last-Event-ID. */
class Consumer {
  readonly ids: number[] = [];
  connects = 0;
  private lastId: string | undefined;
  private stopped = false;
  private abort = new AbortController();
  private readonly url: string;

  constructor(url: string) {
    this.url = url;
  }

  async run(): Promise<void> {
    while (!this.stopped) {
      try {
        const headers: Record<string, string> = this.lastId ? { 'Last-Event-ID': this.lastId } : {};
        const res = await fetch(this.url, { headers, signal: this.abort.signal });
        if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);
        this.connects++;
        await this.read(res.body.pipeThrough(new TextDecoderStream()));
      } catch {
        // connection refused / reset while the service is down
      }
      if (!this.stopped) await sleep(500); // EventSource's retry delay
    }
  }

  private async read(text: ReadableStream<string>): Promise<void> {
    let buffer = '';
    for await (const chunk of text) {
      buffer += chunk;
      let end: number;
      while ((end = buffer.indexOf('\n\n')) >= 0) {
        const block = buffer.slice(0, end);
        buffer = buffer.slice(end + 2);
        const idLine = block.split('\n').find((l) => l.startsWith('id:'));
        if (!idLine) continue; // keepalive comment
        this.lastId = idLine.slice(3).trim();
        this.ids.push(Number(this.lastId));
      }
    }
  }

  stop(): void {
    this.stopped = true;
    this.abort.abort();
  }
}

async function waitUntilUp(timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      await fetch(new URL('/alarms?since=0', args.target), { signal: AbortSignal.timeout(1000) });
      return;
    } catch {
      await sleep(250);
    }
  }
  throw new Error('service did not come back');
}

/** Ids that are missing, doubled or out of order in `got`, compared to `expected`. */
function compare(expected: number[], got: number[]) {
  const seen = new Set<number>();
  let doubles = 0;
  let outOfOrder = 0;
  got.forEach((id, i) => {
    if (seen.has(id)) doubles++;
    seen.add(id);
    if (i > 0 && id <= got[i - 1]) outOfOrder++;
  });
  const missing = expected.filter((id) => !seen.has(id));
  const unexpected = [...seen].filter((id) => !expected.includes(id));
  return { missing, doubles, outOfOrder, unexpected };
}

async function main() {
  const db = new pg.Pool({ connectionString: args.db });
  const startId = Number(
    (await db.query<{ id: string | null }>('SELECT max(id) AS id FROM alarms')).rows[0].id ?? 0,
  );

  const consumers = Array.from(
    { length: Number(args.clients) },
    () => new Consumer(new URL('/alarms/stream', args.target).href),
  );
  const running = consumers.map((c) => c.run());
  await sleep(1000); // all connected before the first alarm

  const rate = Number(args.rate);
  const load = spawn(
    process.execPath,
    ['scripts/load-test.ts', '--target', args.target, '--db', args.db,
      '--phases', `${rate}:${args.duration}`, '--timeout', '2000'],
    { stdio: ['ignore', 'pipe', 'inherit'] },
  );
  let loadOutput = '';
  load.stdout.on('data', (d: Buffer) => (loadOutput += d.toString()));
  const loadDone = new Promise<void>((resolve) => load.on('exit', () => resolve()));

  await sleep(Number(args['kill-at']) * 1000);
  const killedAt = Date.now();
  docker('kill', '--signal', 'KILL', args.container);
  console.log(`t=${args['kill-at']}s  SIGKILL ${args.container}`);
  await sleep(Number(args['down-for']) * 1000);
  docker('start', args.container);
  await waitUntilUp(30_000);
  console.log(`service back after ${((Date.now() - killedAt) / 1000).toFixed(1)}s`);

  await loadDone;
  await sleep(3000); // let the last alarms and catch-ups arrive
  consumers.forEach((c) => c.stop());
  await Promise.all(running);

  const expected = (
    await db.query<{ id: string }>('SELECT id FROM alarms WHERE id > $1 ORDER BY id', [startId])
  ).rows.map((r) => Number(r.id));
  await db.end();

  console.log('\n--- load test');
  console.log(loadOutput.trim());
  console.log(`\n--- SSE consumers (${expected.length} alarms committed during the run)`);
  // Requests failing while the service is down are expected (the client doesn't
  // retry); every acknowledged event must still be in Postgres.
  let ok = /missing in Postgres 0 /.test(loadOutput) && expected.length > 0;
  consumers.forEach((c, i) => {
    const r = compare(expected, c.ids);
    const pass =
      r.missing.length === 0 && r.doubles === 0 && r.outOfOrder === 0 && r.unexpected.length === 0;
    ok &&= pass;
    console.log(
      `client ${i + 1}: received ${c.ids.length}  connects ${c.connects}  missing ${r.missing.length}` +
        `  doubles ${r.doubles}  out-of-order ${r.outOfOrder}  ${pass ? 'OK' : 'FAIL'}`,
    );
  });
  console.log(ok ? '\nPASS' : '\nFAIL');
  process.exit(ok ? 0 : 1);
}

void main();
