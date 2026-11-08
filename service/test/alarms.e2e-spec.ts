import type { NestFastifyApplication } from '@nestjs/platform-fastify';
import type { FastifyInstance } from 'fastify';
import pg from 'pg';
import { startApp } from './app.js';

interface SseMessage {
  id?: string;
  event?: string;
  data: string;
}

/** Opens GET /alarms/stream and collects parsed SSE messages as they arrive. */
async function openStream(
  baseUrl: string,
  headers: Record<string, string> = {},
) {
  const controller = new AbortController();
  const res = await fetch(`${baseUrl}/alarms/stream`, {
    headers,
    signal: controller.signal,
  });
  const messages: SseMessage[] = [];
  let comments = 0;
  const reader = res.body!.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = '';
  void (async () => {
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) return;
        buffer += value;
        let end: number;
        while ((end = buffer.indexOf('\n\n')) >= 0) {
          const block = buffer.slice(0, end).replace(/^\n+/, ''); // Nest opens with "\n"
          buffer = buffer.slice(end + 2);
          if (block.startsWith(':')) {
            comments++; // keepalive comment, not an event
            continue;
          }
          const msg: SseMessage = { data: '' };
          for (const line of block.split('\n')) {
            const [field, ...rest] = line.split(':');
            const value = rest.join(':').replace(/^ /, '');
            if (field === 'id') msg.id = value;
            else if (field === 'event') msg.event = value;
            else if (field === 'data') msg.data += value;
          }
          messages.push(msg);
        }
      }
    } catch {
      // aborted
    }
  })();
  /** Waits until `count` messages have arrived (or fails after timeoutMs). */
  const waitFor = async (count: number, timeoutMs = 2000) => {
    const deadline = Date.now() + timeoutMs;
    while (messages.length < count) {
      if (Date.now() > deadline)
        throw new Error(`got ${messages.length}/${count} SSE messages`);
      await new Promise((r) => setTimeout(r, 10));
    }
    return messages;
  };
  return {
    status: res.status,
    messages,
    comments: () => comments,
    waitFor,
    close: () => controller.abort(),
  };
}

describe('alarms', () => {
  let db: pg.Pool;
  let app: NestFastifyApplication;
  let fastify: FastifyInstance;
  let baseUrl: string;
  let seq = 0;

  const fall = (deviceId: string, tsMs: number, confidence: number) =>
    fastify.inject({
      method: 'POST',
      url: '/events',
      payload: {
        device_id: deviceId,
        room_id: 'room_007',
        type: 'fall_warn',
        ts: new Date(tsMs).toISOString(),
        seq: ++seq,
        confidence,
      },
    });

  const listAlarms = async (since = '0') =>
    (
      await fastify.inject({ method: 'GET', url: `/alarms?since=${since}` })
    ).json<{
      alarms: Array<{
        event_id: string;
        room_id: string;
        ts: string;
        confidence: number;
      }>;
    }>().alarms;

  async function boot() {
    ({ app, fastify } = await startApp());
    await app.listen(0, '127.0.0.1');
    baseUrl = await app.getUrl();
  }

  beforeAll(() => {
    db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
  });
  afterAll(() => db.end());

  beforeEach(async () => {
    await db.query('TRUNCATE events, alarms RESTART IDENTITY');
    await boot();
  });
  afterEach(() => app.close());

  it('turns jitter copies into one alarm with the original ts and max confidence', async () => {
    const ts = Date.now() - 2000;
    const responses = await Promise.all([
      fall('dev_f', ts, 0.8),
      fall('dev_f', ts, 0.93),
      fall('dev_f', ts + 1000, 0.85),
    ]);
    expect(responses.map((r) => r.statusCode)).toEqual([200, 200, 200]);

    await new Promise((r) => setTimeout(r, 100)); // alarm write follows the event commit
    expect(await listAlarms()).toEqual([
      {
        event_id: `dev_f:${new Date(ts).toISOString()}`,
        room_id: 'room_007',
        ts: new Date(ts).toISOString(),
        confidence: 0.93,
      },
    ]);
    const rows = await db.query(
      'SELECT count(*)::int AS n, max(confidence) AS c FROM alarms',
    );
    expect(rows.rows[0]).toEqual({ n: 1, c: 0.93 });
  });

  it('pushes a new fall over SSE within 1s, once', async () => {
    const stream = await openStream(baseUrl);
    expect(stream.status).toBe(200);
    const started = Date.now();
    await Promise.all([
      fall('dev_s', Date.now(), 0.9),
      fall('dev_s', Date.now(), 0.9),
    ]);
    const [msg] = await stream.waitFor(1, 1000);
    expect(Date.now() - started).toBeLessThan(1000);
    expect(msg.id).toBe('1');
    expect(JSON.parse(msg.data)).toMatchObject({
      room_id: 'room_007',
      confidence: 0.9,
    });

    await new Promise((r) => setTimeout(r, 200));
    expect(stream.messages).toHaveLength(1); // the copy was not pushed
    stream.close();
  });

  it('sends keepalives as comments, so they never take an event id', async () => {
    await app.close();
    process.env.SSE_KEEPALIVE_MS = '30';
    try {
      await boot();
    } finally {
      delete process.env.SSE_KEEPALIVE_MS;
    }
    const stream = await openStream(baseUrl);
    try {
      await new Promise((r) => setTimeout(r, 120));
      await fall('dev_k', Date.now(), 0.9);
      const [msg] = await stream.waitFor(1);
      expect(stream.comments()).toBeGreaterThan(0);
      expect(stream.messages).toHaveLength(1); // only the alarm is an event
      expect(msg.id).toBe('1'); // the alarm's own id, not a counter bumped by keepalives
    } finally {
      stream.close();
    }
  });

  it('catches up from Last-Event-ID after a reconnect', async () => {
    const now = Date.now();
    await fall('dev_1', now - 30_000, 0.9);
    await fall('dev_2', now - 20_000, 0.9);
    await fall('dev_3', now - 10_000, 0.9);
    await new Promise((r) => setTimeout(r, 100));

    const stream = await openStream(baseUrl, { 'Last-Event-ID': '1' });
    const messages = await stream.waitFor(2);
    expect(messages.map((m) => m.id)).toEqual(['2', '3']);
    stream.close();
  });

  it('keeps alarms and dedup state across a restart', async () => {
    const ts = Date.now() - 1000;
    await fall('dev_r', ts, 0.8);
    await new Promise((r) => setTimeout(r, 100));
    await app.close();

    await boot();
    expect((await listAlarms()).map((a) => a.event_id)).toEqual([
      `dev_r:${new Date(ts).toISOString()}`,
    ]);
    // A jitter copy arriving after the restart joins the persisted incident.
    await fall('dev_r', ts + 1000, 0.95);
    await new Promise((r) => setTimeout(r, 100));
    const alarms = await listAlarms();
    expect(alarms).toHaveLength(1);
    expect(alarms[0].confidence).toBe(0.95);
  });

  it('filters by since and rejects a bad since', async () => {
    const now = Date.now();
    await fall('dev_a', now - 60_000, 0.9);
    await fall('dev_b', now - 1000, 0.9);
    await new Promise((r) => setTimeout(r, 100));
    expect(await listAlarms(String((now - 30_000) / 1000))).toHaveLength(1);
    expect(await listAlarms(new Date(now - 30_000).toISOString())).toHaveLength(
      1,
    );
    const res = await fastify.inject({
      method: 'GET',
      url: '/alarms?since=yesterday',
    });
    expect(res.statusCode).toBe(400);
  });
});
