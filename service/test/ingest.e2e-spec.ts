import type { NestFastifyApplication } from '@nestjs/platform-fastify';
import type { FastifyInstance } from 'fastify';
import pg from 'pg';
import { IngestService } from '../src/ingest/ingest.service.js';
import { startApp } from './app.js';

const event = (seq: number, type = 'heartbeat', extra: Record<string, unknown> = {}) => ({
  device_id: 'dev_0001',
  room_id: 'room_000',
  type,
  ts: new Date(Date.now() - 1000 + seq).toISOString(),
  seq,
  ...extra,
});

describe('POST /events', () => {
  let db: pg.Pool;
  let app: NestFastifyApplication;
  let fastify: FastifyInstance;

  const count = async () =>
    (await db.query<{ n: number }>('SELECT count(*)::int AS n FROM events')).rows[0].n;
  const post = (payload: unknown) =>
    fastify.inject({ method: 'POST', url: '/events', payload: payload as object });

  beforeAll(() => {
    db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
  });
  afterAll(() => db.end());

  beforeEach(async () => {
    await db.query('TRUNCATE events RESTART IDENTITY');
    ({ app, fastify } = await startApp());
  });
  afterEach(async () => {
    await app?.close();
  });

  it('persists a valid event before replying 200', async () => {
    const res = await post(event(1));
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ status: 'ok' });
    expect(await count()).toBe(1);
  });

  it('replies 200 duplicate for a replayed event and stores it once', async () => {
    const e = event(1, 'fall_warn', { confidence: 0.9 });
    expect((await post(e)).json()).toEqual({ status: 'ok' });
    expect((await post(e)).json()).toEqual({ status: 'duplicate' });
    expect(await count()).toBe(1);
  });

  it('rejects an invalid event with 400 and the reason', async () => {
    const res = await post({ ...event(1), type: 'presence' }); // missing in_room
    expect(res.statusCode).toBe(400);
    expect(res.json()).toEqual({ error: 'missing_field' });

    const future = await post({ ...event(2), ts: new Date(Date.now() + 2 * 3600_000).toISOString() });
    expect(future.json()).toEqual({ error: 'ts_future' });
    expect(await count()).toBe(0);
  });

  it('rejects malformed JSON with 400 invalid_json', async () => {
    const res = await fastify.inject({
      method: 'POST',
      url: '/events',
      headers: { 'content-type': 'application/json' },
      payload: '{"device_id": ',
    });
    expect(res.statusCode).toBe(400);
    expect(res.json()).toEqual({ error: 'invalid_json' });
  });

  it('accepts a 5000-event concurrent burst without losing any', async () => {
    const payloads = Array.from({ length: 5000 }, (_, i) =>
      i % 100 === 0 ? event(i, 'fall_warn', { confidence: 0.8 }) : event(i),
    );
    const responses = await Promise.all(payloads.map(post));
    expect(responses.every((r) => r.statusCode === 200)).toBe(true);
    expect(await count()).toBe(5000);
  });

  it('persists queued and in-flight events on shutdown before closing the pool', async () => {
    const writer = app.get(IngestService).writer;

    // Simulate a slow database: another session holds a lock that blocks inserts.
    const locker = await db.connect();
    await locker.query('BEGIN');
    await locker.query('LOCK TABLE events IN EXCLUSIVE MODE');

    const pending = Array.from({ length: 300 }, (_, i) => post(event(i)));
    await vi.waitFor(() => {
      const d = writer.queueDepth();
      expect(d.normal + d.inFlight).toBe(300); // all stuck behind the lock
    });

    // Close waits for in-flight requests, which finish once the insert can run;
    // the drain in onApplicationShutdown then writes anything left before pool.end().
    const closed = app.close();
    setTimeout(() => void locker.query('COMMIT').finally(() => locker.release()), 200);
    await closed;

    const responses = await Promise.all(pending);
    expect(responses.every((r) => r.statusCode === 200)).toBe(true);
    expect(await count()).toBe(300);
    app = undefined as unknown as NestFastifyApplication; // already closed
  });
});
