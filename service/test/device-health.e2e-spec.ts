import type { NestFastifyApplication } from '@nestjs/platform-fastify';
import type { FastifyInstance } from 'fastify';
import pg from 'pg';
import { startApp } from './app.js';

describe('GET /devices/:id/health', () => {
  let db: pg.Pool;
  let app: NestFastifyApplication;
  let fastify: FastifyInstance;

  const heartbeat = (deviceId: string, seq: number, tsMs: number) =>
    fastify.inject({
      method: 'POST',
      url: '/events',
      payload: {
        device_id: deviceId,
        room_id: 'room_000',
        type: 'heartbeat',
        ts: new Date(tsMs).toISOString(),
        seq,
      },
    });

  beforeAll(() => {
    db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
  });
  afterAll(() => db.end());

  beforeEach(async () => {
    await db.query('TRUNCATE events RESTART IDENTITY');
    ({ app, fastify } = await startApp());
  });
  afterEach(() => app.close());

  it('reports the last heartbeat and availability once events are committed', async () => {
    const now = Date.now();
    // 60 heartbeats over the last 60s, every other second missing in the last 20s.
    const posts = [];
    for (let i = 0; i < 60; i++) {
      if (i >= 40 && i % 2 === 1) continue;
      posts.push(heartbeat('dev_e2e', i + 1, now - (59 - i) * 1000));
    }
    const responses = await Promise.all(posts);
    expect(responses.every((r) => r.statusCode === 200)).toBe(true);

    const res = await fastify.inject({ method: 'GET', url: '/devices/dev_e2e/health' });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.last_heartbeat_ts).toBe(new Date(now - 1000).toISOString()); // second 59 was skipped
    expect(body.availability_5m).toBeCloseTo(50 / 60, 1);
  });

  it('adds a new device without any configuration', async () => {
    await heartbeat('dev_5001', 1, Date.now());
    const res = await fastify.inject({ method: 'GET', url: '/devices/dev_5001/health' });
    expect(res.statusCode).toBe(200);
    expect(res.json().availability_5m).toBe(1);
  });

  it('returns 404 for an unknown device', async () => {
    const res = await fastify.inject({ method: 'GET', url: '/devices/dev_nope/health' });
    expect(res.statusCode).toBe(404);
  });

  it('does not apply rejected events', async () => {
    await heartbeat('dev_future', 1, Date.now() + 2 * 3600_000); // > 1h ahead: 400
    const res = await fastify.inject({ method: 'GET', url: '/devices/dev_future/health' });
    expect(res.statusCode).toBe(404);
  });
});
