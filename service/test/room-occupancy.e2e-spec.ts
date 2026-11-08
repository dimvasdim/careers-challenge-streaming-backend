import type { NestFastifyApplication } from '@nestjs/platform-fastify';
import type { FastifyInstance } from 'fastify';
import pg from 'pg';
import { startApp } from './app.js';

describe('GET /rooms/:id/occupancy', () => {
  let db: pg.Pool;
  let app: NestFastifyApplication;
  let fastify: FastifyInstance;

  const presence = (
    roomId: string,
    deviceId: string,
    seq: number,
    tsMs: number,
    inRoom: boolean,
  ) =>
    fastify.inject({
      method: 'POST',
      url: '/events',
      payload: {
        device_id: deviceId,
        room_id: roomId,
        type: 'presence',
        ts: new Date(tsMs).toISOString(),
        seq,
        in_room: inRoom,
      },
    });

  const occupancy = (roomId: string, window: string) =>
    fastify.inject({
      method: 'GET',
      url: `/rooms/${roomId}/occupancy?window=${window}`,
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

  it('reports in_room and the occupied share of the window', async () => {
    const now = Date.now();
    expect(
      (await presence('room_e2e', 'dev_a', 1, now - 60_000, true)).statusCode,
    ).toBe(200);
    expect(
      (await presence('room_e2e', 'dev_b', 1, now - 30_000, false)).statusCode,
    ).toBe(200);

    const res = await occupancy('room_e2e', '1m');
    expect(res.statusCode).toBe(200);
    expect(res.json().in_room).toBe(false);
    expect(res.json().occupied_pct).toBeCloseTo(0.5, 1);
  });

  it('fixes up history when a late event arrives after later ones', async () => {
    const now = Date.now();
    await presence('room_late', 'dev_a', 1, now - 20 * 60_000, false);
    expect((await occupancy('room_late', '1h')).json().occupied_pct).toBe(0);

    // Offline replay: present from minute -15 to minute -5.
    await presence('room_late', 'dev_b', 1, now - 15 * 60_000, true);
    await presence('room_late', 'dev_b', 2, now - 5 * 60_000, false);

    const body = (await occupancy('room_late', '1h')).json();
    expect(body.in_room).toBe(false);
    expect(body.occupied_pct).toBeCloseTo(10 / 60, 2);
  });

  it('returns 400 for a missing or unknown window', async () => {
    await presence('room_e2e', 'dev_a', 1, Date.now(), true);
    expect((await occupancy('room_e2e', '10m')).statusCode).toBe(400);
    const res = await fastify.inject({
      method: 'GET',
      url: '/rooms/room_e2e/occupancy',
    });
    expect(res.statusCode).toBe(400);
  });

  it('returns 404 for an unknown room', async () => {
    expect((await occupancy('room_nope', '1m')).statusCode).toBe(404);
  });
});
