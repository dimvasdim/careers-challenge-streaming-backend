import type { NestFastifyApplication } from '@nestjs/platform-fastify';
import type { FastifyInstance } from 'fastify';
import pg from 'pg';
import { startApp } from './app.js';
import { openStream } from './sse-client.js';

// Two app instances on one database stand in for two worker processes:
// an alarm committed by one reaches SSE subscribers of the other.

interface Instance {
  app: NestFastifyApplication;
  fastify: FastifyInstance;
  url: string;
}

async function boot(): Promise<Instance> {
  const { app, fastify } = await startApp();
  await app.listen(0, '127.0.0.1');
  return { app, fastify, url: await app.getUrl() };
}

describe('alarm fan-out across processes', () => {
  let db: pg.Pool;
  let a: Instance;
  let b: Instance;
  let seq = 0;

  const fall = (target: Instance, deviceId: string, tsMs: number) =>
    target.fastify.inject({
      method: 'POST',
      url: '/events',
      payload: {
        device_id: deviceId,
        room_id: 'room_fan',
        type: 'fall_warn',
        ts: new Date(tsMs).toISOString(),
        seq: ++seq,
        confidence: 0.9,
      },
    });

  beforeAll(() => {
    db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
  });
  afterAll(() => db.end());

  beforeEach(async () => {
    await db.query('TRUNCATE events, alarms RESTART IDENTITY');
    [a, b] = await Promise.all([boot(), boot()]);
  });
  afterEach(() => Promise.all([a.app.close(), b.app.close()]));

  it('pushes an alarm ingested by instance A to a subscriber of instance B', async () => {
    const stream = await openStream(b.url);
    const res = await fall(a, 'dev_x', Date.now() - 1000);
    expect(res.statusCode).toBe(200);

    const [msg] = await stream.waitFor(1);
    expect(msg.id).toBe('1');
    expect(JSON.parse(msg.data)).toMatchObject({ room_id: 'room_fan', confidence: 0.9 });
    stream.close();
  });

  it('delivers alarms from both instances to every subscriber, in id order, once', async () => {
    const onA = await openStream(a.url);
    const onB = await openStream(b.url);
    const now = Date.now();
    // Different devices, so each fall is its own alarm.
    await Promise.all(
      Array.from({ length: 10 }, (_, i) => fall(i % 2 ? a : b, `dev_${i}`, now - 1000)),
    );

    const ids = (msgs: Array<{ id?: string }>) => msgs.map((m) => Number(m.id));
    const expected = Array.from({ length: 10 }, (_, i) => i + 1);
    expect(ids(await onA.waitFor(10))).toEqual(expected);
    expect(ids(await onB.waitFor(10))).toEqual(expected);
    await new Promise((r) => setTimeout(r, 100));
    expect(onA.messages).toHaveLength(10); // no doubles
    onA.close();
    onB.close();
  });

  it('catches up from Last-Event-ID on the other instance', async () => {
    const now = Date.now();
    await fall(a, 'dev_1', now - 3000);
    await fall(a, 'dev_2', now - 2000);
    await fall(a, 'dev_3', now - 1000);

    const stream = await openStream(b.url, { 'Last-Event-ID': '1' });
    const msgs = await stream.waitFor(2);
    expect(msgs.map((m) => m.id)).toEqual(['2', '3']);
    stream.close();
  });
});
