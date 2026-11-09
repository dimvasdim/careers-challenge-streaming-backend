import pg from 'pg';
import type { IngestEvent } from '../src/core/event.js';
import { lockAlarms } from '../src/db/alarms-repository.js';
import { commitBatch } from '../src/db/batch-commit.js';

// commitBatch against real Postgres: events and alarms in one transaction,
// and concurrent transactions (as with several worker processes).

const T0 = Date.parse('2026-05-23T18:00:00.000Z');
const SEC = 1000;

let seq = 0;
const fall = (
  tsMs: number,
  deviceId = 'dev_0001',
  confidence = 0.8,
): IngestEvent => ({
  type: 'fall_warn',
  confidence,
  deviceId,
  roomId: 'room_000',
  seq: ++seq,
  tsMs,
  receivedAtMs: tsMs,
});

describe('commitBatch', () => {
  let db: pg.Pool;
  const count = async (table: string) =>
    (await db.query<{ n: number }>(`SELECT count(*)::int AS n FROM ${table}`))
      .rows[0].n;

  beforeAll(() => {
    db = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 8 });
  });
  afterAll(() => db.end());
  beforeEach(() => db.query('TRUNCATE events, alarms RESTART IDENTITY'));

  it('commits events and the alarms of their falls together', async () => {
    const result = await commitBatch(db, [
      fall(T0),
      fall(T0 + SEC),
      fall(T0, 'dev_0002'),
    ]);
    expect(result.inserted).toHaveLength(3);
    expect(result.alarms.map((a) => [a.id, a.deviceId])).toEqual([
      [1, 'dev_0001'],
      [2, 'dev_0002'],
    ]);
    expect(await count('alarms')).toBe(2);
  });

  it('rolls back events and alarms together when the transaction fails', async () => {
    const broken = { ...fall(T0), roomId: null } as unknown as IngestEvent; // NOT NULL violation
    await expect(
      commitBatch(db, [fall(T0 + SEC, 'dev_0002'), broken]),
    ).rejects.toThrow();
    expect(await count('events')).toBe(0);
    expect(await count('alarms')).toBe(0);
  });

  it('a replayed duplicate batch creates no alarm and reports none', async () => {
    const batch = [fall(T0)];
    await commitBatch(db, batch);
    const again = await commitBatch(db, batch);
    expect(again).toEqual({ inserted: [], alarms: [] });
    expect(await count('alarms')).toBe(1);
  });

  it('hands out alarm ids in commit order across concurrent transactions', async () => {
    // Another writer (think: another worker) holds the alarm lock and has
    // inserted alarm id 1, not yet committed.
    const other = await db.connect();
    await other.query('BEGIN');
    await lockAlarms(other);
    await other.query(
      `INSERT INTO alarms (event_id, device_id, room_id, ts, confidence, received_at)
       VALUES ('dev_a:x', 'dev_a', 'room_000', now(), 0.5, now())`,
    );

    let done = false;
    const pending = commitBatch(db, [fall(T0, 'dev_b')]).then((r) => {
      done = true;
      return r;
    });
    await new Promise((r) => setTimeout(r, 200));
    expect(done).toBe(false); // waits for the lock: can't commit id 2 before id 1

    await other.query('COMMIT');
    other.release();
    const result = await pending;
    expect(result.alarms.map((a) => a.id)).toEqual([2]);
  });

  it('commits concurrent batches sharing duplicates in opposite order without deadlock', async () => {
    for (let round = 0; round < 10; round++) {
      const events = Array.from({ length: 200 }, (_, i) =>
        fall(T0 + round * 60 * SEC + i, `dev_${i % 7}`),
      ).map((e) => ({ ...e, type: 'heartbeat' }) as IngestEvent);
      const results = await Promise.all([
        commitBatch(db, events),
        commitBatch(db, [...events].reverse()),
      ]);
      expect(results[0].inserted.length + results[1].inserted.length).toBe(200);
    }
    expect(await count('events')).toBe(2000);
  });

  it('creates one alarm when copies of a fall commit concurrently (alarm lock)', async () => {
    for (let round = 0; round < 20; round++) {
      const ts = T0 + round * 60 * SEC;
      // Two transactions at once, copies 2s apart (different event ids).
      const results = await Promise.all([
        commitBatch(db, [fall(ts, 'dev_race', 0.8)]),
        commitBatch(db, [fall(ts + 2 * SEC, 'dev_race', 0.9)]),
      ]);
      expect(results.flatMap((r) => r.alarms)).toHaveLength(1);
    }
    expect(await count('alarms')).toBe(20);
    const max = await db.query<{ c: number }>(
      'SELECT min(confidence) AS c FROM alarms',
    );
    expect(max.rows[0].c).toBe(0.9); // each incident kept the higher copy
  });
});
