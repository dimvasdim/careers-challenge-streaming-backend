import pg from 'pg';
import type { IngestEvent } from '../src/core/event.js';
import { insertEvents } from '../src/db/events-repository.js';

const T0 = Date.parse('2026-05-23T18:53:49.123Z');
const envelope = { deviceId: 'dev_0001', roomId: 'room_000', receivedAtMs: T0 + 500 };

const allTypes: IngestEvent[] = [
  { ...envelope, seq: 1, tsMs: T0, type: 'heartbeat' },
  { ...envelope, seq: 2, tsMs: T0 + 1, type: 'presence', inRoom: true },
  { ...envelope, seq: 3, tsMs: T0 + 2, type: 'motion', magnitude: 0.81 },
  { ...envelope, seq: 4, tsMs: T0 + 3, type: 'sleep_state', state: 'asleep' },
  { ...envelope, seq: 5, tsMs: T0 + 4, type: 'fall_warn', confidence: 0.92 },
  { ...envelope, seq: 6, tsMs: T0 + 5, type: 'net_status', rssi: -68 },
];

describe('insertEvents', () => {
  let pool: pg.Pool;

  beforeAll(() => {
    pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
  });
  afterAll(() => pool.end());
  beforeEach(() => pool.query('TRUNCATE events RESTART IDENTITY'));

  it('round-trips every event type with millisecond timestamps', async () => {
    const inserted = await insertEvents(pool, allTypes);
    expect(inserted.map((r) => r.seq)).toEqual([1, 2, 3, 4, 5, 6]);
    expect(inserted[0]).toEqual({ id: 1, deviceId: 'dev_0001', seq: 1, tsMs: T0 });

    const { rows } = await pool.query(
      'SELECT type, ts, received_at, in_room, magnitude, state, confidence, rssi FROM events ORDER BY id',
    );
    expect(rows.map((r) => (r.ts as Date).getTime())).toEqual(allTypes.map((e) => e.tsMs));
    expect((rows[0].received_at as Date).getTime()).toBe(T0 + 500);
    expect(rows[1].in_room).toBe(true);
    expect(rows[2].magnitude).toBe(0.81);
    expect(rows[3].state).toBe('asleep');
    expect(rows[4].confidence).toBe(0.92);
    expect(rows[5].rssi).toBe(-68);
    expect(rows[0]).toMatchObject({ in_room: null, magnitude: null, confidence: null });
  });

  it('returns only new rows for duplicates within and across batches', async () => {
    const [a, b, c] = allTypes;
    expect(await insertEvents(pool, [a, a, b])).toHaveLength(2);
    const second = await insertEvents(pool, [b, c, a]);
    expect(second.map((r) => r.seq)).toEqual([3]);

    const { rows } = await pool.query('SELECT count(*)::int AS n FROM events');
    expect(rows[0].n).toBe(3);
  });

  it('keeps the same (device, seq) with a different ts as a distinct row', async () => {
    const a = allTypes[0];
    const inserted = await insertEvents(pool, [a, { ...a, tsMs: a.tsMs + 1 }]);
    expect(inserted).toHaveLength(2);
  });

  it('assigns increasing ids in batch order', async () => {
    const first = await insertEvents(pool, allTypes.slice(0, 3));
    const second = await insertEvents(pool, allTypes.slice(3));
    const ids = [...first, ...second].map((r) => r.id);
    expect(ids).toEqual([...ids].sort((x, y) => x - y));
  });
});
