import pg from 'pg';
import {
  computeHealth,
  MAX_CLOCK_OFFSET_MS,
} from '../src/core/device-health.js';
import type { IngestEvent } from '../src/core/event.js';
import {
  computeOccupancy,
  OCCUPANCY_WINDOWS,
} from '../src/core/room-occupancy.js';
import { insertEvents } from '../src/db/events-repository.js';
import { loadHealthInput, loadPresencePoints } from '../src/db/read-queries.js';

// The read queries against real Postgres, with fixed timestamps: which rows
// they select for late replays, clock skew, broken clocks and carry-in state.

const T0 = Date.parse('2026-05-23T18:00:00.000Z');
const SEC = 1000;
const MIN = 60 * SEC;

let seq = 0;
const hb = (
  tsMs: number,
  receivedAtMs = tsMs,
  deviceId = 'dev_0001',
): IngestEvent => ({
  type: 'heartbeat',
  deviceId,
  roomId: 'room_000',
  seq: ++seq,
  tsMs,
  receivedAtMs,
});
const presence = (
  inRoom: boolean,
  tsMs: number,
  receivedAtMs = tsMs,
  deviceId = 'dev_0001',
): IngestEvent => ({
  type: 'presence',
  inRoom,
  deviceId,
  roomId: 'room_000',
  seq: ++seq,
  tsMs,
  receivedAtMs,
});
/** One heartbeat per second for `seconds` seconds from server time `fromMs`. */
const steady = (
  fromMs: number,
  seconds: number,
  skewMs = 0,
  deviceId = 'dev_0001',
) =>
  Array.from({ length: seconds }, (_, i) =>
    hb(fromMs + i * SEC + skewMs, fromMs + i * SEC, deviceId),
  );

describe('read queries', () => {
  let db: pg.Pool;
  const insert = (events: IngestEvent[]) => insertEvents(db, events);
  const health = async (nowMs: number, deviceId = 'dev_0001') => {
    const input = await loadHealthInput(db, deviceId, nowMs);
    return input && computeHealth(input, nowMs);
  };
  const occupancy = async (
    window: keyof typeof OCCUPANCY_WINDOWS,
    nowMs: number,
  ) => {
    const windowMs = OCCUPANCY_WINDOWS[window];
    return computeOccupancy(
      await loadPresencePoints(db, 'room_000', nowMs - windowMs),
      windowMs,
      nowMs,
    );
  };

  beforeAll(() => {
    db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
  });
  afterAll(() => db.end());
  beforeEach(() => db.query('TRUNCATE events RESTART IDENTITY'));

  describe('device health', () => {
    it('returns undefined for a device never seen', async () => {
      expect(await health(T0)).toBeUndefined();
    });

    it('is 1.0 for a steady 1 Hz device', async () => {
      await insert(steady(T0, 600));
      const now = T0 + 599 * SEC;
      expect(await health(now)).toEqual({
        last_heartbeat_ts: new Date(now).toISOString(),
        availability_5m: 1,
      });
    });

    it('drops as a silent device ages, with no new events', async () => {
      await insert(steady(T0, 300));
      const lastSent = T0 + 299 * SEC;
      expect((await health(lastSent + 60 * SEC))!.availability_5m).toBeCloseTo(
        240 / 300,
      );
      expect((await health(lastSent + 400 * SEC))!.availability_5m).toBe(0);
    });

    it('fills a gap when late heartbeats are replayed inside the window', async () => {
      const now = T0 + 299 * SEC;
      await insert(steady(T0, 300).filter((_, i) => i < 100 || i >= 160));
      expect((await health(now))!.availability_5m).toBeCloseTo(240 / 300);
      await insert(
        Array.from({ length: 60 }, (_, i) => hb(T0 + (100 + i) * SEC, now)),
      ); // replay
      expect((await health(now))!.availability_5m).toBe(1);
    });

    it('reports the newest heartbeat ts even when it arrived before older ones', async () => {
      await insert([hb(T0 + 10 * SEC)]);
      await insert([hb(T0 + 5 * SEC, T0 + 11 * SEC)]);
      expect((await health(T0 + 11 * SEC))!.last_heartbeat_ts).toBe(
        new Date(T0 + 10 * SEC).toISOString(),
      );
    });

    it('measures the window on the device clock: a skewed healthy device scores 1.0', async () => {
      await insert(steady(T0, 600, -30 * SEC, 'dev_slow'));
      await insert(steady(T0, 600, 30 * SEC, 'dev_fast'));
      expect((await health(T0 + 599 * SEC, 'dev_slow'))!.availability_5m).toBe(
        1,
      );
      expect((await health(T0 + 599 * SEC, 'dev_fast'))!.availability_5m).toBe(
        1,
      );
    });

    it('does not let a glitched far-future timestamp move the clock or count', async () => {
      await insert(steady(T0, 300));
      const now = T0 + 299 * SEC;
      await insert([hb(now + MAX_CLOCK_OFFSET_MS + 50 * MIN, now)]); // 55 min ahead, accepted
      expect(await health(now)).toEqual({
        last_heartbeat_ts: new Date(now).toISOString(),
        availability_5m: 1,
      });
    });

    it('reports a device whose clock is consistently >5 min fast as unhealthy (documented)', async () => {
      await insert(steady(T0, 300, 7 * MIN));
      expect(await health(T0 + 299 * SEC)).toEqual({
        last_heartbeat_ts: null,
        availability_5m: 0,
      });
    });

    it('still counts a late heartbeat that is far behind its arrival', async () => {
      const now = T0 + 20 * MIN;
      await insert([hb(now, now), hb(now - 200 * SEC, now)]);
      expect((await health(now))!.availability_5m).toBeCloseTo(2 / 201);
    });

    it('starts a device with only non-heartbeat events at 0', async () => {
      await insert([{ ...hb(T0), type: 'motion', magnitude: 0.5 }]);
      expect(await health(T0 + 10 * SEC)).toEqual({
        last_heartbeat_ts: null,
        availability_5m: 0,
      });
    });
  });

  describe('room occupancy', () => {
    it('is absent for a room without presence events', async () => {
      await insert([hb(T0)]);
      expect(await occupancy('1m', T0)).toEqual({
        in_room: false,
        occupied_pct: 0,
      });
    });

    it('lets a late offline replay fix up the history', async () => {
      const now = T0 + 20 * MIN;
      await insert([presence(false, T0)]);
      expect((await occupancy('1h', now)).occupied_pct).toBe(0);
      await insert([
        presence(true, T0 + 5 * MIN, now, 'dev_0002'),
        presence(false, T0 + 15 * MIN, now, 'dev_0002'),
      ]);
      expect(await occupancy('1h', now)).toEqual({
        in_room: false,
        occupied_pct: 10 / 60,
      });
    });

    it('merges the devices of a room: the latest event by ts wins', async () => {
      await insert([presence(true, T0, T0, 'dev_0000')]);
      await insert([presence(false, T0 + 30 * SEC, T0 + 30 * SEC, 'dev_0001')]);
      expect(await occupancy('1m', T0 + MIN)).toEqual({
        in_room: false,
        occupied_pct: 0.5,
      });
    });

    it('uses the latest event by ts for in_room, even if it arrived first', async () => {
      const now = T0 + 10 * MIN;
      await insert([presence(false, T0 + 5 * MIN, now)]);
      await insert([presence(true, T0 + MIN, now)]);
      expect((await occupancy('1h', now)).in_room).toBe(false);
    });

    it('keeps the state of a room quiet for hours (carry-in)', async () => {
      await insert([presence(false, T0 - 30 * MIN), presence(true, T0)]);
      expect(await occupancy('1h', T0 + 5 * 60 * MIN)).toEqual({
        in_room: true,
        occupied_pct: 1,
      });
    });

    it('takes the newest point before the window as carry-in, not an older one', async () => {
      await insert([
        presence(true, T0 - 3 * 60 * MIN),
        presence(false, T0 - 2 * 60 * MIN),
      ]);
      expect(await occupancy('1h', T0)).toEqual({
        in_room: false,
        occupied_pct: 0,
      });
    });

    it('breaks a ts tie by commit order', async () => {
      await insert([presence(true, T0, T0, 'dev_0000')]);
      await insert([presence(false, T0, T0, 'dev_0001')]); // same ts, committed later
      expect((await occupancy('1m', T0 + 30 * SEC)).in_room).toBe(false);
    });

    it('evaluates every point in an hour of a busy room', async () => {
      await insert(
        Array.from({ length: 180 }, (_, m) =>
          presence(m % 2 === 0, T0 + m * MIN),
        ),
      );
      expect((await occupancy('1h', T0 + 180 * MIN)).occupied_pct).toBeCloseTo(
        0.5,
      );
    });
  });
});
