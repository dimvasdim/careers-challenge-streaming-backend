import type { IngestEvent } from './event.js';
import {
  OCCUPANCY_WINDOWS,
  RoomOccupancyStore,
  isOccupancyWindow,
} from './room-occupancy.js';

const T0 = Date.parse('2026-05-23T18:00:00.000Z');
const SEC = 1000;
const MIN = 60 * SEC;
const HOUR = 60 * MIN;
const { '1m': W1M, '5m': W5M, '1h': W1H } = OCCUPANCY_WINDOWS;

let seq = 0;
/** A presence event with device time `tsMs`, received at server time `receivedAtMs`. */
const presence = (
  inRoom: boolean,
  tsMs: number,
  receivedAtMs = tsMs,
  deviceId = 'dev_0001',
  roomId = 'room_000',
): IngestEvent => ({
  type: 'presence',
  inRoom,
  deviceId,
  roomId,
  seq: ++seq,
  tsMs,
  receivedAtMs,
});

describe('RoomOccupancyStore', () => {
  let store: RoomOccupancyStore;
  beforeEach(() => {
    store = new RoomOccupancyStore();
  });

  it('returns undefined for a room never seen', () => {
    expect(store.getOccupancy('nope', W1M, T0)).toBeUndefined();
  });

  it('knows a room from any event and treats it as absent until presence arrives', () => {
    store.apply({
      type: 'heartbeat',
      deviceId: 'd',
      roomId: 'room_hb',
      seq: 1,
      tsMs: T0,
      receivedAtMs: T0,
    });
    expect(store.getOccupancy('room_hb', W1M, T0)).toEqual({
      in_room: false,
      occupied_pct: 0,
    });
  });

  it('extends the latest state to now', () => {
    store.apply(presence(true, T0));
    expect(store.getOccupancy('room_000', W1M, T0 + 2 * MIN)).toEqual({
      in_room: true,
      occupied_pct: 1,
    });
  });

  it('counts the room as absent before its first known event', () => {
    store.apply(presence(true, T0 + 30 * SEC));
    expect(
      store.getOccupancy('room_000', W1M, T0 + MIN)?.occupied_pct,
    ).toBeCloseTo(0.5);
  });

  it('sums present intervals clipped to the window', () => {
    store.apply(presence(true, T0));
    store.apply(presence(false, T0 + 2 * MIN));
    store.apply(presence(true, T0 + 4 * MIN));
    const now = T0 + 5 * MIN;
    // 5m: present 0–2 and 4–5 → 3/5. 1m: present 4–5 → 1.
    expect(store.getOccupancy('room_000', W5M, now)?.occupied_pct).toBeCloseTo(
      3 / 5,
    );
    expect(store.getOccupancy('room_000', W1M, now)?.occupied_pct).toBe(1);
    // 1h: only the 3 known-present minutes count.
    expect(store.getOccupancy('room_000', W1H, now)?.occupied_pct).toBeCloseTo(
      3 / 60,
    );
  });

  it('handles a point exactly at the window start', () => {
    const now = T0 + MIN;
    store.apply(presence(true, now - W1M));
    store.apply(presence(false, now - W1M / 2));
    expect(store.getOccupancy('room_000', W1M, now)?.occupied_pct).toBeCloseTo(
      0.5,
    );
  });

  it('uses the latest event by ts for in_room, even if it arrived first', () => {
    const now = T0 + 10 * MIN;
    store.apply(presence(false, T0 + 5 * MIN, now));
    store.apply(presence(true, T0 + 1 * MIN, now)); // late, older
    expect(store.getOccupancy('room_000', W1H, now)?.in_room).toBe(false);
  });

  it('lets a late offline replay fix up the history', () => {
    // Live: room went absent at T0 and stayed so for 20 minutes.
    store.apply(presence(false, T0));
    const now = T0 + 20 * MIN;
    expect(store.getOccupancy('room_000', W1H, now)?.occupied_pct).toBe(0);
    // An offline device replays: someone was present from minute 5 to minute 15.
    store.apply(presence(true, T0 + 5 * MIN, now, 'dev_0002'));
    store.apply(presence(false, T0 + 15 * MIN, now, 'dev_0002'));
    expect(store.getOccupancy('room_000', W1H, now)?.occupied_pct).toBeCloseTo(
      10 / 60,
    );
    expect(store.getOccupancy('room_000', W1H, now)?.in_room).toBe(false);
  });

  it('merges devices of one room: the latest event wins', () => {
    store.apply(presence(true, T0, T0, 'dev_0000'));
    store.apply(presence(false, T0 + 30 * SEC, T0 + 30 * SEC, 'dev_0001'));
    const now = T0 + MIN;
    expect(store.getOccupancy('room_000', W1M, now)).toEqual({
      in_room: false,
      occupied_pct: 0.5,
    });
  });

  it('ignores an exact duplicate and tolerates repeated states', () => {
    store.apply(presence(true, T0));
    store.apply(presence(true, T0)); // exact copy
    store.apply(presence(true, T0 + 20 * SEC)); // repeated state
    // A late opposite event between the two "present" readings still lands.
    store.apply(presence(false, T0 + 10 * SEC, T0 + MIN));
    const now = T0 + 30 * SEC;
    // Present 0–10 and 20–30 of [now−1m, now] → 20s / 60s.
    expect(store.getOccupancy('room_000', W1M, now)?.occupied_pct).toBeCloseTo(
      20 / 60,
    );
  });

  it('keeps one carry-in point for a room quiet for hours', () => {
    store.apply(presence(false, T0 - 30 * MIN));
    store.apply(presence(true, T0));
    const later = T0 + 5 * HOUR;
    // A late replay (older than the 1h window) triggers eviction: the points before
    // the cutoff collapse to the newest one, which survives as carry-in state.
    store.apply(presence(true, later - 2 * HOUR, later));
    expect(store.getOccupancy('room_000', W1H, later)).toEqual({
      in_room: true,
      occupied_pct: 1,
    });
  });

  it('evicts points older than 1h but keeps the room state correct', () => {
    for (let m = 0; m < 180; m++)
      store.apply(presence(m % 2 === 0, T0 + m * MIN));
    const now = T0 + 180 * MIN;
    // Every other minute present across the last hour.
    expect(store.getOccupancy('room_000', W1H, now)?.occupied_pct).toBeCloseTo(
      0.5,
    );
  });

  it('does not let a future-ts event evict history', () => {
    store.apply(presence(true, T0));
    // A broken clock 55 minutes fast, accepted by validation (< 1h ahead).
    store.apply(presence(false, T0 + 30 * MIN + 55 * MIN, T0 + 30 * MIN));
    const now = T0 + 30 * MIN;
    // The present stretch from T0 to now is still there; the future point is
    // ignored until server time reaches it, but it is the newest by ts.
    expect(store.getOccupancy('room_000', W1H, now)).toEqual({
      in_room: false,
      occupied_pct: 0.5,
    });
  });
});

describe('isOccupancyWindow', () => {
  it('accepts only 1m, 5m and 1h', () => {
    expect(['1m', '5m', '1h'].every(isOccupancyWindow)).toBe(true);
    expect(['', '10m', 'toString', undefined].some(isOccupancyWindow)).toBe(
      false,
    );
  });
});
