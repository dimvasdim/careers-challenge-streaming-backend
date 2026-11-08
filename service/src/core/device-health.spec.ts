import { DeviceHealthStore, MAX_CLOCK_OFFSET_MS } from './device-health.js';
import type { IngestEvent } from './event.js';

const T0 = Date.parse('2026-05-23T18:00:00.000Z'); // server time at the start of each test
const SEC = 1000;

let seq = 0;
/** A heartbeat with device time `tsMs`, received at server time `receivedAtMs`. */
const hb = (tsMs: number, receivedAtMs = tsMs, deviceId = 'dev_0001'): IngestEvent => ({
  type: 'heartbeat',
  deviceId,
  roomId: 'room_000',
  seq: ++seq,
  tsMs,
  receivedAtMs,
});

/** One heartbeat per second for `seconds` seconds starting at `fromMs`. */
function steady(store: DeviceHealthStore, fromMs: number, seconds: number, skewMs = 0) {
  for (let i = 0; i < seconds; i++) {
    const server = fromMs + i * SEC;
    store.apply(hb(server + skewMs, server));
  }
}

describe('DeviceHealthStore', () => {
  it('returns undefined for a device never seen', () => {
    expect(new DeviceHealthStore().getHealth('nope', T0)).toBeUndefined();
  });

  it('is 1.0 for a steady 1 Hz device over the full window', () => {
    const store = new DeviceHealthStore();
    steady(store, T0, 600);
    const now = T0 + 599 * SEC;
    expect(store.getHealth('dev_0001', now)).toEqual({
      last_heartbeat_ts: new Date(now).toISOString(),
      availability_5m: 1,
    });
  });

  it('counts missing seconds against availability', () => {
    const store = new DeviceHealthStore();
    for (let i = 0; i < 300; i++) {
      if (i % 3 !== 0) store.apply(hb(T0 + i * SEC)); // 2 of every 3 seconds
    }
    const health = store.getHealth('dev_0001', T0 + 299 * SEC)!;
    expect(health.availability_5m).toBeCloseTo(200 / 300);
  });

  it('does not penalise a device seen for less than 5 minutes', () => {
    const store = new DeviceHealthStore();
    steady(store, T0, 60);
    expect(store.getHealth('dev_0001', T0 + 59 * SEC)!.availability_5m).toBe(1);
  });

  it('drops as a silent device ages, even though no new events arrive', () => {
    const store = new DeviceHealthStore();
    steady(store, T0, 300);
    const lastSent = T0 + 299 * SEC;
    expect(store.getHealth('dev_0001', lastSent)!.availability_5m).toBe(1);
    expect(store.getHealth('dev_0001', lastSent + 60 * SEC)!.availability_5m).toBeCloseTo(240 / 300);
    expect(store.getHealth('dev_0001', lastSent + 400 * SEC)!.availability_5m).toBe(0);
  });

  it('counts each second once: duplicates and several heartbeats per second', () => {
    const store = new DeviceHealthStore();
    for (let i = 0; i < 300; i++) {
      const t = T0 + i * SEC;
      store.apply(hb(t));
      store.apply(hb(t)); // replayed duplicate
      store.apply(hb(t + 400)); // second heartbeat in the same second (burst)
    }
    expect(store.getHealth('dev_0001', T0 + 299 * SEC)!.availability_5m).toBe(1);
  });

  it('fills a gap when late heartbeats are replayed inside the window', () => {
    const store = new DeviceHealthStore();
    const now = T0 + 299 * SEC;
    for (let i = 0; i < 300; i++) if (i < 100 || i >= 160) store.apply(hb(T0 + i * SEC, T0 + i * SEC));
    expect(store.getHealth('dev_0001', now)!.availability_5m).toBeCloseTo(240 / 300);

    for (let i = 100; i < 160; i++) store.apply(hb(T0 + i * SEC, now)); // offline buffer replay
    expect(store.getHealth('dev_0001', now)!.availability_5m).toBe(1);
  });

  it('ignores a late heartbeat older than the window and keeps the newest in its slot', () => {
    const store = new DeviceHealthStore();
    steady(store, T0 + 600 * SEC, 300);
    const now = T0 + 899 * SEC;
    store.apply(hb(T0 + 600 * SEC - 300 * SEC, now)); // same slot as T0+600s, 5 minutes older
    expect(store.getHealth('dev_0001', now)!.availability_5m).toBe(1);
  });

  it('reports the newest heartbeat ts even when it arrived before older ones', () => {
    const store = new DeviceHealthStore();
    store.apply(hb(T0 + 10 * SEC));
    store.apply(hb(T0 + 5 * SEC, T0 + 11 * SEC)); // late, older
    expect(store.getHealth('dev_0001', T0 + 11 * SEC)!.last_heartbeat_ts).toBe(
      new Date(T0 + 10 * SEC).toISOString(),
    );
  });

  it('measures the window on the device clock, so a skewed healthy device scores 1.0', () => {
    for (const skew of [-30 * SEC, 30 * SEC]) {
      const store = new DeviceHealthStore();
      steady(store, T0, 600, skew);
      expect(store.getHealth('dev_0001', T0 + 599 * SEC)!.availability_5m).toBe(1);
    }
  });

  it('does not let a glitched far-future timestamp move the device clock', () => {
    const store = new DeviceHealthStore();
    steady(store, T0, 300);
    const now = T0 + 299 * SEC;
    store.apply(hb(now + MAX_CLOCK_OFFSET_MS + 50 * 60 * SEC, now)); // 55 min ahead, still accepted
    expect(store.getHealth('dev_0001', now)).toEqual({
      last_heartbeat_ts: new Date(now).toISOString(),
      availability_5m: 1,
    });
  });

  it('reports a device whose clock is consistently >5 min fast as unhealthy (documented)', () => {
    const store = new DeviceHealthStore();
    steady(store, T0, 300, 7 * 60 * SEC);
    expect(store.getHealth('dev_0001', T0 + 299 * SEC)).toEqual({
      last_heartbeat_ts: null,
      availability_5m: 0,
    });
  });

  it('still counts a late replayed heartbeat (far behind arrival, not ahead)', () => {
    const store = new DeviceHealthStore();
    const now = T0 + 20 * 60 * SEC;
    store.apply(hb(now, now));
    store.apply(hb(now - 200 * SEC, now)); // 200s old, arrives now
    expect(store.getHealth('dev_0001', now)!.availability_5m).toBeCloseTo(2 / 201);
  });

  it('starts a device with only non-heartbeat events at 0 with no last heartbeat', () => {
    const store = new DeviceHealthStore();
    store.apply({ ...hb(T0), type: 'motion', magnitude: 0.5 });
    expect(store.getHealth('dev_0001', T0 + 10 * SEC)).toEqual({
      last_heartbeat_ts: null,
      availability_5m: 0,
    });
  });

  it('tracks devices independently and adds new ones on first event', () => {
    const store = new DeviceHealthStore();
    steady(store, T0, 300);
    store.apply(hb(T0 + 299 * SEC, T0 + 299 * SEC, 'dev_5001'));
    expect(store.getHealth('dev_0001', T0 + 299 * SEC)!.availability_5m).toBe(1);
    expect(store.getHealth('dev_5001', T0 + 299 * SEC)!.availability_5m).toBe(1);
  });
});
