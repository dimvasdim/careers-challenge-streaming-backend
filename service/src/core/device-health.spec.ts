import { computeHealth, type HealthInput } from './device-health.js';

// Pure math only. Which rows the database selects (clock offset, far-future
// heartbeats, late replays) is covered in test/read-queries.e2e-spec.ts.

const T0 = Date.parse('2026-05-23T18:00:00.000Z');
const SEC = 1000;

/** One heartbeat per second for `seconds` seconds from `fromMs`. */
const everySecond = (fromMs: number, seconds: number) =>
  Array.from({ length: seconds }, (_, i) => fromMs + i * SEC);

const input = (over: Partial<HealthInput>): HealthInput => ({
  firstSeenMs: T0,
  clockOffsetMs: 0,
  lastHeartbeatMs: null,
  heartbeatsMs: [],
  ...over,
});

describe('computeHealth', () => {
  it('is 1.0 for a heartbeat in every second of the window', () => {
    const now = T0 + 599 * SEC;
    expect(
      computeHealth(
        input({
          heartbeatsMs: everySecond(T0 + 300 * SEC, 300),
          lastHeartbeatMs: now,
        }),
        now,
      ),
    ).toEqual({
      last_heartbeat_ts: new Date(now).toISOString(),
      availability_5m: 1,
    });
  });

  it('counts missing seconds against availability', () => {
    const hbs = everySecond(T0, 300).filter((_, i) => i % 3 !== 0);
    expect(
      computeHealth(input({ heartbeatsMs: hbs }), T0 + 299 * SEC)
        .availability_5m,
    ).toBeCloseTo(200 / 300);
  });

  it('does not penalise a device seen for less than 5 minutes', () => {
    const hbs = everySecond(T0, 60);
    expect(
      computeHealth(input({ heartbeatsMs: hbs }), T0 + 59 * SEC)
        .availability_5m,
    ).toBe(1);
  });

  it('counts each second once: duplicates and several heartbeats per second', () => {
    const hbs = everySecond(T0, 300).flatMap((t) => [t, t, t + 400]);
    expect(
      computeHealth(input({ heartbeatsMs: hbs }), T0 + 299 * SEC)
        .availability_5m,
    ).toBe(1);
  });

  it('applies the exact window to a wider input', () => {
    // 600 seconds of heartbeats; only the last 300 are inside the window.
    const hbs = everySecond(T0, 600);
    const now = T0 + 599 * SEC;
    expect(
      computeHealth(input({ heartbeatsMs: hbs }), now).availability_5m,
    ).toBe(1);
    expect(
      computeHealth(input({ heartbeatsMs: hbs }), now + 60 * SEC)
        .availability_5m,
    ).toBeCloseTo(240 / 300);
  });

  it('measures the window on the device clock', () => {
    // Device 30s fast: its last 300 seconds end 30s after server now.
    const skew = 30 * SEC;
    const hbs = everySecond(T0 + skew, 600);
    const now = T0 + 599 * SEC;
    expect(
      computeHealth(
        input({
          firstSeenMs: T0 + skew,
          clockOffsetMs: -skew,
          heartbeatsMs: hbs,
        }),
        now,
      ).availability_5m,
    ).toBe(1);
  });

  it('is 0 with no heartbeats, or when the device clock is ahead of all its events', () => {
    expect(computeHealth(input({}), T0 + 10 * SEC)).toEqual({
      last_heartbeat_ts: null,
      availability_5m: 0,
    });
    // First seen 7 minutes in the future of the device clock: lifetime < 1s.
    expect(
      computeHealth(input({ firstSeenMs: T0 + 7 * 60 * SEC }), T0)
        .availability_5m,
    ).toBe(0);
  });
});
