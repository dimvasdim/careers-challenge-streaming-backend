import {
  computeOccupancy,
  isOccupancyWindow,
  OCCUPANCY_WINDOWS,
  type PresencePoint,
} from './room-occupancy.js';

// Pure math only. Which points the database selects (carry-in, late events,
// several devices, eviction-free history) is covered in test/read-queries.e2e-spec.ts.

const T0 = Date.parse('2026-05-23T18:00:00.000Z');
const SEC = 1000;
const MIN = 60 * SEC;
const { '1m': W1M, '5m': W5M, '1h': W1H } = OCCUPANCY_WINDOWS;

const p = (inRoom: boolean, tsMs: number): PresencePoint => ({ tsMs, inRoom });

describe('computeOccupancy', () => {
  it('is absent with no points', () => {
    expect(computeOccupancy([], W1M, T0)).toEqual({
      in_room: false,
      occupied_pct: 0,
    });
  });

  it('extends the latest state to now', () => {
    expect(computeOccupancy([p(true, T0)], W1M, T0 + 2 * MIN)).toEqual({
      in_room: true,
      occupied_pct: 1,
    });
  });

  it('counts the room as absent before its first known point', () => {
    expect(
      computeOccupancy([p(true, T0 + 30 * SEC)], W1M, T0 + MIN).occupied_pct,
    ).toBeCloseTo(0.5);
  });

  it('sums present intervals clipped to the window', () => {
    const points = [p(true, T0), p(false, T0 + 2 * MIN), p(true, T0 + 4 * MIN)];
    const now = T0 + 5 * MIN;
    expect(computeOccupancy(points, W5M, now).occupied_pct).toBeCloseTo(3 / 5);
    expect(computeOccupancy(points, W1M, now).occupied_pct).toBe(1);
    expect(computeOccupancy(points, W1H, now).occupied_pct).toBeCloseTo(3 / 60);
  });

  it('uses a carry-in point from before the window start', () => {
    // Present since 3 hours ago: the whole hour counts.
    expect(computeOccupancy([p(true, T0 - 3 * 60 * MIN)], W1H, T0)).toEqual({
      in_room: true,
      occupied_pct: 1,
    });
  });

  it('handles a point exactly at the window start', () => {
    const now = T0 + MIN;
    const points = [p(true, now - W1M), p(false, now - W1M / 2)];
    expect(computeOccupancy(points, W1M, now).occupied_pct).toBeCloseTo(0.5);
  });

  it('tolerates exact copies and repeated states', () => {
    const points = [
      p(true, T0),
      p(true, T0),
      p(false, T0 + 10 * SEC),
      p(true, T0 + 20 * SEC),
    ];
    // Present 0–10 and 20–30 of [now−1m, now].
    expect(
      computeOccupancy(points, W1M, T0 + 30 * SEC).occupied_pct,
    ).toBeCloseTo(20 / 60);
  });

  it('ignores a future point for the window but reports it as in_room', () => {
    // A clock 55 minutes fast: the point lies after now.
    const now = T0 + 30 * MIN;
    const points = [p(true, T0), p(false, now + 55 * MIN)];
    expect(computeOccupancy(points, W1H, now)).toEqual({
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
