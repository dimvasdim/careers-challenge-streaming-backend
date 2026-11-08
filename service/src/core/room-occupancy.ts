export const OCCUPANCY_WINDOWS = {
  '1m': 60_000,
  '5m': 5 * 60_000,
  '1h': 60 * 60_000,
} as const;
export type OccupancyWindow = keyof typeof OCCUPANCY_WINDOWS;

export interface RoomOccupancy {
  in_room: boolean;
  occupied_pct: number;
}

/** A presence reading: from `tsMs` on, the room is `inRoom` until the next point. */
export interface PresencePoint {
  tsMs: number;
  inRoom: boolean;
}

export function isOccupancyWindow(value: unknown): value is OccupancyWindow {
  return typeof value === 'string' && Object.hasOwn(OCCUPANCY_WINDOWS, value);
}

/**
 * Occupancy over [nowMs − windowMs, nowMs] from a room's presence points.
 *
 * `points` (see db/read-queries.ts) are sorted by ts (ties in commit order) and
 * hold every point from the window start on, plus the newest point before it
 * (carry-in state). They come from all devices of the room: "latest presence
 * event wins". Device `ts` is used as-is: a room mixes devices with different
 * drift, so there is no single device clock to shift the window to; windows
 * end at server now.
 *
 * Present intervals run from a point to the next one; the latest state extends
 * to now. Before the first known point the room counts as absent. Exact copies
 * and repeated states need no special handling: they only add empty intervals
 * or split one interval in two.
 */
export function computeOccupancy(
  points: PresencePoint[],
  windowMs: number,
  nowMs: number,
): RoomOccupancy {
  if (points.length === 0) return { in_room: false, occupied_pct: 0 };

  const windowStart = nowMs - windowMs;
  let occupiedMs = 0;
  for (let i = 0; i < points.length; i++) {
    if (!points[i].inRoom) continue;
    const from = Math.max(points[i].tsMs, windowStart);
    const to = Math.min(
      i + 1 < points.length ? points[i + 1].tsMs : nowMs,
      nowMs,
    );
    if (to > from) occupiedMs += to - from;
  }

  return {
    in_room: points[points.length - 1].inRoom,
    occupied_pct: Math.min(1, occupiedMs / windowMs),
  };
}
