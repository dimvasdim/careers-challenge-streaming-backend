import type { IngestEvent } from './event.js';

export const OCCUPANCY_WINDOWS = {
  '1m': 60_000,
  '5m': 5 * 60_000,
  '1h': 60 * 60_000,
} as const;
export type OccupancyWindow = keyof typeof OCCUPANCY_WINDOWS;

/** History kept per room: the longest window. */
export const HISTORY_MS = OCCUPANCY_WINDOWS['1h'];

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
 * Per-room occupancy, computed on read.
 *
 * Clock roles: `ts` decides ORDER (always); server time only answers "what time
 * is it now" (where a window ends, what is too old to keep). Nothing is ordered
 * by arrival, so a late replayed event lands in its `ts` position and fixes up
 * the history.
 *
 * Each room has one timeline of presence points from all its devices, sorted by
 * `ts` ("latest presence event wins"). Device `ts` is used as-is: a room mixes
 * devices with different drift, so unlike device health there is no single
 * device clock to shift the window to; windows end at server now.
 */
export class RoomOccupancyStore {
  /** roomId → presence points sorted by tsMs (ties keep commit order). */
  private readonly rooms = new Map<string, PresencePoint[]>();

  apply(event: IngestEvent): void {
    let points = this.rooms.get(event.roomId);
    if (!points) {
      // Any event makes the room known; it counts as absent until a presence event.
      points = [];
      this.rooms.set(event.roomId, points);
    }
    if (event.type !== 'presence') return;

    insertPoint(points, { tsMs: event.tsMs, inRoom: event.inRoom });
    // Eviction uses server time (the event's arrival), not the room's newest ts.
    // Reads use [serverNow − 1h, serverNow] and server time only moves forward,
    // so a point older than receivedAt − 1h can never be inside a future window
    // (except as carry-in). The newest ts would be unsafe: one accepted event with
    // a clock up to 1h fast would evict almost the whole past hour.
    evictBefore(points, event.receivedAtMs - HISTORY_MS);
  }

  /** Occupancy over [nowMs − windowMs, nowMs], or undefined for a room never seen. */
  getOccupancy(
    roomId: string,
    windowMs: number,
    nowMs: number,
  ): RoomOccupancy | undefined {
    const points = this.rooms.get(roomId);
    if (!points) return undefined;
    if (points.length === 0) return { in_room: false, occupied_pct: 0 };

    const windowStart = nowMs - windowMs;
    let occupiedMs = 0;
    for (let i = 0; i < points.length; i++) {
      if (!points[i].inRoom) continue;
      // Present from this point until the next one; the latest state extends to now.
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
}

/**
 * Inserts by ts (binary search, after any equal ts). An exact copy (same ts and
 * same state) is skipped. Equal consecutive states are NOT merged: a late
 * opposite event may still land between them.
 */
function insertPoint(points: PresencePoint[], point: PresencePoint): void {
  // First index whose ts is greater than the new point's ts.
  let lo = 0;
  let hi = points.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (points[mid].tsMs <= point.tsMs) lo = mid + 1;
    else hi = mid;
  }
  for (let i = lo - 1; i >= 0 && points[i].tsMs === point.tsMs; i--) {
    if (points[i].inRoom === point.inRoom) return;
  }
  points.splice(lo, 0, point);
}

/** Drops points before `cutoffMs`, keeping the newest of them as carry-in state. */
function evictBefore(points: PresencePoint[], cutoffMs: number): void {
  let drop = 0;
  while (drop + 1 < points.length && points[drop + 1].tsMs <= cutoffMs) drop++;
  if (drop > 0) points.splice(0, drop);
}
