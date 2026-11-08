import type pg from 'pg';
import {
  HEALTH_WINDOW_SEC,
  type HealthInput,
  MAX_CLOCK_OFFSET_MS,
} from '../core/device-health.js';
import type { PresencePoint } from '../core/room-occupancy.js';

// Read endpoints query Postgres directly; it is the only source of truth.
// SQL selects the rows, the pure functions in core/ do the math.

// $1 device id, $2 server now, $3 max sane clock offset (s), $4 window (s).
// Every subquery is one index range on events (device_id, ts, seq).
const DEVICE_HEALTH = `
  WITH clock AS (
    -- Clock offset from the newest event whose clock is sane.
    SELECT received_at - ts AS clock_offset
    FROM events
    WHERE device_id = $1
      AND received_at - ts BETWEEN -make_interval(secs => $3) AND make_interval(secs => $3)
    ORDER BY ts DESC
    LIMIT 1
  ),
  device_now AS (
    SELECT $2::timestamptz - coalesce((SELECT clock_offset FROM clock), interval '0') AS t
  )
  SELECT
    (SELECT ts FROM events WHERE device_id = $1 ORDER BY ts LIMIT 1) AS first_seen,
    (SELECT extract(epoch FROM clock_offset) * 1000 FROM clock) AS clock_offset_ms,
    -- Heartbeats far ahead of their own arrival are broken clock readings.
    (SELECT ts FROM events
      WHERE device_id = $1 AND type = 'heartbeat' AND ts <= received_at + make_interval(secs => $3)
      ORDER BY ts DESC
      LIMIT 1) AS last_heartbeat,
    -- A little wider than the window; computeHealth applies the exact seconds.
    ARRAY(
      SELECT (extract(epoch FROM e.ts) * 1000)::bigint
      FROM events e, device_now
      WHERE e.device_id = $1 AND e.type = 'heartbeat'
        AND e.ts <= e.received_at + make_interval(secs => $3)
        AND e.ts > device_now.t - make_interval(secs => $4 + 1)
        AND e.ts < device_now.t + interval '1 second'
    ) AS heartbeats`;

/** Inputs for computeHealth, or undefined for a device that never sent an event. */
export async function loadHealthInput(
  db: pg.Pool,
  deviceId: string,
  nowMs: number,
): Promise<HealthInput | undefined> {
  const { rows } = await db.query<{
    first_seen: Date | null;
    clock_offset_ms: string | null;
    last_heartbeat: Date | null;
    heartbeats: string[];
  }>(DEVICE_HEALTH, [
    deviceId,
    new Date(nowMs).toISOString(),
    MAX_CLOCK_OFFSET_MS / 1000,
    HEALTH_WINDOW_SEC,
  ]);
  const row = rows[0];
  if (!row.first_seen) return undefined;
  return {
    firstSeenMs: row.first_seen.getTime(),
    clockOffsetMs:
      row.clock_offset_ms === null ? 0 : Number(row.clock_offset_ms),
    lastHeartbeatMs: row.last_heartbeat?.getTime() ?? null,
    heartbeatsMs: row.heartbeats.map(Number),
  };
}

// $1 room id, $2 window start. Both halves are index-only scans on the covering
// partial index events_presence_room_ts_idx. Ties on ts keep commit order (id).
const PRESENCE_POINTS = `
  SELECT ts, in_room FROM (
    (SELECT ts, in_room, id FROM events
      WHERE room_id = $1 AND type = 'presence' AND ts < $2
      ORDER BY ts DESC, id DESC
      LIMIT 1)                 -- carry-in: the state at the window start
    UNION ALL
    (SELECT ts, in_room, id FROM events
      WHERE room_id = $1 AND type = 'presence' AND ts >= $2)
  ) AS points
  ORDER BY ts, id`;

/** Presence points of a room from `windowStartMs` on, plus the carry-in point. */
export async function loadPresencePoints(
  db: pg.Pool,
  roomId: string,
  windowStartMs: number,
): Promise<PresencePoint[]> {
  const { rows } = await db.query<{ ts: Date; in_room: boolean }>(
    PRESENCE_POINTS,
    [roomId, new Date(windowStartMs).toISOString()],
  );
  return rows.map((r) => ({ tsMs: r.ts.getTime(), inRoom: r.in_room }));
}
