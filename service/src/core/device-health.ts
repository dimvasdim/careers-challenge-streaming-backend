export const HEALTH_WINDOW_SEC = 300; // availability over the last 5 minutes
// Clock offsets beyond ±5 min (10x the documented ±30s drift) are treated as a
// device fault: such an event never sets the device's clock offset, and a
// heartbeat this far *ahead* of its arrival is kept out of health entirely.
// Late heartbeats (behind arrival, e.g. offline-buffer replays) are unaffected.
//
// Known consequence: a device whose clock is *consistently* more than 5 min fast
// reports availability 0 and no last heartbeat until its clock is fixed. This is
// deliberate: its timestamps would also distort occupancy and alarms, so flagging
// it as unhealthy is the useful signal. Learning a consistent large offset is
// possible but must not be fooled by single glitches or by buffer replays.
export const MAX_CLOCK_OFFSET_MS = 5 * 60 * 1000;

export interface DeviceHealth {
  last_heartbeat_ts: string | null;
  availability_5m: number;
}

/** What the database returns for one device (see db/read-queries.ts). */
export interface HealthInput {
  /** Oldest event ts of the device (any type). */
  firstSeenMs: number;
  /** Arrival − ts of the device's newest event with a sane clock; 0 if none. */
  clockOffsetMs: number;
  /** Newest heartbeat ts that is not far ahead of its arrival, or null. */
  lastHeartbeatMs: number | null;
  /**
   * Heartbeat ts values around the device's last 5 minutes (a superset is fine:
   * the exact window is applied here), excluding ones far ahead of arrival.
   */
  heartbeatsMs: number[];
}

/**
 * Per-device heartbeat health as of server time `nowMs`.
 *
 * Availability = seconds in the device's last 5 minutes that hold a heartbeat,
 * divided by min(300, seconds since the device was first seen). Each second
 * counts once, so duplicates and replays cannot inflate it.
 *
 * "Last 5 minutes" is measured on the device's own clock: server now − offset,
 * where offset = arrival time − ts of the device's newest event. A skewed but
 * healthy device still scores fully, and a device that goes silent keeps
 * aging (the offset is fixed, server time keeps moving) so its score drops.
 *
 * Note: the provided generator picks one event type per tick and heartbeats are
 * ~2/3 of them, so a healthy simulated device scores ~0.66 — the fraction of
 * expected 1 Hz heartbeats actually received.
 */
export function computeHealth(input: HealthInput, nowMs: number): DeviceHealth {
  const deviceNowSec = Math.floor((nowMs - input.clockOffsetMs) / 1000);
  const windowStart = deviceNowSec - HEALTH_WINDOW_SEC; // exclusive

  const seconds = new Set<number>();
  for (const ts of input.heartbeatsMs) {
    const sec = Math.floor(ts / 1000);
    if (sec > windowStart && sec <= deviceNowSec) seconds.add(sec);
  }

  const lifetime = deviceNowSec - Math.floor(input.firstSeenMs / 1000) + 1;
  const expected = Math.min(HEALTH_WINDOW_SEC, Math.max(1, lifetime));

  return {
    last_heartbeat_ts:
      input.lastHeartbeatMs === null
        ? null
        : new Date(input.lastHeartbeatMs).toISOString(),
    availability_5m: Math.min(1, seconds.size / expected),
  };
}
