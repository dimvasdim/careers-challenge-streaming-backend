import type { IngestEvent } from './event.js';

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

interface DeviceState {
  /** ring[sec % 300] = the unix second of a heartbeat in that slot (0 = empty). */
  ring: Uint32Array;
  /** Newest heartbeat ts seen (ms), or undefined if no heartbeat yet. */
  lastHeartbeatTs: number | undefined;
  /** Oldest event second seen: availability over a shorter life is not penalised. */
  firstSeenSec: number;
  /** Device clock offset: server arrival time − device ts, from its newest sane event. */
  offsetMs: number;
  /** Device ts of the event the offset was taken from. */
  offsetFromTs: number;
}

/**
 * Per-device heartbeat health, computed on read.
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
export class DeviceHealthStore {
  private readonly devices = new Map<string, DeviceState>();

  apply(event: IngestEvent): void {
    const sec = Math.floor(event.tsMs / 1000);
    const offset = event.receivedAtMs - event.tsMs;
    const saneClock = Math.abs(offset) <= MAX_CLOCK_OFFSET_MS;

    let d = this.devices.get(event.deviceId);
    if (!d) {
      d = {
        ring: new Uint32Array(HEALTH_WINDOW_SEC),
        lastHeartbeatTs: undefined,
        firstSeenSec: sec,
        offsetMs: saneClock ? offset : 0,
        offsetFromTs: saneClock ? event.tsMs : -Infinity,
      };
      this.devices.set(event.deviceId, d);
    }

    if (sec < d.firstSeenSec) d.firstSeenSec = sec; // late replay of an earlier event
    if (saneClock && event.tsMs > d.offsetFromTs) {
      d.offsetMs = offset;
      d.offsetFromTs = event.tsMs;
    }

    if (event.type !== 'heartbeat') return;
    // A heartbeat far ahead of its own arrival time is a broken clock reading: it is
    // persisted, but kept out of health so it can't take a ring slot or the "last" ts.
    if (-offset > MAX_CLOCK_OFFSET_MS) return;
    if (d.lastHeartbeatTs === undefined || event.tsMs > d.lastHeartbeatTs) {
      d.lastHeartbeatTs = event.tsMs;
    }
    // A newer second overwrites an older one in the same slot; a late older one is ignored.
    const slot = sec % HEALTH_WINDOW_SEC;
    if (d.ring[slot] < sec) d.ring[slot] = sec;
  }

  /** Health as of server time `nowMs`, or undefined for a device never seen. */
  getHealth(deviceId: string, nowMs: number): DeviceHealth | undefined {
    const d = this.devices.get(deviceId);
    if (!d) return undefined;

    const deviceNowSec = Math.floor((nowMs - d.offsetMs) / 1000);
    const windowStart = deviceNowSec - HEALTH_WINDOW_SEC; // exclusive
    let seconds = 0;
    for (const s of d.ring) {
      if (s > windowStart && s <= deviceNowSec) seconds++;
    }

    const lifetime = deviceNowSec - d.firstSeenSec + 1;
    const expected = Math.min(HEALTH_WINDOW_SEC, Math.max(1, lifetime));

    return {
      last_heartbeat_ts:
        d.lastHeartbeatTs === undefined ? null : new Date(d.lastHeartbeatTs).toISOString(),
      availability_5m: Math.min(1, seconds / expected),
    };
  }
}
