import type { IngestEvent } from './event.js';

/**
 * fall_warns from one device whose ts is within this of an incident's first ts
 * are jitter copies of that fall ("multiple times within a few seconds").
 * Measured from the first ts, not the last copy, so an incident can't grow forever.
 * Trade-off: two real falls of one device less than 5s apart become one alarm.
 */
export const FALL_DEDUP_WINDOW_MS = 5_000;

/** Incidents and alarms are kept for the 1h acceptance window. */
export const ALARM_HISTORY_MS = 60 * 60_000;

/** One deduplicated fall. The same object is shared by dedup state and the feed. */
export interface Alarm {
  /** alarms.id once persisted (0 before): commit order, used as the SSE event id. */
  id: number;
  /** `${device_id}:${first ts}`, unique in the alarms table. */
  eventId: string;
  deviceId: string;
  roomId: string;
  /** Device ts of the first fall_warn of the incident (original timestamp). */
  tsMs: number;
  /** Highest confidence seen over the incident's copies. */
  confidence: number;
  /** Server time the first copy was ingested (for alarm latency). */
  receivedAtMs: number;
}

/** What a fall_warn changed: a new alarm, or a higher confidence on an existing one. */
export type FallChange =
  { kind: 'new'; alarm: Alarm } | { kind: 'raised'; alarm: Alarm };

export function alarmEventId(deviceId: string, tsMs: number): string {
  return `${deviceId}:${new Date(tsMs).toISOString()}`;
}

/**
 * Jitter dedup of fall warnings, per device.
 *
 * Each device keeps its incidents of the last hour sorted by ts, not only the
 * newest one: a late replayed fall (offline buffer) must not replace the
 * current incident, or the next jitter copy of the live fall would alarm again.
 * Only committed, non-duplicate events arrive here, so an identical replayed
 * event never reaches this code.
 */
export class FallIncidents {
  private readonly byDevice = new Map<string, Alarm[]>();

  apply(event: IngestEvent): FallChange | undefined {
    if (event.type !== 'fall_warn') return undefined;

    let incidents = this.byDevice.get(event.deviceId);
    if (!incidents) {
      incidents = [];
      this.byDevice.set(event.deviceId, incidents);
    }
    // Older events are rejected at ingest, so these incidents can't match again.
    prune(
      incidents,
      event.receivedAtMs - ALARM_HISTORY_MS - FALL_DEDUP_WINDOW_MS,
    );

    const match = closestWithinWindow(incidents, event.tsMs);
    if (match) {
      if (event.confidence <= match.confidence) return undefined;
      match.confidence = event.confidence;
      return { kind: 'raised', alarm: match };
    }

    const alarm: Alarm = {
      id: 0,
      eventId: alarmEventId(event.deviceId, event.tsMs),
      deviceId: event.deviceId,
      roomId: event.roomId,
      tsMs: event.tsMs,
      confidence: event.confidence,
      receivedAtMs: event.receivedAtMs,
    };
    insertByTs(incidents, alarm);
    return { kind: 'new', alarm };
  }

  /** Restores dedup state from persisted alarms (startup), so it survives a restart. */
  seed(alarm: Alarm): void {
    let incidents = this.byDevice.get(alarm.deviceId);
    if (!incidents) {
      incidents = [];
      this.byDevice.set(alarm.deviceId, incidents);
    }
    if (!incidents.some((a) => a.eventId === alarm.eventId))
      insertByTs(incidents, alarm);
  }
}

/** The incident whose first ts is nearest to `tsMs`, if within the window. */
function closestWithinWindow(
  incidents: Alarm[],
  tsMs: number,
): Alarm | undefined {
  let best: Alarm | undefined;
  for (const a of incidents) {
    const distance = Math.abs(tsMs - a.tsMs);
    if (
      distance <= FALL_DEDUP_WINDOW_MS &&
      (!best || distance < Math.abs(tsMs - best.tsMs))
    ) {
      best = a;
    }
  }
  return best;
}

function insertByTs(alarms: Alarm[], alarm: Alarm): void {
  let i = alarms.length;
  while (i > 0 && alarms[i - 1].tsMs > alarm.tsMs) i--; // late ones are rare: scan from the end
  alarms.splice(i, 0, alarm);
}

function prune(alarms: Alarm[], cutoffMs: number): void {
  let drop = 0;
  while (drop < alarms.length && alarms[drop].tsMs < cutoffMs) drop++;
  if (drop > 0) alarms.splice(0, drop);
}
