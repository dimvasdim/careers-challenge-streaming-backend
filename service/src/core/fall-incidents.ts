import type { IngestEvent } from './event.js';

/**
 * fall_warns from one device whose ts is within this of an incident's first ts
 * are jitter copies of that fall ("multiple times within a few seconds").
 * Measured from the first ts, not the last copy, so an incident can't grow forever.
 * Trade-off: two real falls of one device less than 5s apart become one alarm.
 */
export const FALL_DEDUP_WINDOW_MS = 5_000;

/** One deduplicated fall: a row of the alarms table. */
export interface Alarm {
  /** alarms.id (0 until inserted): commit order, used as the SSE event id. */
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

export type FallEvent = Extract<IngestEvent, { type: 'fall_warn' }>;

export interface AlarmPlan {
  /** Rows to upsert: new alarms and existing ones whose confidence went up. */
  upserts: Alarm[];
  /** The new alarms among them (to push to subscribers once committed). */
  created: Alarm[];
}

export function alarmEventId(deviceId: string, tsMs: number): string {
  return `${deviceId}:${new Date(tsMs).toISOString()}`;
}

/**
 * Jitter dedup for one batch of newly inserted fall_warns.
 *
 * `existing` are the alarms already in Postgres near these falls (same
 * devices, within the window); the caller holds a per-device lock, so no other
 * transaction can add one meanwhile. Falls are applied in order: each joins the
 * incident of its device whose first ts is nearest within 5s (raising the
 * confidence if higher), or starts a new incident. Matching against every
 * nearby incident, not only the newest, keeps a late replayed fall from
 * breaking dedup of a live one.
 */
export function planAlarms(existing: Alarm[], falls: FallEvent[]): AlarmPlan {
  const byDevice = new Map<string, Alarm[]>();
  for (const a of existing) listFor(byDevice, a.deviceId).push(a);

  const upserts = new Map<string, Alarm>(); // by eventId: one row per statement
  const created: Alarm[] = [];
  for (const fall of falls) {
    const incidents = listFor(byDevice, fall.deviceId);
    const match = closestWithinWindow(incidents, fall.tsMs);
    if (match) {
      if (fall.confidence > match.confidence) {
        match.confidence = fall.confidence;
        upserts.set(match.eventId, match);
      }
      continue;
    }
    const alarm: Alarm = {
      id: 0,
      eventId: alarmEventId(fall.deviceId, fall.tsMs),
      deviceId: fall.deviceId,
      roomId: fall.roomId,
      tsMs: fall.tsMs,
      confidence: fall.confidence,
      receivedAtMs: fall.receivedAtMs,
    };
    incidents.push(alarm);
    created.push(alarm);
    upserts.set(alarm.eventId, alarm);
  }
  return { upserts: [...upserts.values()], created };
}

function listFor(map: Map<string, Alarm[]>, deviceId: string): Alarm[] {
  let list = map.get(deviceId);
  if (!list) {
    list = [];
    map.set(deviceId, list);
  }
  return list;
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
