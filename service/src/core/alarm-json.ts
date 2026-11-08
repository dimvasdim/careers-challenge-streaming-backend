import type { Alarm } from './fall-incidents.js';

/** Response shape of an alarm, for GET /alarms and the SSE feed. */
export interface AlarmJson {
  event_id: string;
  room_id: string;
  ts: string;
  confidence: number;
}

export function toAlarmJson(a: Alarm): AlarmJson {
  return {
    event_id: a.eventId,
    room_id: a.roomId,
    ts: new Date(a.tsMs).toISOString(),
    confidence: a.confidence,
  };
}

/**
 * `since` for GET /alarms: epoch seconds (`since=0`, `since=1779562429.5`) or an
 * ISO 8601 date. Missing means "all". Returns ms, or undefined if invalid.
 */
export function parseSince(raw: unknown): number | undefined {
  if (raw === undefined || raw === '') return 0;
  if (typeof raw !== 'string') return undefined;
  const asNumber = Number(raw);
  if (Number.isFinite(asNumber)) return asNumber * 1000;
  const asDate = Date.parse(raw);
  return Number.isNaN(asDate) ? undefined : asDate;
}
