import {
  EVENT_TYPES,
  SLEEP_STATES,
  type EventType,
  type IngestEvent,
  type RejectReason,
  type SleepState,
} from './event.js';

export const ACCEPT_WINDOW_MS = 60 * 60 * 1000; // ±1h around server time

export type ValidationResult =
  | { ok: true; event: IngestEvent }
  | { ok: false; reason: RejectReason };

// ISO 8601 date-time with an explicit zone (Z or ±hh:mm). Without a zone,
// Date.parse would read the value as server-local time.
const ISO_TS = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$/;

const reject = (reason: RejectReason): ValidationResult => ({ ok: false, reason });

const isNonEmptyString = (v: unknown): v is string => typeof v === 'string' && v.length > 0;
const isUnitInterval = (v: unknown): v is number =>
  typeof v === 'number' && Number.isFinite(v) && v >= 0 && v <= 1;

/**
 * Validates a parsed JSON body against docs/event_schema.md.
 * `nowMs` is server time; events outside [now-1h, now+1h] are rejected.
 * Unknown extra fields are ignored.
 */
export function validateEvent(body: unknown, nowMs: number): ValidationResult {
  if (typeof body !== 'object' || body === null || Array.isArray(body)) {
    return reject('not_object');
  }
  const b = body as Record<string, unknown>;

  for (const field of ['device_id', 'room_id', 'type', 'ts', 'seq']) {
    if (b[field] === undefined || b[field] === null) return reject('missing_field');
  }

  if (!isNonEmptyString(b.device_id) || !isNonEmptyString(b.room_id)) {
    return reject('invalid_field');
  }
  if (typeof b.type !== 'string' || !(EVENT_TYPES as readonly string[]).includes(b.type)) {
    return reject('unknown_type');
  }
  if (typeof b.seq !== 'number' || !Number.isSafeInteger(b.seq) || b.seq < 0) {
    return reject('invalid_field');
  }

  if (typeof b.ts !== 'string' || !ISO_TS.test(b.ts)) return reject('invalid_ts');
  const tsMs = Date.parse(b.ts);
  if (Number.isNaN(tsMs)) return reject('invalid_ts');
  if (tsMs > nowMs + ACCEPT_WINDOW_MS) return reject('ts_future');
  if (tsMs < nowMs - ACCEPT_WINDOW_MS) return reject('ts_too_old');

  const envelope = {
    deviceId: b.device_id,
    roomId: b.room_id,
    seq: b.seq,
    tsMs,
    receivedAtMs: nowMs,
  };
  const type = b.type as EventType;

  switch (type) {
    case 'heartbeat':
      return { ok: true, event: { ...envelope, type } };
    case 'presence':
      if (b.in_room === undefined) return reject('missing_field');
      if (typeof b.in_room !== 'boolean') return reject('invalid_field');
      return { ok: true, event: { ...envelope, type, inRoom: b.in_room } };
    case 'motion':
      if (b.magnitude === undefined) return reject('missing_field');
      if (!isUnitInterval(b.magnitude)) return reject('invalid_field');
      return { ok: true, event: { ...envelope, type, magnitude: b.magnitude } };
    case 'sleep_state':
      if (b.state === undefined) return reject('missing_field');
      if (typeof b.state !== 'string' || !(SLEEP_STATES as readonly string[]).includes(b.state)) {
        return reject('invalid_field');
      }
      return { ok: true, event: { ...envelope, type, state: b.state as SleepState } };
    case 'fall_warn':
      if (b.confidence === undefined) return reject('missing_field');
      if (!isUnitInterval(b.confidence)) return reject('invalid_field');
      return { ok: true, event: { ...envelope, type, confidence: b.confidence } };
    case 'net_status':
      if (b.rssi === undefined) return reject('missing_field');
      if (typeof b.rssi !== 'number' || !Number.isInteger(b.rssi)) return reject('invalid_field');
      return { ok: true, event: { ...envelope, type, rssi: b.rssi } };
  }
}
