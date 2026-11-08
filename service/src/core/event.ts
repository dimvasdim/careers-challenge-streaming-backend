// Validated event shapes. `tsMs` is the device timestamp parsed once at ingest.

export const EVENT_TYPES = [
  'heartbeat',
  'presence',
  'motion',
  'sleep_state',
  'fall_warn',
  'net_status',
] as const;
export type EventType = (typeof EVENT_TYPES)[number];

export const SLEEP_STATES = ['asleep', 'awake', 'unknown'] as const;
export type SleepState = (typeof SLEEP_STATES)[number];

interface Envelope {
  deviceId: string;
  roomId: string;
  seq: number;
  tsMs: number;
}

export type IngestEvent = Envelope &
  (
    | { type: 'heartbeat' }
    | { type: 'presence'; inRoom: boolean }
    | { type: 'motion'; magnitude: number }
    | { type: 'sleep_state'; state: SleepState }
    | { type: 'fall_warn'; confidence: number }
    | { type: 'net_status'; rssi: number }
  );

export type RejectReason =
  | 'invalid_json'
  | 'not_object'
  | 'missing_field'
  | 'invalid_field'
  | 'unknown_type'
  | 'invalid_ts'
  | 'ts_future'
  | 'ts_too_old';

/** Identity key of an event: (device_id, seq, ts). */
export function eventKey(e: { deviceId: string; seq: number; tsMs: number }): string {
  return `${e.deviceId}|${e.seq}|${e.tsMs}`;
}
