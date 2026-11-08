import type { IngestEvent } from './event.js';
import {
  ALARM_HISTORY_MS,
  type Alarm,
  FALL_DEDUP_WINDOW_MS,
  FallIncidents,
  alarmEventId,
} from './fall-incidents.js';

const T0 = Date.parse('2026-05-23T18:00:00.000Z');
const SEC = 1000;

let seq = 0;
const fall = (
  tsMs: number,
  confidence = 0.8,
  receivedAtMs = tsMs,
  deviceId = 'dev_0001',
): IngestEvent => ({
  type: 'fall_warn',
  confidence,
  deviceId,
  roomId: 'room_000',
  seq: ++seq,
  tsMs,
  receivedAtMs,
});

describe('FallIncidents', () => {
  let falls: FallIncidents;
  beforeEach(() => {
    falls = new FallIncidents();
  });

  it('ignores other event types', () => {
    expect(
      falls.apply({
        type: 'heartbeat',
        deviceId: 'd',
        roomId: 'r',
        seq: 1,
        tsMs: T0,
        receivedAtMs: T0,
      }),
    ).toBeUndefined();
  });

  it('creates one alarm with the original ts for a fall', () => {
    const change = falls.apply(fall(T0, 0.9));
    expect(change).toEqual({
      kind: 'new',
      alarm: {
        id: 0,
        eventId: `dev_0001:${new Date(T0).toISOString()}`,
        deviceId: 'dev_0001',
        roomId: 'room_000',
        tsMs: T0,
        confidence: 0.9,
        receivedAtMs: T0,
      },
    });
  });

  it('merges jitter copies (same ts, new seq) into one alarm', () => {
    expect(falls.apply(fall(T0, 0.8))?.kind).toBe('new');
    expect(falls.apply(fall(T0, 0.8))).toBeUndefined();
    expect(falls.apply(fall(T0, 0.7))).toBeUndefined();
  });

  it('raises confidence to the max of the copies, without a new alarm', () => {
    const first = falls.apply(fall(T0, 0.8))!.alarm;
    const change = falls.apply(fall(T0 + 2 * SEC, 0.95));
    expect(change).toEqual({ kind: 'raised', alarm: first });
    expect(first.confidence).toBe(0.95);
    expect(first.tsMs).toBe(T0); // the incident keeps its first ts
  });

  it('measures the window from the first ts, inclusive at exactly 5s', () => {
    falls.apply(fall(T0));
    expect(falls.apply(fall(T0 + FALL_DEDUP_WINDOW_MS))).toBeUndefined();
    // 4s after the last copy but 5.001s after the first: a new fall.
    expect(falls.apply(fall(T0 + FALL_DEDUP_WINDOW_MS + 1))?.kind).toBe('new');
  });

  it('merges a late copy that is earlier than the first one, keeping the event id', () => {
    const first = falls.apply(fall(T0, 0.8))!.alarm;
    expect(falls.apply(fall(T0 - 3 * SEC, 0.85))?.kind).toBe('raised');
    expect(first.eventId).toBe(alarmEventId('dev_0001', T0));
  });

  it('keeps devices apart', () => {
    falls.apply(fall(T0, 0.8, T0, 'dev_a'));
    expect(falls.apply(fall(T0, 0.8, T0, 'dev_b'))?.kind).toBe('new');
  });

  it('a late replayed fall does not break dedup of the live one', () => {
    const now = T0 + 20 * 60 * SEC;
    expect(falls.apply(fall(now, 0.8, now))?.kind).toBe('new'); // live fall
    expect(falls.apply(fall(T0, 0.8, now))?.kind).toBe('new'); // replayed, 20 min old
    expect(falls.apply(fall(T0, 0.8, now))).toBeUndefined(); // its jitter copy
    expect(falls.apply(fall(now + SEC, 0.8, now + SEC))).toBeUndefined(); // live copy
  });

  it('forgets incidents after the 1h window', () => {
    falls.apply(fall(T0));
    const later = T0 + ALARM_HISTORY_MS + FALL_DEDUP_WINDOW_MS + SEC;
    // A copy of the old fall can't arrive any more (rejected at ingest), so a new
    // fall at the same device creates its own alarm and the old state is gone.
    expect(falls.apply(fall(later, 0.8, later))?.kind).toBe('new');
  });

  it('dedups against seeded alarms after a restart', () => {
    const persisted: Alarm = {
      id: 7,
      eventId: alarmEventId('dev_0001', T0),
      deviceId: 'dev_0001',
      roomId: 'room_000',
      tsMs: T0,
      confidence: 0.9,
      receivedAtMs: T0,
    };
    falls.seed(persisted);
    falls.seed(persisted); // seeding twice is harmless
    expect(falls.apply(fall(T0 + SEC, 0.5))).toBeUndefined();
    expect(falls.apply(fall(T0 + 2 * SEC, 0.95))).toEqual({
      kind: 'raised',
      alarm: persisted,
    });
  });
});
