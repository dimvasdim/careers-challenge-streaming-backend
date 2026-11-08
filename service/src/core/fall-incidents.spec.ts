import {
  type Alarm,
  alarmEventId,
  FALL_DEDUP_WINDOW_MS,
  type FallEvent,
  planAlarms,
} from './fall-incidents.js';

const T0 = Date.parse('2026-05-23T18:00:00.000Z');
const SEC = 1000;

let seq = 0;
const fall = (
  tsMs: number,
  confidence = 0.8,
  deviceId = 'dev_0001',
  receivedAtMs = tsMs,
): FallEvent => ({
  type: 'fall_warn',
  confidence,
  deviceId,
  roomId: 'room_000',
  seq: ++seq,
  tsMs,
  receivedAtMs,
});
const persisted = (
  id: number,
  tsMs: number,
  confidence = 0.8,
  deviceId = 'dev_0001',
): Alarm => ({
  id,
  eventId: alarmEventId(deviceId, tsMs),
  deviceId,
  roomId: 'room_000',
  tsMs,
  confidence,
  receivedAtMs: tsMs,
});

describe('planAlarms', () => {
  it('creates one alarm with the original ts for a fall', () => {
    const plan = planAlarms([], [fall(T0, 0.9)]);
    expect(plan.created).toEqual([
      {
        id: 0,
        eventId: `dev_0001:${new Date(T0).toISOString()}`,
        deviceId: 'dev_0001',
        roomId: 'room_000',
        tsMs: T0,
        confidence: 0.9,
        receivedAtMs: T0,
      },
    ]);
    expect(plan.upserts).toEqual(plan.created);
  });

  it('merges jitter copies in one batch, keeping the max confidence', () => {
    const plan = planAlarms(
      [],
      [fall(T0, 0.8), fall(T0, 0.95), fall(T0 + 2 * SEC, 0.7)],
    );
    expect(plan.created).toHaveLength(1);
    expect(plan.created[0]).toMatchObject({ tsMs: T0, confidence: 0.95 });
    expect(plan.upserts).toHaveLength(1); // one row per statement
  });

  it('measures the window from the first ts, inclusive at exactly 5s', () => {
    const plan = planAlarms(
      [],
      [
        fall(T0),
        fall(T0 + FALL_DEDUP_WINDOW_MS),
        fall(T0 + FALL_DEDUP_WINDOW_MS + 1),
      ],
    );
    // 4s after the last copy but 5.001s after the first: a new fall.
    expect(plan.created.map((a) => a.tsMs)).toEqual([
      T0,
      T0 + FALL_DEDUP_WINDOW_MS + 1,
    ]);
  });

  it('merges a copy into an alarm committed by an earlier batch', () => {
    const existing = persisted(7, T0, 0.8);
    const plan = planAlarms([existing], [fall(T0 + 2 * SEC, 0.95)]);
    expect(plan.created).toEqual([]);
    expect(plan.upserts).toEqual([{ ...existing, confidence: 0.95 }]);
  });

  it('writes nothing for a copy that does not raise the confidence', () => {
    expect(planAlarms([persisted(7, T0, 0.9)], [fall(T0 + SEC, 0.5)])).toEqual({
      upserts: [],
      created: [],
    });
  });

  it('merges a late copy that is earlier than the first one, keeping the event id', () => {
    const existing = persisted(7, T0, 0.8);
    const plan = planAlarms([existing], [fall(T0 - 3 * SEC, 0.85)]);
    expect(plan.created).toEqual([]);
    expect(plan.upserts[0].eventId).toBe(alarmEventId('dev_0001', T0));
  });

  it('keeps devices apart', () => {
    const plan = planAlarms(
      [persisted(1, T0, 0.8, 'dev_a')],
      [fall(T0, 0.8, 'dev_b')],
    );
    expect(plan.created.map((a) => a.deviceId)).toEqual(['dev_b']);
  });

  it('a late replayed fall does not break dedup of the live one', () => {
    const now = T0 + 20 * 60 * SEC;
    const live = persisted(1, now);
    const plan = planAlarms(
      [live],
      [
        fall(T0, 0.8, 'dev_0001', now),
        fall(T0, 0.8, 'dev_0001', now),
        fall(now + SEC, 0.7),
      ],
    );
    expect(plan.created.map((a) => a.tsMs)).toEqual([T0]); // only the replayed fall is new
  });

  it('joins the nearest incident when two are within the window', () => {
    const a = persisted(1, T0, 0.8);
    const b = persisted(2, T0 + 6 * SEC, 0.8);
    const plan = planAlarms([a, b], [fall(T0 + 4 * SEC, 0.9)]);
    expect(plan.upserts.map((x) => x.id)).toEqual([2]);
  });
});
