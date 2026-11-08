import { ACCEPT_WINDOW_MS, validateEvent } from './validate.js';

const NOW = Date.parse('2026-05-23T18:53:49.123Z');
const iso = (ms: number) => new Date(ms).toISOString();

const base = {
  device_id: 'dev_0001',
  room_id: 'room_014',
  ts: iso(NOW),
  seq: 42,
};

const valid = {
  heartbeat: { ...base, type: 'heartbeat' },
  presence: { ...base, type: 'presence', in_room: true },
  motion: { ...base, type: 'motion', magnitude: 0.81 },
  sleep_state: { ...base, type: 'sleep_state', state: 'asleep' },
  fall_warn: { ...base, type: 'fall_warn', confidence: 0.92 },
  net_status: { ...base, type: 'net_status', rssi: -68 },
};

const reasonOf = (body: unknown) => {
  const r = validateEvent(body, NOW);
  return r.ok ? 'ok' : r.reason;
};

describe('validateEvent', () => {
  it('accepts a valid event of every type and parses the envelope', () => {
    for (const body of Object.values(valid)) {
      const r = validateEvent(body, NOW);
      expect(r.ok).toBe(true);
      if (r.ok) {
        expect(r.event).toMatchObject({
          deviceId: 'dev_0001',
          roomId: 'room_014',
          seq: 42,
          tsMs: NOW,
          type: body.type,
        });
      }
    }
  });

  it('maps type-specific fields', () => {
    const pick = (body: unknown) => {
      const r = validateEvent(body, NOW);
      if (!r.ok) throw new Error(r.reason);
      return r.event;
    };
    expect(pick(valid.presence)).toMatchObject({ inRoom: true });
    expect(pick(valid.motion)).toMatchObject({ magnitude: 0.81 });
    expect(pick(valid.sleep_state)).toMatchObject({ state: 'asleep' });
    expect(pick(valid.fall_warn)).toMatchObject({ confidence: 0.92 });
    expect(pick(valid.net_status)).toMatchObject({ rssi: -68 });
  });

  it('rejects non-object bodies', () => {
    for (const body of [null, undefined, 'x', 1, [], [valid.heartbeat]]) {
      expect(reasonOf(body)).toBe('not_object');
    }
  });

  it('rejects missing envelope fields', () => {
    for (const field of ['device_id', 'room_id', 'type', 'ts', 'seq']) {
      const body: Record<string, unknown> = { ...valid.heartbeat };
      delete body[field];
      expect(reasonOf(body)).toBe('missing_field');
      expect(reasonOf({ ...valid.heartbeat, [field]: null })).toBe('missing_field');
    }
  });

  it('rejects envelope fields of the wrong type', () => {
    expect(reasonOf({ ...valid.heartbeat, device_id: '' })).toBe('invalid_field');
    expect(reasonOf({ ...valid.heartbeat, device_id: 7 })).toBe('invalid_field');
    expect(reasonOf({ ...valid.heartbeat, room_id: {} })).toBe('invalid_field');
    expect(reasonOf({ ...valid.heartbeat, seq: '42' })).toBe('invalid_field');
    expect(reasonOf({ ...valid.heartbeat, seq: -1 })).toBe('invalid_field');
    expect(reasonOf({ ...valid.heartbeat, seq: 1.5 })).toBe('invalid_field');
  });

  it('rejects unknown types', () => {
    expect(reasonOf({ ...valid.heartbeat, type: 'smoke_alarm' })).toBe('unknown_type');
    expect(reasonOf({ ...valid.heartbeat, type: 3 })).toBe('unknown_type');
  });

  it('rejects malformed timestamps', () => {
    for (const ts of [
      '2026-05-23T18:53:49.123', // no zone: would be parsed as local time
      '2026-05-23 18:53:49Z',
      '2026-13-45T99:99:99Z',
      'yesterday',
      NOW,
    ]) {
      expect(reasonOf({ ...valid.heartbeat, ts })).toBe('invalid_ts');
    }
  });

  it('accepts timestamps with an explicit offset', () => {
    expect(reasonOf({ ...valid.heartbeat, ts: '2026-05-23T20:53:49.123+02:00' })).toBe('ok');
  });

  it('applies the ±1h acceptance window inclusively', () => {
    const at = (ms: number) => reasonOf({ ...valid.heartbeat, ts: iso(ms) });
    expect(at(NOW + ACCEPT_WINDOW_MS)).toBe('ok');
    expect(at(NOW + ACCEPT_WINDOW_MS + 1)).toBe('ts_future');
    expect(at(NOW - ACCEPT_WINDOW_MS)).toBe('ok');
    expect(at(NOW - ACCEPT_WINDOW_MS - 1)).toBe('ts_too_old');
  });

  it('rejects missing or invalid type-specific fields', () => {
    expect(reasonOf({ ...base, type: 'presence' })).toBe('missing_field');
    expect(reasonOf({ ...valid.presence, in_room: 'true' })).toBe('invalid_field');
    expect(reasonOf({ ...base, type: 'motion' })).toBe('missing_field');
    expect(reasonOf({ ...valid.motion, magnitude: 1.01 })).toBe('invalid_field');
    expect(reasonOf({ ...valid.motion, magnitude: -0.1 })).toBe('invalid_field');
    expect(reasonOf({ ...base, type: 'sleep_state' })).toBe('missing_field');
    expect(reasonOf({ ...valid.sleep_state, state: 'dreaming' })).toBe('invalid_field');
    expect(reasonOf({ ...base, type: 'fall_warn' })).toBe('missing_field');
    expect(reasonOf({ ...valid.fall_warn, confidence: 2 })).toBe('invalid_field');
    expect(reasonOf({ ...valid.fall_warn, confidence: '0.9' })).toBe('invalid_field');
    expect(reasonOf({ ...base, type: 'net_status' })).toBe('missing_field');
    expect(reasonOf({ ...valid.net_status, rssi: -68.5 })).toBe('invalid_field');
  });

  it('accepts the boundary values 0 and 1 for magnitude and confidence', () => {
    expect(reasonOf({ ...valid.motion, magnitude: 0 })).toBe('ok');
    expect(reasonOf({ ...valid.fall_warn, confidence: 1 })).toBe('ok');
  });

  it('ignores extra fields', () => {
    const r = validateEvent({ ...valid.heartbeat, firmware: '1.2.3', in_room: 'x' }, NOW);
    expect(r.ok).toBe(true);
    if (r.ok) expect(r.event).not.toHaveProperty('firmware');
  });
});
