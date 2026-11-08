import { AlarmHub } from './alarm-hub.js';
import { parseSince, toAlarmJson } from './alarm-json.js';
import { openSubscription } from './alarm-subscription.js';
import type { Alarm } from './fall-incidents.js';

const T0 = Date.parse('2026-05-23T18:00:00.000Z');

const alarm = (id: number, tsMs = T0): Alarm => ({
  id,
  eventId: `dev_${id}:${new Date(tsMs).toISOString()}`,
  deviceId: `dev_${id}`,
  roomId: 'room_000',
  tsMs,
  confidence: 0.9,
  receivedAtMs: tsMs,
});

const flush = () => new Promise((resolve) => setImmediate(resolve));

function open(
  hub: AlarmHub,
  lastEventId: number | undefined,
  loadAfter: (id: number) => Promise<Alarm[]>,
) {
  const sent: number[] = [];
  const close = openSubscription({
    hub,
    lastEventId,
    loadAfter,
    send: (a) => sent.push(a.id),
    onError: () => {},
  });
  return { sent, close };
}

describe('openSubscription', () => {
  it('without Last-Event-ID sends only live alarms and never queries', async () => {
    const hub = new AlarmHub();
    const loadAfter = vi.fn(async () => [] as Alarm[]);
    const { sent } = open(hub, undefined, loadAfter);
    hub.publish([alarm(3)]);
    await flush();
    expect(sent).toEqual([3]);
    expect(loadAfter).not.toHaveBeenCalled();
  });

  it('catches up from Postgres and merges live alarms without gaps or doubles', async () => {
    const hub = new AlarmHub();
    let finishLoad!: (alarms: Alarm[]) => void;
    const loadAfter = vi.fn(
      () => new Promise<Alarm[]>((resolve) => (finishLoad = resolve)),
    );
    const { sent } = open(hub, 7, loadAfter);
    expect(loadAfter).toHaveBeenCalledWith(7);

    // Alarm 11 is committed while the catch-up query runs, and the query sees it too.
    hub.publish([alarm(11)]);
    expect(sent).toEqual([]);
    finishLoad([alarm(8), alarm(9), alarm(10), alarm(11)]);
    await flush();
    expect(sent).toEqual([8, 9, 10, 11]);

    hub.publish([alarm(12)]);
    expect(sent).toEqual([8, 9, 10, 11, 12]);
  });

  it('stops sending once closed, also for a catch-up still loading', async () => {
    const hub = new AlarmHub();
    let finishLoad!: (alarms: Alarm[]) => void;
    const { sent, close } = open(
      hub,
      0,
      () => new Promise((resolve) => (finishLoad = resolve)),
    );
    close();
    finishLoad([alarm(1)]);
    hub.publish([alarm(2)]);
    await flush();
    expect(sent).toEqual([]);
    expect(hub.subscriberCount()).toBe(0);
  });
});

describe('alarm json', () => {
  it('parses since as epoch seconds, ISO date or nothing', () => {
    expect(parseSince('0')).toBe(0);
    expect(parseSince('1779559200.5')).toBe(1779559200500);
    expect(parseSince('2026-05-23T18:00:00.000Z')).toBe(T0);
    expect(parseSince(undefined)).toBe(0);
    expect(parseSince('yesterday')).toBeUndefined();
    expect(parseSince(['1', '2'])).toBeUndefined();
  });

  it('shapes an alarm for the API', () => {
    expect(toAlarmJson(alarm(1))).toEqual({
      event_id: `dev_1:${new Date(T0).toISOString()}`,
      room_id: 'room_000',
      ts: new Date(T0).toISOString(),
      confidence: 0.9,
    });
  });
});
