import { AlarmFeed, parseSince, toAlarmJson } from './alarm-feed.js';
import { openSubscription } from './alarm-subscription.js';
import { ALARM_HISTORY_MS, type Alarm } from './fall-incidents.js';

const T0 = Date.parse('2026-05-23T18:00:00.000Z');
const SEC = 1000;

const alarm = (id: number, tsMs: number, roomId = 'room_000'): Alarm => ({
  id,
  eventId: `dev_${id}:${new Date(tsMs).toISOString()}`,
  deviceId: `dev_${id}`,
  roomId,
  tsMs,
  confidence: 0.9,
  receivedAtMs: tsMs,
});

const flush = () => new Promise((resolve) => setImmediate(resolve));

describe('parseSince', () => {
  it('accepts epoch seconds, ISO dates and nothing', () => {
    expect(parseSince('0')).toBe(0);
    expect(parseSince('1779559200.5')).toBe(1779559200500);
    expect(parseSince('2026-05-23T18:00:00.000Z')).toBe(T0);
    expect(parseSince(undefined)).toBe(0);
  });
  it('rejects garbage', () => {
    expect(parseSince('yesterday')).toBeUndefined();
    expect(parseSince(['1', '2'])).toBeUndefined();
  });
});

describe('AlarmFeed', () => {
  it('returns alarms since a ts, sorted by ts even if persisted out of order', () => {
    const feed = new AlarmFeed();
    feed.publish([alarm(1, T0 + 10 * SEC)], T0 + 10 * SEC);
    feed.publish([alarm(2, T0)], T0 + 11 * SEC); // late replayed fall
    feed.publish([alarm(3, T0 + 20 * SEC)], T0 + 20 * SEC);
    expect(feed.since(0).map((a) => a.id)).toEqual([2, 1, 3]);
    expect(feed.since(T0 + 10 * SEC).map((a) => a.id)).toEqual([1, 3]);
    expect(toAlarmJson(feed.since(0)[0])).toEqual({
      event_id: `dev_2:${new Date(T0).toISOString()}`,
      room_id: 'room_000',
      ts: new Date(T0).toISOString(),
      confidence: 0.9,
    });
  });

  it('prunes after 1h and then sends catch-up for pruned ids to Postgres', () => {
    const feed = new AlarmFeed();
    feed.publish([alarm(1, T0), alarm(2, T0 + 10 * SEC)], T0 + 10 * SEC);
    feed.publish(
      [alarm(3, T0 + ALARM_HISTORY_MS + 5 * SEC)],
      T0 + ALARM_HISTORY_MS + 5 * SEC,
    );
    expect(feed.since(0).map((a) => a.id)).toEqual([2, 3]);
    expect(feed.after(0)).toBeUndefined(); // id 1 is gone from memory
    expect(feed.after(1)?.map((a) => a.id)).toEqual([2, 3]);
  });

  it('starts from loaded alarms after a restart without pushing them', () => {
    const feed = new AlarmFeed();
    const pushed: number[] = [];
    feed.subscribe((alarms) => pushed.push(...alarms.map((a) => a.id)));
    feed.load([alarm(5, T0), alarm(6, T0 + SEC)], 4);
    expect(pushed).toEqual([]);
    expect(feed.lastId()).toBe(6);
    expect(feed.after(3)).toBeUndefined(); // id 4 was never loaded
    expect(feed.after(4)?.map((a) => a.id)).toEqual([5, 6]);
  });
});

describe('openSubscription', () => {
  function setup(
    lastEventId: number | undefined,
    loadAfter = vi.fn(async () => [] as Alarm[]),
  ) {
    const feed = new AlarmFeed();
    feed.publish([alarm(1, T0), alarm(2, T0 + SEC)], T0 + SEC);
    const sent: number[] = [];
    const close = openSubscription({
      feed,
      lastEventId,
      loadAfter,
      send: (a) => sent.push(a.id),
      onError: (err) => {
        throw err;
      },
    });
    return { feed, sent, close, loadAfter };
  }

  it('without Last-Event-ID sends only new alarms', async () => {
    const { feed, sent } = setup(undefined);
    feed.publish([alarm(3, T0 + 2 * SEC)], T0 + 2 * SEC);
    await flush();
    expect(sent).toEqual([3]);
  });

  it('with Last-Event-ID first sends what was missed, from memory', async () => {
    const { feed, sent, loadAfter } = setup(1);
    feed.publish([alarm(3, T0 + 2 * SEC)], T0 + 2 * SEC);
    await flush();
    expect(sent).toEqual([2, 3]);
    expect(loadAfter).not.toHaveBeenCalled();
  });

  it('falls back to Postgres and merges live alarms without gaps or doubles', async () => {
    let finishLoad!: (alarms: Alarm[]) => void;
    const loadAfter = vi.fn(
      () => new Promise<Alarm[]>((resolve) => (finishLoad = resolve)),
    );
    const feed = new AlarmFeed();
    feed.load([alarm(10, T0)], 9); // ids <= 9 only in Postgres
    const sent: number[] = [];
    openSubscription({
      feed,
      lastEventId: 7,
      loadAfter,
      send: (a) => sent.push(a.id),
      onError: () => {},
    });
    expect(loadAfter).toHaveBeenCalledWith(7);

    // A live alarm arrives while the catch-up query is still running.
    feed.publish([alarm(11, T0 + SEC)], T0 + SEC);
    expect(sent).toEqual([]);
    // Postgres returns 8..11 (it already sees 11, committed before the query ran).
    finishLoad([
      alarm(8, T0),
      alarm(9, T0),
      alarm(10, T0),
      alarm(11, T0 + SEC),
    ]);
    await flush();
    expect(sent).toEqual([8, 9, 10, 11]);

    feed.publish([alarm(12, T0 + 2 * SEC)], T0 + 2 * SEC);
    expect(sent).toEqual([8, 9, 10, 11, 12]);
  });

  it('stops sending once closed', async () => {
    const { feed, sent, close } = setup(undefined);
    close();
    feed.publish([alarm(3, T0 + 2 * SEC)], T0 + 2 * SEC);
    await flush();
    expect(sent).toEqual([]);
    expect(feed.subscriberCount()).toBe(0);
  });
});
