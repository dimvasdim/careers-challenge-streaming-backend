import type { AlarmFeed } from './alarm-feed.js';
import type { Alarm } from './fall-incidents.js';

export interface SubscriptionOptions {
  feed: AlarmFeed;
  /** Last alarm id the client saw (SSE Last-Event-ID), or undefined for live only. */
  lastEventId: number | undefined;
  /** Postgres fallback when memory no longer holds everything after `afterId`. */
  loadAfter: (afterId: number) => Promise<Alarm[]>;
  send: (alarm: Alarm) => void;
  onError: (err: unknown) => void;
}

/**
 * One SSE subscriber: optional catch-up, then live alarms, with no gaps or
 * doubles. Live alarms are listened to BEFORE the catch-up starts and buffered
 * until it is sent; anything with an id already sent is skipped. Returns a
 * function that ends the subscription.
 */
export function openSubscription(opts: SubscriptionOptions): () => void {
  const { feed, lastEventId } = opts;
  let lastSent = lastEventId ?? feed.lastId();
  let live = lastEventId === undefined;
  let closed = false;
  const buffered: Alarm[] = [];

  const sendNew = (alarm: Alarm) => {
    if (closed || alarm.id <= lastSent) return;
    lastSent = alarm.id;
    opts.send(alarm);
  };

  const unsubscribe = feed.subscribe((alarms) => {
    if (live) alarms.forEach(sendNew);
    else buffered.push(...alarms);
  });

  if (!live) {
    const afterId = lastSent;
    const fromMemory = feed.after(afterId);
    const backlog = fromMemory
      ? Promise.resolve(fromMemory)
      : opts.loadAfter(afterId);
    backlog
      .then((alarms) => {
        alarms.forEach(sendNew);
        buffered.splice(0).forEach(sendNew);
        live = true;
      })
      .catch(opts.onError);
  }

  return () => {
    closed = true; // a catch-up still loading must not send to a closed stream
    unsubscribe();
  };
}
