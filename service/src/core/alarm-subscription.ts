import type { AlarmHub } from './alarm-hub.js';
import type { Alarm } from './fall-incidents.js';

export interface SubscriptionOptions {
  hub: AlarmHub;
  /** Last alarm id the client saw (SSE Last-Event-ID), or undefined for live only. */
  lastEventId: number | undefined;
  /** Catch-up from Postgres: alarms with id > afterId, in id order. */
  loadAfter: (afterId: number) => Promise<Alarm[]>;
  send: (alarm: Alarm) => void;
  onError: (err: unknown) => void;
}

/**
 * One SSE subscriber: optional catch-up, then live alarms, with no gaps or
 * doubles. The live listener is registered BEFORE the catch-up query runs and
 * buffers until the catch-up is sent, so an alarm committed in between is
 * either in the query result or in the buffer; anything with an id already
 * sent is skipped. Returns a function that ends the subscription.
 */
export function openSubscription(opts: SubscriptionOptions): () => void {
  let lastSent = opts.lastEventId ?? 0;
  let live = opts.lastEventId === undefined;
  let closed = false;
  const buffered: Alarm[] = [];

  const sendNew = (alarm: Alarm) => {
    if (closed || alarm.id <= lastSent) return;
    lastSent = alarm.id;
    opts.send(alarm);
  };

  const unsubscribe = opts.hub.subscribe((alarms) => {
    if (live) alarms.forEach(sendNew);
    else buffered.push(...alarms);
  });

  if (!live) {
    opts
      .loadAfter(lastSent)
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
