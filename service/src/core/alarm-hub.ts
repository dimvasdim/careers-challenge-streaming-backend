import type { Alarm } from './fall-incidents.js';

/** One open SSE connection's callback for new alarms. */
export type AlarmSubscriber = (alarms: Alarm[]) => void;

/**
 * Live fan-out of newly committed alarms to the open SSE connections of this
 * process. Holds no data: missed alarms are always read back from Postgres.
 * (Fed by db/alarm-listener.ts, which hears about new alarms from any worker.)
 */
export class AlarmHub {
  private readonly subscribers = new Set<AlarmSubscriber>();

  /** Pushes alarms (already committed, sorted by id) to every subscriber. */
  publish(alarms: Alarm[]): void {
    if (alarms.length === 0) return;
    for (const subscriber of this.subscribers) subscriber(alarms);
  }

  subscribe(subscriber: AlarmSubscriber): () => void {
    this.subscribers.add(subscriber);
    return () => this.subscribers.delete(subscriber);
  }

  subscriberCount(): number {
    return this.subscribers.size;
  }
}
