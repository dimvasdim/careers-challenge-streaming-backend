import type { Alarm } from './fall-incidents.js';

export type AlarmListener = (alarms: Alarm[]) => void;

/**
 * Live fan-out of newly committed alarms to the open SSE connections of this
 * process. Holds no data: missed alarms are always read back from Postgres.
 */
export class AlarmHub {
  private readonly listeners = new Set<AlarmListener>();

  /** Pushes alarms (already committed, sorted by id) to every subscriber. */
  publish(alarms: Alarm[]): void {
    if (alarms.length === 0) return;
    for (const listener of this.listeners) listener(alarms);
  }

  subscribe(listener: AlarmListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  subscriberCount(): number {
    return this.listeners.size;
  }
}
