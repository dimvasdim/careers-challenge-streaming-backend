import { AlarmFeed } from './alarm-feed.js';
import type { CommittedEvent } from './batch-writer.js';
import { DeviceHealthStore } from './device-health.js';
import { type FallChange, FallIncidents } from './fall-incidents.js';
import { RoomOccupancyStore } from './room-occupancy.js';

/**
 * All in-memory read models. Fed only with committed, non-duplicate events
 * (BatchWriter.onCommitted), so memory never holds anything Postgres doesn't.
 */
export class ReadModels {
  readonly deviceHealth = new DeviceHealthStore();
  readonly roomOccupancy = new RoomOccupancyStore();
  readonly fallIncidents = new FallIncidents();
  /** Holds only alarms already persisted to the alarms table (see AlarmWriter). */
  readonly alarmFeed = new AlarmFeed();

  /** Applies committed events; returns the fall changes still to be persisted. */
  apply(committed: CommittedEvent[]): FallChange[] {
    const fallChanges: FallChange[] = [];
    for (const { event } of committed) {
      this.deviceHealth.apply(event);
      this.roomOccupancy.apply(event);
      const change = this.fallIncidents.apply(event);
      if (change) fallChanges.push(change);
    }
    return fallChanges;
  }
}
