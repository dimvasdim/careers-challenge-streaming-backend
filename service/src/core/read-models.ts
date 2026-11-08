import type { CommittedEvent } from './batch-writer.js';
import { DeviceHealthStore } from './device-health.js';
import { RoomOccupancyStore } from './room-occupancy.js';

/**
 * All in-memory read models. Fed only with committed, non-duplicate events
 * (BatchWriter.onCommitted), so memory never holds anything Postgres doesn't.
 */
export class ReadModels {
  readonly deviceHealth = new DeviceHealthStore();
  readonly roomOccupancy = new RoomOccupancyStore();

  apply(committed: CommittedEvent[]): void {
    for (const { event } of committed) {
      this.deviceHealth.apply(event);
      this.roomOccupancy.apply(event);
    }
  }
}
