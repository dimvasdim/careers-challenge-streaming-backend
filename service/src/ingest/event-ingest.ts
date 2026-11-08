import type { RejectReason } from '../core/event.js';

export type IngestResult =
  | { status: 'inserted' | 'duplicate' } // durable in Postgres
  | { status: 'rejected'; reason: RejectReason };

/**
 * What a transport (HTTP today, MQTT later) needs from the ingest pipeline.
 * The returned promise resolves once the event is persisted or rejected.
 */
export interface EventIngest {
  ingest(body: unknown): Promise<IngestResult>;
}
