import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationShutdown,
} from '@nestjs/common';
import type pg from 'pg';
import { AlarmsService } from '../alarms/alarms.service.js';
import { loadConfig } from '../config.js';
import { BatchWriter } from '../core/batch-writer.js';
import { ReadModels } from '../core/read-models.js';
import { validateEvent } from '../core/validate.js';
import { PG_POOL } from '../db/database.module.js';
import { insertEvents } from '../db/events-repository.js';
import type { EventIngest, IngestResult } from './event-ingest.js';

@Injectable()
export class IngestService implements EventIngest, OnApplicationShutdown {
  private readonly logger = new Logger('Ingest');
  readonly writer: BatchWriter;

  constructor(
    @Inject(PG_POOL) pool: pg.Pool,
    readModels: ReadModels,
    private readonly alarms: AlarmsService,
  ) {
    const config = loadConfig();
    this.writer = new BatchWriter({
      insertBatch: (events) => insertEvents(pool, events),
      // Committed, non-duplicate events only; applied before the requests get their 2xx.
      // New falls go on to be persisted as alarms, then pushed to subscribers.
      onCommitted: (committed) => alarms.enqueue(readModels.apply(committed)),
      maxBatchSize: config.batchMaxSize,
      onError: (err, { attempt, batchSize }) => {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.warn(
          `batch write failed (attempt ${attempt}, ${batchSize} events): ${message}`,
        );
      },
    });
  }

  async ingest(body: unknown): Promise<IngestResult> {
    const result = validateEvent(body, Date.now());
    if (!result.ok) return { status: 'rejected', reason: result.reason };
    return { status: await this.writer.submit(result.event) };
  }

  // Runs after the HTTP server has closed (no new requests) and before the
  // global DatabaseModule closes the pool.
  async onApplicationShutdown(): Promise<void> {
    const depth = this.writer.queueDepth();
    this.logger.log(
      `draining ingest queue (${depth.priority + depth.normal + depth.inFlight} events)`,
    );
    await this.writer.drain();
    await this.alarms.drain(); // alarms created by the last batches
  }
}
