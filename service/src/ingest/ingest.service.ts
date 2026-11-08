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
import { validateEvent } from '../core/validate.js';
import { PG_POOL } from '../db/database.module.js';
import { commitBatch } from '../db/batch-commit.js';
import type { EventIngest, IngestResult } from './event-ingest.js';

@Injectable()
export class IngestService implements EventIngest, OnApplicationShutdown {
  private readonly logger = new Logger('Ingest');
  readonly writer: BatchWriter;

  constructor(@Inject(PG_POOL) pool: pg.Pool, alarms: AlarmsService) {
    const config = loadConfig();
    this.writer = new BatchWriter({
      // Events and the alarms their falls create commit in one transaction.
      insertBatch: (events) => commitBatch(pool, events),
      // After the commit, before the requests get their 2xx: push new alarms live.
      onCommitted: (_committed, newAlarms) => alarms.publish(newAlarms),
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
  }
}
