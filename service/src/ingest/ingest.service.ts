import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationShutdown,
} from '@nestjs/common';
import type pg from 'pg';
import { loadConfig } from '../config.js';
import { BatchWriter } from '../core/batch-writer.js';
import { validateEvent } from '../core/validate.js';
import { PG_POOL } from '../db/database.module.js';
import { commitBatch } from '../db/batch-commit.js';
import { metrics } from '../metrics/metrics.js';
import type { EventIngest, IngestResult } from './event-ingest.js';

@Injectable()
export class IngestService implements EventIngest, OnApplicationShutdown {
  private readonly logger = new Logger('Ingest');
  readonly writer: BatchWriter;

  private readonly monitor: NodeJS.Timeout;
  private backpressured = false;
  private rejectedSinceLog = new Map<string, number>();

  constructor(@Inject(PG_POOL) pool: pg.Pool) {
    const config = loadConfig();
    this.writer = new BatchWriter({
      // Events and the alarms their falls create commit in one transaction.
      insertBatch: async (events) => {
        const start = performance.now();
        const result = await commitBatch(pool, events);
        metrics.batchCommitted(events.length, (performance.now() - start) / 1000);
        return result;
      },
      maxBatchSize: config.batchMaxSize,
      onError: (err, { attempt, batchSize }) => {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.warn(
          `batch write failed (attempt ${attempt}, ${batchSize} events): ${message}`,
        );
      },
    });
    metrics.setQueueDepthSource(() => this.writer.queueDepth());
    this.monitor = setInterval(() => this.logStatus(config.backpressureLogDepth), 5_000);
    this.monitor.unref();
  }

  async ingest(body: unknown): Promise<IngestResult> {
    const result = validateEvent(body, Date.now());
    if (!result.ok) {
      this.countRejected(result.reason);
      return { status: 'rejected', reason: result.reason };
    }
    const event = result.event;
    const status = await this.writer.submit(event);
    if (status === 'inserted') metrics.ingested(event.type);
    else metrics.duplicate();
    metrics.persisted(event.type === 'fall_warn', (Date.now() - event.receivedAtMs) / 1000);
    return { status };
  }

  /** Counts a rejected request (also used by the transport for unparsable bodies). */
  countRejected(reason: string): void {
    metrics.rejected(reason);
    this.rejectedSinceLog.set(reason, (this.rejectedSinceLog.get(reason) ?? 0) + 1);
  }

  /**
   * Every 5s: one log line for rejected events (counts by reason, never one
   * line per event) and one when the queue enters or leaves backpressure.
   */
  private logStatus(threshold: number): void {
    if (this.rejectedSinceLog.size > 0) {
      this.logger.warn({ msg: 'rejected events (last 5s)', ...Object.fromEntries(this.rejectedSinceLog) });
      this.rejectedSinceLog = new Map();
    }
    const depth = this.writer.queueDepth();
    const queued = depth.priority + depth.normal;
    if (!this.backpressured && queued >= threshold) {
      this.backpressured = true;
      this.logger.warn({ msg: 'backpressure: ingest queue is growing', ...depth });
    } else if (this.backpressured && queued < threshold / 2) {
      this.backpressured = false;
      this.logger.log({ msg: 'backpressure cleared', ...depth });
    }
  }

  // Runs after the HTTP server has closed (no new requests) and before the
  // global DatabaseModule closes the pool.
  async onApplicationShutdown(): Promise<void> {
    clearInterval(this.monitor);
    const depth = this.writer.queueDepth();
    this.logger.log(
      `draining ingest queue (${depth.priority + depth.normal + depth.inFlight} events)`,
    );
    await this.writer.drain();
  }
}
