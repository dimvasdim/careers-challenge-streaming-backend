import { Inject, Injectable, Logger, type OnModuleInit } from '@nestjs/common';
import type pg from 'pg';
import { AlarmWriter } from '../core/alarm-writer.js';
import {
  ALARM_HISTORY_MS,
  FALL_DEDUP_WINDOW_MS,
  type FallChange,
} from '../core/fall-incidents.js';
import { ReadModels } from '../core/read-models.js';
import { PG_POOL } from '../db/database.module.js';
import {
  loadAlarmsAfter,
  loadRecentAlarms,
  upsertAlarms,
} from '../db/alarms-repository.js';

/** Persists fall incidents, then publishes them to the in-memory feed (and SSE). */
@Injectable()
export class AlarmsService implements OnModuleInit {
  private readonly logger = new Logger('Alarms');
  private readonly writer: AlarmWriter;

  constructor(
    @Inject(PG_POOL) private readonly pool: pg.Pool,
    private readonly readModels: ReadModels,
  ) {
    this.writer = new AlarmWriter({
      upsertAlarms: (alarms) => upsertAlarms(pool, alarms),
      // Only after the commit: subscribers never see an alarm Postgres doesn't have.
      onPersisted: (alarms) => readModels.alarmFeed.publish(alarms, Date.now()),
      onError: (err, { attempt, batchSize }) => {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.warn(
          `alarm write failed (attempt ${attempt}, ${batchSize} alarms): ${message}`,
        );
      },
    });
  }

  // Runs before the HTTP server listens: dedup state and the feed survive a restart.
  async onModuleInit(): Promise<void> {
    const started = Date.now();
    const fromMs = started - ALARM_HISTORY_MS - FALL_DEDUP_WINDOW_MS;
    const { alarms, maxOlderId } = await loadRecentAlarms(this.pool, fromMs);
    for (const alarm of alarms) this.readModels.fallIncidents.seed(alarm);
    this.readModels.alarmFeed.load(alarms, maxOlderId);
    this.logger.log(
      `loaded ${alarms.length} recent alarms in ${Date.now() - started} ms`,
    );
  }

  enqueue(changes: FallChange[]): void {
    this.writer.enqueue(changes);
  }

  loadAfter(afterId: number) {
    return loadAlarmsAfter(this.pool, afterId);
  }

  /** Called by IngestService on shutdown, after the event queue is drained. */
  drain(): Promise<void> {
    return this.writer.drain();
  }
}
