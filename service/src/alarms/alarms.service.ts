import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
} from '@nestjs/common';
import type pg from 'pg';
import { loadConfig } from '../config.js';
import { AlarmHub } from '../core/alarm-hub.js';
import type { Alarm } from '../core/fall-incidents.js';
import { AlarmListener } from '../db/alarm-listener.js';
import { listAlarmsSince, loadAlarmsAfter } from '../db/alarms-repository.js';
import { PG_POOL } from '../db/database.module.js';
import { metrics } from '../metrics/metrics.js';

/**
 * Alarms live in Postgres (written with their events, see db/batch-commit.ts).
 * This service reads them, and fans newly committed ones out to this process's
 * SSE subscribers. New alarms arrive through the AlarmListener, whichever
 * worker process committed them.
 */
@Injectable()
export class AlarmsService implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger('Alarms');
  readonly hub = new AlarmHub();
  private readonly listener: AlarmListener;

  constructor(@Inject(PG_POOL) private readonly pool: pg.Pool) {
    metrics.setSubscriberSource(() => this.hub.subscriberCount());
    this.listener = new AlarmListener({
      connectionString: loadConfig().databaseUrl,
      pool,
      onAlarms: (alarms) => this.publish(alarms),
      onError: (err) =>
        this.logger.warn(
          `alarm listener: ${err instanceof Error ? err.message : String(err)}`,
        ),
    });
  }

  // Before the HTTP server accepts requests, so no alarm is missed.
  onApplicationBootstrap(): Promise<void> {
    return this.listener.start();
  }

  // Shutdown hooks of the global DatabaseModule run later: the pool is still open.
  onApplicationShutdown(): Promise<void> {
    return this.listener.stop();
  }

  private publish(alarms: Alarm[]): void {
    this.hub.publish(alarms);
    const now = Date.now();
    for (const a of alarms) metrics.alarmPushed((now - a.receivedAtMs) / 1000);
  }

  listSince(sinceMs: number): Promise<Alarm[]> {
    return metrics.timeRead('alarms', () => listAlarmsSince(this.pool, sinceMs));
  }

  loadAfter(afterId: number): Promise<Alarm[]> {
    return loadAlarmsAfter(this.pool, afterId);
  }
}
