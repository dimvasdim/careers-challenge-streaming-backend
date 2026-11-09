import { Inject, Injectable } from '@nestjs/common';
import type pg from 'pg';
import { AlarmHub } from '../core/alarm-hub.js';
import type { Alarm } from '../core/fall-incidents.js';
import { PG_POOL } from '../db/database.module.js';
import { listAlarmsSince, loadAlarmsAfter } from '../db/alarms-repository.js';
import { metrics } from '../metrics/metrics.js';

/**
 * Alarms live in Postgres (written with their events, see db/batch-commit.ts).
 * This service reads them and fans newly committed ones out to SSE subscribers.
 */
@Injectable()
export class AlarmsService {
  readonly hub = new AlarmHub();

  constructor(@Inject(PG_POOL) private readonly pool: pg.Pool) {
    metrics.setSubscriberSource(() => this.hub.subscriberCount());
  }

  /** Called by ingest after each commit, before the requests get their 2xx. */
  publish(alarms: Alarm[]): void {
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
