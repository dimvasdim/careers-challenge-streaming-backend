import pg from 'pg';
import type { Alarm } from '../core/fall-incidents.js';
import { ALARMS_CHANNEL, loadAlarmsAfter, maxAlarmId } from './alarms-repository.js';

export interface AlarmListenerOptions {
  connectionString: string;
  /** Pool for the catch-up reads. */
  pool: pg.Pool;
  /** New alarms in id order (= commit order), each exactly once. */
  onAlarms: (alarms: Alarm[]) => void;
  onError: (err: unknown) => void;
  reconnectMs?: number;
}

/**
 * Brings alarms committed by ANY worker process to this process's SSE
 * subscribers. Each batch transaction that creates alarms sends
 * `NOTIFY alarms`; on a notification we read `id > lastSeenId` from Postgres.
 * The notification carries no data: Postgres stays the only source, and a
 * missed notification (e.g. while reconnecting) is healed by the next read.
 */
export class AlarmListener {
  private client: pg.Client | undefined;
  private lastSeenId = 0;
  private loading = false;
  private loadAgain = false;
  private stopped = false;
  private reconnectTimer: NodeJS.Timeout | undefined;

  constructor(private readonly opts: AlarmListenerOptions) {}

  /** Starts after the newest existing alarm: older ones are SSE catch-up's job. */
  async start(): Promise<void> {
    this.lastSeenId = await maxAlarmId(this.opts.pool);
    await this.connect();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    clearTimeout(this.reconnectTimer);
    await this.client?.end().catch(() => undefined);
  }

  private async connect(): Promise<void> {
    const client = new pg.Client({ connectionString: this.opts.connectionString });
    this.client = client;
    client.on('notification', () => this.load());
    client.on('error', (err) => {
      this.opts.onError(err);
      this.reconnect(client);
    });
    client.on('end', () => this.reconnect(client));
    await client.connect();
    await client.query(`LISTEN ${ALARMS_CHANNEL}`);
    this.load(); // anything committed before LISTEN was active
  }

  private reconnect(client: pg.Client): void {
    // Only react for the current connection, and only once.
    if (this.stopped || client !== this.client) return;
    this.client = undefined;
    client.end().catch(() => undefined);
    this.reconnectTimer = setTimeout(() => {
      this.connect().catch((err: unknown) => {
        this.opts.onError(err);
        if (this.client) this.reconnect(this.client);
      });
    }, this.opts.reconnectMs ?? 1000);
  }

  /** One read at a time; notifications arriving meanwhile trigger one more read. */
  private load(): void {
    if (this.loading) {
      this.loadAgain = true;
      return;
    }
    this.loading = true;
    loadAlarmsAfter(this.opts.pool, this.lastSeenId)
      .then((alarms) => {
        if (this.stopped || alarms.length === 0) return;
        this.lastSeenId = alarms[alarms.length - 1].id;
        this.opts.onAlarms(alarms);
      })
      .catch((err: unknown) => this.opts.onError(err))
      .finally(() => {
        this.loading = false;
        if (this.loadAgain && !this.stopped) {
          this.loadAgain = false;
          this.load();
        }
      });
  }
}
