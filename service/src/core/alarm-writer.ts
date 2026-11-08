import type { Alarm, FallChange } from './fall-incidents.js';

export interface AlarmWriterOptions {
  /**
   * Upserts the alarms in one statement (new rows inserted, existing ones get
   * the higher confidence) and returns the id of every row.
   */
  upsertAlarms: (
    alarms: Alarm[],
  ) => Promise<Array<{ id: number; eventId: string }>>;
  /** Called after each commit with the NEW alarms, ids set, sorted by id. */
  onPersisted: (alarms: Alarm[]) => void;
  onError?: (
    err: unknown,
    context: { attempt: number; batchSize: number },
  ) => void;
  retryBaseMs?: number;
  retryMaxMs?: number;
}

/**
 * Persists alarms before anyone is told about them. Same group commit as the
 * event BatchWriter: write on the next event-loop turn when idle, batch while
 * an upsert is in flight (one at a time), retry the same batch with capped
 * backoff on failure.
 *
 * The upsert is idempotent: a retried batch whose first attempt actually
 * committed still returns every row's id, so new alarms are published exactly
 * once from here.
 */
export class AlarmWriter {
  /** Alarms waiting to be written, by eventId; `isNew` = not yet published. */
  private pending = new Map<string, { alarm: Alarm; isNew: boolean }>();
  private retryBatch: Array<{ alarm: Alarm; isNew: boolean }> | undefined;
  private failedAttempts = 0;
  private scheduled = false;
  private flushing = false;
  private idleWaiters: Array<() => void> = [];

  private readonly retryBaseMs: number;
  private readonly retryMaxMs: number;

  constructor(private readonly opts: AlarmWriterOptions) {
    this.retryBaseMs = opts.retryBaseMs ?? 50;
    this.retryMaxMs = opts.retryMaxMs ?? 1000;
  }

  enqueue(changes: FallChange[]): void {
    for (const { kind, alarm } of changes) {
      const queued = this.pending.get(alarm.eventId);
      // A raise of an alarm still queued as new is covered by that insert:
      // the shared object already carries the higher confidence.
      if (queued) queued.isNew ||= kind === 'new';
      else this.pending.set(alarm.eventId, { alarm, isNew: kind === 'new' });
    }
    if (changes.length > 0) this.schedule(0);
  }

  /** Alarms waiting or being written. */
  depth(): number {
    return this.pending.size + (this.retryBatch?.length ?? 0);
  }

  /** Resolves once everything queued so far is written. */
  async drain(): Promise<void> {
    if (this.isIdle()) return;
    await new Promise<void>((resolve) => this.idleWaiters.push(resolve));
  }

  private isIdle(): boolean {
    return !this.flushing && !this.retryBatch && this.pending.size === 0;
  }

  private schedule(delayMs: number): void {
    if (this.scheduled || this.flushing) return;
    this.scheduled = true;
    const run = () => {
      this.scheduled = false;
      void this.flush();
    };
    if (delayMs > 0) setTimeout(run, delayMs);
    else setImmediate(run);
  }

  private async flush(): Promise<void> {
    this.flushing = true;
    let nextDelay = 0;
    try {
      let batch = this.retryBatch;
      this.retryBatch = undefined;
      if (!batch) {
        batch = [...this.pending.values()];
        this.pending.clear();
      }
      if (batch.length === 0) return;
      // Ids follow insert order, so a batch's new alarms get ids in ts order.
      batch.sort((x, y) => x.alarm.tsMs - y.alarm.tsMs);

      let rows: Array<{ id: number; eventId: string }>;
      try {
        rows = await this.opts.upsertAlarms(batch.map((p) => p.alarm));
      } catch (err) {
        this.failedAttempts++;
        this.retryBatch = batch;
        this.opts.onError?.(err, {
          attempt: this.failedAttempts,
          batchSize: batch.length,
        });
        nextDelay = Math.min(
          this.retryBaseMs * 2 ** (this.failedAttempts - 1),
          this.retryMaxMs,
        );
        return;
      }
      this.failedAttempts = 0;

      const idByEventId = new Map(rows.map((r) => [r.eventId, r.id]));
      const persisted: Alarm[] = [];
      for (const { alarm, isNew } of batch) {
        const id = idByEventId.get(alarm.eventId);
        if (id === undefined) continue;
        alarm.id = id;
        if (isNew) persisted.push(alarm);
      }
      persisted.sort((x, y) => x.id - y.id);
      if (persisted.length > 0) {
        try {
          this.opts.onPersisted(persisted);
        } catch (err) {
          this.opts.onError?.(err, { attempt: 0, batchSize: batch.length });
        }
      }
    } finally {
      this.flushing = false;
      if (this.isIdle()) {
        for (const wake of this.idleWaiters.splice(0)) wake();
      } else {
        this.schedule(nextDelay);
      }
    }
  }
}
