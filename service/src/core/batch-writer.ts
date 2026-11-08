import { eventKey, type IngestEvent } from './event.js';

/** A row the database reports as newly inserted (absent = duplicate). */
export interface InsertedRow {
  id: number;
  deviceId: string;
  seq: number;
  tsMs: number;
}

/** An event that is now durable, with its events-table id. */
export interface CommittedEvent {
  id: number;
  event: IngestEvent;
}

export type SubmitOutcome = 'inserted' | 'duplicate';

export interface BatchWriterOptions {
  /** Inserts the batch in one statement; returns only the rows actually inserted. */
  insertBatch: (events: IngestEvent[]) => Promise<InsertedRow[]>;
  /** Called after each commit with the newly inserted events, in batch order. */
  onCommitted?: (committed: CommittedEvent[]) => void;
  /** Called when an insert fails (the batch is retried) or onCommitted throws. */
  onError?: (err: unknown, context: { attempt: number; batchSize: number }) => void;
  maxBatchSize?: number;
  retryBaseMs?: number;
  retryMaxMs?: number;
}

interface Pending {
  event: IngestEvent;
  resolve: (outcome: SubmitOutcome) => void;
}

/**
 * Group commit: buffers events in two lanes and writes them to the database
 * in batches, one batch at a time. A submit() promise resolves only after its
 * batch is committed, so callers can treat resolution as "persisted".
 *
 * - fall_warn goes to the priority lane and is always taken first.
 * - Flush when idle, batch while busy: if no insert is running, the queue is
 *   written on the next event-loop turn (events from the same turn share it).
 *   While an insert is in flight, new events queue up and form the next batch,
 *   so batch size grows with load without a fixed wait when the system is quiet.
 * - Insert failures are retried with capped backoff. Submits never reject.
 */
export class BatchWriter {
  private readonly priority: Pending[] = [];
  private readonly normal: Pending[] = [];
  private retryBatch: Pending[] | undefined;
  private failedAttempts = 0;
  private inFlightCount = 0;

  private scheduled = false;
  private flushing = false;
  private stopped = false;
  private idleWaiters: Array<() => void> = [];

  private readonly maxBatchSize: number;
  private readonly retryBaseMs: number;
  private readonly retryMaxMs: number;

  constructor(private readonly opts: BatchWriterOptions) {
    this.maxBatchSize = opts.maxBatchSize ?? 2000;
    this.retryBaseMs = opts.retryBaseMs ?? 50;
    this.retryMaxMs = opts.retryMaxMs ?? 1000;
  }

  submit(event: IngestEvent): Promise<SubmitOutcome> {
    if (this.stopped) throw new Error('BatchWriter is stopped');
    return new Promise((resolve) => {
      const lane = event.type === 'fall_warn' ? this.priority : this.normal;
      lane.push({ event, resolve });
      this.schedule(0);
    });
  }

  /** Events waiting to be written; `inFlight` is the batch being written or retried. */
  queueDepth(): { priority: number; normal: number; inFlight: number } {
    return {
      priority: this.priority.length,
      normal: this.normal.length,
      inFlight: this.inFlightCount,
    };
  }

  /** Writes everything still queued, then refuses new submits. */
  async drain(): Promise<void> {
    this.stopped = true;
    if (this.isIdle()) return;
    this.schedule(0);
    await new Promise<void>((resolve) => this.idleWaiters.push(resolve));
  }

  private isIdle(): boolean {
    return (
      !this.flushing && !this.retryBatch && this.priority.length === 0 && this.normal.length === 0
    );
  }

  /** delayMs > 0 only for retry backoff; otherwise flush on the next event-loop turn. */
  private schedule(delayMs: number): void {
    if (this.scheduled || this.flushing) return; // a flush is already coming
    this.scheduled = true;
    const run = () => {
      this.scheduled = false;
      void this.flush();
    };
    if (delayMs > 0) setTimeout(run, delayMs);
    else setImmediate(run);
  }

  private takeBatch(): Pending[] {
    const fromPriority = this.priority.splice(0, this.maxBatchSize);
    const room = this.maxBatchSize - fromPriority.length;
    const fromNormal = room > 0 ? this.normal.splice(0, room) : [];
    return fromPriority.concat(fromNormal);
  }

  private async flush(): Promise<void> {
    this.flushing = true;
    let nextDelay = 0;
    try {
      const batch = this.retryBatch ?? this.takeBatch();
      this.retryBatch = undefined;
      if (batch.length === 0) return;
      this.inFlightCount = batch.length;

      let inserted: InsertedRow[];
      try {
        inserted = await this.opts.insertBatch(batch.map((p) => p.event));
      } catch (err) {
        this.failedAttempts++;
        this.retryBatch = batch; // retry the same batch, keeping order
        this.opts.onError?.(err, { attempt: this.failedAttempts, batchSize: batch.length });
        nextDelay = Math.min(this.retryBaseMs * 2 ** (this.failedAttempts - 1), this.retryMaxMs);
        return;
      }
      this.failedAttempts = 0;
      this.inFlightCount = 0;
      this.settle(batch, inserted);
    } finally {
      this.flushing = false;
      if (this.isIdle()) {
        for (const wake of this.idleWaiters.splice(0)) wake();
      } else {
        this.schedule(nextDelay);
      }
    }
  }

  /** Matches inserted rows back to requests, applies them, then replies. */
  private settle(batch: Pending[], inserted: InsertedRow[]): void {
    const idByKey = new Map<string, number>();
    for (const row of inserted) idByKey.set(eventKey(row), row.id);

    const outcomes: SubmitOutcome[] = [];
    const committed: CommittedEvent[] = [];
    for (const p of batch) {
      const key = eventKey(p.event);
      const id = idByKey.get(key);
      if (id === undefined) {
        outcomes.push('duplicate');
      } else {
        idByKey.delete(key); // an identical copy later in the batch is a duplicate
        committed.push({ id, event: p.event });
        outcomes.push('inserted');
      }
    }

    // Apply to memory before replying, so a 2xx means the event is also readable.
    if (committed.length > 0 && this.opts.onCommitted) {
      try {
        this.opts.onCommitted(committed);
      } catch (err) {
        this.opts.onError?.(err, { attempt: 0, batchSize: batch.length });
      }
    }
    batch.forEach((p, i) => p.resolve(outcomes[i]));
  }
}
