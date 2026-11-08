import { BatchWriter, type BatchResult, type BatchWriterOptions, type InsertedRow } from './batch-writer.js';
import { eventKey, type IngestEvent } from './event.js';

const T0 = Date.parse('2026-05-23T18:00:00.000Z');

const hb = (seq: number, deviceId = 'dev_0001'): IngestEvent => ({
  type: 'heartbeat',
  deviceId,
  roomId: 'room_000',
  seq,
  tsMs: T0 + seq * 1000,
  receivedAtMs: T0 + seq * 1000,
});
const fall = (seq: number): IngestEvent => ({
  type: 'fall_warn',
  deviceId: 'dev_0001',
  roomId: 'room_000',
  seq,
  tsMs: T0 + seq * 1000,
  receivedAtMs: T0 + seq * 1000,
  confidence: 0.9,
});

/**
 * In-memory stand-in for Postgres: unique on (device, seq, ts), sequential ids.
 * `hold()` makes inserts wait until `release()`, to simulate a slow database.
 */
function fakeDb() {
  const keys = new Set<string>();
  let nextId = 1;
  const batches: IngestEvent[][] = [];
  let inFlight = 0;
  let maxInFlight = 0;
  let failNext = 0;
  let gate: Promise<void> | undefined;
  let openGate: (() => void) | undefined;

  const insertBatch = async (events: IngestEvent[]): Promise<BatchResult> => {
    inFlight++;
    maxInFlight = Math.max(maxInFlight, inFlight);
    try {
      await (gate ?? Promise.resolve()); // simulate an async round-trip
      if (failNext > 0) {
        failNext--;
        throw new Error('connection lost');
      }
      batches.push(events);
      const rows: InsertedRow[] = [];
      for (const e of events) {
        const k = eventKey(e);
        if (keys.has(k)) continue;
        keys.add(k);
        rows.push({ id: nextId++, deviceId: e.deviceId, seq: e.seq, tsMs: e.tsMs });
      }
      return { inserted: rows, alarms: [] };
    } finally {
      inFlight--;
    }
  };

  return {
    insertBatch,
    batches,
    get inFlight() {
      return inFlight;
    },
    get maxInFlight() {
      return maxInFlight;
    },
    failTimes(n: number) {
      failNext = n;
    },
    hold() {
      gate = new Promise((resolve) => (openGate = resolve));
    },
    release() {
      gate = undefined;
      openGate?.();
    },
  };
}

function makeWriter(db: ReturnType<typeof fakeDb>, extra: Partial<BatchWriterOptions> = {}) {
  return new BatchWriter({
    insertBatch: db.insertBatch,
    maxBatchSize: 100,
    retryBaseMs: 5,
    retryMaxMs: 20,
    ...extra,
  });
}

describe('BatchWriter', () => {
  it('writes immediately when idle, batching events from the same tick', async () => {
    const db = fakeDb();
    const w = makeWriter(db);
    const results = [w.submit(hb(1)), w.submit(hb(2)), w.submit(hb(3))];

    expect(await Promise.all(results)).toEqual(['inserted', 'inserted', 'inserted']);
    expect(db.batches.map((b) => b.map((e) => e.seq))).toEqual([[1, 2, 3]]);
  });

  it('batches events that arrive while an insert is in flight', async () => {
    const db = fakeDb();
    const w = makeWriter(db);
    db.hold();

    const first = w.submit(hb(1));
    await vi.waitFor(() => expect(db.inFlight).toBe(1)); // INSERT [1] is running
    const later = [w.submit(hb(2)), w.submit(hb(3)), w.submit(hb(4))];
    expect(w.queueDepth()).toEqual({ priority: 0, normal: 3, inFlight: 1 });

    db.release();
    await Promise.all([first, ...later]);
    expect(db.batches.map((b) => b.map((e) => e.seq))).toEqual([[1], [2, 3, 4]]);
  });

  it('caps the batch size and writes the rest in following batches', async () => {
    const db = fakeDb();
    const w = makeWriter(db);
    await Promise.all(Array.from({ length: 250 }, (_, i) => w.submit(hb(i))));
    expect(db.batches.map((b) => b.length)).toEqual([100, 100, 50]);
  });

  it('puts fall_warn into the next batch ahead of queued heartbeats', async () => {
    const db = fakeDb();
    const w = makeWriter(db);
    const heartbeats = Array.from({ length: 500 }, (_, i) => w.submit(hb(i)));
    const alarm = w.submit(fall(10_000));

    await Promise.all([...heartbeats, alarm]);
    expect(db.batches[0][0].type).toBe('fall_warn');
    expect(db.batches[0]).toHaveLength(100);
  });

  it('never has more than one insert in flight', async () => {
    const db = fakeDb();
    const w = makeWriter(db);
    const results: Promise<unknown>[] = [];
    for (let round = 0; round < 5; round++) {
      for (let i = 0; i < 150; i++) results.push(w.submit(hb(round * 1000 + i)));
      await new Promise((r) => setImmediate(r));
    }
    await Promise.all(results);
    expect(db.maxInFlight).toBe(1);
    expect(db.batches.flat()).toHaveLength(750);
  });

  it('reports duplicates, including identical events in the same batch', async () => {
    const db = fakeDb();
    const w = makeWriter(db);

    expect(await w.submit(hb(1))).toBe('inserted');
    const results = [w.submit(hb(1)), w.submit(hb(2)), w.submit(hb(2))];
    expect(await Promise.all(results)).toEqual(['duplicate', 'inserted', 'duplicate']);
  });

  it('treats the same seq with a different ts as a distinct event', async () => {
    const db = fakeDb();
    const w = makeWriter(db);
    const a = hb(1);
    const b = { ...hb(1), tsMs: a.tsMs + 1 };
    expect(await Promise.all([w.submit(a), w.submit(b)])).toEqual(['inserted', 'inserted']);
  });

  it('retries a failed insert with backoff, keeping order and never rejecting', async () => {
    const db = fakeDb();
    const onError = vi.fn();
    // Backoff longer than vi.waitFor's 50ms poll, so the test can see the retry state.
    const w = makeWriter(db, { onError, retryBaseMs: 150, retryMaxMs: 300 });
    db.failTimes(2);

    const results = [w.submit(hb(1)), w.submit(hb(2))];
    await vi.waitFor(() => expect(onError).toHaveBeenCalledTimes(1));
    expect(w.queueDepth().inFlight).toBe(2);
    const late = w.submit(hb(3)); // arrives while retrying

    expect(await Promise.all([...results, late])).toEqual(['inserted', 'inserted', 'inserted']);
    expect(onError).toHaveBeenCalledTimes(2);
    expect(onError.mock.calls[1][1]).toEqual({ attempt: 2, batchSize: 2 });
    expect(db.batches.map((b) => b.map((e) => e.seq))).toEqual([[1, 2], [3]]);
  });

  it('waits for the backoff before retrying', async () => {
    const db = fakeDb();
    const w = makeWriter(db, { retryBaseMs: 40, retryMaxMs: 40 });
    db.failTimes(1);
    const started = performance.now();
    await w.submit(hb(1));
    expect(performance.now() - started).toBeGreaterThanOrEqual(35);
  });

  it('calls onCommitted with only the inserted events, before replying', async () => {
    const db = fakeDb();
    const order: string[] = [];
    const onCommitted = vi.fn(() => order.push('committed'));
    const w = makeWriter(db, { onCommitted });

    await w.submit(hb(1));
    await Promise.all([w.submit(hb(1)), w.submit(hb(2))].map((p) => p.then((r) => order.push(r))));

    expect(onCommitted).toHaveBeenCalledTimes(2);
    expect(onCommitted.mock.calls[1]).toEqual([[{ id: 2, event: hb(2) }], []]);
    expect(order).toEqual(['committed', 'committed', 'duplicate', 'inserted']);
  });

  it('does not call onCommitted when the insert fails', async () => {
    const db = fakeDb();
    const onCommitted = vi.fn();
    const onError = vi.fn();
    const w = makeWriter(db, { onCommitted, onError, retryBaseMs: 150, retryMaxMs: 150 });
    db.failTimes(1);

    const result = w.submit(hb(1));
    await vi.waitFor(() => expect(onError).toHaveBeenCalledTimes(1));
    expect(onCommitted).not.toHaveBeenCalled();
    await result;
    expect(onCommitted).toHaveBeenCalledTimes(1);
  });

  it('reports queue depth per lane before the flush runs', () => {
    const w = makeWriter(fakeDb());
    void w.submit(hb(1));
    void w.submit(hb(2));
    void w.submit(fall(3));
    expect(w.queueDepth()).toEqual({ priority: 1, normal: 2, inFlight: 0 });
  });

  it('drain writes everything queued and then refuses new submits', async () => {
    const db = fakeDb();
    const w = makeWriter(db);
    db.hold();
    const results = Array.from({ length: 250 }, (_, i) => w.submit(hb(i)));

    const drained = w.drain();
    db.release();
    await drained;
    expect(await Promise.all(results)).toHaveLength(250);
    expect(db.batches.flat()).toHaveLength(250);
    expect(() => w.submit(hb(999))).toThrow(/stopped/);
  });

  it('drain resolves immediately when idle', async () => {
    const w = makeWriter(fakeDb());
    await expect(w.drain()).resolves.toBeUndefined();
  });
});
