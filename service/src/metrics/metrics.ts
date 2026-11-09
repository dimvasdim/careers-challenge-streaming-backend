import client from 'prom-client';
import { EVENT_TYPES, type EventType } from '../core/event.js';

// All metrics live in prom-client's global registry. With several worker
// processes, the primary can merge the workers' registries
// (prom-client AggregatorRegistry) without changes here.
// Defined once at import, so several app instances in one test process share them.

const register = client.register;
client.collectDefaultMetrics({ register }); // includes nodejs_eventloop_lag_* (event-loop lag)

const ingested = new client.Counter({
  name: 'events_ingested_total',
  help: 'Events persisted (new rows), by type',
  labelNames: ['type'],
});
// Children bound once: the per-event path does no label lookups.
const ingestedByType = new Map(EVENT_TYPES.map((t) => [t, ingested.labels(t)]));

const rejected = new client.Counter({
  name: 'events_rejected_total',
  help: 'Events rejected with 400, by reason',
  labelNames: ['reason'],
});

const duplicates = new client.Counter({
  name: 'events_duplicate_total',
  help: 'Events already in Postgres (same device_id, ts, seq), acknowledged and dropped',
});

const persistLatency = new client.Histogram({
  name: 'ingest_persist_seconds',
  help: 'Time from request validation to commit (the 2xx), by lane',
  labelNames: ['lane'],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
});
const persistPriority = persistLatency.labels('priority');
const persistNormal = persistLatency.labels('normal');

const batchSize = new client.Histogram({
  name: 'ingest_batch_size',
  help: 'Events per committed batch',
  buckets: [1, 10, 50, 100, 250, 500, 1000, 2000],
});

const batchCommit = new client.Histogram({
  name: 'ingest_batch_commit_seconds',
  help: 'Duration of one batch transaction (insert events + alarms + COMMIT)',
  buckets: [0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
});

const alarmLatency = new client.Histogram({
  name: 'alarm_latency_seconds',
  help: 'Time from fall_warn ingest to the alarm being pushed to SSE subscribers',
  buckets: [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 2, 5],
});

const readLatency = new client.Histogram({
  name: 'read_query_seconds',
  help: 'Read endpoint query duration, by endpoint',
  labelNames: ['endpoint'],
  buckets: [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25],
});

// Gauges read their value when /metrics is scraped, from sources set at startup.
type QueueDepth = { priority: number; normal: number; inFlight: number };
let queueDepthSource: (() => QueueDepth) | undefined;
let subscriberSource: (() => number) | undefined;

new client.Gauge({
  name: 'ingest_queue_depth',
  help: 'Events waiting to be written, by lane (in_flight = batch being written)',
  labelNames: ['lane'],
  collect() {
    const depth = queueDepthSource?.() ?? { priority: 0, normal: 0, inFlight: 0 };
    this.set({ lane: 'priority' }, depth.priority);
    this.set({ lane: 'normal' }, depth.normal);
    this.set({ lane: 'in_flight' }, depth.inFlight);
  },
});

new client.Gauge({
  name: 'sse_subscribers',
  help: 'Open SSE alarm stream connections',
  collect() {
    this.set(subscriberSource?.() ?? 0);
  },
});

export const metrics = {
  register,

  ingested(type: EventType): void {
    ingestedByType.get(type)?.inc();
  },
  /** A RejectReason, or a Fastify body error code for unparsable requests. */
  rejected(reason: string): void {
    rejected.labels(reason).inc();
  },
  duplicate(): void {
    duplicates.inc();
  },
  persisted(isFall: boolean, seconds: number): void {
    (isFall ? persistPriority : persistNormal).observe(seconds);
  },
  batchCommitted(size: number, seconds: number): void {
    batchSize.observe(size);
    batchCommit.observe(seconds);
  },
  alarmPushed(seconds: number): void {
    alarmLatency.observe(seconds);
  },
  /** Times a read query: `await metrics.timeRead('health', () => query())`. */
  async timeRead<T>(endpoint: string, query: () => Promise<T>): Promise<T> {
    const end = readLatency.labels(endpoint).startTimer();
    try {
      return await query();
    } finally {
      end();
    }
  },

  setQueueDepthSource(source: () => QueueDepth): void {
    queueDepthSource = source;
  },
  setSubscriberSource(source: () => number): void {
    subscriberSource = source;
  },
};
