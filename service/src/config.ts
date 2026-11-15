// Runtime settings from environment variables. Read when needed (not at import)
// so tests can set process.env first.

import { availableParallelism } from 'node:os';

export interface Config {
  port: number;
  workers: number;
  metricsPort: number;
  databaseUrl: string;
  dbPoolSize: number;
  batchMaxSize: number;
  sseKeepaliveMs: number;
  backpressureLogDepth: number;
}

function int(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer, got "${raw}"`);
  }
  return value;
}

function atLeast(name: string, value: number, min: number): number {
  if (value < min) throw new Error(`${name} must be at least ${min}, got ${value}`);
  return value;
}

export function loadConfig(): Config {
  return {
    port: int('PORT', 8080),
    // Worker processes sharing the port, each with its own batch writer, so
    // several batch inserts run at once. 1 = a single process, no cluster.
    workers: int('WORKERS', Math.min(availableParallelism(), 4)),
    // Primary process only (WORKERS > 1): merged metrics of all workers, on
    // localhost. Each worker's /metrics returns them.
    metricsPort: int('METRICS_PORT', 9464),
    databaseUrl:
      process.env.DATABASE_URL ?? 'postgres://teton:teton@localhost:5433/teton',
    // Per worker, at least 2: one connection in the batch writer's own pool (one
    // insert in flight), the rest in the read pool, so a batch commit never
    // waits behind reads. Plus one connection per worker for the AlarmListener.
    dbPoolSize: atLeast('DB_POOL_SIZE', int('DB_POOL_SIZE', 4), 2),
    batchMaxSize: int('BATCH_MAX_SIZE', 2000),
    // SSE keepalive comment, so idle connections survive proxies and load balancers.
    sseKeepaliveMs: int('SSE_KEEPALIVE_MS', 15_000),
    // Queued events at which a "backpressure" warning is logged (cleared below half).
    backpressureLogDepth: int('BACKPRESSURE_LOG_DEPTH', 5_000),
  };
}
