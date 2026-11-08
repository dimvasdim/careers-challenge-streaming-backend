// Runtime settings from environment variables. Read when needed (not at import)
// so tests can set process.env first.

export interface Config {
  port: number;
  databaseUrl: string;
  dbPoolSize: number;
  batchMaxSize: number;
  sseKeepaliveMs: number;
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

export function loadConfig(): Config {
  return {
    port: int('PORT', 8080),
    databaseUrl:
      process.env.DATABASE_URL ?? 'postgres://teton:teton@localhost:5433/teton',
    // The batch writer uses one connection at a time (one insert in flight);
    // reads are served from memory. One spare for alarm writes / startup queries.
    dbPoolSize: int('DB_POOL_SIZE', 2),
    batchMaxSize: int('BATCH_MAX_SIZE', 2000),
    // SSE keepalive comment, so idle connections survive proxies and load balancers.
    sseKeepaliveMs: int('SSE_KEEPALIVE_MS', 15_000),
  };
}
