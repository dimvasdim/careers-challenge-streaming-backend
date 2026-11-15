import { Global, Inject, Module, type OnApplicationShutdown } from '@nestjs/common';
import pg from 'pg';
import { loadConfig } from '../config.js';

/** Read endpoints, SSE catch-up and the alarm listener's reads. */
export const PG_READ_POOL = Symbol('PG_READ_POOL');
/** The batch writer only: one connection, so a commit never waits behind reads. */
export const PG_WRITE_POOL = Symbol('PG_WRITE_POOL');

// The batch writer has one insert in flight at a time (group commit), so it
// never needs more than one connection; write parallelism comes from workers.
const WRITE_POOL_SIZE = 1;

/**
 * Provides the two pg Pools of this process (DB_POOL_SIZE connections in total).
 * Global modules run their shutdown hooks last, so the pools close only after
 * IngestService has drained its queue.
 */
@Global()
@Module({
  providers: [
    {
      provide: PG_READ_POOL,
      useFactory: () => {
        const config = loadConfig();
        return new pg.Pool({
          connectionString: config.databaseUrl,
          max: config.dbPoolSize - WRITE_POOL_SIZE,
        });
      },
    },
    {
      provide: PG_WRITE_POOL,
      useFactory: () =>
        new pg.Pool({ connectionString: loadConfig().databaseUrl, max: WRITE_POOL_SIZE }),
    },
  ],
  exports: [PG_READ_POOL, PG_WRITE_POOL],
})
export class DatabaseModule implements OnApplicationShutdown {
  constructor(
    @Inject(PG_READ_POOL) private readonly readPool: pg.Pool,
    @Inject(PG_WRITE_POOL) private readonly writePool: pg.Pool,
  ) {}

  async onApplicationShutdown(): Promise<void> {
    await Promise.all([this.readPool.end(), this.writePool.end()]);
  }
}
