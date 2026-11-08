import { Global, Inject, Module, type OnApplicationShutdown } from '@nestjs/common';
import pg from 'pg';
import { loadConfig } from '../config.js';

export const PG_POOL = Symbol('PG_POOL');

/**
 * Provides the shared pg Pool. Global modules run their shutdown hooks last,
 * so the pool closes only after IngestService has drained its queue.
 */
@Global()
@Module({
  providers: [
    {
      provide: PG_POOL,
      useFactory: () => {
        const config = loadConfig();
        return new pg.Pool({ connectionString: config.databaseUrl, max: config.dbPoolSize });
      },
    },
  ],
  exports: [PG_POOL],
})
export class DatabaseModule implements OnApplicationShutdown {
  constructor(@Inject(PG_POOL) private readonly pool: pg.Pool) {}

  async onApplicationShutdown(): Promise<void> {
    await this.pool.end();
  }
}
