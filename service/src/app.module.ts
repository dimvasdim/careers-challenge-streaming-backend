import { Module } from '@nestjs/common';
import { DatabaseModule } from './db/database.module.js';
import { IngestModule } from './ingest/ingest.module.js';

@Module({
  imports: [DatabaseModule, IngestModule],
})
export class AppModule {}
