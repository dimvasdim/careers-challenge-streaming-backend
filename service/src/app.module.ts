import { Module } from '@nestjs/common';
import { DatabaseModule } from './db/database.module.js';
import { DevicesModule } from './devices/devices.module.js';
import { IngestModule } from './ingest/ingest.module.js';
import { ReadModelsModule } from './read-models/read-models.module.js';

@Module({
  imports: [DatabaseModule, ReadModelsModule, IngestModule, DevicesModule],
})
export class AppModule {}
