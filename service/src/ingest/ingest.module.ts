import { Module } from '@nestjs/common';
import { AlarmsModule } from '../alarms/alarms.module.js';
import { HttpIngestTransport } from './http-transport.js';
import { IngestService } from './ingest.service.js';

@Module({
  imports: [AlarmsModule],
  providers: [IngestService, HttpIngestTransport],
  exports: [IngestService],
})
export class IngestModule {}
