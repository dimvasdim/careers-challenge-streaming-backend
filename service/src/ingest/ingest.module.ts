import { Module } from '@nestjs/common';
import { HttpIngestTransport } from './http-transport.js';
import { IngestService } from './ingest.service.js';

@Module({
  providers: [IngestService, HttpIngestTransport],
  exports: [IngestService],
})
export class IngestModule {}
