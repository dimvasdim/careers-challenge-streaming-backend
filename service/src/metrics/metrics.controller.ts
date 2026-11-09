import { Controller, Get, Header } from '@nestjs/common';
import { metrics } from './metrics.js';

@Controller('metrics')
export class MetricsController {
  /** Prometheus text format. */
  @Get()
  @Header('Content-Type', metrics.register.contentType)
  scrape(): Promise<string> {
    return metrics.register.metrics();
  }
}
