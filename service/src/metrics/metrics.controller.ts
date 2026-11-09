import cluster from 'node:cluster';
import { Controller, Get, Header } from '@nestjs/common';
import { loadConfig } from '../config.js';
import { metrics } from './metrics.js';

@Controller('metrics')
export class MetricsController {
  private readonly primaryUrl = `http://127.0.0.1:${loadConfig().metricsPort}/`;

  /**
   * Prometheus text format. In a worker process, the primary's merged metrics
   * of all workers (a scrape hits one random worker); otherwise this process's.
   */
  @Get()
  @Header('Content-Type', metrics.register.contentType)
  async scrape(): Promise<string> {
    if (!cluster.isWorker) return metrics.register.metrics();
    const res = await fetch(this.primaryUrl);
    if (!res.ok) throw new Error(`metrics from primary: HTTP ${res.status}`);
    return res.text();
  }
}
