import cluster from 'node:cluster';
import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { AppModule } from './app.module.js';
import { runPrimary } from './cluster.js';
import { type Config, loadConfig } from './config.js';
import { log, PinoNestLogger } from './logger.js';

async function bootstrap(config: Config) {
  const app = await NestFactory.create<NestFastifyApplication>(AppModule, new FastifyAdapter(), {
    logger: new PinoNestLogger(), // JSON logs; Fastify's per-request logging stays off
  });
  app.enableShutdownHooks(); // SIGTERM/SIGINT: close HTTP, drain ingest queue, close pool
  await app.listen(config.port, '0.0.0.0');
  log.info(
    { port: config.port, dbPoolSize: config.dbPoolSize, batchMaxSize: config.batchMaxSize },
    'service started',
  );
}

const config = loadConfig();
if (config.workers > 1 && cluster.isPrimary) runPrimary(config);
else void bootstrap(config);
