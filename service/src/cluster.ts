import cluster from 'node:cluster';
import http from 'node:http';
import { AggregatorRegistry } from 'prom-client';
import type { Config } from './config.js';
import { log } from './logger.js';

const RESTART_DELAY_MS = 1000;

/**
 * The primary process when WORKERS > 1. It runs no app code: it forks the
 * workers (each a full app on the shared port), replaces any that crash,
 * forwards SIGTERM/SIGINT so every worker drains its ingest queue, and serves
 * the workers' merged metrics on localhost:METRICS_PORT.
 *
 * Workers share nothing in memory: all state is in Postgres, and new alarms
 * reach every worker's SSE clients via LISTEN/NOTIFY (db/alarm-listener.ts).
 */
export function runPrimary(config: Config): void {
  let stopping = false;
  // Counted here rather than read from cluster.workers: a worker can still be
  // listed there when its 'exit' event fires (it leaves after 'disconnect').
  let running = 0;

  const fork = () => {
    running++;
    cluster.fork();
  };
  for (let i = 0; i < config.workers; i++) fork();

  cluster.on('exit', (worker, code, signal) => {
    running--;
    if (stopping) {
      if (running === 0) {
        log.info('all workers stopped');
        process.exit(0);
      }
      return;
    }
    log.error({ workerPid: worker.process.pid, code, signal }, 'worker died, starting a new one');
    setTimeout(() => {
      if (!stopping) fork();
    }, RESTART_DELAY_MS);
  });

  const stop = (signal: NodeJS.Signals) => {
    if (stopping) return;
    stopping = true;
    log.info({ signal }, 'stopping workers');
    if (running === 0) process.exit(0);
    for (const worker of Object.values(cluster.workers ?? {})) worker?.process.kill('SIGTERM');
  };
  process.on('SIGTERM', stop);
  process.on('SIGINT', stop);

  const registry = new AggregatorRegistry();
  http
    .createServer((_req, res) => {
      registry
        .clusterMetrics()
        .then((text) => {
          res.writeHead(200, { 'content-type': registry.contentType }).end(text);
        })
        .catch((err: Error) => {
          res.writeHead(500).end(err.message);
        });
    })
    .listen(config.metricsPort, '127.0.0.1');

  log.info({ workers: config.workers, port: config.port }, 'primary started');
}
