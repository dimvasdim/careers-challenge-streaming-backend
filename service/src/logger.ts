import type { LoggerService } from '@nestjs/common';
import pino from 'pino';

/** JSON logs to stdout. LOG_LEVEL overrides the level (default info). */
export const log = pino({ level: process.env.LOG_LEVEL ?? 'info' });

type Level = 'trace' | 'debug' | 'info' | 'warn' | 'error';

/**
 * Routes Nest's logger (including `new Logger('Ingest')` in services) to pino,
 * so every line is JSON with a `context` field. An object message becomes
 * fields of the line: `logger.warn({ msg: 'backpressure', normal: 9000 })`.
 */
export class PinoNestLogger implements LoggerService {
  log(message: unknown, context?: string): void {
    write('info', message, { context });
  }
  warn(message: unknown, context?: string): void {
    write('warn', message, { context });
  }
  error(message: unknown, trace?: string, context?: string): void {
    write('error', message, { context, trace });
  }
  debug(message: unknown, context?: string): void {
    write('debug', message, { context });
  }
  verbose(message: unknown, context?: string): void {
    write('trace', message, { context });
  }
}

function write(level: Level, message: unknown, fields: Record<string, unknown>): void {
  if (typeof message === 'object' && message !== null) {
    log[level]({ ...fields, ...message });
  } else {
    log[level](fields, String(message));
  }
}
