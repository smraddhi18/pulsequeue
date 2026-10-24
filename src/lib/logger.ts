import pino, { type Logger, type LoggerOptions } from 'pino';
import type { Env } from '../config/env.js';

/**
 * Structured JSON logger. Always emits JSON (no pretty transport) so local
 * and production behaviour match what we ship to log aggregators.
 */
export function createLogger(env: Pick<Env, 'NODE_ENV' | 'LOG_LEVEL'>): Logger {
  const options: LoggerOptions = {
    level: env.LOG_LEVEL,
    base: {
      service: 'pulsequeue',
      env: env.NODE_ENV,
    },
    timestamp: pino.stdTimeFunctions.isoTime,
  };

  return pino(options);
}

export type { Logger };

/**
 * The logging surface shared components need. Both a pino Logger (worker,
 * scheduler) and Fastify's request-independent `app.log` (API) satisfy it,
 * so a component wired into every process does not care which one it gets.
 */
export interface StructuredLogger {
  debug: Logger['debug'];
  info: Logger['info'];
  warn: Logger['warn'];
  error: Logger['error'];
  child(bindings: Record<string, unknown>): StructuredLogger;
}
