import { z } from 'zod';
import { InvalidPayloadError, NonRetryableError } from '../errors.js';
import type { HandlerRegistry, JobContext } from '../registry.js';

/**
 * Built-in handlers. This module is the extension point for real work:
 * register one handler per queue here (or build your own HandlerRegistry and
 * start the worker programmatically). Handlers must be idempotent: delivery
 * is at-least-once.
 */
export function registerBuiltinHandlers(registry: HandlerRegistry): HandlerRegistry {
  return registry.register('echo', echoHandler).register('demo', demoHandler);
}

/** Logs the payload. Handy for smoke-testing a deployment end to end. */
export async function echoHandler(ctx: JobContext): Promise<void> {
  ctx.logger.info({ payload: ctx.job.payload }, 'echo');
}

const demoPayload = z
  .object({
    /** Simulated work. */
    durationMs: z.number().int().min(0).max(600_000).default(0),
    /** Throw a retryable error on every attempt below this number. */
    failUntilAttempt: z.number().int().min(1).max(100).optional(),
    /** Fail without retries. */
    nonRetryable: z.boolean().default(false),
  })
  .strict();

/**
 * Exercises the worker's failure paths from the API, e.g.
 *   POST /queues/demo/jobs {"payload":{"durationMs":500,"failUntilAttempt":3},"maxAttempts":5}
 */
export async function demoHandler(ctx: JobContext): Promise<void> {
  const parsed = demoPayload.safeParse(ctx.job.payload);
  if (!parsed.success) {
    // A malformed payload will be just as malformed next time: dead-letter it
    // at once with reason invalid_payload instead of burning retries.
    throw new InvalidPayloadError(`Invalid demo payload: ${parsed.error.message}`);
  }
  const { durationMs, failUntilAttempt, nonRetryable } = parsed.data;
  await abortableDelay(durationMs, ctx.signal);
  if (nonRetryable) throw new NonRetryableError('Demo non-retryable failure');
  if (failUntilAttempt !== undefined && ctx.job.attempt < failUntilAttempt) {
    throw new Error(`Demo failure on attempt ${ctx.job.attempt}`);
  }
}

/** setTimeout that honours an AbortSignal, the pattern every long handler should follow. */
export function abortableDelay(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(signal.reason as Error);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal.reason as Error);
    };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}
