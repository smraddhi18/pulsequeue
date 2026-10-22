/**
 * Errors that drive worker decisions. Handlers throw NonRetryableError to
 * skip remaining attempts; the others are abort reasons the worker sets on a
 * job's AbortSignal, so a cooperative handler can tell why it was stopped.
 */

/** Throw from a handler when retrying cannot help (bad payload, 4xx from a downstream). */
export class NonRetryableError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'NonRetryableError';
  }
}

/**
 * Throw from a handler when the payload itself is unusable (fails schema
 * validation, references something that cannot exist). Non-retryable, and
 * dead-lettered with reason invalid_payload so operators can tell "fix the
 * producer" apart from "fix the handler or the downstream".
 */
export class InvalidPayloadError extends NonRetryableError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'InvalidPayloadError';
  }
}

/**
 * Poison-message classification for a thrown value. Besides the typed
 * classes, any error carrying `retryable: false` counts as non-retryable:
 * handlers can flag errors from libraries (or from another copy of this
 * package, where instanceof fails) without wrapping them.
 */
export function classifyNonRetryable(err: unknown): 'invalid_payload' | 'non_retryable' | null {
  if (err instanceof InvalidPayloadError) return 'invalid_payload';
  if (err instanceof NonRetryableError) return 'non_retryable';
  if (
    typeof err === 'object' &&
    err !== null &&
    (err as { retryable?: unknown }).retryable === false
  ) {
    return 'non_retryable';
  }
  return null;
}

/** The attempt exceeded WORKER_JOB_TIMEOUT_MS. Recorded as timed_out and retried. */
export class JobTimeoutError extends Error {
  constructor(readonly timeoutMs: number) {
    super(`Job attempt timed out after ${timeoutMs}ms`);
    this.name = 'JobTimeoutError';
  }
}

/**
 * A heartbeat found the lease gone (it expired and was reaped, maybe
 * re-leased elsewhere). The worker stops and writes nothing: the job's
 * outcome now belongs to whoever holds the new lease.
 */
export class LeaseLostError extends Error {
  constructor() {
    super('Lease lost: the job was reclaimed after its lease expired');
    this.name = 'LeaseLostError';
  }
}

/** Graceful shutdown ran out of drain time; the job's lease is released. */
export class ShutdownAbortError extends Error {
  constructor() {
    super('Worker shutting down: drain timeout exceeded, lease released');
    this.name = 'ShutdownAbortError';
  }
}

/** The handler registry has no entry for a leased job's queue. */
export class HandlerNotFoundError extends NonRetryableError {
  constructor(queue: string) {
    super(`No handler registered for queue "${queue}"`);
    this.name = 'HandlerNotFoundError';
  }
}

/** Human-readable error text for job_attempts.error / jobs.last_error. */
export function describeError(err: unknown): string {
  if (err instanceof Error) {
    return err.name && err.name !== 'Error' ? `${err.name}: ${err.message}` : err.message;
  }
  if (typeof err === 'string') return err;
  try {
    return `Non-error thrown: ${JSON.stringify(err)}`;
  } catch {
    return 'Non-error thrown';
  }
}
