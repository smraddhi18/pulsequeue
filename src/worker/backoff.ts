export interface BackoffOptions {
  /** Ceiling for the first retry; doubles per attempt. */
  baseMs: number;
  /** Upper bound for any single delay. */
  maxMs: number;
}

/** 2^30 * base already exceeds any sane cap; clamping the exponent avoids Infinity. */
const MAX_EXPONENT = 30;

/**
 * Upper bound of the delay before retry number `attempt` (1 = first retry):
 * min(maxMs, baseMs * 2^(attempt - 1)).
 */
export function backoffCeilingMs(attempt: number, options: BackoffOptions): number {
  if (!Number.isInteger(attempt) || attempt < 1) {
    throw new RangeError(`attempt must be a positive integer, got ${attempt}`);
  }
  if (!(options.baseMs > 0) || !(options.maxMs >= options.baseMs)) {
    throw new RangeError('backoff requires 0 < baseMs <= maxMs');
  }
  const exponent = Math.min(attempt - 1, MAX_EXPONENT);
  return Math.min(options.maxMs, options.baseMs * 2 ** exponent);
}

/**
 * Exponential backoff with "full jitter": a uniformly random delay in
 * [0, ceiling). Compared with plain exponential backoff, the randomness
 * spreads retries of jobs that failed together (a downstream outage) so they
 * do not hammer the recovering dependency in synchronized waves; full jitter
 * minimizes total work under contention (AWS Architecture Blog, "Exponential
 * Backoff and Jitter"). `random` is injectable for deterministic tests.
 */
export function computeBackoffMs(
  attempt: number,
  options: BackoffOptions,
  random: () => number = Math.random,
): number {
  const ceiling = backoffCeilingMs(attempt, options);
  const r = Math.min(Math.max(random(), 0), 1);
  return Math.min(Math.floor(r * ceiling), ceiling);
}
