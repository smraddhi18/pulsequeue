import { computeBackoffMs, type BackoffOptions } from './backoff.js';
import { NonRetryableError } from './errors.js';

export type RetryDecision =
  | { kind: 'retry'; delayMs: number }
  | { kind: 'fail'; reason: 'non_retryable' | 'attempts_exhausted' };

export interface RetryInput {
  /** Counted attempts so far, including the one that just failed (1-based). */
  attempt: number;
  maxAttempts: number;
  error: unknown;
}

/**
 * Decide what happens after a failed attempt. Pure, so the policy is unit
 * tested without a worker or a database. The delay before retry k is drawn
 * from computeBackoffMs(k), so the first retry waits at most baseMs.
 */
export function decideRetry(
  input: RetryInput,
  backoff: BackoffOptions,
  random: () => number = Math.random,
): RetryDecision {
  if (input.error instanceof NonRetryableError) {
    return { kind: 'fail', reason: 'non_retryable' };
  }
  if (input.attempt >= input.maxAttempts) {
    return { kind: 'fail', reason: 'attempts_exhausted' };
  }
  return { kind: 'retry', delayMs: computeBackoffMs(input.attempt, backoff, random) };
}
