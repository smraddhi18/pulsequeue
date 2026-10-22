import type { DeadLetterReason } from '../domain/dead-letter.js';
import { computeBackoffMs, type BackoffOptions } from './backoff.js';
import { classifyNonRetryable } from './errors.js';

/** Reasons the worker itself can dead-letter for (poison_lease_expired is the reaper's). */
export type WorkerDeadLetterReason = Exclude<DeadLetterReason, 'poison_lease_expired'>;

export type RetryDecision =
  | { kind: 'retry'; delayMs: number }
  /** Dead-letter the job now, with this reason. */
  | { kind: 'fail'; reason: WorkerDeadLetterReason };

export interface RetryInput {
  /** Counted attempts so far, including the one that just failed (1-based). */
  attempt: number;
  maxAttempts: number;
  error: unknown;
}

/**
 * Decide what happens after a failed attempt: retry with backoff, or
 * dead-letter now (poison message: non-retryable / invalid payload) or
 * because the budget is spent. Pure, so the policy is unit
 * tested without a worker or a database. The delay before retry k is drawn
 * from computeBackoffMs(k), so the first retry waits at most baseMs.
 */
export function decideRetry(
  input: RetryInput,
  backoff: BackoffOptions,
  random: () => number = Math.random,
): RetryDecision {
  const nonRetryable = classifyNonRetryable(input.error);
  if (nonRetryable !== null) {
    return { kind: 'fail', reason: nonRetryable };
  }
  if (input.attempt >= input.maxAttempts) {
    return { kind: 'fail', reason: 'attempts_exhausted' };
  }
  return { kind: 'retry', delayMs: computeBackoffMs(input.attempt, backoff, random) };
}
