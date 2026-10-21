import { describe, expect, it } from 'vitest';
import { backoffCeilingMs, computeBackoffMs } from '../../src/worker/backoff.js';
import { decideRetry } from '../../src/worker/retry-policy.js';
import { NonRetryableError } from '../../src/worker/errors.js';

const opts = { baseMs: 1_000, maxMs: 60_000 };

describe('backoffCeilingMs', () => {
  it('doubles per attempt starting at baseMs', () => {
    expect([1, 2, 3, 4, 5].map((a) => backoffCeilingMs(a, opts))).toEqual([
      1_000, 2_000, 4_000, 8_000, 16_000,
    ]);
  });

  it('is capped at maxMs', () => {
    expect(backoffCeilingMs(7, opts)).toBe(60_000);
    expect(backoffCeilingMs(25, opts)).toBe(60_000);
  });

  it('does not overflow to Infinity for huge attempt numbers', () => {
    expect(backoffCeilingMs(10_000, opts)).toBe(60_000);
    expect(
      Number.isFinite(backoffCeilingMs(10_000, { baseMs: 1, maxMs: Number.MAX_SAFE_INTEGER })),
    ).toBe(true);
  });

  it.each([0, -1, 1.5, Number.NaN])('rejects attempt %s', (attempt) => {
    expect(() => backoffCeilingMs(attempt, opts)).toThrow(RangeError);
  });

  it('rejects base > max and non-positive base', () => {
    expect(() => backoffCeilingMs(1, { baseMs: 10, maxMs: 5 })).toThrow(RangeError);
    expect(() => backoffCeilingMs(1, { baseMs: 0, maxMs: 5 })).toThrow(RangeError);
  });
});

describe('computeBackoffMs (full jitter)', () => {
  it('is 0 when random() is 0', () => {
    expect(computeBackoffMs(3, opts, () => 0)).toBe(0);
  });

  it('approaches the ceiling as random() approaches 1, never exceeding it', () => {
    expect(computeBackoffMs(3, opts, () => 0.999_999)).toBe(3_999);
    expect(computeBackoffMs(3, opts, () => 1)).toBe(4_000);
  });

  it('scales linearly with random() inside [0, ceiling]', () => {
    expect(computeBackoffMs(2, opts, () => 0.5)).toBe(1_000);
    expect(computeBackoffMs(10, opts, () => 0.25)).toBe(15_000);
  });

  it('clamps a misbehaving random source', () => {
    expect(computeBackoffMs(1, opts, () => -3)).toBe(0);
    expect(computeBackoffMs(1, opts, () => 7)).toBe(1_000);
  });

  it('stays within bounds and is spread out over many samples', () => {
    const samples = Array.from({ length: 2_000 }, () => computeBackoffMs(4, opts));
    for (const s of samples) {
      expect(Number.isInteger(s)).toBe(true);
      expect(s).toBeGreaterThanOrEqual(0);
      expect(s).toBeLessThanOrEqual(8_000);
    }
    const mean = samples.reduce((a, b) => a + b, 0) / samples.length;
    // Uniform on [0, 8000): mean ~4000. Loose bounds keep this non-flaky.
    expect(mean).toBeGreaterThan(3_400);
    expect(mean).toBeLessThan(4_600);
    expect(new Set(samples).size).toBeGreaterThan(500);
  });
});

describe('decideRetry', () => {
  const half = () => 0.5;

  it('retries with backoff while attempts remain', () => {
    expect(decideRetry({ attempt: 1, maxAttempts: 3, error: new Error('x') }, opts, half)).toEqual({
      kind: 'retry',
      delayMs: 500,
    });
    expect(decideRetry({ attempt: 2, maxAttempts: 3, error: new Error('x') }, opts, half)).toEqual({
      kind: 'retry',
      delayMs: 1_000,
    });
  });

  it('fails once the last attempt fails', () => {
    expect(decideRetry({ attempt: 3, maxAttempts: 3, error: new Error('x') }, opts, half)).toEqual({
      kind: 'fail',
      reason: 'attempts_exhausted',
    });
  });

  it('never retries a NonRetryableError', () => {
    expect(
      decideRetry({ attempt: 1, maxAttempts: 10, error: new NonRetryableError('bad') }, opts, half),
    ).toEqual({ kind: 'fail', reason: 'non_retryable' });
  });

  it('treats non-Error throwables as retryable', () => {
    expect(decideRetry({ attempt: 1, maxAttempts: 2, error: 'boom' }, opts, half).kind).toBe(
      'retry',
    );
  });
});
