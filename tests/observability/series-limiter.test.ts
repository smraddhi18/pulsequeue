import { describe, expect, it } from 'vitest';
import { OVERFLOW_LABEL, SeriesLimiter } from '../../src/observability/series-limiter.js';

describe('SeriesLimiter', () => {
  it('keeps the first `max` tenant/queue pairs and folds later ones into __other__', () => {
    const limiter = new SeriesLimiter(2);
    expect(limiter.labels('t1', 'a')).toEqual({ tenant: 't1', queue: 'a' });
    expect(limiter.labels('t1', 'b')).toEqual({ tenant: 't1', queue: 'b' });
    expect(limiter.hasOverflowed).toBe(false);

    expect(limiter.labels('t2', 'a')).toEqual({ tenant: OVERFLOW_LABEL, queue: OVERFLOW_LABEL });
    expect(limiter.hasOverflowed).toBe(true);
    // Pairs admitted earlier keep their own labels after the overflow.
    expect(limiter.labels('t1', 'a')).toEqual({ tenant: 't1', queue: 'a' });
    expect(limiter.size).toBe(2);
  });

  it('distinguishes pairs whose concatenation would collide', () => {
    const limiter = new SeriesLimiter(10);
    limiter.labels('ab', 'c');
    limiter.labels('a', 'bc');
    expect(limiter.size).toBe(2);
  });

  it('rejects a non-positive cap', () => {
    expect(() => new SeriesLimiter(0)).toThrow(/>= 1/);
    expect(() => new SeriesLimiter(1.5)).toThrow(/>= 1/);
  });
});
