import { describe, expect, it } from 'vitest';
import { NOOP_QUEUE_SIGNALS } from '../../src/domain/signals.js';
import { RedisQueueSignals } from '../../src/redis/queue-signals.js';
import type { StatsInvalidator } from '../../src/redis/stats-invalidator.js';
import type { WakeupPublisher } from '../../src/redis/wakeup-publisher.js';

function fakes() {
  const published: string[] = [];
  const invalidated: string[] = [];
  let flushed = 0;
  const publisher = {
    notify: (q: string) => published.push(q),
    flush: async () => void flushed++,
  } as unknown as WakeupPublisher;
  const invalidator = {
    invalidate: (q: string) => invalidated.push(q),
    flush: async () => void flushed++,
  } as unknown as StatsInvalidator;
  return { published, invalidated, publisher, invalidator, flushed: () => flushed };
}

describe('RedisQueueSignals', () => {
  it('jobsRunnable wakes workers and invalidates stats; jobsChanged only invalidates', () => {
    const f = fakes();
    const signals = new RedisQueueSignals(f.publisher, f.invalidator);
    signals.jobsRunnable('a');
    signals.jobsChanged('b');
    expect(f.published).toEqual(['a']);
    expect(f.invalidated).toEqual(['a', 'b']);
  });

  it('works with either feature disabled and flushes both', async () => {
    const f = fakes();
    new RedisQueueSignals(null, f.invalidator).jobsRunnable('a');
    new RedisQueueSignals(f.publisher, null).jobsChanged('b');
    expect(f.published).toEqual([]);
    expect(f.invalidated).toEqual(['a']);
    await new RedisQueueSignals(f.publisher, f.invalidator).flush();
    expect(f.flushed()).toBe(2);
  });

  it('the no-op signals are frozen and harmless', () => {
    expect(Object.isFrozen(NOOP_QUEUE_SIGNALS)).toBe(true);
    expect(() => NOOP_QUEUE_SIGNALS.jobsRunnable('a')).not.toThrow();
  });
});
