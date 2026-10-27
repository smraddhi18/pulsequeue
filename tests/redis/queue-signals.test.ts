import { describe, expect, it } from 'vitest';
import { NOOP_QUEUE_SIGNALS, queueRefKey, uniqueQueueRefs } from '../../src/domain/signals.js';
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
    signals.jobsRunnable({ tenantId: 'acme', name: 'a' });
    signals.jobsChanged({ tenantId: 'acme', name: 'b' });
    // Wake-ups are per queue name (shared worker fleet), stats per tenant + queue.
    expect(f.published).toEqual(['a']);
    expect(f.invalidated).toEqual(['acme:a', 'acme:b']);
  });

  it("keeps two tenants' queues of the same name apart in the stats cache only", () => {
    const f = fakes();
    const signals = new RedisQueueSignals(f.publisher, f.invalidator);
    signals.jobsRunnable({ tenantId: 'acme', name: 'emails' });
    signals.jobsRunnable({ tenantId: 'globex', name: 'emails' });
    expect(f.published).toEqual(['emails', 'emails']);
    expect(f.invalidated).toEqual(['acme:emails', 'globex:emails']);
  });

  it('works with either feature disabled and flushes both', async () => {
    const f = fakes();
    new RedisQueueSignals(null, f.invalidator).jobsRunnable({ tenantId: 't', name: 'a' });
    new RedisQueueSignals(f.publisher, null).jobsChanged({ tenantId: 't', name: 'b' });
    expect(f.published).toEqual([]);
    expect(f.invalidated).toEqual(['t:a']);
    await new RedisQueueSignals(f.publisher, f.invalidator).flush();
    expect(f.flushed()).toBe(2);
  });

  it('the no-op signals are frozen and harmless', () => {
    expect(Object.isFrozen(NOOP_QUEUE_SIGNALS)).toBe(true);
    expect(() => NOOP_QUEUE_SIGNALS.jobsRunnable({ tenantId: 't', name: 'a' })).not.toThrow();
  });
});

describe('queue refs', () => {
  it('keys a ref as tenant:name and de-duplicates refs in first-seen order', () => {
    expect(queueRefKey({ tenantId: 'acme', name: 'emails' })).toBe('acme:emails');
    const refs = uniqueQueueRefs([
      { tenantId: 'acme', name: 'emails' },
      { tenantId: 'globex', name: 'emails' },
      { tenantId: 'acme', name: 'emails' },
    ]);
    expect(refs).toEqual([
      { tenantId: 'acme', name: 'emails' },
      { tenantId: 'globex', name: 'emails' },
    ]);
  });
});
