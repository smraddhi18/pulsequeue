import { queueRefKey, type QueueRef, type QueueSignals } from '../domain/signals.js';
import type { StatsInvalidator } from './stats-invalidator.js';
import type { WakeupPublisher } from './wakeup-publisher.js';

/**
 * QueueSignals backed by Redis: jobsRunnable publishes a wake-up (by queue
 * name: the worker fleet is shared by all tenants) and invalidates the
 * tenant's cached stats, jobsChanged only invalidates. Either half may be absent
 * (WAKEUP_ENABLED / STATS_CACHE_ENABLED). Never throws.
 */
export class RedisQueueSignals implements QueueSignals {
  constructor(
    private readonly publisher: WakeupPublisher | null,
    private readonly invalidator: StatsInvalidator | null,
  ) {}

  jobsRunnable(queue: QueueRef): void {
    this.publisher?.notify(queue.name);
    this.invalidator?.invalidate(queueRefKey(queue));
  }

  jobsChanged(queue: QueueRef): void {
    this.invalidator?.invalidate(queueRefKey(queue));
  }

  /** Deliver coalesced, not-yet-sent signals (graceful shutdown). */
  async flush(): Promise<void> {
    await Promise.all([this.publisher?.flush(), this.invalidator?.flush()]);
  }
}
