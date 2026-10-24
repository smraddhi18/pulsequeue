import type { QueueSignals } from '../domain/signals.js';
import type { StatsInvalidator } from './stats-invalidator.js';
import type { WakeupPublisher } from './wakeup-publisher.js';

/**
 * QueueSignals backed by Redis: jobsRunnable publishes a wake-up and
 * invalidates stats, jobsChanged only invalidates. Either half may be absent
 * (WAKEUP_ENABLED / STATS_CACHE_ENABLED). Never throws.
 */
export class RedisQueueSignals implements QueueSignals {
  constructor(
    private readonly publisher: WakeupPublisher | null,
    private readonly invalidator: StatsInvalidator | null,
  ) {}

  jobsRunnable(queue: string): void {
    this.publisher?.notify(queue);
    this.invalidator?.invalidate(queue);
  }

  jobsChanged(queue: string): void {
    this.invalidator?.invalidate(queue);
  }

  /** Deliver coalesced, not-yet-sent signals (graceful shutdown). */
  async flush(): Promise<void> {
    await Promise.all([this.publisher?.flush(), this.invalidator?.flush()]);
  }
}
