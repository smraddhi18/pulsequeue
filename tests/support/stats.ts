import type { QueueName } from '../../src/domain/job.js';
import type { QueueStats } from '../../src/domain/stats.js';

export function makeStats(overrides: Partial<QueueStats> = {}): QueueStats {
  return {
    queue: 'emails' as QueueName,
    counts: { queued: 2, scheduled: 1, running: 1, succeeded: 5, dead: 0 },
    total: 9,
    oldestQueuedAt: new Date('2026-10-24T10:00:00.000Z'),
    nextScheduledAt: new Date('2026-10-24T10:05:00.000Z'),
    oldestRunningSince: null,
    computedAt: new Date('2026-10-24T10:01:00.000Z'),
    ...overrides,
  };
}
