import { NotFoundError } from '../domain/errors.js';
import type { QueueName } from '../domain/job.js';
import type { QueueStatsResult } from '../domain/stats.js';
import type { QueueStatsRepository } from '../repositories/stats.repository.js';
import type { StatsCache } from './stats-cache.js';

/** The slice of QueueStatsRepository StatsService needs; lets tests pass a fake. */
export type QueueStatsStore = Pick<QueueStatsRepository, 'compute'>;

/**
 * Queue statistics for dashboards and autoscalers. Reads never create
 * queues: an unknown queue is a 404. The existence check is part of the
 * stats statement, so a cache hit costs no Postgres query at all, and 404s
 * are never cached (a queue created a moment later shows up at once).
 */
export class StatsService {
  constructor(
    private readonly stats: QueueStatsStore,
    private readonly cache: StatsCache,
  ) {}

  async getQueueStats(queue: QueueName): Promise<QueueStatsResult> {
    return this.cache.get(queue, async () => {
      const stats = await this.stats.compute(queue);
      if (!stats) throw new NotFoundError('Queue not found', { queue });
      return stats;
    });
  }
}
