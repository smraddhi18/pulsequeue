import { NotFoundError } from '../domain/errors.js';
import type { QueueName } from '../domain/job.js';
import { queueRefKey } from '../domain/signals.js';
import type { QueueStatsResult } from '../domain/stats.js';
import type { TenantId } from '../domain/tenant.js';
import type { QueueStatsRepository } from '../repositories/stats.repository.js';
import type { StatsCache } from './stats-cache.js';

/** The slice of QueueStatsRepository StatsService needs; lets tests pass a fake. */
export type QueueStatsStore = Pick<QueueStatsRepository, 'compute'>;

/**
 * Queue statistics for dashboards and autoscalers. Reads never create
 * queues: an unknown queue is a 404. The existence check is part of the
 * stats statement, so a cache hit costs no Postgres query at all, and 404s
 * are never cached (a queue created a moment later shows up at once).
 * Cache entries are per tenant + queue (two tenants may share a queue name).
 */
export class StatsService {
  constructor(
    private readonly stats: QueueStatsStore,
    private readonly cache: StatsCache,
  ) {}

  async getQueueStats(tenantId: TenantId, queue: QueueName): Promise<QueueStatsResult> {
    return this.cache.get(queueRefKey({ tenantId, name: queue }), async () => {
      const stats = await this.stats.compute(tenantId, queue);
      if (!stats) throw new NotFoundError('Queue not found', { queue });
      return stats;
    });
  }
}
