import type { FastifyInstance } from 'fastify';
import type { QueueName } from '../domain/job.js';
import type { QueueStats, QueueStatusCounts } from '../domain/stats.js';
import type { StatsService } from '../services/stats.service.js';
import { queueParamsSchema } from './jobs.schemas.js';

export interface StatsRoutesOptions {
  statsService: StatsService;
}

/** How the response was served: HIT, STALE, MISS or BYPASS (see StatsCacheStatus). */
export const CACHE_STATUS_HEADER = 'x-cache';

export interface QueueStatsResponse {
  queue: string;
  counts: QueueStatusCounts;
  total: number;
  oldestQueuedAt: string | null;
  /** Age of the head of the backlog at computedAt (ms); null when nothing is queued. */
  oldestQueuedAgeMs: number | null;
  nextScheduledAt: string | null;
  oldestRunningSince: string | null;
  computedAt: string;
}

export function toQueueStatsResponse(stats: QueueStats): QueueStatsResponse {
  const at = stats.computedAt.getTime();
  return {
    queue: stats.queue,
    counts: stats.counts,
    total: stats.total,
    oldestQueuedAt: stats.oldestQueuedAt?.toISOString() ?? null,
    // Relative to the snapshot, not to "now": a cached response stays
    // self-consistent, and computedAt tells the client how old it is.
    oldestQueuedAgeMs:
      stats.oldestQueuedAt === null ? null : Math.max(0, at - stats.oldestQueuedAt.getTime()),
    nextScheduledAt: stats.nextScheduledAt?.toISOString() ?? null,
    oldestRunningSince: stats.oldestRunningSince?.toISOString() ?? null,
    computedAt: stats.computedAt.toISOString(),
  };
}

export async function statsRoutes(app: FastifyInstance, opts: StatsRoutesOptions): Promise<void> {
  const { statsService } = opts;

  /**
   * Counts by status plus backlog ages for one queue. Served from the Redis
   * cache when possible (bounded staleness, see StatsCache); `computedAt`
   * is when the numbers were true.
   */
  app.get('/queues/:name/stats', async (request, reply) => {
    const { name } = queueParamsSchema.parse(request.params);
    const { stats, cache } = await statsService.getQueueStats(name as QueueName);
    reply.header(CACHE_STATUS_HEADER, cache.toUpperCase());
    reply.header('cache-control', 'no-store');
    return reply.status(200).send(toQueueStatsResponse(stats));
  });
}
