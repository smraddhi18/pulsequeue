import pino from 'pino';
import { describe, expect, it } from 'vitest';
import { NotFoundError } from '../src/domain/errors.js';
import type { QueueName } from '../src/domain/job.js';
import type { QueueStats } from '../src/domain/stats.js';
import { toQueueStatsResponse } from '../src/routes/stats.js';
import { StatsCache } from '../src/services/stats-cache.js';
import { StatsService } from '../src/services/stats.service.js';
import { makeStats } from './support/stats.js';

const logger = pino({ level: 'silent' });

describe('StatsService', () => {
  const uncached = new StatsCache({ store: null, minRefreshMs: 0, logger });

  it('returns stats for a known queue', async () => {
    const stats = makeStats();
    const service = new StatsService({ compute: async () => stats }, uncached);
    expect(await service.getQueueStats('emails' as QueueName)).toEqual({ stats, cache: 'bypass' });
  });

  it('404s an unknown queue (reads never create queues)', async () => {
    const service = new StatsService(
      { compute: async (): Promise<QueueStats | null> => null },
      uncached,
    );
    await expect(service.getQueueStats('nope' as QueueName)).rejects.toBeInstanceOf(NotFoundError);
  });
});

describe('toQueueStatsResponse', () => {
  it('serializes dates and derives the backlog age at computedAt', () => {
    const body = toQueueStatsResponse(makeStats());
    expect(body).toEqual({
      queue: 'emails',
      counts: { queued: 2, scheduled: 1, running: 1, succeeded: 5, dead: 0 },
      total: 9,
      oldestQueuedAt: '2026-10-24T10:00:00.000Z',
      oldestQueuedAgeMs: 60_000,
      nextScheduledAt: '2026-10-24T10:05:00.000Z',
      oldestRunningSince: null,
      computedAt: '2026-10-24T10:01:00.000Z',
    });
  });

  it('reports a null age for an empty backlog and never a negative one', () => {
    expect(toQueueStatsResponse(makeStats({ oldestQueuedAt: null })).oldestQueuedAgeMs).toBeNull();
    const skewed = makeStats({ oldestQueuedAt: new Date('2026-10-24T10:01:00.500Z') });
    expect(toQueueStatsResponse(skewed).oldestQueuedAgeMs).toBe(0);
  });
});
