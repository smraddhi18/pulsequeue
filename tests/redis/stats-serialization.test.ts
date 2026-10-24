import { describe, expect, it } from 'vitest';
import { deserializeStatsEntry, serializeStatsEntry } from '../../src/redis/stats-cache-store.js';
import { makeStats } from '../support/stats.js';

describe('stats cache entry serialization', () => {
  it('round-trips dates and nulls', () => {
    const entry = { stats: makeStats(), cachedAt: 1_234 };
    expect(deserializeStatsEntry(serializeStatsEntry(entry))).toEqual(entry);
  });

  it.each([
    ['not json', 'nope'],
    ['missing counts', JSON.stringify({ cachedAt: 1, stats: { queue: 'q' } })],
    [
      'a missing status',
      JSON.stringify({
        cachedAt: 1,
        stats: { queue: 'q', counts: { queued: 1 }, computedAt: '2026-10-24T00:00:00Z' },
      }),
    ],
    ['no cachedAt', JSON.stringify({ stats: {} })],
  ])('treats %s as a cache miss (null), never as an error', (_label, raw) => {
    expect(deserializeStatsEntry(raw)).toBeNull();
  });

  it('rejects an unparseable computedAt', () => {
    const raw = serializeStatsEntry({ stats: makeStats(), cachedAt: 1 }).replace(
      '2026-10-24T10:01:00.000Z',
      'yesterday',
    );
    expect(deserializeStatsEntry(raw)).toBeNull();
  });
});
