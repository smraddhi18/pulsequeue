import pino from 'pino';
import { beforeEach, describe, expect, it } from 'vitest';
import type { QueueStats } from '../src/domain/stats.js';
import {
  StatsCache,
  type CachedQueueStats,
  type StatsCacheStore,
} from '../src/services/stats-cache.js';
import { makeStats } from './support/stats.js';

const logger = pino({ level: 'silent' });

/** In-memory StatsCacheStore with the Redis store's semantics (DEL claim, CAS on computedAt). */
class FakeStatsStore implements StatsCacheStore {
  readonly entries = new Map<string, CachedQueueStats>();
  readonly dirty = new Set<string>();
  available = true;
  failReads = false;
  failWrites = false;
  writes = 0;
  claims = 0;

  isAvailable(): boolean {
    return this.available;
  }

  async read(queue: string) {
    if (this.failReads) throw new Error('read failed');
    return { entry: this.entries.get(queue) ?? null, dirty: this.dirty.has(queue) };
  }

  async claimRefresh(queue: string): Promise<boolean> {
    this.claims++;
    return this.dirty.delete(queue);
  }

  async write(queue: string, entry: CachedQueueStats): Promise<boolean> {
    if (this.failWrites) throw new Error('write failed');
    this.writes++;
    const current = this.entries.get(queue);
    if (current && current.stats.computedAt > entry.stats.computedAt) return false;
    this.entries.set(queue, entry);
    return true;
  }
}

describe('StatsCache policy', () => {
  let store: FakeStatsStore;
  let now: number;
  let cache: StatsCache;
  let computes: number;
  let next: QueueStats;
  const compute = async () => {
    computes++;
    return next;
  };

  beforeEach(() => {
    store = new FakeStatsStore();
    now = 1_000_000;
    cache = new StatsCache({ store, minRefreshMs: 1_000, logger, now: () => now });
    computes = 0;
    next = makeStats();
  });

  it('MISS computes and writes; the next read is a HIT without computing', async () => {
    expect((await cache.get('emails', compute)).cache).toBe('miss');
    expect(store.writes).toBe(1);
    const hit = await cache.get('emails', compute);
    expect(hit.cache).toBe('hit');
    expect(hit.stats).toEqual(next);
    expect(computes).toBe(1);
  });

  it('a dirty entry younger than minRefresh is served STALE (bounded recompute rate)', async () => {
    await cache.get('emails', compute);
    store.dirty.add('emails');
    now += 999;
    expect((await cache.get('emails', compute)).cache).toBe('stale');
    expect(computes).toBe(1);
    expect(store.dirty.has('emails')).toBe(true); // nobody claimed it yet
  });

  it('a dirty entry past minRefresh is claimed and recomputed by exactly one caller', async () => {
    await cache.get('emails', compute);
    store.dirty.add('emails');
    now += 1_000;
    next = makeStats({ total: 10, computedAt: new Date('2026-10-24T10:02:00.000Z') });
    const refreshed = await cache.get('emails', compute);
    expect(refreshed).toMatchObject({ cache: 'miss', stats: { total: 10 } });
    expect(store.dirty.has('emails')).toBe(false);
    expect((await cache.get('emails', compute)).cache).toBe('hit');
  });

  it('a caller that loses the claim serves the old entry (refresh in progress elsewhere)', async () => {
    await cache.get('emails', compute);
    store.dirty.add('emails');
    now += 5_000;
    // Another API instance claims between our read and our claim.
    const read = store.read.bind(store);
    store.read = async (q) => {
      const r = await read(q);
      store.dirty.delete(q);
      return r;
    };
    const result = await cache.get('emails', compute);
    expect(result.cache).toBe('stale');
    expect(computes).toBe(1);
  });

  it('20 concurrent cold reads cost one compute', async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const slow = async () => {
      computes++;
      await gate;
      return next;
    };
    const all = Promise.all(Array.from({ length: 20 }, () => cache.get('emails', slow)));
    await new Promise((r) => setTimeout(r, 10));
    release();
    const results = await all;
    expect(computes).toBe(1);
    expect(results.every((r) => r.cache === 'miss')).toBe(true);
  });

  it('20 concurrent cold reads of a dirty key (the normal case after an enqueue) cost one compute', async () => {
    // Regression: every reader saw "no entry, dirty" and forced its own
    // fresh computation. Only the claim winner may force one.
    store.dirty.add('emails');
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const slow = async () => {
      computes++;
      await gate;
      return next;
    };
    const all = Promise.all(Array.from({ length: 20 }, () => cache.get('emails', slow)));
    await new Promise((r) => setTimeout(r, 10));
    release();
    const results = await all;
    expect(computes).toBe(1);
    expect(store.claims).toBe(20);
    expect(results.every((r) => r.cache === 'miss')).toBe(true);
    expect(store.dirty.has('emails')).toBe(false);
  });

  it('a refresh after a claim never joins a computation that started before it', async () => {
    // A cold read starts computing (snapshot S0) ...
    let releaseOld!: () => void;
    const oldGate = new Promise<void>((r) => (releaseOld = r));
    const old = makeStats({ total: 1, computedAt: new Date('2026-10-24T10:00:00.000Z') });
    const coldRead = cache.get('emails', async () => {
      computes++;
      await oldGate;
      return old;
    });
    await new Promise((r) => setTimeout(r, 0));
    // ... meanwhile an older entry exists, a write dirties it and the claim
    // succeeds: this caller must compute a new snapshot, not join S0.
    store.entries.set('emails', { stats: old, cachedAt: now - 10_000 });
    store.dirty.add('emails');
    next = makeStats({ total: 2, computedAt: new Date('2026-10-24T10:03:00.000Z') });
    const refresh = await cache.get('emails', compute);
    expect(refresh.stats.total).toBe(2);
    releaseOld();
    await coldRead;
    expect(computes).toBe(2);
    // CAS kept the newer snapshot even though the old compute wrote last.
    expect(store.entries.get('emails')?.stats.total).toBe(2);
  });

  it('clears a leftover dirty flag before computing when there is no entry', async () => {
    store.dirty.add('emails');
    expect((await cache.get('emails', compute)).cache).toBe('miss');
    expect(store.dirty.has('emails')).toBe(false);
    expect((await cache.get('emails', compute)).cache).toBe('hit');
  });

  it('BYPASS when the store is unavailable, without touching it', async () => {
    store.available = false;
    expect((await cache.get('emails', compute)).cache).toBe('bypass');
    expect(store.writes).toBe(0);
    expect(computes).toBe(1);
  });

  it('BYPASS when a read fails; a failed write still returns the fresh MISS', async () => {
    store.failReads = true;
    expect((await cache.get('emails', compute)).cache).toBe('bypass');
    store.failReads = false;
    store.failWrites = true;
    expect((await cache.get('emails', compute)).cache).toBe('miss');
    expect(computes).toBe(2);
  });

  it('serves STALE when the claim itself fails', async () => {
    await cache.get('emails', compute);
    store.dirty.add('emails');
    now += 5_000;
    store.claimRefresh = async () => {
      throw new Error('claim failed');
    };
    expect((await cache.get('emails', compute)).cache).toBe('stale');
  });

  it('BYPASS for every read with caching disabled', async () => {
    const disabled = new StatsCache({ store: null, minRefreshMs: 0, logger });
    expect(disabled.enabled).toBe(false);
    expect((await disabled.get('emails', compute)).cache).toBe('bypass');
    expect((await disabled.get('emails', compute)).cache).toBe('bypass');
    expect(computes).toBe(2);
  });

  it('propagates compute errors (e.g. unknown queue) and caches nothing', async () => {
    await expect(
      cache.get('emails', async () => {
        throw new Error('not found');
      }),
    ).rejects.toThrow('not found');
    expect(store.entries.size).toBe(0);
  });
});
