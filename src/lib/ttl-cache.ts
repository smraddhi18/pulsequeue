/**
 * Small in-process LRU with a per-entry TTL. Map iteration order is insertion
 * order, so re-inserting on a hit keeps the most recently used entry last and
 * the first key is always the eviction candidate. O(1) get/set.
 */
export class TtlCache<K, V> {
  private readonly entries = new Map<K, { value: V; expiresAt: number }>();

  constructor(
    private readonly options: { ttlMs: number; maxEntries: number; now?: () => number },
  ) {}

  get size(): number {
    return this.entries.size;
  }

  get(key: K): V | undefined {
    const entry = this.entries.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt <= this.now()) {
      this.entries.delete(key);
      return undefined;
    }
    this.entries.delete(key);
    this.entries.set(key, entry);
    return entry.value;
  }

  /** No-op when ttlMs is 0 (caching disabled). */
  set(key: K, value: V): void {
    if (this.options.ttlMs <= 0) return;
    this.entries.delete(key);
    this.entries.set(key, { value, expiresAt: this.now() + this.options.ttlMs });
    while (this.entries.size > this.options.maxEntries) {
      const oldest = this.entries.keys().next();
      if (oldest.done) break;
      this.entries.delete(oldest.value);
    }
  }

  delete(key: K): void {
    this.entries.delete(key);
  }

  /** Remove every entry whose value matches; O(n), for rare events like revocation. */
  deleteWhere(predicate: (value: V) => boolean): number {
    let removed = 0;
    for (const [key, entry] of this.entries) {
      if (predicate(entry.value)) {
        this.entries.delete(key);
        removed++;
      }
    }
    return removed;
  }

  clear(): void {
    this.entries.clear();
  }

  private now(): number {
    return (this.options.now ?? Date.now)();
  }
}
