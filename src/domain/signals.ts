/**
 * Change notifications (Milestone 5).
 *
 * Postgres is the source of truth; signals only tell other processes that it
 * is worth looking again sooner. Every call is made *after* the change
 * committed (a subscriber that reacts at once must see the new rows), is
 * fire-and-forget and never throws: losing a signal costs latency (the next
 * fallback poll, the cache TTL), never a job.
 */

/**
 * A queue is identified by its owner and its name (M6: names are unique per
 * tenant). Wake-ups go out by name (workers consume a name across tenants);
 * cached stats are per tenant + name.
 */
export interface QueueRef {
  readonly tenantId: string;
  readonly name: string;
}

/** Stable string form of a QueueRef, e.g. `acme:emails` (neither part can contain ':'). */
export function queueRefKey(ref: QueueRef): string {
  return `${ref.tenantId}:${ref.name}`;
}

/** Distinct refs, first occurrence order (one signal per queue per batch). */
export function uniqueQueueRefs(refs: Iterable<QueueRef>): QueueRef[] {
  const seen = new Map<string, QueueRef>();
  for (const ref of refs) {
    const key = queueRefKey(ref);
    if (!seen.has(key)) seen.set(key, { tenantId: ref.tenantId, name: ref.name });
  }
  return [...seen.values()];
}

export interface QueueSignals {
  /**
   * Jobs in `queue` became runnable or got an earlier run_at: an enqueue
   * (immediate or delayed), a replay, a retry or a released/reclaimed lease.
   * Wakes idle workers and marks the queue's cached stats dirty.
   */
  jobsRunnable(queue: QueueRef): void;
  /**
   * Jobs in `queue` changed state without becoming runnable (leased,
   * succeeded, dead-lettered, purged). Only marks the cached stats dirty.
   */
  jobsChanged(queue: QueueRef): void;
}

/** Default for components built without Redis (and for unit tests). */
export const NOOP_QUEUE_SIGNALS: QueueSignals = Object.freeze({
  jobsRunnable: () => undefined,
  jobsChanged: () => undefined,
});
