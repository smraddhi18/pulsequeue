/**
 * Change notifications (Milestone 5).
 *
 * Postgres is the source of truth; signals only tell other processes that it
 * is worth looking again sooner. Every call is made *after* the change
 * committed (a subscriber that reacts at once must see the new rows), is
 * fire-and-forget and never throws: losing a signal costs latency (the next
 * fallback poll, the cache TTL), never a job.
 */
export interface QueueSignals {
  /**
   * Jobs in `queue` became runnable or got an earlier run_at: an enqueue
   * (immediate or delayed), a replay, a retry or a released/reclaimed lease.
   * Wakes idle workers and marks the queue's cached stats dirty.
   */
  jobsRunnable(queue: string): void;
  /**
   * Jobs in `queue` changed state without becoming runnable (leased,
   * succeeded, dead-lettered, purged). Only marks the cached stats dirty.
   */
  jobsChanged(queue: string): void;
}

/** Default for components built without Redis (and for unit tests). */
export const NOOP_QUEUE_SIGNALS: QueueSignals = Object.freeze({
  jobsRunnable: () => undefined,
  jobsChanged: () => undefined,
});
