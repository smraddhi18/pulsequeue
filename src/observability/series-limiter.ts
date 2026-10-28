/** Label value used once a process has seen METRICS_MAX_QUEUE_SERIES tenant/queue pairs. */
export const OVERFLOW_LABEL = '__other__';

export interface QueueLabels {
  tenant: string;
  queue: string;
}

/**
 * Caps the number of distinct (tenant, queue) label pairs a process emits.
 *
 * Queue names are tenant-controlled: without a cap, one tenant creating
 * 100k queues would create 100k series per metric (times histogram buckets)
 * in this process and in Prometheus. The first `max` pairs seen keep their
 * own labels for the life of the process; later ones are folded into
 * tenant/queue="__other__", so totals stay correct and only attribution is
 * lost. The dashboard API, which reads Postgres, still has exact per-queue
 * numbers.
 */
export class SeriesLimiter {
  private readonly seen = new Set<string>();
  private overflowed = false;

  constructor(private readonly max: number) {
    if (!Number.isInteger(max) || max < 1) throw new Error('SeriesLimiter max must be >= 1');
  }

  labels(tenant: string, queue: string): QueueLabels {
    const key = `${tenant}\u0000${queue}`;
    if (this.seen.has(key)) return { tenant, queue };
    if (this.seen.size < this.max) {
      this.seen.add(key);
      return { tenant, queue };
    }
    this.overflowed = true;
    return { tenant: OVERFLOW_LABEL, queue: OVERFLOW_LABEL };
  }

  /** true once at least one pair was folded into __other__. */
  get hasOverflowed(): boolean {
    return this.overflowed;
  }

  get size(): number {
    return this.seen.size;
  }
}
