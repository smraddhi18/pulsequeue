import type { QueueName } from './job.js';

/**
 * Dashboard API (Milestone 7): time-windowed aggregates over job history,
 * computed by Postgres for one tenant's queues. Unlike /metrics (process
 * counters since start, scraped by Prometheus), these read the source of
 * truth, so they are exact per queue and survive restarts, at the cost of a
 * bounded query per request.
 */

const SECOND = 1_000;
const MINUTE = 60 * SECOND;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

export const DASHBOARD_LIMITS = {
  /** Smallest throughput bucket: finer than the scrape interval is noise. */
  MIN_BUCKET_MS: 10 * SECOND,
  MAX_BUCKET_MS: DAY,
  /** Throughput and failure-rate windows. */
  MAX_WINDOW_MS: 7 * DAY,
  /** Latency percentiles sort their samples: a shorter horizon bounds the sort. */
  MAX_LATENCY_WINDOW_MS: DAY,
  /** Buckets per throughput response (1d at 1m, or 7d at 7m). */
  MAX_BUCKETS: 1_440,
  DEFAULT_WINDOW: '1h',
  DEFAULT_BUCKET: '1m',
  DEFAULT_QUEUE_PAGE: 100,
  MAX_QUEUE_PAGE: 500,
} as const;

/** Window / bucket syntax: an integer and one unit, e.g. 30s, 5m, 1h, 7d. */
export const DURATION_PATTERN = /^(\d{1,5})(s|m|h|d)$/;

const UNIT_MS = { s: SECOND, m: MINUTE, h: HOUR, d: DAY } as const;

/** Milliseconds for "15m"-style strings; null when malformed. */
export function parseDuration(value: string): number | null {
  const match = DURATION_PATTERN.exec(value);
  if (!match) return null;
  const amount = Number(match[1]);
  const unit = match[2] as keyof typeof UNIT_MS;
  return amount * UNIT_MS[unit];
}

export interface DashboardQueueSummary {
  queue: QueueName;
  queued: number;
  scheduled: number;
  running: number;
  dead: number;
  maxConcurrency: number | null;
  /** run_at of the oldest runnable job (head-of-line age). */
  oldestQueuedAt: Date | null;
}

export interface DashboardQueuePage {
  queues: DashboardQueueSummary[];
  /** Pass as `after` for the next page; null on the last page. */
  nextAfter: string | null;
  computedAt: Date;
}

export interface ThroughputBucket {
  start: Date;
  /** Jobs created in the bucket (still existing: purged DLQ jobs are gone). */
  enqueued: number;
  /** Attempts that finished in the bucket, by outcome. */
  succeeded: number;
  failed: number;
  timedOut: number;
  /** Jobs currently dead whose dead_lettered_at falls in the bucket. */
  deadLettered: number;
}

export interface ThroughputSeries {
  queue: QueueName;
  from: Date;
  to: Date;
  bucketMs: number;
  buckets: ThroughputBucket[];
}

export interface LatencySummary {
  samples: number;
  p50: number | null;
  p95: number | null;
  p99: number | null;
  max: number | null;
}

export interface LatencyReport {
  queue: QueueName;
  from: Date;
  to: Date;
  /** Run time of finished attempts (succeeded, failed, timed out), ms. */
  execution: LatencySummary;
  /** run_at -> lease, ms (attempts recorded since migration 0008). */
  queueWait: LatencySummary;
  /** true when the window held more attempts than the sample cap (newest were used). */
  truncated: boolean;
  sampleLimit: number;
}

export interface FailureRateReport {
  queue: QueueName;
  from: Date;
  to: Date;
  attempts: { succeeded: number; failed: number; timedOut: number; total: number };
  /** (failed + timedOut) / total attempts; null with no attempts. */
  attemptFailureRate: number | null;
  jobs: { succeeded: number; deadLettered: number };
  /** deadLettered / (succeeded + deadLettered): terminal outcomes only; null with none. */
  jobFailureRate: number | null;
}
