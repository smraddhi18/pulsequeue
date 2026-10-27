import type { QueueName } from './job.js';
import type { RateLimit, TenantId } from './tenant.js';

/**
 * Per-queue limits (Milestone 6). Both are opt-in; null means unlimited.
 *
 * - maxConcurrency caps how many of the queue's jobs run at once across the
 *   whole worker fleet (enforced by the lease query, not by workers).
 * - rateLimit is an enqueue token bucket for this queue, applied on top of
 *   the tenant's own bucket.
 */
export interface QueueSettings {
  queue: QueueName;
  maxConcurrency: number | null;
  rateLimit: RateLimit | null;
  updatedAt: Date;
}

export interface UpdateQueueSettingsInput {
  tenantId: TenantId;
  queue: QueueName;
  maxConcurrency: number | null;
  rateLimit: RateLimit | null;
}

export const QUEUE_SETTINGS_LIMITS = {
  /** Far above any sane fleet; a typo like 1e9 is rejected instead of meaning "unlimited". */
  MAX_CONCURRENCY: 10_000,
  MAX_RATE_PER_SEC: 100_000,
  MAX_BURST: 1_000_000,
} as const;
