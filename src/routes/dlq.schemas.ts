import { z } from 'zod';
import { DEAD_LETTER_REASONS, DLQ_LIMITS } from '../domain/dead-letter.js';
import { JOB_LIMITS, PAGINATION_LIMITS } from '../domain/job.js';

/**
 * HTTP request shapes for the dead-letter queue API. Same conventions as
 * jobs.schemas.ts: unknown fields are rejected, numbers in query strings are
 * coerced, everything else is a 400 VALIDATION_ERROR via the central handler.
 */

export const dlqJobParamsSchema = z.object({
  jobId: z.string().uuid({ message: 'Job id must be a UUID' }),
});

export const listDeadLettersQuerySchema = z
  .object({
    reason: z.enum(DEAD_LETTER_REASONS).optional(),
    limit: z.coerce
      .number()
      .int()
      .min(1)
      .max(PAGINATION_LIMITS.MAX_LIMIT)
      .default(PAGINATION_LIMITS.DEFAULT_LIMIT),
    cursor: z.string().min(1).max(512).optional(),
  })
  .strict();

const delayMsSchema = z.number().int().min(0).max(JOB_LIMITS.MAX_DELAY_MS);

/** Body is optional: no body (or `{}`) replays immediately. */
export const replayJobBodySchema = z
  .object({ delayMs: delayMsSchema.optional() })
  .strict()
  .optional()
  .transform((body) => body ?? {});

export const bulkReplayBodySchema = z
  .object({
    limit: z
      .number()
      .int()
      .min(1)
      .max(DLQ_LIMITS.MAX_BULK_REPLAY)
      .default(DLQ_LIMITS.DEFAULT_BULK_REPLAY),
    reason: z.enum(DEAD_LETTER_REASONS).optional(),
    delayMs: delayMsSchema.optional(),
  })
  .strict()
  .optional()
  .transform((body) => body ?? { limit: DLQ_LIMITS.DEFAULT_BULK_REPLAY });

/**
 * Bulk purge requires an explicit cutoff: there is deliberately no "delete
 * the whole DLQ" call without one. `olderThan` is an ISO-8601 instant with a
 * zone (e.g. 2026-10-01T00:00:00Z); pass the current time to purge everything.
 */
export const purgeDeadLettersQuerySchema = z
  .object({
    olderThan: z
      .string()
      .datetime({ offset: true, message: 'olderThan must be an ISO-8601 datetime with a zone' })
      .transform((v) => new Date(v)),
    limit: z.coerce
      .number()
      .int()
      .min(1)
      .max(DLQ_LIMITS.MAX_BULK_PURGE)
      .default(DLQ_LIMITS.DEFAULT_BULK_PURGE),
  })
  .strict();

export type ListDeadLettersQuery = z.infer<typeof listDeadLettersQuerySchema>;
export type ReplayJobBody = z.infer<typeof replayJobBodySchema>;
export type BulkReplayBody = z.infer<typeof bulkReplayBodySchema>;
export type PurgeDeadLettersQuery = z.infer<typeof purgeDeadLettersQuerySchema>;
