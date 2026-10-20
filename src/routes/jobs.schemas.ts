import { z } from 'zod';
import { JOB_LIMITS, JOB_STATUSES, PAGINATION_LIMITS, QUEUE_NAME_PATTERN } from '../domain/job.js';

/**
 * HTTP request shapes for the jobs API. Unknown fields are rejected (.strict())
 * so a typo such as `delay` instead of `delayMs` fails loudly instead of being
 * silently ignored. Payload *size* is a business rule enforced by JobService.
 */

export const queueParamsSchema = z.object({
  name: z.string().regex(QUEUE_NAME_PATTERN, {
    message: 'Queue name must be 1-64 chars of a-z, 0-9, ".", "_", "-" and start alphanumeric',
  }),
});

export const jobParamsSchema = z.object({
  id: z.string().uuid({ message: 'Job id must be a UUID' }),
});

export const enqueueJobBodySchema = z
  .object({
    payload: z.unknown().refine((v) => v !== undefined, { message: 'payload is required' }),
    priority: z.number().int().min(JOB_LIMITS.MIN_PRIORITY).max(JOB_LIMITS.MAX_PRIORITY).optional(),
    delayMs: z.number().int().min(0).max(JOB_LIMITS.MAX_DELAY_MS).optional(),
    maxAttempts: z
      .number()
      .int()
      .min(JOB_LIMITS.MIN_MAX_ATTEMPTS)
      .max(JOB_LIMITS.MAX_MAX_ATTEMPTS)
      .optional(),
  })
  .strict();

/** Printable ASCII without spaces, as recommended for Idempotency-Key values. */
export const idempotencyKeySchema = z
  .string()
  .min(1)
  .max(JOB_LIMITS.IDEMPOTENCY_KEY_MAX_LENGTH)
  .regex(/^[\x21-\x7E]+$/, { message: 'Idempotency-Key must be printable ASCII without spaces' });

export const enqueueJobHeadersSchema = z.object({
  'idempotency-key': idempotencyKeySchema.optional(),
});

export const listJobsQuerySchema = z
  .object({
    status: z.enum(JOB_STATUSES).optional(),
    limit: z.coerce
      .number()
      .int()
      .min(1)
      .max(PAGINATION_LIMITS.MAX_LIMIT)
      .default(PAGINATION_LIMITS.DEFAULT_LIMIT),
    cursor: z.string().min(1).max(512).optional(),
  })
  .strict();

export type EnqueueJobBody = z.infer<typeof enqueueJobBodySchema>;
export type ListJobsQuery = z.infer<typeof listJobsQuerySchema>;
