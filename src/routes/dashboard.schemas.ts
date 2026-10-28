import { z } from 'zod';
import { DASHBOARD_LIMITS, DURATION_PATTERN, parseDuration } from '../domain/dashboard.js';
import { QUEUE_NAME_PATTERN } from '../domain/job.js';

const fmt = (ms: number): string =>
  ms % 86_400_000 === 0
    ? `${ms / 86_400_000}d`
    : ms % 3_600_000 === 0
      ? `${ms / 3_600_000}h`
      : ms % 60_000 === 0
        ? `${ms / 60_000}m`
        : `${ms / 1000}s`;

/** "15m" -> 900000, with bounds; rejects anything that is not <int><s|m|h|d>. */
function durationSchema(min: number, max: number) {
  return z
    .string()
    .regex(DURATION_PATTERN, {
      message: 'Must be an integer followed by s, m, h or d (e.g. 30s, 5m, 1h, 7d)',
    })
    .transform((value, ctx) => {
      const ms = parseDuration(value) ?? 0;
      if (ms < min || ms > max) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: `Must be between ${fmt(min)} and ${fmt(max)}`,
        });
        return z.NEVER;
      }
      return ms;
    });
}

const windowSchema = durationSchema(DASHBOARD_LIMITS.MIN_BUCKET_MS, DASHBOARD_LIMITS.MAX_WINDOW_MS);

export const dashboardQueuesQuerySchema = z
  .object({
    limit: z.coerce
      .number()
      .int()
      .min(1)
      .max(DASHBOARD_LIMITS.MAX_QUEUE_PAGE)
      .default(DASHBOARD_LIMITS.DEFAULT_QUEUE_PAGE),
    after: z.string().regex(QUEUE_NAME_PATTERN, { message: 'Must be a queue name' }).optional(),
  })
  .strict();

/**
 * Throughput: the window must be a whole number of buckets (so every bucket
 * is complete-width and aligned) and at most MAX_BUCKETS of them (bounded
 * response size and generate_series length).
 */
export const throughputQuerySchema = z
  .object({
    window: windowSchema.default(DASHBOARD_LIMITS.DEFAULT_WINDOW),
    bucket: durationSchema(DASHBOARD_LIMITS.MIN_BUCKET_MS, DASHBOARD_LIMITS.MAX_BUCKET_MS).default(
      DASHBOARD_LIMITS.DEFAULT_BUCKET,
    ),
  })
  .strict()
  .superRefine((q, ctx) => {
    if (q.bucket > q.window) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['bucket'],
        message: 'Must not be larger than window',
      });
      return;
    }
    if (q.window % q.bucket !== 0) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['window'],
        message: 'Must be a whole multiple of bucket',
      });
      return;
    }
    if (q.window / q.bucket > DASHBOARD_LIMITS.MAX_BUCKETS) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['bucket'],
        message: `Too many buckets (${q.window / q.bucket}); at most ${DASHBOARD_LIMITS.MAX_BUCKETS} per window, use a larger bucket`,
      });
    }
  });

export const latencyQuerySchema = z
  .object({
    window: durationSchema(
      DASHBOARD_LIMITS.MIN_BUCKET_MS,
      DASHBOARD_LIMITS.MAX_LATENCY_WINDOW_MS,
    ).default(DASHBOARD_LIMITS.DEFAULT_WINDOW),
  })
  .strict();

export const failureRateQuerySchema = z
  .object({ window: windowSchema.default(DASHBOARD_LIMITS.DEFAULT_WINDOW) })
  .strict();
