import { z } from 'zod';
import { JOB_LIMITS, PAGINATION_LIMITS } from '../domain/job.js';
import { MISFIRE_POLICIES, SCHEDULE_NAME_PATTERN } from '../domain/schedule.js';
import { compileCron, CRON_LIMITS, InvalidCronError } from '../lib/cron.js';

/**
 * HTTP request shapes for the schedules API. Unknown fields are rejected
 * (.strict()); the cron expression and time zone are evaluated here so a bad
 * schedule is a field-level 400 with the same body as every other validation
 * error. Payload size is a business rule enforced by ScheduleService.
 */

export const scheduleParamsSchema = z.object({
  id: z.string().uuid({ message: 'Schedule id must be a UUID' }),
});

export const createScheduleBodySchema = z
  .object({
    name: z.string().regex(SCHEDULE_NAME_PATTERN, {
      message:
        'Schedule name must be 1-128 chars of A-Z, a-z, 0-9, ".", "_", ":", "-" and start alphanumeric',
    }),
    cron: z.string().min(1).max(CRON_LIMITS.MAX_EXPRESSION_LENGTH),
    timezone: z.string().min(1).max(CRON_LIMITS.MAX_TIMEZONE_LENGTH).optional(),
    payload: z.unknown().refine((v) => v !== undefined, { message: 'payload is required' }),
    priority: z.number().int().min(JOB_LIMITS.MIN_PRIORITY).max(JOB_LIMITS.MAX_PRIORITY).optional(),
    maxAttempts: z
      .number()
      .int()
      .min(JOB_LIMITS.MIN_MAX_ATTEMPTS)
      .max(JOB_LIMITS.MAX_MAX_ATTEMPTS)
      .optional(),
    misfirePolicy: z.enum(MISFIRE_POLICIES).optional(),
    enabled: z.boolean().optional(),
  })
  .strict()
  .superRefine((body, ctx) => {
    try {
      compileCron(body.cron, body.timezone ?? CRON_LIMITS.DEFAULT_TIMEZONE);
    } catch (err) {
      if (!(err instanceof InvalidCronError)) throw err;
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: [err.field], message: err.message });
    }
  });

export const listSchedulesQuerySchema = z
  .object({
    enabled: z
      .enum(['true', 'false'], { message: 'enabled must be "true" or "false"' })
      .transform((v) => v === 'true')
      .optional(),
    limit: z.coerce
      .number()
      .int()
      .min(1)
      .max(PAGINATION_LIMITS.MAX_LIMIT)
      .default(PAGINATION_LIMITS.DEFAULT_LIMIT),
    cursor: z.string().min(1).max(512).optional(),
  })
  .strict();

export type CreateScheduleBody = z.infer<typeof createScheduleBodySchema>;
export type ListSchedulesQuery = z.infer<typeof listSchedulesQuerySchema>;
