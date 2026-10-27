import { z } from 'zod';
import { API_KEY_LIMITS, API_KEY_ROLES } from '../domain/tenant.js';

const expiresAtSchema = z
  .string()
  .datetime({
    offset: true,
    message: 'expiresAt must be an ISO-8601 date-time with a UTC offset',
  })
  .transform((v) => new Date(v))
  .refine((d) => d.getTime() > Date.now(), { message: 'expiresAt must be in the future' });

export const apiKeyParamsSchema = z.object({
  id: z.string().uuid({ message: 'API key id must be a UUID' }),
});

export const createApiKeyBodySchema = z
  .object({
    name: z.string().trim().min(1).max(API_KEY_LIMITS.NAME_MAX_LENGTH),
    role: z.enum(API_KEY_ROLES),
    expiresAt: expiresAtSchema.optional(),
  })
  .strict();

export const rotateApiKeyBodySchema = z
  .object({
    /** How long the old key keeps working; default 0 = revoke it now. */
    gracePeriodSeconds: z
      .number()
      .int()
      .min(0)
      .max(API_KEY_LIMITS.MAX_ROTATION_GRACE_SECONDS)
      .default(0),
    expiresAt: expiresAtSchema.optional(),
  })
  .strict();

export const listApiKeysQuerySchema = z
  .object({
    limit: z.coerce
      .number()
      .int()
      .min(1)
      .max(API_KEY_LIMITS.MAX_LIST_LIMIT)
      .default(API_KEY_LIMITS.DEFAULT_LIST_LIMIT),
    cursor: z.string().min(1).max(512).optional(),
    includeRevoked: z
      .enum(['true', 'false'], { message: 'includeRevoked must be "true" or "false"' })
      .default('false')
      .transform((v) => v === 'true'),
  })
  .strict();
