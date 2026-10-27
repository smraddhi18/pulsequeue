import { z } from 'zod';
import { QUEUE_SETTINGS_LIMITS } from '../domain/queue.js';

const rateLimitSchema = z
  .object({
    perSec: z.number().int().min(1).max(QUEUE_SETTINGS_LIMITS.MAX_RATE_PER_SEC),
    burst: z.number().int().min(1).max(QUEUE_SETTINGS_LIMITS.MAX_BURST),
  })
  .strict();

/**
 * PUT /queues/:name/settings is a full replacement: both keys are required
 * and `null` clears a limit, so a client can never half-update by omission.
 */
export const updateQueueSettingsBodySchema = z
  .object({
    maxConcurrency: z.number().int().min(1).max(QUEUE_SETTINGS_LIMITS.MAX_CONCURRENCY).nullable(),
    rateLimit: rateLimitSchema.nullable(),
  })
  .strict();
