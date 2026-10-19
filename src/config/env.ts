import { z } from 'zod';

const postgresUrl = z
  .string()
  .min(1)
  .refine((v) => v.startsWith('postgres://') || v.startsWith('postgresql://'), {
    message: 'Must be a postgres:// or postgresql:// URL',
  });

const redisUrl = z
  .string()
  .min(1)
  .refine((v) => v.startsWith('redis://') || v.startsWith('rediss://'), {
    message: 'Must be a redis:// or rediss:// URL',
  });

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  HOST: z.string().min(1).default('0.0.0.0'),
  PORT: z.coerce.number().int().positive().max(65535).default(3000),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
  DATABASE_URL: postgresUrl,
  REDIS_URL: redisUrl,
});

export type Env = z.infer<typeof envSchema>;

/**
 * Parse and validate process env. Throws a readable Error on bad config
 * so the process fails fast before binding a port or opening connections.
 */
export function loadEnv(raw: NodeJS.ProcessEnv = process.env): Env {
  const result = envSchema.safeParse(raw);
  if (!result.success) {
    const issues = result.error.issues
      .map((i) => `  - ${i.path.join('.') || '(root)'}: ${i.message}`)
      .join('\n');
    throw new Error(`Invalid environment configuration:\n${issues}`);
  }
  return result.data;
}
