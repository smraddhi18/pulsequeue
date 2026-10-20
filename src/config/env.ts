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

const positiveInt = z.coerce.number().int().positive();

/**
 * Database settings. Split out so the migration CLI can validate only what it
 * needs (it must not require REDIS_URL just to apply schema changes).
 */
const databaseEnvShape = {
  DATABASE_URL: postgresUrl,
  /** Max connections per process. Total = replicas x PG_POOL_MAX; keep under max_connections. */
  PG_POOL_MAX: positiveInt.max(200).default(10),
  /** Close idle clients after this long so we release server slots. */
  PG_IDLE_TIMEOUT_MS: positiveInt.default(30_000),
  /** Fail fast when the pool cannot get a connection (surfaces as 503, not a hung request). */
  PG_CONNECTION_TIMEOUT_MS: positiveInt.default(5_000),
  /** Server-side statement_timeout for every pooled connection. */
  PG_STATEMENT_TIMEOUT_MS: positiveInt.default(10_000),
} as const;

const databaseEnvSchema = z.object(databaseEnvShape);

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  HOST: z.string().min(1).default('0.0.0.0'),
  PORT: z.coerce.number().int().positive().max(65535).default(3000),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
  ...databaseEnvShape,
  REDIS_URL: redisUrl,
  /** Max serialized size of a job payload (UTF-8 bytes of its JSON). */
  JOB_MAX_PAYLOAD_BYTES: positiveInt.max(10 * 1024 * 1024).default(256 * 1024),
  /** Readiness probe budget per dependency check. */
  READINESS_TIMEOUT_MS: positiveInt.max(30_000).default(2_000),
});

export type Env = z.infer<typeof envSchema>;
export type DatabaseEnv = z.infer<typeof databaseEnvSchema>;

function parseEnv<T extends z.ZodTypeAny>(schema: T, raw: NodeJS.ProcessEnv): z.infer<T> {
  const result = schema.safeParse(raw);
  if (!result.success) {
    const issues = result.error.issues
      .map((i) => `  - ${i.path.join('.') || '(root)'}: ${i.message}`)
      .join('\n');
    throw new Error(`Invalid environment configuration:\n${issues}`);
  }
  return result.data as z.infer<T>;
}

/**
 * Parse and validate process env. Throws a readable Error on bad config
 * so the process fails fast before binding a port or opening connections.
 */
export function loadEnv(raw: NodeJS.ProcessEnv = process.env): Env {
  return parseEnv(envSchema, raw);
}

/** Database-only subset, used by the migration CLI. */
export function loadDatabaseEnv(raw: NodeJS.ProcessEnv = process.env): DatabaseEnv {
  return parseEnv(databaseEnvSchema, raw);
}
