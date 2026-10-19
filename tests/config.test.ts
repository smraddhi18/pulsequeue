import { describe, expect, it } from 'vitest';
import { loadEnv } from '../src/config/env.js';

const validEnv = {
  NODE_ENV: 'test',
  HOST: '127.0.0.1',
  PORT: '3000',
  LOG_LEVEL: 'error',
  DATABASE_URL: 'postgres://pulsequeue:pulsequeue@localhost:5432/pulsequeue',
  REDIS_URL: 'redis://localhost:6379',
} as const;

describe('loadEnv', () => {
  it('parses a valid environment', () => {
    const env = loadEnv({ ...validEnv });
    expect(env.NODE_ENV).toBe('test');
    expect(env.HOST).toBe('127.0.0.1');
    expect(env.PORT).toBe(3000);
    expect(env.LOG_LEVEL).toBe('error');
    expect(env.DATABASE_URL).toContain('postgres://');
    expect(env.REDIS_URL).toContain('redis://');
  });

  it('applies defaults for optional fields', () => {
    const env = loadEnv({
      DATABASE_URL: validEnv.DATABASE_URL,
      REDIS_URL: validEnv.REDIS_URL,
    });
    expect(env.NODE_ENV).toBe('development');
    expect(env.HOST).toBe('0.0.0.0');
    expect(env.PORT).toBe(3000);
    expect(env.LOG_LEVEL).toBe('info');
  });

  it('fails fast on missing DATABASE_URL', () => {
    expect(() =>
      loadEnv({
        REDIS_URL: validEnv.REDIS_URL,
      }),
    ).toThrow(/Invalid environment configuration/);
  });

  it('fails fast on invalid PORT', () => {
    expect(() =>
      loadEnv({
        ...validEnv,
        PORT: 'not-a-number',
      }),
    ).toThrow(/Invalid environment configuration/);
  });

  it('fails fast on invalid NODE_ENV', () => {
    expect(() =>
      loadEnv({
        ...validEnv,
        NODE_ENV: 'staging',
      }),
    ).toThrow(/Invalid environment configuration/);
  });
});
