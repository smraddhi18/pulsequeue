import { describe, expect, it } from 'vitest';
import { loadEnv, loadSchedulerEnv, loadWorkerEnv } from '../../src/config/env.js';

const base = {
  NODE_ENV: 'test',
  DATABASE_URL: 'postgres://pulsequeue:pulsequeue@localhost:5432/pulsequeue',
} as const;

describe('observability env (M7)', () => {
  it('defaults: no metrics listener, loopback host, tracing off, depth gauge on', () => {
    const env = loadEnv({ ...base });
    expect(env.METRICS_PORT).toBeUndefined();
    expect(env.METRICS_HOST).toBe('127.0.0.1');
    expect(env.METRICS_TOKEN).toBeUndefined();
    expect(env.METRICS_MAX_QUEUE_SERIES).toBe(1000);
    expect(env.METRICS_QUEUE_DEPTH_ENABLED).toBe(true);
    expect(env.METRICS_QUEUE_DEPTH_CACHE_MS).toBe(5000);
    expect(env.TRACING_ENABLED).toBe(false);
    expect(env.OTEL_TRACES_SAMPLER_RATIO).toBe(1);
    expect(env.DASHBOARD_LATENCY_MAX_SAMPLES).toBe(100_000);
  });

  it('parses metrics and tracing settings for every process kind', () => {
    const raw = {
      ...base,
      METRICS_PORT: '9464',
      METRICS_HOST: '0.0.0.0',
      METRICS_TOKEN: 'a-long-enough-token',
      TRACING_ENABLED: 'true',
      OTEL_EXPORTER_OTLP_ENDPOINT: 'http://jaeger:4318',
      OTEL_TRACES_SAMPLER_RATIO: '0.25',
    };
    for (const env of [loadEnv(raw), loadWorkerEnv(raw), loadSchedulerEnv(raw)]) {
      expect(env.METRICS_PORT).toBe(9464);
      expect(env.METRICS_HOST).toBe('0.0.0.0');
      expect(env.TRACING_ENABLED).toBe(true);
      expect(env.OTEL_TRACES_SAMPLER_RATIO).toBe(0.25);
    }
  });

  it('requires an OTLP endpoint when tracing is enabled', () => {
    expect(() => loadWorkerEnv({ ...base, TRACING_ENABLED: 'true' })).toThrow(
      /OTEL_EXPORTER_OTLP_ENDPOINT/,
    );
    expect(() => loadEnv({ ...base, OTEL_EXPORTER_OTLP_ENDPOINT: 'ftp://collector:4318' })).toThrow(
      /http/,
    );
  });

  it('requires METRICS_TOKEN in production when metrics are exposed beyond loopback', () => {
    const prod = { ...base, NODE_ENV: 'production', METRICS_PORT: '9464' };
    expect(() => loadWorkerEnv({ ...prod, METRICS_HOST: '0.0.0.0' })).toThrow(/METRICS_TOKEN/);
    expect(() => loadSchedulerEnv({ ...prod, METRICS_HOST: '0.0.0.0' })).toThrow(/METRICS_TOKEN/);
    // Loopback (a sidecar scraper) or a token are both fine.
    expect(loadWorkerEnv({ ...prod }).METRICS_HOST).toBe('127.0.0.1');
    expect(
      loadWorkerEnv({ ...prod, METRICS_HOST: '0.0.0.0', METRICS_TOKEN: 'a-long-enough-token' })
        .METRICS_TOKEN,
    ).toBe('a-long-enough-token');
  });

  it('rejects bad values', () => {
    expect(() => loadEnv({ ...base, METRICS_PORT: '70000' })).toThrow(/METRICS_PORT/);
    expect(() => loadEnv({ ...base, METRICS_TOKEN: 'short' })).toThrow(/METRICS_TOKEN/);
    expect(() => loadEnv({ ...base, OTEL_TRACES_SAMPLER_RATIO: '1.5' })).toThrow(
      /OTEL_TRACES_SAMPLER_RATIO/,
    );
  });
});
