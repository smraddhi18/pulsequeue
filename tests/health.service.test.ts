import { describe, expect, it, vi } from 'vitest';
import { HealthService } from '../src/services/health.service.js';

describe('HealthService.getReadiness', () => {
  it('is ready when every probe passes', async () => {
    const service = new HealthService({ probes: { postgres: async () => undefined } });
    const result = await service.getReadiness();
    expect(result.status).toBe('ready');
    expect(result.checks).toEqual({ app: 'ok', postgres: 'ok' });
  });

  it('is not ready when a probe rejects, and reports the reason out-of-band', async () => {
    const onProbeFailure = vi.fn();
    const service = new HealthService({
      probes: {
        postgres: async () => {
          throw new Error('ECONNREFUSED');
        },
      },
      onProbeFailure,
    });
    const result = await service.getReadiness();
    expect(result.status).toBe('not_ready');
    expect(result.checks.postgres).toBe('fail');
    expect(JSON.stringify(result)).not.toContain('ECONNREFUSED');
    expect(onProbeFailure).toHaveBeenCalledWith('postgres', expect.any(Error));
  });

  it('fails a probe that hangs past the timeout', async () => {
    const service = new HealthService({
      probes: { postgres: () => new Promise<void>(() => undefined) },
      timeoutMs: 20,
    });
    const started = Date.now();
    const result = await service.getReadiness();
    expect(result.checks.postgres).toBe('fail');
    expect(Date.now() - started).toBeLessThan(1_000);
  });
});
