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

  describe('optional probes (Milestone 5)', () => {
    const ok = async () => undefined;
    const fail = async () => {
      throw new Error('down');
    };

    it('is ready when required and optional probes pass', async () => {
      const service = new HealthService({
        probes: { postgres: ok },
        optionalProbes: { redis: ok },
      });
      const result = await service.getReadiness();
      expect(result).toMatchObject({
        status: 'ready',
        checks: { app: 'ok', postgres: 'ok', redis: 'ok' },
      });
    });

    it('is degraded (not not_ready) when only an optional probe fails', async () => {
      const onProbeFailure = vi.fn();
      const service = new HealthService({
        probes: { postgres: ok },
        optionalProbes: { redis: fail },
        onProbeFailure,
      });
      const result = await service.getReadiness();
      expect(result.status).toBe('degraded');
      expect(result.checks).toEqual({ app: 'ok', postgres: 'ok', redis: 'fail' });
      expect(onProbeFailure).toHaveBeenCalledWith('redis', expect.any(Error));
    });

    it('is not_ready when a required probe fails, whatever the optional ones say', async () => {
      const service = new HealthService({
        probes: { postgres: fail },
        optionalProbes: { redis: fail },
      });
      expect((await service.getReadiness()).status).toBe('not_ready');
    });

    it('applies the timeout to optional probes too', async () => {
      const service = new HealthService({
        probes: { postgres: ok },
        optionalProbes: { redis: () => new Promise<void>(() => undefined) },
        timeoutMs: 20,
      });
      expect((await service.getReadiness()).status).toBe('degraded');
    });

    it('rejects a probe registered as both required and optional', () => {
      expect(
        () => new HealthService({ probes: { redis: ok }, optionalProbes: { redis: ok } }),
      ).toThrow(/registered twice/);
    });
  });

  describe('draining (Milestone 8)', () => {
    it('is not_ready once draining begins, without running any probe', async () => {
      const probe = vi.fn(() => Promise.resolve());
      const service = new HealthService({ probes: { postgres: probe } });
      expect((await service.getReadiness()).status).toBe('ready');
      expect(service.isDraining).toBe(false);

      probe.mockClear();
      service.beginDraining();
      const result = await service.getReadiness();
      expect(service.isDraining).toBe(true);
      expect(result).toMatchObject({
        status: 'not_ready',
        checks: { app: 'fail' },
        draining: true,
      });
      expect(probe).not.toHaveBeenCalled();
    });

    it('keeps liveness ok while draining', () => {
      const service = new HealthService();
      service.beginDraining();
      expect(service.getLiveness().status).toBe('ok');
    });
  });
});
