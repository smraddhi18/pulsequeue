/**
 * Health / readiness checks.
 * Liveness never touches dependencies; readiness runs every registered probe
 * (Postgres from M1, Redis from M5) with a timeout so a hung dependency
 * reports "fail" instead of hanging the probe.
 */

export type CheckStatus = 'ok' | 'fail';

export interface LivenessResult {
  status: 'ok';
  service: string;
  timestamp: string;
}

export interface ReadinessResult {
  status: 'ready' | 'not_ready';
  checks: Record<string, CheckStatus>;
  timestamp: string;
}

/** Resolves when the dependency is healthy, rejects otherwise. */
export type ReadinessProbe = () => Promise<void>;

export interface HealthServiceOptions {
  probes?: Record<string, ReadinessProbe>;
  timeoutMs?: number;
  /** Called for each failing probe (wired to the logger); keeps reasons out of the response body. */
  onProbeFailure?: (name: string, err: unknown) => void;
  bootedAt?: Date;
}

export class HealthService {
  private readonly bootedAt: Date;
  private readonly probes: Record<string, ReadinessProbe>;
  private readonly timeoutMs: number;
  private readonly onProbeFailure: ((name: string, err: unknown) => void) | undefined;

  constructor(options: HealthServiceOptions = {}) {
    this.bootedAt = options.bootedAt ?? new Date();
    this.probes = options.probes ?? {};
    this.timeoutMs = options.timeoutMs ?? 2_000;
    this.onProbeFailure = options.onProbeFailure;
  }

  getLiveness(): LivenessResult {
    return {
      status: 'ok',
      service: 'pulsequeue',
      timestamp: new Date().toISOString(),
    };
  }

  async getReadiness(): Promise<ReadinessResult> {
    const checks: Record<string, CheckStatus> = {
      app: this.bootedAt.getTime() <= Date.now() ? 'ok' : 'fail',
    };

    const results = await Promise.all(
      Object.entries(this.probes).map(async ([name, probe]) => {
        try {
          await withTimeout(probe(), this.timeoutMs, name);
          return [name, 'ok'] as const;
        } catch (err) {
          this.onProbeFailure?.(name, err);
          return [name, 'fail'] as const;
        }
      }),
    );
    for (const [name, status] of results) checks[name] = status;

    const ready = Object.values(checks).every((s) => s === 'ok');
    return {
      status: ready ? 'ready' : 'not_ready',
      checks,
      timestamp: new Date().toISOString(),
    };
  }
}

async function withTimeout<T>(promise: Promise<T>, ms: number, name: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${name} probe timed out after ${ms}ms`)), ms);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
}
