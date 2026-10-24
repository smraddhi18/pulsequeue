/**
 * Health / readiness checks.
 * Liveness never touches dependencies; readiness runs every registered probe
 * (Postgres from M1, Redis from M5) with a timeout so a hung dependency
 * reports "fail" instead of hanging the probe.
 *
 * Probes come in two kinds (Milestone 5):
 * - required (Postgres): the source of truth; failing => not_ready (503), the
 *   load balancer stops sending traffic here.
 * - optional (Redis): an accelerator with a Postgres fallback; failing =>
 *   degraded (still 200). Pulling every API replica out of rotation because
 *   the shared cache is down would turn a latency problem into an outage.
 */

export type CheckStatus = 'ok' | 'fail';

export interface LivenessResult {
  status: 'ok';
  service: string;
  timestamp: string;
}

export interface ReadinessResult {
  /** ready: all ok; degraded: only optional checks fail (still serving); not_ready: a required check fails. */
  status: 'ready' | 'degraded' | 'not_ready';
  checks: Record<string, CheckStatus>;
  timestamp: string;
}

/** Resolves when the dependency is healthy, rejects otherwise. */
export type ReadinessProbe = () => Promise<void>;

export interface HealthServiceOptions {
  /** Required dependencies: any failure makes the instance not_ready. */
  probes?: Record<string, ReadinessProbe>;
  /** Optional dependencies: a failure only makes the instance degraded. */
  optionalProbes?: Record<string, ReadinessProbe>;
  timeoutMs?: number;
  /** Called for each failing probe (wired to the logger); keeps reasons out of the response body. */
  onProbeFailure?: (name: string, err: unknown) => void;
  bootedAt?: Date;
}

export class HealthService {
  private readonly bootedAt: Date;
  private readonly probes: Record<string, ReadinessProbe>;
  private readonly optionalProbes: Record<string, ReadinessProbe>;
  private readonly timeoutMs: number;
  private readonly onProbeFailure: ((name: string, err: unknown) => void) | undefined;

  constructor(options: HealthServiceOptions = {}) {
    this.bootedAt = options.bootedAt ?? new Date();
    this.probes = options.probes ?? {};
    this.optionalProbes = options.optionalProbes ?? {};
    for (const name of Object.keys(this.optionalProbes)) {
      if (name in this.probes || name === 'app') {
        throw new Error(`Probe "${name}" is registered twice`);
      }
    }
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

    const required = new Set(['app', ...Object.keys(this.probes)]);
    const all = { ...this.probes, ...this.optionalProbes };
    const results = await Promise.all(
      Object.entries(all).map(async ([name, probe]) => {
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

    const failing = Object.entries(checks).filter(([, s]) => s !== 'ok');
    const status = failing.some(([name]) => required.has(name))
      ? 'not_ready'
      : failing.length > 0
        ? 'degraded'
        : 'ready';
    return {
      status,
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
