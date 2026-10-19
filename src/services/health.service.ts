/**
 * Health / readiness checks.
 * Milestone 0: app-booted only. Later milestones will probe Postgres + Redis.
 */

export interface LivenessResult {
  status: 'ok';
  service: string;
  timestamp: string;
}

export interface ReadinessResult {
  status: 'ready' | 'not_ready';
  checks: {
    app: 'ok' | 'fail';
  };
  timestamp: string;
}

export class HealthService {
  private readonly bootedAt: Date;

  constructor(bootedAt: Date = new Date()) {
    this.bootedAt = bootedAt;
  }

  getLiveness(): LivenessResult {
    return {
      status: 'ok',
      service: 'pulsequeue',
      timestamp: new Date().toISOString(),
    };
  }

  getReadiness(): ReadinessResult {
    // M0: process is ready once the app has been constructed.
    // M1+: also require DATABASE_URL connectivity and Redis PING.
    const appOk = this.bootedAt.getTime() <= Date.now();
    return {
      status: appOk ? 'ready' : 'not_ready',
      checks: {
        app: appOk ? 'ok' : 'fail',
      },
      timestamp: new Date().toISOString(),
    };
  }
}
