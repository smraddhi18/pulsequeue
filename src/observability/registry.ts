import { collectDefaultMetrics, Registry } from 'prom-client';

export type ProcessRole = 'api' | 'worker' | 'scheduler';

/** Prefix for every metric this project defines (default Node metrics included). */
export const METRIC_PREFIX = 'pulsequeue_';

/**
 * One registry per process role, never prom-client's global one: tests
 * build many apps/workers in one process and each must see only its own
 * series. `role` becomes a default label so one Grafana query can split
 * API, worker and scheduler instances.
 */
export function createMetricsRegistry(role: ProcessRole): Registry {
  const registry = new Registry();
  registry.setDefaultLabels({ role });
  return registry;
}

/**
 * Process metrics (CPU, memory, event-loop lag, GC). Only enabled by the
 * process entry points when a metrics listener is configured: they install
 * perf hooks, which tests building dozens of registries do not need.
 */
export function enableDefaultMetrics(registry: Registry): void {
  collectDefaultMetrics({ register: registry, prefix: METRIC_PREFIX });
}
