import { NotFoundError } from '../domain/errors.js';
import type {
  DashboardQueuePage,
  FailureRateReport,
  LatencyReport,
  ThroughputSeries,
} from '../domain/dashboard.js';
import type { QueueName } from '../domain/job.js';
import type { TenantId } from '../domain/tenant.js';
import type { DashboardRepository } from '../repositories/dashboard.repository.js';
import type { QueueStore } from './job.service.js';

export type DashboardStore = Pick<
  DashboardRepository,
  'listQueues' | 'throughput' | 'latency' | 'failureRate'
>;

export interface DashboardServiceOptions {
  /** Cap on attempts fed into one percentile computation (DASHBOARD_LATENCY_MAX_SAMPLES). */
  latencySampleLimit: number;
}

/**
 * Dashboard reads (Milestone 7). Tenant isolation lives here: the queue is
 * resolved by (tenant, name) first, so another tenant's queue with the same
 * name is a 404, exactly like a queue that does not exist, and the
 * repository only ever sees an id the caller owns. Reads never create
 * queues.
 */
export class DashboardService {
  constructor(
    private readonly queues: Pick<QueueStore, 'findByName'>,
    private readonly dashboard: DashboardStore,
    private readonly options: DashboardServiceOptions,
  ) {}

  listQueues(tenantId: TenantId, limit: number, after: string | null): Promise<DashboardQueuePage> {
    return this.dashboard.listQueues(tenantId, limit, after);
  }

  async throughput(
    tenantId: TenantId,
    queue: QueueName,
    windowMs: number,
    bucketMs: number,
  ): Promise<ThroughputSeries> {
    const queueId = await this.resolve(tenantId, queue);
    return this.dashboard.throughput(queueId, queue, windowMs, bucketMs);
  }

  async latency(tenantId: TenantId, queue: QueueName, windowMs: number): Promise<LatencyReport> {
    const queueId = await this.resolve(tenantId, queue);
    return this.dashboard.latency({
      queueId,
      queue,
      windowMs,
      sampleLimit: this.options.latencySampleLimit,
    });
  }

  async failureRate(
    tenantId: TenantId,
    queue: QueueName,
    windowMs: number,
  ): Promise<FailureRateReport> {
    const queueId = await this.resolve(tenantId, queue);
    return this.dashboard.failureRate(queueId, queue, windowMs);
  }

  private async resolve(tenantId: TenantId, queue: QueueName): Promise<string> {
    const found = await this.queues.findByName(tenantId, queue);
    if (!found) throw new NotFoundError('Queue not found', { queue });
    return found.id;
  }
}
