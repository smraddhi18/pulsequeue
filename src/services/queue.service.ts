import { NotFoundError } from '../domain/errors.js';
import type { QueueName } from '../domain/job.js';
import type { QueueSettings, UpdateQueueSettingsInput } from '../domain/queue.js';
import type { TenantId } from '../domain/tenant.js';
import type { QueueRepository } from '../repositories/queue.repository.js';

/** The slice of QueueRepository the service needs; lets tests pass in-memory fakes. */
export type QueueSettingsStore = Pick<
  QueueRepository,
  'ensure' | 'findSettings' | 'updateSettings'
>;

/**
 * Queue administration (M6): per-queue concurrency caps and enqueue rate
 * limits. Reads 404 for a queue the tenant does not own; a write creates the
 * queue, so limits can be configured before the first job is enqueued.
 */
export interface QueueServiceOptions {
  /**
   * Called after a successful update, e.g. so this instance's rate limiter
   * forgets its cached copy of the queue's limit at once.
   */
  onSettingsChanged?: (settings: QueueSettings, tenantId: TenantId) => void;
}

export class QueueService {
  constructor(
    private readonly queues: QueueSettingsStore,
    private readonly options: QueueServiceOptions = {},
  ) {}

  async getSettings(tenantId: TenantId, queue: QueueName): Promise<QueueSettings> {
    const settings = await this.queues.findSettings(tenantId, queue);
    if (!settings) throw new NotFoundError(`Queue "${queue}" not found`);
    return settings;
  }

  /** Full replacement: null clears a limit. */
  async updateSettings(input: UpdateQueueSettingsInput): Promise<QueueSettings> {
    const queue = await this.queues.ensure(input.tenantId, input.queue);
    const settings = await this.queues.updateSettings({
      queueId: queue.id,
      maxConcurrency: input.maxConcurrency,
      rateLimit: input.rateLimit,
    });
    this.options.onSettingsChanged?.(settings, input.tenantId);
    return settings;
  }
}
