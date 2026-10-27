/**
 * Repository layer: the only place that knows SQL. Each repository takes a
 * Queryable (pool or transaction client) and returns domain objects; driver
 * errors are translated to typed domain errors in db/query.ts.
 */
export { QueueRepository, type UpdateQueueSettingsParams } from './queue.repository.js';
export {
  JobRepository,
  type StoredJob,
  type InsertJobParams,
  type InsertJobResult,
  type ListJobsParams,
} from './job.repository.js';
export {
  LeaseRepository,
  MAX_ERROR_LENGTH,
  PROMOTE_BATCH_SIZE,
  truncateError,
  type FailAttemptParams,
  type LeaseJobsParams,
  type ReapExpiredOptions,
} from './lease.repository.js';
export {
  DeadLetterRepository,
  type StoredDeadLetterJob,
  type ListDeadLettersParams,
  type ReplayParams,
  type ReplayBatchParams,
  type PurgeOlderThanParams,
} from './dead-letter.repository.js';
export {
  ScheduleRepository,
  type StoredSchedule,
  type InsertScheduleParams,
  type ListSchedulesParams,
  type RecordFireParams,
} from './schedule.repository.js';
export { QueueStatsRepository } from './stats.repository.js';
export { TenantRepository } from './tenant.repository.js';
export {
  ApiKeyRepository,
  type ApiKeyCredential,
  type InsertApiKeyParams,
  type ListApiKeysParams,
  type RotateApiKeyParams,
  type StoredApiKey,
} from './api-key.repository.js';
