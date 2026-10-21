/**
 * Repository layer: the only place that knows SQL. Each repository takes a
 * Queryable (pool or transaction client) and returns domain objects; driver
 * errors are translated to typed domain errors in db/query.ts.
 */
export { QueueRepository } from './queue.repository.js';
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
  truncateError,
  type FailAttemptParams,
  type LeaseJobsParams,
} from './lease.repository.js';
