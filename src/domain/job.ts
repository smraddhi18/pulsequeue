/**
 * Job-related type stubs for Milestone 0.
 * Concrete schema, enqueue, and leasing land in Milestones 1–2.
 */

export type JobStatus =
  'pending' | 'leased' | 'running' | 'succeeded' | 'failed' | 'dead_lettered' | 'cancelled';

export type JobId = string & { readonly __brand: 'JobId' };
export type QueueName = string & { readonly __brand: 'QueueName' };

export interface Job {
  id: JobId;
  queue: QueueName;
  status: JobStatus;
  payload: unknown;
  priority: number;
  maxAttempts: number;
  attempts: number;
  availableAt: Date;
  createdAt: Date;
  updatedAt: Date;
}

export interface EnqueueJobInput {
  queue: QueueName;
  payload: unknown;
  priority?: number;
  delayMs?: number;
  maxAttempts?: number;
  idempotencyKey?: string;
}
