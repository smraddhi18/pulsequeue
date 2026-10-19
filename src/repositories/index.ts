/**
 * Repository layer stubs.
 * Milestone 1 introduces JobRepository / QueueRepository backed by Postgres.
 * Keep data access out of routes and services through these interfaces.
 */

export interface RepositoryContext {
  // Placeholder for a future DB client / transaction handle.
  readonly ready: boolean;
}

export function createRepositoryContext(): RepositoryContext {
  return { ready: false };
}
