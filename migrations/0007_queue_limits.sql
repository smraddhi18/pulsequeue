-- 0007: per-queue limits (Milestone 6, part 3: fairness).
--
-- Both limits are opt-in and NULL means "unlimited", so every existing queue
-- keeps its M5 behaviour and the ALTERs are catalog-only (no rewrite).
--
-- max_concurrency: at most this many jobs of the queue are 'running' at
--   once across the whole worker fleet. Enforced inside the lease
--   transaction (see LeaseRepository), not by workers, so it holds with any
--   number of worker processes.
-- rate_limit_per_sec / rate_limit_burst: an enqueue token bucket for this
--   queue, checked in Redis on top of the tenant's bucket. Both or neither.

ALTER TABLE queues
  ADD COLUMN max_concurrency     integer,
  ADD COLUMN rate_limit_per_sec  integer,
  ADD COLUMN rate_limit_burst    integer;

-- Written NULL-safely: a CHECK that evaluates to NULL passes.
ALTER TABLE queues ADD CONSTRAINT queues_max_concurrency_positive
  CHECK (coalesce(max_concurrency, 1) > 0);

ALTER TABLE queues ADD CONSTRAINT queues_rate_limit_positive CHECK (
  (rate_limit_per_sec IS NULL) = (rate_limit_burst IS NULL)
  AND coalesce(rate_limit_per_sec, 1) > 0
  AND coalesce(rate_limit_burst, 1) > 0
);

-- The capped lease counts running jobs per queue:
--   SELECT count(*) FROM jobs WHERE queue_id = $1 AND status = 'running'
-- Partial: only in-flight rows are indexed (bounded by fleet concurrency),
-- so the count is an index-only scan over a handful of entries.
CREATE INDEX jobs_running_queue_idx
  ON jobs (queue_id)
  WHERE status = 'running';
