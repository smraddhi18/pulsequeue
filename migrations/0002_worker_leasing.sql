-- 0002: worker leasing, heartbeats and attempt bookkeeping (Milestone 2).
--
-- A worker owns a job through a *lease*: (locked_by, lease_token,
-- lease_expires_at). Every write a worker makes after leasing is fenced on
-- lease_token, so a worker whose lease expired (GC pause, network partition)
-- can never overwrite the outcome of the worker that re-leased the job.
-- All lease timestamps come from the database clock (now()), never from a
-- worker, so clock skew between worker hosts cannot shorten or extend leases.

-- An attempt released on graceful shutdown: it ran, but was handed back
-- before finishing and does not count against max_attempts. (ADD VALUE is
-- allowed inside a transaction on PG 12+ as long as this migration does not
-- use the new value.)
ALTER TYPE job_attempt_status ADD VALUE IF NOT EXISTS 'released';

ALTER TABLE jobs
  ADD COLUMN locked_by        text,
  ADD COLUMN lease_token      uuid,
  ADD COLUMN locked_at        timestamptz,
  ADD COLUMN lease_expires_at timestamptz,
  ADD COLUMN heartbeat_at     timestamptz;

-- A job is running if and only if it holds a complete lease. Catches any code
-- path (or manual SQL) that would leave a running job unowned (never reaped)
-- or a finished job still "locked".
ALTER TABLE jobs ADD CONSTRAINT jobs_running_has_lease CHECK (
  (status = 'running') = (
    locked_by IS NOT NULL
    AND lease_token IS NOT NULL
    AND locked_at IS NOT NULL
    AND lease_expires_at IS NOT NULL
  )
);

ALTER TABLE jobs ADD CONSTRAINT jobs_locked_by_length
  CHECK (locked_by IS NULL OR char_length(locked_by) BETWEEN 1 AND 200);

-- Reaper path:
--   SELECT ... FROM jobs WHERE status = 'running' AND lease_expires_at < now()
--   ORDER BY lease_expires_at LIMIT $n FOR UPDATE SKIP LOCKED
-- Partial: only in-flight rows (bounded by total worker concurrency) are
-- indexed, so this index stays tiny regardless of table size.
CREATE INDEX jobs_running_lease_expiry_idx
  ON jobs (lease_expires_at)
  WHERE status = 'running';

-- The lease path itself is served by jobs_lease_idx from 0001
-- (queue_id, priority DESC, run_at, id) WHERE status IN ('queued', 'scheduled').

-- At most one in-flight attempt per job: a second concurrent lease of the
-- same job (the bug class this milestone must rule out) fails loudly here.
CREATE UNIQUE INDEX job_attempts_one_running_per_job_uq
  ON job_attempts (job_id)
  WHERE status = 'running';

-- An attempt is finished exactly when it has finished_at.
ALTER TABLE job_attempts ADD CONSTRAINT job_attempts_finished_iff_not_running
  CHECK ((status = 'running') = (finished_at IS NULL));
