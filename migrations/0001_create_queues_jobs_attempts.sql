-- 0001: queues, jobs and job attempts.
--
-- Postgres is the source of truth for every job. Invariants that must hold no
-- matter which code path writes a row (API, worker, scheduler, manual SQL) are
-- enforced here with CHECK / UNIQUE / FK constraints; tighter product policy
-- (e.g. the max_attempts ceiling exposed by the API) lives in the service layer.

CREATE TYPE job_status AS ENUM (
  'queued',     -- ready to run now (run_at <= now())
  'scheduled',  -- waiting for run_at (delayed enqueue or retry backoff)
  'running',    -- leased by a worker
  'succeeded',  -- terminal: finished successfully
  'failed',     -- terminal: non-retryable failure
  'dead'        -- terminal: exhausted max_attempts, lives in the dead-letter view
);

CREATE TYPE job_attempt_status AS ENUM ('running', 'succeeded', 'failed', 'timed_out');

CREATE FUNCTION set_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

-- ---------------------------------------------------------------------------
-- queues
-- ---------------------------------------------------------------------------
CREATE TABLE queues (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name        text        NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT queues_name_uq UNIQUE (name),
  CONSTRAINT queues_name_format CHECK (name ~ '^[a-z0-9][a-z0-9._-]{0,63}$')
);

CREATE TRIGGER queues_set_updated_at
  BEFORE UPDATE ON queues
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- jobs
-- ---------------------------------------------------------------------------
CREATE TABLE jobs (
  id                   uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  queue_id             bigint      NOT NULL REFERENCES queues (id) ON DELETE RESTRICT,
  status               job_status  NOT NULL DEFAULT 'queued',
  priority             smallint    NOT NULL DEFAULT 0,
  payload              jsonb       NOT NULL,
  attempts             integer     NOT NULL DEFAULT 0,
  max_attempts         integer     NOT NULL DEFAULT 3,
  run_at               timestamptz NOT NULL DEFAULT now(),
  idempotency_key      text,
  request_fingerprint  text,
  last_error           text,
  finished_at          timestamptz,
  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT jobs_priority_range CHECK (priority BETWEEN 0 AND 100),
  CONSTRAINT jobs_attempts_non_negative CHECK (attempts >= 0),
  CONSTRAINT jobs_max_attempts_range CHECK (max_attempts BETWEEN 1 AND 100),
  CONSTRAINT jobs_attempts_within_max CHECK (attempts <= max_attempts),
  CONSTRAINT jobs_idempotency_key_length
    CHECK (idempotency_key IS NULL OR char_length(idempotency_key) BETWEEN 1 AND 255),
  -- A key without a fingerprint could never be safely compared on replay.
  CONSTRAINT jobs_idempotency_key_has_fingerprint
    CHECK ((idempotency_key IS NULL) = (request_fingerprint IS NULL))
);

CREATE TRIGGER jobs_set_updated_at
  BEFORE UPDATE ON jobs
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Idempotency: at most one job per (queue, key). Partial so the (majority of)
-- jobs enqueued without a key add nothing to this index. Enqueue relies on
-- INSERT ... ON CONFLICT against this index; never check-then-insert.
CREATE UNIQUE INDEX jobs_queue_idempotency_key_uq
  ON jobs (queue_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

-- Lease path (Milestone 2):
--   SELECT ... FROM jobs
--   WHERE queue_id = $1 AND status IN ('queued', 'scheduled') AND run_at <= now()
--   ORDER BY priority DESC, run_at, id
--   LIMIT $n FOR UPDATE SKIP LOCKED
-- Partial: only runnable rows are indexed, so the index stays small even when
-- the table holds millions of finished jobs, and the scan walks rows in lease
-- order without a sort.
CREATE INDEX jobs_lease_idx
  ON jobs (queue_id, priority DESC, run_at, id)
  WHERE status IN ('queued', 'scheduled');

-- Keyset pagination for GET /queues/:name/jobs (newest first), with and
-- without a status filter. The unfiltered index also covers the queue_id FK.
CREATE INDEX jobs_queue_created_idx
  ON jobs (queue_id, created_at DESC, id DESC);

CREATE INDEX jobs_queue_status_created_idx
  ON jobs (queue_id, status, created_at DESC, id DESC);

-- ---------------------------------------------------------------------------
-- job_attempts: one row per execution attempt (written by workers from M2).
-- ---------------------------------------------------------------------------
CREATE TABLE job_attempts (
  id              bigint             GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  job_id          uuid               NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  attempt_number  integer            NOT NULL,
  status          job_attempt_status NOT NULL DEFAULT 'running',
  worker_id       text,
  error           text,
  started_at      timestamptz        NOT NULL DEFAULT now(),
  finished_at     timestamptz,
  CONSTRAINT job_attempts_attempt_number_positive CHECK (attempt_number > 0),
  CONSTRAINT job_attempts_finished_after_start
    CHECK (finished_at IS NULL OR finished_at >= started_at),
  -- Also serves FK lookups / "attempt history for job X" in attempt order.
  CONSTRAINT job_attempts_job_attempt_uq UNIQUE (job_id, attempt_number)
);
