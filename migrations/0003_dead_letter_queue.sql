-- 0003: dead-letter queue, replay bookkeeping and poison-message counter (Milestone 3).
--
-- Design: a dead-lettered job stays in `jobs` with status 'dead' (the value
-- reserved for this in 0001) plus the DLQ metadata below, instead of being
-- moved into a separate dead_letters table. Why:
--   * The dead-letter transition is the same single fenced UPDATE as any other
--     outcome (WHERE status = 'running' AND lease_token = $token), so it is
--     atomic with recording the attempt and a stale worker cannot dead-letter.
--   * job_attempts keeps its FK to the same job id: the full history survives
--     dead-lettering and replays without copying rows between tables.
--   * Replay is an UPDATE of the same row: the job keeps its id (clients
--     polling GET /jobs/:id see it come back) and its idempotency key (a
--     producer retrying enqueue still gets this job, not a duplicate).
-- Trade-off: DLQ rows share the jobs table. The partial index below keeps DLQ
-- reads independent of the live set, and the lease index (0001) is partial on
-- queued/scheduled, so dead rows cost the hot path nothing.

CREATE TYPE dead_letter_reason AS ENUM (
  'attempts_exhausted',   -- every counted attempt failed
  'non_retryable',        -- handler threw NonRetryableError / retryable: false
  'invalid_payload',      -- handler rejected the payload (InvalidPayloadError)
  'poison_lease_expired'  -- lease expired N times: the job keeps killing workers
);

-- All metadata-only on PG 11+ (nullable columns, constant defaults): no table rewrite.
ALTER TABLE jobs
  ADD COLUMN dead_reason       dead_letter_reason,
  ADD COLUMN dead_lettered_at  timestamptz,
  -- Poison-message counter: lease expiries since enqueue or the last replay.
  ADD COLUMN lease_expiries    integer NOT NULL DEFAULT 0,
  ADD COLUMN replay_count      integer NOT NULL DEFAULT 0,
  ADD COLUMN last_replayed_at  timestamptz;

-- M2 parked terminal failures in 'failed'; move them into the DLQ so they can
-- be inspected and replayed like any other dead-lettered job.
UPDATE jobs
SET status           = 'dead',
    dead_reason      = CASE WHEN attempts >= max_attempts THEN 'attempts_exhausted'
                            ELSE 'non_retryable' END::dead_letter_reason,
    dead_lettered_at = coalesce(finished_at, updated_at)
WHERE status = 'failed';

-- A job is dead if and only if it carries DLQ metadata: replay must clear it,
-- dead-lettering must set it, whichever code path (or manual SQL) does it.
ALTER TABLE jobs ADD CONSTRAINT jobs_dead_has_metadata CHECK (
  (status = 'dead') = (dead_reason IS NOT NULL AND dead_lettered_at IS NOT NULL)
);

-- 'failed' cannot be dropped from the enum, so forbid it: every terminal
-- failure goes to the DLQ from now on.
ALTER TABLE jobs ADD CONSTRAINT jobs_no_legacy_failed CHECK (status <> 'failed');

ALTER TABLE jobs
  ADD CONSTRAINT jobs_lease_expiries_non_negative CHECK (lease_expiries >= 0),
  ADD CONSTRAINT jobs_replay_count_non_negative CHECK (replay_count >= 0);

-- DLQ reads and purges:
--   GET    /queues/:name/dlq  ORDER BY dead_lettered_at DESC, id DESC (keyset)
--   DELETE /queues/:name/dlq?olderThan=...  (range on dead_lettered_at)
--   POST   /queues/:name/dlq/replay  (oldest first)
-- Partial: only dead rows are indexed, so it is as small as the DLQ itself.
CREATE INDEX jobs_dlq_idx
  ON jobs (queue_id, dead_lettered_at DESC, id DESC)
  WHERE status = 'dead';
