-- 0004: delayed-job promotion and recurring schedules (Milestone 4).
--
-- Part 1: delayed jobs.
-- run_at (0001) already says when a job may run. Until M3 workers leased
-- straight from jobs_lease_idx (queue_id, priority DESC, run_at, id) over
-- queued + scheduled rows. That index cannot skip future rows: they sort by
-- priority first, so every poll re-read all not-yet-due delayed/backoff rows
-- that outrank the first due one. With a million jobs delayed for tomorrow at
-- priority 10, every poll for a priority-0 job walked a million index entries.
--
-- From M4 the two states are indexed separately:
--   * scheduled rows are found by due time: (queue_id, run_at) range scan,
--     touching only rows that are actually due;
--   * workers move due scheduled rows to queued ("promotion", bounded batch,
--     SKIP LOCKED) and then lease only queued rows, in priority order.
-- The ready index holds only runnable rows, so the lease scan never walks a
-- future job, and priority still orders everything that is due.
--
-- Expand/contract: jobs_lease_idx is kept so M3 workers still running during a
-- rolling deploy keep an index for their (queued, scheduled) lease query. It
-- is dropped in a later migration once no pre-M4 worker can be running.

CREATE INDEX jobs_ready_idx
  ON jobs (queue_id, priority DESC, run_at, id)
  WHERE status = 'queued';

CREATE INDEX jobs_scheduled_due_idx
  ON jobs (queue_id, run_at, id)
  WHERE status = 'scheduled';

-- Part 2: recurring schedules.
-- One row per (queue, name). The scheduler leader claims due rows with
--   SELECT ... WHERE enabled AND next_run_at <= now()
--   ORDER BY next_run_at FOR UPDATE SKIP LOCKED
-- and, in the same transaction, enqueues the job(s) with the deterministic
-- idempotency key 'schedule:<id>:<fire time>' and advances next_run_at. The
-- row lock, the advanced next_run_at and the jobs unique index each make a
-- duplicate fire impossible, even if two schedulers ever ran a tick at once.

CREATE TYPE schedule_misfire_policy AS ENUM (
  'skip',       -- fire only occurrences within the grace window; drop late ones
  'fire_once',  -- coalesce all late occurrences into a single run (default)
  'catch_up'    -- fire every late occurrence, up to the configured limit
);

CREATE TABLE schedules (
  id               uuid                    PRIMARY KEY DEFAULT gen_random_uuid(),
  queue_id         bigint                  NOT NULL REFERENCES queues (id) ON DELETE RESTRICT,
  name             text                    NOT NULL,
  cron_expression  text                    NOT NULL,
  -- IANA zone the cron expression is evaluated in ('0 9 * * *' = 09:00 local).
  timezone         text                    NOT NULL DEFAULT 'UTC',
  -- Template for every job this schedule enqueues.
  payload          jsonb                   NOT NULL,
  priority         smallint                NOT NULL DEFAULT 0,
  max_attempts     integer                 NOT NULL DEFAULT 3,
  misfire_policy   schedule_misfire_policy NOT NULL DEFAULT 'fire_once',
  enabled          boolean                 NOT NULL DEFAULT true,
  -- Next occurrence to fire; NULL while paused (resume recomputes it from now,
  -- so the paused period is not replayed).
  next_run_at      timestamptz,
  -- Scheduled time of the most recent occurrence that enqueued a job.
  last_run_at      timestamptz,
  -- No FK: jobs can be purged independently of their schedule.
  last_job_id      uuid,
  fire_count       bigint                  NOT NULL DEFAULT 0,
  -- Why the last evaluation failed (cleared by the next successful fire).
  last_error       text,
  created_at       timestamptz             NOT NULL DEFAULT now(),
  updated_at       timestamptz             NOT NULL DEFAULT now(),
  CONSTRAINT schedules_queue_name_uq UNIQUE (queue_id, name),
  CONSTRAINT schedules_name_format CHECK (name ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
  CONSTRAINT schedules_cron_length CHECK (char_length(cron_expression) BETWEEN 1 AND 256),
  CONSTRAINT schedules_timezone_length CHECK (char_length(timezone) BETWEEN 1 AND 64),
  CONSTRAINT schedules_priority_range CHECK (priority BETWEEN 0 AND 100),
  CONSTRAINT schedules_max_attempts_range CHECK (max_attempts BETWEEN 1 AND 100),
  CONSTRAINT schedules_fire_count_non_negative CHECK (fire_count >= 0),
  -- Enabled if and only if there is a next occurrence: pause/resume must keep
  -- both in step, and the scheduler can never pick up a paused schedule.
  CONSTRAINT schedules_enabled_has_next_run CHECK (enabled = (next_run_at IS NOT NULL))
);

CREATE TRIGGER schedules_set_updated_at
  BEFORE UPDATE ON schedules
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Scheduler tick: due, enabled schedules in due order. Partial, so paused
-- schedules cost the tick nothing.
CREATE INDEX schedules_due_idx
  ON schedules (next_run_at)
  WHERE enabled;

-- Keyset pagination for GET /queues/:name/schedules (newest first).
CREATE INDEX schedules_queue_created_idx
  ON schedules (queue_id, created_at DESC, id DESC);
