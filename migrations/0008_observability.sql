-- 0008: observability (Milestone 7): dashboard queries and trace propagation.
--
-- jobs.traceparent: the W3C trace context of the enqueue span (only written
--   when tracing is enabled). The worker starts its execution span as a
--   child of it, so one trace covers HTTP request -> enqueue -> every
--   attempt. 55 chars for version 00; the CHECK keeps garbage out of the
--   column without parsing it in SQL (the application validates the rest).
--
-- job_attempts.queue_id: denormalized from jobs so the dashboard can range
--   scan "attempts of queue X that finished in the last hour" on one index
--   instead of joining every attempt back to its job. Attempts never move
--   between queues, so the copy cannot go stale.
-- job_attempts.ready_at: when the job became runnable (jobs.run_at at lease
--   time). started_at - ready_at is the queue wait of that attempt, measured
--   entirely on the database clock, so worker clock skew cannot distort it.
--
-- Both attempt columns are nullable: rows written before 0008 have no
-- ready_at, and the dashboard simply ignores them for queue-wait latency.

ALTER TABLE jobs ADD COLUMN traceparent text;
ALTER TABLE jobs ADD CONSTRAINT jobs_traceparent_format
  CHECK (traceparent IS NULL OR traceparent ~ '^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$');

ALTER TABLE job_attempts
  ADD COLUMN queue_id  bigint REFERENCES queues (id) ON DELETE CASCADE,
  ADD COLUMN ready_at  timestamptz;

-- Backfill so existing history shows up in throughput and failure rate.
-- One statement is fine at this project's scale; on a large production
-- table this would run out-of-band in id-range batches before the index.
UPDATE job_attempts a
SET queue_id = j.queue_id
FROM jobs j
WHERE j.id = a.job_id AND a.queue_id IS NULL;

-- Throughput, latency and failure rate all read "attempts of one queue
-- finished in [from, to)". INCLUDE makes those scans index-only: the
-- percentile and FILTER aggregates need nothing from the heap.
CREATE INDEX job_attempts_queue_finished_idx
  ON job_attempts (queue_id, finished_at)
  INCLUDE (status, started_at, ready_at)
  WHERE finished_at IS NOT NULL;

-- Dashboard depth counts per (queue, status) are served by the existing
-- jobs_queue_status_created_idx; enqueue throughput by jobs_queue_created_idx;
-- dead-letter throughput by jobs_dlq_idx. No other index is needed.
