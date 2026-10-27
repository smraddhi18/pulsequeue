-- 0006: queues belong to tenants (Milestone 6, part 2: isolation).
--
-- Every existing queue is assigned to the 'default' tenant created in 0005.
-- ADD COLUMN with a constant DEFAULT is a catalog-only change (no table
-- rewrite); the FK is then checked against the (small) queues table.
--
-- The DEFAULT stays on purpose (expand phase): processes and fixtures that
-- predate tenancy keep inserting into the default tenant. API servers always
-- pass tenant_id explicitly.
--
-- Queue names are now unique per tenant, not globally: tenants 'acme' and
-- 'globex' can both own an 'emails' queue. Jobs, schedules and DLQ entries
-- hang off queues by id, so they inherit the owner without a column of
-- their own, and every API lookup by id joins queues and filters on
-- tenant_id (another tenant's resources are indistinguishable from missing
-- ones: 404).
--
-- Rollout: drain M5 API servers before applying. Their get-or-create uses
-- ON CONFLICT (name), which has no matching unique constraint after this.
-- M5 workers and schedulers are unaffected (they look queues up by name).

ALTER TABLE queues
  ADD COLUMN tenant_id text NOT NULL DEFAULT 'default'
    REFERENCES tenants (id) ON DELETE RESTRICT;

ALTER TABLE queues DROP CONSTRAINT queues_name_uq;
ALTER TABLE queues ADD CONSTRAINT queues_tenant_name_uq UNIQUE (tenant_id, name);

-- Workers are a shared fleet and consume a queue name across all tenants
-- (WHERE q.name = ANY($1)); the (tenant_id, name) index cannot serve that.
CREATE INDEX queues_name_idx ON queues (name);
