-- 0005: tenants and API keys (Milestone 6, part 1: authentication).
--
-- Additive only: nothing existing reads or writes these tables, so M5
-- processes keep working while this runs. Queue ownership (queues.tenant_id)
-- follows in 0006.
--
-- tenants.id is a short, immutable, human-readable slug ('acme', 'default'),
-- used verbatim in API keys' tenant_id, in logs and in Redis key names (the
-- format excludes ':' '{' '}' so it can never break a key or a hash tag).

CREATE TABLE tenants (
  id                  text        PRIMARY KEY,
  name                text        NOT NULL,
  -- Per-tenant enqueue rate limit override (token bucket); NULL = the
  -- RATE_LIMIT_TENANT_* defaults. Both or neither.
  rate_limit_per_sec  integer,
  rate_limit_burst    integer,
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT tenants_id_format CHECK (id ~ '^[a-z0-9][a-z0-9-]{0,62}$'),
  CONSTRAINT tenants_name_length CHECK (char_length(name) BETWEEN 1 AND 200),
  -- Written NULL-safely: a CHECK that evaluates to NULL passes.
  CONSTRAINT tenants_rate_limit_positive CHECK (
    (rate_limit_per_sec IS NULL) = (rate_limit_burst IS NULL)
    AND coalesce(rate_limit_per_sec, 1) > 0
    AND coalesce(rate_limit_burst, 1) > 0
  )
);

CREATE TRIGGER tenants_set_updated_at
  BEFORE UPDATE ON tenants
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Every queue that exists before 0006 is assigned to this tenant.
INSERT INTO tenants (id, name) VALUES ('default', 'Default tenant');

-- admin:    everything in the tenant, including API keys, queue settings,
--           schedules and destructive DLQ operations (replay, purge)
-- producer: enqueue jobs + every read
-- reader:   reads only (jobs, DLQ, schedules, stats)
CREATE TYPE api_key_role AS ENUM ('admin', 'producer', 'reader');

-- A key is `pq_<prefix>_<secret>`. Only SHA-256(full token) is stored: the
-- secret is 256 bits from a CSPRNG, so a fast hash is enough (there is
-- nothing to brute-force, unlike a password) and verification costs
-- microseconds per request. `prefix` is random and non-secret: it is the
-- lookup key (unique index) and what the API shows to identify a key.
CREATE TABLE api_keys (
  id            uuid          PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id     text          NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
  name          text          NOT NULL,
  prefix        text          NOT NULL,
  secret_hash   bytea         NOT NULL,
  role          api_key_role  NOT NULL,
  created_at    timestamptz   NOT NULL DEFAULT now(),
  expires_at    timestamptz,
  last_used_at  timestamptz,
  revoked_at    timestamptz,
  -- Set on the key a rotation created; the old key keeps a grace period.
  rotated_from  uuid          REFERENCES api_keys (id) ON DELETE SET NULL,
  CONSTRAINT api_keys_prefix_uq UNIQUE (prefix),
  CONSTRAINT api_keys_prefix_format CHECK (prefix ~ '^[a-z0-9]{12}$'),
  CONSTRAINT api_keys_secret_hash_sha256 CHECK (octet_length(secret_hash) = 32),
  CONSTRAINT api_keys_name_length CHECK (char_length(name) BETWEEN 1 AND 100),
  CONSTRAINT api_keys_expiry_after_creation CHECK (expires_at IS NULL OR expires_at > created_at)
);

-- GET /api-keys: a tenant's keys, newest first (keyset on created_at, id).
CREATE INDEX api_keys_tenant_created_idx ON api_keys (tenant_id, created_at DESC, id DESC);
