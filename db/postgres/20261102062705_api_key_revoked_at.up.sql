SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- When an admin revoked the key. A revoked row stays so the admin key list can
-- show it as revoked instead of dropping it. Revocation also replaces
-- `key_hash` with a value no SHA-256 digest can equal, so a revoked key stops
-- authenticating on every reader of `key_hash`, including releases that don't
-- know this column.
--
-- Nullable with no default, backfill or index: NULL means the key is live, and
-- nothing filters on the column beyond one organization's keys.
ALTER TABLE api_keys
    ADD COLUMN revoked_at timestamptz;
