-- Roll back the images that read and write the column before removing it.
-- Revoked keys stay unusable, because their `key_hash` was already replaced,
-- but the admin key list would show them as live again. Delete them first:
--   DELETE FROM api_keys WHERE revoked_at IS NOT NULL;
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE api_keys
    DROP COLUMN revoked_at;
