-- Roll back the images that read and write the column before removing it.
-- Revoked keys are deleted first: their `key_hash` was replaced, so they can't
-- authenticate, but without the column the admin key list would show them as
-- live.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

DELETE FROM api_keys WHERE revoked_at IS NOT NULL;

ALTER TABLE api_keys
    DROP COLUMN revoked_at;
