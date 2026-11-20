SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The tenant's KMS key (BYOK) for a dedicated group's node caches. A node is
-- claimed for the group only after it reboots with its instance-store cache
-- on LUKS under a data key from this key. NULL for unencrypted groups.
ALTER TABLE node_groups
    ADD COLUMN cache_kms_key_arn TEXT;

ALTER TABLE node_groups
    ADD CONSTRAINT node_groups_cache_kms_key_arn_shape_check
    CHECK (cache_kms_key_arn IS NULL
        OR (pool IS NOT NULL AND cache_kms_key_arn LIKE 'arn:aws%:kms:%:key/%'));
