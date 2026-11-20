SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE node_groups
    DROP CONSTRAINT IF EXISTS node_groups_cache_kms_key_arn_shape_check;

ALTER TABLE node_groups
    DROP COLUMN IF EXISTS cache_kms_key_arn;
