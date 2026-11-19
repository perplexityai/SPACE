-- Roll back the writers of node_k8s.instance_id before this migration. The
-- recorded instances are dropped; node sync records the current instance
-- again once the migration is re-applied.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE node_k8s
    DROP COLUMN instance_id;
