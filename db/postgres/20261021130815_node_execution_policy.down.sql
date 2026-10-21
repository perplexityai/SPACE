-- Roll back policy readers and writers before removing their columns.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE node_status_snapshots
    DROP COLUMN execution_policy,
    DROP COLUMN execution_policy_received_at;
