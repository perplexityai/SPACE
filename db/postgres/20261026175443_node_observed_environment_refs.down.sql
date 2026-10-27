-- Roll back environment readers and writers before removing their columns.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Dropping the columns drops their index and CHECK.
ALTER TABLE node_status_snapshots
    DROP COLUMN usable_environment_refs,
    DROP COLUMN known_environment_refs,
    DROP COLUMN environment_refs_received_at;
