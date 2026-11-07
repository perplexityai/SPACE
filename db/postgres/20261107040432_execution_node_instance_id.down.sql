-- Roll back the execution store that writes node_instance_id before this
-- migration: its statements name the column. The recorded instances are
-- dropped, and nothing restores them once the migration is re-applied, so the
-- executions that held one read as rows that predate the column.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE sandbox_k8s_executions
    -- squawk-ignore ban-drop-column
    DROP COLUMN IF EXISTS node_instance_id;
