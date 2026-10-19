SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The execution store and the backup fence of the release that added this
-- column write and read it, so roll those services back first. Without the
-- column the fence reads only `pod_uid`, as it did before.
ALTER TABLE sandbox_k8s_executions
    -- squawk-ignore ban-drop-column
    DROP COLUMN IF EXISTS pod_materialized_at;
