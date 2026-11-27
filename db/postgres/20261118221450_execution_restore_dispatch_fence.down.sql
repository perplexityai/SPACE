-- Roll back the images that read and write these columns, then run this.
-- Dropping them is safe at any time for the restores themselves: the fence
-- only lets a re-drive take over sooner, and without it every restore keeps
-- the restore lease and execution-age guards.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE sandbox_k8s_executions
    DROP CONSTRAINT sandbox_k8s_executions_restore_dispatch_fence_check,
    DROP COLUMN restore_dispatch_fence_token,
    DROP COLUMN restore_dispatch_fence_expires_at;
