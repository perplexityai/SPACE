SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- A user restore's proof that it has not sent its Wake yet. The owner of a
-- Kubernetes user restore writes both columns on the active execution it
-- claims: its sandboxes.restore_lease_token, and an expiry it renews while
-- its process is alive. Immediately before it sends the Wake, it clears them
-- in a statement that also checks the sandbox row still holds its token. A
-- re-drive may take over a restoring sandbox whose active execution still
-- carries the sandbox's current token with an expired fence: that owner has
-- stopped renewing and never sent a Wake, so no node work can be in flight
-- for it. The takeover rotates the token and clears the fence in the same
-- transaction, under the sandbox row lock the clearing statement also takes,
-- so of an owner's Wake and a takeover at most one goes ahead.
--
-- The fence only relaxes ownership: a NULL fence, as on every row that
-- predates its writers or that an older image wrote, keeps the restore lease
-- and execution-age guards as they are. A fence whose token no longer matches
-- the sandbox row, as after an older image took the restore over, counts as
-- NULL.
--
-- Nullable columns with no default change the catalog only; no row is
-- rewritten. The CHECK is NOT VALID so adding it doesn't scan the table; every
-- existing row is NULL and satisfies it, and new writes are checked either way.
ALTER TABLE sandbox_k8s_executions
    ADD COLUMN restore_dispatch_fence_token UUID,
    ADD COLUMN restore_dispatch_fence_expires_at TIMESTAMPTZ,
    ADD CONSTRAINT sandbox_k8s_executions_restore_dispatch_fence_check
        CHECK (
            (restore_dispatch_fence_token IS NULL)
                = (restore_dispatch_fence_expires_at IS NULL)
        ) NOT VALID;

COMMENT ON COLUMN sandbox_k8s_executions.restore_dispatch_fence_token IS
    'Restore lease token of the user restore that owns this execution and has not sent its Wake; set exactly when restore_dispatch_fence_expires_at is';
COMMENT ON COLUMN sandbox_k8s_executions.restore_dispatch_fence_expires_at IS
    'When that owner stops counting as alive unless it renews; set exactly when restore_dispatch_fence_token is';
