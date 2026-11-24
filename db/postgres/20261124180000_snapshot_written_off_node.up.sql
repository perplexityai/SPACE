SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Whether the control plane's memory drop wrote this disk artifact. That drop
-- copies a suspended Kubernetes sandbox's rootfs into a new disk prefix after
-- the sandbox's node is gone, and registers the disk in the transaction that
-- re-pins the sandbox to it while leaving the row `suspended`, so the flag is
-- fixed when the drop commits. The restore plan reads it to refuse a restore
-- or start that would pin the sandbox's pod to the gone node, or to a
-- replacement that reused its name. Node presence and lifecycle timestamps
-- can't answer that: a node registration that commits after the drop can carry
-- an earlier `created_at`, the drop doesn't restamp `lifecycle_changed_at`,
-- and a later lifecycle transition overwrites `lifecycle_changed_at`.
--
-- A NOT NULL column with a constant default changes the catalog only; no row
-- is rewritten. Every existing row, and every row an older image inserts, is a
-- node's backup or another path's, so FALSE is accurate for them.
ALTER TABLE snapshots
    ADD COLUMN written_off_node BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN snapshots.written_off_node IS
    'TRUE only on a disk artifact the memory drop wrote while the sandbox''s node was gone';
