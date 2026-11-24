-- Roll back the images that read and write this column, then run this.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Refuse while an unplaced sandbox is still pinned to a disk the memory drop
-- wrote. Turning the drop off doesn't undo those pins, and the flag is the only
-- record that no node holds the disk: dropping it lets a restore or start pin a
-- pod to the node name the sandbox's executions recorded, which is gone or a
-- replacement that doesn't hold the disk. Repoint those sandboxes to their
-- retained checkpoints, delete them, or boot them once a release can boot an
-- off-node disk on a new node, before rolling back this migration.
-- A flagged pin only appears in a transaction that inserts into snapshots, so
-- locking snapshots alone fences the drop: an in-flight one finishes first and
-- the check sees it, and no new one starts before the column drop. Locking
-- sandboxes too could deadlock with a drop that holds snapshots and then
-- updates the row.
LOCK TABLE snapshots IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1
                 FROM sandboxes s
                 JOIN snapshots sn
                   ON sn.sandbox_id = s.id
                  AND sn.local_artifact_id = s.current_local_artifact_id
                WHERE s.active_node_id IS NULL
                  AND sn.written_off_node) THEN
        RAISE EXCEPTION 'an unplaced sandbox is pinned to a disk the off-node '
            'stop wrote, and dropping snapshots.written_off_node would let its '
            'start pin a pod to a node that does not hold the disk; delete those '
            'sandboxes before rolling back this migration';
    END IF;
END;
$$;

ALTER TABLE snapshots
    DROP COLUMN written_off_node;
