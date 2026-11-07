SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The lifecycle a restore episode lands the sandbox in when it isn't the
-- default: `started`, `paused` or `stopped`. It is the node drain's v2
-- counterpart of `evacuation_restore_target`, which only the v1 drain writes
-- and the `sandboxes_clear_evacuation_restore_target` trigger clears. Every
-- reader of that marker that asks whether a restore is the system's, or which
-- lifecycle it lands in, reads this column too, with the same meaning.
--
-- No trigger maintains it: each statement that ends a restore episode (its
-- commit, a revert, the transient sweeper's fail or converge, a delete, a
-- lost or failure give-up, an activation rollback) clears it in its own SET.
-- Nothing writes a value yet, so every row stays NULL and every reader
-- behaves as it did before this migration.
--
-- Nullable with no default, backfill or index. The first CHECK holds it to
-- the three landing lifecycles. The second keeps it and the v1 marker from
-- both being set on one row, so a reader never has to pick between them. Both
-- are NOT VALID so adding them doesn't scan `sandboxes`: every existing row
-- has this column NULL, which satisfies both, and new writes are checked
-- either way.
ALTER TABLE sandboxes
    ADD COLUMN restore_episode_target text,
    ADD CONSTRAINT sandboxes_restore_episode_target_check
        CHECK (restore_episode_target IS NULL
               OR restore_episode_target IN ('started', 'paused', 'stopped'))
        NOT VALID,
    ADD CONSTRAINT sandboxes_restore_target_exclusive_check
        CHECK (NOT (evacuation_restore_target IS NOT NULL
                    AND restore_episode_target IS NOT NULL))
        NOT VALID;
