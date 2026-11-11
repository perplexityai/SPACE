-- Cleanup ownership moves out of the sandbox lifecycle into its own column.
-- `state` keeps only lifecycle states. `cleanup_state` records whether a
-- forced teardown (`required`) or a teardown whose last attempt failed
-- (`failed`) owns the row; such a row's `state` is the transition its
-- teardown is in: `deleting` for a row-deleting cleanup, `suspending` for a
-- suspend whose artifact is pinned, otherwise `stopping`. `legacy_state`
-- rebuilds the combined status every reader used before the split, so those
-- readers keep their answers while they move to the two columns.
ALTER TABLE sandboxes ADD COLUMN cleanup_state TEXT
    CHECK (cleanup_state IS NULL OR cleanup_state IN ('required', 'failed'));

ALTER TABLE sandboxes ADD COLUMN legacy_state TEXT
    GENERATED ALWAYS AS (
        CASE WHEN cleanup_state IS NOT NULL THEN 'cleanup_' || cleanup_state ELSE state END
    ) VIRTUAL;

-- A migration can't tell a handed-off suspend from a restore rollback, so a
-- `mark_suspended` cleanup row becomes `stopping`; its `legacy_state`, which
-- every reader decides from, is unchanged.
UPDATE sandboxes
   SET cleanup_state = CASE state WHEN 'cleanup_required' THEN 'required' ELSE 'failed' END,
       state = CASE
           WHEN cleanup_completion IN ('delete_row', 'delete_pool_clone') THEN 'deleting'
           ELSE 'stopping'
       END
 WHERE state IN ('cleanup_required', 'cleanup_failed');

DROP TRIGGER validate_sandbox_lifecycle_insert;
DROP TRIGGER validate_sandbox_lifecycle_update;

CREATE TRIGGER validate_sandbox_lifecycle_insert
BEFORE INSERT ON sandboxes
WHEN NEW.state NOT IN (
      'creating', 'created', 'starting', 'started', 'stopping', 'stopped',
      'deleting', 'pausing', 'paused', 'resuming', 'checkpointing',
      'suspending', 'suspended', 'restoring', 'quarantined', 'failed'
  )
BEGIN
    SELECT RAISE(ABORT, 'invalid sandbox lifecycle state');
END;

CREATE TRIGGER validate_sandbox_lifecycle_update
BEFORE UPDATE OF state ON sandboxes
WHEN NEW.state NOT IN (
      'creating', 'created', 'starting', 'started', 'stopping', 'stopped',
      'deleting', 'pausing', 'paused', 'resuming', 'checkpointing',
      'suspending', 'suspended', 'restoring', 'quarantined', 'failed'
  )
BEGIN
    SELECT RAISE(ABORT, 'invalid sandbox lifecycle state');
END;

-- A cleanup-owned row is in a teardown transition: `deleting` exactly for a
-- row-deleting intent, `suspending` only for `mark_suspended`, and `stopping`
-- for any other intent or none. Each term is false, not NULL, when its column
-- is NULL.
CREATE TRIGGER validate_sandbox_cleanup_state_insert
BEFORE INSERT ON sandboxes
WHEN NEW.cleanup_state IS NOT NULL
 AND (
        NEW.state NOT IN ('stopping', 'deleting', 'suspending')
     OR (
            NEW.state = 'deleting'
            AND (
                   NEW.cleanup_completion IS NULL
                OR NEW.cleanup_completion NOT IN ('delete_row', 'delete_pool_clone')
            )
        )
     OR (
            NEW.state = 'suspending'
            AND (NEW.cleanup_completion IS NULL OR NEW.cleanup_completion <> 'mark_suspended')
        )
     OR (
            NEW.state = 'stopping'
            AND NEW.cleanup_completion IS NOT NULL
            AND NEW.cleanup_completion IN ('delete_row', 'delete_pool_clone')
        )
 )
BEGIN
    SELECT RAISE(ABORT, 'invalid sandbox cleanup state');
END;

CREATE TRIGGER validate_sandbox_cleanup_state_update
BEFORE UPDATE OF state, cleanup_state, cleanup_completion ON sandboxes
WHEN NEW.cleanup_state IS NOT NULL
 AND (
        NEW.state NOT IN ('stopping', 'deleting', 'suspending')
     OR (
            NEW.state = 'deleting'
            AND (
                   NEW.cleanup_completion IS NULL
                OR NEW.cleanup_completion NOT IN ('delete_row', 'delete_pool_clone')
            )
        )
     OR (
            NEW.state = 'suspending'
            AND (NEW.cleanup_completion IS NULL OR NEW.cleanup_completion <> 'mark_suspended')
        )
     OR (
            NEW.state = 'stopping'
            AND NEW.cleanup_completion IS NOT NULL
            AND NEW.cleanup_completion IN ('delete_row', 'delete_pool_clone')
        )
 )
BEGIN
    SELECT RAISE(ABORT, 'invalid sandbox cleanup state');
END;
