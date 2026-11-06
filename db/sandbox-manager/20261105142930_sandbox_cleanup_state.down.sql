DROP TRIGGER validate_sandbox_cleanup_state_update;
DROP TRIGGER validate_sandbox_cleanup_state_insert;
DROP TRIGGER validate_sandbox_lifecycle_update;
DROP TRIGGER validate_sandbox_lifecycle_insert;

-- The baseline validators accept the combined labels; restore them before
-- writing those labels back into `state`.
CREATE TRIGGER validate_sandbox_lifecycle_insert
BEFORE INSERT ON sandboxes
WHEN NEW.state NOT IN (
      'creating', 'created', 'starting', 'started', 'stopping', 'stopped',
      'deleting', 'pausing', 'paused', 'resuming', 'checkpointing',
      'suspending', 'suspended', 'restoring', 'cleanup_required',
      'cleanup_failed', 'quarantined', 'failed'
  )
BEGIN
    SELECT RAISE(ABORT, 'invalid sandbox lifecycle state');
END;

CREATE TRIGGER validate_sandbox_lifecycle_update
BEFORE UPDATE OF state ON sandboxes
WHEN NEW.state NOT IN (
      'creating', 'created', 'starting', 'started', 'stopping', 'stopped',
      'deleting', 'pausing', 'paused', 'resuming', 'checkpointing',
      'suspending', 'suspended', 'restoring', 'cleanup_required',
      'cleanup_failed', 'quarantined', 'failed'
  )
BEGIN
    SELECT RAISE(ABORT, 'invalid sandbox lifecycle state');
END;

UPDATE sandboxes SET state = legacy_state WHERE cleanup_state IS NOT NULL;

ALTER TABLE sandboxes DROP COLUMN legacy_state;
ALTER TABLE sandboxes DROP COLUMN cleanup_state;
