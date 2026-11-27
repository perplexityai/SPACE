-- Admission identity and its immutable clock survive caller/process loss.
-- Existing rows remain NULL: directory age cannot identify a legacy attempt.
ALTER TABLE sandboxes ADD COLUMN suspend_attempt_artifact_id TEXT;
ALTER TABLE sandboxes ADD COLUMN suspend_attempt_started_at INTEGER;

-- Stopping/deleting and cleanup-owned rows can still overlap physical capture.
-- Only a settled, unowned lifecycle releases the admission identity.
CREATE TRIGGER clear_finished_suspend_attempt
AFTER UPDATE OF state ON sandboxes
WHEN NEW.unowned_state IN ('started', 'paused', 'stopped', 'suspended', 'failed')
 AND NEW.suspend_attempt_artifact_id IS NOT NULL
BEGIN
    UPDATE sandboxes SET suspend_attempt_artifact_id = NULL,
                         suspend_attempt_started_at = NULL WHERE id = NEW.id;
END;
