-- Do not erase the ownership of unresolved captures on rollback.
CREATE TEMP TABLE suspend_attempt_rollback_guard (empty INTEGER CHECK (empty = 0));
INSERT INTO suspend_attempt_rollback_guard
SELECT COUNT(*) FROM sandboxes WHERE suspend_attempt_artifact_id IS NOT NULL;
DROP TABLE suspend_attempt_rollback_guard;
DROP TRIGGER clear_finished_suspend_attempt;
ALTER TABLE sandboxes DROP COLUMN suspend_attempt_started_at;
ALTER TABLE sandboxes DROP COLUMN suspend_attempt_artifact_id;
