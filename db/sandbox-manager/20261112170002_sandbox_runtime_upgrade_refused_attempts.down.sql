-- An older release doesn't read these refusals, so it would let a refused
-- attempt's retry activate the sandbox. Refuse the rollback while one is
-- recorded.
CREATE TEMP TABLE runtime_upgrade_refused_attempt_rollback_guard (count INTEGER CHECK (count = 0));
INSERT INTO runtime_upgrade_refused_attempt_rollback_guard
    SELECT COUNT(*) FROM sandbox_runtime_upgrade_refused_attempts;
DROP TABLE runtime_upgrade_refused_attempt_rollback_guard;
DROP INDEX sandbox_runtime_upgrade_refused_attempts_by_sandbox;
DROP TABLE sandbox_runtime_upgrade_refused_attempts;
