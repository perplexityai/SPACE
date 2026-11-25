-- Dropping the columns makes every final attempt outcome final for no
-- attempt, so a retry of an attempt its caller already gave up would run
-- again. Refuse the rollback while any final attempt outcome is recorded.
CREATE TEMP TABLE runtime_upgrade_attempt_identity_rollback_guard (count INTEGER CHECK (count = 0));
INSERT INTO runtime_upgrade_attempt_identity_rollback_guard
    SELECT COUNT(*) FROM sandbox_runtime_upgrade_attempt_outcomes
     WHERE target_environment IS NOT NULL;
DROP TABLE runtime_upgrade_attempt_identity_rollback_guard;
ALTER TABLE sandbox_runtime_upgrade_attempt_outcomes DROP COLUMN authorization_id;
ALTER TABLE sandbox_runtime_upgrade_attempt_outcomes DROP COLUMN target_state;
ALTER TABLE sandbox_runtime_upgrade_attempt_outcomes DROP COLUMN target_environment;
