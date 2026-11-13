-- An older release reads only the operation's latest outcome, so it would
-- let a delayed retry of an earlier rolled-back or refused attempt fence the
-- sandbox again. Refuse the rollback while one is recorded.
CREATE TEMP TABLE runtime_upgrade_attempt_outcome_rollback_guard (count INTEGER CHECK (count = 0));
INSERT INTO runtime_upgrade_attempt_outcome_rollback_guard
    SELECT COUNT(*) FROM sandbox_runtime_upgrade_attempt_outcomes;
DROP TABLE runtime_upgrade_attempt_outcome_rollback_guard;
DROP INDEX sandbox_runtime_upgrade_attempt_outcomes_by_sandbox;
DROP TABLE sandbox_runtime_upgrade_attempt_outcomes;
