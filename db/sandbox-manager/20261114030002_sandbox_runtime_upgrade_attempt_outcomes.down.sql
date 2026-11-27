-- Dropping the table loses every attempt outcome it holds, and nothing else
-- keeps an earlier attempt's: `sandbox_runtime_upgrade_outcomes` holds one
-- outcome per operation. Refuse the rollback while any is recorded.
CREATE TEMP TABLE runtime_upgrade_attempt_outcome_rollback_guard (count INTEGER CHECK (count = 0));
INSERT INTO runtime_upgrade_attempt_outcome_rollback_guard
    SELECT COUNT(*) FROM sandbox_runtime_upgrade_attempt_outcomes;
DROP TABLE runtime_upgrade_attempt_outcome_rollback_guard;
DROP INDEX sandbox_runtime_upgrade_attempt_outcomes_by_sandbox;
DROP TABLE sandbox_runtime_upgrade_attempt_outcomes;
