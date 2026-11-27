-- A release without this migration parses only the `rolled_back` and
-- `already_on_target` outcomes in `sandbox_runtime_upgrade_outcomes`. Refuse
-- the rollback while an operation's outcome is anything else.
CREATE TEMP TABLE runtime_upgrade_outcome_rollback_guard (count INTEGER CHECK (count = 0));
INSERT INTO runtime_upgrade_outcome_rollback_guard
    SELECT COUNT(*) FROM sandbox_runtime_upgrade_outcomes
     WHERE outcome NOT IN ('rolled_back', 'already_on_target');
DROP TABLE runtime_upgrade_outcome_rollback_guard;
ALTER TABLE sandbox_runtime_upgrade_outcomes DROP COLUMN attempt_id;
ALTER TABLE sandbox_runtime_upgrades DROP COLUMN attempt_id;
