-- A `refused` outcome can't be read by an older release. Refuse the rollback
-- while one is recorded.
CREATE TEMP TABLE runtime_upgrade_refusal_rollback_guard (count INTEGER CHECK (count = 0));
INSERT INTO runtime_upgrade_refusal_rollback_guard
    SELECT COUNT(*) FROM sandbox_runtime_upgrade_outcomes WHERE outcome = 'refused';
DROP TABLE runtime_upgrade_refusal_rollback_guard;
ALTER TABLE sandbox_runtime_upgrade_outcomes DROP COLUMN attempt_id;
ALTER TABLE sandbox_runtime_upgrades DROP COLUMN attempt_id;
