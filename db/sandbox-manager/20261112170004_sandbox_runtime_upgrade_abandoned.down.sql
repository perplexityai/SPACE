-- An older release doesn't know an operation was abandoned, so it would let
-- a late call for it change a disk the control plane has already captured.
-- Refuse the rollback while one is recorded.
CREATE TEMP TABLE runtime_upgrade_abandoned_rollback_guard (count INTEGER CHECK (count = 0));
INSERT INTO runtime_upgrade_abandoned_rollback_guard
    SELECT COUNT(*) FROM sandbox_runtime_upgrade_abandoned;
DROP TABLE runtime_upgrade_abandoned_rollback_guard;
DROP TABLE sandbox_runtime_upgrade_abandoned;
