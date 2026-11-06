SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Approval history is a rollback floor, even after its attempts are pruned.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM backup_recovery_grants) THEN
        RAISE EXCEPTION 'recovery approval history must be retained';
    END IF;
END $$;

ALTER TABLE snapshots DROP COLUMN recovery_grant_id;
DROP TABLE backup_recovery_grants;
