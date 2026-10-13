SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE snapshots DROP COLUMN recovery_grant_id;
DROP TABLE backup_recovery_grants;
