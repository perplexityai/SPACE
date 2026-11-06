SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5min';

ALTER TABLE snapshots VALIDATE CONSTRAINT snapshots_recovery_grant_id_fkey;
