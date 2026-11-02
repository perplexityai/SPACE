-- Roll back the images that read and write the columns before removing them.
-- The recorded targets are lost; each event keeps its method and path.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE admin_audit_events
    DROP COLUMN target_org_id,
    DROP COLUMN target_type,
    DROP COLUMN target_id;
