SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Roll back readers and writers before dropping this column. Reapplying the
-- up migration initializes empty selections; it cannot recover prior selections.
ALTER TABLE sandboxes DROP COLUMN IF EXISTS egress_profiles;
