SET LOCAL lock_timeout = '200ms';
SET LOCAL statement_timeout = '5s';

-- Drop the foreign keys into `template_releases` from tables that later
-- migrations drop. No code reads or writes those tables, but deletes reach them
-- through foreign keys, so a session can hold a lock on one while it waits for
-- `template_releases`. Dropping a key takes ACCESS EXCLUSIVE on both tables. So
-- take `template_releases` first, waiting at most 200ms while its queries queue
-- behind this lock, and then the referencing tables with NOWAIT: this migration
-- never waits while it holds `template_releases`, so it can't deadlock, and any
-- wait is bounded by that one 200ms. A lock it can't get fails the migration
-- with a lock timeout, which the migrator retries. The statements then change
-- only the catalog, so `template_releases` stays locked briefly. The table
-- drops that follow lock nothing but the dropped table.
LOCK TABLE template_releases IN ACCESS EXCLUSIVE MODE;
LOCK TABLE sandbox_archives IN ACCESS EXCLUSIVE MODE NOWAIT;
-- squawk-ignore ban-drop-constraint
ALTER TABLE sandbox_archives DROP CONSTRAINT sandbox_archives_template_release_id_fkey;
