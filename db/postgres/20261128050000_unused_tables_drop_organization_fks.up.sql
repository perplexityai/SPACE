SET LOCAL lock_timeout = '200ms';
SET LOCAL statement_timeout = '5s';

-- Drop the foreign keys into `organizations` from tables that later migrations
-- drop. No code reads or writes those tables, but deletes reach them through
-- foreign keys, so a session can hold a lock on one while it waits for
-- `organizations`. Dropping a key takes ACCESS EXCLUSIVE on both tables. So
-- take `organizations` first, waiting at most 200ms while its queries queue
-- behind this lock, and then the referencing tables with NOWAIT: this migration
-- never waits while it holds `organizations`, so it can't deadlock, and any
-- wait is bounded by that one 200ms. A lock it can't get fails the migration
-- with a lock timeout, which the migrator retries. The statements then change
-- only the catalog, so `organizations` stays locked briefly. The table drops
-- that follow lock nothing but the dropped table.
LOCK TABLE organizations IN ACCESS EXCLUSIVE MODE;
LOCK TABLE org_credentials, sandbox_archives, user_credentials IN ACCESS EXCLUSIVE MODE NOWAIT;
-- squawk-ignore ban-drop-constraint
ALTER TABLE org_credentials DROP CONSTRAINT org_credentials_org_id_fkey;
-- squawk-ignore ban-drop-constraint
ALTER TABLE user_credentials DROP CONSTRAINT user_credentials_org_id_fkey;
-- squawk-ignore ban-drop-constraint
ALTER TABLE sandbox_archives DROP CONSTRAINT sandbox_archives_org_id_fkey;
