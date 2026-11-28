SET LOCAL lock_timeout = '200ms';
SET LOCAL statement_timeout = '5s';

-- Drop the foreign keys into `org_master_keys` from tables that later
-- migrations drop. No code reads or writes those tables, but deletes reach them
-- through foreign keys, so a session can hold a lock on one while it waits for
-- `org_master_keys`. Dropping a key takes ACCESS EXCLUSIVE on both tables. So
-- take `org_master_keys` first, waiting at most 200ms while its queries queue
-- behind this lock, and then the referencing tables with NOWAIT: this migration
-- never waits while it holds `org_master_keys`, so it can't deadlock, and any
-- wait is bounded by that one 200ms. A lock it can't get fails the migration
-- with a lock timeout, which the migrator retries. The statements then change
-- only the catalog, so `org_master_keys` stays locked briefly. The table drops
-- that follow lock nothing but the dropped table.
LOCK TABLE org_master_keys IN ACCESS EXCLUSIVE MODE;
LOCK TABLE sandbox_encrypted_fields IN ACCESS EXCLUSIVE MODE NOWAIT;
-- squawk-ignore ban-drop-constraint
ALTER TABLE sandbox_encrypted_fields DROP CONSTRAINT sandbox_encrypted_fields_master_key_id_org_id_fkey;
