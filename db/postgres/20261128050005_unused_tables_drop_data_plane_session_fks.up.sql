SET LOCAL lock_timeout = '200ms';
SET LOCAL statement_timeout = '5s';

-- Drop the foreign keys into `data_plane_sessions` from tables that later
-- migrations drop. No code reads or writes those tables, but deletes reach them
-- through foreign keys, so a session can hold a lock on one while it waits for
-- `data_plane_sessions`. Dropping a key takes ACCESS EXCLUSIVE on both tables.
-- So take `data_plane_sessions` first, waiting at most 200ms while its queries
-- queue behind this lock, and then the referencing tables with NOWAIT: this
-- migration never waits while it holds `data_plane_sessions`, so it can't
-- deadlock, and any wait is bounded by that one 200ms. A lock it can't get
-- fails the migration with a lock timeout, which the migrator retries. The
-- statements then change only the catalog, so `data_plane_sessions` stays
-- locked briefly. The table drops that follow lock nothing but the dropped
-- table.
LOCK TABLE data_plane_sessions IN ACCESS EXCLUSIVE MODE;
LOCK TABLE data_plane_session_output_events IN ACCESS EXCLUSIVE MODE NOWAIT;
-- squawk-ignore ban-drop-constraint
ALTER TABLE data_plane_session_output_events DROP CONSTRAINT data_plane_session_output_events_session_id_fkey;
