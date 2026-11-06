SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE snapshots ADD COLUMN source_node_uid TEXT;
