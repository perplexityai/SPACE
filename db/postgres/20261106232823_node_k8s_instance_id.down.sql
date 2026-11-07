-- Roll back the writers of node_k8s.instance_id before this migration. The
-- recorded instances and their history are dropped; node sync records the
-- current instance again once the migration is re-applied, but the history
-- of superseded instances is not rebuilt.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

DROP TABLE node_k8s_superseded_instances;

ALTER TABLE node_k8s
    DROP COLUMN instance_id;
