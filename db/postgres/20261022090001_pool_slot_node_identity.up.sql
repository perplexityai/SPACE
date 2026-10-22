SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE kubernetes_pool_slots ADD COLUMN k8s_node_uid TEXT;
