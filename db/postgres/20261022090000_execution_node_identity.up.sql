SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE sandbox_k8s_executions ADD COLUMN k8s_node_uid TEXT;
