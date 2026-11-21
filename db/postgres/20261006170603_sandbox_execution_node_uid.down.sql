SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

REVOKE SELECT (k8s_node_uid) ON sandbox_k8s_executions FROM space_credmgr;

ALTER TABLE sandbox_k8s_executions
    DROP COLUMN k8s_node_uid;
