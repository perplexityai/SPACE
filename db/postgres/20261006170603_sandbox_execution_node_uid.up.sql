-- Bind each active execution to the exact Kubernetes node incarnation that
-- materialized its Pod. A nullable column makes the rollout fail closed until
-- placement reconciliation observes and persists the Pod's node UID.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE sandbox_k8s_executions
    ADD COLUMN k8s_node_uid TEXT;

GRANT SELECT (k8s_node_uid) ON sandbox_k8s_executions TO space_credmgr;
