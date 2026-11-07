SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The EC2 instance of the Kubernetes Node an execution's pod is bound to, from
-- that Node's AWS providerID, as node_k8s.instance_id names it for the node. A
-- same-IP replacement reuses the node's name and id, so the node alone can't
-- tell the executions that ran on a lost instance from those running on its
-- replacement.
--
-- The Kubernetes substrate's execution store writes it in the statements that
-- set or clear k8s_node_name, and clears it with the pod UID when an execution
-- is claimed for a new pod. For one pod it is set once: a write that names the
-- same node without an instance leaves it alone, and one that names another
-- instance is refused. A write that names another node replaces it. NULL means
-- no instance is known: Kind and other providers have no AWS providerID, the
-- Node wasn't known to the writer, the execution has no placed pod, or the row
-- predates the writers. Nothing backfills it.
--
-- Adding a nullable column without a default changes the catalog only; no row
-- is rewritten.
ALTER TABLE sandbox_k8s_executions
    ADD COLUMN node_instance_id TEXT;

COMMENT ON COLUMN sandbox_k8s_executions.node_instance_id IS
    'EC2 instance ID of the Kubernetes Node the execution''s pod is bound to; NULL means none known';
