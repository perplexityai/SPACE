SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
REVOKE SELECT (reserved_sandbox_id, k8s_node_name, state) ON kubernetes_pool_slots FROM space_credmgr;
