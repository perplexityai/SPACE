SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
GRANT SELECT (id, pod_name, pod_namespace, pod_uid) ON kubernetes_pool_slots TO space_credmgr;
