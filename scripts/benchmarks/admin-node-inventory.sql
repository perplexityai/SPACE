-- Node metadata only: exact projection/order; default list filters simplified.
-- Uses a 10s query budget; this does not run occupancy accounting.
\set ON_ERROR_STOP on
\pset pager off
BEGIN READ ONLY;
SET LOCAL statement_timeout = '10s';
SET LOCAL lock_timeout = '1s';
\set ON_ERROR_STOP off
EXPLAIN (ANALYZE, BUFFERS, SETTINGS)
SELECT n.id, nd.name, host(n.address) AS address,
       nd.node_api_endpoint, n.role, n.managed_by,
       n.state, nss.scheduling_status, nss.scheduling_reason,
       nss.stale_after AS status_stale_after, nss.reachable AS status_reachable, nss.error AS status_error,
       nss.role AS status_role, nss.node_id_reported AS status_node_id_reported, nss.network_slots_ready,
       nss.network_pool_min_ready, nss.network_slots_degraded, nss.network_slots_quarantined,
       nss.network_slots_total, nss.network_slots_leased, nss.network_pool_max_slots,
       n.drain_phase, n.drain_mode, n.drain_started_at,
       n.drain_last_error, n.terminate_decrement_capacity, nd.metadata,
       n.last_heartbeat, nss.observed_at AS status_observed_at,
       nss.execution_inventory_status, nss.execution_inventory_received_at,
       nss.health_healthy AS status_health_healthy,
       nss.services AS status_services, nss.node_ctl AS status_node_ctl, n.created_at,
       nd.updated_at, nd.deleted_at, nk.k8s_node_name AS substrate_node_id
FROM nodes n
LEFT JOIN node_direct nd ON nd.node_id = n.id
LEFT JOIN node_k8s nk ON nk.node_id = n.id
LEFT JOIN node_status_snapshots nss ON nss.node_id = n.id
WHERE nd.deleted_at IS NULL
ORDER BY n.role ASC, COALESCE(nd.name, nk.k8s_node_name) ASC NULLS LAST,
         nd.node_api_endpoint ASC NULLS LAST;
ROLLBACK;
\set ON_ERROR_STOP on
