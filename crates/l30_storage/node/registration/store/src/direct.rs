//! Reads of `nodes` that join the node's Direct registration in
//! `node_direct`: the admin node row, its Kubernetes registration and its
//! `node_status_snapshots` row, by id, as a filtered listing, and
//! locked for an administrative transition.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::{NodeAddress, NodeGroupId, NodeManagedBy, NodeRole};
use pplx_space_sql_locator::sql;
use sqlx::{PgExecutor, Postgres, Transaction};
use time::OffsetDateTime;

/// One node as the admin API reads it: the `nodes` row, its `node_direct`
/// and `node_k8s` registrations, and its status snapshot, each joined row
/// optional. `status_services` and `status_node_ctl` are JSON the caller
/// decodes as `S` and `C`; a row whose JSON doesn't decode fails the read.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AdminNodeRow<S, C> {
    pub id: NodeId,
    pub name: Option<String>,
    pub address: Option<NodeAddress>,
    pub node_api_endpoint: Option<String>,
    pub role: NodeRole,
    pub managed_by: NodeManagedBy,
    pub state: Option<String>,
    pub scheduling_status: Option<String>,
    pub scheduling_reason: Option<String>,
    pub status_stale_after: Option<OffsetDateTime>,
    pub status_reachable: Option<bool>,
    pub status_error: Option<String>,
    pub status_role: Option<String>,
    pub status_node_id_reported: Option<String>,
    pub network_slots_ready: Option<i32>,
    pub network_pool_min_ready: Option<i32>,
    pub network_slots_degraded: Option<i32>,
    pub network_slots_quarantined: Option<i32>,
    pub network_slots_total: Option<i32>,
    pub network_slots_leased: Option<i32>,
    pub network_pool_max_slots: Option<i32>,
    pub drain_phase: Option<String>,
    /// Raw column value, not a decoded `NodeDrainMode`: a typed decode fails
    /// the whole row on a value this build does not recognize, which would turn
    /// one bad column into a 500 for the entire node listing. Callers resolve
    /// it through `NodeDrainMode::from_column_str`.
    pub drain_mode: Option<String>,
    pub drain_started_at: Option<OffsetDateTime>,
    pub drain_last_error: Option<String>,
    pub terminate_decrement_capacity: bool,
    pub node_group_id: Option<NodeGroupId>,
    pub metadata: Option<sqlx::types::Json<serde_json::Value>>,
    pub last_heartbeat: Option<OffsetDateTime>,
    pub status_observed_at: Option<OffsetDateTime>,
    // Decode diagnostics after reading the row: unknown policy schemas must
    // not prevent node inspection or the lock used by administrative actions.
    pub execution_policy: Option<sqlx::types::Json<serde_json::Value>>,
    pub execution_policy_received_at: Option<OffsetDateTime>,
    pub execution_inventory_status: Option<String>,
    pub execution_inventory_received_at: Option<OffsetDateTime>,
    pub status_health_healthy: Option<bool>,
    pub status_services: Option<sqlx::types::Json<S>>,
    pub status_node_ctl: Option<sqlx::types::Json<C>>,
    pub created_at: OffsetDateTime,
    pub updated_at: Option<OffsetDateTime>,
    pub deleted_at: Option<OffsetDateTime>,
    pub substrate_node_id: Option<String>,
}

/// The admin row of `node_id`; `None` when the `nodes` row is gone.
pub async fn admin_node<S, C>(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
) -> Result<Option<AdminNodeRow<S, C>>, sqlx::Error>
where
    AdminNodeRow<S, C>: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Unpin,
{
    sqlx::query_as::<_, AdminNodeRow<S, C>>(sql!(
        "SELECT n.id,
       nd.name,
       host(n.address) AS address,
       nd.node_api_endpoint,
       n.role,
       n.managed_by,
       n.state,
       nss.scheduling_status,
       nss.scheduling_reason,
       nss.stale_after AS status_stale_after,
       nss.reachable AS status_reachable,
       nss.error AS status_error,
       nss.role AS status_role,
       nss.node_id_reported AS status_node_id_reported,
       nss.network_slots_ready,
       nss.network_pool_min_ready,
       nss.network_slots_degraded,
       nss.network_slots_quarantined,
       nss.network_slots_total,
       nss.network_slots_leased,
       nss.network_pool_max_slots,
       n.drain_phase,
       n.drain_mode,
       n.drain_started_at,
       n.drain_last_error,
       n.terminate_decrement_capacity,
       n.node_group_id,
       nd.metadata,
       n.last_heartbeat,
       nss.observed_at AS status_observed_at,
       nss.execution_policy,
       nss.execution_policy_received_at,
       nss.execution_inventory_status,
       nss.execution_inventory_received_at,
       nss.health_healthy AS status_health_healthy,
       nss.services AS status_services,
       nss.node_ctl AS status_node_ctl,
       n.created_at,
       nd.updated_at,
       nd.deleted_at,
       nk.k8s_node_name AS substrate_node_id
FROM nodes n
LEFT JOIN node_direct nd ON nd.node_id = n.id
LEFT JOIN node_k8s nk ON nk.node_id = n.id
LEFT JOIN node_status_snapshots nss ON nss.node_id = n.id
WHERE n.id = $1"
    ))
    .bind(node_id)
    .fetch_optional(executor)
    .await
}

/// Every admin row matching all three filters, ordered by role, then by
/// name (the Direct name, else the Kubernetes node name), then by node API
/// endpoint, missing values last. `role` `None` matches every role;
/// `include_deleted` false skips nodes whose `node_direct` row is deleted;
/// `drain_phase` `None` matches every phase, `Some("")` only nodes with no
/// phase.
pub async fn list_admin_nodes<S, C>(
    executor: impl PgExecutor<'_>,
    role: Option<&str>,
    include_deleted: bool,
    drain_phase: Option<&str>,
) -> Result<Vec<AdminNodeRow<S, C>>, sqlx::Error>
where
    AdminNodeRow<S, C>: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Unpin,
{
    sqlx::query_as::<_, AdminNodeRow<S, C>>(sql!(
        "SELECT n.id,
       nd.name,
       host(n.address) AS address,
       nd.node_api_endpoint,
       n.role,
       n.managed_by,
       n.state,
       nss.scheduling_status,
       nss.scheduling_reason,
       nss.stale_after AS status_stale_after,
       nss.reachable AS status_reachable,
       nss.error AS status_error,
       nss.role AS status_role,
       nss.node_id_reported AS status_node_id_reported,
       nss.network_slots_ready,
       nss.network_pool_min_ready,
       nss.network_slots_degraded,
       nss.network_slots_quarantined,
       nss.network_slots_total,
       nss.network_slots_leased,
       nss.network_pool_max_slots,
       n.drain_phase,
       n.drain_mode,
       n.drain_started_at,
       n.drain_last_error,
       n.terminate_decrement_capacity,
       n.node_group_id,
       nd.metadata,
       n.last_heartbeat,
       nss.observed_at AS status_observed_at,
       nss.execution_policy,
       nss.execution_policy_received_at,
       nss.execution_inventory_status,
       nss.execution_inventory_received_at,
       nss.health_healthy AS status_health_healthy,
       nss.services AS status_services,
       nss.node_ctl AS status_node_ctl,
       n.created_at,
       nd.updated_at,
       nd.deleted_at,
       nk.k8s_node_name AS substrate_node_id
FROM nodes n
LEFT JOIN node_direct nd ON nd.node_id = n.id
LEFT JOIN node_k8s nk ON nk.node_id = n.id
LEFT JOIN node_status_snapshots nss ON nss.node_id = n.id
WHERE ($1::TEXT IS NULL OR n.role = $1)
  AND ($2::BOOL OR nd.deleted_at IS NULL)
  AND (
    $3::TEXT IS NULL
    OR ($3 = '' AND n.drain_phase IS NULL)
    OR n.drain_phase = $3
  )
ORDER BY n.role ASC,
         COALESCE(nd.name, nk.k8s_node_name) ASC NULLS LAST,
         nd.node_api_endpoint ASC NULLS LAST"
    ))
    .bind(role)
    .bind(include_deleted)
    .bind(drain_phase)
    .fetch_all(executor)
    .await
}

/// The admin row of `node_id`, its `nodes` row locked `FOR UPDATE`; `None`
/// when the row is gone. The lock lasts until the caller's transaction ends,
/// so the caller can validate a transition against the row and write it
/// before another transaction reads the row to do the same.
pub async fn lock_admin_node<S, C>(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
) -> Result<Option<AdminNodeRow<S, C>>, sqlx::Error>
where
    AdminNodeRow<S, C>: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Unpin,
{
    sqlx::query_as::<_, AdminNodeRow<S, C>>(sql!(
        "SELECT n.id,
       nd.name,
       host(n.address) AS address,
       nd.node_api_endpoint,
       n.role,
       n.managed_by,
       n.state,
       nss.scheduling_status,
       nss.scheduling_reason,
       nss.stale_after AS status_stale_after,
       nss.reachable AS status_reachable,
       nss.error AS status_error,
       nss.role AS status_role,
       nss.node_id_reported AS status_node_id_reported,
       nss.network_slots_ready,
       nss.network_pool_min_ready,
       nss.network_slots_degraded,
       nss.network_slots_quarantined,
       nss.network_slots_total,
       nss.network_slots_leased,
       nss.network_pool_max_slots,
       n.drain_phase,
       n.drain_mode,
       n.drain_started_at,
       n.drain_last_error,
       n.terminate_decrement_capacity,
       n.node_group_id,
       nd.metadata,
       n.last_heartbeat,
       nss.observed_at AS status_observed_at,
       nss.execution_policy,
       nss.execution_policy_received_at,
       nss.execution_inventory_status,
       nss.execution_inventory_received_at,
       nss.health_healthy AS status_health_healthy,
       nss.services AS status_services,
       nss.node_ctl AS status_node_ctl,
       n.created_at,
       nd.updated_at,
       nd.deleted_at,
       nk.k8s_node_name AS substrate_node_id
FROM nodes n
LEFT JOIN node_direct nd ON nd.node_id = n.id
LEFT JOIN node_k8s nk ON nk.node_id = n.id
LEFT JOIN node_status_snapshots nss ON nss.node_id = n.id
WHERE n.id = $1
FOR UPDATE OF n"
    ))
    .bind(node_id)
    .fetch_optional(&mut **tx)
    .await
}
