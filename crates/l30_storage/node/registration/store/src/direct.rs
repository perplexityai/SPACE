//! Statements on `nodes` that join or test the node's Direct registration in
//! `node_direct`. Reads of the admin node row, its Kubernetes registration
//! and its `node_status_snapshots` row, by id, as a filtered listing, and
//! locked for an administrative transition; and the two hard deletes of a
//! Direct node, by id ([`delete_direct_node`]) and of its expired tombstones
//! ([`prune_expired_direct_node_tombstones`]), whose `node_direct` rows go by
//! the schema's `ON DELETE CASCADE`. [`registry`] holds the admin Direct node
//! registry's statements on `nodes`, and the reads of Direct nodes' registry
//! rows. The other submodules hold the other reads of `nodes` joined to
//! `node_direct`: [`endpoints`] and [`placement_candidates`].

pub mod endpoints;
pub mod placement_candidates;
pub mod registry;

use pplx_space_accounts::NodeId;
use pplx_space_node_model::{NodeAddress, NodeGroupId, NodeManagedBy, NodeRole};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use serde::de::DeserializeOwned;
use sqlx::types::Json;
use time::OffsetDateTime;

/// One node as the admin API reads it: the `nodes` row, its `node_direct`
/// and `node_k8s` registrations, and its status snapshot, each joined row
/// optional. `status_services` and `status_node_ctl` are the snapshot's JSON
/// columns decoded as the caller's `S` and `C`; a row whose JSON doesn't
/// decode as them fails the read.
#[derive(Debug, Clone)]
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
    pub drain_engine: Option<String>,
    pub drain_status: Option<serde_json::Value>,
    pub terminate_decrement_capacity: bool,
    pub node_group_id: Option<NodeGroupId>,
    pub metadata: Option<serde_json::Value>,
    pub last_heartbeat: Option<OffsetDateTime>,
    pub status_observed_at: Option<OffsetDateTime>,
    // Decode diagnostics after reading the row: unknown policy schemas must
    // not prevent node inspection or the lock used by administrative actions.
    pub execution_policy: Option<serde_json::Value>,
    pub execution_policy_received_at: Option<OffsetDateTime>,
    pub execution_inventory_status: Option<String>,
    pub execution_inventory_received_at: Option<OffsetDateTime>,
    pub status_health_healthy: Option<bool>,
    pub status_services: Option<S>,
    pub status_node_ctl: Option<C>,
    pub created_at: OffsetDateTime,
    pub updated_at: Option<OffsetDateTime>,
    pub deleted_at: Option<OffsetDateTime>,
    pub substrate_node_id: Option<String>,
}

/// [`AdminNodeRow`] as the statements decode it, its JSON columns through
/// [`Json`].
#[derive(sqlx::FromRow)]
struct AdminNodeDecodeRow<S, C> {
    id: NodeId,
    name: Option<String>,
    address: Option<NodeAddress>,
    node_api_endpoint: Option<String>,
    role: NodeRole,
    managed_by: NodeManagedBy,
    state: Option<String>,
    scheduling_status: Option<String>,
    scheduling_reason: Option<String>,
    status_stale_after: Option<OffsetDateTime>,
    status_reachable: Option<bool>,
    status_error: Option<String>,
    status_role: Option<String>,
    status_node_id_reported: Option<String>,
    network_slots_ready: Option<i32>,
    network_pool_min_ready: Option<i32>,
    network_slots_degraded: Option<i32>,
    network_slots_quarantined: Option<i32>,
    network_slots_total: Option<i32>,
    network_slots_leased: Option<i32>,
    network_pool_max_slots: Option<i32>,
    drain_phase: Option<String>,
    drain_mode: Option<String>,
    drain_started_at: Option<OffsetDateTime>,
    drain_last_error: Option<String>,
    drain_engine: Option<String>,
    drain_status: Option<Json<serde_json::Value>>,
    terminate_decrement_capacity: bool,
    node_group_id: Option<NodeGroupId>,
    metadata: Option<Json<serde_json::Value>>,
    last_heartbeat: Option<OffsetDateTime>,
    status_observed_at: Option<OffsetDateTime>,
    execution_policy: Option<Json<serde_json::Value>>,
    execution_policy_received_at: Option<OffsetDateTime>,
    execution_inventory_status: Option<String>,
    execution_inventory_received_at: Option<OffsetDateTime>,
    status_health_healthy: Option<bool>,
    status_services: Option<Json<S>>,
    status_node_ctl: Option<Json<C>>,
    created_at: OffsetDateTime,
    updated_at: Option<OffsetDateTime>,
    deleted_at: Option<OffsetDateTime>,
    substrate_node_id: Option<String>,
}

impl<S, C> From<AdminNodeDecodeRow<S, C>> for AdminNodeRow<S, C> {
    fn from(row: AdminNodeDecodeRow<S, C>) -> Self {
        Self {
            id: row.id,
            name: row.name,
            address: row.address,
            node_api_endpoint: row.node_api_endpoint,
            role: row.role,
            managed_by: row.managed_by,
            state: row.state,
            scheduling_status: row.scheduling_status,
            scheduling_reason: row.scheduling_reason,
            status_stale_after: row.status_stale_after,
            status_reachable: row.status_reachable,
            status_error: row.status_error,
            status_role: row.status_role,
            status_node_id_reported: row.status_node_id_reported,
            network_slots_ready: row.network_slots_ready,
            network_pool_min_ready: row.network_pool_min_ready,
            network_slots_degraded: row.network_slots_degraded,
            network_slots_quarantined: row.network_slots_quarantined,
            network_slots_total: row.network_slots_total,
            network_slots_leased: row.network_slots_leased,
            network_pool_max_slots: row.network_pool_max_slots,
            drain_phase: row.drain_phase,
            drain_mode: row.drain_mode,
            drain_started_at: row.drain_started_at,
            drain_last_error: row.drain_last_error,
            drain_engine: row.drain_engine,
            drain_status: row.drain_status.map(|json| json.0),
            terminate_decrement_capacity: row.terminate_decrement_capacity,
            node_group_id: row.node_group_id,
            metadata: row.metadata.map(|json| json.0),
            last_heartbeat: row.last_heartbeat,
            status_observed_at: row.status_observed_at,
            execution_policy: row.execution_policy.map(|json| json.0),
            execution_policy_received_at: row.execution_policy_received_at,
            execution_inventory_status: row.execution_inventory_status,
            execution_inventory_received_at: row.execution_inventory_received_at,
            status_health_healthy: row.status_health_healthy,
            status_services: row.status_services.map(|json| json.0),
            status_node_ctl: row.status_node_ctl.map(|json| json.0),
            created_at: row.created_at,
            updated_at: row.updated_at,
            deleted_at: row.deleted_at,
            substrate_node_id: row.substrate_node_id,
        }
    }
}

/// The admin row of `node_id`; `None` when the `nodes` row is gone.
pub async fn admin_node<S, C>(
    executor: impl Executor<'_>,
    node_id: NodeId,
) -> Result<Option<AdminNodeRow<S, C>>, DbError>
where
    S: DeserializeOwned + Send + Unpin + 'static,
    C: DeserializeOwned + Send + Unpin + 'static,
{
    Ok(sqlx::query_as::<_, AdminNodeDecodeRow<S, C>>(sql!(
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
       n.drain_engine,
       n.drain_status,
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
    .fetch_optional(executor.into_sqlx())
    .await?
    .map(AdminNodeRow::from))
}

/// Every admin row matching all three filters, ordered by role, then by
/// name (the Direct name, else the Kubernetes node name), then by node API
/// endpoint, missing values last. `role` `None` matches every role;
/// `include_deleted` false skips nodes whose `node_direct` row is deleted;
/// `drain_phase` `None` matches every phase, `Some("")` only nodes with no
/// phase.
pub async fn list_admin_nodes<S, C>(
    executor: impl Executor<'_>,
    role: Option<&str>,
    include_deleted: bool,
    drain_phase: Option<&str>,
) -> Result<Vec<AdminNodeRow<S, C>>, DbError>
where
    S: DeserializeOwned + Send + Unpin + 'static,
    C: DeserializeOwned + Send + Unpin + 'static,
{
    Ok(sqlx::query_as::<_, AdminNodeDecodeRow<S, C>>(sql!(
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
       n.drain_engine,
       n.drain_status,
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
    .fetch_all(executor.into_sqlx())
    .await?
    .into_iter()
    .map(AdminNodeRow::from)
    .collect())
}

/// The admin row of `node_id`, its `nodes` row locked `FOR UPDATE`; `None`
/// when the row is gone. The lock lasts until the caller's transaction ends,
/// so the caller can validate a transition against the row and write it
/// before another transaction reads the row to do the same. The caller must
/// pass its open transaction.
pub async fn lock_admin_node<S, C, T>(
    tx: &mut T,
    node_id: NodeId,
) -> Result<Option<AdminNodeRow<S, C>>, DbError>
where
    for<'a> &'a mut T: Executor<'a>,
    S: DeserializeOwned + Send + Unpin + 'static,
    C: DeserializeOwned + Send + Unpin + 'static,
{
    Ok(sqlx::query_as::<_, AdminNodeDecodeRow<S, C>>(sql!(
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
       n.drain_engine,
       n.drain_status,
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
    .fetch_optional(tx.into_sqlx())
    .await?
    .map(AdminNodeRow::from))
}

/// Hard-delete the `nodes` row of `node_id` when it is managed by
/// `managed_by` and has a `node_direct` registration, returning the number of
/// rows deleted: 0 when no such node exists. Rows whose foreign key to
/// `nodes` cascades, `node_direct` among them, go with it.
pub async fn delete_direct_node(
    executor: impl Executor<'_>,
    node_id: NodeId,
    managed_by: NodeManagedBy,
) -> Result<u64, DbError> {
    let result = sqlx::query(sql!(
        "DELETE FROM nodes n
         WHERE n.id = $1
           AND n.managed_by = $2
           AND EXISTS (
               SELECT 1
               FROM node_direct nd
               WHERE nd.node_id = n.id
           )"
    ))
    .bind(node_id)
    .bind(managed_by)
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected())
}

/// Hard-delete up to `max_records` Direct node tombstones managed by
/// `managed_by` whose `node_direct` row was deleted more than
/// `retention_secs` seconds before the database's `NOW()`, oldest first,
/// returning how many it deleted: 0 when none has expired. A node that a
/// non-terminal sandbox still names as its active or target node stays.
/// `NOW()` is the start of the executor's transaction.
pub async fn prune_expired_direct_node_tombstones(
    executor: impl Executor<'_>,
    retention_secs: i64,
    max_records: i64,
    managed_by: NodeManagedBy,
) -> Result<u64, DbError> {
    // Quarantined sandboxes are not terminal for tombstone retention:
    // they intentionally keep the node association until quarantine-aware
    // cleanup or operator release resolves the preserved state.
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "WITH candidates AS (
             SELECT n.id
               FROM nodes n
               JOIN node_direct nd ON nd.node_id = n.id
              WHERE n.managed_by = $3
                AND COALESCE(n.state, 'registered') = 'deleted'
                AND nd.deleted_at IS NOT NULL
                AND nd.deleted_at < NOW() - ($1::BIGINT * INTERVAL '1 second')
                AND NOT EXISTS (
                    SELECT 1
                      FROM sandboxes s
                     WHERE s.active_node_id = n.id
                       AND s.lifecycle NOT IN ('deleted', 'failed')
                )
                AND NOT EXISTS (
                    SELECT 1
                      FROM sandboxes s
                     WHERE s.target_node_id = n.id
                       AND s.lifecycle NOT IN ('deleted', 'failed')
                )
              ORDER BY nd.deleted_at ASC, n.id ASC
              LIMIT $2
         )
         DELETE FROM nodes n
          USING candidates c
          WHERE n.id = c.id
          RETURNING n.id::TEXT"
    ))
    .bind(retention_secs)
    .bind(max_records)
    .bind(managed_by)
    .fetch_all(executor.into_sqlx())
    .await?
    .len() as u64)
}

#[cfg(test)]
mod tests;
