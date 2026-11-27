//! The node-api endpoint of a live Direct node: a `nodes` row joined to its
//! `node_direct` row, whose `deleted_at` decides whether it is live, by id
//! and role or among the nodes whose `node_status_snapshots` row
//! (`pplx-space-node-status-store`'s) is fresh and ready. The readiness read
//! compares against `NOW()`, the database clock, so its caller keeps the
//! executor it ran it on. [`node_api_target`] reads a registered node of any
//! role, with or without a live endpoint.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::{NodeRole, SubstrateKind};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use sqlx::FromRow;

/// A Direct node and the node-api endpoint its `node_direct` row records.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct NodeEndpoint {
    pub id: NodeId,
    pub node_api_endpoint: String,
}

/// The live, registered Direct nodes of `role` under `managed_by` whose
/// snapshot is reachable, fresh (`stale_after` after `NOW()`) and `ready`, in
/// random order. Empty when there are none.
pub async fn random_ready(
    executor: impl Executor<'_>,
    role: NodeRole,
    managed_by: SubstrateKind,
) -> Result<Vec<NodeEndpoint>, DbError> {
    Ok(sqlx::query_as::<_, NodeEndpoint>(sql!(
        "SELECT n.id, nd.node_api_endpoint
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             WHERE n.managed_by = $2
               AND n.role = $1
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
             ORDER BY random()"
    ))
    .bind(role.as_str())
    .bind(managed_by)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// Worker `node_id` under `managed_by`, when its `node_direct` row is live.
/// Its `nodes.state` and snapshot are not read.
pub async fn live_worker(
    executor: impl Executor<'_>,
    node_id: NodeId,
    managed_by: SubstrateKind,
) -> Result<Option<NodeEndpoint>, DbError> {
    Ok(sqlx::query_as::<_, NodeEndpoint>(sql!(
        "SELECT n.id, nd.node_api_endpoint
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             WHERE n.id = $1
               AND n.managed_by = $2
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
             LIMIT 1"
    ))
    .bind(node_id)
    .bind(managed_by)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Builder `node_id` under `managed_by`, when its `node_direct` row is live.
/// Its `nodes.state` and snapshot are not read.
pub async fn live_builder(
    executor: impl Executor<'_>,
    node_id: NodeId,
    managed_by: SubstrateKind,
) -> Result<Option<NodeEndpoint>, DbError> {
    Ok(sqlx::query_as::<_, NodeEndpoint>(sql!(
        "SELECT n.id, nd.node_api_endpoint
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             WHERE n.id = $1
               AND n.managed_by = $2
               AND n.role = 'builder'
               AND nd.deleted_at IS NULL
             LIMIT 1"
    ))
    .bind(node_id.as_uuid())
    .bind(managed_by)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// A registered Direct node's role and the node-api endpoint its live
/// `node_direct` row records: `None` when that row is deleted or missing.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct RegisteredNodeEndpoint {
    pub role: NodeRole,
    pub node_api_endpoint: Option<String>,
}

/// Node `node_id` under `managed_by`, of any role. `None` when there's no
/// such node or its `nodes` row is soft-deleted (`state = 'deleted'`); a node
/// that's registered without a live `node_direct` row is `Some` with no
/// endpoint. Its snapshot is not read.
pub async fn node_api_target(
    executor: impl Executor<'_>,
    node_id: NodeId,
    managed_by: SubstrateKind,
) -> Result<Option<RegisteredNodeEndpoint>, DbError> {
    Ok(sqlx::query_as::<_, RegisteredNodeEndpoint>(sql!(
        "SELECT n.role, nd.node_api_endpoint
           FROM nodes n
           LEFT JOIN node_direct nd ON nd.node_id = n.id AND nd.deleted_at IS NULL
          WHERE n.id = $1
            AND n.managed_by = $2
            AND COALESCE(n.state, 'registered') != 'deleted'"
    ))
    .bind(node_id)
    .bind(managed_by)
    .fetch_optional(executor.into_sqlx())
    .await?)
}
