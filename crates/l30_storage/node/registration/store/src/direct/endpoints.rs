//! The node-api endpoint of a live Direct node: a `nodes` row joined to its
//! `node_direct` row, whose `deleted_at` decides whether it is live, by id
//! and role or among the nodes whose `node_status_snapshots` row
//! (`pplx-space-node-status-store`'s) is fresh and ready. The readiness read
//! compares against `NOW()`, the database clock, so its caller keeps the
//! executor it ran it on.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::{NodeRole, SubstrateKind};
use pplx_space_sql_locator::sql;
use sqlx::{FromRow, PgExecutor};

/// A Direct node and the node-api endpoint its `node_direct` row records.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct NodeEndpoint {
    pub id: NodeId,
    pub node_api_endpoint: String,
}

/// The live, registered Direct nodes of `role` under `managed_by` whose
/// snapshot is reachable, fresh (`stale_after` after `NOW()`) and `ready`, in
/// random order. Empty when there are none.
pub async fn random_ready<'e>(
    executor: impl PgExecutor<'e>,
    role: NodeRole,
    managed_by: SubstrateKind,
) -> Result<Vec<NodeEndpoint>, sqlx::Error> {
    sqlx::query_as::<_, NodeEndpoint>(sql!(
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
    .fetch_all(executor)
    .await
}

/// Worker `node_id` under `managed_by`, when its `node_direct` row is live.
/// Its `nodes.state` and snapshot are not read.
pub async fn live_worker<'e>(
    executor: impl PgExecutor<'e>,
    node_id: NodeId,
    managed_by: SubstrateKind,
) -> Result<Option<NodeEndpoint>, sqlx::Error> {
    sqlx::query_as::<_, NodeEndpoint>(sql!(
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
    .fetch_optional(executor)
    .await
}

/// Builder `node_id` under `managed_by`, when its `node_direct` row is live.
/// Its `nodes.state` and snapshot are not read.
pub async fn live_builder<'e>(
    executor: impl PgExecutor<'e>,
    node_id: NodeId,
    managed_by: SubstrateKind,
) -> Result<Option<NodeEndpoint>, sqlx::Error> {
    sqlx::query_as::<_, NodeEndpoint>(sql!(
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
    .fetch_optional(executor)
    .await
}
