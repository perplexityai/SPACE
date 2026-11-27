//! Reads that resolve a node through its registration: a Kubernetes node
//! name to its `nodes` rows, a node to its Kubernetes node name and UID or its
//! node-api target, and an address to the live nodes registered at it.

use std::net::IpAddr;

use pplx_space_accounts::NodeId;
use pplx_space_node_model::{NodeManagedBy, NodeRole, SubstrateKind};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;

/// The Kubernetes node name registered for `node_id`.
pub async fn k8s_node_name(
    executor: impl Executor<'_>,
    node_id: NodeId,
) -> Result<Option<String>, DbError> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT k8s_node_name FROM node_k8s WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// A node's registered Kubernetes node name and UID.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct K8sNodeIdentity {
    pub k8s_node_name: String,
    pub k8s_node_uid: Option<String>,
}

/// The Kubernetes node name and UID registered for `node_id`, read in one
/// statement so the UID is the identity of that name.
pub async fn k8s_node_identity(
    executor: impl Executor<'_>,
    node_id: NodeId,
) -> Result<Option<K8sNodeIdentity>, DbError> {
    Ok(sqlx::query_as::<_, K8sNodeIdentity>(sql!(
        "SELECT k8s_node_name, k8s_node_uid FROM node_k8s WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// What reaching a node's node-api needs from its `nodes` row and its
/// `node_k8s` sibling. `substrate_node_id` is the Kubernetes node name, `None`
/// without a registration.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct NodeApiTargetRow {
    pub role: NodeRole,
    pub managed_by: NodeManagedBy,
    pub substrate_node_id: Option<String>,
}

/// The node-api target columns of `node_id`; `None` when the `nodes` row is
/// gone.
pub async fn node_api_target(
    executor: impl Executor<'_>,
    node_id: NodeId,
) -> Result<Option<NodeApiTargetRow>, DbError> {
    Ok(sqlx::query_as::<_, NodeApiTargetRow>(sql!(
        "SELECT n.role,
                n.managed_by,
                nk.k8s_node_name AS substrate_node_id
           FROM nodes n
           LEFT JOIN node_k8s nk ON nk.node_id = n.id
          WHERE n.id = $1"
    ))
    .bind(node_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The Kubernetes node name of `node_id` when it is a registered
/// Kubernetes builder.
pub async fn builder_k8s_node_name(
    executor: impl Executor<'_>,
    node_id: NodeId,
) -> Result<Option<String>, DbError> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT nk.k8s_node_name
             FROM nodes n
             JOIN node_k8s nk ON nk.node_id = n.id
             WHERE n.id = $1
               AND n.managed_by = $2
               AND n.role = 'builder'
             LIMIT 1"
    ))
    .bind(node_id.as_uuid())
    .bind(SubstrateKind::Kubernetes)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The worker registered under a Kubernetes node name. Node ids are scoped by
/// role, so a builder on the same host has its own row under that name.
pub async fn worker_for_k8s_node_name(
    executor: impl Executor<'_>,
    node_name: &str,
) -> Result<Option<NodeId>, DbError> {
    Ok(sqlx::query_scalar::<_, NodeId>(sql!(
        "SELECT nk.node_id
         FROM node_k8s nk
         JOIN nodes n ON n.id = nk.node_id
         WHERE nk.k8s_node_name = $1
           AND n.role = 'worker'"
    ))
    .bind(node_name)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Every live Direct or Kubernetes node registered at `address`.
pub async fn live_nodes_at_address(
    executor: impl Executor<'_>,
    address: IpAddr,
) -> Result<Vec<NodeId>, DbError> {
    Ok(sqlx::query_scalar::<_, NodeId>(sql!(
        "SELECT id
             FROM nodes
             WHERE address = $1::INET
               AND managed_by IN ($2, $3)
               AND COALESCE(state, 'registered') != 'deleted'"
    ))
    .bind(address.to_string())
    .bind(SubstrateKind::Space)
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// A live registered node's id, address and role.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct LiveNode {
    pub id: NodeId,
    /// `host(address)`; `None` without a registered address.
    pub address: Option<String>,
    pub role: String,
}

/// The live Kubernetes node in `role` registered under a Kubernetes node name.
/// When several rows match, the most recently re-registered wins.
pub async fn live_k8s_node_by_name(
    executor: impl Executor<'_>,
    k8s_node_name: &str,
    role: NodeRole,
) -> Result<Option<LiveNode>, DbError> {
    Ok(sqlx::query_as::<_, LiveNode>(sql!(
        "SELECT n.id,
                    host(n.address) AS address,
                    n.role
             FROM nodes n
             JOIN node_k8s nk ON nk.node_id = n.id
             WHERE nk.k8s_node_name = $1
               AND n.managed_by = $3
               AND n.role = $2
               AND COALESCE(n.state, 'registered') != 'deleted'
             ORDER BY nk.updated_at DESC, n.created_at DESC
             LIMIT 1"
    ))
    .bind(k8s_node_name)
    .bind(role.as_str())
    .bind(SubstrateKind::Kubernetes)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// `node_id` when it is a live Direct or Kubernetes node, in `role` when one
/// is given.
pub async fn live_node(
    executor: impl Executor<'_>,
    node_id: NodeId,
    role: Option<NodeRole>,
) -> Result<Option<LiveNode>, DbError> {
    Ok(sqlx::query_as::<_, LiveNode>(sql!(
        "SELECT n.id,
                host(n.address) AS address,
                n.role
         FROM nodes n
         WHERE n.id = $1
           AND ($2::TEXT IS NULL OR n.role = $2)
           AND n.managed_by IN ($3, $4)
           AND COALESCE(n.state, 'registered') != 'deleted'
         LIMIT 1"
    ))
    .bind(node_id.as_uuid())
    .bind(role.map(NodeRole::as_str))
    .bind(SubstrateKind::Space)
    .bind(SubstrateKind::Kubernetes)
    .fetch_optional(executor.into_sqlx())
    .await?)
}
