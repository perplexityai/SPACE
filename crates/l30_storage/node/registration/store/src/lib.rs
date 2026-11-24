//! Postgres state of SPACE nodes: `nodes`, the Kubernetes registration in
//! `node_k8s`, and `node_k8s_tombstones`.
//!
//! Registering a Kubernetes node ([`register_k8s_node`]), which also records
//! the EC2 instance it runs on, and the
//! registration check on `node_k8s`: whether a Kubernetes node name and UID
//! map to a SPACE node ([`registered_node`]), or which live registrations in
//! given roles carry that name ([`nodes_for_identity`]). [`lookup`] resolves
//! nodes through their registration, [`drain_reads`] reads the drain columns
//! without writing them, [`drain_write`] writes them checking only the node
//! id, [`tombstone`] records terminated nodes, [`inventory`] lists the
//! Kubernetes nodes, and [`node_group`] mirrors their node-group labels.
//!
//! Every verb runs on the caller's executor, so it composes into the
//! caller's transaction. The drain-phase compare-and-sets, whose `WHERE`
//! re-checks the current phase, stay with their callers.

use pplx_space_accounts::NodeId;
use pplx_space_k8s_ids::{K8sNodeName, K8sNodeUid};
use pplx_space_node_model::{NodeRole, SubstrateKind};
use pplx_space_sql_locator::sql;

pub mod drain_reads;
pub mod drain_v2;
pub mod drain_write;
pub mod inventory;
pub mod lookup;
pub mod node_group;
mod register;
pub mod tombstone;
pub use register::{
    K8sNodeRegistration, K8sNodeRegistrationError, K8sNodeRegistrationStep, register_k8s_node,
};

#[derive(Debug, thiserror::Error)]
pub enum NodeRegistrationError {
    #[error("node registration lookup failed: {0}")]
    Query(#[from] sqlx::Error),
}

/// Require both name and UID; a registration without a UID cannot match.
pub async fn registered_node(
    executor: impl sqlx::PgExecutor<'_>,
    name: &K8sNodeName,
    uid: &K8sNodeUid,
) -> Result<Option<NodeId>, NodeRegistrationError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT node_id FROM node_k8s
         WHERE k8s_node_name = $1 AND k8s_node_uid = $2 AND k8s_node_uid IS NOT NULL"
    ))
    .bind(name.as_str())
    .bind(uid.as_str())
    .fetch_optional(executor)
    .await?)
}

/// Live registrations of the reviewed node name in `roles`, each with
/// whether its inventory UID matches (`None` when it has none).
pub async fn nodes_for_identity(
    executor: impl sqlx::PgExecutor<'_>,
    name: &K8sNodeName,
    uid: &K8sNodeUid,
    roles: &[NodeRole],
) -> Result<Vec<(NodeId, Option<bool>)>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT n.id, k.k8s_node_uid = $2
         FROM nodes n JOIN node_k8s k ON k.node_id = n.id
         WHERE k.k8s_node_name = $1
           AND n.managed_by = $3 AND n.role = ANY($4::TEXT[])
           AND COALESCE(n.state, 'registered') != 'deleted'"
    ))
    .bind(name.as_str())
    .bind(uid.as_str())
    .bind(SubstrateKind::Kubernetes)
    .bind(roles.iter().map(|role| role.as_str()).collect::<Vec<_>>())
    .fetch_all(executor)
    .await
}

#[cfg(test)]
mod tests;
