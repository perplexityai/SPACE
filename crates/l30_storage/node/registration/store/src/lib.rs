//! Postgres registration of Kubernetes nodes.
//!
//! Registering a Kubernetes node ([`register_k8s_node`]), and the
//! registration check on `node_k8s`: whether a Kubernetes node name and UID
//! map to a SPACE node. Other `node_k8s` reads stay with their callers.

use pplx_space_accounts::NodeId;
use pplx_space_k8s_ids::{K8sNodeName, K8sNodeUid};
use pplx_space_sql_locator::sql;

mod register;
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
