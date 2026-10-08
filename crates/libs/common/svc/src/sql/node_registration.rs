//! Postgres registration reads for Kubernetes nodes.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{K8sNodeName, K8sNodeUid, NodeId};

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
