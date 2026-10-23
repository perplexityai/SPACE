//! Postgres registration reads for Kubernetes nodes.
//!
//! The registration check on `node_k8s`: whether a Kubernetes node name and
//! UID map to a SPACE node. Other `node_k8s` reads and its writers stay with
//! their callers. It is its own crate so a caller can make the check without
//! `pplx-space-common-svc`, which re-exports it as `sql::node_registration`.

use pplx_space_common_core::types::{K8sNodeName, K8sNodeUid, NodeId};
use pplx_space_sql_locator::sql;

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
