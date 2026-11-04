//! SPACE node identity derived from a Kubernetes node: the role-scoped node
//! UUID that node-local daemons and the control plane compute from
//! `(k8s node name, SPACE node role)`, and the parser that accepts either a
//! direct node UUID or a Kubernetes node name.
//!
//! It is its own crate so that code can derive a node id without depending on
//! `pplx-space-common-svc`, which re-exports both functions from
//! `types::k8s`. It cannot live in `pplx-space-k8s-ids`, which holds only
//! Kubernetes' own identifiers: it returns `NodeId` and reads `NodeRole`,
//! which belong to `pplx-space-common-core`.

use pplx_space_common_core::types::{K8sNodeName, K8sNodeNameParseError, NodeId, NodeRole};
use uuid::Uuid;

const SPACE_K8S_NODE_ID_NAMESPACE: Uuid =
    Uuid::from_u128(0x2a5f_ef2d_8a55_4d2a_b87f_1d2f_77d0_c3a1);

/// Stable SPACE node id for a Kubernetes node-local service role.
///
/// Kubernetes exposes the node name to DaemonSet pods via the downward API, so
/// node-ctl and the inventory writer both derive identity from
/// `(k8s node name, SPACE node role)`. Role scoping lets worker and builder
/// daemons share the same Kubernetes host without queue ownership collisions.
pub fn space_k8s_node_id(node_name: &str, role: &str) -> NodeId {
    let key = format!("{node_name}\0{role}");
    NodeId::from_uuid(Uuid::new_v5(&SPACE_K8S_NODE_ID_NAMESPACE, key.as_bytes()))
}

/// Parses a direct node UUID or derives a role-scoped ID from a Kubernetes node name.
pub fn space_node_id(value: &str, role: NodeRole) -> Result<NodeId, K8sNodeNameParseError> {
    if let Ok(node_id) = value.parse::<NodeId>() {
        return Ok(node_id);
    }
    let node_name = K8sNodeName::parse(value)?;
    Ok(space_k8s_node_id(node_name.as_str(), role.as_str()))
}

#[cfg(test)]
mod tests;
