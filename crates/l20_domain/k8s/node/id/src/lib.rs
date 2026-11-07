//! SPACE node identity derived from a Kubernetes node: the role-scoped node
//! UUID that node-local daemons and the control plane compute from
//! `(k8s node name, SPACE node role)`, the parser that accepts either a
//! direct node UUID or a Kubernetes node name, and the EC2 instance a
//! Kubernetes node runs on, read from its `providerID`.
//!
//! It cannot live in `pplx-space-k8s-ids`, which holds only
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

/// An EC2 instance ID: `i-` and 8 or 17 lowercase hex digits, the form EC2
/// issues. Whether such an instance exists is EC2's to say; an ID in any
/// other form is never asked about, since EC2 would answer as for an
/// instance that is gone.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ec2InstanceId(String);

impl Ec2InstanceId {
    /// The instance ID in `aws:///<az>/<instance-id>`, the canonical pointer
    /// from a Kubernetes Node back to its EC2 instance. Everything before the
    /// final `/` is bookkeeping for the cloud-provider layer. `None` for any
    /// other provider (Kind's `kind://…`, for one) or a last segment that is
    /// not an instance ID.
    pub fn from_provider_id(provider_id: &str) -> Option<Self> {
        provider_id
            .strip_prefix("aws:///")
            .and_then(|rest| rest.rsplit('/').next())
            .and_then(Self::parse)
    }

    /// `value`, when it is an instance ID.
    pub fn parse(value: &str) -> Option<Self> {
        let hex = value.strip_prefix("i-")?;
        (matches!(hex.len(), 8 | 17)
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
        .then(|| Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Ec2InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests;
