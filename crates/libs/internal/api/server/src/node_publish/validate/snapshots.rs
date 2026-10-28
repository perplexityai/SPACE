use pplx_space_common_core::types::SandboxId;
use pplx_space_sbx_mgr_client::proto::{SnapshotDetail, SnapshotType};
use tonic::Status;

use super::fields::{u32_to_i32, u64_to_i64};

/// Descriptive node evidence, not a central snapshot or a restore authorization.
/// Names, states and provenance retain the inventory's open text grammar; they
/// must not acquire the stricter path-name or central UUID validation rules.
#[derive(Debug)]
pub(crate) struct SnapshotClaim {
    pub(in crate::node_publish) sandbox_id: SandboxId,
    pub(in crate::node_publish) snapshot_id: String,
    kind: SnapshotType,
    pub(in crate::node_publish) state: String,
    pub(in crate::node_publish) timestamp: Option<String>,
    pub(in crate::node_publish) source_node_id: Option<String>,
    pub(in crate::node_publish) label: Option<String>,
    pub(in crate::node_publish) retention_tier: Option<String>,
    pub(in crate::node_publish) vcpu: i32,
    pub(in crate::node_publish) memory_mib: i32,
    pub(in crate::node_publish) size_bytes: Option<i64>,
    pub(in crate::node_publish) template_id: Option<String>,
    pub(in crate::node_publish) content_hash: Option<String>,
    pub(in crate::node_publish) locations: Vec<SnapshotLocationClaim>,
}

#[derive(Debug, serde::Serialize)]
pub(in crate::node_publish) struct SnapshotLocationClaim {
    location_type: String,
    node_id: Option<String>,
    node_address: Option<String>,
    volume_id: Option<String>,
    state: String,
    size_bytes: u64,
}

impl SnapshotClaim {
    pub(crate) fn try_from_proto(
        sandbox_id: SandboxId,
        snapshot: &SnapshotDetail,
    ) -> Result<Self, Status> {
        let kind = match SnapshotType::try_from(snapshot.r#type) {
            Ok(kind) => kind,
            Err(_) => SnapshotType::Unspecified,
        };
        Ok(Self {
            sandbox_id,
            snapshot_id: snapshot.id.clone(),
            kind,
            state: snapshot.state.clone(),
            timestamp: snapshot.timestamp.clone(),
            source_node_id: snapshot.source_node_id.clone(),
            label: snapshot.label.clone(),
            retention_tier: snapshot.retention_tier.clone(),
            vcpu: u32_to_i32(snapshot.vcpu, "snapshot.vcpu")?,
            memory_mib: u32_to_i32(snapshot.memory_mib, "snapshot.memory_mib")?,
            size_bytes: snapshot
                .size_bytes
                .map(|value| u64_to_i64(value, "snapshot.size_bytes"))
                .transpose()?,
            template_id: snapshot.template_id.clone(),
            content_hash: snapshot.content_hash.clone(),
            locations: snapshot
                .locations
                .iter()
                .map(|location| SnapshotLocationClaim {
                    location_type: location.location_type.clone(),
                    node_id: location.node_id.clone(),
                    node_address: location.node_address.clone(),
                    volume_id: location.volume_id.clone(),
                    state: location.state.clone(),
                    size_bytes: location.size_bytes,
                })
                .collect(),
        })
    }

    pub(in crate::node_publish) fn kind(&self) -> &'static str {
        match self.kind {
            SnapshotType::Disk => "disk",
            SnapshotType::Full => "full",
            SnapshotType::Unspecified => "unknown",
        }
    }
}

#[cfg(test)]
mod tests;
