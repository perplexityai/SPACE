use pplx_space_common_core::types::SandboxId;
use pplx_space_sbx_mgr_client::proto::{SnapshotDetail, SnapshotType};
use tonic::Status;

use super::fields::{u32_to_i32, u64_to_i64};

/// Descriptive node evidence, not a central snapshot or a restore authorization.
/// Names, states and provenance retain the inventory's open text grammar; they
/// must not acquire the stricter path-name or central UUID validation rules.
#[derive(Debug)]
pub(crate) struct SnapshotClaim {
    sandbox_id: SandboxId,
    snapshot_id: String,
    kind: SnapshotType,
    state: String,
    timestamp: Option<String>,
    source_node_id: Option<String>,
    label: Option<String>,
    retention_tier: Option<String>,
    vcpu: i32,
    memory_mib: i32,
    size_bytes: Option<i64>,
    template_id: Option<String>,
    content_hash: Option<String>,
    locations: Vec<SnapshotLocationClaim>,
}

#[derive(Debug, serde::Serialize)]
struct SnapshotLocationClaim {
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

    pub(crate) fn legacy_key(&self) -> serde_json::Value {
        serde_json::json!({
            "sandbox_id": self.sandbox_id,
            "snapshot_id": self.snapshot_id,
        })
    }

    pub(crate) fn legacy_row(&self) -> serde_json::Value {
        let kind = match self.kind {
            SnapshotType::Disk => "disk",
            SnapshotType::Full => "full",
            SnapshotType::Unspecified => "unknown",
        };
        serde_json::json!({
            "sandbox_id": self.sandbox_id,
            "snapshot_id": self.snapshot_id,
            "type": kind,
            "state": self.state,
            "timestamp": self.timestamp,
            "source_node_id": self.source_node_id,
            "label": self.label,
            "retention_tier": self.retention_tier,
            "vcpu": self.vcpu,
            "memory_mib": self.memory_mib,
            "size_bytes": self.size_bytes,
            "template_id": self.template_id,
            "content_hash": self.content_hash,
            "locations": self.locations,
        })
    }
}

#[cfg(test)]
mod tests;
