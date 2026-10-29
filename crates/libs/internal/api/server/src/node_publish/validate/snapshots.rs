use pplx_space_common_core::types::SandboxId;
use pplx_space_sbx_mgr_client::proto::{SnapshotDetail, SnapshotType};
use tonic::Status;

use super::fields::{u32_to_i32, u64_to_i64};

pub(crate) fn snapshot_json(
    sandbox_id: SandboxId,
    snapshot: &SnapshotDetail,
) -> Result<serde_json::Value, Status> {
    Ok(serde_json::json!({
        "sandbox_id": sandbox_id,
        "snapshot_id": &snapshot.id,
        "type": snapshot_type_name(snapshot.r#type),
        "state": &snapshot.state,
        "timestamp": snapshot.timestamp.as_deref(),
        "source_node_id": snapshot.source_node_id.as_deref(),
        "label": snapshot.label.as_deref(),
        "retention_tier": snapshot.retention_tier.as_deref(),
        "vcpu": u32_to_i32(snapshot.vcpu, "snapshot.vcpu")?,
        "memory_mib": u32_to_i32(snapshot.memory_mib, "snapshot.memory_mib")?,
        "size_bytes": snapshot.size_bytes
            .map(|value| u64_to_i64(value, "snapshot.size_bytes"))
            .transpose()?,
        "template_id": snapshot.template_id.as_deref(),
        "content_hash": snapshot.content_hash.as_deref(),
        "locations": snapshot_locations_json(snapshot),
    }))
}

fn snapshot_type_name(value: i32) -> &'static str {
    match SnapshotType::try_from(value) {
        Ok(SnapshotType::Disk) => "disk",
        Ok(SnapshotType::Full) => "full",
        _ => "unknown",
    }
}

fn snapshot_locations_json(snapshot: &SnapshotDetail) -> serde_json::Value {
    serde_json::Value::Array(
        snapshot
            .locations
            .iter()
            .map(|location| {
                serde_json::json!({
                    "location_type": location.location_type,
                    "node_id": location.node_id,
                    "node_address": location.node_address,
                    "volume_id": location.volume_id,
                    "state": location.state,
                    "size_bytes": location.size_bytes,
                })
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests;
