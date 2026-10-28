use pplx_space_common_core::types::{
    Arch, AutoLifecyclePolicy, NodeRole, PolicyAuthority, RuntimeKind,
};
use pplx_space_template_mgr_client::proto::SnapshotInfo;
use tonic::Status;

use super::fields::{
    execution_projection, non_zero_u64_to_i64, optional_u32_to_i32_when, optional_u64_to_i64_when,
    u32_to_i32, u64_to_i64,
};
use crate::proto::PublishedTemplateState;
use crate::{optional_trimmed, proto_network_policy_to_common, trimmed_required};

/// Checked template publication input. Labels retain the legacy inventory
/// grammar, which is intentionally broader than template-creation path names.
#[derive(Debug)]
pub(crate) struct ValidatedTemplateInventory {
    absent_templates: Vec<String>,
    rows: Vec<serde_json::Value>,
}

impl ValidatedTemplateInventory {
    pub(crate) fn try_from_proto(
        publisher_role: NodeRole,
        states: &[PublishedTemplateState],
    ) -> Result<Self, Status> {
        let include_template_metadata = publisher_role == NodeRole::Builder;
        let mut absent_templates = Vec::new();
        let mut rows = Vec::new();
        for state in states {
            // Identity rejection precedes optional metadata rejection, even for
            // an absence claim. Do not deduplicate conflicting declarations.
            let template = trimmed_required(&state.template, "template")?;
            if !state.present {
                absent_templates.push(template);
                continue;
            }
            rows.push(template_state_row(state, include_template_metadata)?);
        }
        Ok(Self {
            absent_templates,
            rows,
        })
    }

    pub(crate) fn into_legacy_rows(self) -> (Vec<String>, Vec<serde_json::Value>) {
        (self.absent_templates, self.rows)
    }
}

pub(crate) fn template_state_row(
    state: &PublishedTemplateState,
    include_template_metadata: bool,
) -> Result<serde_json::Value, Status> {
    let execution = execution_projection(state.execution.as_ref())?;
    if let Some(execution) = execution.as_ref() {
        if let Some(runtime) = optional_trimmed(state.runtime.as_deref()) {
            let runtime = RuntimeKind::parse(&runtime)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let execution_runtime = RuntimeKind::parse(&execution.runtime)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            if runtime != execution_runtime {
                return Err(Status::invalid_argument(
                    "published template runtime disagrees with execution metadata",
                ));
            }
        }
        if let Some(arch) = optional_trimmed(state.arch.as_deref()) {
            let arch = arch
                .parse::<Arch>()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let execution_arch = execution
                .guest_arch
                .parse::<Arch>()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            if arch != execution_arch {
                return Err(Status::invalid_argument(
                    "published template architecture disagrees with execution metadata",
                ));
            }
        }
    }
    let policy = state
        .network_policy
        .as_ref()
        .map(proto_network_policy_to_common)
        .transpose()?;
    let default_auto_lifecycle = if include_template_metadata {
        state
            .default_auto_lifecycle
            .as_ref()
            .and_then(|policy| AutoLifecyclePolicy::try_from(policy).ok())
    } else {
        None
    };
    Ok(serde_json::json!({
        "template_name": trimmed_required(&state.template, "template")?,
        "content_hash": optional_trimmed(state.content_hash.as_deref()).unwrap_or_default(),
        "runtime": include_template_metadata
            .then(|| optional_trimmed(state.runtime.as_deref()))
            .flatten()
            .or_else(|| execution.as_ref().map(|execution| execution.runtime.clone())),
        "arch": include_template_metadata
            .then(|| optional_trimmed(state.arch.as_deref()))
            .flatten()
            .or_else(|| execution.as_ref().map(|execution| execution.guest_arch.clone())),
        "runtime_version": execution.as_ref().map(|execution| execution.runtime_version.clone()),
        "guest_os": execution.as_ref().map(|execution| execution.guest_os.clone()),
        "state_format_name": execution.as_ref().and_then(|execution| execution.state_format_name.clone()),
        "state_format_version": execution.as_ref().and_then(|execution| execution.state_format_version.clone()),
        "execution_metadata": execution.as_ref().map(|execution| execution.metadata.clone()),
        "execution_components": execution.as_ref().map(|execution| execution.components.clone()).unwrap_or_default(),
        "built_at": include_template_metadata
            .then(|| optional_trimmed(state.built_at.as_deref()))
            .flatten(),
        "artifact_size_bytes": optional_u64_to_i64_when(
            include_template_metadata,
            state.artifact_size_bytes,
            "artifact_size_bytes",
        )?,
        "cache_size_bytes": state.cache_size_bytes
            .map(|value| u64_to_i64(value, "cache_size_bytes"))
            .transpose()?,
        "has_base_snapshot": state.has_base_snapshot,
        "base_snapshot_size_bytes": state.base_snapshot_size_bytes
            .map(|value| u64_to_i64(value, "base_snapshot_size_bytes"))
            .transpose()?,
        "pool_target": state.pool_target
            .map(|value| u32_to_i32(value, "pool_target"))
            .transpose()?,
        "pool_current": u32_to_i32(state.pool_current, "pool_current")?,
        "pool_pending": u32_to_i32(state.pool_pending, "pool_pending")?,
        "pool_sandbox_ids": &state.pool_sandbox_ids,
        "snapshots": template_snapshots_json(&state.snapshots),
        "default_auto_lifecycle_json": default_auto_lifecycle,
        "default_auto_lifecycle_authority": PolicyAuthority::from_optional_proto(state.auto_lifecycle_authority)
            .map(|authority| authority.map(|authority| authority.as_str()))
            .map_err(|error| Status::invalid_argument(error.to_string()))?,
        "network_policy_json": policy,
        "network_policy_generation": non_zero_u64_to_i64(
            state.network_policy_generation,
            "network_policy_generation",
        )?,
        "network_policy_authority": PolicyAuthority::from_optional_proto(state.network_policy_authority)
            .map(|authority| authority.map(|authority| authority.as_str()))
            .map_err(|error| Status::invalid_argument(error.to_string()))?,
        "ensure_base_snapshot": include_template_metadata.then_some(state.ensure_base_snapshot).flatten(),
        "snapshot_vcpu": optional_u32_to_i32_when(
            include_template_metadata,
            state.snapshot_vcpu,
            "snapshot_vcpu",
        )?,
        "snapshot_memory_mib": optional_u32_to_i32_when(
            include_template_metadata,
            state.snapshot_memory_mib,
            "snapshot_memory_mib",
        )?,
        "vcpu": optional_u32_to_i32_when(include_template_metadata, state.vcpu, "vcpu")?,
        "memory_mib": optional_u32_to_i32_when(
            include_template_metadata,
            state.memory_mib,
            "memory_mib",
        )?,
        "disk_size_mb": optional_u32_to_i32_when(
            include_template_metadata,
            state.disk_size_mb,
            "disk_size_mb",
        )?,
    }))
}

fn template_snapshots_json(snapshots: &[SnapshotInfo]) -> serde_json::Value {
    serde_json::Value::Array(
        snapshots
            .iter()
            .map(|snapshot| {
                serde_json::json!({
                    "vcpu": snapshot.vcpu,
                    "memory_mib": snapshot.memory_mib,
                    "snapshot_hash": snapshot.snapshot_hash,
                })
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests;
