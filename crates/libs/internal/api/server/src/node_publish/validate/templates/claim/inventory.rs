use pplx_space_common_core::types::{
    Arch, AutoLifecyclePolicy, GuestOperatingSystem, PolicyAuthority, RuntimeKind,
};

use super::{PublishedTemplateName, TemplateClaim, TemplateDescription, TemplateSnapshotClaim};
use crate::node_publish::validate::fields::ExecutionProjection;

#[derive(Debug)]
pub(crate) struct TemplateExecutionClaim {
    pub(crate) runtime: RuntimeKind,
    pub(crate) arch: Arch,
    pub(crate) runtime_version: String,
    pub(crate) guest_os: GuestOperatingSystem,
    pub(crate) state_format_name: Option<String>,
    pub(crate) state_format_version: Option<String>,
    pub(crate) metadata: serde_json::Value,
}

impl From<ExecutionProjection> for TemplateExecutionClaim {
    fn from(execution: ExecutionProjection) -> Self {
        Self {
            runtime: execution.runtime_kind(),
            arch: execution.guest().architecture,
            guest_os: execution.guest().os,
            runtime_version: execution.runtime_version,
            state_format_name: execution.state_format_name,
            state_format_version: execution.state_format_version,
            metadata: execution.metadata,
        }
    }
}

/// Descriptive node evidence only. Registration and conflict guards remain in
/// the store; component and policy-application claims have separate inputs.
#[derive(Debug)]
pub(crate) struct TemplateInventoryClaim {
    pub(crate) name: PublishedTemplateName,
    pub(crate) content_hash: String,
    description: TemplateDescription,
    pub(crate) execution: Option<TemplateExecutionClaim>,
    pub(crate) artifact_size_bytes: Option<i64>,
    pub(crate) cache_size_bytes: Option<i64>,
    pub(crate) has_base_snapshot: bool,
    pub(crate) base_snapshot_size_bytes: Option<i64>,
    pub(crate) pool_target: Option<i32>,
    pub(crate) pool_current: i32,
    pub(crate) pool_pending: i32,
    pub(crate) pool_sandbox_ids: Vec<String>,
    pub(crate) snapshots: Vec<TemplateSnapshotClaim>,
    pub(crate) default_auto_lifecycle: Option<AutoLifecyclePolicy>,
    pub(crate) default_auto_lifecycle_authority: Option<PolicyAuthority>,
    pub(crate) network_policy_authority: Option<PolicyAuthority>,
    pub(crate) ensure_base_snapshot: Option<bool>,
    pub(crate) snapshot_vcpu: Option<i32>,
    pub(crate) snapshot_memory_mib: Option<i32>,
    pub(crate) vcpu: Option<i32>,
    pub(crate) memory_mib: Option<i32>,
    pub(crate) disk_size_mb: Option<i32>,
}

impl From<TemplateClaim> for TemplateInventoryClaim {
    fn from(claim: TemplateClaim) -> Self {
        Self {
            name: claim.name,
            content_hash: claim.content_hash,
            description: claim.description,
            execution: claim.execution.map(Into::into),
            artifact_size_bytes: claim.artifact_size_bytes,
            cache_size_bytes: claim.cache_size_bytes,
            has_base_snapshot: claim.has_base_snapshot,
            base_snapshot_size_bytes: claim.base_snapshot_size_bytes,
            pool_target: claim.pool_target,
            pool_current: claim.pool_current,
            pool_pending: claim.pool_pending,
            pool_sandbox_ids: claim.pool_sandbox_ids,
            snapshots: claim.snapshots,
            default_auto_lifecycle: claim.default_auto_lifecycle,
            default_auto_lifecycle_authority: claim.default_auto_lifecycle_authority,
            network_policy_authority: claim.network_policy_authority,
            ensure_base_snapshot: claim.ensure_base_snapshot,
            snapshot_vcpu: claim.snapshot_vcpu,
            snapshot_memory_mib: claim.snapshot_memory_mib,
            vcpu: claim.vcpu,
            memory_mib: claim.memory_mib,
            disk_size_mb: claim.disk_size_mb,
        }
    }
}

impl TemplateInventoryClaim {
    pub(crate) fn runtime(&self) -> Option<&str> {
        // Explicit builder labels win; workers fall back to execution evidence.
        self.description
            .runtime
            .as_ref()
            .map(|label| label.as_str())
            .or_else(|| {
                self.execution
                    .as_ref()
                    .map(|execution| execution.runtime.alias())
            })
    }

    pub(crate) fn arch(&self) -> Option<&str> {
        // Explicit builder labels win; workers fall back to execution evidence.
        self.description
            .arch
            .as_ref()
            .map(|label| label.as_str())
            .or_else(|| {
                self.execution
                    .as_ref()
                    .map(|execution| execution.arch.as_str())
            })
    }

    pub(crate) fn built_at(&self) -> Option<&str> {
        self.description.built_at.as_deref()
    }
}
