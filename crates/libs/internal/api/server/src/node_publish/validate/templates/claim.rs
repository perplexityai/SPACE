use pplx_space_common_core::types::{Arch, AutoLifecyclePolicy, PolicyAuthority, RuntimeKind};
use pplx_space_net_common::NetworkPolicyV1;
use tonic::Status;

use crate::node_publish::validate::fields::{
    ArtifactComponentProjection, ExecutionProjection, execution_projection, non_zero_u64_to_i64,
    optional_u32_to_i32_when, optional_u64_to_i64_when, u32_to_i32, u64_to_i64,
};
use crate::node_publish::validate::sandbox_metadata::PublishedRuntimeLabel;
use crate::proto::PublishedTemplateState;
use crate::{optional_trimmed, proto_network_policy_to_common, trimmed_required};

/// A publication name follows the open inventory grammar, not path-name rules.
#[derive(Clone, Debug)]
pub(crate) struct PublishedTemplateName(String);

impl PublishedTemplateName {
    pub(super) fn parse(raw: &str) -> Result<Self, Status> {
        trimmed_required(raw, "template").map(Self)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug)]
pub(in crate::node_publish) struct TemplateComponentSet {
    pub(in crate::node_publish) name: PublishedTemplateName,
    pub(in crate::node_publish) content_hash: String,
    pub(in crate::node_publish) components: Vec<ArtifactComponentProjection>,
}

/// A node's policy observation is actionable only with a reported generation.
#[derive(Debug)]
pub(crate) struct TemplatePolicyClaim {
    pub(crate) template: PublishedTemplateName,
    pub(crate) generation: i64,
    pub(crate) policy: Option<NetworkPolicyV1>,
    pub(crate) authority: Option<PolicyAuthority>,
}

/// Optional descriptive metadata retains its legacy spelling. Runtime and
/// architecture are checked against execution evidence before constructing it.
#[derive(Debug)]
struct TemplateDescription {
    runtime: Option<PublishedRuntimeLabel>,
    arch: Option<PublishedArchitectureLabel>,
    built_at: Option<String>,
}

/// An open legacy label, retaining aliases rather than rewriting stored rows.
#[derive(Debug)]
struct PublishedArchitectureLabel(String);

impl PublishedArchitectureLabel {
    fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, serde::Serialize)]
struct TemplateSnapshotClaim {
    vcpu: u32,
    memory_mib: u32,
    snapshot_hash: String,
}

/// Typed node evidence; no database guard or final row change is represented.
#[derive(Debug)]
pub(super) struct TemplateClaim {
    name: PublishedTemplateName,
    content_hash: String,
    description: TemplateDescription,
    execution: Option<ExecutionProjection>,
    artifact_size_bytes: Option<i64>,
    cache_size_bytes: Option<i64>,
    has_base_snapshot: bool,
    base_snapshot_size_bytes: Option<i64>,
    pool_target: Option<i32>,
    pool_current: i32,
    pool_pending: i32,
    // Pool membership and snapshot hashes are legacy descriptions, not grants.
    pool_sandbox_ids: Vec<String>,
    snapshots: Vec<TemplateSnapshotClaim>,
    default_auto_lifecycle: Option<AutoLifecyclePolicy>,
    default_auto_lifecycle_authority: Option<PolicyAuthority>,
    network_policy: Option<NetworkPolicyV1>,
    network_policy_generation: Option<i64>,
    network_policy_authority: Option<PolicyAuthority>,
    ensure_base_snapshot: Option<bool>,
    snapshot_vcpu: Option<i32>,
    snapshot_memory_mib: Option<i32>,
    vcpu: Option<i32>,
    memory_mib: Option<i32>,
    disk_size_mb: Option<i32>,
}

impl TemplateClaim {
    pub(super) fn take_policy(&mut self) -> Option<TemplatePolicyClaim> {
        let generation = self.network_policy_generation?;
        Some(TemplatePolicyClaim {
            template: self.name.clone(),
            generation,
            policy: self.network_policy.take(),
            authority: self.network_policy_authority,
        })
    }

    pub(super) fn take_components(&mut self) -> TemplateComponentSet {
        TemplateComponentSet {
            name: self.name.clone(),
            content_hash: self.content_hash.clone(),
            components: self
                .execution
                .as_mut()
                .map(|execution| std::mem::take(&mut execution.components))
                .unwrap_or_default(),
        }
    }

    pub(super) fn try_from_proto(
        state: &PublishedTemplateState,
        include_template_metadata: bool,
    ) -> Result<Self, Status> {
        let execution = execution_projection(state.execution.as_ref())?;
        check_execution_labels(state, execution.as_ref())?;
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
        // Keep the legacy rejection sequence, including fields ignored for
        // worker reports and policy authority checks between numeric fields.
        Ok(Self {
            name: PublishedTemplateName::parse(&state.template)?,
            content_hash: optional_trimmed(state.content_hash.as_deref()).unwrap_or_default(),
            description: TemplateDescription {
                runtime: include_template_metadata
                    .then(|| PublishedRuntimeLabel::from_optional(state.runtime.as_deref()))
                    .flatten(),
                arch: include_template_metadata
                    .then(|| {
                        optional_trimmed(state.arch.as_deref()).map(PublishedArchitectureLabel)
                    })
                    .flatten(),
                built_at: include_template_metadata
                    .then(|| optional_trimmed(state.built_at.as_deref()))
                    .flatten(),
            },
            artifact_size_bytes: optional_u64_to_i64_when(
                include_template_metadata,
                state.artifact_size_bytes,
                "artifact_size_bytes",
            )?,
            cache_size_bytes: state
                .cache_size_bytes
                .map(|value| u64_to_i64(value, "cache_size_bytes"))
                .transpose()?,
            has_base_snapshot: state.has_base_snapshot,
            base_snapshot_size_bytes: state
                .base_snapshot_size_bytes
                .map(|value| u64_to_i64(value, "base_snapshot_size_bytes"))
                .transpose()?,
            pool_target: state
                .pool_target
                .map(|value| u32_to_i32(value, "pool_target"))
                .transpose()?,
            pool_current: u32_to_i32(state.pool_current, "pool_current")?,
            pool_pending: u32_to_i32(state.pool_pending, "pool_pending")?,
            pool_sandbox_ids: state.pool_sandbox_ids.clone(),
            snapshots: state
                .snapshots
                .iter()
                .map(|snapshot| TemplateSnapshotClaim {
                    vcpu: snapshot.vcpu,
                    memory_mib: snapshot.memory_mib,
                    snapshot_hash: snapshot.snapshot_hash.clone(),
                })
                .collect(),
            default_auto_lifecycle,
            default_auto_lifecycle_authority: PolicyAuthority::from_optional_proto(
                state.auto_lifecycle_authority,
            )
            .map_err(|error| Status::invalid_argument(error.to_string()))?,
            network_policy: policy,
            network_policy_generation: non_zero_u64_to_i64(
                state.network_policy_generation,
                "network_policy_generation",
            )?,
            network_policy_authority: PolicyAuthority::from_optional_proto(
                state.network_policy_authority,
            )
            .map_err(|error| Status::invalid_argument(error.to_string()))?,
            ensure_base_snapshot: include_template_metadata
                .then_some(state.ensure_base_snapshot)
                .flatten(),
            snapshot_vcpu: optional_u32_to_i32_when(
                include_template_metadata,
                state.snapshot_vcpu,
                "snapshot_vcpu",
            )?,
            snapshot_memory_mib: optional_u32_to_i32_when(
                include_template_metadata,
                state.snapshot_memory_mib,
                "snapshot_memory_mib",
            )?,
            vcpu: optional_u32_to_i32_when(include_template_metadata, state.vcpu, "vcpu")?,
            memory_mib: optional_u32_to_i32_when(
                include_template_metadata,
                state.memory_mib,
                "memory_mib",
            )?,
            disk_size_mb: optional_u32_to_i32_when(
                include_template_metadata,
                state.disk_size_mb,
                "disk_size_mb",
            )?,
            execution,
        })
    }

    pub(super) fn legacy_row(&self) -> serde_json::Value {
        let execution = self.execution.as_ref();
        // Explicit builder descriptions win; workers fall back to execution evidence.
        let runtime = self
            .description
            .runtime
            .as_ref()
            .map(PublishedRuntimeLabel::as_str)
            .or_else(|| execution.map(|execution| execution.runtime.as_str()));
        let arch = self
            .description
            .arch
            .as_ref()
            .map(PublishedArchitectureLabel::as_str)
            .or_else(|| execution.map(|execution| execution.guest_arch.as_str()));
        serde_json::json!({
            "template_name": self.name.0,
            "content_hash": self.content_hash,
            "runtime": runtime,
            "arch": arch,
            "runtime_version": execution.map(|execution| &execution.runtime_version),
            "guest_os": execution.map(|execution| &execution.guest_os),
            "state_format_name": execution.and_then(|execution| execution.state_format_name.as_deref()),
            "state_format_version": execution.and_then(|execution| execution.state_format_version.as_deref()),
            "execution_metadata": execution.map(|execution| &execution.metadata),
            "execution_components": execution.map(|execution| execution.components.as_slice()).unwrap_or_default(),
            "built_at": self.description.built_at,
            "artifact_size_bytes": self.artifact_size_bytes,
            "cache_size_bytes": self.cache_size_bytes,
            "has_base_snapshot": self.has_base_snapshot,
            "base_snapshot_size_bytes": self.base_snapshot_size_bytes,
            "pool_target": self.pool_target,
            "pool_current": self.pool_current,
            "pool_pending": self.pool_pending,
            "pool_sandbox_ids": self.pool_sandbox_ids,
            "snapshots": self.snapshots,
            "default_auto_lifecycle_json": self.default_auto_lifecycle,
            "default_auto_lifecycle_authority": self.default_auto_lifecycle_authority.map(|authority| authority.as_str()),
            "network_policy_json": self.network_policy,
            "network_policy_generation": self.network_policy_generation,
            "network_policy_authority": self.network_policy_authority.map(|authority| authority.as_str()),
            "ensure_base_snapshot": self.ensure_base_snapshot,
            "snapshot_vcpu": self.snapshot_vcpu,
            "snapshot_memory_mib": self.snapshot_memory_mib,
            "vcpu": self.vcpu,
            "memory_mib": self.memory_mib,
            "disk_size_mb": self.disk_size_mb,
        })
    }
}

fn check_execution_labels(
    state: &PublishedTemplateState,
    execution: Option<&ExecutionProjection>,
) -> Result<(), Status> {
    if let Some(execution) = execution {
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
    Ok(())
}
