use pplx_space_common_core::types::{PolicyAuthority, RuntimeKind, SandboxId, SemVer};

use crate::node_publish::validate::{
    fields::ExecutionProjection,
    sandbox_metadata::PublishedRuntimeLabel,
    sandbox_observation::{PublishedActivityTimestamp, ValidatedSandboxObservation},
    sandbox_status::PublishedSandboxStatus,
};

/// Rich observed-state evidence without receipts or network policy payloads.
/// Assignment and lifecycle authority remain database decisions.
#[derive(Debug)]
pub(crate) struct ObservedStateClaim {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) status: Option<PublishedSandboxStatus>,
    pub(crate) runtime: Option<PublishedRuntimeLabel>,
    pub(crate) spaced_version: Option<SemVer>,
    pub(crate) execution: Option<ObservedExecutionClaim>,
    pub(crate) state_changed_at: Option<PublishedActivityTimestamp>,
    pub(crate) pid: Option<i32>,
    pub(crate) template: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) uptime: Option<String>,
    pub(crate) owner: Option<String>,
    pub(crate) image_hash: Option<String>,
    pub(crate) pool_template: Option<String>,
    pub(crate) failure_reason: Option<String>,
    pub(crate) observed_created_at: Option<String>,
    pub(crate) snapshots_enabled: bool,
    pub(crate) network_tx_bytes: Option<i64>,
    pub(crate) network_rx_bytes: Option<i64>,
    pub(crate) disk_usage_bytes: Option<i64>,
    pub(crate) auto_lifecycle_authority: Option<PolicyAuthority>,
}

impl From<ValidatedSandboxObservation> for ObservedStateClaim {
    fn from(observation: ValidatedSandboxObservation) -> Self {
        Self {
            sandbox_id: observation.sandbox_id,
            status: observation.status,
            runtime: observation.runtime,
            spaced_version: observation.spaced_version,
            execution: observation.execution.map(Into::into),
            state_changed_at: observation.state_changed_at,
            pid: observation.pid,
            template: observation.template,
            namespace: observation.namespace,
            uptime: observation.uptime,
            owner: observation.owner,
            image_hash: observation.image_hash,
            pool_template: observation.pool_template,
            failure_reason: observation.failure_reason,
            observed_created_at: observation.observed_created_at,
            snapshots_enabled: observation.snapshots_enabled,
            network_tx_bytes: observation.network_tx_bytes,
            network_rx_bytes: observation.network_rx_bytes,
            disk_usage_bytes: observation.disk_usage_bytes,
            auto_lifecycle_authority: observation.auto_lifecycle_authority,
        }
    }
}

/// Persisted projections of checked execution metadata, without separate
/// component rows. Component facts remain inside the opaque metadata column.
#[derive(Debug)]
pub(crate) struct ObservedExecutionClaim {
    pub(crate) runtime: RuntimeKind,
    pub(crate) runtime_version: String,
    pub(crate) guest_os: String,
    pub(crate) guest_arch: String,
    pub(crate) state_format_name: Option<String>,
    pub(crate) state_format_version: Option<String>,
    pub(crate) metadata: serde_json::Value,
}

impl From<ExecutionProjection> for ObservedExecutionClaim {
    fn from(execution: ExecutionProjection) -> Self {
        Self {
            runtime: execution.runtime_kind(),
            runtime_version: execution.runtime_version,
            guest_os: execution.guest_os,
            guest_arch: execution.guest_arch,
            state_format_name: execution.state_format_name,
            state_format_version: execution.state_format_version,
            metadata: execution.metadata,
        }
    }
}
