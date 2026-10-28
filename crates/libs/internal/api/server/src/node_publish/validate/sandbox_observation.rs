use pplx_space_common_core::types::{
    PolicyAuthority, SandboxId, SandboxLifecycle, SandboxQuarantineReasonCode,
    SandboxTransitionReceipt, SemVer, SemVerParseError,
};
use pplx_space_net_common::{NetworkPolicyV1, proto::NetworkPolicySource};
use time::format_description::well_known::Rfc3339;
use tonic::Status;

use super::fields::{
    ExecutionProjection, execution_projection, non_zero_u64_to_i64, proto_timestamp_to_rfc3339,
    u32_to_i32, u64_to_i64,
};
use super::quarantine::QuarantineRecoveryProjection;
use super::sandbox_identity::SandboxIdentityEvidence;
use super::sandbox_metadata::{PublishedRuntimeLabel, policy_authority};
use super::sandbox_status::PublishedSandboxStatus;
use crate::metadata::{optional_trimmed, proto_network_policy_to_common};
use crate::proto::PublishedSandboxState;

#[derive(Clone, Debug, serde::Serialize)]
#[serde(transparent)]
pub(crate) struct PublishedActivityTimestamp(String);

impl PublishedActivityTimestamp {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    fn parse(raw: Option<&prost_types::Timestamp>, field: &str) -> Result<Option<Self>, Status> {
        raw.map(|timestamp| proto_timestamp_to_rfc3339(timestamp, field).map(Self))
            .transpose()
    }
}

#[derive(Debug)]
pub(crate) struct InvalidSpacedVersion {
    pub(crate) label: String,
    pub(crate) error: SemVerParseError,
}

#[derive(Debug)]
pub(crate) struct ObservationValidation {
    pub(crate) observation: Result<ValidatedSandboxObservation, Status>,
    pub(crate) version_warning: Option<InvalidSpacedVersion>,
}

/// Parsed observation evidence; none of these fields decides a guarded transition.
#[derive(Debug)]
pub(crate) struct ValidatedSandboxObservation {
    sandbox_id: SandboxId,
    status: Option<PublishedSandboxStatus>,
    runtime: Option<PublishedRuntimeLabel>,
    spaced_version: Option<SemVer>,
    execution: Option<ExecutionProjection>,
    identity: SandboxIdentityEvidence,
    state_changed_at: Option<PublishedActivityTimestamp>,
    pid: Option<i32>,
    // These annotations are open legacy text, not lifecycle or execution authority.
    template: Option<String>,
    namespace: Option<String>,
    uptime: Option<String>,
    owner: Option<String>,
    image_hash: Option<String>,
    pool_template: Option<String>,
    failure_reason: Option<String>,
    observed_created_at: Option<String>,
    quarantine_reason_code: Option<String>,
    snapshots_enabled: bool,
    last_used_at: Option<PublishedActivityTimestamp>,
    last_access_at: Option<PublishedActivityTimestamp>,
    last_command_at: Option<PublishedActivityTimestamp>,
    network_tx_bytes: Option<i64>,
    network_rx_bytes: Option<i64>,
    disk_usage_bytes: Option<i64>,
    network_policy: Option<NetworkPolicyV1>,
    network_policy_generation: Option<i64>,
    network_policy_source: NetworkPolicySource,
    network_policy_authority: Option<PolicyAuthority>,
    auto_lifecycle_authority: Option<PolicyAuthority>,
}

impl ValidatedSandboxObservation {
    pub(crate) fn sandbox_id(&self) -> SandboxId {
        self.sandbox_id
    }

    pub(crate) fn last_used_at(&self) -> Option<&PublishedActivityTimestamp> {
        self.last_used_at.as_ref()
    }

    pub(crate) fn last_access_at(&self) -> Option<&PublishedActivityTimestamp> {
        self.last_access_at.as_ref()
    }

    pub(crate) fn last_command_at(&self) -> Option<&PublishedActivityTimestamp> {
        self.last_command_at.as_ref()
    }

    pub(crate) fn identity(&self) -> &SandboxIdentityEvidence {
        &self.identity
    }

    pub(crate) fn failure_reason(&self) -> Option<&str> {
        self.failure_reason.as_deref()
    }

    pub(crate) fn reported_lifecycle(&self) -> Option<SandboxLifecycle> {
        self.status
            .as_ref()
            .and_then(PublishedSandboxStatus::lifecycle)
    }

    pub(crate) fn parse(
        sandbox_id: SandboxId,
        state: &PublishedSandboxState,
        receipt: Option<&SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
    ) -> ObservationValidation {
        let mut version_warning = None;
        let observation = Self::try_from_proto(sandbox_id, state, receipt, &mut version_warning);
        ObservationValidation {
            observation,
            version_warning,
        }
    }

    fn try_from_proto(
        sandbox_id: SandboxId,
        state: &PublishedSandboxState,
        receipt: Option<&SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
        version_warning: &mut Option<InvalidSpacedVersion>,
    ) -> Result<Self, Status> {
        // Preserve rejection precedence, including a version warning discovered
        // before a later optional field causes state-only degradation.
        let network_policy = state
            .observed_network_policy
            .as_ref()
            .map(proto_network_policy_to_common)
            .transpose()?;
        let network_policy_generation = non_zero_u64_to_i64(
            state.observed_network_policy_generation,
            "observed_network_policy_generation",
        )?;
        let execution = execution_projection(state.execution.as_ref())?;
        let spaced_version =
            optional_trimmed(state.spaced_version.as_deref()).and_then(
                |label| match SemVer::parse(&label) {
                    Ok(version) => Some(version),
                    Err(error) => {
                        *version_warning = Some(InvalidSpacedVersion { label, error });
                        None
                    }
                },
            );
        let runtime =
            PublishedRuntimeLabel::try_from_proto(state.runtime.as_deref(), execution.as_ref())?;
        let identity = SandboxIdentityEvidence::try_from_proto(state, receipt)?;
        Ok(Self {
            sandbox_id,
            status: PublishedSandboxStatus::from_proto(state.status.as_deref()),
            runtime,
            spaced_version,
            execution,
            pid: state
                .pid
                .map(|value| u32_to_i32(value, "pid"))
                .transpose()?,
            template: optional_trimmed(state.template.as_deref()),
            namespace: optional_trimmed(state.namespace.as_deref()),
            uptime: optional_trimmed(state.uptime.as_deref()),
            owner: optional_trimmed(state.owner.as_deref()),
            image_hash: optional_trimmed(state.content_hash.as_deref()),
            pool_template: optional_trimmed(state.pool_template.as_deref()),
            failure_reason: optional_trimmed(state.failure_reason.as_deref()),
            observed_created_at: optional_trimmed(state.created_at.as_deref()),
            quarantine_reason_code: optional_trimmed(state.quarantine_reason_code.as_deref()),
            snapshots_enabled: state.snapshots_enabled,
            last_used_at: PublishedActivityTimestamp::parse(
                state.last_used_at.as_ref(),
                "last_used_at",
            )?,
            last_access_at: PublishedActivityTimestamp::parse(
                state.last_access_at.as_ref(),
                "last_access_at",
            )?,
            last_command_at: PublishedActivityTimestamp::parse(
                state.last_command_at.as_ref(),
                "last_command_at",
            )?,
            network_tx_bytes: state
                .network_tx_bytes
                .map(|value| u64_to_i64(value, "network_tx_bytes"))
                .transpose()?,
            network_rx_bytes: state
                .network_rx_bytes
                .map(|value| u64_to_i64(value, "network_rx_bytes"))
                .transpose()?,
            disk_usage_bytes: state
                .disk_usage_bytes
                .map(|value| u64_to_i64(value, "disk_usage_bytes"))
                .transpose()?,
            network_policy,
            network_policy_generation,
            network_policy_source: NetworkPolicySource::try_from(
                state.observed_network_policy_source,
            )
            .unwrap_or(NetworkPolicySource::Unspecified),
            network_policy_authority: policy_authority(state.observed_network_policy_authority)?,
            auto_lifecycle_authority: policy_authority(state.auto_lifecycle_authority)?,
            // Formatting is the last fallible core-row conversion. Keeping it
            // here makes accepted evidence safe to serialize without changing
            // which malformed optional field wins.
            state_changed_at: identity
                .state_changed_at()
                .map(|value| {
                    value
                        .format(&Rfc3339)
                        .map(PublishedActivityTimestamp)
                        .map_err(|error| {
                            Status::internal(format!("format state_changed_at: {error}"))
                        })
                })
                .transpose()?,
            identity,
        })
    }

    pub(crate) fn publishes_network_policy(&self) -> bool {
        self.network_policy_generation.is_some()
    }

    pub(crate) fn legacy_row(
        &self,
        receipt: Option<&SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
        recovery: Option<&QuarantineRecoveryProjection>,
        recovery_degraded: bool,
    ) -> serde_json::Value {
        let execution = self.execution.as_ref();
        let artifact = self.identity.current_local_artifact();
        let recovery = recovery.map(|observation| {
            serde_json::json!({
                "schema_version": observation.schema_version,
                "local_quarantine_id": observation.local_quarantine_id,
                "observed_owner": observation.observed_owner_name(),
                "attempt_count": observation.attempt_count,
                "next_attempt_at": observation.next_attempt_at.as_deref(),
            })
        });
        let policy_source = match self.network_policy_source {
            NetworkPolicySource::Template => Some("template"),
            NetworkPolicySource::Override => Some("override"),
            NetworkPolicySource::Unspecified => None,
        };
        serde_json::json!({
            "sandbox_id": self.sandbox_id,
            "status": self.status.as_ref().map(PublishedSandboxStatus::as_str),
            "template": self.template,
            "runtime": self.runtime.as_ref().map(PublishedRuntimeLabel::as_str)
                .or_else(|| execution.map(|execution| execution.runtime.as_str())),
            "spaced_version": self.spaced_version.as_ref().map(ToString::to_string),
            "runtime_version": execution.map(|execution| &execution.runtime_version),
            "guest_os": execution.map(|execution| &execution.guest_os),
            "guest_arch": execution.map(|execution| &execution.guest_arch),
            "state_format_name": execution.and_then(|execution| execution.state_format_name.as_deref()),
            "state_format_version": execution.and_then(|execution| execution.state_format_version.as_deref()),
            "execution_metadata": execution.map(|execution| &execution.metadata),
            "execution_components": execution.map(|execution| execution.components.as_slice()).unwrap_or_default(),
            "pid": self.pid,
            "namespace": self.namespace,
            "uptime": self.uptime,
            "sandbox_owner": self.owner,
            "sandbox_user": self.owner,
            "image_hash": self.image_hash,
            "snapshots_enabled": self.snapshots_enabled,
            "pool_template": self.pool_template,
            "failure_reason": self.failure_reason,
            "observed_created_at": self.observed_created_at,
            "last_used_at": self.last_used_at,
            "last_access_at": self.last_access_at,
            "last_command_at": self.last_command_at,
            "network_tx_bytes": self.network_tx_bytes,
            "network_rx_bytes": self.network_rx_bytes,
            "disk_usage_bytes": self.disk_usage_bytes,
            "observed_network_policy_json": self.network_policy,
            "observed_network_policy_generation": self.network_policy_generation,
            "observed_network_policy_source": policy_source,
            "observed_network_policy_authority": self.network_policy_authority.map(|authority| authority.as_str()),
            "auto_lifecycle_authority": self.auto_lifecycle_authority.map(|authority| authority.as_str()),
            "quarantine_reason_code": self.quarantine_reason_code,
            "transition_receipt_id": receipt.map(|receipt| receipt.id().as_uuid()),
            "transition_receipt_kind": receipt.map(|receipt| receipt.kind().as_str()),
            "transition_receipt_target_state": receipt.map(|receipt| receipt.target_state().as_str()),
            "transition_receipt_execution_generation": receipt.map(|receipt| receipt.execution_generation().get()),
            "transition_receipt_reason_code": receipt.and_then(|receipt| receipt.reason_code().map(|reason| reason.as_str())),
            "transition_receipt_activation_attempt_id": receipt.and_then(|receipt| receipt.activation_attempt_id().map(|id| id.as_uuid())),
            "quarantine_recovery_payload": recovery,
            "quarantine_recovery_observation_degraded": recovery_degraded,
            "transition_receipt_recovery_policy": receipt
                .and_then(|receipt| receipt.reason_code().copied())
                .and_then(|reason| reason.automatic_recovery_policy())
                .map(|policy| policy.to_string()),
            "current_local_artifact_id": artifact.map(|artifact| artifact.id().to_string()),
            "current_local_artifact_kind": artifact.map(|artifact| artifact.kind().to_string()),
            "current_local_artifact_origin": artifact.map(|artifact| artifact.origin().to_string()),
            "state_changed_at": self.state_changed_at,
            "execution_generation": self.identity.execution_generation().map(|generation| generation.get()),
            "pod_uid": self.identity.pod_uid().map(ToString::to_string),
        })
    }
}

#[cfg(test)]
mod tests;
