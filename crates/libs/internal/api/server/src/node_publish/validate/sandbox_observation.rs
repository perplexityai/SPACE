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
    pub(in crate::node_publish) sandbox_id: SandboxId,
    pub(in crate::node_publish) status: Option<PublishedSandboxStatus>,
    pub(in crate::node_publish) runtime: Option<PublishedRuntimeLabel>,
    pub(in crate::node_publish) spaced_version: Option<SemVer>,
    pub(in crate::node_publish) execution: Option<ExecutionProjection>,
    identity: SandboxIdentityEvidence,
    pub(in crate::node_publish) state_changed_at: Option<PublishedActivityTimestamp>,
    pub(in crate::node_publish) pid: Option<i32>,
    // These annotations are open legacy text, not lifecycle or execution authority.
    pub(in crate::node_publish) template: Option<String>,
    pub(in crate::node_publish) namespace: Option<String>,
    pub(in crate::node_publish) uptime: Option<String>,
    pub(in crate::node_publish) owner: Option<String>,
    pub(in crate::node_publish) image_hash: Option<String>,
    pub(in crate::node_publish) pool_template: Option<String>,
    pub(in crate::node_publish) failure_reason: Option<String>,
    pub(in crate::node_publish) observed_created_at: Option<String>,
    pub(in crate::node_publish) snapshots_enabled: bool,
    last_used_at: Option<PublishedActivityTimestamp>,
    last_access_at: Option<PublishedActivityTimestamp>,
    last_command_at: Option<PublishedActivityTimestamp>,
    pub(in crate::node_publish) network_tx_bytes: Option<i64>,
    pub(in crate::node_publish) network_rx_bytes: Option<i64>,
    pub(in crate::node_publish) disk_usage_bytes: Option<i64>,
    pub(in crate::node_publish) network_policy: Option<NetworkPolicyV1>,
    pub(in crate::node_publish) network_policy_generation: Option<i64>,
    pub(in crate::node_publish) network_policy_source: NetworkPolicySource,
    pub(in crate::node_publish) network_policy_authority: Option<PolicyAuthority>,
    pub(in crate::node_publish) auto_lifecycle_authority: Option<PolicyAuthority>,
}

impl ValidatedSandboxObservation {
    pub(crate) fn sandbox_id(&self) -> SandboxId {
        self.sandbox_id
    }

    pub(crate) fn status(&self) -> Option<&PublishedSandboxStatus> {
        self.status.as_ref()
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
}

#[cfg(test)]
mod tests;
