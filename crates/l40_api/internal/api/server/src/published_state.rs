use pplx_space_accounts::NodeId;
use pplx_space_snapshot_model::SnapshotId;

use std::collections::{BTreeMap, BTreeSet};

use sqlx::types::Json;
use time::format_description::well_known::Rfc3339;
use tonic::Status;
use uuid::Uuid;

use crate::metadata::artifact_kind_from_proto;
use pplx_space_auto_lifecycle::AutoLifecyclePolicy;
use pplx_space_cluster_trigger_queue::{
    QueuedRefConflict, enqueue_triggers_tx, queue_node_reconcile_refs,
};
use pplx_space_cpu_arch::Arch;
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_execution_spec::{ExecutionMetadata, RuntimeKind};
use pplx_space_k8s_ids::PodUid;
use pplx_space_net_common::proto::NetworkPolicySource;
use pplx_space_node_model::NodeRole;
use pplx_space_policy_authority::PolicyAuthority;
use pplx_space_sandbox_model::{
    ExecutionGeneration, SandboxActivationAttemptId, SandboxId, SandboxLifecycle,
    SandboxQuarantineHistoryAction, SandboxQuarantineNextAction, SandboxQuarantineReasonCode,
    SandboxQuarantineSource, SandboxTransitionReceipt, SandboxTransitionReceiptInvariantError,
    SandboxTransitionReceiptKind, SandboxTransitionTargetState,
};
use pplx_space_sbx_mgr_client::proto::{
    QuarantineRecoveryObservedOwner, SnapshotDetail, SnapshotType,
};
use pplx_space_semver::SemVer;
use pplx_space_snapshot_model::{ArtifactKind, ArtifactOrigin, LocalArtifactId};
use pplx_space_state_reconciliation::{
    FACET_NETWORK_POLICY, FACET_SANDBOX_DESIRED_STATE, FACET_TEMPLATE_BASE_SNAPSHOT_POLICY,
    FACET_TEMPLATE_DESIRED_STATE, NetworkPolicyTarget, ReconcileReason,
    SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY, SANDBOX_FAILURE_EPISODE_KEY,
    SANDBOX_FAILURE_EXECUTION_GENERATION_KEY, SANDBOX_FAILURE_POD_UID_KEY,
    SANDBOX_LOCAL_SUSPEND_KEY,
};
use pplx_space_template_mgr_client::proto::SnapshotInfo;

use crate::proto::{PublishedSandboxState, PublishedTemplateState};
use pplx_space_sandbox_store::ingest::SuspendedFinalizationCandidate;
use pplx_space_sandbox_store::published_quarantine_candidates::{
    self, PublishedQuarantineCandidate,
};
use pplx_space_sandbox_teardown::direct::observed_publication::{
    AcceptedSandboxPublishRow, DegradedObservedState,
};
use pplx_space_sandbox_teardown::{conditions, direct, observed_status};

use super::{optional_trimmed, parse_sandbox_id, proto_network_policy_to_common, trimmed_required};

#[path = "published_state/input.rs"]
mod input;
use input::PublishedSandboxRow;
#[path = "published_state/published_status.rs"]
mod published_status;
use published_status::PublishedStatus;

const QUARANTINE_RECOVERY_OBSERVATION_HEARTBEAT_SECS: i64 = 60 * 60;

#[derive(Debug, thiserror::Error)]
#[error("invalid {field}: {message}")]
struct PublishedTransitionReceiptError {
    field: pplx_space_sandbox_metrics::publication::StatePublicationField,
    message: String,
}

fn parse_published_transition_receipt(
    state: &PublishedSandboxState,
    status: Option<&PublishedStatus>,
) -> Result<
    Option<SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
    PublishedTransitionReceiptError,
> {
    use pplx_space_sandbox_metrics::publication::StatePublicationField as Field;

    let Some(receipt) = state.transition_receipt.as_ref() else {
        return Ok(None);
    };
    let required = |value: Option<&str>, field| {
        optional_trimmed(value).ok_or_else(|| PublishedTransitionReceiptError {
            field,
            message: "required receipt field is missing or blank".to_string(),
        })
    };
    let id = required(Some(&receipt.id), Field::TransitionReceiptId)?
        .parse()
        .map_err(|error: uuid::Error| PublishedTransitionReceiptError {
            field: Field::TransitionReceiptId,
            message: error.to_string(),
        })?;
    let kind = required(Some(&receipt.kind), Field::TransitionReceiptKind)?
        .parse()
        .map_err(
            |error: pplx_space_sandbox_model::SandboxTransitionReceiptKindParseError| {
                PublishedTransitionReceiptError {
                    field: Field::TransitionReceiptKind,
                    message: error.to_string(),
                }
            },
        )?;
    let target_state: SandboxTransitionTargetState = required(
        Some(&receipt.target_state),
        Field::TransitionReceiptTargetState,
    )?
    .parse()
    .map_err(
        |error: pplx_space_sandbox_model::SandboxTransitionTargetStateParseError| {
            PublishedTransitionReceiptError {
                field: Field::TransitionReceiptTargetState,
                message: error.to_string(),
            }
        },
    )?;
    let execution_generation =
        ExecutionGeneration::new(receipt.execution_generation).map_err(|error| {
            PublishedTransitionReceiptError {
                field: Field::TransitionReceiptExecutionGeneration,
                message: error.to_string(),
            }
        })?;
    let reason_code: Option<SandboxQuarantineReasonCode> = if kind
        == SandboxTransitionReceiptKind::ActivationCompleted
    {
        None
    } else {
        optional_trimmed(Some(&receipt.reason_code))
                .map(|reason_value| match reason_value.parse() {
                    Ok(reason_code) => Ok(reason_code),
                    Err(error) if kind == SandboxTransitionReceiptKind::ActivationRollback => {
                        let fallback = match target_state {
                            SandboxTransitionTargetState::Created => {
                                SandboxQuarantineReasonCode::CreateInterrupted
                            }
                            SandboxTransitionTargetState::Stopped => {
                                SandboxQuarantineReasonCode::ColdActivationInterrupted
                            }
                            SandboxTransitionTargetState::Suspended => {
                                SandboxQuarantineReasonCode::RestoreActivationInterrupted
                            }
                            SandboxTransitionTargetState::Started
                            | SandboxTransitionTargetState::Paused => {
                                return Err(PublishedTransitionReceiptError {
                                    field: Field::TransitionReceiptTargetState,
                                    message: format!(
                                        "activation rollback cannot target {target_state}"
                                    ),
                                });
                            }
                        };
                        let disposition = pplx_space_sandbox_metrics::publication::StatePublicationDisposition::FallbackTransitionReason;
                        pplx_space_sandbox_metrics::publication::degraded(
                            pplx_space_sandbox_metrics::publication::StatePublicationComponent::Api,
                            Field::TransitionReceiptReasonCode,
                            disposition,
                        );
                        tracing::warn!(
                            sandbox_id = %state.sandbox_id,
                            reason_code = reason_value,
                            %error,
                            %disposition,
                            fallback_reason_code = %fallback,
                            "using target-specific fallback for a forward-compatible activation rollback reason"
                        );
                        Ok(fallback)
                    }
                    Err(error) => Err(PublishedTransitionReceiptError {
                        field: Field::TransitionReceiptReasonCode,
                        message: error.to_string(),
                    }),
                })
                .transpose()?
    };
    if !status.is_some_and(|status| status.is(target_state.lifecycle())) {
        let status = status.map(PublishedStatus::as_str).unwrap_or_default();
        return Err(PublishedTransitionReceiptError {
            field: Field::TransitionReceiptTargetState,
            message: format!("receipt target {target_state} disagrees with status {status:?}"),
        });
    }
    if kind == SandboxTransitionReceiptKind::QuarantineRecovery
        && reason_code
            .as_ref()
            .is_some_and(|reason| reason.automatic_recovery_policy().is_none())
    {
        return Err(PublishedTransitionReceiptError {
            field: Field::TransitionReceiptKind,
            message: "quarantine recovery requires a reviewed pre-resume reason".to_string(),
        });
    }
    let activation_attempt_id = receipt
        .activation_attempt_id
        .as_deref()
        .map(|value| {
            value
                .trim()
                .parse()
                .map_err(|error: uuid::Error| PublishedTransitionReceiptError {
                    field: Field::TransitionReceiptActivationAttemptId,
                    message: error.to_string(),
                })
        })
        .transpose()?;
    let receipt = SandboxTransitionReceipt::try_new(
        id,
        kind,
        target_state,
        execution_generation,
        reason_code,
        activation_attempt_id,
    )
    .map_err(|error| PublishedTransitionReceiptError {
        field: match error {
            SandboxTransitionReceiptInvariantError::MissingActivationAttempt => {
                Field::TransitionReceiptActivationAttemptId
            }
            SandboxTransitionReceiptInvariantError::MissingReasonCode
            | SandboxTransitionReceiptInvariantError::UnexpectedReasonCode => {
                Field::TransitionReceiptReasonCode
            }
            SandboxTransitionReceiptInvariantError::InvalidActivationCompletionTarget {
                ..
            }
            | SandboxTransitionReceiptInvariantError::InvalidActivationRollbackTarget { .. }
            | SandboxTransitionReceiptInvariantError::InvalidQuarantineRecoveryTarget { .. } => {
                Field::TransitionReceiptTargetState
            }
        },
        message: error.to_string(),
    })?;
    Ok(Some(receipt))
}

pub(crate) struct ExecutionProjection {
    pub(crate) runtime: String,
    pub(crate) runtime_version: String,
    pub(crate) guest_os: String,
    pub(crate) guest_arch: String,
    pub(crate) state_format_name: Option<String>,
    pub(crate) state_format_version: Option<String>,
    pub(crate) metadata: serde_json::Value,
    pub(crate) components: Vec<ArtifactComponentProjection>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct ArtifactComponentProjection {
    pub(crate) kind: String,
    pub(crate) role_key: String,
    pub(crate) version: Option<String>,
    pub(crate) digest: Option<String>,
    pub(crate) format: Option<String>,
}

#[derive(Clone, Debug)]
struct QuarantineProjectionInput {
    sandbox_id: SandboxId,
    failure_reason: Option<String>,
    source: SandboxQuarantineSource,
    reason_code: SandboxQuarantineReasonCode,
    quarantine_id: Uuid,
    history_id: Uuid,
}

#[derive(Clone, Debug, serde::Serialize)]
struct QuarantineProjectionSqlInput<'a> {
    sandbox_id: SandboxId,
    failure_reason: &'a Option<String>,
    source: &'static str,
    reason_code: &'static str,
    quarantine_id: Uuid,
    history_id: Uuid,
}

impl QuarantineProjectionInput {
    fn sql_input(&self) -> QuarantineProjectionSqlInput<'_> {
        QuarantineProjectionSqlInput {
            sandbox_id: self.sandbox_id,
            failure_reason: &self.failure_reason,
            source: self.source.as_str(),
            reason_code: self.reason_code.as_str(),
            quarantine_id: self.quarantine_id,
            history_id: self.history_id,
        }
    }
}

#[derive(Clone, Debug)]
struct ClaimedQuarantineProjection {
    sandbox_id: SandboxId,
    from_lifecycle: String,
    failure_reason: Option<String>,
    source: SandboxQuarantineSource,
    reason_code: SandboxQuarantineReasonCode,
    quarantine_id: Uuid,
    history_id: Uuid,
}

#[derive(Clone, Debug, serde::Serialize)]
struct ClaimedQuarantineSqlProjection<'a> {
    sandbox_id: SandboxId,
    from_lifecycle: &'a str,
    failure_reason: &'a Option<String>,
    source: &'a str,
    reason_code: &'a str,
    quarantine_id: Uuid,
    history_id: Uuid,
}

impl ClaimedQuarantineProjection {
    fn sql_projection(&self) -> ClaimedQuarantineSqlProjection<'_> {
        ClaimedQuarantineSqlProjection {
            sandbox_id: self.sandbox_id,
            from_lifecycle: &self.from_lifecycle,
            failure_reason: &self.failure_reason,
            source: self.source.as_str(),
            reason_code: self.reason_code.as_str(),
            quarantine_id: self.quarantine_id,
            history_id: self.history_id,
        }
    }
}

fn projection_from_execution(execution: ExecutionMetadata) -> Result<ExecutionProjection, Status> {
    let state_format_name = execution
        .state_format()
        .map(|state_format| state_format.format.to_string());
    let state_format_version = execution
        .state_format()
        .map(|state_format| state_format.version.to_string());
    let components = execution
        .components()
        .iter()
        .map(|component| ArtifactComponentProjection {
            kind: component.kind.as_str().to_string(),
            role_key: component
                .role
                .as_ref()
                .map_or_else(String::new, ToString::to_string),
            version: component.version.as_ref().map(ToString::to_string),
            digest: component.digest.as_ref().map(ToString::to_string),
            format: component.format.as_ref().map(ToString::to_string),
        })
        .collect();
    Ok(ExecutionProjection {
        runtime: execution.runtime().kind.alias().to_string(),
        runtime_version: execution.runtime().version.to_string(),
        guest_os: execution.guest().os.to_string(),
        guest_arch: execution.guest().architecture.to_string(),
        state_format_name,
        state_format_version,
        metadata: serde_json::to_value(&execution)
            .map_err(|error| Status::internal(format!("serialize execution metadata: {error}")))?,
        components,
    })
}

fn execution_projection(
    execution: Option<&pplx_space_execution_common::proto::ExecutionMetadata>,
) -> Result<Option<ExecutionProjection>, Status> {
    let Some(execution) = execution else {
        return Ok(None);
    };
    let execution = ExecutionMetadata::try_from(execution)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    projection_from_execution(execution).map(Some)
}

fn proto_timestamp_to_rfc3339(
    timestamp: &prost_types::Timestamp,
    field: &str,
) -> Result<String, Status> {
    proto_timestamp(timestamp, field)?
        .format(&Rfc3339)
        .map_err(|error| Status::internal(format!("format {field}: {error}")))
}

fn proto_timestamp(
    timestamp: &prost_types::Timestamp,
    field: &str,
) -> Result<time::OffsetDateTime, Status> {
    let nanos = u32::try_from(timestamp.nanos)
        .ok()
        .filter(|nanos| *nanos < 1_000_000_000)
        .ok_or_else(|| Status::invalid_argument(format!("{field} has invalid nanos")))?;
    time::OffsetDateTime::from_unix_timestamp(timestamp.seconds)
        .map_err(|error| Status::invalid_argument(format!("{field} is out of range: {error}")))?
        .replace_nanosecond(nanos)
        .map_err(|error| Status::invalid_argument(format!("{field} is invalid: {error}")))
}

#[derive(Clone, Debug)]
struct QuarantineRecoveryProjection {
    schema_version: u32,
    local_quarantine_id: Uuid,
    observed_owner: QuarantineRecoveryObservedOwner,
    attempt_count: i64,
    next_attempt_at: Option<String>,
}

enum QuarantineRecoveryProjectionState {
    Absent,
    Degraded,
    Observed(QuarantineRecoveryProjection),
}

impl QuarantineRecoveryProjection {
    fn observed_owner_name(&self) -> &'static str {
        match self.observed_owner {
            QuarantineRecoveryObservedOwner::Unspecified => "unspecified",
            QuarantineRecoveryObservedOwner::LegacyRecovery => "legacy_recovery",
        }
    }
}

fn quarantine_recovery_projection(
    state: &PublishedSandboxState,
) -> Result<QuarantineRecoveryProjectionState, Status> {
    let Some(observation) = state.quarantine_recovery.as_ref() else {
        return Ok(QuarantineRecoveryProjectionState::Absent);
    };
    if observation.degraded {
        return Ok(QuarantineRecoveryProjectionState::Degraded);
    };
    if observation.schema_version == 0 {
        return Err(Status::invalid_argument(format!(
            "unsupported quarantine recovery observation schema version {}",
            observation.schema_version
        )));
    }
    let local_quarantine_id = optional_trimmed(Some(&observation.local_quarantine_id))
        .ok_or_else(|| Status::invalid_argument("local_quarantine_id is required"))?
        .parse::<Uuid>()
        .map_err(|error| {
            Status::invalid_argument(format!("invalid local_quarantine_id: {error}"))
        })?;
    // Owner is descriptive evidence, not authority. Preserve the episode and
    // scheduling facts when a newer publisher sends an owner this release does
    // not know; rollout safety is better served by an explicit unspecified
    // owner than by freezing the entire prior observation as degraded.
    let observed_owner = QuarantineRecoveryObservedOwner::try_from(observation.observed_owner)
        .unwrap_or(QuarantineRecoveryObservedOwner::Unspecified);
    let next_attempt_at = observation
        .next_attempt_at
        .as_ref()
        .map(|timestamp| {
            if timestamp.seconds < 0 {
                return Err(Status::invalid_argument(
                    "quarantine_next_attempt_at is negative",
                ));
            }
            proto_timestamp_to_rfc3339(timestamp, "quarantine_next_attempt_at")
        })
        .transpose()?;
    Ok(QuarantineRecoveryProjectionState::Observed(
        QuarantineRecoveryProjection {
            schema_version: observation.schema_version,
            local_quarantine_id,
            observed_owner,
            attempt_count: i64::from(observation.attempt_count),
            next_attempt_at,
        },
    ))
}

/// Execution metadata is required: the node-side senders (spaced and the
/// sbx-mgr durable upload worker) have always populated it, and the former
/// mixed-version backfill forced an expensive template/inventory arch
/// resolution onto every suspend/backup begin. A caller that omits it gets
/// a loud `INVALID_ARGUMENT` instead of a silently backfilled identity.
pub(crate) fn snapshot_execution_metadata(
    execution: Option<&pplx_space_execution_common::proto::ExecutionMetadata>,
) -> Result<pplx_space_execution_spec::SnapshotExecutionMetadata, Status> {
    let execution =
        execution.ok_or_else(|| Status::invalid_argument("execution metadata is required"))?;
    let execution = ExecutionMetadata::try_from(execution)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    pplx_space_execution_spec::SnapshotExecutionMetadata::new(execution)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

/// Longest prefix of an invalid capture host CPU that reaches the log. The
/// value is node-supplied and the begin request isn't authorized yet when it's
/// parsed, so the log carries a bounded prefix, not the whole string.
const MAX_LOGGED_CAPTURE_HOST_CPU_BYTES: usize = 64;

/// Parses the capture host CPU of an artifact backup. The CPU is placement
/// evidence, not part of the snapshot's identity, so a value that isn't a valid
/// label is recorded as unknown rather than failing the backup it rides on.
/// An empty value means unknown too, and isn't worth a warning. A disk artifact
/// holds no vCPU state, so it has no capture CPU whatever the sender says.
pub(crate) fn capture_host_cpu(
    raw: Option<&str>,
    artifact_kind: ArtifactKind,
    sandbox_id: SandboxId,
) -> Option<pplx_space_execution_spec::HostCpu> {
    if artifact_kind == ArtifactKind::Disk {
        return None;
    }
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    pplx_space_execution_spec::HostCpu::parse(raw)
        .inspect_err(|error| {
            let logged = pplx_space_grpc_server::truncate_utf8_for_log(
                raw,
                MAX_LOGGED_CAPTURE_HOST_CPU_BYTES,
            );
            tracing::warn!(
                %sandbox_id,
                capture_host_cpu = ?logged.value,
                truncated = logged.truncated,
                %error,
                "ignoring invalid capture host CPU on artifact backup"
            );
        })
        .ok()
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64, Status> {
    i64::try_from(value)
        .map_err(|error| Status::invalid_argument(format!("{field} exceeds i64::MAX: {error}")))
}

fn u32_to_i32(value: u32, field: &str) -> Result<i32, Status> {
    i32::try_from(value)
        .map_err(|error| Status::invalid_argument(format!("{field} exceeds i32::MAX: {error}")))
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn non_zero_u64_to_i64(value: u64, field: &str) -> Result<Option<i64>, Status> {
    if value == 0 {
        Ok(None)
    } else {
        u64_to_i64(value, field).map(Some)
    }
}

fn optional_u64_to_i64_when(
    include: bool,
    value: Option<u64>,
    field: &str,
) -> Result<Option<i64>, Status> {
    if include {
        value.map(|value| u64_to_i64(value, field)).transpose()
    } else {
        Ok(None)
    }
}

fn optional_u32_to_i32_when(
    include: bool,
    value: Option<u32>,
    field: &str,
) -> Result<Option<i32>, Status> {
    if include {
        value.map(|value| u32_to_i32(value, field)).transpose()
    } else {
        Ok(None)
    }
}

fn network_policy_source_str(source: i32) -> Option<&'static str> {
    match NetworkPolicySource::try_from(source).unwrap_or(NetworkPolicySource::Unspecified) {
        NetworkPolicySource::Template => Some("template"),
        NetworkPolicySource::Override => Some("override"),
        NetworkPolicySource::Unspecified => None,
    }
}

fn template_state_row(
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

fn sandbox_policy_reconcile_refs_for_new_sandboxes(
    sandbox_ids: &[SandboxId],
) -> Vec<(&'static str, String)> {
    let mut refs = Vec::new();
    for sandbox_id in sandbox_ids.iter().collect::<BTreeSet<_>>().into_iter() {
        let sandbox_id = *sandbox_id;
        refs.push((FACET_SANDBOX_DESIRED_STATE, sandbox_id.to_string()));
        refs.push((
            FACET_NETWORK_POLICY,
            NetworkPolicyTarget::sandbox(sandbox_id).as_element_string(),
        ));
    }
    refs
}

fn template_policy_reconcile_refs_for_new_templates(
    templates: &[String],
) -> Vec<(&'static str, String)> {
    templates
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .flat_map(|template| {
            [
                (FACET_TEMPLATE_DESIRED_STATE, template.to_string()),
                (FACET_TEMPLATE_BASE_SNAPSHOT_POLICY, template.to_string()),
                (FACET_NETWORK_POLICY, format!("template:{template}")),
            ]
        })
        .collect()
}

async fn queue_policy_reconcile_refs_for_new_inventory(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    refs: Vec<(&'static str, String)>,
    trigger_reason: ReconcileReason,
) -> Result<(), Status> {
    if refs.is_empty() {
        return Ok(());
    }

    let refs = refs
        .into_iter()
        .map(|(facet, element)| (facet.to_string(), element))
        .collect::<Vec<_>>();
    queue_node_reconcile_refs(&mut *conn, &[node_id], &refs, QueuedRefConflict::RetryNow)
        .await
        .map_err(|err| {
            Status::internal(format!("enqueue first-seen inventory reconcile: {err}"))
        })?;

    enqueue_triggers_tx(&mut *conn, [node_id], trigger_reason)
        .await
        .map_err(|err| Status::internal(format!("enqueue first-seen inventory trigger: {err}")))?;

    Ok(())
}

pub async fn publish_template_states(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    publisher_role: NodeRole,
    states: &[PublishedTemplateState],
) -> Result<u64, Status> {
    let include_template_metadata = publisher_role == NodeRole::Builder;
    let mut absent_templates = Vec::new();
    let mut rows = Vec::new();

    for state in states {
        let template = trimmed_required(&state.template, "template")?;
        if !state.present {
            absent_templates.push(template);
            continue;
        }
        rows.push(template_state_row(state, include_template_metadata)?);
    }

    let mut accepted = 0_u64;
    if !absent_templates.is_empty() {
        let deleted = pplx_space_node_template_inventory_store::delete_node_templates(
            &mut *conn,
            node_id,
            &absent_templates,
        )
        .await
        .map_err(|err| Status::internal(format!("delete absent published template: {err}")))?;
        accepted = accepted.saturating_add(deleted);
        let elements = absent_templates
            .iter()
            .map(|template| format!("template:{template}"))
            .collect::<Vec<_>>();
        observed_status::delete_for_elements(&mut *conn, node_id, FACET_NETWORK_POLICY, &elements)
            .await
            .map_err(|err| {
                Status::internal(format!(
                    "delete absent template network policy facet status: {err}"
                ))
            })?;
    }

    if rows.is_empty() {
        return Ok(accepted);
    }

    let rows_json = Json(serde_json::Value::Array(rows));
    let accepted_rows = pplx_space_node_template_inventory_store::upsert_published_templates(
        &mut *conn,
        node_id,
        &rows_json.0,
    )
    .await
    .map_err(|err| Status::internal(format!("upsert published node template inventory: {err}")))?;

    pplx_space_node_template_inventory_store::delete_unpublished_template_components(
        &mut *conn,
        node_id,
        &rows_json.0,
    )
    .await
    .map_err(|err| Status::internal(format!("replace node template components: {err}")))?;
    pplx_space_node_template_inventory_store::upsert_published_template_components(
        &mut *conn,
        node_id,
        &rows_json.0,
    )
    .await
    .map_err(|err| Status::internal(format!("insert node template components: {err}")))?;

    // Node publication deliberately never writes `template_releases`. Release
    // rows are fleet-shared, and any per-node write to them (even a guarded
    // no-op rewrite) takes row locks that are held until the multi-statement
    // publish transaction commits, convoying every other publisher. Release
    // execution metadata is written once, by `RegisterTemplateRelease`, at
    // registration time.

    let first_seen_templates = accepted_rows
        .iter()
        .filter(|row| row.first_seen)
        .map(|row| row.template_name.clone())
        .collect::<Vec<_>>();
    let first_seen_present_templates = accepted_rows
        .iter()
        .filter(|row| row.first_seen && row.desired_state == "present")
        .map(|row| row.template_name.clone())
        .collect::<Vec<_>>();
    let first_seen_absent_desired_refs = first_seen_templates
        .iter()
        .filter(|template| !first_seen_present_templates.contains(template))
        .map(|template| (FACET_TEMPLATE_DESIRED_STATE, template.clone()))
        .collect::<Vec<_>>();
    let mut first_seen_refs =
        template_policy_reconcile_refs_for_new_templates(&first_seen_present_templates);
    first_seen_refs.extend(first_seen_absent_desired_refs);
    queue_policy_reconcile_refs_for_new_inventory(
        conn,
        node_id,
        first_seen_refs,
        ReconcileReason::NodeTemplateInventoryPublished,
    )
    .await?;

    observed_status::upsert_template_network_policies(
        &mut *conn,
        node_id,
        &rows_json.0,
        FACET_NETWORK_POLICY,
    )
    .await
    .map_err(|err| {
        Status::internal(format!(
            "upsert template network policy facet status: {err}"
        ))
    })?;

    conditions::clear_template_apply_failed(
        &mut *conn,
        node_id,
        &rows_json.0,
        FACET_NETWORK_POLICY,
    )
    .await
    .map_err(|err| {
        Status::internal(format!(
            "clear template network policy apply condition: {err}"
        ))
    })?;

    accepted = accepted.saturating_add(usize_to_u64(accepted_rows.len()));
    Ok(accepted)
}

/// Record on the sandbox event timeline that a node-published status
/// converged the control-plane lifecycle row. Node-side events
/// (`runtime_activated`, `snapshot_restore_completed`, ...) only say the
/// sandbox is healthy on the node; this is the only emitter that knows the
/// `sandboxes.lifecycle` SSOT actually flipped, and via which path
/// (`reason=node_published_state` vs the caller's normal completion). Like
/// the failed-demotion warn log below, this fires before the surrounding
/// publish transaction commits; a failed commit can orphan an event.
fn emit_lifecycle_convergence_events(
    node_id: NodeId,
    sandbox_ids: &[SandboxId],
    event_type: SandboxEventType,
    from_state: Option<&str>,
    to_state: &str,
) {
    for sandbox_id in sandbox_ids {
        let mut details = SandboxEventDetails::default()
            .reason("node_published_state")
            .operation("node_state_publish")
            .trigger(SandboxEventTrigger::System)
            .with_to_state(to_state)
            .source_node_id(node_id.to_string());
        if let Some(from_state) = from_state {
            details = details.with_from_state(from_state);
        }
        SandboxEvent::new(*sandbox_id, event_type, SandboxEventSource::Api).emit_with(details);
    }
}

fn emit_node_published_state_events(
    node_id: NodeId,
    sandboxes: &[(SandboxId, String)],
    event_type: SandboxEventType,
    reason: &'static str,
    to_state: &'static str,
) {
    for (sandbox_id, from_state) in sandboxes {
        SandboxEvent::new(*sandbox_id, event_type, SandboxEventSource::Api).emit_with(
            SandboxEventDetails::default()
                .reason(reason)
                .operation("node_state_publish")
                .trigger(SandboxEventTrigger::System)
                .with_from_state(from_state.as_str())
                .with_to_state(to_state)
                .source_node_id(node_id.to_string()),
        );
    }
}

#[derive(Debug)]
pub struct SandboxPublicationOutcome {
    pub accepted: u64,
    /// Emit only after the node-publication transaction commits successfully.
    pub post_commit_events: Vec<PostCommitSandboxEvent>,
    pub post_commit_state_convergence: Vec<StateConvergenceSample>,
    /// Sandboxes this publication could take only as lifecycle state, from
    /// either shape: the node set `state_degraded`, or an optional field
    /// failed to parse and the richer observation was dropped to keep the
    /// lifecycle. Both mean the node's account of that sandbox is incomplete.
    ///
    /// Reported because a caller may need to refuse work that depends on the
    /// node having described a sandbox fully — the pending-local-backup mirror
    /// declines a report that names no artifact for a sandbox in this set,
    /// since the entry it would have carried may be exactly what the
    /// degradation dropped. Sandboxes whose identity did not parse are absent
    /// here, having no id to record under; they force the census prune to be
    /// skipped instead.
    pub state_only_sandbox_ids: BTreeSet<SandboxId>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StateConvergenceSample {
    pub state: String,
    pub changed_at: time::OffsetDateTime,
}

#[derive(Debug)]
pub struct PostCommitSandboxEvent {
    sandbox_id: SandboxId,
    event_type: SandboxEventType,
    prior_lifecycle: SandboxLifecycle,
    target_lifecycle: SandboxTransitionTargetState,
    reason_code: SandboxQuarantineReasonCode,
    error: Option<String>,
}

impl PostCommitSandboxEvent {
    pub fn emit(self, node_id: NodeId) {
        let mut details = SandboxEventDetails::default()
            .reason(self.reason_code.as_str())
            .operation("node_state_publish")
            .trigger(SandboxEventTrigger::System)
            .with_from_state(self.prior_lifecycle.as_str())
            .with_to_state(self.target_lifecycle.as_str())
            .source_node_id(node_id.to_string());
        if let Some(error) = self.error {
            details = details.error(error);
        }
        SandboxEvent::new(self.sandbox_id, self.event_type, SandboxEventSource::Api)
            .emit_with(details);
    }
}

fn published_spaced_version(
    node_id: NodeId,
    sandbox_id: SandboxId,
    raw: Option<&str>,
) -> Option<SemVer> {
    let raw = optional_trimmed(raw)?;
    match SemVer::parse(&raw) {
        Ok(version) => Some(version),
        Err(error) => {
            tracing::warn!(
                %node_id,
                %sandbox_id,
                spaced_version = %raw,
                error = %error,
                "dropping invalid sandbox spaced version from node publish"
            );
            None
        }
    }
}

fn published_artifact_origin(raw: i32) -> Result<ArtifactOrigin, Status> {
    match pplx_space_sbx_mgr_client::proto::ArtifactOrigin::try_from(raw) {
        Ok(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop) => Ok(ArtifactOrigin::Stop),
        Ok(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend) => {
            Ok(ArtifactOrigin::Suspend)
        }
        Ok(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Snapshot) => {
            Ok(ArtifactOrigin::Snapshot)
        }
        Ok(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Fork) => Ok(ArtifactOrigin::Fork),
        _ => Err(Status::invalid_argument(
            "current_local_artifact_origin must be STOP, SUSPEND, SNAPSHOT, or FORK",
        )),
    }
}

fn failure_episode_input(
    sandbox_id: SandboxId,
    state: &PublishedSandboxState,
    status: Option<&PublishedStatus>,
    state_degraded: bool,
) -> serde_json::Value {
    serde_json::json!({
        "sandbox_id": sandbox_id,
        "published_lifecycle": status
            .and_then(PublishedStatus::lifecycle)
            .map(|lifecycle| lifecycle.as_str()),
        "state_degraded": state_degraded,
        "execution_generation": state
            .execution_generation
            .and_then(|generation| ExecutionGeneration::new(generation).ok())
            .map(ExecutionGeneration::get),
        "pod_uid": optional_trimmed(state.pod_uid.as_deref())
            .and_then(|pod_uid| PodUid::parse(pod_uid).ok())
            .map(|pod_uid| pod_uid.to_string()),
        "activation_attempt_id": optional_trimmed(state.activation_attempt_id.as_deref())
            .and_then(|attempt| attempt.parse::<SandboxActivationAttemptId>().ok())
            .map(|attempt| attempt.to_string()),
    })
}

/// [`publish_sandbox_states_with_retirement`] with abandoned-restore
/// retirement disabled, the setting every replica runs until it's turned on.
pub async fn publish_sandbox_states(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    states: &[PublishedSandboxState],
) -> Result<SandboxPublicationOutcome, Status> {
    publish_sandbox_states_with_retirement(
        conn,
        node_id,
        states,
        pplx_space_sandbox_model::AbandonedRestoreRetirement::Disabled,
    )
    .await
}

/// Publishes a node's sandbox observations.
///
/// Callers that need atomic lifecycle convergence should run this inside a
/// transaction. Node-published suspended projection uses multiple statements
/// over one locked candidate set and relies on transaction rollback on error.
/// `retirement` decides whether a rollback receipt for a restore may retire
/// its execution as `replaced` (see
/// `sandbox_quarantine_store::apply_activation_rollback_candidates`).
pub async fn publish_sandbox_states_with_retirement(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    states: &[PublishedSandboxState],
    retirement: pplx_space_sandbox_model::AbandonedRestoreRetirement,
) -> Result<SandboxPublicationOutcome, Status> {
    publish_sandbox_states_with_census(conn, node_id, states, retirement, &[]).await
}

/// Publish explicit reports and lock the candidates that the caller will
/// prune later in the same transaction. Take one sorted lock batch before
/// writing either set, including when there are no explicit reports.
pub(super) async fn publish_sandbox_states_with_census(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    states: &[PublishedSandboxState],
    retirement: pplx_space_sandbox_model::AbandonedRestoreRetirement,
    census_prune_ids: &[SandboxId],
) -> Result<SandboxPublicationOutcome, Status> {
    let mut states_by_id = BTreeMap::new();
    for state in states {
        let sandbox_id = match parse_sandbox_id(&state.sandbox_id) {
            Ok(sandbox_id) => sandbox_id,
            Err(error) => {
                let disposition = pplx_space_sandbox_metrics::publication::StatePublicationDisposition::OmitSandbox;
                pplx_space_sandbox_metrics::publication::degraded(
                    pplx_space_sandbox_metrics::publication::StatePublicationComponent::Api,
                    pplx_space_sandbox_metrics::publication::StatePublicationField::CoreRow,
                    disposition,
                );
                tracing::warn!(
                    %node_id,
                    sandbox_id = %state.sandbox_id,
                    %error,
                    %disposition,
                    "omitting malformed sandbox identity from node publication"
                );
                continue;
            }
        };
        states_by_id.insert(sandbox_id, state);
    }

    let mut absent_ids = Vec::new();
    let mut observation_lock_ids = census_prune_ids.to_vec();
    let mut rows = Vec::new();
    // Keyed rather than two parallel collections: the ids are reported to
    // callers and the payloads are written to `sandbox_observed_states`, and a
    // third state-only disposition that appended to only one of them would
    // fail silently in whichever half it forgot.
    //
    // The payload array this produces is therefore ordered by sandbox id
    // rather than by publication order, which `jsonb_to_recordset` does not
    // care about.
    //
    // Keying by sandbox id also collapses a request that names one sandbox
    // twice, where a vector would have emitted both and hit the `ON
    // CONFLICT`. Nothing dedupes `request.sandboxes`, so that is a real
    // behaviour change and not merely a consequence of the two dispositions
    // being mutually exclusive. It is the wanted direction: the value here
    // is a diagnostic `(sandbox_id, status)` pair, so collapsing duplicates
    // loses at most one status string, whereas the pending-backup report
    // rejects a duplicated sandbox outright because there collapsing would
    // silently drop one of two *artifacts*.
    let mut state_only: BTreeMap<SandboxId, DegradedObservedState> = BTreeMap::new();
    let mut failure_episode_rows = Vec::new();
    let mut snapshot_rows = Vec::new();
    let mut quarantine_inputs = Vec::new();
    let mut post_commit_events = Vec::new();
    let mut post_commit_state_convergence = Vec::new();
    let mut publishes_network_policy = false;

    for (sandbox_id, state) in states_by_id {
        let sandbox_uuid = sandbox_id;
        observation_lock_ids.push(sandbox_uuid);
        if !state.present {
            absent_ids.push(sandbox_uuid);
            continue;
        }
        let parsed_status = PublishedStatus::parse(state.status.as_deref());
        let status = parsed_status.as_ref();
        if state.state_degraded {
            failure_episode_rows.push(failure_episode_input(sandbox_uuid, state, status, true));
            state_only.insert(
                sandbox_uuid,
                DegradedObservedState {
                    sandbox_id: sandbox_uuid,
                    status: optional_trimmed(state.status.as_deref()),
                },
            );
            continue;
        }
        let transition_receipt = match parse_published_transition_receipt(state, status) {
            Ok(receipt) => receipt,
            Err(error) => {
                let disposition = pplx_space_sandbox_metrics::publication::StatePublicationDisposition::OmitTransitionReceipt;
                pplx_space_sandbox_metrics::publication::degraded(
                    pplx_space_sandbox_metrics::publication::StatePublicationComponent::Api,
                    error.field,
                    disposition,
                );
                tracing::warn!(
                    %node_id,
                    %sandbox_id,
                    field = %error.field,
                    %disposition,
                    %error,
                    "ignoring malformed or forward-incompatible transition receipt without rejecting sandbox state"
                );
                None
            }
        };
        let (quarantine_recovery, quarantine_recovery_degraded) =
            match quarantine_recovery_projection(state) {
                Ok(QuarantineRecoveryProjectionState::Absent) => (None, false),
                Ok(QuarantineRecoveryProjectionState::Degraded) => (None, true),
                Ok(QuarantineRecoveryProjectionState::Observed(observation)) => {
                    (Some(observation), false)
                }
                Err(error) => {
                    let disposition = pplx_space_sandbox_metrics::publication::StatePublicationDisposition::OmitQuarantineRecoveryObservation;
                    pplx_space_sandbox_metrics::publication::degraded(
                    pplx_space_sandbox_metrics::publication::StatePublicationComponent::Api,
                    pplx_space_sandbox_metrics::publication::StatePublicationField::QuarantineRecoveryObservation,
                    disposition,
                );
                    tracing::warn!(
                        %node_id,
                        %sandbox_id,
                        %error,
                        %disposition,
                        "ignoring malformed quarantine recovery observation without rejecting sandbox state"
                    );
                    (None, true)
                }
            };
        let row_count = rows.len();
        let snapshot_count = snapshot_rows.len();
        let quarantine_count = quarantine_inputs.len();
        let mut row_publishes_network_policy = false;
        let parse_result = (|| -> Result<(), Status> {
            if status.is_some_and(|status| status.is(SandboxLifecycle::Quarantined)) {
                let (source, reason_code) = match optional_trimmed(
                    state.quarantine_reason_code.as_deref(),
                ) {
                    Some(value) => match value.parse::<SandboxQuarantineReasonCode>() {
                        Ok(reason_code) => (SandboxQuarantineSource::SbxManager, reason_code),
                        Err(error) => {
                            tracing::warn!(
                                %node_id,
                                %sandbox_id,
                                quarantine_reason_code = %value,
                                %error,
                                "downgrading unknown node quarantine reason without rejecting state publication"
                            );
                            (
                                SandboxQuarantineSource::NodeStatePublish,
                                SandboxQuarantineReasonCode::NodePublishedQuarantined,
                            )
                        }
                    },
                    None => (
                        SandboxQuarantineSource::NodeStatePublish,
                        SandboxQuarantineReasonCode::NodePublishedQuarantined,
                    ),
                };
                quarantine_inputs.push(QuarantineProjectionInput {
                    sandbox_id,
                    failure_reason: optional_trimmed(state.failure_reason.as_deref()),
                    source,
                    reason_code,
                    quarantine_id: Uuid::now_v7(),
                    history_id: Uuid::now_v7(),
                });
            }

            let observed_network_policy = state
                .observed_network_policy
                .as_ref()
                .map(proto_network_policy_to_common)
                .transpose()?;
            let observed_network_policy_generation = non_zero_u64_to_i64(
                state.observed_network_policy_generation,
                "observed_network_policy_generation",
            )?;
            row_publishes_network_policy = observed_network_policy_generation.is_some();
            let execution = execution_projection(state.execution.as_ref())?;
            let spaced_version =
                published_spaced_version(node_id, sandbox_uuid, state.spaced_version.as_deref());
            if let Some(execution) = execution.as_ref()
                && let Some(runtime) = optional_trimmed(state.runtime.as_deref())
            {
                let runtime = RuntimeKind::parse(&runtime)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                let execution_runtime = RuntimeKind::parse(&execution.runtime)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                if runtime != execution_runtime {
                    return Err(Status::invalid_argument(
                        "published sandbox runtime disagrees with execution metadata",
                    ));
                }
            }
            let current_local_artifact = match (
                optional_trimmed(state.current_local_artifact_id.as_deref()),
                state.current_local_artifact_kind,
                state.current_local_artifact_origin,
            ) {
                (None, None, None) => None,
                (Some(raw_id), Some(raw_kind), Some(raw_origin)) => {
                    let artifact_id = raw_id.parse::<LocalArtifactId>().map_err(|error| {
                        Status::invalid_argument(format!(
                            "invalid current_local_artifact_id: {error}"
                        ))
                    })?;
                    let kind = artifact_kind_from_proto(raw_kind).ok_or_else(|| {
                        Status::invalid_argument("current_local_artifact_kind must be DISK or FULL")
                    })?;
                    let origin = published_artifact_origin(raw_origin)?;
                    if transition_receipt.is_some_and(|receipt| {
                        receipt.kind() == SandboxTransitionReceiptKind::QuarantineRecovery
                    }) && (kind != ArtifactKind::Full || origin != ArtifactOrigin::Suspend)
                    {
                        return Err(Status::invalid_argument(
                            "suspended recovery artifact must be a full suspend artifact",
                        ));
                    }
                    Some((artifact_id.as_uuid(), kind, origin))
                }
                _ => {
                    return Err(Status::invalid_argument(
                        "current local artifact identity must be fully present or absent",
                    ));
                }
            };
            let state_changed_at = state
                .state_changed_at
                .as_ref()
                .map(|timestamp| proto_timestamp(timestamp, "state_changed_at"))
                .transpose()?;
            if status.is_none() && state_changed_at.is_some() {
                return Err(Status::invalid_argument("state_changed_at requires status"));
            }
            let execution_generation = state
                .execution_generation
                .map(ExecutionGeneration::new)
                .transpose()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let pod_uid = state
                .pod_uid
                .as_deref()
                .map(PodUid::parse)
                .transpose()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let row = PublishedSandboxRow {
                sandbox_id: sandbox_uuid,
                status: optional_trimmed(state.status.as_deref()),
                published_lifecycle: status.and_then(PublishedStatus::lifecycle),
                template: optional_trimmed(state.template.as_deref()),
                runtime: optional_trimmed(state.runtime.as_deref()).or_else(|| {
                    execution
                        .as_ref()
                        .map(|execution| execution.runtime.clone())
                }),
                spaced_version: spaced_version.as_ref().map(ToString::to_string),
                runtime_version: execution
                    .as_ref()
                    .map(|execution| execution.runtime_version.clone()),
                guest_os: execution
                    .as_ref()
                    .map(|execution| execution.guest_os.clone()),
                guest_arch: execution
                    .as_ref()
                    .map(|execution| execution.guest_arch.clone()),
                state_format_name: execution
                    .as_ref()
                    .and_then(|execution| execution.state_format_name.clone()),
                state_format_version: execution
                    .as_ref()
                    .and_then(|execution| execution.state_format_version.clone()),
                execution_metadata: execution
                    .as_ref()
                    .map(|execution| execution.metadata.clone()),
                execution_components: execution
                    .as_ref()
                    .map(|execution| execution.components.clone())
                    .unwrap_or_default(),
                pid: state
                    .pid
                    .map(|value| u32_to_i32(value, "pid"))
                    .transpose()?,
                namespace: optional_trimmed(state.namespace.as_deref()),
                uptime: optional_trimmed(state.uptime.as_deref()),
                sandbox_owner: optional_trimmed(state.owner.as_deref()),
                sandbox_user: optional_trimmed(state.owner.as_deref()),
                image_hash: optional_trimmed(state.content_hash.as_deref()),
                snapshots_enabled: state.snapshots_enabled,
                pool_template: optional_trimmed(state.pool_template.as_deref()),
                failure_reason: optional_trimmed(state.failure_reason.as_deref()),
                observed_created_at: optional_trimmed(state.created_at.as_deref()),
                last_used_at: state
                    .last_used_at
                    .as_ref()
                    .map(|timestamp| proto_timestamp_to_rfc3339(timestamp, "last_used_at"))
                    .transpose()?,
                last_access_at: state
                    .last_access_at
                    .as_ref()
                    .map(|timestamp| proto_timestamp_to_rfc3339(timestamp, "last_access_at"))
                    .transpose()?,
                last_command_at: state
                    .last_command_at
                    .as_ref()
                    .map(|timestamp| proto_timestamp_to_rfc3339(timestamp, "last_command_at"))
                    .transpose()?,
                next_auto_event_at: state
                    .next_auto_event_at
                    .as_ref()
                    .map(|timestamp| proto_timestamp_to_rfc3339(timestamp, "next_auto_event_at"))
                    .transpose()?,
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
                observed_network_policy_json: observed_network_policy,
                observed_network_policy_generation,
                observed_network_policy_source: network_policy_source_str(
                    state.observed_network_policy_source,
                ),
                observed_network_policy_authority: PolicyAuthority::from_optional_proto(
                    state.observed_network_policy_authority,
                )
                .map(|authority| authority.map(|authority| authority.as_str()))
                .map_err(|error| Status::invalid_argument(error.to_string()))?,
                auto_lifecycle_authority: PolicyAuthority::from_optional_proto(
                    state.auto_lifecycle_authority,
                )
                .map(|authority| authority.map(|authority| authority.as_str()))
                .map_err(|error| Status::invalid_argument(error.to_string()))?,
                quarantine_reason_code: optional_trimmed(state.quarantine_reason_code.as_deref()),
                transition_receipt_id: transition_receipt.map(|receipt| receipt.id().as_uuid()),
                transition_receipt_kind: transition_receipt.map(|receipt| receipt.kind().as_str()),
                transition_receipt_target_state: transition_receipt
                    .map(|receipt| receipt.target_state().as_str()),
                transition_receipt_execution_generation: transition_receipt
                    .map(|receipt| receipt.execution_generation().get()),
                transition_receipt_reason_code: transition_receipt
                    .and_then(|receipt| receipt.reason_code().map(|reason| reason.as_str())),
                transition_receipt_activation_attempt_id: transition_receipt
                    .and_then(|receipt| receipt.activation_attempt_id().map(|id| id.as_uuid())),

                quarantine_recovery_payload: quarantine_recovery.as_ref().map(|observation| {
                    serde_json::json!({
                        "schema_version": observation.schema_version,
                        "local_quarantine_id": observation.local_quarantine_id,
                        "observed_owner": observation.observed_owner_name(),
                        "attempt_count": observation.attempt_count,
                        "next_attempt_at": observation.next_attempt_at.as_deref(),
                    })
                }),
                quarantine_recovery_observation_degraded: quarantine_recovery_degraded,
                transition_receipt_recovery_policy: transition_receipt
                    .and_then(|receipt| receipt.reason_code().copied())
                    .and_then(|reason| reason.automatic_recovery_policy())
                    .map(|policy| policy.to_string()),
                current_local_artifact_id: current_local_artifact.map(|(id, _, _)| id.to_string()),
                current_local_artifact_kind: current_local_artifact
                    .map(|(_, kind, _)| kind.to_string()),
                current_local_artifact_origin: current_local_artifact
                    .map(|(_, _, origin)| origin.to_string()),
                state_changed_at: state_changed_at
                    .map(|changed_at| {
                        changed_at.format(&Rfc3339).map_err(|error| {
                            Status::internal(format!("format state_changed_at: {error}"))
                        })
                    })
                    .transpose()?,
                execution_generation: execution_generation.map(|generation| generation.get()),
                pod_uid: pod_uid.map(|pod_uid| pod_uid.to_string()),
            };
            rows.push(row);
            for snapshot in &state.snapshots {
                snapshot_rows.push(snapshot_json(sandbox_uuid, snapshot)?);
            }
            Ok(())
        })();
        if let Err(error) = parse_result {
            failure_episode_rows.push(failure_episode_input(sandbox_uuid, state, status, true));
            rows.truncate(row_count);
            snapshot_rows.truncate(snapshot_count);
            quarantine_inputs.truncate(quarantine_count);
            let disposition =
                pplx_space_sandbox_metrics::publication::StatePublicationDisposition::StateOnly;
            pplx_space_sandbox_metrics::publication::degraded(
                pplx_space_sandbox_metrics::publication::StatePublicationComponent::Api,
                pplx_space_sandbox_metrics::publication::StatePublicationField::CoreRow,
                disposition,
            );
            tracing::warn!(
                %node_id,
                %sandbox_id,
                %error,
                %disposition,
                "publishing sandbox lifecycle without malformed optional fields"
            );
            state_only.insert(
                sandbox_uuid,
                DegradedObservedState {
                    sandbox_id: sandbox_uuid,
                    status: optional_trimmed(state.status.as_deref()),
                },
            );
        } else {
            failure_episode_rows.push(failure_episode_input(sandbox_uuid, state, status, false));
            publishes_network_policy |= row_publishes_network_policy;
        }
    }

    pplx_space_sandbox_store::orphan::lock_published_sandbox_observations(
        &mut *conn,
        &observation_lock_ids,
    )
    .await
    .map_err(|err| {
        Status::internal(format!(
            "lock published sandbox observation advisory locks: {err}"
        ))
    })?;

    let mut accepted = 0_u64;
    if !absent_ids.is_empty() {
        let deleted = pplx_space_sandbox_teardown::observed_states::delete_absent_on_node(
            &mut *conn,
            node_id,
            &absent_ids,
        )
        .await
        .map_err(|err| {
            Status::internal(format!(
                "delete absent published sandbox observed state: {err}"
            ))
        })?;
        accepted = accepted.saturating_add(deleted);

        let elements = absent_ids
            .iter()
            .map(|id| NetworkPolicyTarget::sandbox(*id).as_element_string())
            .collect::<Vec<_>>();
        observed_status::delete_for_elements(&mut *conn, node_id, FACET_NETWORK_POLICY, &elements)
            .await
            .map_err(|err| {
                Status::internal(format!("delete absent network policy facet status: {err}"))
            })?;

        let sandbox_elements = absent_ids
            .iter()
            .map(SandboxId::to_string)
            .collect::<Vec<_>>();
        observed_status::clear_quarantine_recovery_and_key(
            &mut *conn,
            node_id,
            FACET_SANDBOX_DESIRED_STATE,
            &sandbox_elements,
            SANDBOX_FAILURE_EPISODE_KEY,
        )
        .await
        .map_err(|err| {
            Status::internal(format!("clear absent sandbox facet observations: {err}"))
        })?;

        let deleted_ids = pplx_space_sandbox_store::ingest::delete_published_absent_stopped(
            &mut *conn,
            node_id,
            &absent_ids,
        )
        .await
        .map_err(|err| {
            Status::internal(format!(
                "delete node-published absent stopped sandbox: {err}"
            ))
        })?;
        // This delete predates the sandbox store's delete_with_dependents and
        // keeps its own cleanup statements; the warm-pool receipt retire from
        // that helper applies here for the same reason (no FK, unreachable
        // once the sandbox row is gone).
        let deleted_ids = deleted_ids.into_iter().collect::<Vec<_>>();
        pplx_space_cluster_pool::WarmPoolStore::delete_claimed_slots_for_retired_executions(
            &mut *conn,
            &deleted_ids,
        )
        .await
        .map_err(|err| {
            Status::internal(format!(
                "delete absent stopped sandbox warm-pool receipts: {err}"
            ))
        })?;
    }

    if !state_only.is_empty() {
        let result =
            pplx_space_sandbox_teardown::direct::observed_publication::write_degraded_observed_states(
                &mut *conn,
                node_id,
                &state_only.values().cloned().collect::<Vec<_>>(),
            )
            .await
            .map_err(|err| {
                Status::internal(format!(
                    "write degraded published sandbox observed state: {err}"
                ))
            })?;
        accepted = accepted.saturating_add(usize_to_u64(result.len()));
    }

    let failure_episode_rows = serde_json::Value::Array(failure_episode_rows);
    direct::record_failure_episodes(
        &mut *conn,
        node_id,
        &failure_episode_rows,
        FACET_SANDBOX_DESIRED_STATE,
        direct::FailureEpisodeKeys {
            episode: SANDBOX_FAILURE_EPISODE_KEY,
            execution_generation: SANDBOX_FAILURE_EXECUTION_GENERATION_KEY,
            pod_uid: SANDBOX_FAILURE_POD_UID_KEY,
            activation_attempt_id: SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY,
        },
    )
    .await
    .map_err(|err| Status::internal(format!("write sandbox failure episode: {err}")))?;

    direct::clear_failure_episodes(
        &mut *conn,
        node_id,
        &failure_episode_rows,
        FACET_SANDBOX_DESIRED_STATE,
        SANDBOX_FAILURE_EPISODE_KEY,
    )
    .await
    .map_err(|err| Status::internal(format!("clear sandbox failure episode: {err}")))?;

    if rows.is_empty() {
        return Ok(SandboxPublicationOutcome {
            accepted,
            post_commit_events,
            post_commit_state_convergence,
            state_only_sandbox_ids: state_only.keys().copied().collect(),
        });
    }

    let rows_json = Json(serde_json::to_value(&rows).map_err(|error| {
        Status::internal(format!("serialize published sandbox state: {error}"))
    })?);
    let accepted_rows = write_observed_states(conn, node_id, &rows).await?;

    let accepted_ids = accepted_rows
        .iter()
        .map(|row| row.sandbox_id)
        .collect::<Vec<_>>();
    // Preserve the node's exact local handoff before its backup is durable.
    // This observation authorizes only a bounded wait; the ready-snapshot
    // finalizer below remains the sole publisher of the committed artifact.
    observed_status::record_local_suspend_handoffs(
        &mut *conn,
        node_id,
        &rows_json.0,
        FACET_SANDBOX_DESIRED_STATE,
        SANDBOX_LOCAL_SUSPEND_KEY,
    )
    .await
    .map_err(|err| Status::internal(format!("publish local suspend handoff observation: {err}")))?;

    post_commit_state_convergence.extend(accepted_rows.iter().filter_map(|row| {
        if !row.status_changed {
            return None;
        }
        Some(StateConvergenceSample {
            state: row.published_status.clone()?,
            changed_at: row.state_changed_at?,
        })
    }));
    reconcile_recovery_observations(conn, node_id, &rows).await?;
    update_activity_timestamps(conn, node_id, &rows).await?;
    record_idle_deadlines(conn, node_id, &rows).await?;

    let first_seen_sandbox_ids = accepted_rows
        .iter()
        .filter(|row| row.assigned_to_node && row.first_seen)
        .map(|row| row.sandbox_id)
        .collect::<Vec<_>>();
    let restored_ids = converge_restored_sandboxes(conn, node_id, &rows).await?;
    emit_lifecycle_convergence_events(
        node_id,
        &restored_ids,
        SandboxEventType::RestoreCompleted,
        Some("restoring"),
        "started",
    );

    post_commit_events.extend(apply_activation_receipts(conn, node_id, &rows, retirement).await?);

    let suspended_projection =
        project_node_published_suspended_sandboxes(conn, node_id, &rows).await?;
    let recovered_ids = suspended_projection
        .recoveries
        .iter()
        .map(|(sandbox_id, _, _)| *sandbox_id)
        .collect::<BTreeSet<_>>();
    let suspended_ids = suspended_projection
        .rows
        .iter()
        .map(|(sandbox_id, _)| *sandbox_id)
        .filter(|sandbox_id| !recovered_ids.contains(sandbox_id))
        .collect::<Vec<_>>();
    emit_lifecycle_convergence_events(
        node_id,
        &suspended_ids,
        SandboxEventType::SuspendCompleted,
        None,
        "suspended",
    );
    post_commit_events.extend(suspended_projection.recoveries.iter().map(
        |(sandbox_id, prior_lifecycle, reason_code)| PostCommitSandboxEvent {
            sandbox_id: *sandbox_id,
            event_type: SandboxEventType::QuarantineRecovered,
            prior_lifecycle: *prior_lifecycle,
            target_lifecycle: SandboxTransitionTargetState::Suspended,
            reason_code: *reason_code,
            error: None,
        },
    ));

    let stopped_ids = converge_stopped_sandboxes(conn, node_id, &rows).await?;
    emit_lifecycle_convergence_events(
        node_id,
        &stopped_ids,
        SandboxEventType::StopCompleted,
        None,
        "stopped",
    );

    let failed_sandboxes = converge_failed_sandboxes(conn, node_id, &rows).await?;
    emit_node_published_state_events(
        node_id,
        &failed_sandboxes,
        SandboxEventType::SandboxFailed,
        "node_published_failed_state",
        "failed",
    );
    if !failed_sandboxes.is_empty() {
        tracing::warn!(
            node_id = %node_id,
            sandboxes_marked_failed = failed_sandboxes.len(),
            "node-published failed sandbox state marked control-plane lifecycle failed"
        );
    }

    let mut quarantined_sandboxes = Vec::new();
    if !quarantine_inputs.is_empty() {
        let quarantine_sql_inputs = quarantine_inputs
            .iter()
            .map(QuarantineProjectionInput::sql_input)
            .collect::<Vec<_>>();
        let quarantine_rows_json = Json(
            serde_json::to_value(&quarantine_sql_inputs)
                .map_err(|err| Status::internal(format!("serialize quarantine input: {err}")))?,
        );
        // Unlike failed projection, quarantine is a node-authored hold that
        // preserves local evidence and hot-node affiliation across substrates.
        // Quarantine is a stronger hold than cleanup/delete intent: it stops
        // automated cleanup until a healer or operator decides the next step.
        // Deleted still wins, but direct-managed rows are allowed to publish
        // the same hold state as Kubernetes-managed rows.
        let claimed_rows = published_quarantine_candidates::list(
            &mut *conn,
            node_id,
            quarantine_rows_json.clone(),
        )
        .await
        .map_err(|err| Status::internal(format!("claim quarantined sandboxes: {err}")))?;
        let claimed = claimed_rows
            .into_iter()
            .map(
                |PublishedQuarantineCandidate {
                     id,
                     lifecycle,
                     failure_reason,
                     source,
                     reason_code,
                     quarantine_id,
                     history_id,
                 }|
                 -> Result<_, Status> {
                    Ok(ClaimedQuarantineProjection {
                        sandbox_id: SandboxId::from_uuid(id),
                        from_lifecycle: lifecycle,
                        failure_reason,
                        source: source.parse().map_err(|error| {
                            Status::internal(format!("parse claimed quarantine source: {error}"))
                        })?,
                        reason_code: reason_code.parse().map_err(|error| {
                            Status::internal(format!(
                                "parse claimed quarantine reason code: {error}"
                            ))
                        })?,
                        quarantine_id,
                        history_id,
                    })
                },
            )
            .collect::<Result<Vec<_>, _>>()?;
        if !claimed.is_empty() {
            let claimed_sql = claimed
                .iter()
                .map(ClaimedQuarantineProjection::sql_projection)
                .collect::<Vec<_>>();
            let claimed_json = Json(serde_json::to_value(&claimed_sql).map_err(|err| {
                Status::internal(format!("serialize claimed quarantine input: {err}"))
            })?);
            let updated_ids = pplx_space_sandbox_store::ingest::mark_published_quarantined(
                &mut *conn,
                node_id,
                claimed_json,
            )
            .await
            .map_err(|err| {
                Status::internal(format!("mark quarantined sandbox lifecycle: {err}"))
            })?;
            let updated_ids = updated_ids.into_iter().collect::<BTreeSet<_>>();
            let updated = claimed
                .into_iter()
                .filter(|row| updated_ids.contains(&row.sandbox_id))
                .collect::<Vec<_>>();
            if !updated.is_empty() {
                let updated_sql = updated
                    .iter()
                    .map(ClaimedQuarantineProjection::sql_projection)
                    .collect::<Vec<_>>();
                let updated_json = serde_json::to_value(&updated_sql).map_err(|err| {
                    Status::internal(format!("serialize updated quarantine input: {err}"))
                })?;
                pplx_space_sandbox_quarantine_store::published_quarantine::upsert_states(
                    &mut *conn,
                    node_id,
                    SandboxQuarantineNextAction::RequiresOperatorAttention,
                    updated_json.clone(),
                )
                .await
                .map_err(|err| Status::internal(format!("record quarantine state: {err}")))?;
                pplx_space_sandbox_quarantine_store::published_quarantine::insert_history(
                    &mut *conn,
                    node_id,
                    SandboxQuarantineHistoryAction::EnteredQuarantine,
                    SandboxQuarantineNextAction::RequiresOperatorAttention,
                    updated_json,
                )
                .await
                .map_err(|err| Status::internal(format!("record quarantine history: {err}")))?;
                quarantined_sandboxes = updated
                    .iter()
                    .map(|row| (row.sandbox_id, row.from_lifecycle.clone()))
                    .collect();
            }
        }
    }
    emit_node_published_state_events(
        node_id,
        &quarantined_sandboxes,
        SandboxEventType::SandboxQuarantined,
        "node_published_quarantined_state",
        "quarantined",
    );
    if !quarantined_sandboxes.is_empty() {
        tracing::warn!(
            node_id = %node_id,
            sandboxes_marked_quarantined = quarantined_sandboxes.len(),
            "node-published quarantined sandbox state marked control-plane lifecycle quarantined"
        );
    }

    post_commit_events.extend(recover_pause_failed_sandboxes(conn, node_id, &rows).await?);

    queue_policy_reconcile_refs_for_new_inventory(
        conn,
        node_id,
        sandbox_policy_reconcile_refs_for_new_sandboxes(&first_seen_sandbox_ids),
        ReconcileReason::NodeSandboxInventoryPublished,
    )
    .await?;

    if publishes_network_policy {
        pplx_space_sandbox_teardown::observed_states::record_network_policy_authority(
            &mut *conn,
            node_id,
            &rows_json.0,
        )
        .await
        .map_err(|err| {
            Status::internal(format!(
                "update sandbox observed network policy authority: {err}"
            ))
        })?;

        observed_status::upsert_sandbox_network_policies(
            &mut *conn,
            node_id,
            &rows_json.0,
            FACET_NETWORK_POLICY,
        )
        .await
        .map_err(|err| Status::internal(format!("upsert network policy facet status: {err}")))?;

        conditions::clear_sandbox_apply_failed(
            &mut *conn,
            node_id,
            &rows_json.0,
            FACET_NETWORK_POLICY,
        )
        .await
        .map_err(|err| Status::internal(format!("clear network policy apply condition: {err}")))?;
    }

    publish_sandbox_snapshots_batch(conn, &accepted_ids, snapshot_rows).await?;
    accepted = accepted.saturating_add(usize_to_u64(accepted_ids.len()));
    Ok(SandboxPublicationOutcome {
        accepted,
        post_commit_events,
        post_commit_state_convergence,
        state_only_sandbox_ids: state_only.into_keys().collect(),
    })
}

/// Moves the reporting node's `k8s` rows that it published `failed` to
/// Postgres `public.sandboxes.lifecycle = 'failed'`, returning each moved row
/// with the lifecycle it left.
///
/// The claim locks before it checks, as the suspended projection does. A row a
/// concurrent writer holds is read again at its committed version once the lock
/// is granted, so a `deleting` or `quarantined` committed meanwhile, or a row
/// that left this node, is refused rather than overwritten, and the returned
/// from-state is the lifecycle the update actually replaced. `FOR NO KEY UPDATE`
/// is the mode the `UPDATE` itself takes on the same rows, so taking it in the
/// claim moves the lock earlier in the statement without adding a conflict.
async fn project_node_published_failed_sandboxes(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows_json: Json<serde_json::Value>,
) -> Result<Vec<(SandboxId, String)>, Status> {
    pplx_space_sandbox_store::ingest::mark_published_failed(&mut *conn, node_id, rows_json)
        .await
        .map_err(|err| Status::internal(format!("mark failed sandbox lifecycle: {err}")))
}

#[derive(Default)]
struct SuspendedProjectionOutcome {
    rows: Vec<(SandboxId, bool)>,
    recoveries: Vec<(SandboxId, SandboxLifecycle, SandboxQuarantineReasonCode)>,
}

/// Must be called inside the caller-owned node-publish transaction. Candidate
/// selection, execution retirement, receipt recording, and quarantine removal
/// all share the same row locks and rollback boundary.
async fn project_node_published_suspended_sandboxes(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows: &[PublishedSandboxRow],
) -> Result<SuspendedProjectionOutcome, Status> {
    let suspended_rows = rows
        .iter()
        .filter(|row| row.reports_lifecycle(SandboxLifecycle::Suspended))
        .collect::<Vec<_>>();
    if suspended_rows.is_empty() {
        return Ok(SuspendedProjectionOutcome::default());
    }
    let rows_json = Json(serde_json::to_value(suspended_rows).map_err(|error| {
        Status::internal(format!("serialize suspended publication rows: {error}"))
    })?);
    let candidates = crate::sandbox_quarantine_store::select_suspended_projection_candidates(
        conn, node_id, rows_json,
    )
    .await?;
    if candidates.is_empty() {
        return Ok(SuspendedProjectionOutcome {
            rows: Vec::new(),
            recoveries: Vec::new(),
        });
    }
    let finalization_candidates = candidates
        .iter()
        .map(|candidate| SuspendedFinalizationCandidate {
            sandbox_id: candidate.sandbox_id,
            clear_marker: candidate.clear_marker,
            published_artifact: candidate.published_artifact.clone(),
            published_execution_identity: candidate.published_execution_identity.clone(),
        })
        .collect::<Vec<_>>();
    let rows = apply_suspended_k8s_finalization(conn, &finalization_candidates).await?;
    let projected_ids = rows
        .iter()
        .map(|(sandbox_id, _)| *sandbox_id)
        .collect::<BTreeSet<_>>();
    let mut recoveries = Vec::new();
    for candidate in candidates {
        let Some(recovery) = candidate.recovery else {
            continue;
        };
        if !projected_ids.contains(&candidate.sandbox_id) {
            continue;
        }
        crate::sandbox_quarantine_store::record_quarantine_recovery(
            conn,
            node_id,
            candidate.sandbox_id,
            &recovery,
        )
        .await?;
        recoveries.push((
            candidate.sandbox_id,
            recovery.prior_lifecycle,
            recovery.reason_code,
        ));
    }
    Ok(SuspendedProjectionOutcome { rows, recoveries })
}

pub(crate) async fn project_ready_suspend_snapshot(
    conn: &mut sqlx::PgConnection,
    snapshot_id: SnapshotId,
) -> Result<Option<(SandboxId, NodeId, String)>, Status> {
    let candidates =
        pplx_space_sandbox_store::ready_suspend_projection::lock_candidates(conn, snapshot_id)
            .await
            .map_err(|err| Status::internal(format!("select ready suspend projection: {err}")))?;
    let Some(candidate) = candidates.first().cloned() else {
        return Ok(None);
    };
    let sandbox_id = candidate.sandbox_id;
    let clear_marker = candidate.clear_marker;
    let source_node_id = candidate.source_node_id;
    let from_lifecycle = candidate.lifecycle;

    let projected = apply_suspended_k8s_finalization(
        conn,
        &[SuspendedFinalizationCandidate {
            sandbox_id,
            clear_marker,
            published_artifact: None,
            published_execution_identity: None,
        }],
    )
    .await?;
    if projected
        .iter()
        .any(|(projected_id, _)| *projected_id == sandbox_id)
    {
        Ok(Some((sandbox_id, source_node_id, from_lifecycle)))
    } else {
        Ok(None)
    }
}

async fn apply_suspended_k8s_finalization(
    conn: &mut sqlx::PgConnection,
    candidates: &[SuspendedFinalizationCandidate],
) -> Result<Vec<(SandboxId, bool)>, Status> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let projected = pplx_space_sandbox_store::ingest::finalize_suspended(&mut *conn, candidates)
        .await
        .map_err(|err| Status::internal(format!("finalize suspended sandbox lifecycle: {err}")))?;

    // A committed suspend finalization means the claimed pod was already torn
    // down, so the finalized sandbox's warm-pool slot row (if any) is only a
    // receipt. Retire it in the same transaction; otherwise it lives until
    // the warm-pool reconciler's suspended-fossil drain rediscovers it by
    // scanning the whole receipt backlog.
    let retired_reservations = projected
        .iter()
        .map(|(sandbox_id, _)| *sandbox_id)
        .collect::<Vec<_>>();
    pplx_space_cluster_pool::WarmPoolStore::delete_claimed_slots_for_retired_executions(
        &mut *conn,
        &retired_reservations,
    )
    .await
    .map_err(|err| Status::internal(format!("delete retired warm-pool slot receipts: {err}")))?;

    Ok(projected)
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

fn snapshot_json(
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

async fn publish_sandbox_snapshots_batch(
    conn: &mut sqlx::PgConnection,
    sandbox_ids: &[SandboxId],
    snapshot_rows: Vec<serde_json::Value>,
) -> Result<(), Status> {
    if sandbox_ids.is_empty() {
        return Ok(());
    }

    let snapshot_ids_payload = snapshot_rows
        .iter()
        .map(|row| {
            serde_json::json!({
                "sandbox_id": row.get("sandbox_id").cloned().unwrap_or(serde_json::Value::Null),
                "snapshot_id": row.get("snapshot_id").cloned().unwrap_or(serde_json::Value::Null),
            })
        })
        .collect::<Vec<_>>();
    pplx_space_sandbox_store::snapshot_inventory::prune_unpublished(
        &mut *conn,
        sandbox_ids,
        serde_json::Value::Array(snapshot_ids_payload),
    )
    .await
    .map_err(|err| Status::internal(format!("prune absent published sandbox snapshots: {err}")))?;

    if snapshot_rows.is_empty() {
        return Ok(());
    }

    pplx_space_sandbox_store::snapshot_inventory::upsert_published(
        &mut *conn,
        sandbox_ids,
        serde_json::Value::Array(snapshot_rows),
    )
    .await
    .map_err(|err| Status::internal(format!("upsert published sandbox snapshot: {err}")))?;
    Ok(())
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

/// Apply the node-owned observed-state batch and report accepted rows.
/// The caller retains the publication transaction and subsequent receipt ordering.
async fn write_observed_states(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows: &[PublishedSandboxRow],
) -> Result<Vec<AcceptedSandboxPublishRow>, Status> {
    // Keep the ownership and difference guards with the transition in this transaction.
    let published = rows
        .iter()
        .map(PublishedSandboxRow::observed_state)
        .collect::<Vec<_>>();
    let accepted_rows =
        pplx_space_sandbox_teardown::direct::observed_publication::write_published_observed_states(
            &mut *conn, node_id, &published,
        )
        .await
        .map_err(|err| {
            Status::internal(format!("write published sandbox observed state: {err}"))
        })?;

    let inserted_count = accepted_rows.iter().filter(|row| row.inserted).count();
    let updated_count = accepted_rows.iter().filter(|row| row.updated).count();
    let unchanged_count = accepted_rows
        .len()
        .saturating_sub(inserted_count.saturating_add(updated_count));
    tracing::debug!(
        %node_id,
        accepted = accepted_rows.len(),
        inserted = inserted_count,
        updated = updated_count,
        unchanged = unchanged_count,
        "published sandbox observed state diff applied"
    );
    Ok(accepted_rows)
}

/// Consume matching activation receipts within the publication transaction.
/// Return only the events that become publishable after its caller commits.
async fn apply_activation_receipts(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows: &[PublishedSandboxRow],
    retirement: pplx_space_sandbox_model::AbandonedRestoreRetirement,
) -> Result<Vec<PostCommitSandboxEvent>, Status> {
    // These statements require an exact receipt kind. Empty receipt batches
    // cannot release or lock an execution, so ordinary reports need neither query.
    let activation_completions = rows
        .iter()
        .filter(|row| {
            row.transition_receipt_kind
                == Some(SandboxTransitionReceiptKind::ActivationCompleted.as_str())
        })
        .collect::<Vec<_>>();
    if !activation_completions.is_empty() {
        let receipt_rows = Json(
            serde_json::to_value(activation_completions).map_err(|error| {
                Status::internal(format!("serialize activation completion receipts: {error}"))
            })?,
        );
        crate::sandbox_quarantine_store::consume_activation_completion_receipts(
            conn,
            node_id,
            receipt_rows,
        )
        .await?;
    }

    let activation_rollbacks = rows
        .iter()
        .filter(|row| {
            row.transition_receipt_kind
                == Some(SandboxTransitionReceiptKind::ActivationRollback.as_str())
        })
        .collect::<Vec<_>>();
    let rollback_candidates = if activation_rollbacks.is_empty() {
        Vec::new()
    } else {
        let receipt_rows = Json(serde_json::to_value(activation_rollbacks).map_err(|error| {
            Status::internal(format!("serialize activation rollback receipts: {error}"))
        })?);
        crate::sandbox_quarantine_store::select_activation_rollback_candidates(
            conn,
            node_id,
            receipt_rows,
        )
        .await?
    };
    let applied_rollbacks = crate::sandbox_quarantine_store::apply_activation_rollback_candidates(
        conn,
        node_id,
        rollback_candidates,
        retirement,
    )
    .await?;
    Ok(applied_rollbacks
        .into_iter()
        .map(|rollback| PostCommitSandboxEvent {
            sandbox_id: rollback.sandbox_id,
            event_type: SandboxEventType::ActivationRollbackCompleted,
            prior_lifecycle: rollback.prior_lifecycle,
            target_lifecycle: rollback.target_state,
            reason_code: rollback.reason_code,
            error: rollback.failure_message,
        })
        .collect())
}

/// Recover node-owned pause failures from definitive healthy observations.
async fn recover_pause_failed_sandboxes(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows: &[PublishedSandboxRow],
) -> Result<Vec<PostCommitSandboxEvent>, Status> {
    let healthy_rows = rows
        .iter()
        .filter(|row| {
            row.reports_lifecycle(SandboxLifecycle::Started)
                || row.reports_lifecycle(SandboxLifecycle::Paused)
        })
        .map(PublishedSandboxRow::lifecycle_projection)
        .collect::<Vec<_>>();
    let pause_failed_recoveries = if healthy_rows.is_empty() {
        Vec::new()
    } else {
        let healthy_rows = Json(serde_json::to_value(healthy_rows).map_err(|error| {
            Status::internal(format!("serialize healthy publication rows: {error}"))
        })?);
        crate::sandbox_quarantine_store::recover_pause_failed_from_inventory(
            conn,
            node_id,
            healthy_rows,
        )
        .await?
    };
    Ok(pause_failed_recoveries
        .into_iter()
        .map(|recovery| PostCommitSandboxEvent {
            sandbox_id: recovery.sandbox_id,
            event_type: SandboxEventType::QuarantineRecovered,
            prior_lifecycle: SandboxLifecycle::Quarantined,
            target_lifecycle: recovery.target_state,
            reason_code: SandboxQuarantineReasonCode::PauseFailed,
            error: None,
        })
        .collect())
}

/// Advance the publishing node's activity clocks without changing structural timestamps.
async fn update_activity_timestamps(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows: &[PublishedSandboxRow],
) -> Result<(), Status> {
    let activity_rows = rows
        .iter()
        .map(PublishedSandboxRow::activity_projection)
        .collect::<Vec<_>>();
    let activity_rows = Json(serde_json::to_value(activity_rows).map_err(|error| {
        Status::internal(format!("serialize published activity timestamps: {error}"))
    })?);
    pplx_space_sandbox_store::ingest::advance_published_activity(
        &mut *conn,
        node_id,
        activity_rows,
    )
    .await
    .map_err(|err| Status::internal(format!("update sandbox last_used_at from publish: {err}")))?;
    Ok(())
}

/// Record the publishing node's idle deadlines, which `getSandbox` reports as
/// `expires_at`.
async fn record_idle_deadlines(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows: &[PublishedSandboxRow],
) -> Result<(), Status> {
    let deadline_rows = rows
        .iter()
        .map(PublishedSandboxRow::idle_deadline_projection)
        .collect::<Vec<_>>();
    let deadline_rows = Json(serde_json::to_value(deadline_rows).map_err(|error| {
        Status::internal(format!("serialize published idle deadlines: {error}"))
    })?);
    pplx_space_sandbox_store::ingest::record_published_idle_deadlines(
        &mut *conn,
        node_id,
        deadline_rows,
    )
    .await
    .map_err(|err| Status::internal(format!("record published idle deadlines: {err}")))?;
    Ok(())
}

async fn converge_restored_sandboxes(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows: &[PublishedSandboxRow],
) -> Result<Vec<SandboxId>, Status> {
    let restored_rows = rows
        .iter()
        .filter(|row| {
            row.reports_lifecycle(SandboxLifecycle::Started)
                && row.transition_receipt_kind
                    != Some(SandboxTransitionReceiptKind::ActivationCompleted.as_str())
        })
        .map(PublishedSandboxRow::restore_projection)
        .collect::<Vec<_>>();
    let restored_ids = if restored_rows.is_empty() {
        Vec::new()
    } else {
        let restored_rows = Json(serde_json::to_value(restored_rows).map_err(|error| {
            Status::internal(format!("serialize restored publication rows: {error}"))
        })?);
        pplx_space_sandbox_store::ingest::converge_published_restoring_to_started(
            &mut *conn,
            node_id,
            restored_rows,
        )
        .await
        .map_err(|err| Status::internal(format!("converge restored sandbox lifecycle: {err}")))?
    };
    Ok(restored_ids)
}

async fn converge_stopped_sandboxes(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows: &[PublishedSandboxRow],
) -> Result<Vec<SandboxId>, Status> {
    let stopped_rows = rows
        .iter()
        .filter(|row| row.reports_lifecycle(SandboxLifecycle::Stopped))
        .map(PublishedSandboxRow::lifecycle_projection)
        .collect::<Vec<_>>();
    let stopped_ids = if stopped_rows.is_empty() {
        Vec::new()
    } else {
        let stopped_rows = Json(serde_json::to_value(stopped_rows).map_err(|error| {
            Status::internal(format!("serialize stopped publication rows: {error}"))
        })?);
        pplx_space_sandbox_store::ingest::converge_published_stopped(
            &mut *conn,
            node_id,
            stopped_rows,
        )
        .await
        .map_err(|err| Status::internal(format!("promote stopped sandbox lifecycle: {err}")))?
    };
    Ok(stopped_ids)
}

async fn converge_failed_sandboxes(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows: &[PublishedSandboxRow],
) -> Result<Vec<(SandboxId, String)>, Status> {
    let failed_rows = rows
        .iter()
        .filter(|row| row.reports_lifecycle(SandboxLifecycle::Failed))
        .map(PublishedSandboxRow::lifecycle_projection)
        .collect::<Vec<_>>();
    if failed_rows.is_empty() {
        return Ok(Vec::new());
    }
    let rows_json = Json(serde_json::to_value(failed_rows).map_err(|error| {
        Status::internal(format!("serialize failed publication rows: {error}"))
    })?);
    project_node_published_failed_sandboxes(conn, node_id, rows_json).await
}

/// Reconcile node-scoped recovery evidence while preserving degraded observations.
async fn reconcile_recovery_observations(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows: &[PublishedSandboxRow],
) -> Result<(), Status> {
    let recovery_rows = rows
        .iter()
        .filter(|row| {
            !row.quarantine_recovery_observation_degraded && row.has_recovery_observation()
        })
        .map(PublishedSandboxRow::recovery_observation_projection)
        .collect::<Vec<_>>();
    if !recovery_rows.is_empty() {
        let recovery_rows = Json(serde_json::to_value(recovery_rows).map_err(|error| {
            Status::internal(format!("serialize recovery observations: {error}"))
        })?);
        observed_status::upsert_quarantine_recovery(
            &mut *conn,
            node_id,
            &recovery_rows.0,
            FACET_SANDBOX_DESIRED_STATE,
            QUARANTINE_RECOVERY_OBSERVATION_HEARTBEAT_SECS,
        )
        .await
        .map_err(|err| {
            Status::internal(format!(
                "write published quarantine recovery observation: {err}"
            ))
        })?;
    }
    let clear_recovery_rows = rows
        .iter()
        .filter(|row| {
            !row.quarantine_recovery_observation_degraded && !row.has_recovery_observation()
        })
        .map(PublishedSandboxRow::recovery_observation_projection)
        .collect::<Vec<_>>();
    if !clear_recovery_rows.is_empty() {
        let clear_recovery_rows =
            Json(serde_json::to_value(clear_recovery_rows).map_err(|error| {
                Status::internal(format!(
                    "serialize recovery observation withdrawals: {error}"
                ))
            })?);
        observed_status::clear_quarantine_recovery(
            &mut *conn,
            node_id,
            &clear_recovery_rows.0,
            FACET_SANDBOX_DESIRED_STATE,
        )
        .await
        .map_err(|err| {
            Status::internal(format!(
                "clear published quarantine recovery observation: {err}"
            ))
        })?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "published_state_lifecycle_race_tests.rs"]
mod published_state_lifecycle_race_tests;
#[cfg(test)]
#[path = "published_state_tests.rs"]
mod published_state_tests;
