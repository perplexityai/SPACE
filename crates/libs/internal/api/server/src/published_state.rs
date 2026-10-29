use pplx_space_common_core::types::{NodeId, SnapshotId, SubstrateKind};

use pplx_space_common_core::sql;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use sqlx::types::Json;
use time::format_description::well_known::Rfc3339;
use tonic::Status;
use uuid::Uuid;

use crate::metadata::artifact_kind_from_proto;
use pplx_space_cluster_trigger_queue::enqueue_triggers_conn;
use pplx_space_common_core::types::{
    Arch, ArtifactKind, ArtifactOrigin, AutoLifecyclePolicy, ExecutionGeneration,
    ExecutionMetadata, LocalArtifactId, NodeRole, PodUid, PolicyAuthority, RuntimeKind,
    SandboxActivationAttemptId, SandboxId, SandboxLifecycle, SandboxQuarantineHistoryAction,
    SandboxQuarantineNextAction, SandboxQuarantineReasonCode, SandboxQuarantineSource,
    SandboxTransitionReceipt, SandboxTransitionReceiptInvariantError, SandboxTransitionReceiptKind,
    SandboxTransitionTargetState, SemVer,
};
use pplx_space_common_svc::{
    sql::sandbox_quarantine::{
        quarantine_history_insert_columns, quarantine_state_conflict_assignments,
        quarantine_state_insert_columns,
    },
    types::{
        FACET_NETWORK_POLICY, FACET_SANDBOX_DESIRED_STATE, FACET_TEMPLATE_BASE_SNAPSHOT_POLICY,
        FACET_TEMPLATE_DESIRED_STATE, NetworkPolicyTarget, ReconcileReason,
        SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY, SANDBOX_FAILURE_EPISODE_KEY,
        SANDBOX_FAILURE_EXECUTION_GENERATION_KEY, SANDBOX_FAILURE_POD_UID_KEY,
        SANDBOX_LOCAL_SUSPEND_KEY,
    },
};
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_net_common::proto::NetworkPolicySource;
use pplx_space_sandbox_portability::{
    SuspendedTargetNodeAssignment, node_published_execution_identity_matches,
    publish_clear_or_preserve_current_local_artifact_marker_assignments,
    started_suspend_revert_eligibility, stopped_artifact_durable, suspended_k8s_finalization_sql,
    suspended_projection_lifecycle_eligible,
};
use pplx_space_sbx_mgr_client::proto::{
    QuarantineRecoveryObservedOwner, SnapshotDetail, SnapshotType,
};
use pplx_space_template_mgr_client::proto::SnapshotInfo;

use crate::proto::{PublishedSandboxState, PublishedTemplateState};
use crate::sandbox_quarantine_store;

use super::{optional_trimmed, parse_sandbox_id, proto_network_policy_to_common, trimmed_required};

const NODE_RECONCILE_QUEUE_FIRST_SEEN_ON_CONFLICT: &str = "
ON CONFLICT (node_id, facet, element) DO UPDATE
SET generation = node_reconcile_queue.generation + 1,
    requested_at = EXCLUDED.requested_at,
    next_attempt_at = NOW(),
    attempts = 0,
    last_error = '',
    dead_at = NULL,
    dead_reason = '',
    updated_at = NOW()";

const QUARANTINE_RECOVERY_OBSERVATION_HEARTBEAT_SECS: i64 = 60 * 60;

#[derive(Debug, sqlx::FromRow)]
struct AcceptedSandboxPublishRow {
    sandbox_id: SandboxId,
    assigned_to_node: bool,
    inserted: bool,
    updated: bool,
    first_seen: bool,
    published_status: Option<String>,
    state_changed_at: Option<time::OffsetDateTime>,
    status_changed: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid {field}: {message}")]
struct PublishedTransitionReceiptError {
    field: pplx_space_metrics::sandbox::publication::StatePublicationField,
    message: String,
}

fn parse_published_transition_receipt(
    state: &PublishedSandboxState,
) -> Result<
    Option<SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
    PublishedTransitionReceiptError,
> {
    use pplx_space_metrics::sandbox::publication::StatePublicationField as Field;

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
            |error: pplx_space_common_core::types::SandboxTransitionReceiptKindParseError| {
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
        |error: pplx_space_common_core::types::SandboxTransitionTargetStateParseError| {
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
                        let disposition = pplx_space_metrics::sandbox::publication::StatePublicationDisposition::FallbackTransitionReason;
                        pplx_space_metrics::sandbox::publication::degraded(
                            pplx_space_metrics::sandbox::publication::StatePublicationComponent::Api,
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
    let status = optional_trimmed(state.status.as_deref()).unwrap_or_default();
    if !status.eq_ignore_ascii_case(target_state.as_str()) {
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

struct ObservedStateComparableColumn {
    stored: &'static str,
    incoming: &'static str,
    excluded: &'static str,
}

const OBSERVED_STATE_COMPARABLE_COLUMNS: &[ObservedStateComparableColumn] = &[
    ObservedStateComparableColumn {
        stored: "node_id",
        incoming: "$1",
        excluded: "{source}.node_id",
    },
    observed_state_column("status"),
    observed_state_column("runtime"),
    observed_state_column("spaced_version"),
    observed_state_column("runtime_version"),
    observed_state_column("guest_os"),
    observed_state_column("guest_arch"),
    observed_state_column("state_format_name"),
    observed_state_column("state_format_version"),
    observed_state_column("execution_metadata"),
    observed_state_column("pid"),
    observed_state_column("namespace"),
    observed_state_column("uptime"),
    observed_state_column("sandbox_owner"),
    observed_state_column("sandbox_user"),
    observed_state_column("image_hash"),
    ObservedStateComparableColumn {
        stored: "snapshots_enabled",
        incoming: "COALESCE({source}.snapshots_enabled, FALSE)",
        excluded: "{source}.snapshots_enabled",
    },
    observed_state_column("template"),
    observed_state_column("pool_template"),
    observed_state_column("failure_reason"),
    observed_state_column("observed_created_at"),
    observed_state_column("network_tx_bytes"),
    observed_state_column("network_rx_bytes"),
    ObservedStateComparableColumn {
        stored: "disk_usage_bytes",
        incoming: "COALESCE({source}.disk_usage_bytes, observed_current.disk_usage_bytes)",
        excluded: "COALESCE({source}.disk_usage_bytes, sandbox_observed_states.disk_usage_bytes)",
    },
    observed_state_column("auto_lifecycle_authority"),
    ObservedStateComparableColumn {
        stored: "missing_publish_count",
        incoming: "0",
        excluded: "0",
    },
];

const fn observed_state_column(column: &'static str) -> ObservedStateComparableColumn {
    ObservedStateComparableColumn {
        stored: column,
        incoming: "{source}.{column}",
        excluded: "{source}.{column}",
    }
}

fn observed_state_stored_tuple(source: &str) -> String {
    observed_state_tuple(
        OBSERVED_STATE_COMPARABLE_COLUMNS
            .iter()
            .map(|column| format!("{source}.{}", column.stored)),
    )
}

fn observed_state_incoming_tuple(source: &str) -> String {
    observed_state_tuple(
        OBSERVED_STATE_COMPARABLE_COLUMNS
            .iter()
            .map(|column| render_observed_state_expression(column.incoming, source, column.stored)),
    )
}

fn observed_state_excluded_tuple() -> String {
    observed_state_tuple(
        OBSERVED_STATE_COMPARABLE_COLUMNS.iter().map(|column| {
            render_observed_state_expression(column.excluded, "EXCLUDED", column.stored)
        }),
    )
}

fn observed_state_tuple(expressions: impl Iterator<Item = String>) -> String {
    format!(
        "({})",
        expressions.collect::<Vec<_>>().join(",\n                 ")
    )
}

fn render_observed_state_expression(template: &str, source: &str, column: &str) -> String {
    template
        .replace("{source}", source)
        .replace("{column}", column)
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
) -> Result<pplx_space_common_core::types::SnapshotExecutionMetadata, Status> {
    let execution =
        execution.ok_or_else(|| Status::invalid_argument("execution metadata is required"))?;
    let execution = ExecutionMetadata::try_from(execution)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    pplx_space_common_core::types::SnapshotExecutionMetadata::new(execution)
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
) -> Option<pplx_space_common_core::types::HostCpu> {
    if artifact_kind == ArtifactKind::Disk {
        return None;
    }
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    pplx_space_common_core::types::HostCpu::parse(raw)
        .inspect_err(|error| {
            let logged = pplx_space_common_svc::truncate_utf8_for_log(
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

    let facets = refs
        .iter()
        .map(|(facet, _)| (*facet).to_string())
        .collect::<Vec<_>>();
    let elements = refs
        .iter()
        .map(|(_, element)| element.clone())
        .collect::<Vec<_>>();
    let query = format!(
        "WITH refs AS (
             SELECT DISTINCT facet, element
             FROM UNNEST($2::TEXT[], $3::TEXT[]) AS refs(facet, element)
         )
         INSERT INTO node_reconcile_queue (node_id, facet, element, requested_at, updated_at)
         SELECT $1, facet, element, NOW(), NOW()
         FROM refs
         {NODE_RECONCILE_QUEUE_FIRST_SEEN_ON_CONFLICT}",
    );
    sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id)
        .bind(&facets)
        .bind(&elements)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            Status::internal(format!("enqueue first-seen inventory reconcile: {err}"))
        })?;

    enqueue_triggers_conn(conn, [node_id], trigger_reason)
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
        let result = sqlx::query(sql!(
            "DELETE FROM node_template_inventory
             WHERE node_id = $1 AND template_name = ANY($2)"
        ))
        .bind(node_id)
        .bind(&absent_templates)
        .execute(&mut *conn)
        .await
        .map_err(|err| Status::internal(format!("delete absent published template: {err}")))?;
        accepted = accepted.saturating_add(result.rows_affected());
        let elements = absent_templates
            .iter()
            .map(|template| format!("template:{template}"))
            .collect::<Vec<_>>();
        sqlx::query(sql!(
            "DELETE FROM node_facet_observed_status
             WHERE node_id = $1
               AND facet = $2
               AND element = ANY($3)"
        ))
        .bind(node_id)
        .bind(FACET_NETWORK_POLICY)
        .bind(&elements)
        .execute(&mut *conn)
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
    let accepted_rows = sqlx::query_as::<_, (String, String, bool, bool, String)>(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 content_hash TEXT,
                 runtime TEXT,
                 arch TEXT,
                 runtime_version TEXT,
                 guest_os TEXT,
                 state_format_name TEXT,
                 state_format_version TEXT,
                 execution_metadata JSONB,
                 execution_components JSONB,
                 built_at TEXT,
                 artifact_size_bytes BIGINT,
                 cache_size_bytes BIGINT,
                 has_base_snapshot BOOLEAN,
                 base_snapshot_size_bytes BIGINT,
                 pool_target INTEGER,
                 pool_current INTEGER,
                 pool_pending INTEGER,
                 pool_sandbox_ids JSONB,
                 snapshots JSONB,
                 default_auto_lifecycle_json JSONB,
                 default_auto_lifecycle_authority TEXT,
                 network_policy_authority TEXT,
                 ensure_base_snapshot BOOLEAN,
                 snapshot_vcpu INTEGER,
                 snapshot_memory_mib INTEGER,
                 vcpu INTEGER,
                 memory_mib INTEGER,
                 disk_size_mb INTEGER
             )
         ),
         known_templates AS (
             SELECT name, desired_state
             FROM templates
             WHERE name IN (SELECT template_name FROM input)
         ),
         existing AS (
             SELECT DISTINCT nti.template_name, nti.content_hash
             FROM node_template_inventory nti
             JOIN input
               ON input.template_name = nti.template_name
              AND COALESCE(input.content_hash, '') = nti.content_hash
             WHERE nti.node_id = $1
         ),
         accepted AS (
             SELECT DISTINCT input.template_name,
                             COALESCE(input.content_hash, '') AS content_hash,
                             kt.desired_state
             FROM input
             JOIN known_templates kt ON kt.name = input.template_name
         ),
         upserted AS (
             INSERT INTO node_template_inventory (
                 node_id, template_name, content_hash, runtime, arch, runtime_version, guest_os,
                 state_format_name, state_format_version, execution_metadata, built_at,
                 artifact_size_bytes, cache_size_bytes, has_base_snapshot, base_snapshot_size_bytes,
                 pool_target, pool_current, pool_pending, pool_sandbox_ids, snapshots,
                 default_auto_lifecycle_json, default_auto_lifecycle_authority,
                 network_policy_authority, ensure_base_snapshot, snapshot_vcpu, snapshot_memory_mib,
                 vcpu, memory_mib, disk_size_mb,
                 missing_publish_count, observed_at, updated_at
             )
             SELECT
                 $1,
                 template_name,
                 COALESCE(content_hash, ''),
                 runtime,
                 arch,
                 runtime_version,
                 guest_os,
                 state_format_name,
                 state_format_version,
                 execution_metadata,
                 built_at,
                 artifact_size_bytes,
                 cache_size_bytes,
                 COALESCE(has_base_snapshot, FALSE),
                 base_snapshot_size_bytes,
                 pool_target,
                 COALESCE(pool_current, 0),
                 COALESCE(pool_pending, 0),
                 COALESCE(pool_sandbox_ids, '[]'::jsonb),
                 COALESCE(snapshots, '[]'::jsonb),
                 default_auto_lifecycle_json,
                 default_auto_lifecycle_authority,
                 network_policy_authority,
                 ensure_base_snapshot,
                 snapshot_vcpu,
                 snapshot_memory_mib,
                 vcpu,
                 memory_mib,
                 disk_size_mb,
                 0,
                 NOW(),
                 NOW()
             FROM input
             JOIN known_templates kt ON kt.name = input.template_name
             ON CONFLICT (node_id, template_name, content_hash) DO UPDATE
             SET runtime = EXCLUDED.runtime,
                 arch = EXCLUDED.arch,
                 runtime_version = EXCLUDED.runtime_version,
                 guest_os = EXCLUDED.guest_os,
                 state_format_name = EXCLUDED.state_format_name,
                 state_format_version = EXCLUDED.state_format_version,
                 execution_metadata = EXCLUDED.execution_metadata,
                 built_at = EXCLUDED.built_at,
                 artifact_size_bytes = EXCLUDED.artifact_size_bytes,
                 cache_size_bytes = EXCLUDED.cache_size_bytes,
                 has_base_snapshot = EXCLUDED.has_base_snapshot,
                 base_snapshot_size_bytes = EXCLUDED.base_snapshot_size_bytes,
                 pool_target = EXCLUDED.pool_target,
                 pool_current = EXCLUDED.pool_current,
                 pool_pending = EXCLUDED.pool_pending,
                 pool_sandbox_ids = EXCLUDED.pool_sandbox_ids,
                 snapshots = EXCLUDED.snapshots,
                 default_auto_lifecycle_json = EXCLUDED.default_auto_lifecycle_json,
                 default_auto_lifecycle_authority = EXCLUDED.default_auto_lifecycle_authority,
                 network_policy_authority = EXCLUDED.network_policy_authority,
                 ensure_base_snapshot = EXCLUDED.ensure_base_snapshot,
                 snapshot_vcpu = EXCLUDED.snapshot_vcpu,
                 snapshot_memory_mib = EXCLUDED.snapshot_memory_mib,
                 vcpu = EXCLUDED.vcpu,
                 memory_mib = EXCLUDED.memory_mib,
                 disk_size_mb = EXCLUDED.disk_size_mb,
                 missing_publish_count = 0,
                 observed_at = EXCLUDED.observed_at,
                 updated_at = NOW()
             WHERE (
                 node_template_inventory.runtime,
                 node_template_inventory.arch,
                 node_template_inventory.runtime_version,
                 node_template_inventory.guest_os,
                 node_template_inventory.state_format_name,
                 node_template_inventory.state_format_version,
                 node_template_inventory.execution_metadata,
                 node_template_inventory.built_at,
                 node_template_inventory.artifact_size_bytes,
                 node_template_inventory.cache_size_bytes,
                 node_template_inventory.has_base_snapshot,
                 node_template_inventory.base_snapshot_size_bytes,
                 node_template_inventory.pool_target,
                 node_template_inventory.pool_current,
                 node_template_inventory.pool_pending,
                 node_template_inventory.pool_sandbox_ids,
                 node_template_inventory.snapshots,
                 node_template_inventory.default_auto_lifecycle_json,
                 node_template_inventory.default_auto_lifecycle_authority,
                 node_template_inventory.network_policy_authority,
                 node_template_inventory.ensure_base_snapshot,
                 node_template_inventory.snapshot_vcpu,
                 node_template_inventory.snapshot_memory_mib,
                 node_template_inventory.vcpu,
                 node_template_inventory.memory_mib,
                 node_template_inventory.disk_size_mb,
                 node_template_inventory.missing_publish_count
             ) IS DISTINCT FROM (
                 EXCLUDED.runtime,
                 EXCLUDED.arch,
                 EXCLUDED.runtime_version,
                 EXCLUDED.guest_os,
                 EXCLUDED.state_format_name,
                 EXCLUDED.state_format_version,
                 EXCLUDED.execution_metadata,
                 EXCLUDED.built_at,
                 EXCLUDED.artifact_size_bytes,
                 EXCLUDED.cache_size_bytes,
                 EXCLUDED.has_base_snapshot,
                 EXCLUDED.base_snapshot_size_bytes,
                 EXCLUDED.pool_target,
                 EXCLUDED.pool_current,
                 EXCLUDED.pool_pending,
                 EXCLUDED.pool_sandbox_ids,
                 EXCLUDED.snapshots,
                 EXCLUDED.default_auto_lifecycle_json,
                 EXCLUDED.default_auto_lifecycle_authority,
                 EXCLUDED.network_policy_authority,
                 EXCLUDED.ensure_base_snapshot,
                 EXCLUDED.snapshot_vcpu,
                 EXCLUDED.snapshot_memory_mib,
                 EXCLUDED.vcpu,
                 EXCLUDED.memory_mib,
                 EXCLUDED.disk_size_mb,
                 0
             )
             RETURNING template_name, content_hash
         )
         SELECT accepted.template_name,
                accepted.content_hash,
                EXISTS (
                    SELECT 1
                    FROM upserted
                    WHERE upserted.template_name = accepted.template_name
                      AND upserted.content_hash = accepted.content_hash
                ) AS changed,
                NOT EXISTS (
                    SELECT 1
                    FROM existing
                    WHERE existing.template_name = accepted.template_name
                      AND existing.content_hash = accepted.content_hash
                ) AS first_seen,
                accepted.desired_state
         FROM accepted"
    ))
    .bind(node_id)
    .bind(rows_json.clone())
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| Status::internal(format!("upsert published node template inventory: {err}")))?;

    sqlx::query(sql!(
        "WITH scope AS (
             SELECT DISTINCT input.template_name, COALESCE(input.content_hash, '') AS content_hash
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 content_hash TEXT
             )
         ),
         input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 content_hash TEXT,
                 execution_components JSONB
             )
         ),
         incoming AS (
             SELECT input.template_name, COALESCE(input.content_hash, '') AS content_hash,
                    component.kind, component.role_key, component.version,
                    component.digest, component.format
             FROM input
             JOIN node_template_inventory inventory
               ON inventory.node_id = $1
              AND inventory.template_name = input.template_name
              AND inventory.content_hash = COALESCE(input.content_hash, '')
             CROSS JOIN LATERAL jsonb_to_recordset(
                 COALESCE(input.execution_components, '[]'::jsonb)
             ) AS component(
                 kind TEXT, role_key TEXT, version TEXT, digest TEXT, format TEXT
             )
         )
         DELETE FROM node_template_components component
         USING scope
         WHERE component.node_id = $1
           AND component.template_name = scope.template_name
           AND component.content_hash = scope.content_hash
           AND NOT EXISTS (
               SELECT 1
               FROM incoming
               WHERE incoming.template_name = component.template_name
                 AND incoming.content_hash = component.content_hash
                 AND incoming.kind = component.kind
                 AND incoming.role_key = component.role_key
           )"
    ))
    .bind(node_id)
    .bind(rows_json.clone())
    .execute(&mut *conn)
    .await
    .map_err(|err| Status::internal(format!("replace node template components: {err}")))?;
    sqlx::query(
        sql!("WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 content_hash TEXT,
                 execution_components JSONB
             )
         ),
         incoming AS (
             SELECT $1 AS node_id, input.template_name, COALESCE(input.content_hash, '') AS content_hash,
                    component.kind, component.role_key, component.version,
                    component.digest, component.format
             FROM input
             JOIN node_template_inventory inventory
               ON inventory.node_id = $1
              AND inventory.template_name = input.template_name
              AND inventory.content_hash = COALESCE(input.content_hash, '')
             CROSS JOIN LATERAL jsonb_to_recordset(
                 COALESCE(input.execution_components, '[]'::jsonb)
             ) AS component(
                 kind TEXT, role_key TEXT, version TEXT, digest TEXT, format TEXT
             )
         )
         INSERT INTO node_template_components (
             node_id, template_name, content_hash, kind, role_key, version, digest, format
         )
         SELECT node_id, template_name, content_hash, kind, role_key, version, digest, format
         FROM incoming
         ON CONFLICT (node_id, template_name, content_hash, kind, role_key) DO UPDATE
         SET version = EXCLUDED.version,
             digest = EXCLUDED.digest,
             format = EXCLUDED.format
         WHERE (
             node_template_components.version,
             node_template_components.digest,
             node_template_components.format
         ) IS DISTINCT FROM (
             EXCLUDED.version,
             EXCLUDED.digest,
             EXCLUDED.format
         )"),
    )
    .bind(node_id)
    .bind(rows_json.clone())
    .execute(&mut *conn)
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
        .filter(|(_, _, _, first_seen, _)| *first_seen)
        .map(|(template, _, _, _, _)| template.clone())
        .collect::<Vec<_>>();
    let first_seen_present_templates = accepted_rows
        .iter()
        .filter(|(_, _, _, first_seen, desired_state)| *first_seen && desired_state == "present")
        .map(|(template, _, _, _, _)| template.clone())
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

    sqlx::query(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 network_policy_json JSONB,
                 network_policy_generation BIGINT,
                 network_policy_authority TEXT
             )
         )
         INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_generation, observed_payload,
             applied_source, applied_authority, observed_at, updated_at
         )
         SELECT
             $1,
             $3,
             'template:' || template_name,
             network_policy_generation,
             network_policy_json,
             NULL,
             network_policy_authority,
             NOW(),
             NOW()
         FROM input
         WHERE network_policy_generation IS NOT NULL
           AND EXISTS (
               SELECT 1 FROM node_template_inventory inventory
               WHERE inventory.node_id = $1 AND inventory.template_name = input.template_name
           )
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET observed_generation = EXCLUDED.observed_generation,
             observed_payload = EXCLUDED.observed_payload,
             applied_source = EXCLUDED.applied_source,
             applied_authority = EXCLUDED.applied_authority,
             observed_at = EXCLUDED.observed_at,
             updated_at = NOW()
         WHERE (
             node_facet_observed_status.observed_generation,
             node_facet_observed_status.observed_payload,
             node_facet_observed_status.applied_source,
             node_facet_observed_status.applied_authority
         ) IS DISTINCT FROM (
             EXCLUDED.observed_generation,
             EXCLUDED.observed_payload,
             EXCLUDED.applied_source,
             EXCLUDED.applied_authority
         )"
    ))
    .bind(node_id)
    .bind(rows_json.clone())
    .bind(FACET_NETWORK_POLICY)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        Status::internal(format!(
            "upsert template network policy facet status: {err}"
        ))
    })?;

    sqlx::query(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 network_policy_generation BIGINT
             )
         )
         DELETE FROM node_facet_conditions nfc
         USING input
         WHERE nfc.node_id = $1
           AND nfc.facet = $3
           AND nfc.element = 'template:' || input.template_name
           AND input.network_policy_generation IS NOT NULL
           AND nfc.condition_type = 'ApplyFailed'
           AND EXISTS (
               SELECT 1 FROM node_template_inventory inventory
               WHERE inventory.node_id = $1 AND inventory.template_name = input.template_name
           )"
    ))
    .bind(node_id)
    .bind(rows_json.clone())
    .bind(FACET_NETWORK_POLICY)
    .execute(&mut *conn)
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
    state_degraded: bool,
) -> serde_json::Value {
    serde_json::json!({
        "sandbox_id": sandbox_id,
        "status": optional_trimmed(state.status.as_deref()),
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

/// Deletes the stopped Direct sandboxes a node reported absent, and detaches
/// their snapshots' storage locations.
///
/// The delete reads the node's absence as proof that its local rootfs, a
/// stopped sandbox's only copy, is gone. That doesn't hold for a stopped
/// sandbox released from the node once its pinned artifact was durable, so
/// [`stopped_artifact_durable`] exempts it, the same term that keeps the
/// census prune's `marked_deleted` arm off it.
///
/// The candidates are locked (`FOR UPDATE`) before anything is detached or
/// deleted. Without the lock, a row a concurrent writer moved out of `stopped`
/// after this statement's snapshot was still selected, and the `DELETE`, which
/// rechecks only its join to the CTE after waiting on the row, deleted it.
/// Locking makes Postgres re-evaluate the whole condition against the row that
/// writer committed; only the durability `EXISTS`'s own table scans keep the
/// statement's snapshot.
///
/// Built once: every input is bound.
static DELETE_ABSENT_STOPPED_SANDBOXES: LazyLock<String> = LazyLock::new(|| {
    let released_with_durable_artifact = stopped_artifact_durable("s");
    sql!(
        format!(
            "WITH deleted AS MATERIALIZED (
                 SELECT s.id
                 FROM sandboxes s
                 WHERE s.active_node_id = $1
                   AND s.managed_by = $3
                   AND s.lifecycle = 'stopped'
                   AND s.id = ANY($2)
                   AND NOT {released_with_durable_artifact}
                 FOR UPDATE OF s
             ),
             detached_storage AS (
                 UPDATE snapshot_storage_locations ssl
                 SET snapshot_id = NULL, orphaned_at = NOW()
                 WHERE ssl.snapshot_id IN (
                     SELECT snapshots.id
                     FROM snapshots
                     JOIN deleted ON deleted.id = snapshots.sandbox_id
                 )
                 RETURNING 1
             )
             DELETE FROM sandboxes s
             USING deleted
             WHERE s.id = deleted.id
             RETURNING s.id"
        )
        .as_str()
    )
});

/// Publishes a node's sandbox observations.
///
/// Callers that need atomic lifecycle convergence should run this inside a
/// transaction. Node-published suspended projection uses multiple statements
/// over one locked candidate set and relies on transaction rollback on error.
pub async fn publish_sandbox_states(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    states: &[PublishedSandboxState],
) -> Result<SandboxPublicationOutcome, Status> {
    let mut states_by_id = BTreeMap::new();
    for state in states {
        let sandbox_id = match parse_sandbox_id(&state.sandbox_id) {
            Ok(sandbox_id) => sandbox_id,
            Err(error) => {
                let disposition = pplx_space_metrics::sandbox::publication::StatePublicationDisposition::OmitSandbox;
                pplx_space_metrics::sandbox::publication::degraded(
                    pplx_space_metrics::sandbox::publication::StatePublicationComponent::Api,
                    pplx_space_metrics::sandbox::publication::StatePublicationField::CoreRow,
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
    let mut state_only: BTreeMap<SandboxId, serde_json::Value> = BTreeMap::new();
    let mut failure_episode_rows = Vec::new();
    let mut snapshot_rows = Vec::new();
    let mut quarantine_inputs = Vec::new();
    let mut post_commit_events = Vec::new();
    let mut post_commit_state_convergence = Vec::new();
    let mut publishes_network_policy = false;

    for (sandbox_id, state) in states_by_id {
        let sandbox_uuid = sandbox_id;
        if !state.present {
            absent_ids.push(sandbox_uuid);
            continue;
        }
        if state.state_degraded {
            failure_episode_rows.push(failure_episode_input(sandbox_uuid, state, true));
            state_only.insert(
                sandbox_uuid,
                serde_json::json!({
                    "sandbox_id": sandbox_uuid,
                    "status": optional_trimmed(state.status.as_deref()),
                }),
            );
            continue;
        }
        let transition_receipt = match parse_published_transition_receipt(state) {
            Ok(receipt) => receipt,
            Err(error) => {
                let disposition = pplx_space_metrics::sandbox::publication::StatePublicationDisposition::OmitTransitionReceipt;
                pplx_space_metrics::sandbox::publication::degraded(
                    pplx_space_metrics::sandbox::publication::StatePublicationComponent::Api,
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
                    let disposition = pplx_space_metrics::sandbox::publication::StatePublicationDisposition::OmitQuarantineRecoveryObservation;
                    pplx_space_metrics::sandbox::publication::degraded(
                    pplx_space_metrics::sandbox::publication::StatePublicationComponent::Api,
                    pplx_space_metrics::sandbox::publication::StatePublicationField::QuarantineRecoveryObservation,
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
            if optional_trimmed(state.status.as_deref())
                .is_some_and(|status| status.eq_ignore_ascii_case("quarantined"))
            {
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
            if optional_trimmed(state.status.as_deref()).is_none() && state_changed_at.is_some() {
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
            let mut row = serde_json::json!({
                "sandbox_id": sandbox_uuid,
                "status": optional_trimmed(state.status.as_deref()),
                "template": optional_trimmed(state.template.as_deref()),
                "runtime": optional_trimmed(state.runtime.as_deref())
                    .or_else(|| execution.as_ref().map(|execution| execution.runtime.clone())),
                "spaced_version": spaced_version.as_ref().map(ToString::to_string),
                "runtime_version": execution.as_ref().map(|execution| execution.runtime_version.clone()),
                "guest_os": execution.as_ref().map(|execution| execution.guest_os.clone()),
                "guest_arch": execution.as_ref().map(|execution| execution.guest_arch.clone()),
                "state_format_name": execution.as_ref().and_then(|execution| execution.state_format_name.clone()),
                "state_format_version": execution.as_ref().and_then(|execution| execution.state_format_version.clone()),
                "execution_metadata": execution.as_ref().map(|execution| execution.metadata.clone()),
                "execution_components": execution.as_ref().map(|execution| execution.components.clone()).unwrap_or_default(),
                "pid": state.pid.map(|value| u32_to_i32(value, "pid")).transpose()?,
                "namespace": optional_trimmed(state.namespace.as_deref()),
                "uptime": optional_trimmed(state.uptime.as_deref()),
                "sandbox_owner": optional_trimmed(state.owner.as_deref()),
                "sandbox_user": optional_trimmed(state.owner.as_deref()),
                "image_hash": optional_trimmed(state.content_hash.as_deref()),
                "snapshots_enabled": state.snapshots_enabled,
                "pool_template": optional_trimmed(state.pool_template.as_deref()),
                "failure_reason": optional_trimmed(state.failure_reason.as_deref()),
                "observed_created_at": optional_trimmed(state.created_at.as_deref()),
                "last_used_at": state.last_used_at.as_ref()
                    .map(|timestamp| proto_timestamp_to_rfc3339(timestamp, "last_used_at"))
                    .transpose()?,
                "last_access_at": state.last_access_at.as_ref()
                    .map(|timestamp| proto_timestamp_to_rfc3339(timestamp, "last_access_at"))
                    .transpose()?,
                "last_command_at": state.last_command_at.as_ref()
                    .map(|timestamp| proto_timestamp_to_rfc3339(timestamp, "last_command_at"))
                    .transpose()?,
                "network_tx_bytes": state.network_tx_bytes
                    .map(|value| u64_to_i64(value, "network_tx_bytes"))
                    .transpose()?,
                "network_rx_bytes": state.network_rx_bytes
                    .map(|value| u64_to_i64(value, "network_rx_bytes"))
                    .transpose()?,
                "disk_usage_bytes": state.disk_usage_bytes
                    .map(|value| u64_to_i64(value, "disk_usage_bytes"))
                    .transpose()?,
                "observed_network_policy_json": observed_network_policy,
                "observed_network_policy_generation": observed_network_policy_generation,
                "observed_network_policy_source": network_policy_source_str(
                    state.observed_network_policy_source,
                ),
                "observed_network_policy_authority": PolicyAuthority::from_optional_proto(
                    state.observed_network_policy_authority,
                )
                .map(|authority| authority.map(|authority| authority.as_str()))
                .map_err(|error| Status::invalid_argument(error.to_string()))?,
                "auto_lifecycle_authority": PolicyAuthority::from_optional_proto(
                    state.auto_lifecycle_authority,
                )
                .map(|authority| authority.map(|authority| authority.as_str()))
                .map_err(|error| Status::invalid_argument(error.to_string()))?,
                "quarantine_reason_code": optional_trimmed(state.quarantine_reason_code.as_deref()),
                "transition_receipt_id": transition_receipt.map(|receipt| receipt.id().as_uuid()),
                "transition_receipt_kind": transition_receipt.map(|receipt| receipt.kind().as_str()),
                "transition_receipt_target_state": transition_receipt.map(|receipt| receipt.target_state().as_str()),
                "transition_receipt_execution_generation": transition_receipt
                    .map(|receipt| receipt.execution_generation().get()),
                "transition_receipt_reason_code": transition_receipt
                    .and_then(|receipt| receipt.reason_code().map(|reason| reason.as_str())),
                "transition_receipt_activation_attempt_id": transition_receipt
                    .and_then(|receipt| receipt.activation_attempt_id().map(|id| id.as_uuid())),
            });
            let row_fields = row.as_object_mut().ok_or_else(|| {
                Status::internal("published sandbox state did not serialize as an object")
            })?;
            let quarantine_recovery_payload = quarantine_recovery.as_ref().map(|observation| {
                serde_json::json!({
                    "schema_version": observation.schema_version,
                    "local_quarantine_id": observation.local_quarantine_id,
                    "observed_owner": observation.observed_owner_name(),
                    "attempt_count": observation.attempt_count,
                    "next_attempt_at": observation.next_attempt_at.as_deref(),
                })
            });
            row_fields.insert(
                "quarantine_recovery_payload".to_string(),
                quarantine_recovery_payload.unwrap_or(serde_json::Value::Null),
            );
            row_fields.insert(
                "quarantine_recovery_observation_degraded".to_string(),
                serde_json::Value::Bool(quarantine_recovery_degraded),
            );
            row_fields.insert(
                "transition_receipt_recovery_policy".to_string(),
                transition_receipt
                    .and_then(|receipt| receipt.reason_code().copied())
                    .and_then(|reason| reason.automatic_recovery_policy())
                    .map(|policy| serde_json::Value::String(policy.to_string()))
                    .unwrap_or(serde_json::Value::Null),
            );
            row_fields.insert(
                "current_local_artifact_id".to_string(),
                current_local_artifact
                    .map(|(id, _, _)| serde_json::Value::String(id.to_string()))
                    .unwrap_or(serde_json::Value::Null),
            );
            row_fields.insert(
                "current_local_artifact_kind".to_string(),
                current_local_artifact
                    .map(|(_, kind, _)| serde_json::Value::String(kind.to_string()))
                    .unwrap_or(serde_json::Value::Null),
            );
            row_fields.insert(
                "current_local_artifact_origin".to_string(),
                current_local_artifact
                    .map(|(_, _, origin)| serde_json::Value::String(origin.to_string()))
                    .unwrap_or(serde_json::Value::Null),
            );
            row_fields.insert(
                "state_changed_at".to_string(),
                match state_changed_at {
                    Some(changed_at) => {
                        serde_json::Value::String(changed_at.format(&Rfc3339).map_err(|error| {
                            Status::internal(format!("format state_changed_at: {error}"))
                        })?)
                    }
                    None => serde_json::Value::Null,
                },
            );
            row_fields.insert(
                "execution_generation".to_string(),
                execution_generation
                    .map(|generation| serde_json::Value::from(generation.get()))
                    .unwrap_or(serde_json::Value::Null),
            );
            row_fields.insert(
                "pod_uid".to_string(),
                pod_uid
                    .map(|pod_uid| serde_json::Value::String(pod_uid.to_string()))
                    .unwrap_or(serde_json::Value::Null),
            );
            rows.push(row);
            for snapshot in &state.snapshots {
                snapshot_rows.push(snapshot_json(sandbox_uuid, snapshot)?);
            }
            Ok(())
        })();
        if let Err(error) = parse_result {
            failure_episode_rows.push(failure_episode_input(sandbox_uuid, state, true));
            rows.truncate(row_count);
            snapshot_rows.truncate(snapshot_count);
            quarantine_inputs.truncate(quarantine_count);
            let disposition =
                pplx_space_metrics::sandbox::publication::StatePublicationDisposition::StateOnly;
            pplx_space_metrics::sandbox::publication::degraded(
                pplx_space_metrics::sandbox::publication::StatePublicationComponent::Api,
                pplx_space_metrics::sandbox::publication::StatePublicationField::CoreRow,
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
                serde_json::json!({
                    "sandbox_id": sandbox_uuid,
                    "status": optional_trimmed(state.status.as_deref()),
                }),
            );
        } else {
            failure_episode_rows.push(failure_episode_input(sandbox_uuid, state, false));
            publishes_network_policy |= row_publishes_network_policy;
        }
    }

    let mut accepted = 0_u64;
    if !absent_ids.is_empty() {
        let result = sqlx::query(sql!(
            "DELETE FROM sandbox_observed_states sos
             WHERE sos.node_id = $1
               AND sos.sandbox_id = ANY($2)"
        ))
        .bind(node_id)
        .bind(&absent_ids)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            Status::internal(format!(
                "delete absent published sandbox observed state: {err}"
            ))
        })?;
        accepted = accepted.saturating_add(result.rows_affected());

        let elements = absent_ids
            .iter()
            .map(|id| NetworkPolicyTarget::sandbox(*id).as_element_string())
            .collect::<Vec<_>>();
        sqlx::query(sql!(
            "DELETE FROM node_facet_observed_status
             WHERE node_id = $1
               AND facet = $2
               AND element = ANY($3)"
        ))
        .bind(node_id)
        .bind(FACET_NETWORK_POLICY)
        .bind(&elements)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            Status::internal(format!("delete absent network policy facet status: {err}"))
        })?;

        let sandbox_elements = absent_ids
            .iter()
            .map(SandboxId::to_string)
            .collect::<Vec<_>>();
        sqlx::query(sql!(
            "UPDATE node_facet_observed_status
             SET observed_payload = COALESCE(observed_payload, '{}'::JSONB)
                                        - 'quarantine_recovery'
                                        - $4::TEXT,
                 observed_at = NOW(),
                 updated_at = NOW()
             WHERE node_id = $1
               AND facet = $2
               AND element = ANY($3)
               AND (
                   observed_payload ? 'quarantine_recovery'
                   OR observed_payload ? $4::TEXT
               )"
        ))
        .bind(node_id)
        .bind(FACET_SANDBOX_DESIRED_STATE)
        .bind(&sandbox_elements)
        .bind(SANDBOX_FAILURE_EPISODE_KEY)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            Status::internal(format!("clear absent sandbox facet observations: {err}"))
        })?;

        let deleted_ids = sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(
            DELETE_ABSENT_STOPPED_SANDBOXES.as_str(),
        ))
        .bind(node_id)
        .bind(&absent_ids)
        .bind(SubstrateKind::Space)
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| {
            Status::internal(format!(
                "delete node-published absent stopped sandbox: {err}"
            ))
        })?;
        // This delete predates finalize_sandbox_db_delete_tx and keeps its own
        // cleanup statements; the warm-pool receipt retire from that helper
        // applies here for the same reason (no FK, unreachable once the
        // sandbox row is gone).
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
        let result = sqlx::query_scalar::<_, SandboxId>(sql!(
            "WITH input AS (
                 SELECT *
                 FROM jsonb_to_recordset($2::jsonb) AS input(
                     sandbox_id UUID,
                     status TEXT
                 )
             ),
             publishable AS (
                 SELECT input.*
                 FROM input
                 LEFT JOIN sandboxes s ON s.id = input.sandbox_id
                 LEFT JOIN sandbox_direct_route_identities direct_route
                   ON direct_route.sandbox_id = s.id
                 WHERE s.id IS NULL
                    OR COALESCE(direct_route.current_node_id, s.active_node_id) = $1
             )
             INSERT INTO sandbox_observed_states (
                 sandbox_id, node_id, status, snapshots_enabled,
                 missing_publish_count, observed_at, updated_at
             )
             SELECT sandbox_id, $1, status, FALSE, 0, NOW(), NOW()
             FROM publishable
             ON CONFLICT (sandbox_id) DO UPDATE
             SET node_id = EXCLUDED.node_id,
                 status = COALESCE(EXCLUDED.status, sandbox_observed_states.status),
                 missing_publish_count = 0,
                 observed_at = EXCLUDED.observed_at,
                 updated_at = NOW()
             RETURNING sandbox_id"
        ))
        .bind(node_id)
        .bind(Json(serde_json::Value::Array(
            state_only.values().cloned().collect(),
        )))
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| {
            Status::internal(format!(
                "write degraded published sandbox observed state: {err}"
            ))
        })?;
        accepted = accepted.saturating_add(usize_to_u64(result.len()));
    }

    let failure_episode_rows_json = Json(serde_json::Value::Array(failure_episode_rows));
    sqlx::query(sql!(
        "INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_payload, observed_at, updated_at
         )
         SELECT
             $1,
             $3,
             input.sandbox_id::TEXT,
             jsonb_build_object(
                 $4::TEXT,
                 jsonb_strip_nulls(jsonb_build_object(
                     $5::TEXT, input.execution_generation,
                     $6::TEXT, input.pod_uid,
                     $7::TEXT, input.activation_attempt_id
                 ))
             ),
             NOW(),
             NOW()
         FROM jsonb_to_recordset($2::JSONB) AS input(
             sandbox_id UUID,
             status TEXT,
             state_degraded BOOLEAN,
             execution_generation BIGINT,
             pod_uid TEXT,
             activation_attempt_id TEXT
         )
         JOIN sandboxes s
           ON s.id = input.sandbox_id
         LEFT JOIN sandbox_direct_route_identities direct_route
           ON direct_route.sandbox_id = s.id
         WHERE COALESCE(direct_route.current_node_id, s.active_node_id) = $1
           AND LOWER(TRIM(input.status)) = 'failed'
           AND NOT input.state_degraded
           AND input.execution_generation IS NOT NULL
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET observed_payload =
                 (COALESCE(node_facet_observed_status.observed_payload, '{}'::JSONB)
                     - $4::TEXT)
                 || EXCLUDED.observed_payload,
             observed_at = EXCLUDED.observed_at,
             updated_at = NOW()
         WHERE node_facet_observed_status.observed_payload -> $4::TEXT
               IS DISTINCT FROM EXCLUDED.observed_payload -> $4::TEXT"
    ))
    .bind(node_id)
    .bind(failure_episode_rows_json.clone())
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .bind(SANDBOX_FAILURE_EXECUTION_GENERATION_KEY)
    .bind(SANDBOX_FAILURE_POD_UID_KEY)
    .bind(SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY)
    .execute(&mut *conn)
    .await
    .map_err(|err| Status::internal(format!("write sandbox failure episode: {err}")))?;

    sqlx::query(sql!(
        "UPDATE node_facet_observed_status observed
         SET observed_payload = COALESCE(observed.observed_payload, '{}'::JSONB)
                                    - $4::TEXT,
             observed_at = NOW(),
             updated_at = NOW()
         FROM jsonb_to_recordset($2::JSONB) AS input(
             sandbox_id UUID,
             status TEXT,
             state_degraded BOOLEAN,
             execution_generation BIGINT,
             pod_uid TEXT,
             activation_attempt_id TEXT
         )
         JOIN sandboxes s
           ON s.id = input.sandbox_id
         LEFT JOIN sandbox_direct_route_identities direct_route
           ON direct_route.sandbox_id = s.id
         WHERE COALESCE(direct_route.current_node_id, s.active_node_id) = $1
           AND observed.node_id = $1
           AND observed.facet = $3
           AND observed.element = input.sandbox_id::TEXT
           AND (
               LOWER(TRIM(input.status)) IS DISTINCT FROM 'failed'
               OR (
                   NOT input.state_degraded
                   AND input.execution_generation IS NULL
               )
           )
           AND observed.observed_payload ? $4::TEXT"
    ))
    .bind(node_id)
    .bind(failure_episode_rows_json)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .execute(&mut *conn)
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

    let observed_state_write_sql = format!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 sandbox_id UUID,
                 status TEXT,
                 state_changed_at TIMESTAMPTZ,
                 template TEXT,
                 runtime TEXT,
                 spaced_version TEXT,
                 runtime_version TEXT,
                 guest_os TEXT,
                 guest_arch TEXT,
                 state_format_name TEXT,
                 state_format_version TEXT,
                 execution_metadata JSONB,
                 execution_components JSONB,
                 pid INTEGER,
                 namespace TEXT,
                 uptime TEXT,
                 sandbox_owner TEXT,
                 sandbox_user TEXT,
                 image_hash TEXT,
                 snapshots_enabled BOOLEAN,
                 pool_template TEXT,
                 failure_reason TEXT,
                 observed_created_at TEXT,
                 network_tx_bytes BIGINT,
                 network_rx_bytes BIGINT,
                 disk_usage_bytes BIGINT,
                 observed_network_policy_json JSONB,
                 observed_network_policy_generation BIGINT,
                 observed_network_policy_source TEXT,
                 observed_network_policy_authority TEXT,
                 auto_lifecycle_authority TEXT
             )
         ),
         publishable AS (
             SELECT input.*,
                    s.id IS NOT NULL AS assigned_to_node
             FROM input
             LEFT JOIN sandboxes s ON s.id = input.sandbox_id
             LEFT JOIN sandbox_direct_route_identities direct_route
               ON direct_route.sandbox_id = s.id
             WHERE s.id IS NULL
                OR COALESCE(direct_route.current_node_id, s.active_node_id) = $1
         ),
         observed_current AS MATERIALIZED (
             SELECT sos.*
             FROM sandbox_observed_states sos
             JOIN publishable ON publishable.sandbox_id = sos.sandbox_id
         ),
         existing AS (
             SELECT DISTINCT sandbox_id
             FROM observed_current
             WHERE node_id = $1
         ),
         changed_or_missing AS MATERIALIZED (
             SELECT publishable.*
             FROM publishable
             LEFT JOIN observed_current ON observed_current.sandbox_id = publishable.sandbox_id
             WHERE observed_current.sandbox_id IS NULL
                OR {prefilter_current} IS DISTINCT FROM {prefilter_incoming}
         ),
         upserted AS (
             INSERT INTO sandbox_observed_states (
                 sandbox_id, node_id, status, runtime, spaced_version, runtime_version, guest_os, guest_arch,
                 state_format_name, state_format_version, execution_metadata, pid, namespace, uptime,
                 sandbox_owner, sandbox_user, image_hash,
                 snapshots_enabled, template, pool_template, failure_reason,
                 observed_created_at, network_tx_bytes, network_rx_bytes, disk_usage_bytes,
                 auto_lifecycle_authority,
                 missing_publish_count, observed_at, updated_at
             )
             SELECT
                 changed_or_missing.sandbox_id,
                 $1,
                status,
                runtime,
                spaced_version,
                runtime_version,
                 guest_os,
                 guest_arch,
                 state_format_name,
                 state_format_version,
                 execution_metadata,
                 pid,
                 namespace,
                 uptime,
                 sandbox_owner,
                 sandbox_user,
                 image_hash,
                 COALESCE(snapshots_enabled, FALSE),
                 template,
                 pool_template,
                 failure_reason,
                 observed_created_at,
                network_tx_bytes,
                network_rx_bytes,
                disk_usage_bytes,
                auto_lifecycle_authority,
                0,
                NOW(),
                NOW()
             FROM changed_or_missing
             ON CONFLICT (sandbox_id) DO UPDATE
             SET node_id = EXCLUDED.node_id,
                 status = EXCLUDED.status,
                 runtime = EXCLUDED.runtime,
                 spaced_version = EXCLUDED.spaced_version,
                 runtime_version = EXCLUDED.runtime_version,
                 guest_os = EXCLUDED.guest_os,
                 guest_arch = EXCLUDED.guest_arch,
                 state_format_name = EXCLUDED.state_format_name,
                 state_format_version = EXCLUDED.state_format_version,
                 execution_metadata = EXCLUDED.execution_metadata,
                 pid = EXCLUDED.pid,
                 namespace = EXCLUDED.namespace,
                 uptime = EXCLUDED.uptime,
                 sandbox_owner = EXCLUDED.sandbox_owner,
                 sandbox_user = EXCLUDED.sandbox_user,
                 image_hash = EXCLUDED.image_hash,
                 snapshots_enabled = EXCLUDED.snapshots_enabled,
                 template = EXCLUDED.template,
                 pool_template = EXCLUDED.pool_template,
                 failure_reason = EXCLUDED.failure_reason,
                 observed_created_at = EXCLUDED.observed_created_at,
                 network_tx_bytes = EXCLUDED.network_tx_bytes,
                 network_rx_bytes = EXCLUDED.network_rx_bytes,
                 disk_usage_bytes = COALESCE(
                     EXCLUDED.disk_usage_bytes,
                     sandbox_observed_states.disk_usage_bytes
                 ),
                 auto_lifecycle_authority = EXCLUDED.auto_lifecycle_authority,
                 missing_publish_count = 0,
                 observed_at = EXCLUDED.observed_at,
                 updated_at = NOW()
            WHERE {conflict_current} IS DISTINCT FROM {conflict_excluded}
            RETURNING sandbox_id, (xmax = 0) AS inserted
        )
         SELECT publishable.sandbox_id,
                publishable.assigned_to_node,
                COALESCE(upserted.inserted, FALSE) AS inserted,
                COALESCE(NOT upserted.inserted, FALSE) AS updated,
                NOT EXISTS (
                    SELECT 1
                    FROM existing
                    WHERE existing.sandbox_id = publishable.sandbox_id
                ) AS first_seen,
                publishable.status AS published_status,
                publishable.state_changed_at,
                observed_current.sandbox_id IS NULL
                    OR observed_current.status IS DISTINCT FROM publishable.status
                    AS status_changed
         FROM publishable
         LEFT JOIN upserted ON upserted.sandbox_id = publishable.sandbox_id
         LEFT JOIN observed_current ON observed_current.sandbox_id = publishable.sandbox_id",
        prefilter_current = observed_state_stored_tuple("observed_current"),
        prefilter_incoming = observed_state_incoming_tuple("publishable"),
        conflict_current = observed_state_stored_tuple("sandbox_observed_states"),
        conflict_excluded = observed_state_excluded_tuple(),
    );
    let rows_json = Json(serde_json::Value::Array(rows));
    let accepted_rows = sqlx::query_as::<_, AcceptedSandboxPublishRow>(sqlx::AssertSqlSafe(sql!(
        observed_state_write_sql.as_str(),
    )))
    .bind(node_id)
    .bind(rows_json.clone())
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| Status::internal(format!("write published sandbox observed state: {err}")))?;

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

    let accepted_ids = accepted_rows
        .iter()
        .map(|row| row.sandbox_id)
        .collect::<Vec<_>>();
    // Preserve the node's exact local handoff before its backup is durable.
    // This observation authorizes only a bounded wait; the ready-snapshot
    // finalizer below remains the sole publisher of the committed artifact.
    sqlx::query(sql!(
        "WITH input AS MATERIALIZED (
             SELECT input.sandbox_id, input.execution_generation, input.pod_uid,
                    CASE WHEN LOWER(TRIM(input.status)) = 'suspended'
                               AND input.current_local_artifact_id IS NOT NULL
                               AND input.current_local_artifact_kind = 'full'
                               AND input.current_local_artifact_origin = 'suspend'
                         THEN jsonb_build_object($4::TEXT, jsonb_build_object(
                             'execution_generation', input.execution_generation,
                             'pod_uid', input.pod_uid,
                             'local_artifact_id', input.current_local_artifact_id
                         ))
                         ELSE '{}'::JSONB END AS payload
               FROM jsonb_to_recordset($2::JSONB) AS input(
                   sandbox_id UUID, status TEXT, execution_generation BIGINT,
                   pod_uid TEXT, current_local_artifact_id UUID,
                   current_local_artifact_kind TEXT, current_local_artifact_origin TEXT
               )
              WHERE input.status IS NOT NULL
         ), candidate AS MATERIALIZED (
             -- Only rows that record or clear a handoff reach the joins.
             SELECT input.* FROM input
              WHERE input.payload ? $4::TEXT
                 OR input.sandbox_id::TEXT IN (
                     SELECT prior.element FROM node_facet_observed_status prior
                      WHERE prior.node_id = $1 AND prior.facet = $3
                        AND prior.observed_payload ? $4::TEXT
                 )
         ), observation AS (
             SELECT candidate.sandbox_id, candidate.payload
               FROM candidate
               JOIN sandboxes s ON s.id = candidate.sandbox_id
               JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
              WHERE s.active_node_id = $1
                AND s.managed_by = $5
                AND s.lifecycle = 'started'
                AND e.active AND e.state IN ('running', 'recovering')
                AND e.generation = candidate.execution_generation
                AND e.pod_uid = candidate.pod_uid
         )
         INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_payload, observed_at, updated_at
         )
         SELECT $1, $3, observation.sandbox_id::TEXT, observation.payload, NOW(), NOW()
           FROM observation
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET observed_payload = (COALESCE(node_facet_observed_status.observed_payload, '{}'::JSONB)
                                    - $4::TEXT) || EXCLUDED.observed_payload,
             observed_at = EXCLUDED.observed_at,
             updated_at = NOW()
         WHERE node_facet_observed_status.observed_payload -> $4::TEXT
               IS DISTINCT FROM EXCLUDED.observed_payload -> $4::TEXT"
    ))
    .bind(node_id)
    .bind(rows_json.clone())
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(SANDBOX_LOCAL_SUSPEND_KEY)
    .bind(SubstrateKind::Kubernetes)
    .execute(&mut *conn)
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
    sqlx::query(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 sandbox_id UUID,
                 quarantine_recovery_payload JSONB,
                 quarantine_recovery_observation_degraded BOOLEAN
             )
         ),
         publishable AS (
             SELECT input.*
             FROM input
             WHERE NOT COALESCE(input.quarantine_recovery_observation_degraded, FALSE)
               AND input.quarantine_recovery_payload IS NOT NULL
         )
         INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_payload, observed_at, updated_at
         )
         SELECT
             $1,
             $3,
             publishable.sandbox_id::TEXT,
             jsonb_build_object(
                 'quarantine_recovery',
                 publishable.quarantine_recovery_payload
             ),
             NOW(),
             NOW()
         FROM publishable
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET observed_payload =
                 COALESCE(node_facet_observed_status.observed_payload, '{}'::JSONB)
                 || EXCLUDED.observed_payload,
             observed_at = EXCLUDED.observed_at,
             updated_at = NOW()
         WHERE node_facet_observed_status.observed_payload -> 'quarantine_recovery'
               IS DISTINCT FROM EXCLUDED.observed_payload -> 'quarantine_recovery'
            OR node_facet_observed_status.observed_at
               < NOW() - make_interval(secs => $4::BIGINT)"
    ))
    .bind(node_id)
    .bind(rows_json.clone())
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(QUARANTINE_RECOVERY_OBSERVATION_HEARTBEAT_SECS)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        Status::internal(format!(
            "write published quarantine recovery observation: {err}"
        ))
    })?;
    sqlx::query(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 sandbox_id UUID,
                 quarantine_recovery_payload JSONB,
                 quarantine_recovery_observation_degraded BOOLEAN
             )
         ),
         clearable AS (
             SELECT input.sandbox_id
             FROM input
             WHERE NOT COALESCE(input.quarantine_recovery_observation_degraded, FALSE)
               AND input.quarantine_recovery_payload IS NULL
         )
         UPDATE node_facet_observed_status observed
         SET observed_payload = COALESCE(observed.observed_payload, '{}'::JSONB)
                                    - 'quarantine_recovery',
             observed_at = NOW(),
             updated_at = NOW()
         FROM clearable
         WHERE observed.node_id = $1
           AND observed.facet = $3
           AND observed.element = clearable.sandbox_id::TEXT
           AND observed.observed_payload ? 'quarantine_recovery'"
    ))
    .bind(node_id)
    .bind(rows_json.clone())
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        Status::internal(format!(
            "clear published quarantine recovery observation: {err}"
        ))
    })?;
    sqlx::query(sql!(
        "WITH input AS (
         SELECT *
         FROM jsonb_to_recordset($2::jsonb) AS input(
             sandbox_id UUID,
             last_used_at TIMESTAMPTZ,
             last_access_at TIMESTAMPTZ,
             last_command_at TIMESTAMPTZ
         )
     ),
     activity AS (
         SELECT input.sandbox_id,
                input.last_access_at,
                input.last_command_at,
                (
                    SELECT MAX(value)
                    FROM (
                        VALUES
                            (input.last_used_at),
                            (input.last_access_at),
                            (input.last_command_at)
                    ) AS timestamps(value)
                ) AS last_used_at
         FROM input
     ),
     publishable AS (
         SELECT activity.*
         FROM activity
         JOIN sandboxes s ON s.id = activity.sandbox_id
         WHERE s.active_node_id = $1
           AND activity.last_used_at IS NOT NULL
     )
     UPDATE sandboxes s
     SET last_access_at = CASE
             WHEN publishable.last_access_at IS NOT NULL
              AND (
                  s.last_access_at IS NULL
                  OR s.last_access_at < publishable.last_access_at - INTERVAL '1 minute'
              )
             THEN publishable.last_access_at
             ELSE s.last_access_at
         END,
         last_command_at = CASE
             WHEN publishable.last_command_at IS NOT NULL
              AND (
                  s.last_command_at IS NULL
                  OR s.last_command_at < publishable.last_command_at - INTERVAL '1 minute'
              )
             THEN publishable.last_command_at
             ELSE s.last_command_at
         END,
         last_used_at = CASE
             WHEN publishable.last_used_at IS NOT NULL
              AND (
                  s.last_used_at IS NULL
                  OR s.last_used_at < publishable.last_used_at - INTERVAL '1 minute'
              )
             THEN publishable.last_used_at
             ELSE s.last_used_at
         END
     FROM publishable
     WHERE s.id = publishable.sandbox_id
       AND (
           (
               publishable.last_access_at IS NOT NULL
               AND (
                   s.last_access_at IS NULL
                   OR s.last_access_at < publishable.last_access_at - INTERVAL '1 minute'
               )
           )
           OR (
               publishable.last_command_at IS NOT NULL
               AND (
                   s.last_command_at IS NULL
                   OR s.last_command_at < publishable.last_command_at - INTERVAL '1 minute'
               )
           )
           OR (
               publishable.last_used_at IS NOT NULL
               AND (
                   s.last_used_at IS NULL
                   OR s.last_used_at < publishable.last_used_at - INTERVAL '1 minute'
               )
           )
       )"
    ))
    .bind(node_id)
    .bind(rows_json.clone())
    .execute(&mut *conn)
    .await
    .map_err(|err| Status::internal(format!("update sandbox last_used_at from publish: {err}")))?;

    let first_seen_sandbox_ids = accepted_rows
        .iter()
        .filter(|row| row.assigned_to_node && row.first_seen)
        .map(|row| row.sandbox_id)
        .collect::<Vec<_>>();
    // Publishing `started` proves the node-local runtime finished, but for
    // Kubernetes sandboxes the control plane also needs a routable active
    // execution row. The blocking wake/restore path is what hydrates that row
    // with `running` + pod IP; if it timed out first, keep the row in
    // `restoring` so the next connect can wait on and hydrate the in-flight
    // pod instead of treating it as a started sandbox with a broken execution.
    // Evacuation-marked rows are owned by the node-drain redrive (the same
    // exemption the sweeper makes) and are left alone.
    let routable_execution =
        pplx_space_common_svc::sql::sandbox_routability::routable_execution_or_non_k8s("s", "$4");
    let restored_convergence_query = format!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 sandbox_id UUID,
                 status TEXT,
                 transition_receipt_kind TEXT
             )
         )
         UPDATE sandboxes s
         SET lifecycle = 'started'
         FROM input
         WHERE s.id = input.sandbox_id
           AND s.active_node_id = $1
           AND s.evacuation_restore_target IS NULL
           AND LOWER(TRIM(input.status)) = 'started'
           AND input.transition_receipt_kind IS DISTINCT FROM $3
           AND LOWER(TRIM(s.lifecycle)) = 'restoring'
           AND {routable_execution}
         RETURNING s.id"
    );
    let restored_ids = sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(
        restored_convergence_query.as_str()
    )))
    .bind(node_id)
    .bind(rows_json.clone())
    .bind(SandboxTransitionReceiptKind::ActivationCompleted.as_str())
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| Status::internal(format!("converge restored sandbox lifecycle: {err}")))?;
    emit_lifecycle_convergence_events(
        node_id,
        &restored_ids,
        SandboxEventType::RestoreCompleted,
        Some("restoring"),
        "started",
    );

    crate::sandbox_quarantine_store::consume_activation_completion_receipts(
        conn,
        node_id,
        rows_json.clone(),
    )
    .await?;

    let rollback_candidates =
        crate::sandbox_quarantine_store::select_activation_rollback_candidates(
            conn,
            node_id,
            rows_json.clone(),
        )
        .await?;
    let applied_rollbacks = crate::sandbox_quarantine_store::apply_activation_rollback_candidates(
        conn,
        node_id,
        rollback_candidates,
    )
    .await?;
    post_commit_events.extend(applied_rollbacks.into_iter().map(|rollback| {
        PostCommitSandboxEvent {
            sandbox_id: rollback.sandbox_id,
            event_type: SandboxEventType::ActivationRollbackCompleted,
            prior_lifecycle: rollback.prior_lifecycle,
            target_lifecycle: rollback.target_state,
            reason_code: rollback.reason_code,
            error: rollback.failure_message,
        }
    }));

    let suspended_projection =
        project_node_published_suspended_sandboxes(conn, node_id, rows_json.clone()).await?;
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

    let stopped_ids = sqlx::query_scalar::<_, SandboxId>(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 sandbox_id UUID,
                 status TEXT
             )
         )
         UPDATE sandboxes s
         SET lifecycle = 'stopped'
         FROM input
         WHERE s.id = input.sandbox_id
           AND s.active_node_id = $1
           AND LOWER(TRIM(input.status)) = 'stopped'
           AND LOWER(TRIM(s.lifecycle)) IN (
               'started',
               'paused',
               'suspended',
               'stopping',
               'restoring'
           )
         RETURNING s.id"
    ))
    .bind(node_id)
    .bind(rows_json.clone())
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| Status::internal(format!("promote stopped sandbox lifecycle: {err}")))?;
    emit_lifecycle_convergence_events(
        node_id,
        &stopped_ids,
        SandboxEventType::StopCompleted,
        None,
        "stopped",
    );

    let failed_sandboxes =
        project_node_published_failed_sandboxes(conn, node_id, rows_json.clone()).await?;
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
        let claimed_rows =
            sqlx::query_as::<_, (Uuid, String, Option<String>, String, String, Uuid, Uuid)>(sql!(
                "SELECT
                     s.id,
                     s.lifecycle,
                     input.failure_reason,
                     input.source,
                     input.reason_code,
                     input.quarantine_id,
                     input.history_id
                 FROM sandboxes s
                 JOIN jsonb_to_recordset($2::jsonb) AS input(
                     sandbox_id UUID,
                     failure_reason TEXT,
                     source TEXT,
                     reason_code TEXT,
                     quarantine_id UUID,
                     history_id UUID
                 ) ON input.sandbox_id = s.id
                 WHERE s.active_node_id = $1
                   AND LOWER(TRIM(s.lifecycle)) NOT IN ('quarantined', 'deleted')"
            ))
            .bind(node_id)
            .bind(quarantine_rows_json.clone())
            .fetch_all(&mut *conn)
            .await
            .map_err(|err| Status::internal(format!("claim quarantined sandboxes: {err}")))?;
        let claimed = claimed_rows
            .into_iter()
            .map(
                |(
                    sandbox_id,
                    from_lifecycle,
                    failure_reason,
                    source,
                    reason_code,
                    quarantine_id,
                    history_id,
                )|
                 -> Result<_, Status> {
                    Ok(ClaimedQuarantineProjection {
                        sandbox_id: SandboxId::from_uuid(sandbox_id),
                        from_lifecycle,
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
            let updated_ids = sqlx::query_scalar::<_, SandboxId>(sql!(
                "UPDATE sandboxes s
                 SET lifecycle = 'quarantined'
                 FROM jsonb_to_recordset($1::jsonb) AS input(
                     sandbox_id UUID,
                     from_lifecycle TEXT
                 )
                 WHERE s.id = input.sandbox_id
                   AND s.active_node_id = $2
                   AND s.lifecycle = input.from_lifecycle
                 RETURNING s.id"
            ))
            .bind(claimed_json.clone())
            .bind(node_id)
            .fetch_all(&mut *conn)
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
                let updated_json = Json(serde_json::to_value(&updated_sql).map_err(|err| {
                    Status::internal(format!("serialize updated quarantine input: {err}"))
                })?);
                let state_columns = quarantine_state_insert_columns();
                let state_conflict_assignments = quarantine_state_conflict_assignments();
                let query = format!(
                    "INSERT INTO sandbox_quarantine_state ({state_columns})
                     SELECT
                         input.sandbox_id,
                         input.quarantine_id,
                         NOW(),
                         NOW(),
                         input.from_lifecycle,
                         input.source,
                         input.reason_code,
                         NULLIF(BTRIM(input.failure_reason), ''),
                         jsonb_build_object('node_id', $3::TEXT),
                         $2,
                         $1
                     FROM jsonb_to_recordset($4::jsonb) AS input(
                         sandbox_id UUID,
                         from_lifecycle TEXT,
                         failure_reason TEXT,
                         source TEXT,
                         reason_code TEXT,
                         quarantine_id UUID
                     )
                     ON CONFLICT (sandbox_id) DO UPDATE SET {state_conflict_assignments}",
                );
                sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
                    .bind(node_id)
                    .bind(SandboxQuarantineNextAction::RequiresOperatorAttention.as_str())
                    .bind(node_id.to_string())
                    .bind(updated_json.clone())
                    .execute(&mut *conn)
                    .await
                    .map_err(|err| Status::internal(format!("record quarantine state: {err}")))?;
                let history_columns = quarantine_history_insert_columns();
                let query = format!(
                    "INSERT INTO sandbox_quarantine_history ({history_columns})
                     SELECT
                         input.history_id,
                         input.quarantine_id,
                         input.sandbox_id,
                         NOW(),
                         input.source,
                         $2,
                         input.reason_code,
                         NULLIF(BTRIM(input.failure_reason), ''),
                         $3,
                         jsonb_build_object('node_id', $1::TEXT)
                     FROM jsonb_to_recordset($4::jsonb) AS input(
                         sandbox_id UUID,
                         failure_reason TEXT,
                         source TEXT,
                         reason_code TEXT,
                         quarantine_id UUID,
                         history_id UUID
                     )",
                );
                sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
                    .bind(node_id.to_string())
                    .bind(SandboxQuarantineHistoryAction::EnteredQuarantine.as_str())
                    .bind(SandboxQuarantineNextAction::RequiresOperatorAttention.as_str())
                    .bind(updated_json)
                    .execute(&mut *conn)
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

    let pause_failed_recoveries =
        crate::sandbox_quarantine_store::recover_pause_failed_from_inventory(
            conn,
            node_id,
            rows_json.clone(),
        )
        .await?;
    post_commit_events.extend(pause_failed_recoveries.into_iter().map(|recovery| {
        PostCommitSandboxEvent {
            sandbox_id: recovery.sandbox_id,
            event_type: SandboxEventType::QuarantineRecovered,
            prior_lifecycle: SandboxLifecycle::Quarantined,
            target_lifecycle: recovery.target_state,
            reason_code: SandboxQuarantineReasonCode::PauseFailed,
            error: None,
        }
    }));

    queue_policy_reconcile_refs_for_new_inventory(
        conn,
        node_id,
        sandbox_policy_reconcile_refs_for_new_sandboxes(&first_seen_sandbox_ids),
        ReconcileReason::NodeSandboxInventoryPublished,
    )
    .await?;

    if publishes_network_policy {
        sqlx::query(
            sql!("WITH input AS (
                 SELECT *
                 FROM jsonb_to_recordset($2::jsonb) AS input(
                     sandbox_id UUID,
                     observed_network_policy_generation BIGINT,
                     observed_network_policy_authority TEXT
                 )
             ),
             publishable AS (
                 SELECT input.*
                 FROM input
                 LEFT JOIN sandboxes s ON s.id = input.sandbox_id
                 WHERE (s.id IS NULL OR s.active_node_id = $1)
                   AND input.observed_network_policy_generation IS NOT NULL
             )
             UPDATE sandbox_observed_states sos
             SET network_policy_authority = publishable.observed_network_policy_authority
             FROM publishable
             WHERE sos.sandbox_id = publishable.sandbox_id
               AND sos.node_id = $1
               AND sos.network_policy_authority IS DISTINCT FROM publishable.observed_network_policy_authority"),
        )
        .bind(node_id)
        .bind(rows_json.clone())
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            Status::internal(format!("update sandbox observed network policy authority: {err}"))
        })?;

        sqlx::query(sql!(
            "WITH input AS (
                 SELECT *
                 FROM jsonb_to_recordset($2::jsonb) AS input(
                     sandbox_id UUID,
                     observed_network_policy_json JSONB,
                     observed_network_policy_generation BIGINT,
                     observed_network_policy_source TEXT,
                     observed_network_policy_authority TEXT
                 )
             ),
             publishable AS (
                 SELECT input.*
                 FROM input
                 LEFT JOIN sandboxes s ON s.id = input.sandbox_id
                 WHERE s.id IS NULL
                    OR s.active_node_id = $1
             )
             INSERT INTO node_facet_observed_status (
                 node_id, facet, element, observed_generation, observed_payload,
                 applied_source, applied_authority, observed_at, updated_at
             )
             SELECT
                 $1,
                 $3,
                 'sandbox:' || sandbox_id::text,
                 observed_network_policy_generation,
                 observed_network_policy_json,
                 observed_network_policy_source,
                 observed_network_policy_authority,
                 NOW(),
                 NOW()
             FROM publishable
             WHERE observed_network_policy_generation IS NOT NULL
             ON CONFLICT (node_id, facet, element) DO UPDATE
             SET observed_generation = EXCLUDED.observed_generation,
                 observed_payload = EXCLUDED.observed_payload,
                 applied_source = EXCLUDED.applied_source,
                 applied_authority = EXCLUDED.applied_authority,
                 observed_at = EXCLUDED.observed_at,
                 updated_at = NOW()
             WHERE (
                 node_facet_observed_status.observed_generation,
                 node_facet_observed_status.observed_payload,
                 node_facet_observed_status.applied_source,
                 node_facet_observed_status.applied_authority
             ) IS DISTINCT FROM (
                 EXCLUDED.observed_generation,
                 EXCLUDED.observed_payload,
                 EXCLUDED.applied_source,
                 EXCLUDED.applied_authority
             )"
        ))
        .bind(node_id)
        .bind(rows_json.clone())
        .bind(FACET_NETWORK_POLICY)
        .execute(&mut *conn)
        .await
        .map_err(|err| Status::internal(format!("upsert network policy facet status: {err}")))?;

        sqlx::query(sql!(
            "WITH input AS (
                 SELECT *
                 FROM jsonb_to_recordset($2::jsonb) AS input(
                     sandbox_id UUID,
                     observed_network_policy_generation BIGINT
                 )
             ),
             publishable AS (
                 SELECT input.*
                 FROM input
                 LEFT JOIN sandboxes s ON s.id = input.sandbox_id
                 WHERE (s.id IS NULL OR s.active_node_id = $1)
                   AND input.observed_network_policy_generation IS NOT NULL
             )
             DELETE FROM node_facet_conditions nfc
             USING publishable
             WHERE nfc.node_id = $1
               AND nfc.facet = $3
               AND nfc.element = 'sandbox:' || publishable.sandbox_id::text
               AND nfc.condition_type = 'ApplyFailed'"
        ))
        .bind(node_id)
        .bind(rows_json.clone())
        .bind(FACET_NETWORK_POLICY)
        .execute(&mut *conn)
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
    let failed_sandboxes = sqlx::query_as::<_, (Uuid, String)>(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 sandbox_id UUID,
                 status TEXT
             )
         ),
         claimed AS (
             SELECT s.id, s.lifecycle
             FROM sandboxes s
             JOIN input ON input.sandbox_id = s.id
             WHERE s.active_node_id = $1
               AND s.managed_by = $3
               AND LOWER(TRIM(input.status)) = 'failed'
               AND LOWER(TRIM(s.lifecycle)) NOT IN (
                   'failed',
                   'quarantined',
                   'deleted',
                   'cleanup_required',
                   'deleting'
               )
             FOR NO KEY UPDATE OF s
         )
         UPDATE sandboxes s
         SET lifecycle = 'failed'
         FROM claimed
         WHERE s.id = claimed.id
         RETURNING s.id, claimed.lifecycle"
    ))
    .bind(node_id)
    .bind(rows_json)
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| Status::internal(format!("mark failed sandbox lifecycle: {err}")))?;
    Ok(failed_sandboxes
        .into_iter()
        .map(|(sandbox_id, lifecycle)| (SandboxId::from_uuid(sandbox_id), lifecycle))
        .collect())
}

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
    rows_json: Json<serde_json::Value>,
) -> Result<SuspendedProjectionOutcome, Status> {
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
    let eligibility = started_suspend_revert_eligibility("s");
    let clear_marker = eligibility.clear_marker;
    let eligible = eligibility.eligible;
    let committed_suspend_evidence = "
        s.current_local_artifact_id = sn.local_artifact_id
        AND s.current_local_artifact_kind = sn.artifact_kind
        AND s.current_local_artifact_origin = 'suspend'
    ";
    let lifecycle_eligible =
        suspended_projection_lifecycle_eligible("s", committed_suspend_evidence);
    let candidate_query = format!(
        "SELECT s.id,
                {clear_marker} AS clear_marker,
                sn.source_node_id,
                s.lifecycle
           FROM snapshots sn
           JOIN sandboxes s ON s.id = sn.sandbox_id
          WHERE sn.id = $1
            AND sn.state = 'ready'
            AND sn.type = 'full'
            AND (sn.artifact_origin IS NULL OR sn.artifact_origin = 'suspend')
            AND sn.source_node_id IS NOT NULL
            AND s.active_node_id = sn.source_node_id
            AND {lifecycle_eligible}
            AND EXISTS (
                SELECT 1
                  FROM snapshot_storage_locations ssl
                 WHERE ssl.snapshot_id = sn.id
            )
            AND {eligible}
          FOR UPDATE OF s",
    );
    let candidates: Vec<(SandboxId, bool, NodeId, String)> =
        sqlx::query_as(sqlx::AssertSqlSafe(sql!(candidate_query.as_str())))
            .bind(snapshot_id)
            .fetch_all(&mut *conn)
            .await
            .map_err(|err| Status::internal(format!("select ready suspend projection: {err}")))?;
    let Some((sandbox_id, clear_marker, source_node_id, from_lifecycle)) =
        candidates.first().cloned()
    else {
        return Ok(None);
    };

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

struct SuspendedFinalizationCandidate {
    sandbox_id: SandboxId,
    clear_marker: bool,
    published_artifact: Option<sandbox_quarantine_store::PublishedSuspendArtifact>,
    published_execution_identity: Option<sandbox_quarantine_store::PublishedExecutionIdentity>,
}

async fn apply_suspended_k8s_finalization(
    conn: &mut sqlx::PgConnection,
    candidates: &[SuspendedFinalizationCandidate],
) -> Result<Vec<(SandboxId, bool)>, Status> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let artifact_marker_assignments =
        publish_clear_or_preserve_current_local_artifact_marker_assignments(
            "s",
            "candidate.published_artifact_id",
            "candidate.published_artifact_kind",
            "candidate.published_artifact_origin",
            "candidate.clear_marker",
        );
    let finalization = suspended_k8s_finalization_sql(
        "s",
        "candidate",
        &artifact_marker_assignments,
        SuspendedTargetNodeAssignment::PreserveOrBackfillActive,
    );
    let candidate_ids = candidates
        .iter()
        .map(|candidate| candidate.sandbox_id)
        .collect::<Vec<_>>();
    let clear_markers = candidates
        .iter()
        .map(|candidate| candidate.clear_marker)
        .collect::<Vec<_>>();
    let published_artifact_ids = candidates
        .iter()
        .map(|candidate| {
            candidate
                .published_artifact
                .as_ref()
                .map(|artifact| artifact.id.as_uuid())
        })
        .collect::<Vec<_>>();
    let published_artifact_kinds = candidates
        .iter()
        .map(|candidate| {
            candidate
                .published_artifact
                .as_ref()
                .map(|artifact| artifact.kind.as_str())
        })
        .collect::<Vec<_>>();
    let published_artifact_origins = candidates
        .iter()
        .map(|candidate| {
            candidate
                .published_artifact
                .as_ref()
                .map(|artifact| artifact.origin.as_str())
        })
        .collect::<Vec<_>>();
    let published_execution_generations = candidates
        .iter()
        .map(|candidate| {
            candidate
                .published_execution_identity
                .as_ref()
                .map(|identity| identity.generation.get())
        })
        .collect::<Vec<_>>();
    let published_pod_uids = candidates
        .iter()
        .map(|candidate| {
            candidate
                .published_execution_identity
                .as_ref()
                .map(|identity| identity.pod_uid.as_str())
        })
        .collect::<Vec<_>>();
    let published_pod_uids_were_missing = candidates
        .iter()
        .map(|candidate| {
            candidate
                .published_execution_identity
                .as_ref()
                .map(|identity| identity.pod_uid_was_missing)
        })
        .collect::<Vec<_>>();
    let candidate_cte = "candidate AS (
             SELECT *
               FROM UNNEST(
                   $1::UUID[], $2::BOOL[], $3::UUID[], $4::TEXT[], $5::TEXT[],
                   $6::BIGINT[], $7::TEXT[], $8::BOOL[]
               ) AS candidate(
                   id, clear_marker, published_artifact_id,
                   published_artifact_kind, published_artifact_origin,
                   published_execution_generation, published_pod_uid,
                   published_pod_uid_was_missing
               )
         )";
    let exact_execution_identity = node_published_execution_identity_matches(
        "e",
        "candidate.published_execution_generation",
        "candidate.published_pod_uid",
        "candidate.published_pod_uid_was_missing",
    );
    let finalization_query = format!(
        "WITH {candidate_cte},
         retired_execution AS (
             UPDATE sandbox_k8s_executions e
                SET {active_execution_assignments}
               FROM candidate
              WHERE e.sandbox_id = candidate.id
                AND e.active
                AND (
                    candidate.published_artifact_id IS NULL
                    OR ({exact_execution_identity})
                )
             RETURNING e.sandbox_id
         ),
         eligible_candidate AS (
             SELECT candidate.*
               FROM candidate
              WHERE candidate.published_artifact_id IS NULL
                 OR EXISTS (
                     SELECT 1
                       FROM retired_execution retired
                      WHERE retired.sandbox_id = candidate.id
                 )
         ),
         reset_port_exposures AS (
             UPDATE sandbox_port_exposures spe
                SET {open_port_exposure_assignments}
               FROM eligible_candidate candidate
              WHERE spe.sandbox_id = candidate.id
                AND spe.desired_state = 'open'
             RETURNING spe.sandbox_id
         )
         UPDATE sandboxes s
            SET {sandbox_assignments}
           FROM eligible_candidate candidate
          WHERE s.id = candidate.id
          RETURNING s.id, candidate.clear_marker",
        active_execution_assignments = finalization.active_execution_assignments("e"),
        open_port_exposure_assignments = finalization.open_port_exposure_assignments(),
        sandbox_assignments = finalization.sandbox_assignments(),
    );
    let projected: Vec<(SandboxId, bool)> =
        sqlx::query_as(sqlx::AssertSqlSafe(sql!(finalization_query.as_str())))
            .bind(&candidate_ids)
            .bind(&clear_markers)
            .bind(&published_artifact_ids)
            .bind(&published_artifact_kinds)
            .bind(&published_artifact_origins)
            .bind(&published_execution_generations)
            .bind(&published_pod_uids)
            .bind(&published_pod_uids_were_missing)
            .fetch_all(&mut *conn)
            .await
            .map_err(|err| {
                Status::internal(format!("finalize suspended sandbox lifecycle: {err}"))
            })?;

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

pub async fn prune_absent_node_templates(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    present_keys: &[(String, String)],
) -> Result<(), Status> {
    let templates = present_keys
        .iter()
        .map(|(template, _)| template.clone())
        .collect::<Vec<_>>();
    let content_hashes = present_keys
        .iter()
        .map(|(_, content_hash)| content_hash.clone())
        .collect::<Vec<_>>();
    sqlx::query(sql!(
        "WITH present AS (
             SELECT *
             FROM UNNEST($2::TEXT[], $3::TEXT[]) AS present(template_name, content_hash)
         ),
         removed AS (
             DELETE FROM node_template_inventory nti
             WHERE nti.node_id = $1
               AND NOT EXISTS (
                   SELECT 1
                   FROM present
                   WHERE present.template_name = nti.template_name
                     AND present.content_hash = nti.content_hash
               )
             RETURNING nti.template_name
         ),
         removed_elements AS (
             SELECT DISTINCT 'template:' || template_name AS element
             FROM removed
         )
         DELETE FROM node_facet_observed_status nfos
         USING removed_elements
         WHERE nfos.node_id = $1
           AND nfos.facet = $4
           AND nfos.element = removed_elements.element"
    ))
    .bind(node_id)
    .bind(templates)
    .bind(content_hashes)
    .bind(FACET_NETWORK_POLICY)
    .execute(&mut *conn)
    .await
    .map_err(|err| Status::internal(format!("prune absent published templates: {err}")))?;
    Ok(())
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
    sqlx::query(sql!(
        "WITH scope AS (
             SELECT UNNEST($1::UUID[]) AS sandbox_id
         ),
         incoming AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS incoming(
                 sandbox_id UUID,
                 snapshot_id TEXT
             )
         )
         DELETE FROM sandbox_snapshot_inventory ssi
         USING scope
         WHERE ssi.sandbox_id = scope.sandbox_id
           AND NOT EXISTS (
               SELECT 1
               FROM incoming
               WHERE incoming.sandbox_id = ssi.sandbox_id
                 AND incoming.snapshot_id = ssi.snapshot_id
           )"
    ))
    .bind(sandbox_ids)
    .bind(Json(serde_json::Value::Array(snapshot_ids_payload)))
    .execute(&mut *conn)
    .await
    .map_err(|err| Status::internal(format!("prune absent published sandbox snapshots: {err}")))?;

    if snapshot_rows.is_empty() {
        return Ok(());
    }

    sqlx::query(sql!(
        "WITH scope AS (
             SELECT UNNEST($1::UUID[]) AS sandbox_id
         ),
         input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 sandbox_id UUID,
                 snapshot_id TEXT,
                 type TEXT,
                 state TEXT,
                 timestamp TEXT,
                 source_node_id TEXT,
                 label TEXT,
                 retention_tier TEXT,
                 vcpu INTEGER,
                 memory_mib INTEGER,
                 size_bytes BIGINT,
                 template_id TEXT,
                 content_hash TEXT,
                 locations JSONB
             )
         )
         INSERT INTO sandbox_snapshot_inventory (
                 sandbox_id, snapshot_id, type, state, timestamp, source_node_id,
                 label, retention_tier, vcpu, memory_mib, size_bytes, template_id,
                 content_hash, locations, observed_at, updated_at
             )
         SELECT
             input.sandbox_id,
             input.snapshot_id,
             input.type,
             input.state,
             input.timestamp,
             input.source_node_id,
             input.label,
             input.retention_tier,
             input.vcpu,
             input.memory_mib,
             COALESCE(input.size_bytes, 0),
             input.template_id,
             input.content_hash,
             COALESCE(input.locations, '[]'::jsonb),
             NOW(),
             NOW()
         FROM input
         JOIN scope ON scope.sandbox_id = input.sandbox_id
         ON CONFLICT (sandbox_id, snapshot_id) DO UPDATE
         SET type = EXCLUDED.type,
             state = EXCLUDED.state,
             timestamp = EXCLUDED.timestamp,
             source_node_id = EXCLUDED.source_node_id,
             label = EXCLUDED.label,
             retention_tier = EXCLUDED.retention_tier,
             vcpu = EXCLUDED.vcpu,
             memory_mib = EXCLUDED.memory_mib,
             size_bytes = EXCLUDED.size_bytes,
             template_id = EXCLUDED.template_id,
             content_hash = EXCLUDED.content_hash,
             locations = EXCLUDED.locations,
             observed_at = EXCLUDED.observed_at,
             updated_at = NOW()
         WHERE (
             sandbox_snapshot_inventory.type,
             sandbox_snapshot_inventory.state,
             sandbox_snapshot_inventory.timestamp,
             sandbox_snapshot_inventory.source_node_id,
             sandbox_snapshot_inventory.label,
             sandbox_snapshot_inventory.retention_tier,
             sandbox_snapshot_inventory.vcpu,
             sandbox_snapshot_inventory.memory_mib,
             sandbox_snapshot_inventory.size_bytes,
             sandbox_snapshot_inventory.template_id,
             sandbox_snapshot_inventory.content_hash,
             sandbox_snapshot_inventory.locations
         ) IS DISTINCT FROM (
             EXCLUDED.type,
             EXCLUDED.state,
             EXCLUDED.timestamp,
             EXCLUDED.source_node_id,
             EXCLUDED.label,
             EXCLUDED.retention_tier,
             EXCLUDED.vcpu,
             EXCLUDED.memory_mib,
             EXCLUDED.size_bytes,
             EXCLUDED.template_id,
             EXCLUDED.content_hash,
             EXCLUDED.locations
         )"
    ))
    .bind(sandbox_ids)
    .bind(Json(serde_json::Value::Array(snapshot_rows)))
    .execute(&mut *conn)
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

#[cfg(test)]
#[path = "published_state_lifecycle_race_tests.rs"]
mod published_state_lifecycle_race_tests;
#[cfg(test)]
#[path = "published_state_tests.rs"]
mod published_state_tests;
