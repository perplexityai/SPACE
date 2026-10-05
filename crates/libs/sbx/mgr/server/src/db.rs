//! Sandbox-manager persistence and guarded lifecycle transitions.
#![cfg_attr(not(test), deny(clippy::expect_used, clippy::unwrap_used))]

use pplx_space_common_core::types::JobId;

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

pub use pplx_space_common_core::types::PolicyAuthority;
use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, AutoLifecyclePolicy, AutoLifecycleRuntimeState,
    ExecutionGeneration, ExecutionMetadata, LocalArtifactId, NodeManagedBy, ORPHANED_SANDBOX_OWNER,
    ORPHANED_SANDBOX_TEMPLATE, RuntimeKind, SandboxActivationAttemptId, SandboxCleanupEpisodeId,
    SandboxId, SandboxOwner, SandboxQuarantineHistoryAction, SandboxQuarantineNextAction,
    SandboxQuarantineReasonCode, SandboxQuarantineSource, SandboxRuntimeLabel,
    SandboxTransitionReceipt, SandboxTransitionReceiptInvariantError, SandboxTransitionReceiptKind,
    SandboxTransitionTargetState, SnapshotName,
};
use pplx_space_common_svc::sql::sqlite::SqliteU64;
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
    emit_sandbox_event,
};
#[cfg(test)]
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_net_common::{NetworkPolicy, proto::NetworkPolicySource};
use rusqlite::types::Type;

use pplx_space_common_core::sql;
use rusqlite::{Connection, OptionalExtension, Result, params};

pub(crate) mod cleanup_jobs;
pub(crate) mod cleanup_target_scan;
pub(crate) mod inventory;
pub(crate) mod inventory_read;
pub(crate) mod quarantine_history;
pub(crate) mod snapshot_gc;
pub(crate) mod timestamps;
pub(crate) mod transfer;

use crate::endpoint_proof::SpacedEndpointProof;
use crate::restore_operation::{RemoteRestoreOperation, RestoreOrigin};
use crate::state::{SandboxState, StateTransitionError};
use pplx_space_common_core::types::{PodUid, RouteGeneration, SpaceApiRouteIdentity};

const DB_PATH: &str = pplx_space_common_core::host::linux::paths::SANDBOX_MANAGER_DB;
const RECONCILE_FACET_NETWORK_POLICY: i32 = 1;
const RECONCILE_FACET_SANDBOX_INVENTORY: i32 = 2;

mod quarantine_scan;
mod sandbox_row;
#[cfg(test)]
mod test_schema_image_cache;
pub(crate) use quarantine_scan::PeriodicQuarantineRecoveryScan;
pub(crate) use sandbox_row::SandboxListRow;
pub use sandbox_row::SandboxRow;
use sandbox_row::{row_to_list_sandbox, row_to_sandbox};

pub(crate) struct ExecutionReleaseState {
    pub(crate) state_label: String,
    pub(crate) runtime_label: SandboxRuntimeLabel,
    pub(crate) failure_reason: Option<String>,
}

/// Minimal lifecycle evidence that remains readable when optional metadata or
/// a newer lifecycle label cannot be decoded by this binary.
#[derive(Debug)]
pub(crate) struct RawSandboxLifecycle {
    pub state: String,
    pub state_changed_at: String,
}

pub(crate) fn raw_sandbox_lifecycle(
    conn: &Connection,
    id: &str,
) -> Result<Option<RawSandboxLifecycle>> {
    conn.query_row(
        "SELECT state, state_changed_at FROM sandboxes WHERE id = ?1",
        [id],
        |row| {
            Ok(RawSandboxLifecycle {
                state: row.get(0)?,
                state_changed_at: timestamps::diagnostic_column(row, 1)?,
            })
        },
    )
    .optional()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SandboxLifecycleCount {
    pub state: SandboxState,
    pub count: u64,
}

/// Which recovery an eligible episode's evidence admits.
///
/// Node-local, so it lives here rather than in common-core beside the other
/// quarantine vocabularies: it is never persisted, never on the wire, has one
/// reader, and carries a `LocalArtifactId` that only this node's SQLite knows.
/// `QuarantineRecoveryNoProgressReason` is re-exported from common-core for the
/// opposite reason — its rendered form is written into
/// `sandbox_quarantine_state.last_error`.
///
/// Carried on the candidate because the gate below is the only place allowed to
/// decide it. Re-deriving it downstream from `sandboxes.current_local_artifact_id`
/// would make the branch a property of a mutable column rather than of the episode
/// the gate admitted, and two derivations only agree until one of them changes.
/// The reason code is fixed for an episode's lifetime; the column is not.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuarantineRecoveryBranch {
    /// A full suspend artifact is published on this node. Recovery validates that
    /// artifact and adopts the episode into a durable cleanup episode.
    ///
    /// Carries the adoption witness rather than a bare id, so the gate constructs it
    /// once and every consumer moves it. There is no fallible conversion left for a
    /// call site to get wrong or to write a dead arm around.
    AdoptLocalArtifact(AdoptableArtifact),
    /// The node holds no artifact for the sandbox at all, which the gate admits
    /// only for `OrphanedSandboxRowMissing`. Recovery re-stages the snapshot from
    /// remote storage — the multi-hour operation the node-wide permit bounds.
    RestoreOrphanedStaging,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuarantineRecoveryCandidate {
    pub sandbox_id: SandboxId,
    pub local_quarantine_id: uuid::Uuid,
    pub entered_at: u64,
    pub reason_code: SandboxQuarantineReasonCode,
    pub recovery: QuarantineRecoveryBranch,
    pub attempt_count: u32,
}

/// The one gate on legacy automatic recovery: a reason code that declares an
/// automatic recovery policy, plus artifact evidence matching that policy.
/// Returns which recovery that evidence admits, so no later reader can pick a
/// branch the evidence was never validated against.
///
/// The artifact-free branch is keyed on the reason code, not merely on the
/// absent artifact: `OrphanedSandboxRowMissing` is the only reason
/// `run_cross_node_restore_inner` will stage a snapshot for, so any other reason
/// admitted here would occupy the node-wide restore permit for a plan lookup and
/// a claim every backoff interval and never progress.
///
/// `sandbox_quarantine_state.next_action` is `requires_operator_attention`
/// for every sbx-manager quarantine, including all four recoverable reasons.
/// It records that a human may need to look, not that a human has taken
/// ownership, so it excludes nothing this gate admits.
fn quarantine_recovery_branch(
    state: SandboxState,
    reason_code: SandboxQuarantineReasonCode,
    local_artifact_id: Option<LocalArtifactId>,
    artifact_kind: Option<ArtifactKind>,
    artifact_origin: Option<ArtifactOrigin>,
) -> Option<QuarantineRecoveryBranch> {
    reason_code.automatic_recovery_policy()?;
    match local_artifact_id {
        Some(local_artifact_id) => (state == SandboxState::Quarantined
            && artifact_kind == Some(ArtifactKind::Full)
            && artifact_origin == Some(ArtifactOrigin::Suspend))
        .then_some(QuarantineRecoveryBranch::AdoptLocalArtifact(
            AdoptableArtifact(local_artifact_id),
        )),
        None => (reason_code == SandboxQuarantineReasonCode::OrphanedSandboxRowMissing
            && matches!(state, SandboxState::Quarantined | SandboxState::Restoring)
            && artifact_kind.is_none()
            && artifact_origin.is_none())
        .then_some(QuarantineRecoveryBranch::RestoreOrphanedStaging),
    }
}

/// Observations over the node-local quarantine cohort legacy recovery owns.
/// Completed periodic sweeps contain aggregate counts with an empty candidate
/// list: each bounded batch owns its candidates. Counts reflect each row when
/// visited, not a transactionally consistent inventory snapshot.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QuarantineRecoveryScan {
    pub candidates: Vec<QuarantineRecoveryCandidate>,
    pub pending_count: u64,
    pub pending_oldest_age: Duration,
    /// Rows in the legacy-owned cohort whose stored columns failed validation.
    /// They are skipped so valid siblings still recover. A genuine SQLite
    /// failure is not counted here — it aborts the scan, because a partial
    /// candidate list returned as success makes a failed pass look healthy.
    pub malformed_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// A candidate whose claim this process holds.
///
/// `candidate.attempt_count` is the count the claim wrote — post-claim, one above
/// what the scan read. There is deliberately no second copy: this type used to
/// carry its own `attempt_count` beside an embedded candidate still holding the
/// stale pre-claim value, and either compiled wherever a fence or a backoff was
/// built. The fence pins `attempt_count`, so picking the stale one would make
/// every fenced write of the operation miss, report a stale claim, and strand the
/// episode for its whole lease — silently, and only under a retry.
pub struct ClaimedQuarantineRecoveryCandidate {
    pub candidate: QuarantineRecoveryCandidate,
    pub claim_token: uuid::Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QuarantineRecoveryOwner {
    LegacyRecovery,
    OperatorAttention,
}

impl QuarantineRecoveryOwner {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LegacyRecovery => "legacy_recovery",
            Self::OperatorAttention => "operator_attention",
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown quarantine recovery owner {value:?}")]
pub(crate) struct QuarantineRecoveryOwnerParseError {
    value: String,
}

impl FromStr for QuarantineRecoveryOwner {
    type Err = QuarantineRecoveryOwnerParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "legacy_recovery" => Ok(Self::LegacyRecovery),
            "operator_attention" => Ok(Self::OperatorAttention),
            _ => Err(QuarantineRecoveryOwnerParseError {
                value: value.to_string(),
            }),
        }
    }
}

// Most of the no-progress vocabulary is durable: its rendered form is persisted
// into `sandbox_quarantine_state.last_error`. Pre-claim reasons and a detached
// task failure are metric-only, but share the same bounded classification so one
// pass does not invent a second reason type. It therefore belongs in common-core
// rather than in the metrics crate whose tag it also happens to be. Re-exported here so
// `crate::db::QuarantineRecoveryNoProgressReason` stays the name callers use.
pub use pplx_space_common_core::types::QuarantineRecoveryNoProgressReason;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuarantineRecoveryNoProgressOutcome {
    StaleClaim,
    RetryScheduled {
        attempt_count: u32,
        /// Seconds since the epoch, matching how the column is stored.
        next_attempt_at: i64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublishedQuarantineReasonCode {
    Known(SandboxQuarantineReasonCode),
    ForwardCompatible(String),
}

impl std::fmt::Display for PublishedQuarantineReasonCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl PublishedQuarantineReasonCode {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Known(reason_code) => reason_code.as_str(),
            Self::ForwardCompatible(reason_code) => reason_code,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SandboxStatePublicationMetadata {
    Quarantined {
        reason_code: PublishedQuarantineReasonCode,
    },
    TransitionReceipt(SandboxTransitionReceipt<PublishedQuarantineReasonCode>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawSandboxStatePublicationMetadata {
    pub quarantine_reason_code: Option<String>,
    pub transition_receipt_id: Option<String>,
    pub transition_receipt_kind: Option<String>,
    pub transition_receipt_target_state: Option<String>,
    pub transition_receipt_execution_generation: Option<i64>,
    pub transition_receipt_reason_code: Option<String>,
    pub transition_receipt_activation_attempt_id: Option<String>,
}

pub type RawSandboxStatePublicationMetadataRead =
    std::result::Result<RawSandboxStatePublicationMetadata, rusqlite::Error>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawQuarantineRecoveryObservation {
    pub local_quarantine_id: String,
    pub owner: Option<String>,
    pub attempt_count: i64,
    pub next_attempt_at: Option<String>,
}

pub type RawQuarantineRecoveryObservationRead =
    std::result::Result<RawQuarantineRecoveryObservation, rusqlite::Error>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxStatePublicationMetadataField {
    QuarantineReasonCode,
    TransitionReceiptId,
    TransitionReceiptKind,
    TransitionReceiptTargetState,
    TransitionReceiptExecutionGeneration,
    TransitionReceiptReasonCode,
    TransitionReceiptActivationAttemptId,
}

impl SandboxStatePublicationMetadataField {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QuarantineReasonCode => "quarantine_reason_code",
            Self::TransitionReceiptId => "transition_receipt_id",
            Self::TransitionReceiptKind => "transition_receipt_kind",
            Self::TransitionReceiptTargetState => "transition_receipt_target_state",
            Self::TransitionReceiptExecutionGeneration => "transition_receipt_execution_generation",
            Self::TransitionReceiptReasonCode => "transition_receipt_reason_code",
            Self::TransitionReceiptActivationAttemptId => {
                "transition_receipt_activation_attempt_id"
            }
        }
    }
}

impl fmt::Display for SandboxStatePublicationMetadataField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeSandboxStatePublicationMetadataError {
    #[error("missing {field}")]
    Missing {
        field: SandboxStatePublicationMetadataField,
    },
    #[error("invalid {field}: {message}")]
    Invalid {
        field: SandboxStatePublicationMetadataField,
        message: String,
    },
}

impl DecodeSandboxStatePublicationMetadataError {
    pub const fn field(&self) -> SandboxStatePublicationMetadataField {
        match self {
            Self::Missing { field } | Self::Invalid { field, .. } => *field,
        }
    }
}

impl RawSandboxStatePublicationMetadata {
    pub fn decode(
        self,
        state: SandboxState,
    ) -> std::result::Result<
        SandboxStatePublicationMetadata,
        DecodeSandboxStatePublicationMetadataError,
    > {
        if state == SandboxState::Quarantined {
            let reason = required_trimmed(
                self.quarantine_reason_code,
                SandboxStatePublicationMetadataField::QuarantineReasonCode,
            )?;
            return Ok(SandboxStatePublicationMetadata::Quarantined {
                reason_code: parse_published_quarantine_reason(reason),
            });
        }

        let receipt_id = required_trimmed(
            self.transition_receipt_id,
            SandboxStatePublicationMetadataField::TransitionReceiptId,
        )?
        .parse()
        .map_err(|error: uuid::Error| {
            DecodeSandboxStatePublicationMetadataError::Invalid {
                field: SandboxStatePublicationMetadataField::TransitionReceiptId,
                message: error.to_string(),
            }
        })?;
        let kind = required_trimmed(
            self.transition_receipt_kind,
            SandboxStatePublicationMetadataField::TransitionReceiptKind,
        )?
        .parse()
        .map_err(
            |error: pplx_space_common_core::types::SandboxTransitionReceiptKindParseError| {
                DecodeSandboxStatePublicationMetadataError::Invalid {
                    field: SandboxStatePublicationMetadataField::TransitionReceiptKind,
                    message: error.to_string(),
                }
            },
        )?;
        let target_state: SandboxTransitionTargetState = required_trimmed(
            self.transition_receipt_target_state,
            SandboxStatePublicationMetadataField::TransitionReceiptTargetState,
        )?
        .parse()
        .map_err(
            |error: pplx_space_common_core::types::SandboxTransitionTargetStateParseError| {
                DecodeSandboxStatePublicationMetadataError::Invalid {
                    field: SandboxStatePublicationMetadataField::TransitionReceiptTargetState,
                    message: error.to_string(),
                }
            },
        )?;
        if target_state.as_str() != state.as_str() {
            return Err(DecodeSandboxStatePublicationMetadataError::Invalid {
                field: SandboxStatePublicationMetadataField::TransitionReceiptTargetState,
                message: format!("target {target_state} does not match row state {state}"),
            });
        }
        let execution_generation = self
            .transition_receipt_execution_generation
            .ok_or(DecodeSandboxStatePublicationMetadataError::Missing {
                field: SandboxStatePublicationMetadataField::TransitionReceiptExecutionGeneration,
            })
            .and_then(|value| {
                ExecutionGeneration::new(value).map_err(|error| {
                    DecodeSandboxStatePublicationMetadataError::Invalid {
                        field: SandboxStatePublicationMetadataField::TransitionReceiptExecutionGeneration,
                        message: error.to_string(),
                    }
                })
            })?;
        let reason_code = self
            .transition_receipt_reason_code
            .map(|reason| {
                required_trimmed(
                    Some(reason),
                    SandboxStatePublicationMetadataField::TransitionReceiptReasonCode,
                )
                .map(parse_published_quarantine_reason)
            })
            .transpose()?;
        let activation_attempt_id = self
            .transition_receipt_activation_attempt_id
            .map(|value| {
                let value = value.trim();
                value.parse().map_err(|error: uuid::Error| {
                    DecodeSandboxStatePublicationMetadataError::Invalid {
                        field:
                            SandboxStatePublicationMetadataField::TransitionReceiptActivationAttemptId,
                        message: error.to_string(),
                    }
                })
            })
            .transpose()?;
        let receipt = SandboxTransitionReceipt::try_new(
            receipt_id,
            kind,
            target_state,
            execution_generation,
            reason_code,
            activation_attempt_id,
        )
        .map_err(
            |error| DecodeSandboxStatePublicationMetadataError::Invalid {
                field: match error {
                    SandboxTransitionReceiptInvariantError::MissingActivationAttempt => {
                        SandboxStatePublicationMetadataField::TransitionReceiptActivationAttemptId
                    }
                    SandboxTransitionReceiptInvariantError::MissingReasonCode
                    | SandboxTransitionReceiptInvariantError::UnexpectedReasonCode => {
                        SandboxStatePublicationMetadataField::TransitionReceiptReasonCode
                    }
                    SandboxTransitionReceiptInvariantError::InvalidActivationCompletionTarget {
                        ..
                    }
                    | SandboxTransitionReceiptInvariantError::InvalidActivationRollbackTarget {
                        ..
                    }
                    | SandboxTransitionReceiptInvariantError::InvalidQuarantineRecoveryTarget {
                        ..
                    } => SandboxStatePublicationMetadataField::TransitionReceiptTargetState,
                },
                message: error.to_string(),
            },
        )?;
        Ok(SandboxStatePublicationMetadata::TransitionReceipt(receipt))
    }
}

fn required_trimmed(
    value: Option<String>,
    field: SandboxStatePublicationMetadataField,
) -> std::result::Result<String, DecodeSandboxStatePublicationMetadataError> {
    let value = value.ok_or(DecodeSandboxStatePublicationMetadataError::Missing { field })?;
    let value = value.trim();
    if value.is_empty() {
        return Err(DecodeSandboxStatePublicationMetadataError::Invalid {
            field,
            message: "value is blank".to_string(),
        });
    }
    Ok(value.to_string())
}

fn parse_published_quarantine_reason(value: String) -> PublishedQuarantineReasonCode {
    value.parse().map_or_else(
        |_| PublishedQuarantineReasonCode::ForwardCompatible(value),
        PublishedQuarantineReasonCode::Known,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuarantineRecoveryFence {
    pub local_artifact_id: LocalArtifactId,
    pub execution_generation: ExecutionGeneration,
}

pub const SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE: &str =
    "suspend_publish_failed_no_artifact_runtime_gone";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalArtifactRow {
    pub local_artifact_id: LocalArtifactId,
    pub sandbox_id: String,
    pub artifact_kind: ArtifactKind,
    pub artifact_origin: ArtifactOrigin,
    pub snapshot_name: Option<SnapshotName>,
    pub local_path: String,
}

pub struct RuntimeFields<'a> {
    pub uid: u32,
    pub slot: u32,
    pub guest_cid: u32,
    pub pid: u32,
    pub spaced_host_pid: u32,
    pub netgtw_pid: u32,
    pub vsock_uds_path: &'a str,
    pub api_socket_path: &'a str,
    pub space_api_route_identity: SpaceApiRouteIdentity,
    pub route_identity_provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance,
    pub spaced_endpoint_proof: Option<&'a SpacedEndpointProof>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreTransferScopeRow {
    pub transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
    pub execution_generation: Option<ExecutionGeneration>,
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub staging_dir: String,
}

fn restore_transfer_scope_from_row(
    row: &rusqlite::Row<'_>,
    first_column: usize,
) -> rusqlite::Result<RestoreTransferScopeRow> {
    let scope: String = row.get(first_column)?;
    let generation: Option<i64> = row.get(first_column + 1)?;
    let attempt: Option<String> = row.get(first_column + 2)?;
    Ok(RestoreTransferScopeRow {
        transfer_scope_id: scope.parse().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                first_column,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        execution_generation: generation
            .map(ExecutionGeneration::try_from)
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    first_column + 1,
                    rusqlite::types::Type::Integer,
                    Box::new(error),
                )
            })?,
        activation_attempt_id: attempt
            .map(|value| value.parse())
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    first_column + 2,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
        staging_dir: row.get(first_column + 3)?,
    })
}

pub fn restore_transfer_scope(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<RestoreTransferScopeRow>> {
    conn.query_row(
        "SELECT transfer_scope_id, execution_generation, activation_attempt_id, staging_dir
           FROM sandbox_restore_transfer_scopes
          WHERE sandbox_id = ?1",
        params![sandbox_id],
        |row| restore_transfer_scope_from_row(row, 0),
    )
    .optional()
}

pub fn rowless_restore_transfer_scopes(
    conn: &Connection,
) -> Result<Vec<(String, RestoreTransferScopeRow)>> {
    let mut statement = conn.prepare(
        "SELECT scopes.sandbox_id, scopes.transfer_scope_id,
                scopes.execution_generation, scopes.activation_attempt_id,
                scopes.staging_dir
           FROM sandbox_restore_transfer_scopes AS scopes
      LEFT JOIN sandboxes ON sandboxes.id = scopes.sandbox_id
          WHERE sandboxes.id IS NULL",
    )?;
    let mut rows = statement.query([])?;
    let mut scopes = Vec::new();
    while let Some(row) = rows.next()? {
        let decoded = (|| Ok((row.get(0)?, restore_transfer_scope_from_row(row, 1)?)))();
        match decoded {
            Ok(scope) => scopes.push(scope),
            Err(error) if is_row_decode_error(&error) => tracing::warn!(
                sandbox_id = ?row.get_ref(0)?,
                error = %describe_error(&error),
                "retaining malformed rowless restore scope; operator repair required"
            ),
            Err(error) => return Err(error),
        }
    }
    Ok(scopes)
}

pub fn retire_rowless_restore_transfer_scope(
    conn: &Connection,
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
) -> Result<bool> {
    let changed = conn.execute(
        "DELETE FROM sandbox_restore_transfer_scopes
          WHERE sandbox_id = ?1
            AND transfer_scope_id = ?2
            AND NOT EXISTS (
                SELECT 1 FROM sandboxes WHERE id = ?1
            )",
        params![sandbox_id, transfer_scope_id.to_string()],
    )?;
    Ok(changed == 1)
}

pub fn retire_orphan_quarantine_restore_transfer_scope(
    conn: &Connection,
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
) -> Result<bool> {
    let changed = conn.execute(
        "DELETE FROM sandbox_restore_transfer_scopes
          WHERE sandbox_id = ?1
            AND transfer_scope_id = ?2
            AND EXISTS (
                SELECT 1
                  FROM sandboxes
                  JOIN sandbox_quarantine_state qs ON qs.sandbox_id = sandboxes.id
                 WHERE sandboxes.id = ?1
                   AND sandboxes.state IN ('quarantined', 'restoring')
                   AND qs.source = ?3
                   AND qs.reason_code = ?4
            )",
        params![
            sandbox_id,
            transfer_scope_id.to_string(),
            SandboxQuarantineSource::SbxManager.as_str(),
            SandboxQuarantineReasonCode::OrphanedSandboxRowMissing.as_str(),
        ],
    )?;
    Ok(changed == 1)
}

/// Claim cleanup of a retryable interrupted-restore scope with the current or
/// a strictly newer control-plane activation attempt.
///
/// This deliberately retains the physical scope and staging path. Cancellation
/// does not permanently close a volume-manager scope, so the new owner can
/// reuse it after cleanup while a crash leaves a durable retry reference.
pub fn claim_retryable_restore_transfer_scope_if_current_or_newer(
    conn: &Connection,
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
    execution_generation: ExecutionGeneration,
    activation_attempt_id: SandboxActivationAttemptId,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandbox_restore_transfer_scopes
            SET execution_generation = ?1,
                activation_attempt_id = ?2,
                created_at = ?5
          WHERE sandbox_id = ?3
            AND transfer_scope_id = ?4
            AND (
                (
                    execution_generation IS NULL
                    AND activation_attempt_id IS NULL
                )
                OR (
                    execution_generation IS NOT NULL
                    AND activation_attempt_id IS NOT NULL
                    AND (
                        execution_generation < ?1
                        OR (
                            execution_generation = ?1
                            AND activation_attempt_id <= ?2
                        )
                    )
                )
            )
            AND EXISTS (
                SELECT 1
                 FROM sandboxes
                 WHERE sandboxes.id = ?3
                   AND sandboxes.state = 'restoring'
                   AND sandboxes.restore_origin = 'remote'
                   AND sandboxes.uid IS NULL
                   AND sandboxes.slot IS NULL
                   AND sandboxes.guest_cid IS NULL
                   AND sandboxes.pid IS NULL
                   AND sandboxes.spaced_host_pid IS NULL
                   AND sandboxes.netgtw_pid IS NULL
                   AND sandboxes.vsock_uds_path IS NULL
                   AND sandboxes.api_socket_path IS NULL
                   AND sandboxes.current_local_artifact_id IS NULL
                   AND sandboxes.cleanup_completion IS NULL
                   AND sandboxes.cleanup_keep_filesystem IS NULL
            )",
        params![
            execution_generation.get(),
            activation_attempt_id.to_string(),
            sandbox_id,
            transfer_scope_id.to_string(),
            now()?,
        ],
    )?;
    Ok(changed == 1)
}

/// Transfer cleanup for an already-exchanged canonical root to a strictly
/// newer control-plane activation attempt without changing the scope that
/// owns the old root on disk.
///
/// The exchange receipt remains immutable evidence of the attempt that moved
/// the old root. This CAS only advances cleanup ownership, so a successor can
/// retry fallible removal while delayed predecessors remain fenced out.
pub fn advance_exchanged_restore_cleanup_owner_if_newer(
    conn: &Connection,
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
    execution_generation: ExecutionGeneration,
    activation_attempt_id: SandboxActivationAttemptId,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandbox_restore_transfer_scopes
            SET execution_generation = ?1,
                activation_attempt_id = ?2,
                created_at = ?5
          WHERE sandbox_id = ?3
            AND transfer_scope_id = ?4
            AND execution_generation IS NOT NULL
            AND activation_attempt_id IS NOT NULL
            AND (
                execution_generation < ?1
                OR (
                    execution_generation = ?1
                    AND activation_attempt_id < ?2
                )
            )
            AND EXISTS (
                SELECT 1
                  FROM sandboxes
                 WHERE sandboxes.id = ?3
                   AND sandboxes.state = 'restoring'
                   AND sandboxes.restore_origin = 'remote'
                   AND sandboxes.uid IS NULL
                   AND sandboxes.slot IS NULL
                   AND sandboxes.guest_cid IS NULL
                   AND sandboxes.pid IS NULL
                   AND sandboxes.spaced_host_pid IS NULL
                   AND sandboxes.netgtw_pid IS NULL
                   AND sandboxes.vsock_uds_path IS NULL
                   AND sandboxes.api_socket_path IS NULL
                   AND sandboxes.current_local_artifact_id IS NULL
                   AND sandboxes.cleanup_completion IS NULL
                   AND sandboxes.cleanup_keep_filesystem IS NULL
            )",
        params![
            execution_generation.get(),
            activation_attempt_id.to_string(),
            sandbox_id,
            transfer_scope_id.to_string(),
            now()?,
        ],
    )?;
    Ok(changed == 1)
}

pub fn is_orphan_quarantine_recovery_state(
    conn: &Connection,
    sandbox_id: &str,
    expected_state: SandboxState,
) -> Result<bool> {
    if !matches!(
        expected_state,
        SandboxState::Quarantined | SandboxState::Restoring
    ) {
        return Ok(false);
    }
    conn.query_row(
        "SELECT EXISTS (
             SELECT 1
               FROM sandboxes
               JOIN sandbox_quarantine_state qs ON qs.sandbox_id = sandboxes.id
              WHERE sandboxes.id = ?1
                AND sandboxes.state = ?2
                AND sandboxes.current_local_artifact_id IS NULL
                AND qs.source = ?3
                AND qs.reason_code = ?4
         )",
        params![
            sandbox_id,
            expected_state.as_str(),
            SandboxQuarantineSource::SbxManager.as_str(),
            SandboxQuarantineReasonCode::OrphanedSandboxRowMissing.as_str(),
        ],
        |row| row.get(0),
    )
}

pub fn insert_restore_transfer_scope(
    conn: &Connection,
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
    execution_generation: Option<ExecutionGeneration>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    staging_dir: &str,
) -> Result<bool> {
    let changed = conn.execute(
        "INSERT INTO sandbox_restore_transfer_scopes (
             sandbox_id, transfer_scope_id, execution_generation,
             activation_attempt_id, staging_dir, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(sandbox_id) DO NOTHING",
        params![
            sandbox_id,
            transfer_scope_id.to_string(),
            execution_generation.map(ExecutionGeneration::get),
            activation_attempt_id.map(|value| value.to_string()),
            staging_dir,
            now()?,
        ],
    )?;
    Ok(changed == 1)
}

fn retire_restore_transfer_scope(conn: &Connection, sandbox_id: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM sandbox_restore_transfer_scopes WHERE sandbox_id = ?1",
        params![sandbox_id],
    )?;
    Ok(())
}

pub fn retire_restore_transfer_scope_if_matches(
    conn: &Connection,
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
) -> Result<bool> {
    let changed = conn.execute(
        "DELETE FROM sandbox_restore_transfer_scopes
          WHERE sandbox_id = ?1 AND transfer_scope_id = ?2",
        params![sandbox_id, transfer_scope_id.to_string()],
    )?;
    Ok(changed == 1)
}

pub const TEARDOWN_SUSPEND_JOB_KIND: &str = "teardown_suspend";
pub(crate) const PLANNED_TEARDOWN_SUSPEND_OPERATION: &str = "planned_teardown";
const TEARDOWN_SUSPEND_SUPERSEDED_REASON: &str = "superseded by a newer local execution lease";
const EXECUTION_DELETE_RECEIPT_RETENTION_SECS: i64 = 7 * 24 * 60 * 60;

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
struct TeardownSuspendJobPayload {
    execution_generation: ExecutionGeneration,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    published_artifact_id: Option<LocalArtifactId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TeardownSuspendOperation {
    pub job_id: JobId,
    pub execution_generation: ExecutionGeneration,
    pub published_artifact_id: Option<LocalArtifactId>,
    pub status: TeardownSuspendOperationStatus,
}

pub type SupersededTeardownGeneration = Option<ExecutionGeneration>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TeardownSuspendOperationStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl TeardownSuspendOperationStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub const fn is_active(self) -> bool {
        matches!(self, Self::Pending | Self::Running)
    }

    pub const fn can_support_release_proof(self) -> bool {
        matches!(self, Self::Pending | Self::Running | Self::Completed)
    }
}

impl fmt::Display for TeardownSuspendOperationStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown teardown suspend job status {0:?}")]
pub struct TeardownSuspendOperationStatusParseError(String);

impl FromStr for TeardownSuspendOperationStatus {
    type Err = TeardownSuspendOperationStatusParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(TeardownSuspendOperationStatusParseError(other.to_string())),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TeardownSuspendStatus {
    pub state: SandboxState,
    pub operation: Option<TeardownSuspendOperation>,
    pub artifact: Option<LocalArtifactRow>,
    pub backup_completed: bool,
}

impl TeardownSuspendStatus {
    pub fn is_ready(&self) -> bool {
        self.state == SandboxState::Suspended && self.artifact.is_some() && self.backup_completed
    }

    pub fn is_ready_for(&self, execution_generation: ExecutionGeneration) -> bool {
        self.is_ready()
            && self.operation.as_ref().is_some_and(|operation| {
                operation.execution_generation == execution_generation
                    && operation.status.can_support_release_proof()
            })
    }

    pub fn is_durable_for(&self, execution_generation: ExecutionGeneration) -> bool {
        self.is_ready()
            && self.operation.as_ref().is_some_and(|operation| {
                operation.execution_generation == execution_generation
                    && operation.status == TeardownSuspendOperationStatus::Completed
                    && self.artifact.as_ref().is_some_and(|artifact| {
                        operation.published_artifact_id == Some(artifact.local_artifact_id)
                    })
            })
    }
}

pub struct InsertSandboxParams<'a> {
    pub id: &'a str,
    pub uuid8: &'a SandboxRuntimeLabel,
    pub state: SandboxState,
    pub template: &'a str,
    pub template_hash: Option<&'a str>,
    pub user: &'a SandboxOwner,
    pub runtime: &'a str,
    pub snapshots_enabled: bool,
    pub rootfs_size_mb: Option<u32>,
    pub disk_io_limits: pplx_space_common_core::types::DiskIoLimits,
    pub pool_template: Option<&'a str>,
    pub volume_encryption: bool,
    pub process_env: &'a BTreeMap<String, String>,
    pub auto_lifecycle: &'a AutoLifecyclePolicy,
    pub network_policy_override: Option<&'a NetworkPolicy>,
    pub network_policy_generation: u64,
    pub network_policy_authority: PolicyAuthority,
    pub auto_lifecycle_authority: PolicyAuthority,
}

#[derive(Debug, thiserror::Error)]
pub enum RestoreAdoptOrphanInsertError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("restore sandbox row state must be restoring, got {state}")]
    NonRestoringState { state: SandboxState },
    #[error("existing sandbox row for id {id} is not a recoverable synthetic orphan")]
    ExistingRowNotSyntheticOrphan { id: String },
}

fn parse_policy_authority(value: &str, column: usize) -> Result<PolicyAuthority> {
    value.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(error))
    })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NetworkPolicyAppliedSource {
    #[default]
    Unknown,
    Template,
    Override,
}

impl NetworkPolicyAppliedSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "",
            Self::Template => "template",
            Self::Override => "override",
        }
    }

    pub fn parse(value: &str) -> Self {
        value.parse().unwrap_or(Self::Unknown)
    }

    pub fn as_proto(self) -> i32 {
        match self {
            Self::Unknown => NetworkPolicySource::Unspecified as i32,
            Self::Template => NetworkPolicySource::Template as i32,
            Self::Override => NetworkPolicySource::Override as i32,
        }
    }
}

impl fmt::Display for NetworkPolicyAppliedSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NetworkPolicyAppliedSource {
    type Err = NetworkPolicyAppliedSourceParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "" => Ok(Self::Unknown),
            "template" => Ok(Self::Template),
            "override" => Ok(Self::Override),
            _ => Err(NetworkPolicyAppliedSourceParseError),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error("network policy applied source must be template or override")]
pub struct NetworkPolicyAppliedSourceParseError;

impl fmt::Display for CleanupCompletion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for CleanupCompletion {
    type Err = CleanupCompletionParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "mark_created" => Ok(Self::MarkCreated),
            "mark_stopped" => Ok(Self::MarkStopped),
            "mark_suspended" => Ok(Self::MarkSuspended),
            "mark_failed" => Ok(Self::Quarantine),
            "delete_row" => Ok(Self::DeleteRow),
            "delete_pool_clone" => Ok(Self::DeletePoolClone),
            _ => Err(CleanupCompletionParseError),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error(
    "cleanup completion must be mark_created, mark_stopped, mark_suspended, mark_failed, delete_row, or delete_pool_clone"
)]
pub struct CleanupCompletionParseError;

#[derive(Debug, thiserror::Error)]
pub enum MarkQuarantinedError {
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error("unknown current sandbox state '{state}'")]
    UnknownCurrentState { state: String },
    #[error("illegal transition into quarantined: {0}")]
    IllegalTransition(#[from] StateTransitionError),
    #[error("quarantined sandbox insert must use quarantined state, got {state}")]
    NonQuarantinedInsert { state: SandboxState },
}

#[derive(Debug, thiserror::Error)]
pub enum InsertRecoveredSuspendedSandboxError {
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error("recovered suspended sandbox insert must use suspended state, got {state}")]
    NonSuspendedInsert { state: SandboxState },
    #[error("recovered suspended sandbox artifact pin updated {changed} rows instead of one")]
    ArtifactPinInvariant { changed: usize },
}

#[derive(Debug, thiserror::Error)]
pub enum CompleteCleanupEpisodeError {
    #[error(transparent)]
    InvalidReceipt(#[from] SandboxTransitionReceiptInvariantError),
    #[error("row-deleting cleanup cannot complete as a resident state")]
    DeleteHasNoTargetState,
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error("cleanup episode cannot restore suspended without a registered full suspend artifact")]
    MissingSuspendArtifact,
    #[error("cleanup episode sandbox row disappeared")]
    MissingSandboxRow,
    #[error("quarantine recovery history insert affected {changed} rows instead of one")]
    RecoveryHistoryInvariant { changed: usize },
    #[error("quarantine recovery state retirement affected {changed} rows instead of one")]
    RecoveryStateRetirementInvariant { changed: usize },
}

#[derive(Debug, thiserror::Error)]
pub enum InsertSandboxWithCleanupEpisodeError {
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error(
        "sandbox state {sandbox_state} does not match cleanup activation state {activation_state}"
    )]
    ActivationStateMismatch {
        sandbox_state: SandboxState,
        activation_state: SandboxState,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivationKind {
    ColdStart,
    Restore,
}

impl ActivationKind {
    pub const fn activation_state(self) -> SandboxState {
        match self {
            Self::ColdStart => SandboxState::Starting,
            Self::Restore => SandboxState::Restoring,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BeginActivationError {
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error("sandbox state {state} cannot begin a {kind:?} activation")]
    UnsupportedState {
        kind: ActivationKind,
        state: SandboxState,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum RollbackActivationEpisodeError {
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error("sandbox state {state} is not an activation state")]
    UnsupportedActivationState { state: SandboxState },
    #[error("an activation episode cannot roll back to quarantined")]
    QuarantinedTarget,
}

#[derive(Debug, thiserror::Error)]
pub enum RequestCleanupEpisodeError {
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error("cleanup episode phase update affected {changed} rows instead of one")]
    PhaseUpdateInvariant { changed: usize },
}

#[derive(Debug, thiserror::Error)]
pub enum SupersedeActivationError {
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error("sandbox quarantine recovery is already in progress")]
    QuarantineRecoveryInProgress,
    #[error(transparent)]
    StateWriteNotApplied(#[from] SandboxStateWriteNotApplied),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupCompletion {
    MarkCreated,
    MarkStopped,
    MarkSuspended,
    Quarantine,
    DeleteRow,
    DeletePoolClone,
}

impl CleanupCompletion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MarkCreated => "mark_created",
            Self::MarkStopped => "mark_stopped",
            Self::MarkSuspended => "mark_suspended",
            // This spelling is enforced by the deployed SQLite CHECK constraint.
            Self::Quarantine => "mark_failed",
            Self::DeleteRow => "delete_row",
            Self::DeletePoolClone => "delete_pool_clone",
        }
    }

    pub const fn deletes_row(self) -> bool {
        matches!(self, Self::DeleteRow | Self::DeletePoolClone)
    }

    pub const fn target_state(self) -> Option<SandboxState> {
        match self {
            Self::MarkCreated => Some(SandboxState::Created),
            Self::MarkStopped => Some(SandboxState::Stopped),
            Self::MarkSuspended => Some(SandboxState::Suspended),
            Self::Quarantine => Some(SandboxState::Quarantined),
            Self::DeleteRow | Self::DeletePoolClone => None,
        }
    }

    pub const fn transition_target(self) -> Option<SandboxTransitionTargetState> {
        match self {
            Self::MarkCreated => Some(SandboxTransitionTargetState::Created),
            Self::MarkStopped => Some(SandboxTransitionTargetState::Stopped),
            Self::MarkSuspended => Some(SandboxTransitionTargetState::Suspended),
            Self::Quarantine | Self::DeleteRow | Self::DeletePoolClone => None,
        }
    }

    pub fn owns_row_intent(self, row: &SandboxRow) -> bool {
        self.owns_intent(row.state, row.cleanup_completion)
    }

    fn owns_intent(self, state: SandboxState, intent: Option<Self>) -> bool {
        let persisted_intent = matches!(
            state,
            SandboxState::CleanupRequired | SandboxState::CleanupFailed
        ) && intent == Some(self);
        match self {
            Self::MarkCreated => persisted_intent,
            Self::MarkStopped => state == SandboxState::Stopping || persisted_intent,
            Self::MarkSuspended => state == SandboxState::Suspending || persisted_intent,
            Self::Quarantine => {
                persisted_intent || (state == SandboxState::CleanupRequired && intent.is_none())
            }
            Self::DeleteRow | Self::DeletePoolClone => {
                state == SandboxState::Deleting || persisted_intent
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CleanupEpisodeSpec {
    pub id: SandboxCleanupEpisodeId,
    pub completion: CleanupCompletion,
    pub keep_filesystem: bool,
    pub entered_from_state: Option<SandboxState>,
    pub activation_state: SandboxState,
    pub execution_generation: Option<ExecutionGeneration>,
    /// Route-installation ownership, not the independently supplied operation.
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PreparedCloneRoot {
    pub device: u64,
    pub inode: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupEpisode {
    /// Receipt owner; route authorization continues to use activation_attempt_id.
    pub operation_activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub(crate) prepared_clone_root: Option<PreparedCloneRoot>,
    pub id: SandboxCleanupEpisodeId,
    pub completion: CleanupCompletion,
    pub keep_filesystem: bool,
    pub entered_from_state: Option<SandboxState>,
    pub activation_state: SandboxState,
    pub requested_at: Option<u64>,
    pub quarantine_reason_code: Option<SandboxQuarantineReasonCode>,
    pub failure_message: Option<String>,
    pub last_cleanup_error: Option<String>,
    pub execution_generation: Option<ExecutionGeneration>,
    /// Legacy physical column: retained route-installation ownership evidence.
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub attempt_count: u32,
    pub quarantine_recovery: Option<QuarantineRecoveryFence>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupEpisodeRequestOutcome {
    Requested,
    AlreadyRequested,
    Superseded,
}

impl CleanupEpisodeRequestOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::AlreadyRequested => "already_requested",
            Self::Superseded => "superseded",
        }
    }
}

impl fmt::Display for CleanupEpisodeRequestOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "callers must distinguish an applied state write from a superseded one"]
pub enum SandboxStateWriteOutcome {
    Applied,
    Missing,
    BlockedByQuarantine,
    SupersededByCleanupEpisode,
}

impl SandboxStateWriteOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Missing => "missing",
            Self::BlockedByQuarantine => "blocked_by_quarantine",
            Self::SupersededByCleanupEpisode => "superseded_by_cleanup_episode",
        }
    }

    pub const fn is_applied(self) -> bool {
        matches!(self, Self::Applied)
    }

    pub fn require_applied(self) -> std::result::Result<(), SandboxStateWriteNotApplied> {
        if self.is_applied() {
            Ok(())
        } else {
            Err(SandboxStateWriteNotApplied { outcome: self })
        }
    }
}

impl fmt::Display for SandboxStateWriteOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("sandbox state write was not applied: {outcome}")]
pub struct SandboxStateWriteNotApplied {
    pub outcome: SandboxStateWriteOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupRecoveryOutcome {
    State(SandboxState),
    CleanupRequired {
        keep_filesystem: bool,
        completion: CleanupCompletion,
    },
    Deferred,
}

impl StartupRecoveryOutcome {
    pub fn changed(self) -> bool {
        !matches!(self, Self::Deferred)
    }

    pub fn requires_quarantine(self) -> bool {
        matches!(self, Self::State(SandboxState::Quarantined))
            || matches!(
                self,
                Self::CleanupRequired {
                    completion: CleanupCompletion::Quarantine,
                    ..
                }
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupRecoveryRecord {
    pub id: String,
    pub previous_state: SandboxState,
    pub outcome: StartupRecoveryOutcome,
}

/// Prepared statements kept per connection. Hot-path reads and writes use
/// `prepare_cached`; rusqlite's default of 16 lets the statements of
/// background loops evict them.
const STATEMENT_CACHE_CAPACITY: usize = 128;

pub fn open() -> Result<Connection, pplx_space_common_svc::sql::sqlite::SqliteError> {
    let conn = pplx_space_common_svc::sql::sqlite::open_rusqlite(
        Path::new(DB_PATH),
        pplx_space_common_svc::sql::sqlite::RusqliteOpenOptions::default(),
    )?;
    conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
    Ok(conn)
}

/// Count every durable sandbox-manager row by its bounded lifecycle. This stays
/// in SQL so the sampler's memory use is independent of the number of sandboxes
/// on the node.
pub(crate) fn sandbox_lifecycle_counts(conn: &Connection) -> Result<Vec<SandboxLifecycleCount>> {
    let mut statement = conn.prepare(
        "SELECT state, COUNT(*)
           FROM sandboxes
       GROUP BY state
       ORDER BY state",
    )?;
    let rows = statement.query_map([], |row| {
        let state_label: String = row.get(0)?;
        let state = state_label.parse::<SandboxState>().map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                Type::Text,
                format!("unknown sandbox state '{state_label}'").into(),
            )
        })?;
        let count = row.get::<_, SqliteU64>(1)?.0;
        Ok(SandboxLifecycleCount { state, count })
    })?;
    rows.collect()
}

pub(crate) fn describe_error(error: &rusqlite::Error) -> String {
    match error.sqlite_error() {
        Some(sqlite) => format!(
            "{error} (sqlite_code={:?}, sqlite_extended_code={})",
            sqlite.code, sqlite.extended_code
        ),
        None => error.to_string(),
    }
}

pub(crate) fn immediate_transaction(conn: &Connection) -> Result<rusqlite::Transaction<'_>> {
    rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)
}

pub(crate) fn now() -> Result<String> {
    // Preserve whole epoch-second storage without fabricating persisted evidence
    // when the host clock is invalid.
    now_epoch_i64().map(|seconds| seconds.to_string())
}

pub(crate) fn now_epoch_i64() -> Result<i64> {
    persisted_epoch_seconds(jiff::Timestamp::now())
}

#[derive(Debug, thiserror::Error)]
#[error("cannot persist a timestamp before the Unix epoch")]
struct PersistedClockBeforeEpoch;

fn persisted_epoch_seconds(timestamp: jiff::Timestamp) -> Result<i64> {
    if timestamp < jiff::Timestamp::UNIX_EPOCH {
        return Err(rusqlite::Error::ToSqlConversionFailure(Box::new(
            PersistedClockBeforeEpoch,
        )));
    }
    Ok(timestamp.as_second())
}

fn u64_to_sql_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

pub(crate) fn current_state_label(conn: &Connection, id: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT state FROM sandboxes WHERE id = ?1",
        params![id],
        |row| row.get(0),
    )
    .optional()
}

pub(crate) fn execution_release_state(
    conn: &Connection,
    id: &str,
) -> Result<Option<ExecutionReleaseState>> {
    conn.query_row(
        "SELECT state, uuid8, failure_reason FROM sandboxes WHERE id = ?1",
        params![id],
        |row| {
            let runtime_label_raw = row.get::<_, String>(1)?;
            let runtime_label = SandboxRuntimeLabel::parse(runtime_label_raw).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(error))
            })?;
            Ok(ExecutionReleaseState {
                state_label: row.get(0)?,
                runtime_label,
                failure_reason: row.get(2)?,
            })
        },
    )
    .optional()
}

pub fn record_remote_restore_artifact_staging(conn: &Connection, sandbox_id: &str) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET restore_origin = 'remote',
             restore_phase = 'artifact_staging',
             updated_at = ?1
         WHERE id = ?2
           AND state = 'restoring'
           AND restore_origin = 'remote'
           AND restore_phase IN ('metadata_staging', 'artifact_staging')",
        params![now()?, sandbox_id],
    )?;
    Ok(changed == 1)
}

pub fn replace_retryable_remote_restore_source(
    conn: &Connection,
    sandbox_id: &str,
    expected_source: &str,
    selected_source: &str,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET restore_storage_uri = ?1,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'restoring'
           AND restore_origin = 'remote'
           AND restore_phase IN ('metadata_staging', 'artifact_staging')
           AND restore_storage_uri = ?4
           AND uid IS NULL
           AND slot IS NULL
           AND guest_cid IS NULL
           AND pid IS NULL
           AND spaced_host_pid IS NULL
           AND netgtw_pid IS NULL
           AND vsock_uds_path IS NULL
           AND api_socket_path IS NULL
           AND current_local_artifact_id IS NULL
           AND cleanup_completion IS NULL
           AND cleanup_keep_filesystem IS NULL",
        params![selected_source, now()?, sandbox_id, expected_source],
    )?;
    Ok(changed == 1)
}

fn emit_sandbox_state_event(
    id: &str,
    event_type: SandboxEventType,
    from_state: Option<&str>,
    reason: &str,
    operation: &'static str,
    to_state: SandboxState,
    error: Option<&str>,
) {
    let mut details = SandboxEventDetails::default()
        .reason(reason)
        .operation(operation)
        .trigger(SandboxEventTrigger::System)
        .with_to_state(to_state.as_str());
    if let Some(from_state) = from_state {
        details = details.with_from_state(from_state);
    }
    if let Some(error) = error {
        details = details.error(error);
    }
    emit_sandbox_event(id, event_type, SandboxEventSource::SbxManager, details);
}

fn initial_auto_lifecycle_state(
    policy: &AutoLifecyclePolicy,
    state: SandboxState,
) -> AutoLifecycleRuntimeState {
    if state == SandboxState::Started {
        return crate::auto_lifecycle::initial_runtime_state(
            policy,
            crate::auto_lifecycle::now_secs(),
        );
    }
    crate::auto_lifecycle::initial_disarmed_runtime_state(policy)
}

pub fn insert_sandbox(conn: &Connection, params: InsertSandboxParams<'_>) -> Result<()> {
    let InsertSandboxParams {
        id,
        uuid8,
        state,
        template,
        template_hash,
        user,
        runtime,
        snapshots_enabled,
        rootfs_size_mb,
        disk_io_limits,
        pool_template,
        volume_encryption,
        process_env,
        auto_lifecycle,
        network_policy_override,
        network_policy_generation,
        network_policy_authority,
        auto_lifecycle_authority,
    } = params;

    let ts = now()?;
    let process_env_json = serde_json::to_string(process_env)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let auto_lifecycle_json = serde_json::to_string(auto_lifecycle)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let auto_lifecycle_state = initial_auto_lifecycle_state(auto_lifecycle, state);
    let auto_lifecycle_state_json = serde_json::to_string(&auto_lifecycle_state)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let network_policy_override_json = network_policy_override
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "INSERT INTO sandboxes (id, uuid8, state, template, template_hash, user, runtime, snapshots_enabled, rootfs_size_mb, pool_template, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, network_policy_override_json, network_policy_generation, network_policy_authority, auto_lifecycle_authority, volume_encryption, created_at, updated_at, state_changed_at, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id, node_execution_retired)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?21, ?22, ?23, ?24, 1)",
        params![
            id,
            uuid8.as_str(),
            state.as_str(),
            template,
            template_hash,
            user.as_str(),
            runtime,
            snapshots_enabled,
            rootfs_size_mb,
            pool_template,
            process_env_json,
            auto_lifecycle_json,
            auto_lifecycle_state_json,
            auto_lifecycle_state.next_auto_event_at.map(SqliteU64),
            network_policy_override_json.as_deref(),
            u64_to_sql_i64(network_policy_generation),
            network_policy_authority.as_str(),
            auto_lifecycle_authority.as_str(),
            volume_encryption,
            ts,
            ts,
            u64_to_sql_i64(disk_io_limits.max_operations_per_sec()),
            u64_to_sql_i64(disk_io_limits.max_bandwidth_mib_per_sec()),
            // Staging can fail before the first activation. Give its cleanup
            // a durable request identity, but not an allocated predecessor.
            SandboxActivationAttemptId::new().to_string(),
        ],
    )?;
    Ok(())
}

pub(crate) fn node_execution_fence(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<pplx_space_node_mgr_client::execution_fence::ExecutionFence>> {
    use pplx_space_node_mgr_client::execution_fence::ExecutionFence;
    let (current, previous): (Option<String>, Option<String>) = conn.query_row(
        "SELECT node_execution_id, previous_node_execution_id FROM sandboxes WHERE id = ?1",
        params![sandbox_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let parse = |raw: String| {
        raw.parse::<SandboxActivationAttemptId>().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
    };
    match current {
        Some(current) => Ok(Some(ExecutionFence::new(
            parse(current)?,
            previous.map(parse).transpose()?,
        ))),
        None if previous.is_none() => Ok(None),
        None => Err(rusqlite::Error::InvalidQuery),
    }
}

/// Bounded startup preflight; legacy NULL rows require the adoption baseline.
pub(crate) fn unbound_node_executions(conn: &Connection) -> Result<Vec<String>> {
    let mut statement = conn
        .prepare("SELECT id FROM sandboxes WHERE node_execution_id IS NULL ORDER BY id LIMIT ?1")?;
    statement
        .query_map(
            [pplx_space_node_mgr_client::execution_fence::EXECUTION_PREFLIGHT_LIMIT as i64],
            |row| row.get(0),
        )?
        .collect()
}

/// Import receiver authority only while inserting a previously absent orphan
/// row in this transaction. Never use inspection to re-authorize a rejected
/// cleanup of an existing row: the receiver may own a successor execution.
pub(crate) fn inherit_orphan_node_execution(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
    fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
) -> Result<()> {
    let changed = tx.execute(
        "UPDATE sandboxes SET node_execution_id = ?2, previous_node_execution_id = ?3,
                              node_execution_retired = 0
         WHERE id = ?1 AND node_execution_retired = 1 AND previous_node_execution_id IS NULL",
        params![
            sandbox_id,
            fence.current().to_string(),
            fence.previous().map(|id| id.to_string())
        ],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    Ok(())
}

/// A cleanup reply may acknowledge the predecessor when allocation never
/// reached the node. Never overwrite an activation that superseded this request.
pub(crate) fn acknowledge_node_cleanup(
    conn: &Connection,
    sandbox_id: &str,
    expected: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
    retained: Option<SandboxActivationAttemptId>,
) -> Result<bool> {
    if retained.is_some_and(|id| !expected.owns(id)) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(conn.execute(
        "UPDATE sandboxes SET node_execution_id = ?4, previous_node_execution_id = NULL,
                              node_execution_retired = ?5
         WHERE id = ?1 AND node_execution_id = ?2 AND previous_node_execution_id IS ?3",
        params![
            sandbox_id,
            expected.current().to_string(),
            expected.previous().map(|id| id.to_string()),
            retained.unwrap_or(expected.current()).to_string(),
            retained.is_none(),
        ],
    )? == 1)
}

fn insert_cleanup_episode(
    conn: &Connection,
    sandbox_id: &str,
    episode: CleanupEpisodeSpec,
) -> Result<()> {
    conn.execute(
        "INSERT INTO sandbox_cleanup_episodes (
             sandbox_id, cleanup_id, completion, keep_filesystem, phase,
             entered_from_state, activation_state,
             execution_generation, activation_attempt_id, created_at
         ) VALUES (?1, ?2, ?3, ?4, 'armed', ?5, ?6, ?7, ?8, ?9)",
        params![
            sandbox_id,
            episode.id.to_string(),
            episode.completion.as_str(),
            episode.keep_filesystem,
            episode.entered_from_state.map(|state| state.as_str()),
            episode.activation_state.as_str(),
            episode.execution_generation.map(ExecutionGeneration::get),
            episode.activation_attempt_id.map(|id| id.to_string()),
            now_epoch_i64()?,
        ],
    )?;
    conn.execute(
        "UPDATE sandboxes
            SET cleanup_completion = ?1,
                cleanup_keep_filesystem = ?2
          WHERE id = ?3",
        params![
            episode.completion.as_str(),
            episode.keep_filesystem,
            sandbox_id
        ],
    )?;
    Ok(())
}

pub(crate) fn insert_sandbox_with_cleanup_episode_and_route_evidence(
    conn: &Connection,
    params: InsertSandboxParams<'_>,
    episode: CleanupEpisodeSpec,
    route_evidence: Option<crate::auto_lifecycle_delete_receipts::RouteEvidence>,
    open_route_activation: Option<&crate::route_activation_claims::OpenRouteActivationClaim>,
) -> std::result::Result<(), InsertSandboxWithCleanupEpisodeError> {
    if params.state != episode.activation_state {
        return Err(
            InsertSandboxWithCleanupEpisodeError::ActivationStateMismatch {
                sandbox_state: params.state,
                activation_state: episode.activation_state,
            },
        );
    }
    let sandbox_id = params.id.to_string();
    let tx = immediate_transaction(conn)?;
    insert_sandbox(&tx, params)?;
    if let Some(route_evidence) = route_evidence {
        crate::auto_lifecycle_delete_receipts::db::record_retained_route_evidence(
            &tx,
            sandbox_id
                .parse()
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
            route_evidence,
        )?;
    }
    if let Some(open_route_activation) = open_route_activation {
        crate::route_activation_claims::insert_create_claim(
            &tx,
            &sandbox_id,
            open_route_activation,
        )?;
    }
    insert_cleanup_episode(&tx, &sandbox_id, episode)?;
    tx.commit()?;
    Ok(())
}

pub fn upsert_sandbox_execution_metadata(
    conn: &Connection,
    sandbox_id: &str,
    execution: &pplx_space_common_core::types::ExecutionMetadata,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    upsert_sandbox_execution_metadata_in_transaction(&tx, sandbox_id, execution)?;
    tx.commit()?;
    Ok(())
}

fn upsert_sandbox_execution_metadata_in_transaction(
    conn: &Connection,
    sandbox_id: &str,
    execution: &pplx_space_common_core::types::ExecutionMetadata,
) -> Result<()> {
    let runtime: String = conn.query_row(
        "SELECT runtime FROM sandboxes WHERE id = ?1",
        params![sandbox_id],
        |row| row.get(0),
    )?;
    let runtime_matches = runtime
        .parse::<RuntimeKind>()
        .is_ok_and(|runtime| runtime == execution.runtime().kind);
    if !runtime_matches {
        return Err(rusqlite::Error::ToSqlConversionFailure(Box::new(
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "sandbox runtime column disagrees with execution metadata",
            ),
        )));
    }

    let execution_json = serde_json::to_string(execution)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "UPDATE sandboxes
         SET execution_metadata_json = ?2,
             updated_at = ?3
         WHERE id = ?1",
        params![sandbox_id, execution_json, now()?],
    )?;
    Ok(())
}

pub(crate) fn insert_recovered_suspended_sandbox_in_transaction(
    conn: &Connection,
    params: InsertSandboxParams<'_>,
    execution: &ExecutionMetadata,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> std::result::Result<(), InsertRecoveredSuspendedSandboxError> {
    if params.state != SandboxState::Suspended {
        return Err(InsertRecoveredSuspendedSandboxError::NonSuspendedInsert {
            state: params.state,
        });
    }
    let sandbox_id = params.id.to_string();
    insert_sandbox(conn, params)?;
    upsert_sandbox_execution_metadata_in_transaction(conn, &sandbox_id, execution)?;
    insert_local_artifact(
        conn,
        &sandbox_id,
        local_artifact_id,
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
        local_path,
    )?;
    let changed = conn.execute(
        "UPDATE sandboxes
         SET current_local_artifact_id = ?1,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend',
             updated_at = ?2
         WHERE id = ?3
           AND state = 'suspended'
           AND current_local_artifact_id IS NULL",
        params![local_artifact_id.to_string(), now()?, sandbox_id],
    )?;
    if changed != 1 {
        return Err(InsertRecoveredSuspendedSandboxError::ArtifactPinInvariant { changed });
    }
    Ok(())
}

/// Adopts or inserts a restore's sandbox row in a transaction of its own.
///
/// Callers already inside a transaction — the quarantine-recovery paths, which
/// have to commit the row and its claim together — use
/// `restore_adopt_orphan_or_insert_sandbox_in_transaction` directly.
pub fn restore_adopt_orphan_or_insert_sandbox(
    conn: &Connection,
    params: InsertSandboxParams<'_>,
    restore_operation: &RemoteRestoreOperation,
) -> std::result::Result<(), RestoreAdoptOrphanInsertError> {
    let tx = immediate_transaction(conn)?;
    restore_adopt_orphan_or_insert_sandbox_in_transaction(&tx, params, restore_operation)?;
    tx.commit()?;
    Ok(())
}

pub(crate) fn restore_adopt_orphan_or_insert_sandbox_in_transaction(
    conn: &Connection,
    params: InsertSandboxParams<'_>,
    restore_operation: &RemoteRestoreOperation,
) -> std::result::Result<(), RestoreAdoptOrphanInsertError> {
    let InsertSandboxParams {
        id,
        uuid8,
        state,
        template,
        template_hash,
        user,
        runtime,
        snapshots_enabled,
        rootfs_size_mb,
        disk_io_limits,
        pool_template,
        volume_encryption,
        process_env,
        auto_lifecycle,
        network_policy_override,
        network_policy_generation,
        network_policy_authority,
        auto_lifecycle_authority,
    } = params;

    if state != SandboxState::Restoring {
        return Err(RestoreAdoptOrphanInsertError::NonRestoringState { state });
    }
    let ts = now()?;
    let process_env_json = serde_json::to_string(process_env)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let auto_lifecycle_json = serde_json::to_string(auto_lifecycle)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let auto_lifecycle_state = initial_auto_lifecycle_state(auto_lifecycle, state);
    let auto_lifecycle_state_json = serde_json::to_string(&auto_lifecycle_state)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let network_policy_override_json = network_policy_override
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let orphan_auto_lifecycle_json = serde_json::to_string(&AutoLifecyclePolicy::disabled())
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;

    let rows_affected = conn.execute(
        "INSERT INTO sandboxes (id, uuid8, state, template, template_hash, user, runtime, snapshots_enabled, rootfs_size_mb, pool_template, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, network_policy_override_json, network_policy_generation, network_policy_authority, auto_lifecycle_authority, volume_encryption, created_at, updated_at, state_changed_at, restore_origin, restore_phase, restore_storage_uri, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id, node_execution_retired)
         VALUES (?1, ?2, 'restoring', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?23, ?18, ?18, ?18, 'remote', ?24, ?25, ?26, ?27, ?28, 1)
         ON CONFLICT(id) DO UPDATE SET
             uuid8 = excluded.uuid8,
             state = excluded.state,
             state_changed_at = CASE
                 WHEN sandboxes.state IS NOT excluded.state THEN excluded.state_changed_at
                 ELSE sandboxes.state_changed_at
             END,
             template = excluded.template,
             template_hash = excluded.template_hash,
             user = excluded.user,
             runtime = excluded.runtime,
             uid = NULL,
             slot = NULL,
             guest_cid = NULL,
             pid = NULL,
             spaced_host_pid = NULL,
             netgtw_pid = NULL,
             vsock_uds_path = NULL,
             api_socket_path = NULL,
             space_api_route_generation = NULL,
             space_api_route_node_id = NULL,
             space_api_route_execution_generation = NULL,
             space_api_route_pod_uid = NULL,
             space_api_route_substrate_kind = NULL,
             space_api_route_activation_attempt_id = NULL,
             space_api_route_identity_provenance = NULL,
             spaced_endpoint_proof_json = NULL,
             snapshots_enabled = excluded.snapshots_enabled,
             rootfs_size_mb = excluded.rootfs_size_mb,
             disk_io_max_operations_per_sec = excluded.disk_io_max_operations_per_sec,
             disk_io_max_bandwidth_mib_per_sec = excluded.disk_io_max_bandwidth_mib_per_sec,
             pool_template = excluded.pool_template,
             volume_encryption = excluded.volume_encryption,
             guest_uptime_secs = NULL,
             process_env_json = excluded.process_env_json,
             auto_lifecycle_json = excluded.auto_lifecycle_json,
             auto_lifecycle_state_json = excluded.auto_lifecycle_state_json,
             next_auto_event_at = excluded.next_auto_event_at,
             auto_lifecycle_delete_block_reason = NULL,
             auto_lifecycle_delete_blocked_at = NULL,
             failure_reason = NULL,
             cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             network_tx_bytes = 0,
             network_rx_bytes = 0,
             applied_network_policy_generation = 0,
             applied_network_policy_source = '',
             auto_lifecycle_authority = excluded.auto_lifecycle_authority,
             restore_origin = excluded.restore_origin,
             restore_phase = excluded.restore_phase,
             restore_storage_uri = excluded.restore_storage_uri,
             created_at = excluded.created_at,
             updated_at = excluded.updated_at
         WHERE sandboxes.state IN ('quarantined', 'restoring')
           AND EXISTS (
               SELECT 1 FROM sandbox_quarantine_state qs
                WHERE qs.sandbox_id = sandboxes.id
                  AND qs.source = 'sbx_manager'
                  AND qs.reason_code = 'orphaned_sandbox_row_missing'
           )
           AND sandboxes.template = ?19
           AND sandboxes.template_hash IS NULL
           AND sandboxes.user = ?20
           AND sandboxes.runtime = ?21
           AND sandboxes.uid IS NULL
           AND sandboxes.slot IS NULL
           AND sandboxes.guest_cid IS NULL
           AND sandboxes.pid IS NULL
           AND sandboxes.spaced_host_pid IS NULL
           AND sandboxes.netgtw_pid IS NULL
           AND sandboxes.vsock_uds_path IS NULL
           AND sandboxes.api_socket_path IS NULL
           AND sandboxes.snapshots_enabled = 0
           AND sandboxes.pool_template IS NULL
           AND sandboxes.guest_uptime_secs IS NULL
           AND sandboxes.process_env_json = '{}'
           AND sandboxes.auto_lifecycle_json = ?22
           AND sandboxes.cleanup_completion IS NULL
           AND sandboxes.cleanup_keep_filesystem IS NULL
           AND sandboxes.applied_network_policy_generation = 0
           AND sandboxes.applied_network_policy_source = ''
           AND sandboxes.network_policy_authority = 'cluster'
           AND sandboxes.auto_lifecycle_authority = 'cluster'",
        params![
            id,
            uuid8.as_str(),
            template,
            template_hash,
            user.as_str(),
            runtime,
            snapshots_enabled,
            rootfs_size_mb,
            pool_template,
            process_env_json,
            auto_lifecycle_json,
            auto_lifecycle_state_json,
            auto_lifecycle_state.next_auto_event_at.map(SqliteU64),
            network_policy_override_json.as_deref(),
            u64_to_sql_i64(network_policy_generation),
            network_policy_authority.as_str(),
            auto_lifecycle_authority.as_str(),
            ts,
            ORPHANED_SANDBOX_TEMPLATE,
            ORPHANED_SANDBOX_OWNER.as_str(),
            RuntimeKind::Firecracker.alias(),
            orphan_auto_lifecycle_json,
            volume_encryption,
            restore_operation.phase().as_str(),
            restore_operation.source().as_wire(),
            u64_to_sql_i64(disk_io_limits.max_operations_per_sec()),
            u64_to_sql_i64(disk_io_limits.max_bandwidth_mib_per_sec()),
            // New restore staging needs cleanup authority before activation;
            // the conflict branch preserves the adopted row's authority.
            SandboxActivationAttemptId::new().to_string(),
        ],
    )?;

    if rows_affected == 1 {
        return Ok(());
    }

    // The synthetic orphan reconciler has no dedicated marker column today, so
    // adoption is gated on its exact stable column tuple. Quarantined rollout
    // residue additionally requires the durable quarantine marker; a conflict
    // that misses these fences is a real sandbox row and must not be clobbered.
    Err(RestoreAdoptOrphanInsertError::ExistingRowNotSyntheticOrphan { id: id.to_string() })
}

pub fn update_snapshots_enabled(conn: &Connection, id: &str, enabled: bool) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET snapshots_enabled = ?1, updated_at = ?2 WHERE id = ?3",
        params![enabled, now()?, id],
    )?;
    Ok(())
}

pub fn update_state(
    conn: &Connection,
    id: &str,
    state: SandboxState,
) -> Result<SandboxStateWriteOutcome> {
    if matches!(state, SandboxState::Quarantined | SandboxState::Failed) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let previous_state = current_state_label(conn, id)?;
    let previous_state_value = previous_state
        .as_deref()
        .and_then(|state| state.parse::<SandboxState>().ok());
    if generic_writer_transition_blocked(previous_state_value, state) {
        tracing::warn!(
            sandbox_id = %id,
            target_state = %state,
            "ignored generic sandbox state update because sandbox is quarantined"
        );
        return Ok(SandboxStateWriteOutcome::BlockedByQuarantine);
    }
    if cleanup_episode(conn, id)?
        .is_some_and(|episode| !episode.completion.deletes_row() || episode.requested_at.is_some())
        || (matches!(
            previous_state_value,
            Some(
                SandboxState::Deleting
                    | SandboxState::CleanupRequired
                    | SandboxState::CleanupFailed
            )
        ) && state != SandboxState::Deleting
            && cleanup_intent_current(conn, id, CleanupCompletion::DeleteRow)?)
        || (matches!(
            previous_state_value,
            Some(
                SandboxState::Stopping
                    | SandboxState::CleanupRequired
                    | SandboxState::CleanupFailed
            )
        ) && matches!(
            state,
            SandboxState::Started
                | SandboxState::Paused
                | SandboxState::Pausing
                | SandboxState::Resuming
                | SandboxState::Checkpointing
        ))
    {
        tracing::warn!(
            sandbox_id = %id,
            target_state = %state,
            "ignored generic sandbox state update because cleanup owns the transition"
        );
        return Ok(SandboxStateWriteOutcome::SupersededByCleanupEpisode);
    }

    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = ?1,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             failure_reason = NULL,
             restore_origin = CASE WHEN ?1 = 'restoring' THEN restore_origin ELSE NULL END,
             restore_phase = CASE WHEN ?1 = 'restoring' THEN restore_phase ELSE NULL END,
             restore_storage_uri = CASE WHEN ?1 = 'restoring' THEN restore_storage_uri ELSE NULL END,
             transition_receipt_id = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_id
                 ELSE NULL
             END,
             transition_receipt_kind = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_kind
                 ELSE NULL
             END,
             transition_receipt_target_state = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_target_state
                 ELSE NULL
             END,
             transition_receipt_execution_generation = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_execution_generation
                 ELSE NULL
             END,
             transition_receipt_activation_attempt_id = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_activation_attempt_id
                 ELSE NULL
             END,
             transition_receipt_reason_code = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_reason_code
                 ELSE NULL
             END,
             updated_at = ?2
         WHERE id = ?3
           AND (?1 = 'deleting' OR state != 'quarantined')",
        params![state.as_str(), now()?, id],
    )?;
    Ok(if changed == 0 {
        SandboxStateWriteOutcome::Missing
    } else {
        SandboxStateWriteOutcome::Applied
    })
}

/// Check the observation against the current incarnation before replaying
/// guest setup. The caller holds the wake and cleanup fences across setup.
pub(crate) fn startup_resident_witness_current(
    conn: &Connection,
    observed: &SandboxRow,
) -> Result<bool> {
    let Some(current) = get_sandbox(conn, &observed.id)? else {
        return Ok(false);
    };
    Ok(inventory_deletion_witness_matches(&current, observed)
        && current.uuid8 == observed.uuid8
        && current.space_api_route_identity == observed.space_api_route_identity
        && current.process_env == observed.process_env
        && current.spaced_endpoint_proof == observed.spaced_endpoint_proof
        && current.cleanup_completion.is_none()
        && current.cleanup_keep_filesystem.is_none()
        // Presence alone blocks competing recovery, including ownership written
        // by a newer binary that this reader cannot decode.
        && !has_cleanup_episode(conn, &observed.id)?
        && !conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sandbox_restore_transfer_scopes WHERE sandbox_id = ?1)",
            [&observed.id],
            |row| row.get::<_, bool>(0),
        )?)
}

/// The settlement determines both durable state and event semantics. Healthy
/// outcomes cannot carry quarantine reasons or errors.
pub(crate) enum StartupResidentSettlement<'a> {
    Started,
    Paused,
    Quarantined {
        reason_code: SandboxQuarantineReasonCode,
        message: &'a str,
    },
}

/// Commit lifecycle and quarantine evidence together. A changed row or durable
/// cleanup owner supersedes the entire observation.
pub(crate) fn settle_startup_resident(
    conn: &Connection,
    observed: &SandboxRow,
    settlement: StartupResidentSettlement<'_>,
) -> Result<bool> {
    if !matches!(
        observed.state,
        SandboxState::Started
            | SandboxState::Paused
            | SandboxState::Resuming
            | SandboxState::Checkpointing
    ) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let (target, reason, error) = match &settlement {
        StartupResidentSettlement::Started => (
            SandboxState::Started,
            "startup_recovery_runtime_state_observed",
            None,
        ),
        StartupResidentSettlement::Paused => (
            SandboxState::Paused,
            "startup_recovery_runtime_state_observed",
            None,
        ),
        StartupResidentSettlement::Quarantined {
            reason_code,
            message,
        } => (
            SandboxState::Quarantined,
            reason_code.as_str(),
            Some(*message),
        ),
    };
    let tx = immediate_transaction(conn)?;
    if !startup_resident_witness_current(&tx, observed)? {
        return Ok(false);
    }
    let timestamp = now()?;
    // A receipt acknowledges its current target, not historical success.
    // Preserve a matching healthy acknowledgement across recovery, but retire
    // every receipt on quarantine in the same transaction as its evidence.
    tx.execute(
        "UPDATE sandboxes SET state = ?1, failure_reason = ?2,
             transition_receipt_id = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_id
                 ELSE NULL
             END,
             transition_receipt_kind = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_kind
                 ELSE NULL
             END,
             transition_receipt_target_state = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_target_state
                 ELSE NULL
             END,
             transition_receipt_execution_generation = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_execution_generation
                 ELSE NULL
             END,
             transition_receipt_activation_attempt_id = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_activation_attempt_id
                 ELSE NULL
             END,
             transition_receipt_reason_code = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_reason_code
                 ELSE NULL
             END,
             state_changed_at = ?3, updated_at = ?3 WHERE id = ?4",
        params![target.as_str(), error, timestamp, observed.id],
    )?;
    if let StartupResidentSettlement::Quarantined {
        reason_code,
        message,
    } = settlement
    {
        crate::route_activation_claims::close_claim(&tx, &observed.id)?;
        insert_new_quarantine_episode(
            &tx,
            &QuarantineEpisode {
                sandbox_id: &observed.id,
                entered_from_state: Some(observed.state.as_str()),
                source: SandboxQuarantineSource::SbxManager,
                reason_code,
                reason_message: Some(message),
                next_action: SandboxQuarantineNextAction::RequiresOperatorAttention,
                timestamp: &timestamp,
            },
        )?;
    }
    tx.commit()?;
    emit_sandbox_state_event(
        &observed.id,
        if target == SandboxState::Quarantined {
            SandboxEventType::SandboxQuarantined
        } else {
            SandboxEventType::LifecycleConverged
        },
        Some(observed.state.as_str()),
        reason,
        "startup_resident_recovery",
        target,
        error,
    );
    Ok(true)
}

pub(crate) fn restore_checkpoint_state(
    conn: &Connection,
    id: &str,
    stable_state: SandboxState,
) -> Result<bool> {
    if !matches!(stable_state, SandboxState::Started | SandboxState::Paused) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'checkpointing'",
        params![stable_state.as_str(), now()?, id],
    )?;
    Ok(changed == 1)
}

/// Fence policy installation and Paused -> Resuming in one transaction.
/// An absent generation matches only an identity-free legacy resident; it is
/// never a wildcard for an execution that has acquired a generation.
pub(crate) fn begin_resident_resume_with_policy(
    conn: &Connection,
    id: &str,
    generation: Option<ExecutionGeneration>,
    policy: &AutoLifecyclePolicy,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let Some(sandbox) = get_sandbox(&tx, id)? else {
        return Ok(false);
    };
    if sandbox.state != SandboxState::Paused
        || sandbox.space_api_route_identity.execution_generation != generation
    {
        return Ok(false);
    }
    if update_state(&tx, id, SandboxState::Resuming)? != SandboxStateWriteOutcome::Applied {
        return Ok(false);
    }
    install_activation_auto_lifecycle(&tx, id, policy)?;
    tx.commit()?;
    Ok(true)
}

pub(crate) fn restore_resident_resume_state(
    conn: &Connection,
    id: &str,
    stable_state: SandboxState,
) -> Result<bool> {
    if !matches!(stable_state, SandboxState::Started | SandboxState::Paused) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let tx = immediate_transaction(conn)?;
    let changed = rollback_activation_state(&tx, id, SandboxState::Resuming, stable_state)?;
    if changed && stable_state == SandboxState::Started {
        arm_auto_lifecycle_after_activation(&tx, id)?;
    }
    tx.commit()?;
    Ok(changed)
}

pub(crate) fn recover_interrupted_pause_state(
    conn: &Connection,
    id: &str,
    target: PauseFailedRecoveryTarget,
) -> Result<bool> {
    let target_state = target.sandbox_state();
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'pausing'",
        params![target_state.as_str(), now()?, id],
    )?;
    Ok(changed == 1)
}

/// Transfer lifecycle ownership from an armed activation to an explicit
/// stop/delete teardown. The caller is responsible for completing that
/// teardown; the activation guard becomes a superseded no-op.
fn update_state_superseding_activation(
    conn: &Connection,
    id: &str,
    state: SandboxState,
) -> std::result::Result<(), SupersedeActivationError> {
    debug_assert!(matches!(
        state,
        SandboxState::Stopping | SandboxState::Deleting
    ));
    let tx = immediate_transaction(conn)?;
    update_state_superseding_activation_in_transaction(&tx, id, state)?;
    tx.commit()?;
    Ok(())
}

pub(crate) fn update_state_superseding_activation_in_transaction(
    conn: &Connection,
    id: &str,
    state: SandboxState,
) -> std::result::Result<(), SupersedeActivationError> {
    debug_assert!(matches!(
        state,
        SandboxState::Stopping | SandboxState::Deleting
    ));
    if let Some(episode) = cleanup_episode(conn, id)? {
        if state == SandboxState::Stopping
            && (episode.entered_from_state == Some(SandboxState::Quarantined)
                || episode.quarantine_recovery.is_some())
        {
            return Err(SupersedeActivationError::QuarantineRecoveryInProgress);
        }
        conn.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, episode.id.to_string()],
        )?;
        conn.execute(
            "UPDATE sandboxes
                SET cleanup_completion = NULL,
                    cleanup_keep_filesystem = NULL
              WHERE id = ?1
                AND cleanup_completion = ?2
                AND cleanup_keep_filesystem = ?3",
            params![id, episode.completion.as_str(), episode.keep_filesystem],
        )?;
    }
    update_state(conn, id, state)?.require_applied()?;
    Ok(())
}

pub fn mark_stopping_superseding_activation(
    conn: &Connection,
    id: &str,
) -> std::result::Result<(), SupersedeActivationError> {
    update_state_superseding_activation(conn, id, SandboxState::Stopping)
}

pub fn mark_deleting_superseding_activation(
    conn: &Connection,
    id: &str,
) -> std::result::Result<(), SupersedeActivationError> {
    update_state_superseding_activation(conn, id, SandboxState::Deleting)
}

/// Claim the caller-observed sandbox state for activation.
///
/// A false result means the row was removed or another state transition won;
/// the caller must not allocate or launch runtime resources.
pub fn begin_activation(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
    kind: ActivationKind,
    execution_generation: Option<ExecutionGeneration>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
) -> std::result::Result<Option<CleanupEpisode>, BeginActivationError> {
    begin_activation_with_route_evidence(
        conn,
        id,
        expected_state,
        kind,
        execution_generation,
        activation_attempt_id,
        ActivationRouteAdmission::default(),
    )
}

#[derive(Default)]
pub(crate) struct ActivationRouteAdmission<'a> {
    pub(crate) auto_lifecycle: Option<&'a AutoLifecyclePolicy>,
    pub(crate) operation_activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub(crate) evidence: Option<crate::auto_lifecycle_delete_receipts::RouteEvidence>,
    pub(crate) open_claim: Option<&'a crate::route_activation_claims::OpenRouteActivationClaim>,
}

pub(crate) fn begin_activation_with_route_evidence(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
    kind: ActivationKind,
    execution_generation: Option<ExecutionGeneration>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    route_admission: ActivationRouteAdmission<'_>,
) -> std::result::Result<Option<CleanupEpisode>, BeginActivationError> {
    let completion = match (kind, expected_state) {
        (ActivationKind::ColdStart, SandboxState::Created) => CleanupCompletion::MarkCreated,
        (ActivationKind::ColdStart, SandboxState::Stopped) => CleanupCompletion::MarkStopped,
        (ActivationKind::ColdStart, SandboxState::Suspended) => CleanupCompletion::MarkSuspended,
        (ActivationKind::Restore, SandboxState::Suspended) => CleanupCompletion::MarkSuspended,
        _ => {
            return Err(BeginActivationError::UnsupportedState {
                kind,
                state: expected_state,
            });
        }
    };
    let activation_state = kind.activation_state();
    let tx = immediate_transaction(conn)?;
    if cleanup_episode(&tx, id)?.is_some() {
        tx.commit()?;
        return Ok(None);
    }
    let keep_filesystem = true;
    // Only activation authorizes node allocation. Filesystem-only staging keeps
    // an unbound request identity and must not become an allocation predecessor.
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = NULL,
             restore_origin = CASE WHEN ?1 = 'restoring' THEN 'local' ELSE NULL END,
             restore_phase = CASE WHEN ?1 = 'restoring' THEN 'activation' ELSE NULL END,
             restore_storage_uri = NULL,
             cleanup_completion = ?2,
             cleanup_keep_filesystem = ?3,
             previous_node_execution_id = CASE WHEN node_execution_retired = 0 THEN node_execution_id END,
             node_execution_id = ?8,
             node_execution_retired = 0,
             transition_receipt_id = NULL,
             transition_receipt_kind = NULL,
             transition_receipt_target_state = NULL,
             transition_receipt_execution_generation = NULL,
             transition_receipt_reason_code = NULL,
             transition_receipt_activation_attempt_id = NULL,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?6 ELSE state_changed_at END,
             updated_at = ?6
         WHERE id = ?4
           AND state = ?5
           AND state != 'quarantined'
           AND NOT EXISTS (
               SELECT 1
                 FROM auto_lifecycle_delete_outbox
                WHERE sandbox_id = ?4
                  AND state = 'pending'
                  AND (?7 IS NULL OR execution_generation >= ?7)
           )
           AND (
               (cleanup_completion IS NULL AND cleanup_keep_filesystem IS NULL)
               OR (cleanup_completion = ?2 AND cleanup_keep_filesystem = ?3)
           )",
        params![
            activation_state.as_str(),
            completion.as_str(),
            keep_filesystem,
            id,
            expected_state.as_str(),
            now()?,
            execution_generation.map(ExecutionGeneration::get),
            SandboxActivationAttemptId::new().to_string(),
        ],
    )?;
    if changed != 1 {
        tx.commit()?;
        return Ok(None);
    }
    if let Some(policy) = route_admission.auto_lifecycle {
        install_activation_auto_lifecycle(&tx, id, policy)?;
    }
    let sandbox_id = (route_admission.evidence.is_some() || execution_generation.is_some())
        .then(|| {
            id.parse::<SandboxId>()
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        })
        .transpose()?;
    match route_admission.open_claim {
        Some(open_route_activation) => {
            crate::route_activation_claims::insert_create_claim(&tx, id, open_route_activation)?;
        }
        None => {
            // Beginning any other activation supersedes an older Kubernetes
            // continuation ticket in the same transaction that claims the row.
            crate::route_activation_claims::close_claim(&tx, id)?;
        }
    }
    if let Some(route_evidence) = route_admission.evidence {
        let sandbox_id = sandbox_id.ok_or(rusqlite::Error::InvalidQuery)?;
        crate::auto_lifecycle_delete_receipts::db::record_retained_route_evidence(
            &tx,
            sandbox_id,
            route_evidence,
        )?;
    }
    let receipt_supersession = match sandbox_id {
        Some(sandbox_id) => crate::auto_lifecycle_delete_receipts::db::discard_pending_receipt_for_execution_advance(
            &tx,
            sandbox_id,
            execution_generation,
        )?,
        None => None,
    };
    let cleanup_id = SandboxCleanupEpisodeId::new();
    insert_cleanup_episode(
        &tx,
        id,
        CleanupEpisodeSpec {
            id: cleanup_id,
            completion,
            keep_filesystem,
            entered_from_state: Some(expected_state),
            activation_state,
            execution_generation,
            activation_attempt_id,
        },
    )?;
    tx.execute(
        "UPDATE sandbox_cleanup_episodes SET operation_activation_attempt_id = ?3
         WHERE sandbox_id = ?1 AND cleanup_id = ?2",
        params![
            id,
            cleanup_id.to_string(),
            route_admission
                .operation_activation_attempt_id
                .map(|id| id.to_string())
        ],
    )?;
    let episode = cleanup_episode(&tx, id)?.filter(|episode| episode.id == cleanup_id);
    tx.commit()?;
    if let Some(sandbox_id) = sandbox_id {
        emit_auto_lifecycle_delete_receipt_superseded_if_present(sandbox_id, receipt_supersession);
    }
    Ok(episode)
}

pub fn sandbox_is_retryable_interrupted_restore(row: &SandboxRow) -> bool {
    row.state == SandboxState::Restoring
        && row
            .restore_operation
            .as_ref()
            .is_some_and(|operation| operation.origin() == RestoreOrigin::Remote)
        && !sandbox_has_runtime_fields(row)
        && row.current_local_artifact_id.is_none()
        && row.cleanup_completion.is_none()
        && row.cleanup_keep_filesystem.is_none()
}

pub fn canonical_restore_reclaim_attempt_is_current(
    conn: &Connection,
    sandbox_id: &str,
    transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
    execution_generation: ExecutionGeneration,
    activation_attempt_id: SandboxActivationAttemptId,
) -> Result<bool> {
    let Some(row) = get_sandbox(conn, sandbox_id)? else {
        return Ok(false);
    };
    if !sandbox_is_retryable_interrupted_restore(&row) {
        return Ok(false);
    }
    Ok(
        restore_transfer_scope(conn, sandbox_id)?.is_some_and(|scope| {
            scope.transfer_scope_id == transfer_scope_id
                && scope.execution_generation == Some(execution_generation)
                && scope.activation_attempt_id == Some(activation_attempt_id)
        }),
    )
}

/// Persists a cross-node staging failure on behalf of a recovery claim.
///
/// The target row is the claim's own sandbox, bound from the fence rather than
/// supplied alongside it. Taking the id separately let a caller fence one episode
/// and write another sandbox's row — every sibling fenced statement targets
/// `:fence_sandbox_id`, and this was the one exception.
///
/// The rootfs pull is a transfer, so a claim checked before it can be superseded
/// while it runs. Guarding the write on the claim inside the same statement is
/// what keeps a spent claimant from recording its failure as an ordinary
/// deferral on an episode that now belongs to someone else — a check before the
/// write would leave exactly that window open.
pub fn mark_cross_node_restore_staging_failed_under_recovery_claim(
    conn: &Connection,
    failure_reason: &str,
    claimed: &ClaimedQuarantineRecoveryCandidate,
) -> Result<bool> {
    let fence = ExactClaimFence::new(
        conn,
        claimed,
        now_epoch_i64()?,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::MustBeActivatable,
        CleanupRequirement::MustBeAbsent,
    )?;
    let sql = format!(
        "UPDATE sandboxes
            SET failure_reason = :failure_reason,
                updated_at = :updated_at
          WHERE id = :fence_sandbox_id
            AND state = 'restoring'
            AND EXISTS (
                SELECT 1
                  FROM sandbox_quarantine_state AS qs
                 WHERE {}{}
            )",
        fence.quarantine_row_predicate("qs."),
        fence.sandbox_guards("qs.sandbox_id"),
    );
    let updated_at = now()?;
    let mut binds = fence.binds();
    binds.push((":failure_reason", &failure_reason));
    binds.push((":updated_at", &updated_at));
    let changed = conn.prepare_cached(&sql)?.execute(&binds[..])?;
    Ok(changed == 1)
}

/// Stamp the terminal-teardown-suspend marker on a `suspending` row whose
/// durable backup the cluster refused permanently.
///
/// Scoped to the exact staged artifact the failed backup covered, so a row
/// that has already moved on keeps its own witness. The write is conditional
/// on the reason actually changing: the shim re-polls `PrepareTeardownSuspend`
/// for as long as the pod is terminating, and each poll re-drives the suspend
/// through this same terminal job. An unconditional stamp would bump
/// `updated_at` on every poll and hold the row permanently inside the
/// cluster-deleted eviction's quiescence window — the one thing that can
/// reclaim it.
pub fn mark_teardown_suspend_backup_permanently_failed(
    conn: &Connection,
    id: &str,
    staged_artifact: LocalArtifactId,
) -> Result<bool> {
    let reason = pplx_space_sbx_mgr_client::TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON;
    let changed = conn.execute(
        "UPDATE sandboxes
         SET failure_reason = ?1,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'suspending'
           AND current_local_artifact_id = ?4
           AND (failure_reason IS NULL OR failure_reason != ?1)",
        params![reason, now()?, id, staged_artifact.to_string()],
    )?;
    Ok(changed == 1)
}

pub fn mark_cross_node_restore_staging_failed(
    conn: &Connection,
    id: &str,
    failure_reason: &str,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET failure_reason = ?1,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'restoring'",
        params![failure_reason, now()?, id],
    )?;
    Ok(changed == 1)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ActivationOperationError {
    #[error("activation receipt database operation failed: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("sandbox no longer exists")]
    MissingSandbox,
    #[error("activation route evidence does not match the selected execution")]
    RouteMismatch,
    #[error("activation operation was superseded")]
    Superseded,
    #[error("activation target has not been reached or is unsupported")]
    InvalidTarget,
    #[error("activation cleanup is still pending")]
    CleanupPending,
}

/// Join an armed activation without changing its route-ownership fence. The
/// update is durable before a Wake waiter can disappear; cleanup will report
/// the joined operation even if the original worker finishes asynchronously.
pub(crate) fn bind_activation_operation(
    conn: &Connection,
    id: &str,
    route: &SpaceApiRouteIdentity,
    operation: SandboxActivationAttemptId,
) -> std::result::Result<(), ActivationOperationError> {
    let tx = immediate_transaction(conn)?;
    if let Some(episode) = cleanup_episode(&tx, id)?
        && episode.requested_at.is_none()
    {
        if episode.execution_generation != route.execution_generation
            || episode.activation_attempt_id != route.activation_attempt_id
        {
            return Err(ActivationOperationError::RouteMismatch);
        }
        if episode
            .operation_activation_attempt_id
            .is_some_and(|current| current > operation)
        {
            return Err(ActivationOperationError::Superseded);
        }
        tx.execute(
            "UPDATE sandbox_cleanup_episodes SET operation_activation_attempt_id = ?3
             WHERE sandbox_id = ?1 AND cleanup_id = ?2 AND phase = 'armed'",
            params![id, episode.id.to_string(), operation.to_string()],
        )?;
    }
    tx.commit().map_err(Into::into)
}

/// A resident Wake has no cleanup episode. Persist fresh completion evidence
/// while its wake lock is held, after identity adoption and before fallible
/// gateway sync. Lifecycle completion does not depend on that network call. This
/// also closes a join whose activation completed before we bound its episode.
pub(crate) fn complete_resident_wake_operation(
    conn: &Connection,
    id: &str,
    route: &SpaceApiRouteIdentity,
    operation: SandboxActivationAttemptId,
    target: SandboxState,
) -> std::result::Result<(), ActivationOperationError> {
    let tx = immediate_transaction(conn)?;
    let row = get_sandbox(&tx, id)?.ok_or(ActivationOperationError::MissingSandbox)?;
    let actual = &row.space_api_route_identity;
    if row.state != target {
        return Err(ActivationOperationError::InvalidTarget);
    }
    if cleanup_episode(&tx, id)?.is_some() {
        return Err(ActivationOperationError::CleanupPending);
    }
    if actual.clone().try_into_complete().is_err()
        || actual.execution_generation != route.execution_generation
        || actual.route_generation != route.route_generation
        || actual.activation_attempt_id != route.activation_attempt_id
        || actual.pod_uid != route.pod_uid
        || actual.effective_substrate_kind() != route.effective_substrate_kind()
    {
        return Err(ActivationOperationError::RouteMismatch);
    }
    let generation = route
        .execution_generation
        .ok_or(ActivationOperationError::RouteMismatch)?;
    let target =
        activation_completed_target(target).ok_or(ActivationOperationError::InvalidTarget)?;
    let previous: (Option<i64>, Option<String>, Option<String>, Option<String>) = tx.query_row(
        "SELECT transition_receipt_execution_generation, transition_receipt_activation_attempt_id,
                transition_receipt_kind, transition_receipt_target_state
         FROM sandboxes WHERE id = ?1",
        [id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    if previous.0 == Some(generation.get())
        && let Some(previous_id) = previous.1
    {
        let previous_operation: SandboxActivationAttemptId =
            previous_id.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(error))
            })?;
        if previous_operation > operation {
            // Do not replace a potentially newer completion. UUID ordering is
            // only a conservative receipt-retention policy: PostgreSQL clears
            // completed owners, so clock skew can make a legitimate later
            // no-op Wake's ID lower. It must not reject that healthy Wake.
            // Its caller can release the exact lease from lifecycle readiness;
            // no activation cleanup obligation exists on this resident path.
            return tx.commit().map_err(Into::into);
        }
        if previous_operation == operation
            && previous.2.as_deref() == Some("activation_completed")
            && previous.3.as_deref() == Some(target.as_str())
        {
            return tx.commit().map_err(Into::into);
        }
    }
    tx.execute(
        "UPDATE sandboxes SET transition_receipt_id = ?2,
            transition_receipt_kind = 'activation_completed',
            transition_receipt_target_state = ?3,
            transition_receipt_execution_generation = ?4,
            transition_receipt_activation_attempt_id = ?5,
            transition_receipt_reason_code = NULL, updated_at = ?6
         WHERE id = ?1",
        params![
            id,
            SandboxCleanupEpisodeId::new().to_string(),
            target.as_str(),
            generation.get(),
            operation.to_string(),
            now()?
        ],
    )?;
    tx.commit().map_err(Into::into)
}

fn activation_completed_target(state: SandboxState) -> Option<SandboxTransitionTargetState> {
    match state {
        SandboxState::Created => Some(SandboxTransitionTargetState::Created),
        SandboxState::Started => Some(SandboxTransitionTargetState::Started),
        SandboxState::Paused => Some(SandboxTransitionTargetState::Paused),
        _ => None,
    }
}

/// Leave a durable acknowledgement for the exact control-plane activation
/// attempt before retiring its cleanup episode. The API may clear the attempt
/// eagerly, but this receipt closes the crash window between node success and
/// that best-effort control-plane write.
fn persist_activation_completed_receipt(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    target_state: SandboxState,
) -> Result<()> {
    let Some(episode) = cleanup_episode(tx, sandbox_id)?.filter(|episode| episode.id == cleanup_id)
    else {
        return Ok(());
    };
    let (Some(execution_generation), Some(activation_attempt_id), Some(target_state)) = (
        episode.execution_generation,
        // Explicit operation ownership wins; old episodes retain legacy receipts.
        episode
            .operation_activation_attempt_id
            .or(episode.activation_attempt_id),
        activation_completed_target(target_state),
    ) else {
        return Ok(());
    };
    let receipt = SandboxTransitionReceipt::<SandboxQuarantineReasonCode>::activation_completed(
        cleanup_id,
        target_state,
        execution_generation,
        activation_attempt_id,
    );
    tx.execute(
        "UPDATE sandboxes
            SET transition_receipt_id = ?1,
                transition_receipt_kind = ?2,
                transition_receipt_target_state = ?3,
                transition_receipt_execution_generation = ?4,
                transition_receipt_activation_attempt_id = ?5,
                transition_receipt_reason_code = NULL
          WHERE id = ?6 AND state = ?3",
        params![
            receipt.id().to_string(),
            receipt.kind().as_str(),
            receipt.target_state().as_str(),
            receipt.execution_generation().get(),
            receipt.activation_attempt_id().map(|id| id.to_string()),
            sandbox_id,
        ],
    )?;
    Ok(())
}

pub fn mark_start_succeeded(
    conn: &Connection,
    id: &str,
    cleanup_id: Option<SandboxCleanupEpisodeId>,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'started',
             failure_reason = NULL,
             current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL,
             current_local_artifact_origin = NULL,
             cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             state_changed_at = CASE WHEN state != 'started' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2
           AND state = 'starting'
           AND (?3 IS NULL OR EXISTS (
               SELECT 1 FROM sandbox_cleanup_episodes episode
                WHERE episode.sandbox_id = sandboxes.id
                  AND episode.cleanup_id = ?3
                  AND episode.phase = 'armed'
           ))",
        params![now()?, id, cleanup_id.map(|id| id.to_string())],
    )?;
    if changed == 1
        && let Some(cleanup_id) = cleanup_id
    {
        persist_activation_completed_receipt(&tx, id, cleanup_id, SandboxState::Started)?;
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    if changed == 1 {
        arm_auto_lifecycle_after_activation(&tx, id)?;
    }
    if changed == 1 {
        crate::route_activation_claims::close_claim(&tx, id)?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

/// Charge before connecting or dispatching so crashes cannot reset the budget.
pub(crate) fn charge_prepared_clone_disposal_attempt(
    conn: &Connection,
    id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
) -> Result<bool> {
    Ok(conn.execute(
        "UPDATE sandbox_cleanup_episodes SET attempt_count = attempt_count + 1
         WHERE sandbox_id = ?1 AND cleanup_id = ?2 AND completion = 'delete_pool_clone'
           AND phase = 'requested' AND attempt_count < 8
           AND EXISTS (SELECT 1 FROM sandboxes WHERE id = ?1 AND state IN ('cleanup_required', 'cleanup_failed')
               AND cleanup_completion = 'delete_pool_clone')",
        params![id, cleanup_id.to_string()],
    )? == 1)
}

/// An explicit delete retries a held disposal with its original identity.
/// It never grants ownership of the replacement now occupying the pathname.
pub(crate) fn rearm_prepared_clone_disposal(
    conn: &Connection,
    id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes SET state = 'cleanup_required',
            cleanup_completion = 'delete_pool_clone', cleanup_keep_filesystem = 0,
            state_changed_at = ?1, updated_at = ?1
         WHERE id = ?2 AND state = 'quarantined' AND EXISTS (
            SELECT 1 FROM sandbox_cleanup_episodes WHERE sandbox_id = ?2
             AND cleanup_id = ?3 AND completion = 'delete_pool_clone')",
        params![now()?, id, cleanup_id.to_string()],
    )? == 1;
    if changed {
        tx.execute("UPDATE sandbox_cleanup_episodes SET attempt_count = 0, last_cleanup_error = NULL,
                phase = 'requested', requested_at = ?3, quarantine_reason_code = 'create_interrupted'
             WHERE sandbox_id = ?1 AND cleanup_id = ?2", params![id, cleanup_id.to_string(), now_epoch_i64()?])?;
    }
    tx.commit()?;
    Ok(changed)
}

/// Bind disposal authority before any clone RPC can mutate the prepared tree.
/// A missing binding after a crash permits only absence, never pathname deletion.
pub(crate) fn bind_prepared_clone_root(
    conn: &Connection,
    id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    root: PreparedCloneRoot,
) -> Result<bool> {
    let root = serde_json::to_string(&root)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    Ok(conn.execute(
        "UPDATE sandbox_cleanup_episodes SET prepared_clone_root = ?1
         WHERE sandbox_id = ?2 AND cleanup_id = ?3 AND phase = 'armed'
           AND completion = 'delete_pool_clone' AND prepared_clone_root IS NULL",
        params![root, id, cleanup_id.to_string()],
    )? == 1)
}

/// Publish a disposable clone only while its original cleanup episode is armed.
/// Publication and retiring deletion ownership are one transaction.
pub(crate) fn complete_pool_clone(
    conn: &Connection,
    id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
            SET state = 'suspended',
                state_changed_at = ?1,
                updated_at = ?1,
                cleanup_completion = NULL,
                cleanup_keep_filesystem = NULL,
                failure_reason = NULL
          WHERE id = ?2 AND state = 'creating'
            AND EXISTS (
                SELECT 1 FROM sandbox_cleanup_episodes episode
                 WHERE episode.sandbox_id = sandboxes.id
                   AND episode.cleanup_id = ?3
                   AND episode.phase = 'armed'
                   AND episode.activation_state = 'creating'
                   AND episode.completion = 'delete_pool_clone'
            )",
        params![now()?, id, cleanup_id.to_string()],
    )?;
    if changed == 1 {
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

pub fn complete_create(
    conn: &Connection,
    id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    publish_activation_completion: bool,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
            SET cleanup_completion = NULL,
                cleanup_keep_filesystem = NULL,
                failure_reason = NULL,
                updated_at = ?1
          WHERE id = ?2
            AND state IN ('created', 'suspended')
            AND EXISTS (
                SELECT 1 FROM sandbox_cleanup_episodes episode
                 WHERE episode.sandbox_id = sandboxes.id
                   AND episode.cleanup_id = ?3
                   AND episode.phase = 'armed'
            )",
        params![now()?, id, cleanup_id.to_string()],
    )?;
    if changed == 1 {
        if publish_activation_completion {
            let target_state = get_sandbox(&tx, id)?
                .map(|row| row.state)
                .unwrap_or(SandboxState::Created);
            persist_activation_completed_receipt(&tx, id, cleanup_id, target_state)?;
        }
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

pub fn mark_restore_succeeded(
    conn: &Connection,
    id: &str,
    final_state: SandboxState,
    cleanup_id: Option<SandboxCleanupEpisodeId>,
) -> Result<bool> {
    if !matches!(final_state, SandboxState::Started | SandboxState::Paused) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = NULL,
             cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'restoring'
           AND (?4 IS NULL OR EXISTS (
               SELECT 1 FROM sandbox_cleanup_episodes episode
                WHERE episode.sandbox_id = sandboxes.id
                  AND episode.cleanup_id = ?4
                  AND episode.phase = 'armed'
           ))",
        params![
            final_state.as_str(),
            now()?,
            id,
            cleanup_id.map(|id| id.to_string())
        ],
    )?;
    if changed == 1
        && let Some(cleanup_id) = cleanup_id
    {
        persist_activation_completed_receipt(&tx, id, cleanup_id, final_state)?;
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    if changed == 1 && final_state == SandboxState::Started {
        arm_auto_lifecycle_after_activation(&tx, id)?;
    }
    if changed == 1 {
        crate::route_activation_claims::close_claim(&tx, id)?;
    }
    tx.commit()?;
    if changed == 1 {
        emit_sandbox_state_event(
            id,
            SandboxEventType::RestoreCompleted,
            Some(SandboxState::Restoring.as_str()),
            "restore_succeeded",
            "mark_restore_succeeded",
            final_state,
            None,
        );
    }
    Ok(changed == 1)
}

fn arm_auto_lifecycle_after_activation(conn: &Connection, id: &str) -> Result<()> {
    let sandbox = get_sandbox(conn, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    let mut runtime_state = sandbox.auto_lifecycle_state;
    crate::auto_lifecycle::arm_inactivity_conditions(
        &mut runtime_state,
        &sandbox.auto_lifecycle,
        crate::auto_lifecycle::now_secs(),
    );
    let runtime_state_json = serde_json::to_string(&runtime_state)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "UPDATE sandboxes
         SET auto_lifecycle_state_json = ?1,
             next_auto_event_at = ?2
         WHERE id = ?3",
        params![
            runtime_state_json,
            runtime_state.next_auto_event_at.map(SqliteU64),
            id
        ],
    )?;
    Ok(())
}

pub fn begin_suspend(conn: &Connection, id: &str) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = 'suspending',
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             suspend_predecessor_artifact_id = CASE
                 WHEN current_local_artifact_kind = 'full'
                  AND current_local_artifact_origin = 'suspend'
                 THEN current_local_artifact_id
                 ELSE NULL
             END,
             current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL,
             current_local_artifact_origin = NULL,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state != 'suspending' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2 AND state = 'paused'
           AND cleanup_completion IS NULL AND cleanup_keep_filesystem IS NULL",
        params![now()?, id],
    )?;
    Ok(changed == 1)
}

pub fn begin_teardown_suspend(
    conn: &Connection,
    id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<SupersededTeardownGeneration> {
    let tx = immediate_transaction(conn)?;
    let claim = ensure_teardown_suspend_operation(&tx, id, execution_generation)?;
    mark_teardown_suspend_operation_running(&tx, id, execution_generation)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'suspending',
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             suspend_predecessor_artifact_id = CASE
                 WHEN current_local_artifact_kind = 'full'
                  AND current_local_artifact_origin = 'suspend'
                 THEN current_local_artifact_id
                 ELSE NULL
             END,
             current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL,
             current_local_artifact_origin = NULL,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state != 'suspending' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2
           AND state = 'paused'",
        params![now()?, id],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    tx.commit()?;
    emit_teardown_suspend_lease_claim(id, execution_generation, claim);
    Ok(claim)
}

pub fn bind_suspended_teardown_operation(
    conn: &Connection,
    id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<SupersededTeardownGeneration> {
    let tx = immediate_transaction(conn)?;
    let has_suspended_artifact = tx.query_row(
        "SELECT EXISTS (
             SELECT 1
             FROM sandboxes AS s
             JOIN local_artifacts AS a
               ON a.local_artifact_id = s.current_local_artifact_id
              AND a.sandbox_id = s.id
             WHERE s.id = ?1
               AND s.state = 'suspended'
               AND s.current_local_artifact_kind = 'full'
               AND s.current_local_artifact_origin = 'suspend'
               AND a.artifact_kind = 'full'
               AND a.artifact_origin = 'suspend'
         )",
        params![id],
        |row| row.get::<_, bool>(0),
    )?;
    if !has_suspended_artifact {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    let claim = ensure_teardown_suspend_operation(&tx, id, execution_generation)?;
    mark_teardown_suspend_operation_running(&tx, id, execution_generation)?;
    tx.commit()?;
    emit_teardown_suspend_lease_claim(id, execution_generation, claim);
    Ok(claim)
}

fn ensure_teardown_suspend_operation(
    conn: &Connection,
    id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<SupersededTeardownGeneration> {
    if runtime_execution_generation(conn, id)?.is_some_and(|current| current > execution_generation)
    {
        return Err(rusqlite::Error::InvalidQuery);
    }
    if latest_teardown_suspend_operation(conn, id)?
        .is_some_and(|latest| latest.execution_generation > execution_generation)
    {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let mut superseded_generation = None;
    if let Some(active) = active_teardown_suspend_operation(conn, id)? {
        if active.execution_generation > execution_generation {
            return Err(rusqlite::Error::InvalidQuery);
        }
        if active.execution_generation < execution_generation {
            superseded_generation = Some(active.execution_generation);
            pplx_space_common_jobs::db::fail_job_permanent(
                conn,
                &active.job_id,
                TEARDOWN_SUSPEND_SUPERSEDED_REASON,
                None,
            )?;
        }
    }
    if active_teardown_suspend_operation(conn, id)?.is_none() {
        let job_id = JobId::new();
        let payload = serde_json::to_string(&TeardownSuspendJobPayload {
            execution_generation,
            published_artifact_id: None,
        })
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let dedup_key = format!("{id}:{execution_generation}");
        pplx_space_common_jobs::db::insert_job(
            conn,
            pplx_space_common_jobs::db::InsertJob {
                job_id: &job_id,
                kind: TEARDOWN_SUSPEND_JOB_KIND,
                payload: &payload,
                subject_id: Some(id),
                dedup_key: Some(&dedup_key),
                max_attempts: 1,
                traceparent: None,
                priority: pplx_space_common_jobs::JobPriority::default(),
            },
        )?;
    }
    Ok(superseded_generation)
}

fn emit_teardown_suspend_lease_claim(
    sandbox_id: &str,
    execution_generation: ExecutionGeneration,
    superseded_generation: SupersededTeardownGeneration,
) {
    let Some(previous_generation) = superseded_generation else {
        return;
    };
    emit_sandbox_event(
        sandbox_id,
        SandboxEventType::SuspendSuperseded,
        SandboxEventSource::SbxManager,
        SandboxEventDetails::default()
            .operation(PLANNED_TEARDOWN_SUSPEND_OPERATION)
            .trigger(SandboxEventTrigger::System)
            .result("superseded")
            .execution_generation(previous_generation.to_string())
            .reason(format!(
                "superseded by local execution lease generation {execution_generation}"
            )),
    );
}

fn emit_auto_lifecycle_delete_receipt_superseded(
    sandbox_id: SandboxId,
    superseded_receipt: crate::auto_lifecycle_delete_receipts::db::SupersededPendingReceipt,
    current_generation: ExecutionGeneration,
) {
    emit_sandbox_event(
        &sandbox_id.to_string(),
        SandboxEventType::DeleteSuperseded,
        SandboxEventSource::SbxManager,
        SandboxEventDetails::default()
            .operation("delete")
            .trigger(SandboxEventTrigger::System)
            .result("superseded")
            .receipt_id(superseded_receipt.receipt_id.to_string())
            .execution_generation(superseded_receipt.execution_generation.to_string())
            .reason(format!(
                "pending delete receipt superseded by execution generation {current_generation}"
            )),
    );
}

pub(crate) fn emit_auto_lifecycle_delete_receipt_superseded_if_present(
    sandbox_id: SandboxId,
    supersession: Option<crate::auto_lifecycle_delete_receipts::db::PendingReceiptSupersession>,
) {
    if let Some(supersession) = supersession {
        emit_auto_lifecycle_delete_receipt_superseded(
            sandbox_id,
            supersession.receipt,
            supersession.superseding_generation,
        );
    }
}

pub fn claim_teardown_suspend_operation(
    conn: &Connection,
    id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<SupersededTeardownGeneration> {
    let tx = immediate_transaction(conn)?;
    let claim = ensure_teardown_suspend_operation(&tx, id, execution_generation)?;
    tx.commit()?;
    emit_teardown_suspend_lease_claim(id, execution_generation, claim);
    Ok(claim)
}

pub fn begin_teardown_suspend_operation(
    conn: &Connection,
    id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<SupersededTeardownGeneration> {
    let tx = immediate_transaction(conn)?;
    let claim = ensure_teardown_suspend_operation(&tx, id, execution_generation)?;
    mark_teardown_suspend_operation_running(&tx, id, execution_generation)?;
    tx.commit()?;
    emit_teardown_suspend_lease_claim(id, execution_generation, claim);
    Ok(claim)
}

fn mark_teardown_suspend_operation_running(
    conn: &Connection,
    id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<()> {
    let operation =
        active_teardown_suspend_operation(conn, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    if operation.execution_generation != execution_generation {
        return Err(rusqlite::Error::InvalidQuery);
    }
    if operation.status == TeardownSuspendOperationStatus::Pending {
        pplx_space_common_jobs::db::mark_job_running(conn, &operation.job_id)?;
    }
    Ok(())
}

pub fn rollback_suspending_to_paused(conn: &Connection, id: &str) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = 'paused',
             current_local_artifact_id = suspend_predecessor_artifact_id,
             current_local_artifact_kind = CASE
                 WHEN suspend_predecessor_artifact_id IS NULL THEN NULL ELSE 'full'
             END,
             current_local_artifact_origin = CASE
                 WHEN suspend_predecessor_artifact_id IS NULL THEN NULL ELSE 'suspend'
             END,
             suspend_predecessor_artifact_id = NULL,
             cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state != 'paused' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2
           AND state = 'suspending'",
        params![now()?, id],
    )?;
    Ok(changed == 1)
}

pub fn mark_suspending_quarantined(
    conn: &Connection,
    id: &str,
    reason: &str,
) -> std::result::Result<bool, MarkQuarantinedError> {
    mark_quarantined_from_state(
        conn,
        id,
        SandboxState::Suspending,
        SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed,
        reason,
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
}

pub fn mark_starting_quarantined(
    conn: &Connection,
    id: &str,
    reason_code: SandboxQuarantineReasonCode,
    reason: &str,
) -> std::result::Result<bool, MarkQuarantinedError> {
    mark_quarantined_from_state(
        conn,
        id,
        SandboxState::Starting,
        SandboxQuarantineSource::SbxManager,
        reason_code,
        reason,
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
}

pub fn mark_restoring_quarantined(
    conn: &Connection,
    id: &str,
    reason_code: SandboxQuarantineReasonCode,
    reason: &str,
) -> std::result::Result<bool, MarkQuarantinedError> {
    mark_quarantined_from_state(
        conn,
        id,
        SandboxState::Restoring,
        SandboxQuarantineSource::SbxManager,
        reason_code,
        reason,
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
}

pub fn rollback_starting(conn: &Connection, id: &str, state: SandboxState) -> Result<bool> {
    rollback_activation_state(conn, id, SandboxState::Starting, state)
}

pub fn rollback_restoring(conn: &Connection, id: &str, state: SandboxState) -> Result<bool> {
    rollback_activation_state(conn, id, SandboxState::Restoring, state)
}

pub fn rollback_restoring_episode(
    conn: &Connection,
    id: &str,
    state: SandboxState,
    cleanup_id: SandboxCleanupEpisodeId,
) -> std::result::Result<bool, RollbackActivationEpisodeError> {
    rollback_activation_episode(conn, id, SandboxState::Restoring, state, cleanup_id)
}

pub fn rollback_starting_episode(
    conn: &Connection,
    id: &str,
    state: SandboxState,
    cleanup_id: SandboxCleanupEpisodeId,
) -> std::result::Result<bool, RollbackActivationEpisodeError> {
    rollback_activation_episode(conn, id, SandboxState::Starting, state, cleanup_id)
}

fn rollback_activation_episode(
    conn: &Connection,
    id: &str,
    activation_state: SandboxState,
    restored_state: SandboxState,
    cleanup_id: SandboxCleanupEpisodeId,
) -> std::result::Result<bool, RollbackActivationEpisodeError> {
    if !matches!(
        activation_state,
        SandboxState::Starting | SandboxState::Restoring
    ) {
        return Err(RollbackActivationEpisodeError::UnsupportedActivationState {
            state: activation_state,
        });
    }
    if restored_state == SandboxState::Failed {
        return Err(rusqlite::Error::InvalidQuery.into());
    }
    if restored_state == SandboxState::Quarantined {
        return Err(RollbackActivationEpisodeError::QuarantinedTarget);
    }
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
            SET state = ?1,
                failure_reason = NULL,
                cleanup_completion = NULL,
                cleanup_keep_filesystem = NULL,
                restore_origin = NULL,
                restore_phase = NULL,
                restore_storage_uri = NULL,
                state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
                updated_at = ?2
          WHERE id = ?3
            AND state = ?4
            AND EXISTS (
                SELECT 1 FROM sandbox_cleanup_episodes episode
                 WHERE episode.sandbox_id = sandboxes.id
                   AND episode.cleanup_id = ?5
                   AND episode.phase = 'armed'
            )",
        params![
            restored_state.as_str(),
            now()?,
            id,
            activation_state.as_str(),
            cleanup_id.to_string()
        ],
    )?;
    if changed == 1 {
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

fn rollback_activation_state(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
    restored_state: SandboxState,
) -> Result<bool> {
    if matches!(
        restored_state,
        SandboxState::Quarantined | SandboxState::Failed
    ) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = NULL,
             restore_origin = CASE WHEN ?4 = 'restoring' THEN NULL ELSE restore_origin END,
             restore_phase = CASE WHEN ?4 = 'restoring' THEN NULL ELSE restore_phase END,
             restore_storage_uri = CASE WHEN ?4 = 'restoring' THEN NULL ELSE restore_storage_uri END,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             updated_at = ?2
         WHERE id = ?3
           AND state = ?4",
        params![restored_state.as_str(), now()?, id, expected_state.as_str()],
    )?;
    Ok(changed == 1)
}

/// Make `local_artifact_id` the sole suspend-origin artifact for a sandbox.
/// Reusing the same immutable identity updates its path and touch time without
/// discarding its backup-completion marker. A different identity replaces the
/// stale row because the per-sandbox snapshot path has only one byte owner.
fn upsert_suspend_local_artifact(
    conn: &Connection,
    sandbox_id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<()> {
    conn.execute(
        "DELETE FROM local_artifacts
         WHERE sandbox_id = ?1
           AND artifact_origin = 'suspend'
           AND local_artifact_id != ?2",
        params![sandbox_id, local_artifact_id.to_string()],
    )?;
    let staged_artifact_exists = conn.query_row(
        "SELECT EXISTS (
             SELECT 1 FROM local_artifacts
             WHERE local_artifact_id = ?1
               AND sandbox_id = ?2
               AND artifact_kind = 'full'
               AND artifact_origin = 'suspend'
               AND snapshot_name IS NULL
         )",
        params![local_artifact_id.to_string(), sandbox_id],
        |row| row.get(0),
    )?;
    if staged_artifact_exists {
        conn.execute(
            "UPDATE local_artifacts
             SET local_path = ?1, last_touched_at = ?2
             WHERE local_artifact_id = ?3",
            params![local_path, now()?, local_artifact_id.to_string()],
        )?;
    } else {
        insert_local_artifact(
            conn,
            sandbox_id,
            local_artifact_id,
            ArtifactKind::Full,
            ArtifactOrigin::Suspend,
            None,
            local_path,
        )?;
    }
    Ok(())
}

pub fn stage_teardown_suspend_artifact(
    conn: &Connection,
    id: &str,
    execution_generation: ExecutionGeneration,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    let operation =
        active_teardown_suspend_operation(&tx, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    if operation.execution_generation != execution_generation {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    upsert_suspend_local_artifact(&tx, id, local_artifact_id, local_path)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET current_local_artifact_id = ?1,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend',
             suspend_predecessor_artifact_id = NULL,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'suspending'",
        params![local_artifact_id.to_string(), now()?, id],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    tx.commit()?;
    Ok(())
}

pub fn pin_suspended_artifact_for_cleanup(
    conn: &Connection,
    id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    upsert_suspend_local_artifact(&tx, id, local_artifact_id, local_path)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'cleanup_required',
             current_local_artifact_id = ?1,
             current_local_artifact_kind = ?2,
             current_local_artifact_origin = ?3,
             suspend_predecessor_artifact_id = NULL,
             cleanup_completion = ?4,
             cleanup_keep_filesystem = TRUE,
             failure_reason = NULL,
             state_changed_at = CASE
                 WHEN state != 'cleanup_required' THEN ?5
                 ELSE state_changed_at
             END,
             updated_at = ?5
         WHERE id = ?6
           AND state = 'suspending'",
        params![
            local_artifact_id.to_string(),
            ArtifactKind::Full.as_str(),
            ArtifactOrigin::Suspend.as_str(),
            CleanupCompletion::MarkSuspended.as_str(),
            now()?,
            id
        ],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    tx.commit()?;
    Ok(())
}

/// Delete every `sandbox_id`-scoped child row of a sandbox inside the
/// caller's open transaction. The `foreign_keys` pragma is disabled, so this
/// manual cascade is the single source of truth for sandbox child tables:
/// add new child tables here, not at the individual delete call sites.
fn delete_sandbox_child_rows(tx: &Connection, id: &str) -> Result<()> {
    crate::route_activation_claims::close_claim(tx, id)?;
    tx.execute(
        "DELETE FROM sandbox_cleanup_episodes WHERE sandbox_id = ?1",
        params![id],
    )?;
    tx.execute(
        "DELETE FROM local_artifacts WHERE sandbox_id = ?1",
        params![id],
    )?;
    tx.execute(
        "DELETE FROM network_policy_reconcile_queue WHERE sandbox_id = ?1",
        params![id],
    )?;
    tx.execute(
        "DELETE FROM sandbox_quarantine_history WHERE sandbox_id = ?1",
        params![id],
    )?;
    tx.execute(
        "DELETE FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
        params![id],
    )?;
    tx.execute(
        "DELETE FROM sandbox_restore_transfer_scopes WHERE sandbox_id = ?1",
        params![id],
    )?;
    Ok(())
}

pub fn adopt_suspended_local_artifact(
    conn: &Connection,
    id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<bool> {
    adopt_suspended_local_artifact_impl(conn, id, local_artifact_id, local_path, true)
}

pub fn adopt_suspended_local_artifact_preserving_updated_at(
    conn: &Connection,
    id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<bool> {
    adopt_suspended_local_artifact_impl(conn, id, local_artifact_id, local_path, false)
}

fn adopt_suspended_local_artifact_impl(
    conn: &Connection,
    id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
    touch_sandbox: bool,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = if touch_sandbox {
        tx.execute(
            "UPDATE sandboxes
             SET current_local_artifact_id = ?1,
                 current_local_artifact_kind = ?2,
                 current_local_artifact_origin = ?3,
                 updated_at = ?4
             WHERE id = ?5
               AND state = 'suspended'
               AND current_local_artifact_id IS NULL",
            params![
                local_artifact_id.to_string(),
                ArtifactKind::Full.as_str(),
                ArtifactOrigin::Suspend.as_str(),
                now()?,
                id
            ],
        )?
    } else {
        tx.execute(
            "UPDATE sandboxes
             SET current_local_artifact_id = ?1,
                 current_local_artifact_kind = ?2,
                 current_local_artifact_origin = ?3
             WHERE id = ?4
               AND state = 'suspended'
               AND current_local_artifact_id IS NULL",
            params![
                local_artifact_id.to_string(),
                ArtifactKind::Full.as_str(),
                ArtifactOrigin::Suspend.as_str(),
                id
            ],
        )?
    };
    if changed == 1 {
        insert_local_artifact(
            &tx,
            id,
            local_artifact_id,
            ArtifactKind::Full,
            ArtifactOrigin::Suspend,
            None,
            local_path,
        )?;
    }
    tx.commit()?;
    Ok(changed != 0)
}

pub fn publish_restored_suspended_artifact(
    conn: &Connection,
    id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    // Same dedupe rationale as suspended cleanup publication: a cross-node
    // restore back to a node that previously suspended this sandbox can land
    // here with a stale `suspend`-origin row already
    // pointing at the per-sandbox `{CACHE_DIR}/{id}/snapshot` path.
    // Without the DELETE the new row and the stale row would share
    // `local_path`, and the reaper's `SharedLocalPath` safety gate
    // would refuse to reap either — turning the reaper permanently
    // inert for this artifact. The on-disk bytes have just been
    // restored to that path, so the stale row no longer owns them;
    // its cluster-side backup row ages out via
    // cluster-controller's stuck_snapshot_reaper.
    tx.execute(
        "DELETE FROM local_artifacts
         WHERE sandbox_id = ?1 AND artifact_origin = 'suspend'",
        params![id],
    )?;
    insert_local_artifact(
        &tx,
        id,
        local_artifact_id,
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
        local_path,
    )?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'suspended',
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             current_local_artifact_id = ?1,
             current_local_artifact_kind = ?2,
             current_local_artifact_origin = ?3,
             state_changed_at = CASE WHEN state != 'suspended' THEN ?4 ELSE state_changed_at END,
             updated_at = ?4
         WHERE id = ?5
           AND state = 'restoring'
           AND current_local_artifact_id IS NULL",
        params![
            local_artifact_id.to_string(),
            ArtifactKind::Full.as_str(),
            ArtifactOrigin::Suspend.as_str(),
            now()?,
            id
        ],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    retire_restore_transfer_scope(&tx, id)?;
    tx.commit()?;
    Ok(())
}

pub fn publish_restored_suspended_without_artifact(conn: &Connection, id: &str) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'suspended',
             failure_reason = NULL,
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL,
             current_local_artifact_origin = NULL,
             state_changed_at = CASE WHEN state != 'suspended' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2
           AND state = 'restoring'
           AND current_local_artifact_id IS NULL",
        params![now()?, id],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    retire_restore_transfer_scope(&tx, id)?;
    tx.commit()?;
    Ok(())
}

pub fn publish_restored_disk_as_new(conn: &Connection, id: &str) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'stopped',
             failure_reason = NULL,
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL,
             current_local_artifact_origin = NULL,
             state_changed_at = CASE WHEN state != 'stopped' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2 AND state = 'restoring'",
        params![now()?, id],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    clear_runtime_fields(&tx, id)?;
    retire_restore_transfer_scope(&tx, id)?;
    tx.commit()?;
    Ok(())
}

fn insert_local_artifact(
    conn: &Connection,
    sandbox_id: &str,
    local_artifact_id: LocalArtifactId,
    artifact_kind: ArtifactKind,
    artifact_origin: ArtifactOrigin,
    snapshot_name: Option<&SnapshotName>,
    local_path: &str,
) -> Result<()> {
    let ts = now()?;
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at, last_touched_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
        params![
            local_artifact_id.to_string(),
            sandbox_id,
            artifact_kind.as_str(),
            artifact_origin.as_str(),
            snapshot_name.map(SnapshotName::as_str),
            local_path,
            ts,
        ],
    )?;
    Ok(())
}

/// Mark the artifact's remote backup as durably completed. Written by
/// the snapshot-upload worker right after `finalize_artifact_backup`
/// returns `ready` from the cluster API. The reaper gates eviction
/// on `backup_completed_at IS NOT NULL`, which is the per-artifact
/// equivalent of the cluster `snapshots.state='ready'` row — without
/// adding a network dependency for GC.
pub fn mark_local_artifact_backup_completed(
    conn: &Connection,
    local_artifact_id: &str,
) -> Result<()> {
    conn.execute(
        "UPDATE local_artifacts SET backup_completed_at = ?1 WHERE local_artifact_id = ?2",
        params![now()?, local_artifact_id],
    )?;
    Ok(())
}

/// True when the sandbox row is parked `suspended` with
/// `current_local_artifact_id` equal to `local_artifact_id`.
///
/// This is the safety gate for reclaiming per-artifact files under the
/// published snapshot directory after a backup finalizes ready. Every
/// suspend stages into a sibling directory and flips the row to
/// `suspended` with the freshly minted artifact pinned in one
/// transaction, so while this predicate holds — checked under the same
/// connection mutex every state transition serializes on — the files at
/// the published snapshot path are exactly the pinned artifact's bytes,
/// never a newer suspend's output mid-encode or mid-publish.
pub fn sandbox_suspended_with_pinned_artifact(
    conn: &Connection,
    sandbox_id: &str,
    local_artifact_id: &str,
) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM sandboxes
             WHERE id = ?1
               AND state = 'suspended'
               AND current_local_artifact_id = ?2
         )",
        params![sandbox_id, local_artifact_id],
        |r| r.get(0),
    )
}

pub fn publish_named_local_artifact(
    conn: &Connection,
    sandbox_id: &str,
    local_artifact_id: LocalArtifactId,
    artifact_kind: ArtifactKind,
    snapshot_name: &SnapshotName,
    local_path: &str,
) -> Result<()> {
    insert_local_artifact(
        conn,
        sandbox_id,
        local_artifact_id,
        artifact_kind,
        ArtifactOrigin::Snapshot,
        Some(snapshot_name),
        local_path,
    )
}

pub fn publish_stopped_artifact(
    conn: &Connection,
    sandbox_id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    insert_local_artifact(
        &tx,
        sandbox_id,
        local_artifact_id,
        ArtifactKind::Disk,
        ArtifactOrigin::Stop,
        None,
        local_path,
    )?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'stopped',
             current_local_artifact_id = ?1,
             current_local_artifact_kind = ?2,
             current_local_artifact_origin = ?3,
             uid = NULL,
             slot = NULL,
             guest_cid = NULL,
             pid = NULL,
             spaced_host_pid = NULL,
             netgtw_pid = NULL,
             vsock_uds_path = NULL,
             api_socket_path = NULL,
             space_api_route_generation = NULL,
             space_api_route_node_id = NULL,
             space_api_route_execution_generation = NULL,
             space_api_route_pod_uid = NULL,
             space_api_route_substrate_kind = NULL,
             space_api_route_activation_attempt_id = NULL,
             space_api_route_identity_provenance = NULL,
             spaced_endpoint_proof_json = NULL,
             guest_uptime_secs = NULL,
             cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state != 'stopped' THEN ?4 ELSE state_changed_at END,
             updated_at = ?4
         WHERE id = ?5
           AND (
               state = 'stopping'
               OR (
                   state IN ('cleanup_required', 'cleanup_failed')
                   AND cleanup_completion = 'mark_stopped'
               )
               OR (
                   state = 'stopped'
                   AND current_local_artifact_id IS NULL
               )
           )",
        params![
            local_artifact_id.to_string(),
            ArtifactKind::Disk.as_str(),
            ArtifactOrigin::Stop.as_str(),
            now()?,
            sandbox_id,
        ],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    // Clear the runtime first so the retention trigger observes its complete
    // identity, then close the exact attempt in the same transaction.
    crate::route_activation_claims::close_claim(&tx, sandbox_id)?;
    tx.commit()?;
    Ok(())
}

pub fn get_local_artifact(
    conn: &Connection,
    local_artifact_id: LocalArtifactId,
) -> Result<Option<LocalArtifactRow>> {
    conn.query_row(
        "SELECT local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                snapshot_name, local_path
         FROM local_artifacts WHERE local_artifact_id = ?1",
        params![local_artifact_id.to_string()],
        row_to_local_artifact,
    )
    .optional()
}

pub fn sandbox_state_changed_at(
    conn: &Connection,
    sandbox_ids: &[String],
) -> Result<HashMap<String, String>> {
    if sandbox_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = std::iter::repeat_n("?", sandbox_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT id, state_changed_at
         FROM sandboxes
         WHERE id IN ({placeholders})"
    );
    let mut statement = conn.prepare(&query)?;
    let rows = statement.query_map(rusqlite::params_from_iter(sandbox_ids), |row| {
        Ok((row.get(0)?, timestamps::diagnostic_column(row, 1)?))
    })?;
    rows.collect()
}

/// Read one row's `state_changed_at` exactly as stored.
///
/// Raw, unlike [`sandbox_state_changed_at`]'s diagnostic rendering, and for
/// the same reason [`SandboxRow::updated_at`] is raw: the value doubles as a
/// CAS witness, so it has to compare byte-for-byte against the column it came
/// from. Time-based decisions decode it strictly through
/// [`timestamps::EpochSeconds::parse`] instead of trusting the text.
///
/// `None` means no such row. A non-text value is returned rendered rather
/// than rejected, so a corrupt timestamp reaches the caller's explicit
/// refusal path instead of looking like a missing sandbox.
pub fn sandbox_state_changed_at_witness(conn: &Connection, id: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT state_changed_at FROM sandboxes WHERE id = ?1",
        params![id],
        |row| {
            Ok(match row.get::<_, rusqlite::types::Value>(0)? {
                rusqlite::types::Value::Text(raw) => raw,
                rusqlite::types::Value::Integer(raw) => raw.to_string(),
                rusqlite::types::Value::Real(raw) => raw.to_string(),
                rusqlite::types::Value::Null | rusqlite::types::Value::Blob(_) => String::new(),
            })
        },
    )
    .optional()
}

pub fn get_named_local_artifact(
    conn: &Connection,
    sandbox_id: &str,
    snapshot_name: &SnapshotName,
) -> Result<Option<LocalArtifactRow>> {
    conn.query_row(
        "SELECT local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                snapshot_name, local_path
         FROM local_artifacts
         WHERE sandbox_id = ?1 AND artifact_origin = 'snapshot' AND snapshot_name = ?2",
        params![sandbox_id, snapshot_name.as_str()],
        row_to_local_artifact,
    )
    .optional()
}

pub fn delete_named_local_artifact(
    conn: &Connection,
    sandbox_id: &str,
    snapshot_name: &str,
) -> Result<()> {
    conn.execute(
        "DELETE FROM local_artifacts
         WHERE sandbox_id = ?1 AND artifact_origin = 'snapshot' AND snapshot_name = ?2",
        params![sandbox_id, snapshot_name],
    )?;
    Ok(())
}

fn row_to_local_artifact(row: &rusqlite::Row<'_>) -> Result<LocalArtifactRow> {
    Ok(LocalArtifactRow {
        local_artifact_id: row.get::<_, String>(0)?.parse().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
        })?,
        sandbox_id: row.get(1)?,
        artifact_kind: row.get::<_, String>(2)?.parse().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(2, Type::Text, Box::new(error))
        })?,
        artifact_origin: row.get::<_, String>(3)?.parse().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(3, Type::Text, Box::new(error))
        })?,
        snapshot_name: row
            .get::<_, Option<String>>(4)?
            .map(SnapshotName::try_from)
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(4, Type::Text, Box::new(error))
            })?,
        local_path: row.get(5)?,
    })
}

fn generic_writer_transition_blocked(
    previous_state: Option<SandboxState>,
    target_state: SandboxState,
) -> bool {
    previous_state
        .filter(|state| *state == SandboxState::Quarantined)
        .is_some_and(|state| state.transition(target_state).is_err())
}

struct QuarantineEpisode<'a> {
    sandbox_id: &'a str,
    entered_from_state: Option<&'a str>,
    source: SandboxQuarantineSource,
    reason_code: SandboxQuarantineReasonCode,
    reason_message: Option<&'a str>,
    next_action: SandboxQuarantineNextAction,
    timestamp: &'a str,
}

fn insert_quarantine_history(
    conn: &Connection,
    episode: &QuarantineEpisode<'_>,
    quarantine_id: &str,
    action: SandboxQuarantineHistoryAction,
) -> Result<()> {
    conn.execute(
        "INSERT INTO sandbox_quarantine_history (
             id, quarantine_id, sandbox_id, occurred_at, source, action,
             reason_code, message, next_action, evidence_json
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, '{}')",
        params![
            uuid::Uuid::now_v7().to_string(),
            quarantine_id,
            episode.sandbox_id,
            episode.timestamp,
            episode.source.as_str(),
            action.as_str(),
            episode.reason_code.as_str(),
            episode.reason_message,
            episode.next_action.as_str()
        ],
    )?;
    Ok(())
}

/// Cleanup can discard resident memory while retaining newer filesystem data.
/// The orphan reason alone allows recovery from older snapshots, so these
/// episodes need an explicit owner outside automatic legacy recovery. Older
/// binaries already exclude unknown owners from their recovery scans.
fn insert_operator_quarantine_episode(
    conn: &rusqlite::Transaction<'_>,
    episode: &QuarantineEpisode<'_>,
) -> Result<()> {
    insert_new_quarantine_episode(conn, episode)?;
    conn.execute(
        "UPDATE sandbox_quarantine_state SET next_action_owner = ?1
         WHERE sandbox_id = ?2
           AND (next_action_owner IS NULL OR next_action_owner = ?3)",
        params![
            QuarantineRecoveryOwner::OperatorAttention.as_str(),
            episode.sandbox_id,
            QuarantineRecoveryOwner::LegacyRecovery.as_str()
        ],
    )?;
    Ok(())
}

fn insert_new_quarantine_episode(conn: &Connection, episode: &QuarantineEpisode<'_>) -> Result<()> {
    let quarantine_id = uuid::Uuid::now_v7().to_string();
    // A sandbox re-entering quarantine from a non-quarantined state replaces its
    // episode. Two things must survive that: a lease someone is still acting
    // under, and anything owned by a release other than this one.
    //
    // Clearing a live lease would make the episode look due and unclaimed while
    // its operation is still running, so the next tick would start a second one.
    // Clearing a foreign owner's state would reach into a newer release's
    // bookkeeping, which this release cannot reason about — the reconfirmation
    // path refuses that too.
    //
    // Every arm is NULL-guarded on purpose. `owner = ?3` against a NULL owner
    // yields SQL NULL rather than false, which propagates through AND, fails
    // `row.get::<bool>`, and aborts the whole transaction — leaving a sandbox
    // that must be quarantined un-quarantined.
    let current_epoch = now_epoch_i64()?;
    let (claim_dead, legacy_owned): (bool, bool) = conn
        .query_row(
            "SELECT claim_expires_at,
                    (next_action_owner IS NULL OR next_action_owner = ?2)
               FROM sandbox_quarantine_state
              WHERE sandbox_id = ?1",
            params![
                episode.sandbox_id,
                QuarantineRecoveryOwner::LegacyRecovery.as_str(),
            ],
            |row| {
                let expiry: rusqlite::types::Value = row.get(0)?;
                let claim_dead = matches!(expiry, rusqlite::types::Value::Null)
                    || timestamps::EpochSeconds::decode(&expiry)
                        .is_ok_and(|value| value.get() <= current_epoch);
                Ok((claim_dead, row.get(1)?))
            },
        )
        .optional()?
        // With no prior row there is no conflict, so the fragment these flags
        // pick is never executed and the values cannot be observed. This states
        // the intent — nothing to preserve — rather than implying a live branch.
        .unwrap_or((true, true));
    // Three cases, because "preserve" means different things to different owners.
    //
    // A newer release's owner keeps its retry and lease columns: clearing those
    // would reach into a mechanism this release cannot reason about. Its identity
    // columns are a different matter — the `ON CONFLICT` below rewrites everything
    // outside `retry_state_reset` on every branch, foreign-owned included:
    // `quarantine_id`, `source`, `reason_code`, `entered_at`,
    // `entered_from_state`, `reason_message`, `evidence_json`, and `next_action`.
    // A new episode has to be recorded and this row is where it lives, so
    // "never touched" would overstate it.
    //
    // Our own live lease — owner, token, expiry — is kept so a running operation
    // still blocks the episode from being handed to a second one. The token can no
    // longer satisfy any fence, because `ON CONFLICT` rewrites `quarantine_id` and
    // every fence pins it; what it still does is hold `claim_is_live` true until
    // the lease lapses, which is what keeps two operations off one sandbox while
    // the first is mid-transfer.
    //
    // The pacing — attempt count, retry time, last error — is reset whenever the
    // row is ours, because a new `quarantine_id` means the sandbox left quarantine
    // and came back. A genuinely new episode should not wait out the previous
    // episode's backoff or report its error. That differs from a reconfirmation of
    // the same episode, where the pacing is inherited on purpose.
    let retry_state_reset = match (legacy_owned, claim_dead) {
        (false, _) => {
            "next_action_after = next_action_after,
             next_action_owner = next_action_owner,
             attempt_count = attempt_count,
             claim_token = claim_token,
             claim_expires_at = claim_expires_at,
             last_error = last_error"
        }
        (true, false) => {
            "next_action_after = NULL,
             next_action_owner = next_action_owner,
             attempt_count = 0,
             claim_token = claim_token,
             claim_expires_at = claim_expires_at,
             last_error = NULL"
        }
        (true, true) => {
            "next_action_after = NULL,
             next_action_owner = NULL,
             attempt_count = 0,
             claim_token = NULL,
             claim_expires_at = NULL,
             last_error = NULL"
        }
    };
    conn.execute(
        &format!(
            "INSERT INTO sandbox_quarantine_state (
                 sandbox_id, quarantine_id, entered_at, updated_at,
                 entered_from_state, source, reason_code, reason_message,
                 evidence_json, next_action
             )
             VALUES (?1, ?2, ?3, ?3, ?4, ?5, ?6, ?7, '{{}}', ?8)
             ON CONFLICT(sandbox_id) DO UPDATE SET
                 quarantine_id = excluded.quarantine_id,
                 entered_at = excluded.entered_at,
                 updated_at = excluded.updated_at,
                 entered_from_state = excluded.entered_from_state,
                 source = excluded.source,
                 reason_code = excluded.reason_code,
                 reason_message = excluded.reason_message,
                 evidence_json = excluded.evidence_json,
                 next_action = excluded.next_action,
                 {retry_state_reset}"
        ),
        params![
            episode.sandbox_id,
            quarantine_id,
            episode.timestamp,
            episode.entered_from_state,
            episode.source.as_str(),
            episode.reason_code.as_str(),
            episode.reason_message,
            episode.next_action.as_str()
        ],
    )?;
    insert_quarantine_history(
        conn,
        episode,
        &quarantine_id,
        SandboxQuarantineHistoryAction::EnteredQuarantine,
    )
}

pub(crate) fn insert_quarantined_sandbox_in_transaction(
    conn: &Connection,
    params: InsertSandboxParams<'_>,
    source: SandboxQuarantineSource,
    reason_code: SandboxQuarantineReasonCode,
    reason_message: &str,
    next_action: SandboxQuarantineNextAction,
) -> std::result::Result<(), MarkQuarantinedError> {
    if params.state != SandboxState::Quarantined {
        return Err(MarkQuarantinedError::NonQuarantinedInsert {
            state: params.state,
        });
    }
    let sandbox_id = params.id.to_string();
    let reason_message = reason_message.trim();
    let reason_message = (!reason_message.is_empty()).then_some(reason_message);
    let timestamp = now()?;
    insert_sandbox(conn, params)?;
    conn.execute(
        "UPDATE sandboxes SET failure_reason = ?1 WHERE id = ?2",
        params![reason_message, sandbox_id],
    )?;
    insert_new_quarantine_episode(
        conn,
        &QuarantineEpisode {
            sandbox_id: &sandbox_id,
            entered_from_state: None,
            source,
            reason_code,
            reason_message,
            next_action,
            timestamp: &timestamp,
        },
    )?;
    Ok(())
}

pub(crate) fn emit_insert_quarantined_sandbox_event(
    sandbox_id: &str,
    reason_code: SandboxQuarantineReasonCode,
) {
    emit_sandbox_state_event(
        sandbox_id,
        SandboxEventType::SandboxQuarantined,
        None,
        reason_code.as_str(),
        "insert_quarantined_sandbox",
        SandboxState::Quarantined,
        None,
    );
}

pub fn mark_quarantined(
    conn: &Connection,
    id: &str,
    source: SandboxQuarantineSource,
    reason_code: SandboxQuarantineReasonCode,
    reason_message: &str,
    next_action: SandboxQuarantineNextAction,
) -> std::result::Result<(), MarkQuarantinedError> {
    mark_quarantined_inner(
        conn,
        id,
        None,
        source,
        reason_code,
        reason_message,
        next_action,
    )?;
    Ok(())
}

pub fn current_quarantine_reason_wire_value(conn: &Connection, id: &str) -> Result<Option<String>> {
    let raw = conn
        .query_row(
            "SELECT reason_code FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
            params![id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    raw.map(|raw| {
        let value = raw.trim();
        if value.is_empty() {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "quarantine reason code is blank",
                )),
            ));
        }
        Ok(value.to_string())
    })
    .transpose()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PauseFailedRecoveryTarget {
    Started,
    Paused,
}

impl PauseFailedRecoveryTarget {
    pub(crate) const fn sandbox_state(self) -> SandboxState {
        match self {
            Self::Started => SandboxState::Started,
            Self::Paused => SandboxState::Paused,
        }
    }

    const fn history_action(self) -> SandboxQuarantineHistoryAction {
        match self {
            Self::Started => SandboxQuarantineHistoryAction::RecoveredToStarted,
            Self::Paused => SandboxQuarantineHistoryAction::RecoveredToPaused,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PauseFailedRecoveryError {
    #[error("pause-failed recovery database operation failed: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("pause-failed sandbox state CAS affected {changed} rows for {sandbox_id}")]
    SandboxStateInvariant {
        sandbox_id: SandboxId,
        changed: usize,
    },
    #[error("pause-failed recovery history CAS affected {changed} rows for {sandbox_id}")]
    HistoryInvariant {
        sandbox_id: SandboxId,
        changed: usize,
    },
    #[error("pause-failed quarantine retirement CAS affected {changed} rows for {sandbox_id}")]
    RetirementInvariant {
        sandbox_id: SandboxId,
        changed: usize,
    },
}

/// Recover an sbx-manager pause failure after the caller establishes
/// definitive runtime state.
pub(crate) fn recover_pause_failed_quarantine(
    conn: &Connection,
    id: &SandboxId,
    target: PauseFailedRecoveryTarget,
) -> std::result::Result<bool, PauseFailedRecoveryError> {
    let sandbox_id = id.to_string();
    let source = SandboxQuarantineSource::SbxManager.as_str();
    let reason = SandboxQuarantineReasonCode::PauseFailed.as_str();
    let tx = immediate_transaction(conn)?;
    let Some(quarantine_id): Option<String> = tx
        .query_row(
            "SELECT qs.quarantine_id
               FROM sandboxes s
               JOIN sandbox_quarantine_state qs ON qs.sandbox_id = s.id
              WHERE s.id = ?1
                AND s.state = 'quarantined'
                AND qs.source = ?2
                AND qs.reason_code = ?3",
            params![&sandbox_id, source, reason],
            |row| row.get(0),
        )
        .optional()?
    else {
        tx.commit()?;
        return Ok(false);
    };
    let target_state = target.sandbox_state();
    let changed = tx.execute(
        "UPDATE sandboxes
            SET state = ?1,
                failure_reason = NULL,
                state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
                updated_at = ?2
          WHERE id = ?3
            AND state = 'quarantined'
            AND EXISTS (
                SELECT 1
                  FROM sandbox_quarantine_state
                 WHERE sandbox_id = ?3
                   AND quarantine_id = ?4
                   AND source = ?5
                   AND reason_code = ?6
            )",
        params![
            target_state.as_str(),
            now()?,
            &sandbox_id,
            &quarantine_id,
            source,
            reason
        ],
    )?;
    if changed != 1 {
        return Err(PauseFailedRecoveryError::SandboxStateInvariant {
            sandbox_id: *id,
            changed,
        });
    }
    let recorded = tx.execute(
        "INSERT INTO sandbox_quarantine_history (
             id, quarantine_id, sandbox_id, occurred_at, source, action,
             reason_code, message, next_action, evidence_json
         )
         SELECT ?1, quarantine_id, sandbox_id, ?2, source, ?3,
                reason_code, 'runtime state became definitive after pause failure',
                NULL, evidence_json
           FROM sandbox_quarantine_state
          WHERE sandbox_id = ?4
            AND quarantine_id = ?5
            AND source = ?6
            AND reason_code = ?7",
        params![
            uuid::Uuid::now_v7().to_string(),
            now()?,
            target.history_action().as_str(),
            &sandbox_id,
            &quarantine_id,
            source,
            reason
        ],
    )?;
    if recorded != 1 {
        return Err(PauseFailedRecoveryError::HistoryInvariant {
            sandbox_id: *id,
            changed: recorded,
        });
    }
    let cleared = tx.execute(
        "DELETE FROM sandbox_quarantine_state
          WHERE sandbox_id = ?1
            AND quarantine_id = ?2
            AND source = ?3
            AND reason_code = ?4",
        params![&sandbox_id, &quarantine_id, source, reason],
    )?;
    if cleared != 1 {
        return Err(PauseFailedRecoveryError::RetirementInvariant {
            sandbox_id: *id,
            changed: cleared,
        });
    }
    tx.commit()?;
    emit_sandbox_state_event(
        &sandbox_id,
        SandboxEventType::QuarantineRecovered,
        Some(SandboxState::Quarantined.as_str()),
        reason,
        "recover_pause_failed_quarantine",
        target_state,
        None,
    );
    Ok(true)
}

/// Load optional quarantine or transition-receipt metadata for inventory
/// publication. Wire degradation for malformed rows happens in the State RPC,
/// so this storage helper exposes the row vocabulary without deciding whether
/// another sandbox may still be published.
pub fn sandbox_state_publication_metadata(
    conn: &Connection,
    sandbox_ids: &[String],
) -> Result<BTreeMap<String, RawSandboxStatePublicationMetadataRead>> {
    let mut statement = conn.prepare_cached(
        "SELECT s.id, s.state, qs.reason_code,
                s.transition_receipt_id,
                s.transition_receipt_kind,
                s.transition_receipt_target_state,
                s.transition_receipt_execution_generation,
                s.transition_receipt_reason_code,
                s.transition_receipt_activation_attempt_id
           FROM sandboxes s
           LEFT JOIN sandbox_quarantine_state qs ON qs.sandbox_id = s.id
          WHERE s.id = ?1
            AND (s.state = 'quarantined'
                 OR s.transition_receipt_id IS NOT NULL)",
    )?;
    let mut publications = BTreeMap::new();
    for sandbox_id in sandbox_ids {
        let publication = statement
            .query_row(params![sandbox_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    RawSandboxStatePublicationMetadata {
                        quarantine_reason_code: row.get(2)?,
                        transition_receipt_id: row.get(3)?,
                        transition_receipt_kind: row.get(4)?,
                        transition_receipt_target_state: row.get(5)?,
                        transition_receipt_execution_generation: row.get(6)?,
                        transition_receipt_reason_code: row.get(7)?,
                        transition_receipt_activation_attempt_id: row.get(8)?,
                    },
                ))
            })
            .optional();
        match publication {
            Ok(Some((id, publication))) => {
                publications.insert(id, Ok(publication));
            }
            Ok(None) => {}
            Err(error) => {
                publications.insert(sandbox_id.clone(), Err(error));
            }
        }
    }
    Ok(publications)
}

/// Load node-local recovery scheduling facts separately from quarantine
/// classification metadata. The observation covers the automatic-recovery
/// reason class, including owners introduced by a newer release; artifact
/// eligibility is published independently and remains owned by
/// `quarantine_recovery_branch`.
pub fn quarantine_recovery_observations(
    conn: &Connection,
    sandbox_ids: &[String],
) -> Result<BTreeMap<String, RawQuarantineRecoveryObservationRead>> {
    let mut statement = conn.prepare_cached(
        "SELECT qs.sandbox_id, qs.quarantine_id, qs.next_action_owner,
                qs.attempt_count, qs.next_action_after, qs.reason_code
           FROM sandbox_quarantine_state qs
           JOIN sandboxes s ON s.id = qs.sandbox_id
           LEFT JOIN sandbox_cleanup_episodes cleanup ON cleanup.sandbox_id = s.id
          WHERE qs.sandbox_id = ?1
            AND qs.source = ?2
            AND s.state IN ('quarantined', 'restoring')
            AND cleanup.sandbox_id IS NULL",
    )?;
    let mut observations = BTreeMap::new();
    for sandbox_id in sandbox_ids {
        let observation = statement
            .query_row(
                params![sandbox_id, SandboxQuarantineSource::SbxManager.as_str()],
                |row| {
                    let reason_raw = row.get::<_, String>(5)?;
                    let reason =
                        reason_raw
                            .parse::<SandboxQuarantineReasonCode>()
                            .map_err(|error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    5,
                                    Type::Text,
                                    Box::new(error),
                                )
                            })?;
                    let observation = if reason.automatic_recovery_policy().is_some() {
                        Some(RawQuarantineRecoveryObservation {
                            local_quarantine_id: row.get(1)?,
                            owner: row.get(2)?,
                            attempt_count: row.get(3)?,
                            next_attempt_at: {
                                let value = row.get::<_, rusqlite::types::Value>(4)?;
                                let stored_type = value.data_type();
                                nullable_stored_timestamp_text(value).ok_or_else(|| {
                                    rusqlite::Error::InvalidColumnType(
                                        4,
                                        "next_action_after".into(),
                                        stored_type,
                                    )
                                })?
                            },
                        })
                    } else {
                        None
                    };
                    Ok((row.get::<_, String>(0)?, observation))
                },
            )
            .optional();
        match observation {
            Ok(Some((id, Some(observation)))) => {
                observations.insert(id, Ok(observation));
            }
            Ok(Some((_, None)) | None) => {}
            Err(error) => {
                observations.insert(sandbox_id.clone(), Err(error));
            }
        }
    }
    Ok(observations)
}

/// One scanned row of the legacy-recovery quarantine cohort.
///
/// `Malformed` and `Ineligible` are both skipped and are deliberately different
/// nouns: a malformed row is stored data an operator has to look at, an
/// ineligible one is the ordinary case for most of the cohort.
enum QuarantineRecoveryRow {
    Malformed,
    Ineligible,
    Eligible {
        candidate: QuarantineRecoveryCandidate,
        /// The provisional retry time has arrived and no unexpired claim is held,
        /// so this pass may attempt the claim.
        due: bool,
    },
}

/// A stored column that must hold text, or the row is malformed.
///
/// The recovery tables are not `STRICT`. Converting inside `row.get` makes a
/// mistyped value a query error, and one query error retires every candidate in
/// the pass rather than the single bad row, so the wrong storage class has to be
/// classified exactly like an unparseable value.
fn required_stored_text(value: rusqlite::types::Value) -> Option<String> {
    match value {
        rusqlite::types::Value::Text(text) => Some(text),
        _ => None,
    }
}

/// A stored column that may hold text or SQL NULL. `None` means malformed, and
/// `Some(None)` means a legitimately absent value.
fn nullable_stored_text(value: rusqlite::types::Value) -> Option<Option<String>> {
    match value {
        rusqlite::types::Value::Text(text) => Some(Some(text)),
        rusqlite::types::Value::Null => Some(None),
        _ => None,
    }
}

/// Canonicalize valid historical encodings without hiding malformed diagnostic
/// text. Authoritative callers still validate the returned epoch seconds.
fn stored_timestamp_text(value: rusqlite::types::Value) -> Option<String> {
    timestamps::diagnostic_text(value)
}

fn nullable_stored_timestamp_text(value: rusqlite::types::Value) -> Option<Option<String>> {
    match value {
        rusqlite::types::Value::Null => Some(None),
        value => stored_timestamp_text(value).map(Some),
    }
}

/// A stored column that must hold an integer, or the row is malformed.
fn required_stored_integer(value: rusqlite::types::Value) -> Option<i64> {
    match value {
        rusqlite::types::Value::Integer(integer) => Some(integer),
        _ => None,
    }
}

/// Scan the node-local quarantine cohort that legacy automatic recovery owns.
///
/// Returns the due candidates together with the backlog and malformed counts
/// derived from the same rows. Rows whose columns fail validation are counted
/// and skipped; a statement-level SQLite failure aborts the scan so a caller
/// can never mistake a partial read for an empty backlog.
#[cfg(test)]
pub fn scan_quarantine_recovery_candidates(conn: &Connection) -> Result<QuarantineRecoveryScan> {
    // Tests that need a whole cohort still exercise production eligibility,
    // paging and decoding. Stop at exactly one finite sweep.
    let mut cursor = PeriodicQuarantineRecoveryScan::default();
    let mut candidates = Vec::new();
    loop {
        let batch = cursor.next_batch(conn)?;
        candidates.extend(batch.candidates);
        if let Some(mut summary) = batch.completed_sweep {
            summary.candidates = candidates;
            return Ok(summary);
        }
    }
}

fn decode_quarantine_recovery_row(
    row: &rusqlite::Row<'_>,
    current_epoch: i64,
) -> Result<QuarantineRecoveryRow> {
    // Read the stored values without converting. These tables are not
    // `STRICT`, so any column can hold any storage class, and asking
    // rusqlite to convert would turn one mistyped value into a query
    // error that retires every candidate in the pass. `?` here fires only
    // on a genuine SQLite failure, which must still abort.
    // Owned and consumed, not borrowed and cloned. Reading each column
    // into a `Value` and then cloning the `String` out of it allocated
    // twice per text column per row; moving it out allocates once.
    let stored: [rusqlite::types::Value; 12] = [
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
        row.get(11)?,
    ];
    let [
        stored_id,
        stored_quarantine_id,
        stored_entered_at,
        stored_state,
        stored_reason,
        stored_artifact_id,
        stored_artifact_kind,
        stored_artifact_origin,
        stored_attempts,
        stored_next_action,
        stored_claim_token,
        stored_claim_expiry,
    ] = stored;
    let (
        Some(sandbox_id_raw),
        Some(local_quarantine_id_raw),
        Some(entered_at_raw),
        Some(state_raw),
        Some(reason_code_raw),
        Some(local_artifact_id_raw),
        Some(artifact_kind_raw),
        Some(artifact_origin_raw),
        Some(attempt_count_raw),
        Some(next_action_after_raw),
        Some(claim_token_raw),
        Some(claim_expires_at_raw),
    ) = (
        required_stored_text(stored_id),
        required_stored_text(stored_quarantine_id),
        stored_timestamp_text(stored_entered_at),
        required_stored_text(stored_state),
        required_stored_text(stored_reason),
        nullable_stored_text(stored_artifact_id),
        nullable_stored_text(stored_artifact_kind),
        nullable_stored_text(stored_artifact_origin),
        required_stored_integer(stored_attempts),
        nullable_stored_timestamp_text(stored_next_action),
        nullable_stored_text(stored_claim_token),
        nullable_stored_timestamp_text(stored_claim_expiry),
    )
    else {
        tracing::warn!(
            "skipping quarantine recovery row whose stored columns have \
             unexpected storage classes"
        );
        return Ok(QuarantineRecoveryRow::Malformed);
    };
    // Everything above is a read of the stored column and propagates a
    // genuine rusqlite failure. Everything below is validation of what
    // was read, and an unparseable value yields `None` so the row is
    // counted as malformed rather than aborting the pass.
    let (
        Ok(sandbox_id),
        Ok(local_quarantine_id),
        Ok(entered_at),
        Ok(state),
        Ok(reason_code),
        Ok(local_artifact_id),
        Ok(artifact_kind),
        Ok(artifact_origin),
        Ok(attempt_count),
        Ok(next_action_after),
        Ok(claim_expires_at),
    ) = (
        sandbox_id_raw.parse::<SandboxId>(),
        local_quarantine_id_raw.parse::<uuid::Uuid>(),
        timestamps::EpochSeconds::parse(&entered_at_raw).map(timestamps::EpochSeconds::unsigned),
        state_raw.parse::<SandboxState>(),
        reason_code_raw.parse::<SandboxQuarantineReasonCode>(),
        local_artifact_id_raw
            .as_deref()
            .map(LocalArtifactId::from_str)
            .transpose(),
        artifact_kind_raw
            .as_deref()
            .map(ArtifactKind::from_str)
            .transpose(),
        artifact_origin_raw
            .as_deref()
            .map(ArtifactOrigin::from_str)
            .transpose(),
        u32::try_from(attempt_count_raw),
        next_action_after_raw
            .as_deref()
            .map(|value| timestamps::EpochSeconds::parse(value).map(timestamps::EpochSeconds::get))
            .transpose(),
        claim_expires_at_raw
            .as_deref()
            .map(|value| timestamps::EpochSeconds::parse(value).map(timestamps::EpochSeconds::get))
            .transpose(),
    )
    else {
        tracing::warn!(
            sandbox_id = %sandbox_id_raw,
            quarantine_id = %local_quarantine_id_raw,
            "skipping malformed quarantine recovery row"
        );
        return Ok(QuarantineRecoveryRow::Malformed);
    };
    let Some(recovery) = quarantine_recovery_branch(
        state,
        reason_code,
        local_artifact_id,
        artifact_kind,
        artifact_origin,
    ) else {
        return Ok(QuarantineRecoveryRow::Ineligible);
    };
    // A detached orphan restore runs outside the serial inventory pass, so
    // this lease is also the single-dispatch guard on the next one. Its
    // heartbeat extends the guard; a lost lease admits a new claimant, and
    // the superseded task's exact-token fence is what stops it mutating.
    let claim_is_live = claim_token_raw.is_some()
        && claim_expires_at.is_some_and(|expires_at| expires_at > current_epoch);
    let retry_time_reached =
        next_action_after.is_none_or(|next_attempt_at| next_attempt_at <= current_epoch);
    Ok(QuarantineRecoveryRow::Eligible {
        candidate: QuarantineRecoveryCandidate {
            sandbox_id,
            local_quarantine_id,
            entered_at,
            reason_code,
            recovery,
            attempt_count,
        },
        due: retry_time_reached && !claim_is_live,
    })
}

pub(crate) const QUARANTINE_RECOVERY_CLAIM_LEASE: Duration = Duration::from_secs(15 * 60);
pub(crate) const QUARANTINE_RECOVERY_BACKOFF_BASE: Duration = Duration::from_secs(60);
const QUARANTINE_RECOVERY_BACKOFF_MAX: Duration = Duration::from_secs(15 * 60);

/// Seconds since the epoch, for the SQL boundary where quarantine timestamps
/// are stored as decimal strings.
fn epoch_secs_after(current_epoch: i64, span: Duration) -> i64 {
    current_epoch.saturating_add(i64::try_from(span.as_secs()).unwrap_or(i64::MAX))
}

/// Whether a fenced statement still requires the claim's lease to be unexpired.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LeaseRequirement {
    MustBeLive,
    /// The claim token is minted per acquisition, so a lease that expired and
    /// was re-claimed is already caught by the token. Only statements that
    /// deliberately act after a lease may have lapsed use this.
    MayHaveExpired,
}

/// Whether a fenced statement requires the sandbox to still be activatable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SandboxStateRequirement {
    MustBeActivatable,
    AnyState,
}

/// Whether a fenced statement requires no durable cleanup episode to own the
/// sandbox.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CleanupRequirement {
    MustBeAbsent,
    MayExist,
}

/// One exact legacy-recovery claim, rendered as a SQL predicate.
///
/// Every statement that commits under a claim shares this predicate instead of
/// spelling it out. The seven identity conjuncts are not optional: a claimant
/// whose episode changed source, reason, owner, token, or attempt count must not
/// be able to write. The three environmental guards are per statement and each
/// relaxation has to name itself, so a missing guard is a visible argument at
/// the call site rather than an omission buried in a forty-line string.
struct ExactClaimFence {
    sandbox_id: String,
    quarantine_id: String,
    source: &'static str,
    reason_code: &'static str,
    owner: &'static str,
    claim_token: String,
    attempt_count: u32,
    live_expiry: Option<rusqlite::types::Value>,
    lease: LeaseRequirement,
    sandbox_state: SandboxStateRequirement,
    cleanup: CleanupRequirement,
}

impl ExactClaimFence {
    fn new(
        conn: &Connection,
        claimed: &ClaimedQuarantineRecoveryCandidate,
        now_epoch: i64,
        lease: LeaseRequirement,
        sandbox_state: SandboxStateRequirement,
        cleanup: CleanupRequirement,
    ) -> Result<Self> {
        // SQLite numeric CAST accepts prefixes and saturates overflow. Validate
        // the deadline in Rust, then bind its original storage value so a change
        // between this read and the guarded statement cannot reuse that proof.
        let live_expiry = if lease == LeaseRequirement::MustBeLive {
            conn.query_row(
                "SELECT claim_expires_at FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
                [claimed.candidate.sandbox_id.to_string()],
                |row| row.get::<_, rusqlite::types::Value>(0),
            )
            .optional()?
            .filter(|value| {
                timestamps::EpochSeconds::decode(value).is_ok_and(|value| value.get() > now_epoch)
            })
        } else {
            None
        };
        Ok(Self {
            sandbox_id: claimed.candidate.sandbox_id.to_string(),
            quarantine_id: claimed.candidate.local_quarantine_id.to_string(),
            source: SandboxQuarantineSource::SbxManager.as_str(),
            reason_code: claimed.candidate.reason_code.as_str(),
            owner: QuarantineRecoveryOwner::LegacyRecovery.as_str(),
            claim_token: claimed.claim_token.to_string(),
            attempt_count: claimed.candidate.attempt_count,
            live_expiry,
            lease,
            sandbox_state,
            cleanup,
        })
    }

    /// The predicate identifying the quarantine row, qualified by `alias`
    /// (`"qs."` when the statement joins it, `""` when it is the update target).
    fn quarantine_row_predicate(&self, alias: &str) -> String {
        let mut clauses = vec![
            format!("{alias}sandbox_id = :fence_sandbox_id"),
            format!("{alias}quarantine_id = :fence_quarantine_id"),
            format!("{alias}source = :fence_source"),
            format!("{alias}reason_code = :fence_reason_code"),
            format!("{alias}next_action_owner = :fence_owner"),
            format!("{alias}claim_token = :fence_claim_token"),
            format!("{alias}attempt_count = :fence_attempt_count"),
        ];
        if self.lease == LeaseRequirement::MustBeLive {
            clauses.push(format!("{alias}claim_expires_at = :fence_live_expiry"));
        }
        clauses.join("\n            AND ")
    }

    /// The guards on the sandbox that the claim's episode belongs to, where
    /// `sandbox_ref` is the SQL expression naming its id.
    fn sandbox_guards(&self, sandbox_ref: &str) -> String {
        let mut clauses = Vec::new();
        if self.sandbox_state == SandboxStateRequirement::MustBeActivatable {
            clauses.push(format!(
                "EXISTS (
                SELECT 1 FROM sandboxes fence_s
                 WHERE fence_s.id = {sandbox_ref}
                   AND fence_s.state IN ('quarantined', 'restoring')
            )"
            ));
        }
        if self.cleanup == CleanupRequirement::MustBeAbsent {
            clauses.push(format!(
                "NOT EXISTS (
                SELECT 1 FROM sandbox_cleanup_episodes fence_cleanup
                 WHERE fence_cleanup.sandbox_id = {sandbox_ref}
            )"
            ));
        }
        if clauses.is_empty() {
            return String::new();
        }
        format!("\n            AND {}", clauses.join("\n            AND "))
    }

    /// The binds for exactly the names the emitted predicate references.
    ///
    /// SQLite rejects a bind for a name the statement does not mention, so this
    /// has to track `quarantine_row_predicate` and cannot supply a superset.
    fn binds(&self) -> Vec<(&'static str, &dyn rusqlite::ToSql)> {
        let mut binds: Vec<(&'static str, &dyn rusqlite::ToSql)> = vec![
            (":fence_sandbox_id", &self.sandbox_id),
            (":fence_quarantine_id", &self.quarantine_id),
            (":fence_source", &self.source),
            (":fence_reason_code", &self.reason_code),
            (":fence_owner", &self.owner),
            (":fence_claim_token", &self.claim_token),
            (":fence_attempt_count", &self.attempt_count),
        ];
        if self.lease == LeaseRequirement::MustBeLive {
            binds.push((":fence_live_expiry", &self.live_expiry));
        }
        binds
    }
}

pub fn claim_quarantine_recovery_candidate(
    conn: &Connection,
    candidate: &QuarantineRecoveryCandidate,
) -> Result<Option<ClaimedQuarantineRecoveryCandidate>> {
    let current_epoch = now_epoch_i64()?;
    let deadlines = conn
        .query_row(
            "SELECT next_action_after, claim_token, claim_expires_at
         FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
            [candidate.sandbox_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, rusqlite::types::Value>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, rusqlite::types::Value>(2)?,
                ))
            },
        )
        .optional()?;
    let Some((next_action, previous_token, previous_expiry)) = deadlines else {
        return Ok(None);
    };
    let due = matches!(next_action, rusqlite::types::Value::Null)
        || timestamps::EpochSeconds::decode(&next_action)
            .is_ok_and(|value| value.get() <= current_epoch);
    let expiry = match &previous_expiry {
        rusqlite::types::Value::Null => None,
        value => match timestamps::EpochSeconds::decode(value) {
            Ok(value) => Some(value.get()),
            Err(_) => return Ok(None),
        },
    };
    if !due || (previous_token.is_some() && expiry.is_some_and(|value| value > current_epoch)) {
        return Ok(None);
    }
    let claim_expires_at = epoch_secs_after(current_epoch, QUARANTINE_RECOVERY_CLAIM_LEASE);
    let claim_token = uuid::Uuid::now_v7();
    let attempt_count = candidate.attempt_count.saturating_add(1);
    let next_attempt_at = epoch_secs_after(
        claim_expires_at,
        quarantine_recovery_backoff(candidate.local_quarantine_id, attempt_count),
    );
    let changed = conn.execute(
        "UPDATE sandbox_quarantine_state AS qs
            SET next_action_owner = ?1,
                claim_token = ?2,
                claim_expires_at = ?3,
                attempt_count = ?4,
                next_action_after = ?5,
                updated_at = ?6
          WHERE qs.sandbox_id = ?7
            AND qs.quarantine_id = ?8
            AND qs.source = ?9
            AND qs.reason_code = ?10
            AND (qs.next_action_owner IS NULL OR qs.next_action_owner = ?1)
            AND qs.next_action_after IS ?12
            AND qs.claim_expires_at IS ?13
            AND qs.claim_token IS ?14
            AND qs.attempt_count = ?11
            AND EXISTS (
                SELECT 1 FROM sandboxes s
                 WHERE s.id = qs.sandbox_id
                   AND s.state IN ('quarantined', 'restoring')
            )
            AND NOT EXISTS (
                SELECT 1 FROM sandbox_cleanup_episodes cleanup
                 WHERE cleanup.sandbox_id = qs.sandbox_id
            )",
        params![
            QuarantineRecoveryOwner::LegacyRecovery.as_str(),
            claim_token.to_string(),
            claim_expires_at.to_string(),
            attempt_count,
            next_attempt_at.to_string(),
            current_epoch,
            candidate.sandbox_id.to_string(),
            candidate.local_quarantine_id.to_string(),
            SandboxQuarantineSource::SbxManager.as_str(),
            candidate.reason_code.as_str(),
            candidate.attempt_count,
            next_action,
            previous_expiry,
            previous_token,
        ],
    )?;
    Ok((changed == 1).then(|| ClaimedQuarantineRecoveryCandidate {
        candidate: QuarantineRecoveryCandidate {
            attempt_count,
            ..candidate.clone()
        },
        claim_token,
    }))
}

pub fn renew_quarantine_recovery_claim(
    conn: &Connection,
    claimed: &ClaimedQuarantineRecoveryCandidate,
) -> Result<bool> {
    let current_epoch = now_epoch_i64()?;
    let claim_expires_at = epoch_secs_after(current_epoch, QUARANTINE_RECOVERY_CLAIM_LEASE);
    let next_attempt_at = epoch_secs_after(
        claim_expires_at,
        quarantine_recovery_backoff(
            claimed.candidate.local_quarantine_id,
            claimed.candidate.attempt_count,
        ),
    );
    let fence = ExactClaimFence::new(
        conn,
        claimed,
        current_epoch,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::MustBeActivatable,
        CleanupRequirement::MustBeAbsent,
    )?;
    let claim_expires_at = claim_expires_at.to_string();
    let next_attempt_at = next_attempt_at.to_string();
    let sql = format!(
        "UPDATE sandbox_quarantine_state
            SET claim_expires_at = :claim_expires_at,
                next_action_after = :next_attempt_at,
                updated_at = :updated_at
          WHERE {}{}",
        fence.quarantine_row_predicate(""),
        fence.sandbox_guards("sandbox_quarantine_state.sandbox_id"),
    );
    let mut binds: Vec<(&str, &dyn rusqlite::ToSql)> = vec![
        (":claim_expires_at", &claim_expires_at),
        (":next_attempt_at", &next_attempt_at),
        (":updated_at", &current_epoch),
    ];
    binds.extend(fence.binds());
    let changed = conn.prepare_cached(&sql)?.execute(&binds[..])?;
    Ok(changed == 1)
}

pub fn quarantine_recovery_claim_is_current(
    conn: &Connection,
    claimed: &ClaimedQuarantineRecoveryCandidate,
) -> Result<bool> {
    let fence = ExactClaimFence::new(
        conn,
        claimed,
        now_epoch_i64()?,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::MustBeActivatable,
        CleanupRequirement::MustBeAbsent,
    )?;
    let sql = format!(
        "SELECT EXISTS (
             SELECT 1
               FROM sandbox_quarantine_state AS qs
              WHERE {}{}
         )",
        fence.quarantine_row_predicate("qs."),
        fence.sandbox_guards("qs.sandbox_id"),
    );
    conn.prepare_cached(&sql)?
        .query_row(&fence.binds()[..], |row| row.get(0))
}

fn quarantine_recovery_backoff(local_quarantine_id: uuid::Uuid, attempt_count: u32) -> Duration {
    let shift = attempt_count.saturating_sub(1).min(20);
    let capped = QUARANTINE_RECOVERY_BACKOFF_BASE
        .saturating_mul(1_u32 << shift)
        .min(QUARANTINE_RECOVERY_BACKOFF_MAX);
    let jitter_range = capped / 5;
    if jitter_range.is_zero() {
        return capped;
    }
    let seed = local_quarantine_id
        .as_u128()
        .wrapping_add(u128::from(attempt_count));
    let offset = u64::try_from(seed % u128::from(jitter_range.as_secs().saturating_add(1)))
        .unwrap_or_default();
    capped.saturating_sub(Duration::from_secs(offset))
}

pub fn record_quarantine_recovery_no_progress(
    conn: &Connection,
    claimed: &ClaimedQuarantineRecoveryCandidate,
    reason: QuarantineRecoveryNoProgressReason,
) -> Result<QuarantineRecoveryNoProgressOutcome> {
    let current_epoch = now_epoch_i64()?;
    let next_attempt_at = epoch_secs_after(
        current_epoch,
        quarantine_recovery_backoff(
            claimed.candidate.local_quarantine_id,
            claimed.candidate.attempt_count,
        ),
    );
    // The seven identity conjuncts are what prove this process still owns the episode,
    // and they stay pinned. The three environmental guards are relaxed deliberately:
    // this statement's whole job is to record why an attempt made no progress and when
    // to try again, and requiring a live lease, an activatable sandbox and no cleanup
    // episode made it miss in exactly the situations it exists to report. A cleanup
    // owner arriving after the claim then left the lease standing for its full 15
    // minutes instead of one 48-60s backoff, and the episode out of the candidate set
    // with nothing recorded.
    //
    // There used to be a second statement with this identical SET clause and these
    // relaxations, reached only from the two call sites that had noticed the problem.
    // Two writers with the same effect and different preconditions make "which one did
    // this site pick" invisible to every statement-level test, which is why nine call
    // sites kept the strict one and reviewers found them one at a time.
    // `no_two_fenced_writers_share_a_column_set_under_different_guards` now refuses the
    // pair.
    let fence = ExactClaimFence::new(
        conn,
        claimed,
        current_epoch,
        LeaseRequirement::MayHaveExpired,
        SandboxStateRequirement::AnyState,
        CleanupRequirement::MayExist,
    )?;
    let next_attempt_at_text = next_attempt_at.to_string();
    let last_error = reason.as_str();
    let sql = format!(
        "UPDATE sandbox_quarantine_state
            SET next_action_after = :next_attempt_at,
                claim_token = NULL,
                claim_expires_at = NULL,
                last_error = :last_error,
                updated_at = :updated_at
          WHERE {}",
        fence.quarantine_row_predicate(""),
    );
    let mut binds: Vec<(&str, &dyn rusqlite::ToSql)> = vec![
        (":next_attempt_at", &next_attempt_at_text),
        (":last_error", &last_error),
        (":updated_at", &current_epoch),
    ];
    binds.extend(fence.binds());
    let changed = conn.prepare_cached(&sql)?.execute(&binds[..])?;
    if changed != 1 {
        return Ok(QuarantineRecoveryNoProgressOutcome::StaleClaim);
    }
    Ok(QuarantineRecoveryNoProgressOutcome::RetryScheduled {
        attempt_count: claimed.candidate.attempt_count,
        next_attempt_at,
    })
}

pub fn set_orphan_quarantine_recovery_generation(
    conn: &Connection,
    claimed: &ClaimedQuarantineRecoveryCandidate,
    execution_generation: ExecutionGeneration,
) -> Result<bool> {
    let current_epoch = now_epoch_i64()?;
    // `state = 'restoring'` below is stricter than the fence's activatable
    // check, so the fence does not repeat it.
    let fence = ExactClaimFence::new(
        conn,
        claimed,
        current_epoch,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::AnyState,
        CleanupRequirement::MustBeAbsent,
    )?;
    let generation = execution_generation.get();
    let sql = format!(
        "UPDATE sandboxes
            SET space_api_route_execution_generation = :generation,
                updated_at = :updated_at
          WHERE id = :fence_sandbox_id
            AND state = 'restoring'
            AND current_local_artifact_id IS NULL
            AND EXISTS (
                SELECT 1 FROM sandbox_quarantine_state qs
                 WHERE {}
            ){}",
        fence.quarantine_row_predicate("qs."),
        fence.sandbox_guards("sandboxes.id"),
    );
    let mut binds: Vec<(&str, &dyn rusqlite::ToSql)> = vec![
        (":generation", &generation),
        (":updated_at", &current_epoch),
    ];
    binds.extend(fence.binds());
    let changed = conn.prepare_cached(&sql)?.execute(&binds[..])?;
    Ok(changed == 1)
}

pub fn publish_orphan_quarantine_recovered_artifact(
    conn: &Connection,
    claimed: &ClaimedQuarantineRecoveryCandidate,
    local_artifact_id: Option<LocalArtifactId>,
    local_path: &str,
    execution_generation: ExecutionGeneration,
    receipt_id: SandboxCleanupEpisodeId,
) -> Result<bool> {
    let sandbox_id = claimed.candidate.sandbox_id;
    let sandbox_id_string = sandbox_id.to_string();
    let reason_code = SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let timestamp = now_epoch_i64()?;
    // `state = 'restoring'` is stricter than the fence's activatable check, so
    // the fence does not repeat it.
    let fence = ExactClaimFence::new(
        conn,
        claimed,
        timestamp,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::AnyState,
        CleanupRequirement::MustBeAbsent,
    )?;
    let local_artifact_id_text = local_artifact_id.map(|id| id.to_string());
    let receipt_id_text = receipt_id.to_string();
    let generation = execution_generation.get();
    let reason_code_text = reason_code.as_str();
    let publish_sql = format!(
        "UPDATE sandboxes
            SET state = 'suspended',
                restore_origin = NULL,
                restore_phase = NULL,
                restore_storage_uri = NULL,
                failure_reason = NULL,
                current_local_artifact_id = :local_artifact_id,
                current_local_artifact_kind = CASE WHEN :local_artifact_id IS NULL THEN NULL ELSE 'full' END,
                current_local_artifact_origin = CASE WHEN :local_artifact_id IS NULL THEN NULL ELSE 'suspend' END,
                transition_receipt_id = :receipt_id,
                transition_receipt_kind = 'quarantine_recovery',
                transition_receipt_target_state = 'suspended',
                transition_receipt_execution_generation = :generation,
                transition_receipt_activation_attempt_id = NULL,
                transition_receipt_reason_code = :reason_code,
                state_changed_at = CASE
                    WHEN state != 'suspended' THEN :timestamp
                    ELSE state_changed_at
                END,
                updated_at = :timestamp
          WHERE id = :fence_sandbox_id
            AND state = 'restoring'
            AND current_local_artifact_id IS NULL
            AND space_api_route_execution_generation = :generation
            AND EXISTS (
                SELECT 1 FROM sandbox_quarantine_state qs
                 WHERE {}
            ){}",
        fence.quarantine_row_predicate("qs."),
        fence.sandbox_guards("sandboxes.id"),
    );
    let tx = immediate_transaction(conn)?;
    let changed = {
        let mut binds: Vec<(&str, &dyn rusqlite::ToSql)> = vec![
            (":local_artifact_id", &local_artifact_id_text),
            (":receipt_id", &receipt_id_text),
            (":generation", &generation),
            (":reason_code", &reason_code_text),
            (":timestamp", &timestamp),
        ];
        binds.extend(fence.binds());
        tx.prepare_cached(&publish_sql)?.execute(&binds[..])?
    };
    if changed != 1 {
        return Ok(false);
    }
    if let Some(local_artifact_id) = local_artifact_id {
        upsert_suspend_local_artifact(&tx, &sandbox_id_string, local_artifact_id, local_path)?;
    }
    // Retiring the episode shares the claim predicate rather than restating a
    // subset of it. The previous hand-written version dropped `source` and the
    // lease guard, and pinned a hardcoded reason code instead of the claim's, so
    // it could match nothing while the caller still reported success — leaving a
    // quarantine row behind a sandbox now marked `suspended`, which the candidate
    // scan can never see again because it filters on quarantine-bearing states.
    let retire_fence = ExactClaimFence::new(
        &tx,
        claimed,
        timestamp,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::AnyState,
        CleanupRequirement::MayExist,
    )?;
    let retire_sql = format!(
        "DELETE FROM sandbox_quarantine_state WHERE {}",
        retire_fence.quarantine_row_predicate(""),
    );
    let retired = tx
        .prepare_cached(&retire_sql)?
        .execute(&retire_fence.binds()[..])?;
    if retired != 1 {
        // Unreachable as the two statements stand: this fence relaxes the cleanup
        // guard the update above requires and pins the same identity conjuncts at
        // the same timestamp inside one transaction, so a row the update matched
        // this one matches too. The check is here for the divergence, not for a
        // race — tighten this predicate relative to the update's and the two stop
        // agreeing, and the sandbox update would commit without the episode
        // retirement, which is the orphaned-row state described above. Abandoning
        // the transaction lets the next pass retry under a fresh claim.
        return Ok(false);
    }
    retire_restore_transfer_scope(&tx, &sandbox_id_string)?;
    tx.commit()?;
    emit_sandbox_state_event(
        &sandbox_id_string,
        SandboxEventType::QuarantineRecovered,
        Some(SandboxState::Quarantined.as_str()),
        reason_code.as_str(),
        "publish_orphan_quarantine_recovered_artifact",
        SandboxState::Suspended,
        None,
    );
    Ok(true)
}

/// Proof that a claim is on the adoption branch, carrying the artifact id the
/// eligibility gate validated for it.
///
/// Constructible only inside this module, and only by the gate, so it exists exactly
/// where the evidence was checked.
///
/// The adoption statement used to take a bare `LocalArtifactId` and check it against
/// the claim's branch, which left two problems: a caller could pass an id the gate
/// never approved, and the refusal for a restore-branch claim was an arm no test
/// could reach, because the only caller destructures the id out of the adoption
/// branch immediately before calling. Deriving the id from the branch closes both —
/// a wrong branch cannot be constructed and a wrong id cannot be supplied, so
/// neither needs a runtime check or a reason code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdoptableArtifact(LocalArtifactId);

impl AdoptableArtifact {
    pub const fn local_artifact_id(self) -> LocalArtifactId {
        self.0
    }
}

/// What the adoption statement did.
///
/// Named rather than a `bool` so the caller's disposition reads at the call site:
/// an adoption is terminal for the candidate, a fence miss is a race to back off
/// from. `AdoptableArtifact` removes the third case a bare id needed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuarantineAdoptionOutcome {
    Adopted,
    /// The fenced CAS matched nothing: a cleanup owner arrived, the lease lapsed,
    /// or one of the pinned identity conjuncts moved. Retryable.
    FenceChanged,
}

/// Adopt one policy-approved quarantine into the durable compensation worker.
/// The caller has already validated the canonical artifact; this CAS binds
/// that evidence to the current local quarantine episode and artifact pin.
///
/// The artifact comes in as an `AdoptableArtifact`, which only the adoption branch
/// can produce, so this statement cannot be handed a restore-branch claim or an id
/// the gate never approved.
pub fn adopt_quarantine_cleanup_episode(
    conn: &Connection,
    claimed: &ClaimedQuarantineRecoveryCandidate,
    adoptable: AdoptableArtifact,
    execution_generation: ExecutionGeneration,
    cleanup_id: SandboxCleanupEpisodeId,
    execution: &ExecutionMetadata,
    local_path: &str,
) -> Result<QuarantineAdoptionOutcome> {
    let candidate = &claimed.candidate;
    let sandbox_id = candidate.sandbox_id.to_string();
    let local_artifact_id = adoptable.local_artifact_id();
    let timestamp = now_epoch_i64()?;
    let message = "automatic recovery adopted validated suspend evidence";
    // `state = 'quarantined'` is stricter than the fence's activatable check, so
    // the fence does not repeat it.
    let fence = ExactClaimFence::new(
        conn,
        claimed,
        timestamp,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::AnyState,
        CleanupRequirement::MustBeAbsent,
    )?;
    let timestamp_text = timestamp.to_string();
    let local_artifact_id_text = local_artifact_id.to_string();
    let generation = execution_generation.get();
    let adopt_sql = format!(
        "UPDATE sandboxes
            SET state = 'cleanup_required',
                cleanup_completion = 'mark_suspended',
                cleanup_keep_filesystem = TRUE,
                space_api_route_execution_generation = :generation,
                failure_reason = :message,
                state_changed_at = CASE
                    WHEN state != 'cleanup_required' THEN :timestamp
                    ELSE state_changed_at
                END,
                updated_at = :timestamp
          WHERE id = :fence_sandbox_id
            AND state = 'quarantined'
            AND current_local_artifact_id = :local_artifact_id
            AND current_local_artifact_kind = 'full'
            AND current_local_artifact_origin = 'suspend'
            AND (space_api_route_execution_generation IS NULL
                 OR space_api_route_execution_generation = :generation)
            AND EXISTS (
                SELECT 1 FROM sandbox_quarantine_state qs
                 WHERE {}
            ){}",
        fence.quarantine_row_predicate("qs."),
        fence.sandbox_guards("sandboxes.id"),
    );
    let tx = immediate_transaction(conn)?;
    let changed = {
        let mut binds: Vec<(&str, &dyn rusqlite::ToSql)> = vec![
            (":generation", &generation),
            (":message", &message),
            (":timestamp", &timestamp_text),
            (":local_artifact_id", &local_artifact_id_text),
        ];
        binds.extend(fence.binds());
        tx.prepare_cached(&adopt_sql)?.execute(&binds[..])?
    };
    if changed != 1 {
        tx.commit()?;
        return Ok(QuarantineAdoptionOutcome::FenceChanged);
    }
    upsert_sandbox_execution_metadata_in_transaction(&tx, &sandbox_id, execution)?;
    upsert_suspend_local_artifact(&tx, &sandbox_id, local_artifact_id, local_path)?;
    // Runs inside the transaction that just committed the sandbox to cleanup, so
    // it deliberately does not re-check the environment it releases into: the
    // cleanup episode this function inserts below is expected to exist.
    let release_fence = ExactClaimFence::new(
        &tx,
        claimed,
        timestamp,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::AnyState,
        CleanupRequirement::MayExist,
    )?;
    let release_sql = format!(
        "UPDATE sandbox_quarantine_state
            SET claim_token = NULL, claim_expires_at = NULL, updated_at = :timestamp
          WHERE {}",
        release_fence.quarantine_row_predicate(""),
    );
    {
        let mut binds: Vec<(&str, &dyn rusqlite::ToSql)> = vec![(":timestamp", &timestamp_text)];
        binds.extend(release_fence.binds());
        // The count matters here as much as it does in the publisher, and for the
        // same reason: this release is fenced, so zero rows means the fence no
        // longer holds. Discarding it would leave the lease behind while the
        // cleanup episode below is inserted anyway — an adopted episode whose
        // claim never came off, unclaimable until the lease lapses. The earlier
        // `changed != 1` guard covers the `sandboxes` write, not this one.
        let released = tx.prepare_cached(&release_sql)?.execute(&binds[..])?;
        if released != 1 {
            return Ok(QuarantineAdoptionOutcome::FenceChanged);
        }
    }
    tx.execute(
        "INSERT INTO sandbox_cleanup_episodes (
             sandbox_id, cleanup_id, completion, keep_filesystem, phase,
             entered_from_state, activation_state, requested_at,
             quarantine_reason_code, failure_message, created_at,
             recovery_local_artifact_id, recovery_execution_generation
         ) VALUES (
             ?1, ?2, 'mark_suspended', TRUE, 'requested', 'quarantined',
             'restoring', ?3, ?4, ?5, ?3, ?6, ?7
         )",
        params![
            sandbox_id,
            cleanup_id.to_string(),
            timestamp,
            candidate.reason_code.as_str(),
            message,
            local_artifact_id.to_string(),
            execution_generation.get(),
        ],
    )?;
    tx.commit()?;
    emit_sandbox_state_event(
        &sandbox_id,
        SandboxEventType::CleanupRequested,
        Some(SandboxState::Quarantined.as_str()),
        candidate.reason_code.as_str(),
        "adopt_quarantine_cleanup_episode",
        SandboxState::CleanupRequired,
        Some(message),
    );
    Ok(QuarantineAdoptionOutcome::Adopted)
}

pub(crate) fn mark_quarantined_from_state(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
    source: SandboxQuarantineSource,
    reason_code: SandboxQuarantineReasonCode,
    reason_message: &str,
    next_action: SandboxQuarantineNextAction,
) -> std::result::Result<bool, MarkQuarantinedError> {
    mark_quarantined_inner(
        conn,
        id,
        Some(expected_state),
        source,
        reason_code,
        reason_message,
        next_action,
    )
}

fn mark_quarantined_inner(
    conn: &Connection,
    id: &str,
    expected_state: Option<SandboxState>,
    source: SandboxQuarantineSource,
    reason_code: SandboxQuarantineReasonCode,
    reason_message: &str,
    next_action: SandboxQuarantineNextAction,
) -> std::result::Result<bool, MarkQuarantinedError> {
    let reason_message = reason_message.trim();
    let reason_message = (!reason_message.is_empty()).then_some(reason_message);
    let previous_state = current_state_label(conn, id)?;
    if expected_state.is_some_and(|expected| {
        previous_state
            .as_deref()
            .and_then(|state| state.parse::<SandboxState>().ok())
            != Some(expected)
    }) {
        return Ok(false);
    }
    if let Some(previous_state_label) = previous_state.as_deref() {
        let previous_state_value = previous_state_label.parse::<SandboxState>().map_err(|_| {
            MarkQuarantinedError::UnknownCurrentState {
                state: previous_state_label.to_string(),
            }
        })?;
        previous_state_value.transition(SandboxState::Quarantined)?;
    }
    let starts_new_episode = previous_state.as_deref() != Some(SandboxState::Quarantined.as_str());
    let ts = now()?;

    let tx = conn.unchecked_transaction()?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'quarantined',
             failure_reason = ?1,
             suspend_predecessor_artifact_id = NULL,
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             transition_receipt_id = NULL,
             transition_receipt_kind = NULL,
             transition_receipt_target_state = NULL,
             transition_receipt_execution_generation = NULL,
             transition_receipt_activation_attempt_id = NULL,
             transition_receipt_reason_code = NULL,
             state_changed_at = CASE WHEN state != 'quarantined' THEN ?2 ELSE state_changed_at END,
             updated_at = ?2
         WHERE id = ?3 AND (?4 IS NULL OR state = ?4)",
        params![reason_message, ts, id, previous_state.as_deref()],
    )?;
    if changed == 0 {
        tracing::warn!(
            sandbox_id = %id,
            expected_state = previous_state.as_deref().unwrap_or("<missing>"),
            source = %source,
            reason_code = %reason_code,
            "ignored mark_quarantined because sandbox state changed before the CAS write"
        );
    }
    if changed != 0 {
        crate::route_activation_claims::close_claim(&tx, id)?;
        let episode = QuarantineEpisode {
            sandbox_id: id,
            entered_from_state: previous_state.as_deref(),
            source,
            reason_code,
            reason_message,
            next_action,
            timestamp: &ts,
        };
        if starts_new_episode {
            insert_new_quarantine_episode(&tx, &episode)?;
        } else {
            // A reconfirmation that changes source or reason invalidates the
            // retry state legacy recovery accumulated for the old reason — but
            // only once nothing is acting on it. Clearing a live lease would
            // make the episode look due and unclaimed while its claimed
            // operation is still running, so the next tick would start a second
            // one. A newer release's owner keeps its retry and lease columns, which
            // is the whole of its claim. This branch still rewrites `source`,
            // `reason_code`, `reason_message` and `evidence_json` on a
            // foreign-owned row — identity columns a future owner's own fence
            // would pin — because the new classification has to be recorded and
            // this row is where it lives.
            //
            // Nor does a later tick finish the job: `legacy_owned` is `owner IS
            // NULL OR owner = 'legacy_recovery'`, and a foreign owner string does
            // not become NULL when its lease lapses. The retry state stays until
            // that owner clears it — by design, not by deferral.
            // Three facts, not one: whether the classification actually changed,
            // whether legacy recovery still owns the row, and whether a lease is
            // live. The lease decides only whether the *lease* fields may be
            // cleared; the retry bookkeeping is invalidated by the change itself.
            let current_epoch = now_epoch_i64()?;
            let (classification_changed, legacy_owned, claim_live): (bool, bool, bool) = tx
                .query_row(
                    "SELECT (source != ?2 OR reason_code != ?3),
                        (next_action_owner IS NULL OR next_action_owner = ?4),
                        claim_expires_at
                   FROM sandbox_quarantine_state
                  WHERE sandbox_id = ?1",
                    params![
                        id,
                        source.as_str(),
                        reason_code.as_str(),
                        QuarantineRecoveryOwner::LegacyRecovery.as_str(),
                    ],
                    |row| {
                        let expiry: rusqlite::types::Value = row.get(2)?;
                        // Unknown expiry is not evidence that a previous writer
                        // stopped. Keep its lease until explicitly repaired.
                        let claim_live = !matches!(expiry, rusqlite::types::Value::Null)
                            && timestamps::EpochSeconds::decode(&expiry)
                                .map_or(true, |value| value.get() > current_epoch);
                        Ok((row.get(0)?, row.get(1)?, claim_live))
                    },
                )?;
            let reset_legacy_recovery_state = classification_changed && legacy_owned && !claim_live;
            // Under a live lease the lease itself is kept and the attempt count with
            // it, but the retry *time* is cleared. Three separate reasons, because
            // an earlier revision got each of them wrong in turn:
            //
            // The lease stays because clearing it would make the episode look due
            // and unclaimed while its operation is still doing physical work, so
            // the next tick would start a second one on the same sandbox.
            //
            // The attempt count stays because a sandbox whose classification keeps
            // changing would otherwise reset its curve on every reconfirmation and
            // land attempts one tick apart indefinitely — the cadence this exists
            // to bound. It is not kept because `ExactClaimFence` pins it: this
            // statement rewrites `source` and `reason_code`, which the fence pins
            // too, so the running operation's writes already miss regardless.
            // `quarantine_recovery_reason_change_preserves_a_live_claim` asserts
            // exactly that.
            //
            // The retry time is cleared because it was computed as claim expiry
            // plus a backoff, for a classification that no longer applies. Left in
            // place it strands the episode for the lease *and* that backoff — up to
            // half an hour — when the operation holding the lease can no longer
            // commit anything. Clearing it bounds the wait by the lease alone, and
            // the next attempt then recomputes the backoff from the preserved count.
            if reset_legacy_recovery_state {
                tx.execute(
                    "UPDATE sandbox_quarantine_state
                     SET updated_at = ?2,
                         next_action_after = NULL,
                         next_action_owner = NULL,
                         attempt_count = 0,
                         claim_token = NULL,
                         claim_expires_at = NULL,
                         last_error = NULL,
                         source = ?3,
                         reason_code = ?4,
                         reason_message = ?5,
                         evidence_json = '{}'
                     WHERE sandbox_id = ?1",
                    params![
                        id,
                        ts,
                        source.as_str(),
                        reason_code.as_str(),
                        reason_message
                    ],
                )?;
            } else if classification_changed && legacy_owned {
                tx.execute(
                    "UPDATE sandbox_quarantine_state
                     SET updated_at = ?2,
                         next_action_after = NULL,
                         last_error = NULL,
                         source = ?3,
                         reason_code = ?4,
                         reason_message = ?5,
                         evidence_json = '{}'
                     WHERE sandbox_id = ?1",
                    params![
                        id,
                        ts,
                        source.as_str(),
                        reason_code.as_str(),
                        reason_message
                    ],
                )?;
            } else {
                tx.execute(
                    "UPDATE sandbox_quarantine_state
                     SET updated_at = ?2,
                         source = ?3,
                         reason_code = ?4,
                         reason_message = ?5,
                         evidence_json = '{}'
                     WHERE sandbox_id = ?1",
                    params![
                        id,
                        ts,
                        source.as_str(),
                        reason_code.as_str(),
                        reason_message
                    ],
                )?;
            }
            let current_quarantine_id: String = tx.query_row(
                "SELECT quarantine_id FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
                params![id],
                |row| row.get(0),
            )?;
            insert_quarantine_history(
                &tx,
                &episode,
                &current_quarantine_id,
                SandboxQuarantineHistoryAction::ReconfirmedQuarantine,
            )?;
        }
    }
    tx.commit()?;

    if changed != 0 && previous_state.as_deref() != Some(SandboxState::Quarantined.as_str()) {
        emit_sandbox_state_event(
            id,
            SandboxEventType::SandboxQuarantined,
            previous_state.as_deref(),
            reason_code.as_str(),
            "mark_quarantined",
            SandboxState::Quarantined,
            None,
        );
    }
    Ok(changed != 0)
}

/// Presence is authoritative even when a newer binary wrote an intent this
/// reader cannot decode. Startup must not promote or recreate its resources.
pub(crate) fn has_cleanup_episode(conn: &Connection, id: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sandbox_cleanup_episodes WHERE sandbox_id = ?1)",
        [id],
        |row| row.get(0),
    )
}

pub fn cleanup_episode(conn: &Connection, id: &str) -> Result<Option<CleanupEpisode>> {
    conn.query_row(
        "SELECT cleanup_id, completion, keep_filesystem, entered_from_state,
                activation_state, requested_at, quarantine_reason_code,
                failure_message, last_cleanup_error, attempt_count,
                recovery_local_artifact_id, recovery_execution_generation,
                execution_generation, activation_attempt_id, prepared_clone_root,
                operation_activation_attempt_id
           FROM sandbox_cleanup_episodes
          WHERE sandbox_id = ?1",
        params![id],
        |row| {
            let cleanup_id = row.get::<_, String>(0)?.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })?;
            let completion = row.get::<_, String>(1)?.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(error))
            })?;
            let entered_from_state = row
                .get::<_, Option<String>>(3)?
                .map(|state| {
                    state.parse::<SandboxState>().map_err(|_| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            Type::Text,
                            Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("unknown cleanup entered-from state '{state}'"),
                            )),
                        )
                    })
                })
                .transpose()?;
            let activation_state_raw = row.get::<_, String>(4)?;
            let activation_state = activation_state_raw.parse::<SandboxState>().map_err(|_| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unknown cleanup activation state '{activation_state_raw}'"),
                    )),
                )
            })?;
            let quarantine_reason_code = row
                .get::<_, Option<String>>(6)?
                .map(|reason| {
                    reason.parse().map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(6, Type::Text, Box::new(error))
                    })
                })
                .transpose()?;
            let attempt_count = row.get::<_, i64>(9)?;
            let quarantine_recovery = match (
                row.get::<_, Option<String>>(10)?,
                row.get::<_, Option<i64>>(11)?,
            ) {
                (None, None) => None,
                (Some(local_artifact_id), Some(execution_generation)) => {
                    Some(QuarantineRecoveryFence {
                        local_artifact_id: local_artifact_id.parse().map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                10,
                                Type::Text,
                                Box::new(error),
                            )
                        })?,
                        execution_generation: ExecutionGeneration::new(execution_generation)
                            .map_err(|error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    11,
                                    Type::Integer,
                                    Box::new(error),
                                )
                            })?,
                    })
                }
                _ => {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        10,
                        Type::Null,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "quarantine recovery fence is incomplete",
                        )),
                    ));
                }
            };
            Ok(CleanupEpisode {
                operation_activation_attempt_id: row
                    .get::<_, Option<String>>(15)?
                    .map(|value| value.parse())
                    .transpose()
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(15, Type::Text, Box::new(error))
                    })?,
                prepared_clone_root: row
                    .get::<_, Option<String>>(14)?
                    .map(|json| {
                        serde_json::from_str(&json).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                14,
                                Type::Text,
                                Box::new(error),
                            )
                        })
                    })
                    .transpose()?,
                id: cleanup_id,
                completion,
                keep_filesystem: row.get(2)?,
                entered_from_state,
                activation_state,
                requested_at: row
                    .get::<_, Option<i64>>(5)?
                    .and_then(|value| u64::try_from(value).ok()),
                quarantine_reason_code,
                failure_message: row.get(7)?,
                last_cleanup_error: row.get(8)?,
                execution_generation: row
                    .get::<_, Option<i64>>(12)?
                    .map(ExecutionGeneration::new)
                    .transpose()
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            12,
                            Type::Integer,
                            Box::new(error),
                        )
                    })?,
                activation_attempt_id: row
                    .get::<_, Option<String>>(13)?
                    .map(|value| value.parse())
                    .transpose()
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(13, Type::Text, Box::new(error))
                    })?,
                attempt_count: u32::try_from(attempt_count).unwrap_or(u32::MAX),
                quarantine_recovery,
            })
        },
    )
    .optional()
}

/// Retire an armed episode only after another state writer has conclusively
/// moved the row out of the activation state it owned.
pub fn retire_stale_armed_cleanup_episode(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    observed_state: SandboxState,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let Some(episode) = cleanup_episode(&tx, sandbox_id)? else {
        tx.commit()?;
        return Ok(false);
    };
    if episode.id != cleanup_id
        || episode.completion == CleanupCompletion::DeletePoolClone
        || episode.requested_at.is_some()
        || episode.activation_state == observed_state
    {
        tx.commit()?;
        return Ok(false);
    }
    let deleted = tx.execute(
        "DELETE FROM sandbox_cleanup_episodes
          WHERE sandbox_id = ?1 AND cleanup_id = ?2 AND phase = 'armed'",
        params![sandbox_id, cleanup_id.to_string()],
    )?;
    if deleted == 1 {
        tx.execute(
            "UPDATE sandboxes
                SET cleanup_completion = NULL,
                    cleanup_keep_filesystem = NULL
              WHERE id = ?1
                AND state = ?2
                AND cleanup_completion = ?3
                AND cleanup_keep_filesystem = ?4",
            params![
                sandbox_id,
                observed_state.as_str(),
                episode.completion.as_str(),
                episode.keep_filesystem,
            ],
        )?;
    }
    tx.commit()?;
    Ok(deleted == 1)
}

pub fn request_cleanup_episode(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    reason_code: SandboxQuarantineReasonCode,
    failure_message: &str,
) -> std::result::Result<CleanupEpisodeRequestOutcome, RequestCleanupEpisodeError> {
    let tx = immediate_transaction(conn)?;
    let Some(episode) = cleanup_episode(&tx, sandbox_id)? else {
        tx.commit()?;
        return Ok(CleanupEpisodeRequestOutcome::Superseded);
    };
    if episode.id != cleanup_id {
        tx.commit()?;
        return Ok(CleanupEpisodeRequestOutcome::Superseded);
    }
    if episode.requested_at.is_some() {
        let current = cleanup_episode_current(&tx, sandbox_id, cleanup_id)?;
        tx.commit()?;
        return Ok(if current {
            CleanupEpisodeRequestOutcome::AlreadyRequested
        } else {
            CleanupEpisodeRequestOutcome::Superseded
        });
    }
    let previous_state = get_sandbox(&tx, sandbox_id)?.map(|row| row.state);
    let requested_at = now_epoch_i64()?;
    let preserve_restore_operation = episode.quarantine_recovery.is_some();
    let changed = tx.execute(
        "UPDATE sandboxes
            SET state = 'cleanup_required',
                failure_reason = ?1,
                restore_origin = CASE WHEN ?6 THEN restore_origin ELSE NULL END,
                restore_phase = CASE WHEN ?6 THEN restore_phase ELSE NULL END,
                restore_storage_uri = CASE WHEN ?6 THEN restore_storage_uri ELSE NULL END,
                state_changed_at = CASE
                    WHEN state != 'cleanup_required' THEN ?2
                    ELSE state_changed_at
                END,
                updated_at = ?2
          WHERE id = ?3
            AND state NOT IN ('deleting', 'quarantined')
            AND cleanup_completion = ?4
            AND cleanup_keep_filesystem = ?5",
        params![
            failure_message,
            requested_at.to_string(),
            sandbox_id,
            episode.completion.as_str(),
            episode.keep_filesystem,
            preserve_restore_operation,
        ],
    )?;
    if changed != 1 {
        tx.commit()?;
        return Ok(CleanupEpisodeRequestOutcome::Superseded);
    }
    let episode_changed = tx.execute(
        "UPDATE sandbox_cleanup_episodes
            SET phase = 'requested',
                requested_at = ?1,
                quarantine_reason_code = ?2,
                failure_message = ?3,
                last_cleanup_error = NULL
          WHERE sandbox_id = ?4
            AND cleanup_id = ?5
            AND phase = 'armed'",
        params![
            requested_at,
            reason_code.as_str(),
            failure_message,
            sandbox_id,
            cleanup_id.to_string(),
        ],
    )?;
    if episode_changed != 1 {
        return Err(RequestCleanupEpisodeError::PhaseUpdateInvariant {
            changed: episode_changed,
        });
    }
    tx.commit()?;
    emit_sandbox_state_event(
        sandbox_id,
        SandboxEventType::CleanupRequested,
        previous_state.as_ref().map(SandboxState::as_str),
        reason_code.as_str(),
        "request_cleanup_episode",
        SandboxState::CleanupRequired,
        Some(failure_message),
    );
    Ok(CleanupEpisodeRequestOutcome::Requested)
}

pub fn record_cleanup_episode_failure(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    failure_message: &str,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandbox_cleanup_episodes
            SET last_cleanup_error = ?1,
                attempt_count = CASE WHEN completion = 'delete_pool_clone' THEN attempt_count ELSE attempt_count + 1 END
          WHERE sandbox_id = ?2
            AND cleanup_id = ?3
            AND phase = 'requested'",
        params![failure_message, sandbox_id, cleanup_id.to_string()],
    )?;
    let previous_state = if changed == 1 {
        current_state_label(&tx, sandbox_id)?.and_then(|state| state.parse::<SandboxState>().ok())
    } else {
        None
    };
    let mut state_changed = 0;
    if changed == 1 {
        state_changed = tx.execute(
            "UPDATE sandboxes
                SET state = 'cleanup_failed',
                    state_changed_at = CASE
                        WHEN state != 'cleanup_failed' THEN ?1
                        ELSE state_changed_at
                    END,
                    updated_at = ?1
              WHERE id = ?2
                AND state IN ('cleanup_required', 'cleanup_failed')
                AND cleanup_completion = (
                    SELECT completion FROM sandbox_cleanup_episodes
                     WHERE sandbox_id = ?2 AND cleanup_id = ?3
                )",
            params![now()?, sandbox_id, cleanup_id.to_string()],
        )?;
    }
    tx.commit()?;
    if state_changed == 1 && previous_state == Some(SandboxState::CleanupRequired) {
        emit_sandbox_state_event(
            sandbox_id,
            SandboxEventType::CleanupFailed,
            Some(SandboxState::CleanupRequired.as_str()),
            "activation_cleanup_failed",
            "record_cleanup_episode_failure",
            SandboxState::CleanupFailed,
            Some(failure_message),
        );
    }
    Ok(changed == 1)
}

pub fn cleanup_episode_current(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1
               FROM sandbox_cleanup_episodes episode
               JOIN sandboxes sandbox ON sandbox.id = episode.sandbox_id
              WHERE episode.sandbox_id = ?1
                AND episode.cleanup_id = ?2
                AND episode.phase = 'requested'
                AND sandbox.state IN ('cleanup_required', 'cleanup_failed')
                AND sandbox.cleanup_completion = episode.completion
                AND sandbox.cleanup_keep_filesystem = episode.keep_filesystem
         )",
        params![sandbox_id, cleanup_id.to_string()],
        |row| row.get(0),
    )
}

#[derive(Clone, Debug)]
struct QuarantineRecoveryCompletion {
    quarantine_id: String,
    reason_code: SandboxQuarantineReasonCode,
    receipt_id: SandboxCleanupEpisodeId,
    execution_generation: ExecutionGeneration,
}

enum QuarantineRecoveryMatch {
    NotApplicable,
    Superseded,
    Current(QuarantineRecoveryCompletion),
}

fn validate_cleanup_completion_artifact(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
    episode: &CleanupEpisode,
) -> std::result::Result<(), CompleteCleanupEpisodeError> {
    if episode.completion != CleanupCompletion::MarkSuspended {
        return Ok(());
    }

    let row = get_sandbox(tx, sandbox_id)?.ok_or(CompleteCleanupEpisodeError::MissingSandboxRow)?;
    if !sandbox_has_registered_full_suspend_artifact(tx, &row)? {
        return Err(CompleteCleanupEpisodeError::MissingSuspendArtifact);
    }
    Ok(())
}

fn match_quarantine_recovery(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    episode: &CleanupEpisode,
) -> Result<QuarantineRecoveryMatch> {
    if episode.entered_from_state != Some(SandboxState::Quarantined) {
        return Ok(QuarantineRecoveryMatch::NotApplicable);
    }
    let Some(reason_code) = episode
        .quarantine_reason_code
        .filter(|reason| reason.automatic_recovery_policy().is_some())
    else {
        return Ok(QuarantineRecoveryMatch::Superseded);
    };
    let Some(recovery_fence) = episode.quarantine_recovery else {
        return Ok(QuarantineRecoveryMatch::Superseded);
    };

    let recovery = tx
        .query_row(
            "SELECT qs.quarantine_id, s.space_api_route_execution_generation
               FROM sandbox_quarantine_state qs
               JOIN sandboxes s ON s.id = qs.sandbox_id
              WHERE qs.sandbox_id = ?1
                AND qs.source = ?2
                AND qs.reason_code = ?3
                AND s.state IN ('cleanup_required', 'cleanup_failed')
                AND s.current_local_artifact_id = ?4
                AND s.current_local_artifact_kind = 'full'
                AND s.current_local_artifact_origin = 'suspend'
                AND s.space_api_route_execution_generation = ?5",
            params![
                sandbox_id,
                SandboxQuarantineSource::SbxManager.as_str(),
                reason_code.as_str(),
                recovery_fence.local_artifact_id.to_string(),
                recovery_fence.execution_generation.get(),
            ],
            |row| {
                let execution_generation = ExecutionGeneration::new(row.get::<_, i64>(1)?)
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(1, Type::Integer, Box::new(error))
                    })?;
                Ok(QuarantineRecoveryCompletion {
                    quarantine_id: row.get(0)?,
                    reason_code,
                    receipt_id: cleanup_id,
                    execution_generation,
                })
            },
        )
        .optional()?;

    Ok(recovery.map_or(
        QuarantineRecoveryMatch::Superseded,
        QuarantineRecoveryMatch::Current,
    ))
}

fn transition_receipt_for_completion(
    cleanup_id: SandboxCleanupEpisodeId,
    episode: &CleanupEpisode,
    recovery: Option<&QuarantineRecoveryCompletion>,
) -> std::result::Result<
    Option<SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
    SandboxTransitionReceiptInvariantError,
> {
    if let Some(recovery) = recovery {
        return Ok(Some(SandboxTransitionReceipt::quarantine_recovery(
            recovery.receipt_id,
            recovery.execution_generation,
            recovery.reason_code,
            // Prefer the independent operation owner for lifecycle receipts.
            episode
                .operation_activation_attempt_id
                .or(episode.activation_attempt_id),
        )));
    }

    episode
        .completion
        .transition_target()
        .zip(episode.execution_generation)
        .zip(episode.quarantine_reason_code)
        // Old episodes have no operation field; preserve their legacy receipt.
        .zip(
            episode
                .operation_activation_attempt_id
                .or(episode.activation_attempt_id),
        )
        .map(
            |(((target_state, execution_generation), reason_code), activation_attempt_id)| {
                SandboxTransitionReceipt::try_new(
                    cleanup_id,
                    SandboxTransitionReceiptKind::ActivationRollback,
                    target_state,
                    execution_generation,
                    Some(reason_code),
                    Some(activation_attempt_id),
                )
            },
        )
        .transpose()
}

fn apply_cleanup_state(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
    target_state: SandboxState,
    receipt: Option<SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
    failure_reason: Option<&str>,
    published_suspend_artifact: Option<(LocalArtifactId, &str)>,
) -> Result<bool> {
    if let Some((local_artifact_id, local_path)) = published_suspend_artifact {
        upsert_suspend_local_artifact(tx, sandbox_id, local_artifact_id, local_path)?;
    }
    let published_artifact_id = published_suspend_artifact.map(|(id, _)| id.to_string());
    let changed = tx.execute(
        "UPDATE sandboxes
            SET state = ?1,
                uid = NULL,
                slot = NULL,
                guest_cid = NULL,
                pid = NULL,
                spaced_host_pid = NULL,
                netgtw_pid = NULL,
                vsock_uds_path = NULL,
                api_socket_path = NULL,
                space_api_route_generation = NULL,
                space_api_route_node_id = NULL,
                space_api_route_execution_generation = CASE
                    WHEN ?1 = 'suspended'
                         AND (
                             ?11 IS NOT NULL
                             OR (
                                 current_local_artifact_id IS NOT NULL
                                 AND current_local_artifact_kind = 'full'
                                 AND current_local_artifact_origin = 'suspend'
                             )
                         )
                    THEN space_api_route_execution_generation
                    ELSE NULL
                END,
                space_api_route_pod_uid = CASE
                    WHEN ?1 = 'suspended'
                         AND (
                             ?11 IS NOT NULL
                             OR (
                                 current_local_artifact_id IS NOT NULL
                                 AND current_local_artifact_kind = 'full'
                                 AND current_local_artifact_origin = 'suspend'
                             )
                         )
                    THEN space_api_route_pod_uid
                    ELSE NULL
                END,
                space_api_route_substrate_kind = NULL,
                space_api_route_activation_attempt_id = NULL,
                space_api_route_identity_provenance = NULL,
                spaced_endpoint_proof_json = NULL,
                guest_uptime_secs = NULL,
                cleanup_completion = NULL,
                cleanup_keep_filesystem = NULL,
                restore_origin = NULL,
                restore_phase = NULL,
                restore_storage_uri = NULL,
                transition_receipt_id = CASE
                    WHEN ?4 IS NOT NULL THEN ?4
                    WHEN ?1 = transition_receipt_target_state THEN transition_receipt_id
                    ELSE NULL
                END,
                transition_receipt_kind = CASE
                    WHEN ?4 IS NOT NULL THEN ?5
                    WHEN ?1 = transition_receipt_target_state THEN transition_receipt_kind
                    ELSE NULL
                END,
                transition_receipt_target_state = CASE
                    WHEN ?4 IS NOT NULL THEN ?6
                    WHEN ?1 = transition_receipt_target_state THEN transition_receipt_target_state
                    ELSE NULL
                END,
                transition_receipt_execution_generation = CASE
                    WHEN ?4 IS NOT NULL THEN ?7
                    WHEN ?1 = transition_receipt_target_state THEN transition_receipt_execution_generation
                    ELSE NULL
                END,
                transition_receipt_reason_code = CASE
                    WHEN ?4 IS NOT NULL THEN ?8
                    WHEN ?1 = transition_receipt_target_state THEN transition_receipt_reason_code
                    ELSE NULL
                END,
                transition_receipt_activation_attempt_id = CASE
                    WHEN ?4 IS NOT NULL THEN ?9
                    WHEN ?1 = transition_receipt_target_state THEN transition_receipt_activation_attempt_id
                    ELSE NULL
                END,
                failure_reason = ?10,
                current_local_artifact_id = COALESCE(?11, current_local_artifact_id),
                current_local_artifact_kind = CASE
                    WHEN ?11 IS NULL THEN current_local_artifact_kind ELSE 'full'
                END,
                current_local_artifact_origin = CASE
                    WHEN ?11 IS NULL THEN current_local_artifact_origin ELSE 'suspend'
                END,
                suspend_predecessor_artifact_id = NULL,
                state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
                updated_at = ?2
          WHERE id = ?3",
        params![
            target_state.as_str(),
            now()?,
            sandbox_id,
            receipt.map(|receipt| receipt.id().to_string()),
            receipt.map(|receipt| receipt.kind().as_str()),
            receipt.map(|receipt| receipt.target_state().as_str()),
            receipt.map(|receipt| receipt.execution_generation().get()),
            receipt.and_then(|receipt| receipt.reason_code().map(|reason| reason.as_str())),
            receipt.and_then(|receipt| receipt.activation_attempt_id().map(|id| id.to_string())),
            failure_reason,
            published_artifact_id,
        ],
    )?;
    Ok(changed == 1)
}

fn complete_state_cleanup(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    episode: &CleanupEpisode,
    recovery: Option<&QuarantineRecoveryCompletion>,
) -> std::result::Result<bool, CompleteCleanupEpisodeError> {
    let quarantine_completion = episode.completion == CleanupCompletion::Quarantine;
    let target_state = episode
        .completion
        .target_state()
        .ok_or(CompleteCleanupEpisodeError::DeleteHasNoTargetState)?;
    let receipt = transition_receipt_for_completion(cleanup_id, episode, recovery)?;
    let retained_failure_reason = if recovery.is_some() {
        None
    } else {
        episode.failure_message.as_deref()
    };
    let previous_state = current_state_label(tx, sandbox_id)?;
    let applied = apply_cleanup_state(
        tx,
        sandbox_id,
        target_state,
        receipt,
        retained_failure_reason,
        None,
    )?;
    if applied && quarantine_completion {
        insert_operator_quarantine_episode(
            tx,
            &QuarantineEpisode {
                sandbox_id,
                entered_from_state: previous_state.as_deref(),
                source: SandboxQuarantineSource::SbxManager,
                reason_code: episode
                    .quarantine_reason_code
                    .unwrap_or(SandboxQuarantineReasonCode::RuntimeProbeInconsistent),
                reason_message: retained_failure_reason,
                next_action: SandboxQuarantineNextAction::RequiresOperatorAttention,
                timestamp: &now()?,
            },
        )?;
    }
    Ok(applied)
}

fn retire_quarantine_recovery(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
    recovery: &QuarantineRecoveryCompletion,
) -> std::result::Result<(), CompleteCleanupEpisodeError> {
    let inserted = tx.execute(
        "INSERT INTO sandbox_quarantine_history (
             id, quarantine_id, sandbox_id, occurred_at, source, action,
             reason_code, message, next_action, evidence_json
         )
         SELECT ?1, quarantine_id, sandbox_id, ?2, ?3, ?4,
                reason_code, 'canonical suspend artifact validated after runtime cleanup',
                NULL, evidence_json
           FROM sandbox_quarantine_state
          WHERE sandbox_id = ?5
            AND quarantine_id = ?6
            AND reason_code = ?7",
        params![
            recovery.receipt_id.to_string(),
            now()?,
            SandboxQuarantineSource::SbxManager.as_str(),
            SandboxQuarantineHistoryAction::RecoveredToSuspended.as_str(),
            sandbox_id,
            recovery.quarantine_id,
            recovery.reason_code.as_str(),
        ],
    )?;
    if inserted != 1 {
        return Err(CompleteCleanupEpisodeError::RecoveryHistoryInvariant { changed: inserted });
    }

    let removed = tx.execute(
        "DELETE FROM sandbox_quarantine_state
          WHERE sandbox_id = ?1 AND quarantine_id = ?2",
        params![sandbox_id, recovery.quarantine_id],
    )?;
    if removed != 1 {
        return Err(
            CompleteCleanupEpisodeError::RecoveryStateRetirementInvariant { changed: removed },
        );
    }
    Ok(())
}

/// Apply an activation rollback and retire its fence in one transaction.
///
/// Runtime cleanup happens before this call. The episode id prevents an old
/// worker from completing a newer activation, while the transaction prevents
/// a crash from leaving a healthy row blocked by a requested episode.
pub fn complete_cleanup_episode(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
) -> std::result::Result<Option<()>, CompleteCleanupEpisodeError> {
    complete_cleanup_episode_for_job(conn, sandbox_id, cleanup_id, None)
}

pub(crate) fn complete_cleanup_episode_for_job(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    job_id: Option<&JobId>,
) -> std::result::Result<Option<()>, CompleteCleanupEpisodeError> {
    let tx = immediate_transaction(conn)?;
    let Some(episode) = cleanup_episode(&tx, sandbox_id)? else {
        tx.commit()?;
        return Ok(None);
    };
    if episode.id != cleanup_id
        || episode.requested_at.is_none()
        || !cleanup_episode_current(&tx, sandbox_id, cleanup_id)?
    {
        tx.commit()?;
        return Ok(None);
    }

    let previous_state = current_state_label(&tx, sandbox_id)?;
    validate_cleanup_completion_artifact(&tx, sandbox_id, &episode)?;
    let recovery = match match_quarantine_recovery(&tx, sandbox_id, cleanup_id, &episode)? {
        QuarantineRecoveryMatch::NotApplicable => None,
        QuarantineRecoveryMatch::Superseded => {
            tx.commit()?;
            return Ok(None);
        }
        QuarantineRecoveryMatch::Current(recovery) => Some(recovery),
    };

    if episode.completion.deletes_row() {
        let execution_generation = episode
            .execution_generation
            .or(runtime_execution_generation(&tx, sandbox_id)?);
        record_execution_delete_receipt(&tx, sandbox_id, execution_generation)?;
        delete_sandbox_child_rows(&tx, sandbox_id)?;
        tx.execute("DELETE FROM sandboxes WHERE id = ?1", params![sandbox_id])
            .map_err(crate::auto_lifecycle_delete_receipts::db::map_pending_receipt_delete_error)?;
    } else {
        if !complete_state_cleanup(&tx, sandbox_id, cleanup_id, &episode, recovery.as_ref())? {
            tx.commit()?;
            return Ok(None);
        }
        if let Some(recovery) = &recovery {
            retire_quarantine_recovery(&tx, sandbox_id, recovery)?;
        }
        // Created, Stopped, and Suspended are activation rollback states, not
        // completed lifecycle teardown. Keep the exact attempt's continuation
        // claim so a transiently failed RPC can retry after runtime cleanup.
        if episode.completion == CleanupCompletion::Quarantine {
            crate::route_activation_claims::close_claim(&tx, sandbox_id)?;
        }
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![sandbox_id, cleanup_id.to_string()],
        )?;
    }

    cleanup_jobs::finish_in_transaction(&tx, job_id, &cleanup_jobs::CleanupOutcome::Applied)?;
    tx.commit()?;
    if episode.completion == CleanupCompletion::Quarantine {
        emit_sandbox_state_event(
            sandbox_id,
            SandboxEventType::SandboxQuarantined,
            previous_state.as_deref(),
            episode
                .quarantine_reason_code
                .unwrap_or(SandboxQuarantineReasonCode::RuntimeProbeInconsistent)
                .as_str(),
            "complete_cleanup_episode",
            SandboxState::Quarantined,
            // Retain historical failure evidence in storage, but do not
            // republish potentially guest-controlled text in a new event.
            None,
        );
    }
    if let Some(recovery) = recovery {
        emit_sandbox_state_event(
            sandbox_id,
            SandboxEventType::QuarantineRecovered,
            Some(SandboxState::Quarantined.as_str()),
            recovery.reason_code.as_str(),
            "complete_cleanup_episode",
            SandboxState::Suspended,
            None,
        );
    }
    Ok(Some(()))
}

/// Why ordinary cleanup may not erase a sandbox row and its filesystem.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SandboxEraseSafety {
    Safe,
    SuspendPublicationPending,
    RetainedFilesystemOnlyCopy,
}

/// Classify whether erasing this record would drop unpublished suspend state.
///
/// The live and retry-owned states still have a suspend completion that can
/// publish. A terminal missing-artifact failure cannot publish, but its
/// retained rootfs may be the only copy of the sandbox's latest work. The
/// failure reason is the durable witness for that distinct safety condition.
pub(crate) fn sandbox_erase_safety(
    state: SandboxState,
    cleanup_completion: Option<CleanupCompletion>,
    failure_reason: Option<&str>,
) -> SandboxEraseSafety {
    if state == SandboxState::Suspending
        || (matches!(
            state,
            SandboxState::CleanupRequired | SandboxState::CleanupFailed
        ) && cleanup_completion == Some(CleanupCompletion::MarkSuspended))
    {
        SandboxEraseSafety::SuspendPublicationPending
    } else if matches!(state, SandboxState::Failed | SandboxState::Quarantined)
        && failure_reason == Some(SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
    {
        SandboxEraseSafety::RetainedFilesystemOnlyCopy
    } else {
        SandboxEraseSafety::Safe
    }
}

/// Complete cleanup owned by lifecycle intent stored directly on the sandbox
/// row. Routine stop, delete, kill, suspend, and startup recovery use this
/// path; activation compensation uses [`complete_cleanup_episode`] instead.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CompleteRowIntentCleanupError {
    #[error("row-deleting cleanup cannot complete as a resident state")]
    DeleteHasNoTargetState,
    #[error(transparent)]
    Database(#[from] rusqlite::Error),
    #[error(transparent)]
    ReceiptCompletion(
        #[from] crate::auto_lifecycle_delete_receipts::db::CompletePendingReceiptError,
    ),
    #[error(
        "expected auto-lifecycle delete receipt is missing or changed for sandbox {sandbox_id}"
    )]
    ExpectedReceiptMissingOrChanged { sandbox_id: SandboxId },
    #[error("pending receipt outlived its delete intent for sandbox {sandbox_id}")]
    PendingReceiptLostDeleteIntent { sandbox_id: SandboxId },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompleteRowIntentCleanupDisposition {
    Applied,
    Quarantined,
    Superseded,
}

fn receipt_superseding_execution_generation(
    conn: &Connection,
    row: &SandboxRow,
    expected_generation: ExecutionGeneration,
) -> Result<Option<ExecutionGeneration>> {
    let retained_generation = conn
        .query_row(
            "SELECT last_observed_execution_generation
               FROM sandboxes
              WHERE id = ?1",
            [row.id.as_str()],
            |sql_row| sql_row.get::<_, Option<i64>>(0),
        )?
        .map(ExecutionGeneration::new)
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(0, Type::Integer, Box::new(error))
        })?;

    // The current route generation identifies the execution attached to the
    // row. The retained generation is its monotonic route high-water mark.
    // Either one strictly advancing proves that the expected receipt no
    // longer owns this execution; NULL or lower evidence does not.
    Ok(row
        .space_api_route_identity
        .execution_generation
        .into_iter()
        .chain(retained_generation)
        .filter(|generation| *generation > expected_generation)
        .max())
}

fn emit_auto_lifecycle_delete_receipt_completion_superseded(
    sandbox_id: SandboxId,
    expected_receipt: crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt,
    superseding_generation: ExecutionGeneration,
) {
    emit_sandbox_event(
        &sandbox_id.to_string(),
        SandboxEventType::DeleteSuperseded,
        SandboxEventSource::SbxManager,
        SandboxEventDetails::default()
            .operation("delete")
            .trigger(SandboxEventTrigger::System)
            .result("superseded")
            .receipt_id(expected_receipt.receipt_id.to_string())
            .execution_generation(expected_receipt.execution_generation.to_string())
            .reason(format!(
                "delete receipt completion superseded by execution generation {superseding_generation}"
            )),
    );
}

#[cfg(test)]
pub(crate) fn complete_row_intent_cleanup(
    conn: &Connection,
    sandbox_id: &str,
    completion: CleanupCompletion,
    published_suspend_artifact: Option<(LocalArtifactId, &str)>,
) -> std::result::Result<(), CompleteRowIntentCleanupError> {
    complete_row_intent_cleanup_with_receipt(
        conn,
        sandbox_id,
        completion,
        published_suspend_artifact,
        None,
    )
    .map(|_| ())
}

#[cfg(test)]
pub(crate) fn complete_row_intent_cleanup_with_receipt(
    conn: &Connection,
    sandbox_id: &str,
    completion: CleanupCompletion,
    published_suspend_artifact: Option<(LocalArtifactId, &str)>,
    expected_receipt: Option<crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt>,
) -> std::result::Result<CompleteRowIntentCleanupDisposition, CompleteRowIntentCleanupError> {
    complete_row_intent_cleanup_for_job(
        conn,
        sandbox_id,
        completion,
        published_suspend_artifact,
        expected_receipt,
        None,
    )
}

pub(crate) fn complete_row_intent_cleanup_for_job(
    conn: &Connection,
    sandbox_id: &str,
    completion: CleanupCompletion,
    published_suspend_artifact: Option<(LocalArtifactId, &str)>,
    expected_receipt: Option<crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt>,
    job_id: Option<&JobId>,
) -> std::result::Result<CompleteRowIntentCleanupDisposition, CompleteRowIntentCleanupError> {
    if published_suspend_artifact.is_some() && completion != CleanupCompletion::MarkSuspended {
        return Err(rusqlite::Error::InvalidQuery.into());
    }
    let tx = immediate_transaction(conn)?;
    let parsed_delete_sandbox_id = (completion.deletes_row())
        .then(|| {
            sandbox_id
                .parse()
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        })
        .transpose()?;
    let pending_delete_receipt = parsed_delete_sandbox_id
        .map(|sandbox_id| {
            crate::auto_lifecycle_delete_receipts::db::pending_receipt_for_sandbox(&tx, sandbox_id)
        })
        .transpose()?
        .flatten();
    let Some(row) = get_sandbox(&tx, sandbox_id)? else {
        if let Some(sandbox_id) = parsed_delete_sandbox_id
            && pending_delete_receipt.is_some()
        {
            return Err(
                CompleteRowIntentCleanupError::PendingReceiptLostDeleteIntent { sandbox_id },
            );
        }
        tx.commit()?;
        return Ok(if job_id.is_some() {
            CompleteRowIntentCleanupDisposition::Superseded
        } else {
            CompleteRowIntentCleanupDisposition::Applied
        });
    };
    if let (Some(parsed_sandbox_id), Some(expected_receipt)) =
        (parsed_delete_sandbox_id, expected_receipt)
        && pending_delete_receipt != Some(expected_receipt)
    {
        if let Some(superseding_generation) = receipt_superseding_execution_generation(
            &tx,
            &row,
            expected_receipt.execution_generation,
        )? {
            tx.commit()?;
            emit_auto_lifecycle_delete_receipt_completion_superseded(
                parsed_sandbox_id,
                expected_receipt,
                superseding_generation,
            );
            return Ok(CompleteRowIntentCleanupDisposition::Superseded);
        }
        return Err(
            CompleteRowIntentCleanupError::ExpectedReceiptMissingOrChanged {
                sandbox_id: parsed_sandbox_id,
            },
        );
    }
    if cleanup_episode(&tx, sandbox_id)?.is_some() || !completion.owns_row_intent(&row) {
        if let Some(sandbox_id) = parsed_delete_sandbox_id
            && pending_delete_receipt.is_some()
        {
            return Err(
                CompleteRowIntentCleanupError::PendingReceiptLostDeleteIntent { sandbox_id },
            );
        }
        tx.commit()?;
        return Ok(if job_id.is_some() {
            CompleteRowIntentCleanupDisposition::Superseded
        } else {
            CompleteRowIntentCleanupDisposition::Applied
        });
    }
    if let Some(parsed_sandbox_id) = parsed_delete_sandbox_id {
        if let Some(receipt) = pending_delete_receipt {
            // A caller-supplied expected receipt was matched by the hoisted
            // ownership check. Startup and operator recovery instead derive
            // ownership from this durable receipt plus the DeleteRow intent.
            crate::auto_lifecycle_delete_receipts::db::complete_pending_receipt(
                &tx,
                parsed_sandbox_id,
                receipt.receipt_id,
            )?;
        }
        record_execution_delete_receipt(
            &tx,
            sandbox_id,
            row.space_api_route_identity.execution_generation,
        )?;
        delete_sandbox_child_rows(&tx, sandbox_id)?;
        tx.execute("DELETE FROM sandboxes WHERE id = ?1", params![sandbox_id])
            .map_err(crate::auto_lifecycle_delete_receipts::db::map_pending_receipt_delete_error)?;
        cleanup_jobs::finish_in_transaction(&tx, job_id, &cleanup_jobs::CleanupOutcome::Applied)?;
        tx.commit()?;
        return Ok(CompleteRowIntentCleanupDisposition::Applied);
    }

    let missing_suspend_artifact = completion == CleanupCompletion::MarkSuspended
        && published_suspend_artifact.is_none()
        && !sandbox_has_registered_full_suspend_artifact(&tx, &row)?;
    let orphan_cleanup = completion == CleanupCompletion::Quarantine
        && row.template == ORPHANED_SANDBOX_TEMPLATE
        && row.user == ORPHANED_SANDBOX_OWNER;
    let quarantine_completion = completion == CleanupCompletion::Quarantine;
    let quarantine_reason = if orphan_cleanup {
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing
    } else if missing_suspend_artifact
        || row.failure_reason.as_deref() == Some(SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
    {
        SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed
    } else {
        SandboxQuarantineReasonCode::RuntimeProbeInconsistent
    };
    let target_state = if missing_suspend_artifact {
        SandboxState::Quarantined
    } else {
        completion
            .target_state()
            .ok_or(CompleteRowIntentCleanupError::DeleteHasNoTargetState)?
    };
    let failure_reason = if missing_suspend_artifact {
        Some(SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
    } else if quarantine_completion {
        row.failure_reason.as_deref()
    } else {
        None
    };
    if !apply_cleanup_state(
        &tx,
        sandbox_id,
        target_state,
        None,
        failure_reason,
        published_suspend_artifact,
    )? {
        return Err(rusqlite::Error::QueryReturnedNoRows.into());
    }
    if missing_suspend_artifact {
        let timestamp = now()?;
        insert_new_quarantine_episode(
            &tx,
            &QuarantineEpisode {
                sandbox_id,
                entered_from_state: Some(row.state.as_str()),
                source: SandboxQuarantineSource::SbxManager,
                reason_code: SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed,
                reason_message: failure_reason,
                next_action: SandboxQuarantineNextAction::RequiresOperatorAttention,
                timestamp: &timestamp,
            },
        )?;
    }
    if quarantine_completion {
        insert_operator_quarantine_episode(
            &tx,
            &QuarantineEpisode {
                sandbox_id,
                entered_from_state: Some(row.state.as_str()),
                source: SandboxQuarantineSource::SbxManager,
                reason_code: quarantine_reason,
                reason_message: failure_reason,
                next_action: SandboxQuarantineNextAction::RequiresOperatorAttention,
                timestamp: &now()?,
            },
        )?;
    }
    // `apply_cleanup_state` clears the live identity and fires the retained
    // identity trigger. Close the claim only after that observation, but
    // before this transaction commits.
    crate::route_activation_claims::close_claim(&tx, sandbox_id)?;
    let outcome = if missing_suspend_artifact {
        cleanup_jobs::CleanupOutcome::Quarantined(
            SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE.to_owned(),
        )
    } else {
        cleanup_jobs::CleanupOutcome::Applied
    };
    cleanup_jobs::finish_in_transaction(&tx, job_id, &outcome)?;
    tx.commit()?;

    if missing_suspend_artifact || quarantine_completion {
        emit_sandbox_state_event(
            sandbox_id,
            SandboxEventType::SandboxQuarantined,
            Some(row.state.as_str()),
            quarantine_reason.as_str(),
            "complete_row_intent_cleanup",
            SandboxState::Quarantined,
            None,
        );
    }
    Ok(if missing_suspend_artifact {
        CompleteRowIntentCleanupDisposition::Quarantined
    } else {
        CompleteRowIntentCleanupDisposition::Applied
    })
}

pub fn quarantine_cleanup_episode(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    unsafe_evidence: &str,
) -> std::result::Result<bool, MarkQuarantinedError> {
    quarantine_cleanup_episode_for_job(conn, sandbox_id, cleanup_id, unsafe_evidence, None)
}

pub(crate) fn quarantine_cleanup_episode_for_job(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    unsafe_evidence: &str,
    job_id: Option<&JobId>,
) -> std::result::Result<bool, MarkQuarantinedError> {
    let tx = immediate_transaction(conn)?;
    let Some(episode) = cleanup_episode(&tx, sandbox_id)? else {
        tx.commit()?;
        return Ok(false);
    };
    if episode.id != cleanup_id {
        tx.commit()?;
        return Ok(false);
    }
    let Some(reason_code) = episode.quarantine_reason_code else {
        tx.commit()?;
        return Ok(false);
    };
    let Some(row) = get_sandbox(&tx, sandbox_id)? else {
        tx.commit()?;
        return Ok(false);
    };
    if !matches!(
        row.state,
        SandboxState::CleanupRequired | SandboxState::CleanupFailed
    ) {
        tx.commit()?;
        return Ok(false);
    }
    let reason = format!(
        "activation compensation cannot safely reach {}; unsafe_evidence={unsafe_evidence}",
        episode.completion,
    );
    row.state.transition(SandboxState::Quarantined)?;
    let timestamp = now()?;
    let changed = tx.execute(
        "UPDATE sandboxes
            SET state = 'quarantined',
                failure_reason = ?1,
                cleanup_completion = NULL,
                cleanup_keep_filesystem = NULL,
                restore_origin = NULL,
                restore_phase = NULL,
                restore_storage_uri = NULL,
                state_changed_at = CASE
                    WHEN state != 'quarantined' THEN ?2
                    ELSE state_changed_at
                END,
                updated_at = ?2
          WHERE id = ?3
            AND state = ?4
            AND cleanup_completion = ?5
            AND cleanup_keep_filesystem = ?6",
        params![
            reason,
            timestamp,
            sandbox_id,
            row.state.as_str(),
            episode.completion.as_str(),
            episode.keep_filesystem,
        ],
    )?;
    if changed == 1 {
        crate::route_activation_claims::close_claim(&tx, sandbox_id)?;
        insert_new_quarantine_episode(
            &tx,
            &QuarantineEpisode {
                sandbox_id,
                entered_from_state: Some(row.state.as_str()),
                source: SandboxQuarantineSource::SbxManager,
                reason_code,
                reason_message: Some(&reason),
                next_action: SandboxQuarantineNextAction::RequiresOperatorAttention,
                timestamp: &timestamp,
            },
        )?;
        if episode.completion != CleanupCompletion::DeletePoolClone {
            tx.execute(
                "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
                params![sandbox_id, cleanup_id.to_string()],
            )?;
        }
        cleanup_jobs::finish_in_transaction(
            &tx,
            job_id,
            &cleanup_jobs::CleanupOutcome::Quarantined(reason.clone()),
        )?;
    }
    tx.commit()?;
    if changed == 1 {
        emit_sandbox_state_event(
            sandbox_id,
            SandboxEventType::SandboxQuarantined,
            Some(row.state.as_str()),
            reason_code.as_str(),
            "quarantine_cleanup_episode",
            SandboxState::Quarantined,
            Some(&reason),
        );
    }
    Ok(changed == 1)
}

pub fn mark_cleanup_required(
    conn: &Connection,
    id: &str,
    keep_filesystem: bool,
    completion: CleanupCompletion,
    reason: &str,
) -> Result<()> {
    let reason = reason.trim();
    let reason = (!reason.is_empty()).then_some(reason);
    let previous_state = current_state_label(conn, id)?;
    let previous_state_value = previous_state
        .as_deref()
        .and_then(|state| state.parse::<SandboxState>().ok());
    if generic_writer_transition_blocked(previous_state_value, SandboxState::CleanupRequired) {
        tracing::warn!(
            sandbox_id = %id,
            "ignored mark_cleanup_required because sandbox is quarantined"
        );
        return Ok(());
    }
    conn.execute(
        "UPDATE sandboxes
         SET state = 'cleanup_required',
             failure_reason = ?1,
             cleanup_completion = ?2,
             cleanup_keep_filesystem = ?3,
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             state_changed_at = CASE
                 WHEN state != 'cleanup_required' THEN ?4
                 ELSE state_changed_at
             END,
             updated_at = ?4
         WHERE id = ?5
           AND state != 'quarantined'",
        params![reason, completion.as_str(), keep_filesystem, now()?, id],
    )?;
    Ok(())
}

/// Persist a cleanup requirement only while no cleanup intent is already on
/// the row. Returns whether the write landed; `false` means another
/// producer's persisted intent (e.g. a pending user delete) is present and
/// must not be overwritten by a stale lifecycle observer.
pub fn mark_cleanup_required_if_no_intent(
    conn: &Connection,
    id: &str,
    keep_filesystem: bool,
    completion: CleanupCompletion,
    reason: &str,
) -> Result<bool> {
    mark_cleanup_required_if_no_intent_inner(conn, id, keep_filesystem, completion, reason, None)
}

pub fn mark_cleanup_required_if_no_intent_in_state(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
    keep_filesystem: bool,
    completion: CleanupCompletion,
    reason: &str,
) -> Result<bool> {
    mark_cleanup_required_if_no_intent_inner(
        conn,
        id,
        keep_filesystem,
        completion,
        reason,
        Some(expected_state),
    )
}

fn mark_cleanup_required_if_no_intent_inner(
    conn: &Connection,
    id: &str,
    keep_filesystem: bool,
    completion: CleanupCompletion,
    reason: &str,
    expected_state: Option<SandboxState>,
) -> Result<bool> {
    let reason = reason.trim();
    let reason = (!reason.is_empty()).then_some(reason);
    let previous_state = current_state_label(conn, id)?;
    let previous_state_value = previous_state
        .as_deref()
        .and_then(|state| state.parse::<SandboxState>().ok());
    if generic_writer_transition_blocked(previous_state_value, SandboxState::CleanupRequired) {
        tracing::warn!(
            sandbox_id = %id,
            "ignored mark_cleanup_required_if_no_intent because sandbox is quarantined"
        );
        return Ok(false);
    }
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = 'cleanup_required',
             failure_reason = ?1,
             cleanup_completion = ?2,
             cleanup_keep_filesystem = ?3,
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             state_changed_at = CASE
                 WHEN state != 'cleanup_required' THEN ?4
                 ELSE state_changed_at
             END,
             updated_at = ?4
         WHERE id = ?5
           AND state != 'quarantined'
           AND cleanup_completion IS NULL
           AND cleanup_keep_filesystem IS NULL
           AND (?6 IS NULL OR state = ?6)",
        params![
            reason,
            completion.as_str(),
            keep_filesystem,
            now()?,
            id,
            expected_state.map(|state| state.as_str()),
        ],
    )?;
    if changed == 1 {
        emit_sandbox_state_event(
            id,
            SandboxEventType::CleanupRequested,
            previous_state.as_deref(),
            reason.unwrap_or("cleanup_required"),
            "mark_cleanup_required_if_no_intent",
            SandboxState::CleanupRequired,
            None,
        );
    }
    Ok(changed == 1)
}

pub fn set_cleanup_intent(
    conn: &Connection,
    id: &str,
    keep_filesystem: bool,
    completion: CleanupCompletion,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET cleanup_completion = ?1,
             cleanup_keep_filesystem = ?2,
             updated_at = ?3
         WHERE id = ?4
           AND NOT EXISTS (
               SELECT 1 FROM sandbox_cleanup_episodes WHERE sandbox_id = ?4
           )",
        params![completion.as_str(), keep_filesystem, now()?, id],
    )?;
    Ok(changed == 1)
}

pub fn mark_cleanup_failed(
    conn: &Connection,
    id: &str,
    keep_filesystem: bool,
    completion: CleanupCompletion,
    reason: &str,
) -> Result<bool> {
    let reason = reason.trim();
    let reason = (!reason.is_empty()).then_some(reason);
    let previous_state = current_state_label(conn, id)?;
    let previous_state_value = previous_state
        .as_deref()
        .and_then(|state| state.parse::<SandboxState>().ok());
    if generic_writer_transition_blocked(previous_state_value, SandboxState::CleanupFailed) {
        tracing::warn!(
            sandbox_id = %id,
            "ignored mark_cleanup_failed because sandbox is quarantined"
        );
        return Ok(false);
    }
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = 'cleanup_failed',
             failure_reason = ?1,
             cleanup_completion = ?2,
             cleanup_keep_filesystem = ?3,
             state_changed_at = CASE
                 WHEN state != 'cleanup_failed' THEN ?4
                 ELSE state_changed_at
             END,
             updated_at = ?4
         WHERE id = ?5
           AND state != 'quarantined'",
        params![reason, completion.as_str(), keep_filesystem, now()?, id],
    )?;
    if changed != 0 && previous_state_value != Some(SandboxState::CleanupFailed) {
        emit_sandbox_state_event(
            id,
            SandboxEventType::CleanupFailed,
            previous_state.as_deref(),
            "cleanup_failed",
            "mark_cleanup_failed",
            SandboxState::CleanupFailed,
            reason,
        );
    }
    Ok(changed != 0)
}

pub fn clear_cleanup_intent(conn: &Connection, id: &str) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes
         SET cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             updated_at = ?1
         WHERE id = ?2",
        params![now()?, id],
    )?;
    Ok(())
}

pub fn update_uuid8(conn: &Connection, id: &str, uuid8: &SandboxRuntimeLabel) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET uuid8 = ?1, updated_at = ?2 WHERE id = ?3",
        params![uuid8.as_str(), now()?, id],
    )?;
    Ok(())
}

/// Publish route identity and invalidate evidence bound to a replaced execution.
///
/// Unlike [`update_runtime_fields`] this touches no runtime process state and
/// does not advance the teardown suspend lease: it exists for activations that
/// republish the route identity of an already-running VM, where the runtime
/// fields and the execution generation are unchanged.
///
/// Forward-moving callers validate the route epoch before entering this
/// writer. Pool-adoption rollback is the exception: it restores a snapshot of
/// this same row taken before adoption. The pending-receipt predicate still
/// prevents that rollback from crossing a receipt for the restored or a newer
/// execution generation.
///
/// Route-only adoption cannot establish a successor execution. It is limited
/// to lifecycle-stable rows without cleanup ownership; a new runtime must use
/// the activation path that claims its lifecycle state and cleanup episode.
/// A new execution generation or activation attempt invalidates endpoint
/// evidence bound to the prior activation.
pub fn update_space_api_route_identity(
    conn: &Connection,
    sandbox_id: SandboxId,
    identity: &SpaceApiRouteIdentity,
    provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    let receipt_supersession =
        update_space_api_route_identity_in_transaction(&tx, sandbox_id, identity, provenance)?;
    tx.commit()?;
    emit_auto_lifecycle_delete_receipt_superseded_if_present(sandbox_id, receipt_supersession);
    Ok(())
}

pub(crate) fn update_space_api_route_identity_in_transaction(
    conn: &Connection,
    sandbox_id: SandboxId,
    identity: &SpaceApiRouteIdentity,
    provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance,
) -> Result<Option<crate::auto_lifecycle_delete_receipts::db::PendingReceiptSupersession>> {
    let sandbox_id_string = sandbox_id.to_string();
    let previous_generation = runtime_execution_generation(conn, &sandbox_id_string)?;
    let updated = conn.execute(
        "UPDATE sandboxes SET space_api_route_generation = ?1, space_api_route_node_id = ?2,
         space_api_route_execution_generation = ?3, space_api_route_pod_uid = ?4,
         space_api_route_substrate_kind = ?5, space_api_route_activation_attempt_id = ?6,
         space_api_route_identity_provenance = ?7,
         spaced_endpoint_proof_json = CASE
             WHEN space_api_route_execution_generation IS ?3
              AND space_api_route_activation_attempt_id IS ?6
             THEN spaced_endpoint_proof_json
             ELSE NULL
         END,
         updated_at = ?8
         WHERE id = ?9
           AND state IN ('started', 'paused')
           AND cleanup_completion IS NULL
           AND cleanup_keep_filesystem IS NULL
           AND NOT EXISTS (
               SELECT 1
                FROM auto_lifecycle_delete_outbox
               WHERE sandbox_id = ?9
                 AND state = 'pending'
                  AND (?3 IS NULL OR execution_generation >= ?3)
           )",
        params![
            identity
                .route_generation
                .map(|value| SqliteU64(value.get())),
            identity.current_node_id.map(|value| value.to_string()),
            identity.execution_generation.map(|value| value.get()),
            identity.pod_uid.as_ref().map(PodUid::as_str),
            identity.substrate_kind.map(|value| value.to_string()),
            identity
                .activation_attempt_id
                .map(|value| value.to_string()),
            provenance.as_str(),
            now()?,
            sandbox_id_string
        ],
    )?;
    if updated != 1 {
        return Err(
            crate::auto_lifecycle_delete_receipts::db::execution_write_miss_error(
                conn,
                sandbox_id,
                identity.execution_generation,
            ),
        );
    }
    reset_access_floor_for_execution_change(
        conn,
        &sandbox_id_string,
        previous_generation,
        identity.execution_generation,
    )?;
    if let Some(evidence) =
        crate::auto_lifecycle_delete_receipts::RouteEvidence::from_identity(identity, provenance)
    {
        crate::auto_lifecycle_delete_receipts::db::record_retained_route_evidence(
            conn, sandbox_id, evidence,
        )?;
    }
    // Provenance controls whether route evidence may mint a receipt. Once this
    // transaction persists a newer execution generation, an older receipt no
    // longer describes the row, including when the new route is incomplete.
    let receipt_supersession =
        crate::auto_lifecycle_delete_receipts::db::discard_pending_receipt_for_execution_advance(
            conn,
            sandbox_id,
            identity.execution_generation,
        )?;
    Ok(receipt_supersession)
}

pub(crate) fn update_runtime_fields(
    conn: &Connection,
    sandbox_id: SandboxId,
    fields: RuntimeFields<'_>,
) -> Result<SupersededTeardownGeneration> {
    update_runtime_fields_with_activation_owner(conn, sandbox_id, None, fields)
}

pub fn update_runtime_fields_for_activation(
    conn: &Connection,
    sandbox_id: SandboxId,
    cleanup_id: SandboxCleanupEpisodeId,
    fields: RuntimeFields<'_>,
) -> Result<SupersededTeardownGeneration> {
    update_runtime_fields_with_activation_owner(conn, sandbox_id, Some(cleanup_id), fields)
}

fn update_runtime_fields_with_activation_owner(
    conn: &Connection,
    sandbox_id: SandboxId,
    cleanup_id: Option<SandboxCleanupEpisodeId>,
    fields: RuntimeFields<'_>,
) -> Result<SupersededTeardownGeneration> {
    let id = sandbox_id.to_string();
    let RuntimeFields {
        uid,
        slot,
        guest_cid,
        pid,
        spaced_host_pid,
        netgtw_pid,
        vsock_uds_path,
        api_socket_path,
        route_identity_provenance,
        spaced_endpoint_proof,
        space_api_route_identity:
            SpaceApiRouteIdentity {
                route_generation: space_api_route_generation,
                current_node_id: space_api_route_node_id,
                substrate_kind: space_api_route_substrate_kind,
                execution_generation: space_api_route_execution_generation,
                pod_uid: space_api_route_pod_uid,
                activation_attempt_id: space_api_route_activation_attempt_id,
                ..
            },
    } = fields;
    let execution_generation = space_api_route_execution_generation;
    let spaced_endpoint_proof_json = spaced_endpoint_proof
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;

    let tx = immediate_transaction(conn)?;
    let previous_generation = runtime_execution_generation(&tx, &id)?;
    let claim = execution_generation
        .map(|execution_generation| advance_teardown_suspend_lease(&tx, &id, execution_generation))
        .transpose()?
        .unwrap_or(None);
    let updated = tx.execute(
        "UPDATE sandboxes SET uid = ?1, slot = ?2, guest_cid = ?3, pid = ?4,
         spaced_host_pid = ?5, netgtw_pid = ?6, vsock_uds_path = ?7, api_socket_path = ?8,
         space_api_route_generation = ?9, space_api_route_node_id = ?10,
         space_api_route_execution_generation = ?11, space_api_route_pod_uid = ?12,
         space_api_route_substrate_kind = ?13,
         space_api_route_activation_attempt_id = ?14,
         space_api_route_identity_provenance = ?15,
         spaced_endpoint_proof_json = ?16,
         updated_at = ?17
         WHERE id = ?18
           AND NOT EXISTS (
               SELECT 1
                FROM auto_lifecycle_delete_outbox
               WHERE sandbox_id = ?18
                 AND state = 'pending'
                  AND (?11 IS NULL OR execution_generation >= ?11)
           )
           AND (
               (
                   ?19 IS NULL
                   AND state NOT IN ('deleting', 'cleanup_failed')
                   AND cleanup_completion IS NULL
                   AND cleanup_keep_filesystem IS NULL
                   AND NOT EXISTS (
                       SELECT 1 FROM sandbox_cleanup_episodes
                        WHERE sandbox_id = ?18
                   )
               )
               OR EXISTS (
                   SELECT 1
                     FROM sandbox_cleanup_episodes AS episode
                    WHERE episode.sandbox_id = ?18
                      AND episode.cleanup_id = ?19
                      AND episode.phase = 'armed'
                      AND episode.activation_state = sandboxes.state
                      AND episode.completion = sandboxes.cleanup_completion
                      AND episode.keep_filesystem = sandboxes.cleanup_keep_filesystem
               )
           )",
        params![
            uid,
            slot,
            guest_cid,
            pid,
            spaced_host_pid,
            netgtw_pid,
            vsock_uds_path,
            api_socket_path,
            space_api_route_generation.map(|value| SqliteU64(value.get())),
            space_api_route_node_id.map(|value| value.to_string()),
            space_api_route_execution_generation.map(|value| value.get()),
            space_api_route_pod_uid.as_ref().map(PodUid::as_str),
            space_api_route_substrate_kind.map(|value| value.to_string()),
            space_api_route_activation_attempt_id.map(|value| value.to_string()),
            route_identity_provenance.as_str(),
            spaced_endpoint_proof_json,
            now()?,
            id,
            cleanup_id.map(|value| value.to_string())
        ],
    )?;
    if updated != 1 {
        return Err(
            crate::auto_lifecycle_delete_receipts::db::execution_write_miss_error(
                &tx,
                sandbox_id,
                space_api_route_execution_generation,
            ),
        );
    }
    reset_access_floor_for_execution_change(&tx, &id, previous_generation, execution_generation)?;
    if let (Some(execution_generation), Some(activation_attempt_id), Some(substrate)) = (
        space_api_route_execution_generation,
        space_api_route_activation_attempt_id,
        space_api_route_substrate_kind,
    ) {
        crate::auto_lifecycle_delete_receipts::db::record_retained_route_evidence(
            &tx,
            sandbox_id,
            crate::auto_lifecycle_delete_receipts::RouteEvidence {
                execution_generation,
                activation_attempt_id,
                substrate,
                provenance: route_identity_provenance,
            },
        )?;
    }
    let receipt_supersession =
        crate::auto_lifecycle_delete_receipts::db::discard_pending_receipt_for_execution_advance(
            &tx,
            sandbox_id,
            space_api_route_execution_generation,
        )?;
    tx.commit()?;
    emit_auto_lifecycle_delete_receipt_superseded_if_present(sandbox_id, receipt_supersession);
    if let Some(execution_generation) = execution_generation {
        emit_teardown_suspend_lease_claim(&id, execution_generation, claim);
    }
    Ok(claim)
}

fn advance_teardown_suspend_lease(
    conn: &Connection,
    sandbox_id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<SupersededTeardownGeneration> {
    if runtime_execution_generation(conn, sandbox_id)?
        .is_some_and(|current| current > execution_generation)
    {
        return Err(rusqlite::Error::InvalidQuery);
    }
    if latest_teardown_suspend_operation(conn, sandbox_id)?.is_some_and(|latest| {
        latest.execution_generation > execution_generation
            || (latest.execution_generation == execution_generation
                && latest.status == TeardownSuspendOperationStatus::Completed)
    }) {
        return Err(rusqlite::Error::InvalidQuery);
    }

    let mut superseded_generation = None;
    if let Some(active) = active_teardown_suspend_operation(conn, sandbox_id)? {
        if active.execution_generation == execution_generation {
            return Ok(superseded_generation);
        }
        superseded_generation = Some(active.execution_generation);
        pplx_space_common_jobs::db::fail_job_permanent(
            conn,
            &active.job_id,
            TEARDOWN_SUSPEND_SUPERSEDED_REASON,
            None,
        )?;
    }

    Ok(superseded_generation)
}

// Called inside the identity writer's transaction. Same-generation refresh and
// pause/resume preserve the floor; a new execution cannot inherit it.
fn reset_access_floor_for_execution_change(
    conn: &Connection,
    id: &str,
    previous: Option<ExecutionGeneration>,
    next: Option<ExecutionGeneration>,
) -> Result<()> {
    if previous == next {
        return Ok(());
    }
    let sandbox = get_sandbox(conn, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    let mut runtime = sandbox.auto_lifecycle_state;
    if runtime.access_extended_until.take().is_some() {
        crate::auto_lifecycle::recompute_runtime_state(&mut runtime, &sandbox.auto_lifecycle);
        update_auto_lifecycle_state(conn, id, &runtime)?;
    }
    Ok(())
}

pub fn runtime_execution_generation(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<ExecutionGeneration>> {
    let generation = conn
        .query_row(
            "SELECT space_api_route_execution_generation
         FROM sandboxes
         WHERE id = ?1",
            params![sandbox_id],
            |row| row.get::<_, Option<i64>>(0),
        )
        .optional()?
        .flatten();
    generation
        .map(ExecutionGeneration::new)
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(0, Type::Integer, Box::new(error))
        })
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ActivationRouteProvenance {
    pub current_route_generation: Option<RouteGeneration>,
    pub current_execution_generation: Option<ExecutionGeneration>,
    pub current_substrate_kind: Option<NodeManagedBy>,
    pub retained_route_generation: Option<RouteGeneration>,
    pub retained_execution_generation: Option<ExecutionGeneration>,
    pub retained_substrate_kind: Option<NodeManagedBy>,
}

fn activation_route_provenance_from_row(
    row: &rusqlite::Row<'_>,
    offset: usize,
) -> Result<ActivationRouteProvenance> {
    let values = (
        row.get::<_, Option<SqliteU64>>(offset)?
            .map(|value| value.0),
        row.get::<_, Option<i64>>(offset + 1)?,
        row.get::<_, Option<String>>(offset + 2)?,
        row.get::<_, Option<SqliteU64>>(offset + 3)?
            .map(|value| value.0),
        row.get::<_, Option<i64>>(offset + 4)?,
        row.get::<_, Option<String>>(offset + 5)?,
        row.get::<_, Option<String>>(offset + 6)?,
    );
    let parse_route_generation = |value: Option<u64>, column| {
        value
            .map(RouteGeneration::new)
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(column, Type::Integer, Box::new(error))
            })
    };
    let parse_execution_generation = |value: Option<i64>, column| {
        value
            .map(ExecutionGeneration::new)
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(column, Type::Integer, Box::new(error))
            })
    };
    let parse_substrate = |value: Option<String>, column| {
        value
            .map(|value| value.parse::<NodeManagedBy>())
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(error))
            })
    };
    let retained_route_substrate = parse_substrate(values.5, offset + 5)?;
    let retained_legacy_substrate = parse_substrate(values.6, offset + 6)?;
    let retained_substrate_kind = match (retained_route_substrate, retained_legacy_substrate) {
        (Some(NodeManagedBy::Kubernetes), _) | (_, Some(NodeManagedBy::Kubernetes)) => {
            Some(NodeManagedBy::Kubernetes)
        }
        (Some(NodeManagedBy::Space), _) | (_, Some(NodeManagedBy::Space)) => {
            Some(NodeManagedBy::Space)
        }
        (None, None) => None,
    };
    Ok(ActivationRouteProvenance {
        current_route_generation: parse_route_generation(values.0, offset)?,
        current_execution_generation: parse_execution_generation(values.1, offset + 1)?,
        current_substrate_kind: parse_substrate(values.2, offset + 2)?,
        retained_route_generation: parse_route_generation(values.3, offset + 3)?,
        retained_execution_generation: parse_execution_generation(values.4, offset + 4)?,
        retained_substrate_kind,
    })
}

pub fn activation_route_provenance(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<ActivationRouteProvenance> {
    let values = conn.query_row(
        "SELECT space_api_route_generation,
                space_api_route_execution_generation,
                space_api_route_substrate_kind,
                last_observed_route_generation,
                last_observed_execution_generation,
                last_observed_route_generation_substrate_kind,
                last_observed_route_substrate_kind
           FROM sandboxes
          WHERE id = ?1",
        params![sandbox_id],
        |row| activation_route_provenance_from_row(row, 0),
    )?;
    Ok(values)
}

pub fn activation_route_provenance_for_sandboxes(
    conn: &Connection,
    sandbox_ids: &[String],
) -> Result<HashMap<String, Result<ActivationRouteProvenance>>> {
    if sandbox_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = std::iter::repeat_n("?", sandbox_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT id,
                space_api_route_generation,
                space_api_route_execution_generation,
                space_api_route_substrate_kind,
                last_observed_route_generation,
                last_observed_execution_generation,
                last_observed_route_generation_substrate_kind,
                last_observed_route_substrate_kind
           FROM sandboxes
          WHERE id IN ({placeholders})"
    );
    let mut statement = conn.prepare(&query)?;
    let rows = statement.query_map(rusqlite::params_from_iter(sandbox_ids), |row| {
        Ok((
            row.get::<_, String>(0)?,
            activation_route_provenance_from_row(row, 1),
        ))
    })?;
    rows.collect()
}

fn record_execution_delete_receipt(
    conn: &Connection,
    sandbox_id: &str,
    execution_generation: Option<ExecutionGeneration>,
) -> Result<()> {
    let Some(execution_generation) = execution_generation else {
        return Ok(());
    };
    let deleted_at = now_epoch_i64()?;
    conn.execute(
        "DELETE FROM sandbox_execution_delete_receipts WHERE deleted_at < ?1",
        params![deleted_at.saturating_sub(EXECUTION_DELETE_RECEIPT_RETENTION_SECS)],
    )?;
    conn.execute(
        "INSERT INTO sandbox_execution_delete_receipts (
             sandbox_id, execution_generation, deleted_at
         ) VALUES (?1, ?2, ?3)
         ON CONFLICT(sandbox_id) DO UPDATE SET
             execution_generation = MAX(execution_generation, excluded.execution_generation),
             deleted_at = MAX(deleted_at, excluded.deleted_at)",
        params![sandbox_id, execution_generation.get(), deleted_at],
    )?;
    Ok(())
}

pub fn execution_delete_receipt_covers(
    conn: &Connection,
    sandbox_id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<bool> {
    let minimum_deleted_at =
        now_epoch_i64()?.saturating_sub(EXECUTION_DELETE_RECEIPT_RETENTION_SECS);
    conn.query_row(
        "SELECT execution_generation >= ?1
         FROM sandbox_execution_delete_receipts
         WHERE sandbox_id = ?2
           AND deleted_at >= ?3",
        params![execution_generation.get(), sandbox_id, minimum_deleted_at],
        |row| row.get(0),
    )
    .optional()
    .map(|covers| covers.unwrap_or(false))
}

fn active_teardown_suspend_operation(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<TeardownSuspendOperation>> {
    let mut statement = conn.prepare(
        "SELECT job_id, payload, status
         FROM jobs
         WHERE kind = ?1
           AND subject_id = ?2
           AND status IN ('pending', 'running')
         ORDER BY created_at DESC, job_id DESC
         LIMIT 2",
    )?;
    let rows = statement
        .query_map(params![TEARDOWN_SUSPEND_JOB_KIND, sandbox_id], |row| {
            Ok((
                row.get::<_, JobId>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>>>()?;
    if rows.len() > 1 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("multiple active teardown suspend jobs for sandbox {sandbox_id}"),
            )),
        ));
    }
    rows.into_iter()
        .next()
        .map(|(job_id, payload, status)| parse_teardown_suspend_operation(job_id, payload, status))
        .transpose()
}

fn latest_teardown_suspend_operation(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<TeardownSuspendOperation>> {
    conn.query_row(
        "SELECT job_id, payload, status
         FROM jobs
         WHERE kind = ?1
           AND subject_id = ?2
         ORDER BY created_at DESC, job_id DESC
         LIMIT 1",
        params![TEARDOWN_SUSPEND_JOB_KIND, sandbox_id],
        |row| parse_teardown_suspend_operation(row.get(0)?, row.get(1)?, row.get(2)?),
    )
    .optional()
}

fn parse_teardown_suspend_operation(
    job_id: JobId,
    payload: String,
    status: String,
) -> Result<TeardownSuspendOperation> {
    let payload: TeardownSuspendJobPayload = serde_json::from_str(&payload).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(error))
    })?;
    let status = TeardownSuspendOperationStatus::from_str(&status).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(2, Type::Text, Box::new(error))
    })?;
    Ok(TeardownSuspendOperation {
        job_id,
        execution_generation: payload.execution_generation,
        published_artifact_id: payload.published_artifact_id,
        status,
    })
}

pub(crate) struct TeardownReleaseEvidence {
    pub runtime_generation: Option<ExecutionGeneration>,
    pub suspend: Option<TeardownSuspendStatus>,
    pub deleted: bool,
    pub activation_rolled_back: bool,
}

/// Release evidence must describe one SQLite snapshot: a historical rollback
/// cannot be combined with ownership from before a replacement activation.
pub(crate) fn teardown_release_evidence(
    conn: &Connection,
    sandbox_id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<TeardownReleaseEvidence> {
    let tx = conn.unchecked_transaction()?;
    let runtime_generation = runtime_execution_generation(&tx, sandbox_id)?;
    let suspend = teardown_suspend_status(&tx, sandbox_id)?;
    let deleted = execution_delete_receipt_covers(&tx, sandbox_id, execution_generation)?;
    let activation_rolled_back = match &suspend {
        Some(status)
            if runtime_generation.is_none()
                && matches!(status.state, SandboxState::Created | SandboxState::Stopped)
                && status.operation.as_ref().is_none_or(|operation| {
                    operation.status == TeardownSuspendOperationStatus::Completed
                }) =>
        {
            completed_activation_rollback_covers(
                &tx,
                sandbox_id,
                status.state,
                execution_generation,
            )?
        }
        _ => false,
    };
    tx.commit()?;
    Ok(TeardownReleaseEvidence {
        runtime_generation,
        suspend,
        deleted,
        activation_rolled_back,
    })
}

fn completed_activation_rollback_covers(
    conn: &Connection,
    sandbox_id: &str,
    state: SandboxState,
    execution_generation: ExecutionGeneration,
) -> Result<bool> {
    let receipt = conn.query_row(
        sql!("SELECT transition_receipt_id, transition_receipt_kind,
                     transition_receipt_target_state, transition_receipt_execution_generation,
                     transition_receipt_reason_code, transition_receipt_activation_attempt_id
                FROM sandboxes s
               WHERE s.id = ?1
                 AND s.state IN ('created', 'stopped')
                 AND s.space_api_route_execution_generation IS NULL
                 AND s.uid IS NULL AND s.slot IS NULL AND s.guest_cid IS NULL
                 AND s.pid IS NULL AND s.spaced_host_pid IS NULL AND s.netgtw_pid IS NULL
                 AND s.vsock_uds_path IS NULL AND s.api_socket_path IS NULL
                 AND s.space_api_route_generation IS NULL
                 AND s.space_api_route_node_id IS NULL
                 AND s.space_api_route_pod_uid IS NULL
                 AND s.space_api_route_substrate_kind IS NULL
                 AND s.space_api_route_activation_attempt_id IS NULL
                 AND s.cleanup_completion IS NULL AND s.cleanup_keep_filesystem IS NULL
                 AND NOT EXISTS (SELECT 1 FROM sandbox_cleanup_episodes e WHERE e.sandbox_id = s.id)"),
        [sandbox_id],
        |row| Ok(RawSandboxStatePublicationMetadata {
            quarantine_reason_code: None,
            transition_receipt_id: row.get(0)?,
            transition_receipt_kind: row.get(1)?,
            transition_receipt_target_state: row.get(2)?,
            transition_receipt_execution_generation: row.get(3)?,
            transition_receipt_reason_code: row.get(4)?,
            transition_receipt_activation_attempt_id: row.get(5)?,
        }),
    ).optional()?;
    // Missing or malformed receipts supply no release authority. In particular,
    // absence of a runtime generation is also normal after a failed suspend.
    Ok(receipt.is_some_and(|receipt| {
        matches!(
            receipt.decode(state),
            Ok(SandboxStatePublicationMetadata::TransitionReceipt(receipt))
                if receipt.kind() == SandboxTransitionReceiptKind::ActivationRollback
                    && receipt.execution_generation() >= execution_generation
        )
    }))
}

pub fn teardown_suspend_status(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<TeardownSuspendStatus>> {
    let status = conn
        .query_row(
            "SELECT s.state,
                s.current_local_artifact_id,
                s.current_local_artifact_kind,
                s.current_local_artifact_origin,
                a.local_path,
                COALESCE(a.backup_completed_at IS NOT NULL, FALSE)
         FROM sandboxes AS s
         LEFT JOIN local_artifacts AS a
           ON a.local_artifact_id = s.current_local_artifact_id
          AND a.sandbox_id = s.id
         WHERE s.id = ?1",
            params![sandbox_id],
            |row| {
                let state_text = row.get::<_, String>(0)?;
                let state = state_text.parse::<SandboxState>().map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        Type::Text,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("unknown sandbox state {state_text:?}"),
                        )),
                    )
                })?;
                let local_artifact_id = row
                    .get::<_, Option<String>>(1)?
                    .map(|value| {
                        LocalArtifactId::from_str(&value).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                Type::Text,
                                Box::new(error),
                            )
                        })
                    })
                    .transpose()?;
                let artifact_kind = row
                    .get::<_, Option<String>>(2)?
                    .map(|value| {
                        ArtifactKind::from_str(&value).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                Type::Text,
                                Box::new(error),
                            )
                        })
                    })
                    .transpose()?;
                let artifact_origin = row
                    .get::<_, Option<String>>(3)?
                    .map(|value| {
                        ArtifactOrigin::from_str(&value).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                Type::Text,
                                Box::new(error),
                            )
                        })
                    })
                    .transpose()?;
                let artifact_path = row.get::<_, Option<String>>(4)?;
                let backup_completed = row.get::<_, bool>(5)?;

                let artifact = match (
                    local_artifact_id,
                    artifact_kind,
                    artifact_origin,
                    artifact_path,
                ) {
                    (None, None, None, None) => None,
                    (
                        Some(local_artifact_id),
                        Some(ArtifactKind::Full),
                        Some(ArtifactOrigin::Suspend),
                        Some(local_path),
                    ) => Some(LocalArtifactRow {
                        local_artifact_id,
                        sandbox_id: sandbox_id.to_string(),
                        artifact_kind: ArtifactKind::Full,
                        artifact_origin: ArtifactOrigin::Suspend,
                        snapshot_name: None,
                        local_path,
                    }),
                    _ => {
                        return Err(rusqlite::Error::FromSqlConversionFailure(
                            1,
                            Type::Text,
                            Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "sandbox suspend artifact pin is incomplete or invalid",
                            )),
                        ));
                    }
                };

                Ok((state, artifact, backup_completed))
            },
        )
        .optional()?;
    let Some((state, artifact, backup_completed)) = status else {
        return Ok(None);
    };
    Ok(Some(TeardownSuspendStatus {
        state,
        operation: latest_teardown_suspend_operation(conn, sandbox_id)?,
        artifact,
        backup_completed,
    }))
}

pub fn complete_teardown_suspend(
    conn: &Connection,
    sandbox_id: &str,
    execution_generation: ExecutionGeneration,
    published_artifact_id: Option<LocalArtifactId>,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let Some(operation) = active_teardown_suspend_operation(&tx, sandbox_id)? else {
        return Ok(false);
    };
    if operation.execution_generation != execution_generation {
        return Ok(false);
    }
    if let Some(published_artifact_id) = published_artifact_id {
        if !teardown_suspend_artifact_is_current_and_backed(&tx, sandbox_id, published_artifact_id)?
        {
            return Ok(false);
        }
        bind_teardown_suspend_artifact(
            &tx,
            &operation.job_id,
            execution_generation,
            published_artifact_id,
        )?;
    }
    mark_teardown_suspend_operation_running(&tx, sandbox_id, execution_generation)?;
    if !pplx_space_common_jobs::db::complete_job(&tx, &operation.job_id, 0)? {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    tx.commit()?;
    Ok(true)
}

pub fn bind_completed_teardown_suspend_artifact(
    conn: &Connection,
    sandbox_id: &str,
    execution_generation: ExecutionGeneration,
    published_artifact_id: LocalArtifactId,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let Some(operation) = latest_teardown_suspend_operation(&tx, sandbox_id)? else {
        return Ok(false);
    };
    if operation.execution_generation != execution_generation
        || operation.status != TeardownSuspendOperationStatus::Completed
        || operation.published_artifact_id.is_some()
        || !teardown_suspend_artifact_is_current_and_backed(&tx, sandbox_id, published_artifact_id)?
    {
        return Ok(false);
    }
    bind_teardown_suspend_artifact(
        &tx,
        &operation.job_id,
        execution_generation,
        published_artifact_id,
    )?;
    tx.commit()?;
    Ok(true)
}

fn teardown_suspend_artifact_is_current_and_backed(
    conn: &Connection,
    sandbox_id: &str,
    published_artifact_id: LocalArtifactId,
) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS (
             SELECT 1
               FROM sandboxes AS s
               JOIN local_artifacts AS a
                 ON a.local_artifact_id = s.current_local_artifact_id
                AND a.sandbox_id = s.id
              WHERE s.id = ?1
                AND s.state = 'suspended'
                AND s.current_local_artifact_id = ?2
                AND s.current_local_artifact_kind = 'full'
                AND s.current_local_artifact_origin = 'suspend'
                AND a.backup_completed_at IS NOT NULL
         )",
        params![sandbox_id, published_artifact_id.to_string()],
        |row| row.get(0),
    )
}

fn bind_teardown_suspend_artifact(
    conn: &Connection,
    job_id: &JobId,
    execution_generation: ExecutionGeneration,
    published_artifact_id: LocalArtifactId,
) -> Result<()> {
    let payload = serde_json::to_string(&TeardownSuspendJobPayload {
        execution_generation,
        published_artifact_id: Some(published_artifact_id),
    })
    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    if conn.execute(
        "UPDATE jobs SET payload = ?1 WHERE job_id = ?2",
        params![payload, job_id],
    )? != 1
    {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    Ok(())
}

pub fn clear_runtime_fields(conn: &Connection, id: &str) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET uid = NULL, slot = NULL, guest_cid = NULL, pid = NULL,
         spaced_host_pid = NULL, netgtw_pid = NULL, vsock_uds_path = NULL, api_socket_path = NULL,
         space_api_route_generation = NULL, space_api_route_node_id = NULL,
         space_api_route_execution_generation = NULL, space_api_route_pod_uid = NULL,
         space_api_route_substrate_kind = NULL, space_api_route_activation_attempt_id = NULL,
         space_api_route_identity_provenance = NULL,
         spaced_endpoint_proof_json = NULL,
         guest_uptime_secs = NULL, updated_at = ?1 WHERE id = ?2",
        params![now()?, id],
    )?;
    Ok(())
}

/// Identity and authorization fields needed to target a cleanup. Optional
/// launch and snapshot metadata must not make stop/delete unavailable.
pub(crate) struct CleanupTarget {
    pub uuid8: SandboxRuntimeLabel,
    pub template: String,
    pub user: SandboxOwner,
}

pub(crate) fn cleanup_target(conn: &Connection, id: &str) -> Result<Option<CleanupTarget>> {
    conn.query_row(
        sql!("SELECT uuid8, template, user FROM sandboxes WHERE id = ?1"),
        [id],
        |row| {
            Ok(CleanupTarget {
                uuid8: SandboxRuntimeLabel::parse(row.get::<_, String>(0)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
                })?,
                template: row.get(1)?,
                user: SandboxOwner::parse(row.get::<_, String>(2)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(2, Type::Text, Box::new(error))
                })?,
            })
        },
    )
    .optional()
}

pub(crate) fn cleanup_runtime_label(
    conn: &Connection,
    id: &str,
) -> Result<Option<SandboxRuntimeLabel>> {
    conn.query_row(
        sql!("SELECT uuid8 FROM sandboxes WHERE id = ?1"),
        [id],
        |row| {
            SandboxRuntimeLabel::parse(row.get::<_, String>(0)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })
        },
    )
    .optional()
}

pub(crate) fn cleanup_intent_current(
    conn: &Connection,
    id: &str,
    completion: CleanupCompletion,
) -> Result<bool> {
    let intent = conn
        .query_row(
            sql!("SELECT state, cleanup_completion FROM sandboxes WHERE id = ?1"),
            [id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?;
    let Some((state, intent)) = intent else {
        return Ok(false);
    };
    let Ok(state) = state.parse::<SandboxState>() else {
        return Ok(false);
    };
    let intent = intent
        .map(|value| {
            value.parse::<CleanupCompletion>().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(error))
            })
        })
        .transpose()?;
    Ok(completion.owns_intent(state, intent))
}

pub fn sandbox_exists(conn: &Connection, id: &str) -> Result<bool> {
    conn.query_row(
        sql!("SELECT EXISTS(SELECT 1 FROM sandboxes WHERE id = ?1)"),
        [id],
        |row| row.get(0),
    )
}

const GET_SANDBOX_SQL: &str =
        "SELECT id, uuid8, state, template, template_hash, user, runtime, uid, slot, guest_cid, pid,
         spaced_host_pid, netgtw_pid, vsock_uds_path, api_socket_path, created_at, updated_at, snapshots_enabled, rootfs_size_mb, pool_template, guest_uptime_secs, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, failure_reason, cleanup_completion, cleanup_keep_filesystem, network_policy_override_json, network_tx_bytes, network_rx_bytes, network_policy_generation, applied_network_policy_generation, applied_network_policy_source, network_policy_authority, auto_lifecycle_authority, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin, execution_metadata_json, volume_encryption, space_api_route_generation, space_api_route_node_id, space_api_route_execution_generation, space_api_route_pod_uid, restore_origin, restore_phase, restore_storage_uri, space_api_route_substrate_kind, space_api_route_activation_attempt_id, spaced_endpoint_proof_json, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id
         FROM sandboxes WHERE id = ?1";

pub fn get_sandbox(conn: &Connection, id: &str) -> Result<Option<SandboxRow>> {
    // Every guest file operation and process start looks its sandbox up
    // several times under the one database connection; parsing this wide
    // SELECT each time dominated that work.
    let mut stmt = conn.prepare_cached(GET_SANDBOX_SQL)?;
    let mut rows = stmt.query_map(params![id], row_to_sandbox)?;
    rows.next().transpose()
}

/// Select at most two identities: two matches prove ambiguity without decoding
/// unrelated or ambiguous sandbox metadata. Callers decode only a unique match.
pub fn find_by_prefix(conn: &Connection, prefix: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT id FROM sandboxes ORDER BY id")?;
    let mut rows = stmt.query([])?;
    let mut matches = Vec::new();
    while let Some(row) = rows.next()? {
        let id = row.get::<_, String>(0)?;
        if pplx_space_common_core::ids::sandbox_id_matches(&id, prefix) {
            matches.push(id);
            if matches.len() == 2 {
                break;
            }
        }
    }
    Ok(matches)
}

const LIST_SANDBOXES_SQL: &str =
    "SELECT id, uuid8, state, template, template_hash, user, runtime, uid, slot, guest_cid, pid,
         spaced_host_pid, netgtw_pid, vsock_uds_path, api_socket_path, created_at, updated_at, snapshots_enabled, rootfs_size_mb, pool_template, guest_uptime_secs, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, failure_reason, cleanup_completion, cleanup_keep_filesystem, network_policy_override_json, network_tx_bytes, network_rx_bytes, network_policy_generation, applied_network_policy_generation, applied_network_policy_source, network_policy_authority, auto_lifecycle_authority, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin, execution_metadata_json, volume_encryption, space_api_route_generation, space_api_route_node_id, space_api_route_execution_generation, space_api_route_pod_uid, restore_origin, restore_phase, restore_storage_uri, space_api_route_substrate_kind, space_api_route_activation_attempt_id, spaced_endpoint_proof_json, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id
         FROM sandboxes ORDER BY created_at";

pub fn list_sandboxes(conn: &Connection) -> Result<Vec<SandboxRow>> {
    let mut stmt = conn.prepare(LIST_SANDBOXES_SQL)?;
    let rows = stmt.query_map([], row_to_sandbox)?;
    rows.collect()
}

pub fn list_sandboxes_for_recovery(conn: &Connection) -> Result<Vec<SandboxRow>> {
    let mut stmt = conn.prepare(LIST_SANDBOXES_SQL)?;
    let mut rows = stmt.query([])?;
    let mut valid = Vec::new();
    while let Some(row) = rows.next()? {
        match row_to_sandbox(row) {
            Ok(row) => valid.push(row),
            Err(error) if is_row_decode_error(&error) => tracing::warn!(
                sandbox_id = ?row.get_ref(0)?,
                error = %describe_error(&error),
                "skipping undecodable sandbox row during local recovery"
            ),
            Err(error) => return Err(error),
        }
    }
    Ok(valid)
}

/// Only use at read/decode boundaries. Statement, schema and write failures
/// remain fatal; a retained malformed row must not disable independent work.
fn is_row_decode_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::FromSqlConversionFailure(..)
            | rusqlite::Error::InvalidColumnType(..)
            | rusqlite::Error::IntegralValueOutOfRange(..)
            | rusqlite::Error::Utf8Error(..)
    )
}

/// A finite sweep: concurrent inserts are visited on the next sweep instead of
/// keeping this one open forever and starving previously deferred candidates.
/// New candidates wait for a later sweep to reach their rowid; admission pressure
/// and recovery failures can delay progress further. No recovery deadline is promised.
#[derive(Debug)]
pub(crate) struct PeriodicLifecycleRecoveryScan {
    next: i64,
    through: Option<i64>,
}

impl Default for PeriodicLifecycleRecoveryScan {
    fn default() -> Self {
        Self {
            next: i64::MIN,
            through: None,
        }
    }
}

/// Bounded discovery, not a complete inventory or authority to mutate a row.
/// Examine at most 128 raw rowids and return at most eight successfully decoded
/// candidates per tick. Failed decodes do not fill candidate slots, so eligible
/// malformed rows can cause up to 128 individual full-row decode attempts.
/// Advance past malformed/ineligible rows as well as candidates, so a retained
/// or busy first page cannot starve the rest. Rowid is only a scan position;
/// each consumer must revalidate the execution witness under its own fences.
pub(crate) fn periodic_lifecycle_recovery_batch(
    conn: &Connection,
    scan: &mut PeriodicLifecycleRecoveryScan,
) -> Result<Vec<SandboxRow>> {
    if scan.through.is_none() {
        scan.through = conn.query_row(sql!("SELECT MAX(rowid) FROM sandboxes"), [], |row| {
            row.get(0)
        })?;
    }
    let Some(through) = scan.through else {
        return Ok(Vec::new());
    };
    let mut statement = conn.prepare(sql!(
        "SELECT rowid, id, state, current_local_artifact_id FROM sandboxes
         WHERE rowid >= ?1 AND rowid <= ?2 ORDER BY rowid LIMIT 128"
    ))?;
    let mut rows = statement.query([scan.next, through])?;
    let mut candidates = Vec::new();
    for _ in 0..128 {
        let Some(row) = rows.next()? else {
            *scan = PeriodicLifecycleRecoveryScan::default();
            break;
        };
        let rowid: i64 = row.get(0)?;
        if rowid == through {
            *scan = PeriodicLifecycleRecoveryScan::default();
        } else {
            // The upper witness is greater, so this cannot overflow.
            scan.next = rowid + 1;
        }
        let (id, state) = match (row.get::<_, String>(1), row.get::<_, String>(2)) {
            (Ok(id), Ok(state)) => (id, state),
            (Err(error), _) | (_, Err(error)) => {
                tracing::warn!(
                    rowid,
                    error = %describe_error(&error),
                    "skipping undecodable sandbox key/state during periodic recovery"
                );
                continue;
            }
        };
        let eligible = match state.parse::<SandboxState>() {
            Ok(SandboxState::Paused | SandboxState::Resuming | SandboxState::Checkpointing) => true,
            Ok(SandboxState::Suspending) => {
                matches!(row.get_ref(3)?, rusqlite::types::ValueRef::Null)
            }
            _ => false,
        };
        if !eligible {
            continue;
        }
        match get_sandbox(conn, &id) {
            Ok(Some(row)) => candidates.push(row),
            Ok(None) => {}
            Err(error) => tracing::warn!(
                sandbox_id = %id,
                error = %describe_error(&error),
                "skipping undecodable sandbox row during periodic recovery"
            ),
        }
        if candidates.len() == 8 {
            break;
        }
    }
    Ok(candidates)
}

/// Sandboxes with durable cleanup evidence that a live reconciliation pass may
/// safely re-drive after acquiring the per-sandbox cleanup fence.
#[cfg(test)]
pub fn list_requested_cleanup_recovery_candidate_ids(conn: &Connection) -> Result<Vec<SandboxId>> {
    let mut statement = conn.prepare(
        "SELECT sandbox.id
           FROM sandboxes sandbox
           LEFT JOIN sandbox_cleanup_episodes episode
             ON episode.sandbox_id = sandbox.id
          WHERE (episode.phase = 'requested' AND NOT (episode.completion = 'delete_pool_clone' AND sandbox.state = 'quarantined'))
             OR (
                  episode.sandbox_id IS NULL
                  AND (
                       (
                         sandbox.cleanup_completion IS NOT NULL
                         AND sandbox.cleanup_keep_filesystem IS NOT NULL
                       )
                       OR (
                         sandbox.cleanup_completion IS NULL
                         AND sandbox.cleanup_keep_filesystem IS NULL
                         AND (
                              sandbox.state IN ('stopping', 'deleting', 'cleanup_required')
                              OR (
                                   sandbox.state = 'cleanup_failed'
                                   AND (
                                        sandbox.uid IS NOT NULL
                                        OR sandbox.slot IS NOT NULL
                                        OR sandbox.guest_cid IS NOT NULL
                                        OR sandbox.pid IS NOT NULL
                                        OR sandbox.spaced_host_pid IS NOT NULL
                                        OR sandbox.netgtw_pid IS NOT NULL
                                        OR sandbox.vsock_uds_path IS NOT NULL
                                        OR sandbox.api_socket_path IS NOT NULL
                                   )
                              )
                         )
                       )
                  )
             )
          ORDER BY sandbox.created_at",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let mut sandbox_ids = Vec::new();
    for sandbox_id in rows {
        let sandbox_id = sandbox_id?;
        match SandboxId::from_str(&sandbox_id) {
            Ok(sandbox_id) => sandbox_ids.push(sandbox_id),
            Err(error) => tracing::warn!(
                %sandbox_id,
                %error,
                "skipping invalid sandbox id during requested cleanup recovery"
            ),
        }
    }
    Ok(sandbox_ids)
}

/// Discover unscheduled obligations without decoding the entire local inventory.
/// Limit the inventory range first, so a sparse backlog cannot turn one page
/// into an inventory-sized scan. The cursor advances even when none qualify.
pub(crate) struct CleanupRecoveryPage {
    pub ids: Vec<String>,
    pub next_cursor: Option<i64>,
}

pub(crate) fn cleanup_recovery_page(
    conn: &Connection,
    after: Option<i64>,
    limit: usize,
) -> Result<CleanupRecoveryPage> {
    let sql_limit = i64::try_from(limit)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    // The rowid is a scan position, never lifecycle authority.
    let Some(first) = after.map_or(Some(i64::MIN), |rowid| rowid.checked_add(1)) else {
        return Ok(CleanupRecoveryPage {
            ids: Vec::new(),
            next_cursor: None,
        });
    };
    let tx = conn.unchecked_transaction()?;
    let mut inventory = tx.prepare(sql!(
        "SELECT rowid FROM sandboxes WHERE rowid >= ?1 ORDER BY rowid LIMIT ?2"
    ))?;
    let range: Vec<i64> = inventory
        .query_map(params![first, sql_limit], |row| row.get(0))?
        .collect::<Result<_>>()?;
    let Some(&last) = range.last() else {
        return Ok(CleanupRecoveryPage {
            ids: Vec::new(),
            next_cursor: None,
        });
    };
    let mut statement = tx.prepare(
        sql!("SELECT sandbox.id
           FROM sandboxes sandbox
           LEFT JOIN sandbox_cleanup_episodes episode
             ON episode.sandbox_id = sandbox.id
          WHERE sandbox.rowid >= ?1 AND sandbox.rowid <= ?2 AND NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.kind = 'sandbox_cleanup' AND jobs.subject_id = sandbox.id AND jobs.status IN ('pending', 'running')) AND ((episode.phase = 'requested' AND NOT (episode.completion = 'delete_pool_clone' AND sandbox.state = 'quarantined'))
             OR (
                  episode.sandbox_id IS NULL
                  AND (
                       (
                         sandbox.cleanup_completion IS NOT NULL
                         AND sandbox.cleanup_keep_filesystem IS NOT NULL
                       )
                       OR (
                         sandbox.cleanup_completion IS NULL
                         AND sandbox.cleanup_keep_filesystem IS NULL
                         AND (
                              sandbox.state IN ('stopping', 'deleting', 'cleanup_required')
                              OR (
                                   sandbox.state = 'cleanup_failed'
                                   AND (
                                        sandbox.uid IS NOT NULL
                                        OR sandbox.slot IS NOT NULL
                                        OR sandbox.guest_cid IS NOT NULL
                                        OR sandbox.pid IS NOT NULL
                                        OR sandbox.spaced_host_pid IS NOT NULL
                                        OR sandbox.netgtw_pid IS NOT NULL
                                        OR sandbox.vsock_uds_path IS NOT NULL
                                        OR sandbox.api_socket_path IS NOT NULL
                                   )
                              )
                         )
                       )
                  )
             )
          ) ORDER BY sandbox.rowid"),
    )?;
    let ids = statement
        .query_map(params![first, last], |row| row.get::<_, String>(0))?
        .collect::<Result<_>>()?;
    let next_cursor = (range.len() == limit).then_some(last);
    drop(statement);
    drop(inventory);
    tx.commit()?;
    Ok(CleanupRecoveryPage { ids, next_cursor })
}

pub fn list_sandbox_id_states(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut statement = conn.prepare("SELECT id, state FROM sandboxes ORDER BY created_at")?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    rows.collect()
}

/// Host-control net-gateway auth lookup: rows whose registered
/// `(netgtw_pid, uid)` matches the caller's peer credentials.
///
/// Restricted to states where a live gateway can legitimately exist
/// (`SandboxState::can_authorize_net_gateway`): launch-failure and other
/// terminal rows retain the `(uid, netgtw_pid)` of dead processes, and since
/// uids are slot-derived and pids recycle, an unfiltered match would let a
/// stale row shadow — or be served in place of — a later sandbox's gateway.
/// The state list below must stay in sync with that predicate; the
/// per-state lookup test cross-checks them.
pub fn list_by_netgtw_pid_uid(
    conn: &Connection,
    netgtw_pid: u32,
    uid: u32,
) -> Result<Vec<SandboxRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, uuid8, state, template, template_hash, user, runtime, uid, slot, guest_cid, pid,
         spaced_host_pid, netgtw_pid, vsock_uds_path, api_socket_path, created_at, updated_at, snapshots_enabled, rootfs_size_mb, pool_template, guest_uptime_secs, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, failure_reason, cleanup_completion, cleanup_keep_filesystem, network_policy_override_json, network_tx_bytes, network_rx_bytes, network_policy_generation, applied_network_policy_generation, applied_network_policy_source, network_policy_authority, auto_lifecycle_authority, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin, execution_metadata_json, volume_encryption, space_api_route_generation, space_api_route_node_id, space_api_route_execution_generation, space_api_route_pod_uid, restore_origin, restore_phase, restore_storage_uri, space_api_route_substrate_kind, space_api_route_activation_attempt_id, spaced_endpoint_proof_json, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id
         FROM sandboxes
         WHERE netgtw_pid = ?1 AND uid = ?2
           AND state IN ('starting', 'started', 'stopping', 'pausing', 'paused',
                         'resuming', 'checkpointing', 'suspending', 'restoring')
         LIMIT 2",
    )?;
    let rows = stmt.query_map(params![netgtw_pid, uid], row_to_sandbox)?;
    rows.collect()
}

/// Host-control spaced-host auth lookup: rows whose registered
/// `(spaced_host_pid, uid)` matches the caller's peer credentials.
pub fn list_by_spaced_host_pid_uid(
    conn: &Connection,
    spaced_host_pid: u32,
    uid: u32,
) -> Result<Vec<SandboxRow>> {
    let mut stmt = conn.prepare(
        "SELECT id
         FROM sandboxes
         WHERE spaced_host_pid = ?1 AND uid = ?2
         LIMIT 2",
    )?;
    let sandbox_ids = stmt
        .query_map(params![spaced_host_pid, uid], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>>>()?;
    drop(stmt);

    sandbox_ids
        .into_iter()
        .map(|sandbox_id| {
            get_sandbox(conn, &sandbox_id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)
        })
        .collect()
}

pub fn list_by_state(conn: &Connection, state: SandboxState) -> Result<Vec<SandboxRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, uuid8, state, template, template_hash, user, runtime, uid, slot, guest_cid, pid,
         spaced_host_pid, netgtw_pid, vsock_uds_path, api_socket_path, created_at, updated_at, snapshots_enabled, rootfs_size_mb, pool_template, guest_uptime_secs, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, failure_reason, cleanup_completion, cleanup_keep_filesystem, network_policy_override_json, network_tx_bytes, network_rx_bytes, network_policy_generation, applied_network_policy_generation, applied_network_policy_source, network_policy_authority, auto_lifecycle_authority, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin, execution_metadata_json, volume_encryption, space_api_route_generation, space_api_route_node_id, space_api_route_execution_generation, space_api_route_pod_uid, restore_origin, restore_phase, restore_storage_uri, space_api_route_substrate_kind, space_api_route_activation_attempt_id, spaced_endpoint_proof_json, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id
         FROM sandboxes WHERE state = ?1",
    )?;
    let rows = stmt.query_map(params![state.as_str()], row_to_sandbox)?;
    rows.collect()
}

/// Scan position only, never lifecycle authority. The rowid lane makes progress
/// past unchanged failures and rows without due work; the deadline lane reserves prompt
/// admission for newly due work. Neither lane counts the full backlog.
#[derive(Clone, Copy)]
pub(crate) struct AutoLifecycleScan {
    after: i64,
    fresh_turn: bool,
}

impl Default for AutoLifecycleScan {
    fn default() -> Self {
        Self {
            after: i64::MIN,
            fresh_turn: true,
        }
    }
}

// Preserve the selected database key: parsing a UUID validates its identity,
// but formatting it again need not reproduce the stored key.
pub(crate) struct DueAutoLifecycleSandbox {
    pub storage_id: String,
    pub sandbox_id: SandboxId,
}

/// The one state in which no idle action can ever act: a `deleting` row is
/// already being torn down. Every other state can take at least one action
/// once it settles — `Delete` is reachable from every settled state and
/// from every transient one except `creating` (the state machine
/// force-deletes from `restoring`, `suspending`, … but a row still being
/// created has nothing to delete yet) — so eligibility is decided per
/// action in the transition, once the due stage is known — see
/// [`auto_lifecycle_action_can_act`].
///
/// This literal is the deadline lane's filter. The rowid lane cannot use
/// it (that lane must return every row so its cursor keeps advancing) and
/// applies the same rule in Rust inside `eligible`
/// (`state == SandboxState::Deleting`); the two lane tests in
/// `cleanup_discovery_tests` hold the two encodings to the same answer.
const AUTO_LIFECYCLE_NEVER_ACTIONABLE_STATES_SQL: &str = "('deleting')";

/// Push a row's scan deadline out without touching anything else, so a row
/// whose due stage cannot act right now (mid-transition; see
/// `auto_lifecycle_action_can_act`) stops being re-picked every tick, while
/// `updated_at` — the quiescence clock other loops read — and the runtime
/// state JSON (which still carries the true deadline) stay as they were. Any
/// later state write rewrites the column from the JSON, so the deferral is
/// self-healing and never outlives the next real transition.
pub(crate) fn defer_auto_lifecycle_deadline(
    conn: &Connection,
    id: &str,
    until_secs: u64,
) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET next_auto_event_at = ?1 WHERE id = ?2",
        params![SqliteU64(until_secs), id],
    )?;
    Ok(())
}

/// Whether `action` can act on a row in `state`, or has already been
/// satisfied by it. `Pause`/`Suspend`/`Stop` act on settled states and
/// treat a more dormant settled state as satisfied (a `Suspend` due on an
/// already-`suspended` row advances the policy to its later stages); on a
/// mid-transition row — `starting`, `stopping`, `pausing`, `resuming`,
/// `checkpointing`, `suspending`, `restoring` (which includes a failed
/// cross-node restore's retained row) — they can neither act nor claim
/// satisfaction, so the transition must record nothing (recording
/// the stage as fired bumped `updated_at` and reset the cluster-deleted
/// eviction's quiescence clock on exactly that retained row). `Delete`
/// follows the state machine's force-delete contract and acts wherever
/// `Deleting` is reachable. `None` is a no-op that may always be recorded.
pub(crate) fn auto_lifecycle_action_can_act(
    action: pplx_space_common_core::types::AutoLifecycleIdleAction,
    state: SandboxState,
) -> bool {
    use pplx_space_common_core::types::AutoLifecycleIdleAction as Action;
    match action {
        Action::None => true,
        Action::Delete => state.can_transition_to(SandboxState::Deleting),
        Action::Pause | Action::Suspend | Action::Stop => auto_lifecycle_state_is_settled(state),
    }
}

/// A row another writer is not in the middle of moving. The auto-lifecycle
/// transition may write runtime state only on a settled row: on a
/// mid-transition one — `restoring` included, which is also the shape a
/// failed cross-node restore retains — every write bumps `updated_at`, the
/// quiescence clock the cluster-deleted eviction of that row waits on,
/// and the in-flight transition owns the row anyway.
pub(crate) fn auto_lifecycle_state_is_settled(state: SandboxState) -> bool {
    // The state machine owns the transient set; a state added there is
    // non-settled here without a second list to update.
    !state.is_transient()
}

pub(crate) fn due_auto_lifecycle_batch(
    conn: &Connection,
    now_secs: u64,
    active: &[SandboxId],
    available: usize,
    cursor: &mut AutoLifecycleScan,
) -> Result<Vec<DueAutoLifecycleSandbox>> {
    if available == 0 {
        return Ok(Vec::new());
    }
    let now = now_secs.min(i64::MAX as u64) as i64;
    let mut selected = Vec::new();
    let fresh_slots = if available == 1 {
        usize::from(cursor.fresh_turn)
    } else {
        available / 2
    };
    let eligible = |row: &rusqlite::Row<'_>,
                    selected: &[DueAutoLifecycleSandbox]|
     -> Result<Option<DueAutoLifecycleSandbox>> {
        let (id, deadline) = match (row.get::<_, String>(1), row.get::<_, Option<i64>>(2)) {
            (Ok(id), Ok(deadline)) => (id, deadline),
            (Err(error), _) | (_, Err(error)) if is_row_decode_error(&error) => {
                tracing::warn!(rowid = row.get::<_, i64>(0)?, error = %describe_error(&error), "skipping malformed auto lifecycle discovery row");
                return Ok(None);
            }
            (Err(error), _) | (_, Err(error)) => return Err(error),
        };
        let Some(deadline) = deadline else {
            return Ok(None);
        };
        let Ok(parsed) = id.parse::<SandboxId>() else {
            return Ok(None);
        };
        // The rowid lane sees every row; a row no action can ever act on is
        // not due work (see `AUTO_LIFECYCLE_NEVER_ACTIONABLE_STATES_SQL`).
        // A state that does not decode is skipped like a malformed id above;
        // a statement-level failure propagates. An unknown-but-decodable
        // state string is left to the transition's own lookup error.
        let state = match row.get::<_, String>(3) {
            Ok(state) => state,
            Err(error) if is_row_decode_error(&error) => {
                tracing::warn!(rowid = row.get::<_, i64>(0)?, error = %describe_error(&error), "skipping malformed auto lifecycle discovery row");
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if state.parse::<SandboxState>().ok() == Some(SandboxState::Deleting) {
            return Ok(None);
        }
        if !(0..=now).contains(&deadline)
            || active.contains(&parsed)
            || selected.iter().any(|due| due.sandbox_id == parsed)
        {
            return Ok(None);
        }
        Ok(Some(DueAutoLifecycleSandbox {
            storage_id: id,
            sandbox_id: parsed,
        }))
    };
    // LIMIT applies to raw index entries, before identity/metadata eligibility.
    if fresh_slots > 0 {
        let mut statement = conn.prepare(&sql!(format!(
            "SELECT rowid, id, next_auto_event_at, state
             FROM sandboxes INDEXED BY idx_sandboxes_next_auto_event_at
             WHERE next_auto_event_at <= ?1
               AND state NOT IN {AUTO_LIFECYCLE_NEVER_ACTIONABLE_STATES_SQL}
             ORDER BY next_auto_event_at DESC, created_at DESC LIMIT 32"
        )))?;
        let mut rows = statement.query([now])?;
        while let Some(row) = rows.next()? {
            if let Some(id) = eligible(row, &selected)? {
                selected.push(id);
            }
            if selected.len() == fresh_slots {
                break;
            }
        }
    }
    // The rowid lane must still ADVANCE past `deleting` rows (it is a scan
    // position, not a work list), so they stay in the result set of this
    // statement and are filtered by `eligible`, not by SQL.
    let mut statement = conn.prepare(sql!(
        "SELECT rowid, id, next_auto_event_at, state
         FROM sandboxes WHERE rowid > ?1 ORDER BY rowid LIMIT 128"
    ))?;
    let mut rows = statement.query([cursor.after])?;
    let mut scanned = 0;
    while selected.len() < available {
        let Some(row) = rows.next()? else {
            if scanned < 128 {
                cursor.after = i64::MIN;
            }
            break;
        };
        scanned += 1;
        cursor.after = row.get(0)?;
        if let Some(id) = eligible(row, &selected)? {
            selected.push(id);
        }
    }
    if available == 1 {
        cursor.fresh_turn = !cursor.fresh_turn;
    }
    Ok(selected)
}

/// Atomically claim a `suspended` sandbox for cold-sandbox eviction by
/// flipping it to `deleting`. The CAS guards two invariants at once:
///
/// - `state = 'suspended'` — a concurrent resume/restore that already
///   moved the state wins; once the row is in `deleting`, the
///   lifecycle state machine rejects incoming restores and the
///   standard `CleanupCompletion::DeleteRow` machinery treats the
///   cleanup as current.
/// - `current_local_artifact_id` must still equal the identity the
///   eviction decision **proved** (`expected_pin`; `None` asserts the
///   row is still unpinned for the legacy class). This binds the
///   delete to the exact bytes that were proven backed: a fast
///   resume + re-suspend between the decision read and this claim
///   mints a new, unproven pin and the claim fails.
///
/// - `updated_at` must still equal `expected_updated_at` when the
///   caller proved additional predicates (owner, pool, quiescence)
///   against a specific row version: every row mutation bumps
///   `updated_at`, so binding the claim to it extends the CAS to the
///   whole proof, not just state + pin. `None` keeps the historical
///   state + pin contract for the disk-pressure loop, whose proof is
///   the pin itself.
///
/// Returns `false` (and changes nothing) when any condition no longer
/// holds.
pub fn claim_suspended_sandbox_for_eviction(
    conn: &Connection,
    id: &str,
    expected_pin: Option<&str>,
    expected_updated_at: Option<&str>,
) -> Result<bool> {
    let changed = match (expected_pin, expected_updated_at) {
        (Some(pin), Some(updated_at)) => conn.execute(
            "UPDATE sandboxes
             SET state = 'deleting',
                 state_changed_at = CASE WHEN state != 'deleting' THEN ?1 ELSE state_changed_at END,
                 updated_at = ?1
             WHERE id = ?2 AND state = 'suspended'
               AND current_local_artifact_id = ?3
               AND updated_at = ?4",
            params![now()?, id, pin, updated_at],
        )?,
        (Some(pin), None) => conn.execute(
            "UPDATE sandboxes
             SET state = 'deleting',
                 state_changed_at = CASE WHEN state != 'deleting' THEN ?1 ELSE state_changed_at END,
                 updated_at = ?1
             WHERE id = ?2 AND state = 'suspended'
               AND current_local_artifact_id = ?3",
            params![now()?, id, pin],
        )?,
        (None, Some(updated_at)) => conn.execute(
            "UPDATE sandboxes
             SET state = 'deleting',
                 state_changed_at = CASE WHEN state != 'deleting' THEN ?1 ELSE state_changed_at END,
                 updated_at = ?1
             WHERE id = ?2 AND state = 'suspended'
               AND current_local_artifact_id IS NULL
               AND updated_at = ?3",
            params![now()?, id, updated_at],
        )?,
        (None, None) => conn.execute(
            "UPDATE sandboxes
             SET state = 'deleting',
                 state_changed_at = CASE WHEN state != 'deleting' THEN ?1 ELSE state_changed_at END,
                 updated_at = ?1
             WHERE id = ?2 AND state = 'suspended'
               AND current_local_artifact_id IS NULL",
            params![now()?, id],
        )?,
    };
    Ok(changed == 1)
}

/// Atomically claim a `restoring` row that carries a persisted cross-node
/// staging failure for cluster-deleted eviction: `restoring → deleting`.
/// The CAS re-asserts the whole shape the eviction decision was made
/// against:
///
/// - `state = 'restoring'` with a non-blank `failure_reason` — the
///   retained-for-retry row `mark_cross_node_restore_staging_failed`
///   leaves. Blank means the marker is absent, matching the Rust guards on
///   both sides (node-ctl's selection and the service's refusal), which
///   use `str::trim`; SQLite's one-argument `trim` strips spaces only, so
///   the whitespace set is spelled out. A `NULL` marker never matches
///   because `trim(NULL) != ''` is `NULL`, so no separate `IS NOT NULL`
///   test is needed.
/// - `current_local_artifact_id IS NULL` — the retryable interrupted
///   restore shape (`sandbox_is_retryable_interrupted_restore`) never
///   carries a pin; asserting it here makes the accepted shape equal the
///   described one instead of following from an argument about writers.
/// - `updated_at = expected_updated_at` — exactly the version the caller
///   read; a restore phase advance, a retry, or any other writer bumps it
///   and fails the claim.
///
/// The marker alone does not mean no restore is live (a retry reuses the
/// row without clearing it); the version check, the caller's quiescence
/// and staging-lock proofs, and the cluster's absent-row report are what
/// make the claim safe. A first-attempt restore with no persisted failure
/// is never claimable by this statement. Returns `false` (and changes
/// nothing) when any condition no longer holds.
pub fn claim_failed_restoring_sandbox_for_eviction(
    conn: &Connection,
    id: &str,
    expected_updated_at: &str,
) -> Result<bool> {
    // The restore columns are only valid on a `restoring` row (the table's
    // CHECK ties `restore_origin` / `restore_phase` / `restore_storage_uri`
    // to that state); every transition out of `restoring` clears them, and
    // so must this one. `state_changed_at` is set unconditionally: the WHERE
    // clause already forbids a row that is not leaving `restoring`.
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = 'deleting',
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             state_changed_at = ?1,
             updated_at = ?1
         WHERE id = ?2 AND state = 'restoring'
           AND trim(failure_reason, char(9) || char(10) || char(13) || ' ') != ''
           AND current_local_artifact_id IS NULL
           AND updated_at = ?3",
        params![now()?, id, expected_updated_at],
    )?;
    Ok(changed == 1)
}

/// Claim quarantined local state after an authoritative cluster-deleted report.
/// The caller checks platform ownership, pool exclusion, quiescence and holds
/// the cleanup fence and staging lock. Re-checking the row version and state
/// keeps a recovery that advanced the sandbox from losing its local resources.
/// Artifact pins remain until the normal DeleteRow cleanup removes all children.
pub fn claim_quarantined_sandbox_for_eviction(
    conn: &Connection,
    id: &str,
    expected_updated_at: &str,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = 'deleting', state_changed_at = ?1, updated_at = ?1
         WHERE id = ?2 AND state = 'quarantined' AND updated_at = ?3",
        params![now()?, id, expected_updated_at],
    )?;
    Ok(changed == 1)
}

/// Claim a `suspending` row whose teardown suspend is terminally stuck, for
/// the cluster-deleted eviction.
///
/// The CAS re-asserts the row's **identity**, not its version: still
/// `suspending`, still carrying exactly the terminal marker (a `suspending`
/// row with any other failure reason still has a publication path and is not
/// this shape), still pinning the exact artifact the refused backup staged,
/// and still at the `state_changed_at` the decision was taken against.
/// Nothing can leave that tuple intact while moving the sandbox on: every
/// path out of `suspending` rewrites `state` and `state_changed_at` together,
/// and a re-stage rewrites `current_local_artifact_id`.
///
/// It deliberately does **not** assert `updated_at`. This row's runtime is
/// still resident and its auto-lifecycle policy still fires, so two loops
/// re-stamp `updated_at` on it every few seconds with values identical to
/// what is already there (`update_network_counters` and
/// `update_auto_lifecycle_state`). A version CAS against that is not a
/// safety guard, it is a livelock: the claim can lose forever while the row
/// never actually changes.
///
/// The staged artifact pin is left in place for the `DeleteRow` cleanup to
/// erase with the rest of the row's children; unlike the failed-restoring
/// claim there are no state-tied restore columns to clear, because
/// `begin_teardown_suspend` nulls them on the way into `suspending`.
/// `state_changed_at` is set unconditionally: the predicate already forbids a
/// row that is not leaving `suspending`.
pub fn claim_teardown_suspend_failed_sandbox_for_eviction(
    conn: &Connection,
    id: &str,
    staged_artifact: LocalArtifactId,
    expected_state_changed_at: &str,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = 'deleting',
             state_changed_at = ?1,
             updated_at = ?1
         WHERE id = ?2 AND state = 'suspending'
           AND failure_reason = ?3
           AND current_local_artifact_id = ?4
           AND state_changed_at = ?5",
        params![
            now()?,
            id,
            pplx_space_sbx_mgr_client::TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON,
            staged_artifact.to_string(),
            expected_state_changed_at
        ],
    )?;
    Ok(changed == 1)
}

pub fn delete_sandbox(conn: &Connection, id: &str) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    delete_sandbox_child_rows(&tx, id)?;
    tx.execute("DELETE FROM sandboxes WHERE id = ?1", params![id])
        .map_err(crate::auto_lifecycle_delete_receipts::db::map_pending_receipt_delete_error)?;
    tx.commit()?;
    Ok(())
}

fn inventory_deletion_witness_matches(current: &SandboxRow, observed: &SandboxRow) -> bool {
    current.id == observed.id
        && current.created_at == observed.created_at
        && current.updated_at == observed.updated_at
        && current.state == observed.state
        && current.restore_operation == observed.restore_operation
        && current.failure_reason == observed.failure_reason
        && current.uid == observed.uid
        && current.slot == observed.slot
        && current.guest_cid == observed.guest_cid
        && current.pid == observed.pid
        && current.spaced_host_pid == observed.spaced_host_pid
        && current.netgtw_pid == observed.netgtw_pid
        && current.vsock_uds_path == observed.vsock_uds_path
        && current.api_socket_path == observed.api_socket_path
        && current.cleanup_completion == observed.cleanup_completion
        && current.cleanup_keep_filesystem == observed.cleanup_keep_filesystem
        && current.current_local_artifact_id == observed.current_local_artifact_id
        && current.current_local_artifact_kind == observed.current_local_artifact_kind
        && current.current_local_artifact_origin == observed.current_local_artifact_origin
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InventoryOwnershipReason {
    DurableCleanupEpisode,
    RowCleanupIntent,
    MalformedCleanupIntent,
    LifecycleOwned,
    RestoreTransferScope,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InventoryStaleRowMutation {
    Removed,
    Missing,
    WitnessChanged,
    Deferred(InventoryOwnershipReason),
}

/// Delete a row selected by inventory only if its lifecycle ownership witness
/// is unchanged and no durable lifecycle owner exists. The immediate
/// transaction makes the ownership reads and manual child-row cascade
/// indivisible from concurrent lifecycle writers.
pub(crate) fn delete_sandbox_if_inventory_witness_matches(
    conn: &Connection,
    observed: &SandboxRow,
) -> Result<InventoryStaleRowMutation> {
    let tx = immediate_transaction(conn)?;
    let Some(current) = get_sandbox(&tx, &observed.id)? else {
        return Ok(InventoryStaleRowMutation::Missing);
    };
    if !inventory_deletion_witness_matches(&current, observed) {
        return Ok(InventoryStaleRowMutation::WitnessChanged);
    }
    if cleanup_episode(&tx, &observed.id)?.is_some() {
        return Ok(InventoryStaleRowMutation::Deferred(
            InventoryOwnershipReason::DurableCleanupEpisode,
        ));
    }
    match (current.cleanup_completion, current.cleanup_keep_filesystem) {
        (Some(_), Some(_)) => {
            return Ok(InventoryStaleRowMutation::Deferred(
                InventoryOwnershipReason::RowCleanupIntent,
            ));
        }
        (None, None) => {}
        _ => {
            return Ok(InventoryStaleRowMutation::Deferred(
                InventoryOwnershipReason::MalformedCleanupIntent,
            ));
        }
    }
    if matches!(
        current.state,
        SandboxState::Stopping | SandboxState::Deleting
    ) {
        return Ok(InventoryStaleRowMutation::Deferred(
            InventoryOwnershipReason::LifecycleOwned,
        ));
    }
    if restore_transfer_scope(&tx, &observed.id)?.is_some() {
        return Ok(InventoryStaleRowMutation::Deferred(
            InventoryOwnershipReason::RestoreTransferScope,
        ));
    }
    let execution_generation = current.space_api_route_identity.execution_generation;
    delete_sandbox_child_rows(&tx, &observed.id)?;
    let deleted = tx
        .execute("DELETE FROM sandboxes WHERE id = ?1", params![&observed.id])
        .map_err(crate::auto_lifecycle_delete_receipts::db::map_pending_receipt_delete_error)?;
    if deleted != 1 {
        return Ok(InventoryStaleRowMutation::WitnessChanged);
    }
    record_execution_delete_receipt(&tx, &observed.id, execution_generation)?;
    tx.commit()?;
    Ok(InventoryStaleRowMutation::Removed)
}

pub fn sandbox_has_runtime_fields(row: &SandboxRow) -> bool {
    row.uid.is_some()
        || row.slot.is_some()
        || row.guest_cid.is_some()
        || row.pid.is_some()
        || row.spaced_host_pid.is_some()
        || row.netgtw_pid.is_some()
        || row.vsock_uds_path.is_some()
        || row.api_socket_path.is_some()
}

pub fn sandbox_has_full_suspend_artifact_pin(row: &SandboxRow) -> bool {
    row.current_local_artifact_id.is_some()
        && row.current_local_artifact_kind == Some(ArtifactKind::Full)
        && row.current_local_artifact_origin == Some(ArtifactOrigin::Suspend)
}

pub fn sandbox_has_registered_full_suspend_artifact(
    conn: &Connection,
    row: &SandboxRow,
) -> Result<bool> {
    let Some(local_artifact_id) = row.current_local_artifact_id else {
        return Ok(false);
    };
    if row.current_local_artifact_kind != Some(ArtifactKind::Full)
        || row.current_local_artifact_origin != Some(ArtifactOrigin::Suspend)
    {
        return Ok(false);
    }
    Ok(
        get_local_artifact(conn, local_artifact_id)?.is_some_and(|artifact| {
            artifact.sandbox_id == row.id
                && artifact.artifact_kind == ArtifactKind::Full
                && artifact.artifact_origin == ArtifactOrigin::Suspend
        }),
    )
}

pub fn cleanup_intent_for_startup_recovery(row: &SandboxRow) -> Option<(bool, CleanupCompletion)> {
    if let (Some(keep_filesystem), Some(completion)) =
        (row.cleanup_keep_filesystem, row.cleanup_completion)
        && completion.owns_row_intent(row)
    {
        return Some((keep_filesystem, completion));
    }

    match row.state {
        // Transient cleanup states represent an unfinished lifecycle operation.
        // Finish them even if runtime fields were already cleared when the
        // target terminal state is self-contained. Suspending is stricter:
        // recovery may only mark it suspended once a full suspend artifact is
        // already pinned; otherwise service-level startup recovery must either
        // finish a complete staged publish or quarantine the row.
        SandboxState::Stopping => Some((true, CleanupCompletion::MarkStopped)),
        SandboxState::Suspending if sandbox_has_full_suspend_artifact_pin(row) => {
            Some((true, CleanupCompletion::MarkSuspended))
        }
        SandboxState::Suspending => None,
        SandboxState::Deleting => Some((false, CleanupCompletion::DeleteRow)),
        SandboxState::CleanupRequired => Some((true, CleanupCompletion::Quarantine)),
        SandboxState::Starting if sandbox_has_runtime_fields(row) => {
            Some((true, CleanupCompletion::MarkStopped))
        }
        SandboxState::Restoring if sandbox_has_runtime_fields(row) => {
            Some((true, CleanupCompletion::MarkSuspended))
        }
        SandboxState::CleanupFailed if sandbox_has_runtime_fields(row) => {
            Some((true, CleanupCompletion::Quarantine))
        }
        _ => None,
    }
}

/// On startup: reconcile interrupted DB states to a safe local outcome.
/// Also check Started sandboxes for dead PIDs.
pub fn recover_interrupted_startup_states(
    conn: &Connection,
) -> std::result::Result<Vec<StartupRecoveryRecord>, RecoverStartupStatesError> {
    let rows = list_sandboxes_for_recovery(conn)?;
    let mut recovered = Vec::new();

    for row in rows {
        if row.state.is_transient() {
            let episode = match cleanup_episode(conn, &row.id) {
                Ok(episode) => episode,
                Err(error) if is_row_decode_error(&error) => {
                    tracing::warn!(sandbox_id = %row.id, error = %describe_error(&error), "retaining malformed startup cleanup ownership; operator repair required");
                    recovered.push(StartupRecoveryRecord {
                        id: row.id,
                        previous_state: row.state,
                        outcome: StartupRecoveryOutcome::Deferred,
                    });
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if let Some(episode) = episode.filter(|episode| {
                episode.requested_at.is_none() && episode.activation_state == row.state
            }) {
                // Hand off the exact episode before startup can admit work.
                // Generic lifecycle rewriting loses its receipt; merely
                // deferring leaves no periodic owner if later recovery fails.
                let reason = match row.state {
                    SandboxState::Creating => SandboxQuarantineReasonCode::CreateInterrupted,
                    SandboxState::Starting => {
                        SandboxQuarantineReasonCode::ColdActivationInterrupted
                    }
                    SandboxState::Restoring => {
                        SandboxQuarantineReasonCode::RestoreActivationInterrupted
                    }
                    state => {
                        return Err(RecoverStartupStatesError::UnsupportedActivationState(state));
                    }
                };
                let requested = request_cleanup_episode(
                    conn,
                    &row.id,
                    episode.id,
                    reason,
                    "startup recovery resumed interrupted activation cleanup",
                )?;
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: match requested {
                        CleanupEpisodeRequestOutcome::Requested
                        | CleanupEpisodeRequestOutcome::AlreadyRequested => {
                            StartupRecoveryOutcome::CleanupRequired {
                                keep_filesystem: episode.keep_filesystem,
                                completion: episode.completion,
                            }
                        }
                        CleanupEpisodeRequestOutcome::Superseded => {
                            StartupRecoveryOutcome::Deferred
                        }
                    },
                });
                continue;
            }
            if row.state == SandboxState::Creating {
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::Deferred,
                });
                continue;
            }

            if matches!(
                row.state,
                SandboxState::Pausing | SandboxState::Resuming | SandboxState::Checkpointing
            ) || (row.state == SandboxState::Suspending
                && !sandbox_has_full_suspend_artifact_pin(&row))
                || sandbox_is_retryable_interrupted_restore(&row)
            {
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::Deferred,
                });
            } else if let Some((keep_filesystem, completion)) =
                cleanup_intent_for_startup_recovery(&row)
            {
                mark_cleanup_required(
                    conn,
                    &row.id,
                    keep_filesystem,
                    completion,
                    &format!(
                        "startup recovery: sandbox was left in transient state '{}'",
                        row.state
                    ),
                )?;
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::CleanupRequired {
                        keep_filesystem,
                        completion,
                    },
                });
            } else if row.state == SandboxState::Restoring {
                let outcome = update_state(conn, &row.id, SandboxState::Suspended)?;
                if !outcome.is_applied() {
                    continue;
                }
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::State(SandboxState::Suspended),
                });
            } else {
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::Deferred,
                });
            }
            continue;
        }

        if row.state == SandboxState::Started
            && !row
                .pid
                .is_some_and(|pid| std::path::Path::new(&format!("/proc/{pid}")).exists())
        {
            // The state-only Firecracker probe cannot reconstruct trusted
            // process credentials. Preserve all resources until the missing
            // runtime identity can be explained; an older artifact is not
            // evidence of the customer's current state.
            if settle_startup_resident(
                conn,
                &row,
                StartupResidentSettlement::Quarantined {
                    reason_code: SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
                    message: "startup recovery: started sandbox has no live registered VM pid",
                },
            )? {
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::State(SandboxState::Quarantined),
                });
            }
        }
    }

    Ok(recovered)
}

#[derive(Debug, thiserror::Error)]
pub enum RecoverStartupStatesError {
    #[error("startup state recovery database operation failed: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("failed to hand interrupted activation to cleanup: {0}")]
    CleanupRequest(#[from] RequestCleanupEpisodeError),
    #[error("armed cleanup episode has unsupported activation state {0}")]
    UnsupportedActivationState(SandboxState),
}

// -- Pool helpers --
// Migration needed: ALTER TABLE sandboxes ADD COLUMN pool_template TEXT;

/// Find one paused sandbox with matching pool membership and optional content hash.
pub fn find_pool_sandbox(
    conn: &Connection,
    template: &str,
    content_hash: Option<&str>,
    required_user: Option<&SandboxOwner>,
) -> Result<Option<SandboxRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, uuid8, state, template, template_hash, user, runtime, uid, slot, guest_cid, pid,
         spaced_host_pid, netgtw_pid, vsock_uds_path, api_socket_path, created_at, updated_at, snapshots_enabled, rootfs_size_mb, pool_template, guest_uptime_secs, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, failure_reason, cleanup_completion, cleanup_keep_filesystem, network_policy_override_json, network_tx_bytes, network_rx_bytes, network_policy_generation, applied_network_policy_generation, applied_network_policy_source, network_policy_authority, auto_lifecycle_authority, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin, execution_metadata_json, volume_encryption, space_api_route_generation, space_api_route_node_id, space_api_route_execution_generation, space_api_route_pod_uid, restore_origin, restore_phase, restore_storage_uri, space_api_route_substrate_kind, space_api_route_activation_attempt_id, spaced_endpoint_proof_json, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id
         FROM sandboxes
         WHERE pool_template = ?1
           AND state = 'paused'
           AND (?2 IS NULL OR template_hash = ?2)
           AND (?3 IS NULL OR user = ?3)
         ORDER BY created_at, id
         LIMIT 1",
    )?;
    let mut rows = stmt.query_map(
        params![
            template,
            content_hash,
            required_user.map(SandboxOwner::as_str)
        ],
        row_to_sandbox,
    )?;
    rows.next().transpose()
}

pub fn claim_pool_sandbox(
    conn: &Connection,
    sandbox_id: &str,
    template: &str,
    content_hash: Option<&str>,
    required_user: Option<&SandboxOwner>,
    user: &SandboxOwner,
    process_env: &BTreeMap<String, String>,
) -> Result<Option<SandboxRow>> {
    let process_env_json = serde_json::to_string(process_env)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let tx = immediate_transaction(conn)?;
    let mut stmt = tx.prepare(
        "SELECT id, uuid8, state, template, template_hash, user, runtime, uid, slot, guest_cid, pid,
         spaced_host_pid, netgtw_pid, vsock_uds_path, api_socket_path, created_at, updated_at, snapshots_enabled, rootfs_size_mb, pool_template, guest_uptime_secs, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, failure_reason, cleanup_completion, cleanup_keep_filesystem, network_policy_override_json, network_tx_bytes, network_rx_bytes, network_policy_generation, applied_network_policy_generation, applied_network_policy_source, network_policy_authority, auto_lifecycle_authority, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin, execution_metadata_json, volume_encryption, space_api_route_generation, space_api_route_node_id, space_api_route_execution_generation, space_api_route_pod_uid, restore_origin, restore_phase, restore_storage_uri, space_api_route_substrate_kind, space_api_route_activation_attempt_id, spaced_endpoint_proof_json, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id
         FROM sandboxes
         WHERE id = ?4 AND pool_template = ?1
           AND state = 'paused'
           AND (?2 IS NULL OR template_hash = ?2)
           AND (?3 IS NULL OR user = ?3)
         ORDER BY created_at, id
         LIMIT 1",
    )?;
    let donor = stmt
        .query_row(
            params![
                template,
                content_hash,
                required_user.map(SandboxOwner::as_str),
                sandbox_id
            ],
            row_to_sandbox,
        )
        .optional()?;
    drop(stmt);

    let Some(donor) = donor else {
        tx.commit()?;
        return Ok(None);
    };

    let changed = tx.execute(
        "UPDATE sandboxes
         SET pool_template = NULL,
             user = ?1,
             process_env_json = ?2,
             updated_at = ?3
         WHERE id = ?4
           AND pool_template = ?5
           AND state = 'paused'
           AND (?6 IS NULL OR template_hash = ?6)
           AND (?7 IS NULL OR user = ?7)",
        params![
            user.as_str(),
            process_env_json,
            now()?,
            donor.id,
            template,
            content_hash,
            required_user.map(SandboxOwner::as_str)
        ],
    )?;
    if changed != 1 {
        tx.commit()?;
        return Ok(None);
    }
    tx.commit()?;
    Ok(Some(donor))
}

/// Count pool sandboxes: (available=Paused, pending=Creating|Starting|Pausing).
pub fn count_pool_sandboxes(conn: &Connection, template: &str) -> Result<(u32, u32)> {
    let available: u32 = conn.query_row(
        "SELECT COUNT(*) FROM sandboxes WHERE pool_template = ?1 AND state = 'paused'",
        params![template],
        |row| row.get(0),
    )?;
    let pending: u32 = conn.query_row(
        "SELECT COUNT(*) FROM sandboxes WHERE pool_template = ?1 AND state IN ('creating', 'created', 'starting', 'pausing')",
        params![template],
        |row| row.get(0),
    )?;
    Ok((available, pending))
}

/// Set or clear the pool_template field on a sandbox.
pub fn set_pool_template(conn: &Connection, id: &str, pool_template: Option<&str>) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET pool_template = ?1, updated_at = ?2 WHERE id = ?3",
        params![pool_template, now()?, id],
    )?;
    Ok(())
}

pub fn set_network_policy_fields(
    conn: &Connection,
    id: &str,
    network_policy_override: Option<&NetworkPolicy>,
    network_policy_generation: u64,
    network_policy_authority: PolicyAuthority,
) -> Result<()> {
    let network_policy_override_json = network_policy_override
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "UPDATE sandboxes
         SET network_policy_override_json = ?1,
             network_policy_generation = ?2,
             network_policy_authority = ?3,
             updated_at = ?4
         WHERE id = ?5",
        params![
            network_policy_override_json.as_deref(),
            u64_to_sql_i64(network_policy_generation),
            network_policy_authority.as_str(),
            now()?,
            id
        ],
    )?;
    Ok(())
}

fn restore_pool_donor(
    conn: &Connection,
    sandbox_id: SandboxId,
    donor: &SandboxRow,
) -> Result<Option<crate::auto_lifecycle_delete_receipts::db::PendingReceiptSupersession>> {
    let id = sandbox_id.to_string();
    set_pool_template(conn, &id, donor.pool_template.as_deref())?;
    update_user(conn, &id, &donor.user)?;
    update_process_env(conn, &id, &donor.process_env)?;
    update_auto_lifecycle(
        conn,
        &id,
        &donor.auto_lifecycle,
        donor.auto_lifecycle_authority,
    )?;
    update_auto_lifecycle_state(conn, &id, &donor.auto_lifecycle_state)?;
    set_network_policy_fields(
        conn,
        &id,
        donor.network_policy_override.as_ref(),
        donor.network_policy_generation,
        donor.network_policy_authority,
    )?;
    let receipt_supersession = update_space_api_route_identity_in_transaction(
        conn,
        sandbox_id,
        &donor.space_api_route_identity,
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
    )?;
    Ok(receipt_supersession)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolDonorRestoreDisposition {
    Restored,
    NotAdopted,
    ReceiptOwned,
    LifecycleOwned,
}

pub fn restore_pool_donor_if_adopted(
    conn: &Connection,
    sandbox_id: SandboxId,
    donor: &SandboxRow,
) -> Result<PoolDonorRestoreDisposition> {
    let id = sandbox_id.to_string();
    let tx = immediate_transaction(conn)?;
    let current_pool_template = tx
        .query_row(
            "SELECT pool_template FROM sandboxes WHERE id = ?1",
            params![id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?;
    let Some(None) = current_pool_template else {
        tx.commit()?;
        return Ok(PoolDonorRestoreDisposition::NotAdopted);
    };
    let receipt_supersession = match restore_pool_donor(&tx, sandbox_id, donor) {
        Ok(receipt_supersession) => receipt_supersession,
        Err(error)
            if crate::auto_lifecycle_delete_receipts::db::is_pending_receipt_execution_fence(
                &error,
            ) =>
        {
            tx.rollback()?;
            return Ok(PoolDonorRestoreDisposition::ReceiptOwned);
        }
        Err(error)
            if crate::auto_lifecycle_delete_receipts::db::is_lifecycle_execution_fence(&error) =>
        {
            tx.rollback()?;
            return Ok(PoolDonorRestoreDisposition::LifecycleOwned);
        }
        Err(error) => return Err(error),
    };
    tx.commit()?;
    emit_auto_lifecycle_delete_receipt_superseded_if_present(sandbox_id, receipt_supersession);
    Ok(PoolDonorRestoreDisposition::Restored)
}

/// Update the user field on a sandbox.
pub fn update_user(conn: &Connection, id: &str, user: &SandboxOwner) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET user = ?1, updated_at = ?2 WHERE id = ?3",
        params![user.as_str(), now()?, id],
    )?;
    Ok(())
}

pub fn update_process_env(
    conn: &Connection,
    id: &str,
    process_env: &BTreeMap<String, String>,
) -> Result<()> {
    let process_env_json = serde_json::to_string(process_env)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "UPDATE sandboxes SET process_env_json = ?1, updated_at = ?2 WHERE id = ?3",
        params![process_env_json, now()?, id],
    )?;
    Ok(())
}

pub fn set_sandbox_owner_if_current(
    conn: &Connection,
    id: &str,
    expected_current_owner: &SandboxOwner,
    owner: &SandboxOwner,
) -> Result<Option<SandboxRow>> {
    let tx = immediate_transaction(conn)?;
    let Some(previous) = get_sandbox(&tx, id)? else {
        tx.commit()?;
        return Ok(None);
    };
    if previous.user.as_str() == owner.as_str() {
        tx.commit()?;
        return Ok(Some(previous));
    }
    if previous.user.as_str() != expected_current_owner.as_str() {
        tx.commit()?;
        return Ok(None);
    }

    let changed = tx.execute(
        "UPDATE sandboxes
         SET user = ?1,
             updated_at = ?2
         WHERE id = ?3
           AND user = ?4",
        params![owner.as_str(), now()?, id, expected_current_owner.as_str()],
    )?;
    if changed != 1 {
        tx.commit()?;
        return Ok(None);
    }
    tx.commit()?;
    Ok(Some(previous))
}

pub fn update_auto_lifecycle(
    conn: &Connection,
    id: &str,
    auto_lifecycle: &AutoLifecyclePolicy,
    authority: PolicyAuthority,
) -> Result<()> {
    let mut auto_lifecycle_state = crate::auto_lifecycle::initial_runtime_state(
        auto_lifecycle,
        crate::auto_lifecycle::now_secs(),
    );
    crate::auto_lifecycle::recompute_runtime_state(&mut auto_lifecycle_state, auto_lifecycle);
    update_auto_lifecycle_with_state(conn, id, auto_lifecycle, &auto_lifecycle_state, authority)
}

/// Called inside activation admission's transaction, before runtime launch.
/// Snapshot policy is historical; a cluster activation carries current intent.
/// Preserve local overrides and unchanged clocks, including reconnect floors.
fn install_activation_auto_lifecycle(
    conn: &Connection,
    id: &str,
    policy: &AutoLifecyclePolicy,
) -> Result<()> {
    let sandbox = get_sandbox(conn, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    if sandbox.auto_lifecycle_authority == PolicyAuthority::Local {
        return Ok(());
    }
    let runtime_state = if sandbox.auto_lifecycle.normalized() == *policy {
        sandbox.auto_lifecycle_state
    } else {
        AutoLifecycleRuntimeState::default()
    };
    update_auto_lifecycle_with_state(conn, id, policy, &runtime_state, PolicyAuthority::Cluster)
}

pub fn update_auto_lifecycle_with_state(
    conn: &Connection,
    id: &str,
    auto_lifecycle: &AutoLifecyclePolicy,
    auto_lifecycle_state: &AutoLifecycleRuntimeState,
    authority: PolicyAuthority,
) -> Result<()> {
    let auto_lifecycle_json = serde_json::to_string(auto_lifecycle)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let auto_lifecycle_state_json = serde_json::to_string(&auto_lifecycle_state)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "UPDATE sandboxes
         SET auto_lifecycle_json = ?1,
             auto_lifecycle_state_json = ?2,
             next_auto_event_at = ?3,
             auto_lifecycle_authority = ?4,
             auto_lifecycle_delete_block_reason = NULL,
             auto_lifecycle_delete_blocked_at = NULL,
             updated_at = ?5
         WHERE id = ?6",
        params![
            auto_lifecycle_json,
            auto_lifecycle_state_json,
            auto_lifecycle_state.next_auto_event_at.map(SqliteU64),
            authority.as_str(),
            now()?,
            id
        ],
    )?;
    Ok(())
}

/// Persist the auto-lifecycle runtime state, if it actually differs from what
/// is stored.
///
/// A state identical to the row's own is not a transition and must not be
/// written. The scheduler re-runs a due stage every tick, and a stage that
/// keeps failing the same way re-derives a byte-identical runtime state each
/// time (`handle_auto_lifecycle_error` records the same error text, with
/// the same deadline). Writing it anyway bumps `updated_at` — the quiescence
/// clock the cluster-deleted eviction reads — every few seconds on a row that
/// has not moved. `auto_lifecycle_action_can_act` already keeps the gated
/// actions from recording on a mid-transition row; this closes the same gap
/// for the ungated ones, such as delete. Nothing observable is lost: the two
/// columns this statement owns are compared to the exact values it would have
/// written.
///
/// One reader moves the eager way. The disk-pressure eviction ages a
/// `suspended` row with no activity clocks by `updated_at`
/// (`sandbox_eviction::IDLE_WITNESS_SQL`), so such a row whose stage keeps
/// failing identically now ages from its last real change rather than its
/// last repeated record, and can become a candidate sooner than before. That
/// is the intended meaning of idle, and the eviction's backup proof and claim
/// CAS still apply unchanged.
pub fn update_auto_lifecycle_state(
    conn: &Connection,
    id: &str,
    auto_lifecycle_state: &AutoLifecycleRuntimeState,
) -> Result<()> {
    let state_json = serde_json::to_string(auto_lifecycle_state)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.prepare_cached(
        "UPDATE sandboxes
         SET auto_lifecycle_state_json = ?1, next_auto_event_at = ?2, updated_at = ?3
         WHERE id = ?4
           AND (auto_lifecycle_state_json IS NOT ?1 OR next_auto_event_at IS NOT ?2)",
    )?
    .execute(params![
        state_json,
        auto_lifecycle_state.next_auto_event_at.map(SqliteU64),
        now()?,
        id
    ])?;
    Ok(())
}

pub fn update_guest_uptime(conn: &Connection, id: &str, secs: u64) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET guest_uptime_secs = ?1 WHERE id = ?2",
        params![secs as i64, id],
    )?;
    Ok(())
}

/// Persist the net-gateway `(uid, pid)` registration that host-control auth
/// matches against. Launch paths rely on this being durable before the
/// gateway arm returns, so an UPDATE that matched no row (the sandbox was
/// deleted by concurrent cleanup mid-launch) is an error: silently returning
/// `Ok` would leave the just-launched gateway permanently unauthorized
/// without tripping the caller's launch-failure cleanup.
pub fn update_netgtw_pid(conn: &Connection, id: &str, uid: u32, netgtw_pid: u32) -> Result<()> {
    let changed = conn.execute(
        "UPDATE sandboxes SET uid = ?1, netgtw_pid = ?2, updated_at = ?3 WHERE id = ?4",
        params![uid, netgtw_pid, now()?, id],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    Ok(())
}

/// Commit a watchdog replacement only while the persisted endpoint authority
/// still matches the observation that initiated the restart. A concurrent
/// caller may already have committed the same live replacement; confirming
/// that PID is idempotent, while any different PID remains protected.
pub fn replace_netgtw_pid(
    conn: &Connection,
    id: &str,
    uid: u32,
    expected_netgtw_pid: Option<u32>,
    netgtw_pid: u32,
) -> Result<()> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET netgtw_pid = ?1, updated_at = ?2
         WHERE id = ?3
           AND uid = ?4
           AND (netgtw_pid IS ?5 OR netgtw_pid = ?1)",
        params![netgtw_pid, now()?, id, uid, expected_netgtw_pid],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    Ok(())
}

/// Persist the replacement spaced-host PID that endpoint authentication
/// matches against. The owner UID is already part of the sandbox runtime row
/// and is deliberately included in the guard so a stale watchdog observation
/// cannot transfer endpoint authority to a replacement after ownership moved.
/// A concurrent caller may confirm the same replacement PID idempotently.
pub fn update_spaced_host_pid(
    conn: &Connection,
    id: &str,
    uid: u32,
    expected_spaced_host_pid: Option<u32>,
    spaced_host_pid: u32,
) -> Result<()> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET spaced_host_pid = ?1, spaced_endpoint_proof_json = NULL, updated_at = ?2
         WHERE id = ?3
           AND uid = ?4
           AND (spaced_host_pid IS ?5 OR spaced_host_pid = ?1)",
        params![spaced_host_pid, now()?, id, uid, expected_spaced_host_pid],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    Ok(())
}

/// Memoize a proof for an execution that predates proof capture.
///
/// Every runtime identity dimension participates in the compare-and-set so a
/// bridge request racing activation replacement cannot attach its observation
/// to the replacement execution.
pub fn persist_spaced_endpoint_proof_if_missing(
    conn: &Connection,
    runtime: &crate::endpoint_proof::SpacedEndpointRuntimeIdentity,
    proof: &SpacedEndpointProof,
) -> Result<bool> {
    proof
        .validate_runtime(runtime)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let proof_json = serde_json::to_string(proof)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let changed = conn.execute(
        "UPDATE sandboxes
         SET spaced_endpoint_proof_json = ?1, updated_at = ?2
         WHERE id = ?3
           AND spaced_endpoint_proof_json IS NULL
           AND pid = ?4
           AND spaced_host_pid = ?5
           AND uid = ?6
           AND guest_cid = ?7
           AND vsock_uds_path = ?8
           AND space_api_route_execution_generation = ?9
           AND space_api_route_activation_attempt_id = ?10",
        params![
            proof_json,
            now()?,
            runtime.sandbox_id.to_string(),
            runtime.runtime_pid.get().get(),
            runtime.spaced_host_pid.get().get(),
            runtime.sandbox_uid.get(),
            runtime.guest_cid.get().get(),
            runtime.vsock_path.to_string_lossy(),
            runtime.execution_generation.get(),
            runtime.activation_attempt_id.to_string(),
        ],
    )?;
    Ok(changed == 1)
}

pub fn update_network_policy_override(
    conn: &Connection,
    id: &str,
    network_policy_override: Option<&NetworkPolicy>,
    network_policy_generation: u64,
    authority: PolicyAuthority,
    enqueue_live_gateway: bool,
    mark_dirty: bool,
) -> Result<NetworkPolicyOverrideUpdateOutcome> {
    let network_policy_override_json = network_policy_override
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let generation = u64_to_sql_i64(network_policy_generation);
    let tx = immediate_transaction(conn)?;
    let (existing_json, existing_generation, existing_authority) = tx.query_row(
        "SELECT network_policy_override_json, network_policy_generation, network_policy_authority
         FROM sandboxes
         WHERE id = ?1",
        params![id],
        |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, i64>(1)?,
                parse_policy_authority(row.get::<_, String>(2)?.as_str(), 2)?,
            ))
        },
    )?;

    let mut queued_live_gateway = false;
    let mut queued_dirty_ref = false;
    let plan = decide_network_policy_override_update(
        NetworkPolicyOverrideExisting {
            json: existing_json.as_deref(),
            generation: existing_generation,
            authority: existing_authority,
        },
        NetworkPolicyOverrideRequested {
            json: network_policy_override_json.as_deref(),
            generation,
            authority,
        },
    );
    let decision = match plan {
        NetworkPolicyOverrideUpdatePlan::Apply {
            generation,
            authority,
        } => {
            tx.execute(
                "UPDATE sandboxes
             SET network_policy_override_json = ?1,
                 network_policy_generation = ?2,
                 network_policy_authority = ?3,
                 updated_at = ?4
             WHERE id = ?5",
                params![
                    network_policy_override_json.as_deref(),
                    generation,
                    authority.as_str(),
                    now()?,
                    id
                ],
            )?;
            if enqueue_live_gateway {
                queued_live_gateway = enqueue_network_policy_reconcile_if_live_tx(&tx, id)?;
            }
            if mark_dirty {
                queued_dirty_ref = enqueue_node_dirty_network_policy_sandbox_tx(&tx, id)?;
            }
            NetworkPolicyOverrideUpdate::Apply
        }
        NetworkPolicyOverrideUpdatePlan::RepairIfLiveGateway => {
            if enqueue_live_gateway {
                queued_live_gateway = enqueue_network_policy_reconcile_if_live_tx(&tx, id)?;
            }
            if queued_live_gateway {
                NetworkPolicyOverrideUpdate::Repair
            } else {
                NetworkPolicyOverrideUpdate::Skip
            }
        }
        NetworkPolicyOverrideUpdatePlan::Skip => NetworkPolicyOverrideUpdate::Skip,
        NetworkPolicyOverrideUpdatePlan::Conflict => NetworkPolicyOverrideUpdate::Conflict,
    };
    tx.commit()?;
    Ok(NetworkPolicyOverrideUpdateOutcome {
        decision,
        queued_live_gateway,
        queued_dirty_ref,
    })
}

pub fn clear_local_network_policy(
    conn: &Connection,
    id: &str,
    enqueue_live_gateway: bool,
    mark_dirty: bool,
) -> Result<NetworkPolicyOverrideUpdateOutcome> {
    let tx = immediate_transaction(conn)?;
    let existing_authority = tx.query_row(
        "SELECT network_policy_authority FROM sandboxes WHERE id = ?1",
        params![id],
        |row| parse_policy_authority(row.get::<_, String>(0)?.as_str(), 0),
    )?;
    let mut queued_live_gateway = false;
    let mut queued_dirty_ref = false;
    let decision = if existing_authority == PolicyAuthority::Cluster {
        if enqueue_live_gateway {
            queued_live_gateway = enqueue_network_policy_reconcile_if_live_tx(&tx, id)?;
        }
        if queued_live_gateway {
            NetworkPolicyOverrideUpdate::Repair
        } else {
            NetworkPolicyOverrideUpdate::Skip
        }
    } else {
        tx.execute(
            "UPDATE sandboxes
             SET network_policy_override_json = NULL,
                 network_policy_generation = 0,
                 network_policy_authority = ?1,
                 updated_at = ?2
             WHERE id = ?3",
            params![PolicyAuthority::Cluster.as_str(), now()?, id],
        )?;
        if enqueue_live_gateway {
            queued_live_gateway = enqueue_network_policy_reconcile_if_live_tx(&tx, id)?;
        }
        if mark_dirty {
            queued_dirty_ref = enqueue_node_dirty_network_policy_sandbox_tx(&tx, id)?;
        }
        NetworkPolicyOverrideUpdate::Apply
    };
    tx.commit()?;
    Ok(NetworkPolicyOverrideUpdateOutcome {
        decision,
        queued_live_gateway,
        queued_dirty_ref,
    })
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum NetworkPolicyOverrideUpdate {
    Skip,
    Apply,
    Repair,
    Conflict,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct NetworkPolicyOverrideUpdateOutcome {
    pub decision: NetworkPolicyOverrideUpdate,
    pub queued_live_gateway: bool,
    pub queued_dirty_ref: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct NetworkPolicyOverrideExisting<'a> {
    json: Option<&'a str>,
    generation: i64,
    authority: PolicyAuthority,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct NetworkPolicyOverrideRequested<'a> {
    json: Option<&'a str>,
    generation: i64,
    authority: PolicyAuthority,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum NetworkPolicyOverrideUpdatePlan {
    Skip,
    RepairIfLiveGateway,
    Conflict,
    Apply {
        generation: i64,
        authority: PolicyAuthority,
    },
}

fn decide_network_policy_override_update(
    existing: NetworkPolicyOverrideExisting<'_>,
    requested: NetworkPolicyOverrideRequested<'_>,
) -> NetworkPolicyOverrideUpdatePlan {
    if requested.authority == PolicyAuthority::Cluster
        && existing.authority == PolicyAuthority::Local
    {
        return NetworkPolicyOverrideUpdatePlan::Skip;
    }

    if requested.authority == PolicyAuthority::Local {
        if existing.authority == PolicyAuthority::Local && existing.json == requested.json {
            return NetworkPolicyOverrideUpdatePlan::RepairIfLiveGateway;
        }
        return NetworkPolicyOverrideUpdatePlan::Apply {
            generation: 0,
            authority: PolicyAuthority::Local,
        };
    }

    if requested.generation < existing.generation {
        return NetworkPolicyOverrideUpdatePlan::Skip;
    }
    if requested.generation == existing.generation {
        if existing.authority == requested.authority && existing.json == requested.json {
            NetworkPolicyOverrideUpdatePlan::RepairIfLiveGateway
        } else {
            NetworkPolicyOverrideUpdatePlan::Conflict
        }
    } else {
        NetworkPolicyOverrideUpdatePlan::Apply {
            generation: requested.generation,
            authority: requested.authority,
        }
    }
}

pub fn enqueue_network_policy_reconcile_if_live(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let queued = enqueue_network_policy_reconcile_if_live_tx(&tx, sandbox_id)?;
    tx.commit()?;
    Ok(queued)
}

fn enqueue_network_policy_reconcile_if_live_tx(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
) -> Result<bool> {
    let ts = now()?;
    let changed = tx.execute(
        "INSERT INTO network_policy_reconcile_queue (sandbox_id, requested_at, updated_at)
         SELECT ?1, ?2, ?2
         WHERE EXISTS (
             SELECT 1 FROM sandboxes
             WHERE id = ?1
               AND netgtw_pid IS NOT NULL
               AND pool_template IS NULL
         )
         ON CONFLICT(sandbox_id) DO UPDATE SET
             requested_at = excluded.requested_at,
             updated_at = excluded.updated_at",
        params![sandbox_id, ts],
    )?;
    Ok(changed != 0)
}

fn enqueue_node_dirty_network_policy_sandbox_tx(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
) -> Result<bool> {
    enqueue_node_dirty_ref_tx(
        tx,
        RECONCILE_FACET_NETWORK_POLICY,
        "network_policy_sandbox",
        sandbox_id,
    )
}

pub fn enqueue_node_dirty_sandbox_inventory(conn: &Connection, sandbox_id: &str) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let queued = enqueue_node_dirty_sandbox_inventory_tx(&tx, sandbox_id)?;
    tx.commit()?;
    Ok(queued)
}

fn enqueue_node_dirty_sandbox_inventory_tx(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
) -> Result<bool> {
    enqueue_node_dirty_ref_tx(
        tx,
        RECONCILE_FACET_SANDBOX_INVENTORY,
        "sandbox_inventory",
        sandbox_id,
    )
}

fn enqueue_node_dirty_ref_tx(
    tx: &rusqlite::Transaction<'_>,
    facet: i32,
    element_kind: &str,
    element_value: &str,
) -> Result<bool> {
    let ts = now()?;
    let changed = tx.execute(
        "INSERT INTO node_dirty_refs
             (facet, element_kind, element_value, requested_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?4)
         ON CONFLICT(facet, element_kind, element_value) DO UPDATE SET
             requested_at = excluded.requested_at,
             updated_at = excluded.updated_at",
        params![facet, element_kind, element_value, ts],
    )?;
    Ok(changed != 0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDirtyRefRow {
    pub facet: i32,
    pub element_kind: String,
    pub element_value: String,
    /// Opaque send witness; timestamps cannot distinguish same-second writes.
    pub revision: i64,
}

pub fn list_node_dirty_refs(conn: &Connection, limit: usize) -> Result<Vec<NodeDirtyRefRow>> {
    let mut stmt = conn.prepare(
        "SELECT facet, element_kind, element_value, revision
         FROM node_dirty_refs
         ORDER BY requested_at ASC
         LIMIT ?1",
    )?;
    let rows = stmt.query_map(params![limit.min(i64::MAX as usize) as i64], |row| {
        Ok(NodeDirtyRefRow {
            facet: row.get(0)?,
            element_kind: row.get(1)?,
            element_value: row.get(2)?,
            revision: row.get(3)?,
        })
    })?;
    rows.collect()
}

/// Acknowledge only the obligations read before sending. A newer enqueue, even
/// for a previously deleted key, remains pending for the worker's next retry.
pub fn delete_node_dirty_refs(conn: &Connection, refs: &[NodeDirtyRefRow]) -> Result<usize> {
    let tx = immediate_transaction(conn)?;
    let mut deleted = 0;
    for reference in refs {
        deleted += tx.execute(
            "DELETE FROM node_dirty_refs
             WHERE facet = ?1
               AND element_kind = ?2
               AND element_value = ?3
               AND revision = ?4",
            params![
                reference.facet,
                reference.element_kind,
                reference.element_value,
                reference.revision
            ],
        )?;
    }
    tx.commit()?;
    Ok(deleted)
}

pub fn delete_network_policy_reconcile_rows<I>(conn: &Connection, sandbox_ids: I) -> Result<usize>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let tx = immediate_transaction(conn)?;
    let mut deleted = 0;
    for sandbox_id in sandbox_ids {
        deleted += tx.execute(
            "DELETE FROM network_policy_reconcile_queue WHERE sandbox_id = ?1",
            params![sandbox_id.as_ref()],
        )?;
    }
    tx.commit()?;
    Ok(deleted)
}

pub fn mark_network_policy_applied(
    conn: &Connection,
    id: &str,
    applied_network_policy_source: NetworkPolicyAppliedSource,
    applied_network_policy_generation: u64,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET applied_network_policy_generation = ?1,
             applied_network_policy_source = ?2,
             updated_at = ?3
         WHERE id = ?4
           AND (
               ?1 > applied_network_policy_generation
               OR applied_network_policy_source != ?2
           )",
        params![
            u64_to_sql_i64(applied_network_policy_generation),
            applied_network_policy_source.as_str(),
            now()?,
            id
        ],
    )?;
    Ok(changed != 0)
}

/// Persist a net-gateway stats observation, if it actually advances the
/// counters.
///
/// The counters only ever climb (`MAX`), so an observation that carries no
/// new bytes has nothing to persist — and must not be written anyway. The
/// watchdog re-probes every sandbox with a live net-gateway every 30 s for as
/// long as its runtime is resident, and an unconditional write makes each of
/// those probes bump `updated_at` on a row where nothing changed. That column
/// is the quiescence clock the cluster-deleted eviction reads, so an idle
/// sandbox's purely informational probe would hold the row inside the window
/// forever. Same shape as [`mark_network_policy_applied`]: write only when
/// the observation is news.
pub fn update_network_counters(
    conn: &Connection,
    id: &str,
    network_tx_bytes: u64,
    network_rx_bytes: u64,
) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes
         SET network_tx_bytes = MAX(network_tx_bytes, ?1),
             network_rx_bytes = MAX(network_rx_bytes, ?2),
             updated_at = ?3
         WHERE id = ?4
           AND (?1 > network_tx_bytes OR ?2 > network_rx_bytes)",
        params![
            u64_to_sql_i64(network_tx_bytes),
            u64_to_sql_i64(network_rx_bytes),
            now()?,
            id
        ],
    )?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn create_test_schema(conn: &mut Connection) {
    use pplx_space_sbxmgr_migrations::{COMMON_SCHEMA_MIGRATOR, SANDBOX_MANAGER_MIGRATOR};
    pplx_space_common_svc::sql::sqlite::migrate_common_schema_up(conn, &COMMON_SCHEMA_MIGRATOR)
        .unwrap();
    for migration in SANDBOX_MANAGER_MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        conn.execute_batch(migration.sql.as_ref()).unwrap();
    }
}

/// Byte image of a freshly migrated database, built once per nextest run of
/// this test binary (once per process outside nextest).
///
/// Every migration statement runs in its own autocommit transaction, so
/// replaying the set against a file-backed database journals and fsyncs
/// hundreds of times per test case. `sqlite3_serialize` yields exactly the
/// bytes SQLite would have written for that same schema, so a case can
/// materialise a real database file from it and keep opening it the normal way.
///
/// nextest runs each case in its own process, so a per-process cache alone
/// would still replay every migration once per case. The schema is a function
/// of this executable — the migrations are embedded in it and SQLite is linked
/// into it — so the first process of a run to build the image leaves a copy
/// beside the executable, and later processes of the same run and build read
/// that copy (see the `test_schema_image_cache` module).
#[cfg(test)]
fn test_schema_image() -> &'static [u8] {
    static IMAGE: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    IMAGE.get_or_init(|| {
        let cache = test_schema_image_cache::SchemaImageCache::for_current_exe();
        if let Some(image) = cache.as_ref().and_then(|cache| cache.read()) {
            return image;
        }
        let mut conn = Connection::open_in_memory().unwrap();
        create_test_schema(&mut conn);
        let image = conn.serialize(rusqlite::MAIN_DB).unwrap().to_vec();
        if let Some(cache) = &cache {
            cache.write(&image);
        }
        image
    })
}

/// Create a file-backed database carrying the migrated schema at `db_path`.
///
/// For tests that need durability across a close and reopen: the file left
/// behind is the one `create_test_schema` would have produced on that path, so
/// the connections the test then opens are ordinary file connections.
#[cfg(test)]
pub(crate) fn create_test_schema_on_disk(db_path: &std::path::Path) {
    std::fs::write(db_path, test_schema_image()).unwrap();
}

/// Fresh SQLite database using the embedded service and common migrations.
/// Upgrade/history-stamping behavior is covered by the migrations crate.
///
/// The schema arrives as a copy of the process-wide image rather than a fresh
/// replay of every migration, which is what makes a test that opens dozens of
/// connections affordable. Nothing else is shared: each call owns its pages,
/// and the per-connection settings are the ones `Connection::open_in_memory`
/// installs, exactly as a replay would leave them.
#[cfg(test)]
pub(crate) fn test_conn() -> Connection {
    let image = test_schema_image();
    let mut conn = Connection::open_in_memory().unwrap();
    conn.deserialize_read_exact(rusqlite::MAIN_DB, image, image.len(), false)
        .unwrap();
    conn
}

/// Seed pre-validation lifecycle evidence without weakening subsequent writes.
/// Transactional DDL also restores the guards if the seed closure panics.
///
/// The validators are read back from this connection rather than reconstructed
/// from a pinned migration, so a later migration that replaces them is restored
/// as it actually exists. A rename is a loud failure here, not a silent
/// reinstatement of a superseded vocabulary in one test's database.
#[cfg(test)]
pub(crate) fn seed_historical_lifecycle_rows<T>(
    conn: &Connection,
    seed: impl FnOnce(&Connection) -> T,
) -> T {
    let tx = conn.unchecked_transaction().unwrap();
    let validators: Vec<(String, String)> = tx
        .prepare(
            "SELECT name, sql FROM sqlite_master
             WHERE type = 'trigger'
               AND tbl_name = 'sandboxes'
               AND name LIKE 'validate_sandbox_lifecycle%'
             ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_>>()
        .unwrap();
    assert!(
        !validators.is_empty(),
        "no lifecycle validators to suspend; a renamed validator must update this helper"
    );
    for (name, _) in &validators {
        tx.execute_batch(&format!("DROP TRIGGER {name}")).unwrap();
    }
    let result = seed(&tx);
    for (_, sql) in &validators {
        tx.execute_batch(sql).unwrap();
    }
    tx.commit().unwrap();
    result
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
mod dirty_outbox_tests;

#[cfg(test)]
mod cleanup_discovery_tests;

#[cfg(test)]
mod discovery_indexes_tests;

#[cfg(test)]
mod lifecycle_validation_tests;
