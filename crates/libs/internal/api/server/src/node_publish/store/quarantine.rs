//! Transaction-scoped PostgreSQL operations for node-authored quarantine recovery.

use pplx_space_common_core::types::NodeId;

use std::sync::LazyLock;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, ExecutionGeneration, LocalArtifactId, PodUid,
    QuarantineRecoveryPlanRejectionCause, SandboxActivationAttemptId, SandboxCleanupEpisodeId,
    SandboxId, SandboxLifecycle, SandboxQuarantineHistoryAction, SandboxQuarantineReasonCode,
    SandboxQuarantineRecoveryPolicy, SandboxQuarantineSource, SandboxTransitionReceiptKind,
    SandboxTransitionTargetState, SnapshotId,
};
use pplx_space_sandbox_portability::{
    current_suspend_origin_marker_snapshot_ready, exact_marker_snapshot_backup_current,
    node_published_suspend_evidence, pre_quarantine_suspend_snapshot_current,
    quarantine_recovery_suspend_snapshot_current, started_suspend_revert_eligibility,
    suspended_projection_lifecycle_eligible_with_node_evidence,
};
use sqlx::types::Json;
use sqlx::{FromRow, PgPool};
use time::OffsetDateTime;
use tonic::Status;
use uuid::Uuid;

#[path = "quarantine_errors.rs"]
mod errors;
pub(super) use errors::{
    ApplyActivationRollbacksError, ConsumeActivationReceiptsError, RecordQuarantineRecoveryError,
    RecoverPauseFailedError, SelectActivationRollbacksError, SelectSuspendedProjectionsError,
};

#[derive(Debug)]
pub(super) struct QuarantineRecoveryCandidate {
    pub(super) quarantine_id: Uuid,
    pub(super) reason_code: SandboxQuarantineReasonCode,
    pub(super) receipt_id: SandboxCleanupEpisodeId,
    pub(super) prior_lifecycle: SandboxLifecycle,
}

#[derive(Debug)]
pub(super) struct SuspendedProjectionCandidate {
    pub(super) sandbox_id: SandboxId,
    pub(super) clear_marker: bool,
    pub(super) published_artifact: Option<PublishedSuspendArtifact>,
    pub(super) published_execution_identity: Option<PublishedExecutionIdentity>,
    pub(super) recovery: Option<QuarantineRecoveryCandidate>,
}

#[derive(Clone, Debug)]
pub(super) struct PublishedSuspendArtifact {
    pub(super) id: LocalArtifactId,
    pub(super) kind: ArtifactKind,
    pub(super) origin: ArtifactOrigin,
}

#[derive(Clone, Debug)]
pub(super) struct PublishedExecutionIdentity {
    pub(super) generation: ExecutionGeneration,
    pub(super) pod_uid: PodUid,
    pub(super) pod_uid_was_missing: bool,
}

#[derive(Debug)]
pub(super) struct PauseFailedQuarantineRecovery {
    pub(super) sandbox_id: SandboxId,
    pub(super) target_state: SandboxTransitionTargetState,
}

#[derive(Debug)]
pub(super) struct ActivationRollbackCandidate {
    pub(super) sandbox_id: SandboxId,
    pub(super) receipt_id: SandboxCleanupEpisodeId,
    pub(super) execution_generation: i64,
    pub(super) activation_attempt_id: SandboxActivationAttemptId,
    pub(super) target_state: SandboxTransitionTargetState,
    pub(super) reason_code: SandboxQuarantineReasonCode,
    pub(super) prior_lifecycle: SandboxLifecycle,
    pub(super) failure_message: Option<String>,
}

#[derive(Debug)]
pub(super) struct QuarantineRecoveryPlanRow {
    pub(super) recovery_id: SandboxCleanupEpisodeId,
    pub(super) execution_generation: i64,
    pub(super) reason_code: SandboxQuarantineReasonCode,
    pub(super) snapshot_id: SnapshotId,
    pub(super) local_artifact_id: Option<LocalArtifactId>,
    pub(super) storage_uri: String,
    pub(super) bytes_uploaded: i64,
}

#[derive(FromRow)]
struct RecoveryAuthorityRow {
    quarantine_id: Uuid,
    reason_code: String,
    entered_at: OffsetDateTime,
    active_node_id: Option<NodeId>,
    execution_generation: Option<i64>,
    current_local_artifact_id: Option<LocalArtifactId>,
    current_local_artifact_kind: Option<String>,
    current_local_artifact_origin: Option<String>,
}

#[derive(FromRow)]
struct RecoverySnapshotRow {
    snapshot_id: SnapshotId,
    local_artifact_id: Option<LocalArtifactId>,
    storage_uri: String,
    size_bytes: Option<i64>,
}

fn reject_quarantine_recovery_plan(
    sandbox_id: SandboxId,
    node_id: NodeId,
    cause: QuarantineRecoveryPlanRejectionCause,
    message: String,
) -> Status {
    pplx_space_metrics::internal_api::quarantine_recovery_plan_rejected(cause);
    tracing::info!(
        %sandbox_id,
        %node_id,
        %cause,
        "quarantine recovery plan rejected"
    );
    Status::failed_precondition(message)
}

/// The exact-artifact lookup: the snapshot must name the artifact the node holds.
static EXACT_ARTIFACT_SNAPSHOT_QUERY: LazyLock<String> = LazyLock::new(|| {
    let current = quarantine_recovery_suspend_snapshot_current("current_sandbox", "snapshot");
    sql!(format!(
        "SELECT snapshot.id AS snapshot_id,
                snapshot.local_artifact_id,
                location.storage_connection_string AS storage_uri,
                snapshot.size_bytes
           FROM snapshots snapshot
           JOIN snapshot_storage_locations location
             ON location.snapshot_id = snapshot.id
           JOIN sandboxes current_sandbox
             ON current_sandbox.id = snapshot.sandbox_id
          WHERE current_sandbox.id = $1
            AND snapshot.local_artifact_id = $2 AND {current}
          ORDER BY snapshot.timestamp DESC
          LIMIT 1"
    ))
});

/// The pre-quarantine fallback: the newest suspend snapshot from before the
/// episode was entered, which only `OrphanedSandboxRowMissing`'s policy admits.
static PRE_QUARANTINE_SNAPSHOT_QUERY: LazyLock<String> = LazyLock::new(|| {
    let current = pre_quarantine_suspend_snapshot_current("current_sandbox", "snapshot", "$2");
    sql!(format!(
        "SELECT snapshot.id AS snapshot_id,
                snapshot.local_artifact_id,
                location.storage_connection_string AS storage_uri,
                snapshot.size_bytes
           FROM snapshots snapshot
           JOIN snapshot_storage_locations location
             ON location.snapshot_id = snapshot.id
           JOIN sandboxes current_sandbox
             ON current_sandbox.id = snapshot.sandbox_id
          WHERE current_sandbox.id = $1
            AND {current}
          ORDER BY snapshot.timestamp DESC
          LIMIT 1"
    ))
});

/// The one message every rejection that turns on node ownership returns.
///
/// The lookup is deliberately not scoped to the requesting node, so the cause
/// counter and the log can say whether the sandbox has no quarantine at all, no
/// active node, or an active node that is someone else. Returning those three
/// apart on the wire would make this endpoint an oracle: a node walking sandbox
/// ids would learn which ones exist, are quarantined out of `restoring`, and sit
/// on another node. The metric carries the distinction; the caller does not.
///
/// Every rejection *after* these three may carry its own message, and that is safe
/// only because these run first: a caller that is not the owning node never
/// reaches them, so `no_active_execution`, `unsupported_reason`, and the rest are
/// only ever seen by a node that already proved ownership of the sandbox. Moving
/// any of them above the ownership check turns that message into the oracle this
/// helper exists to close.
fn no_recovery_authority_message(sandbox_id: SandboxId) -> String {
    format!("sandbox {sandbox_id} has no active quarantine on this node")
}

pub(super) async fn get_quarantine_recovery_plan(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> Result<QuarantineRecoveryPlanRow, Status> {
    let recovery: RecoveryAuthorityRow = sqlx::query_as(sql!(
        "SELECT quarantine.quarantine_id,
                quarantine.reason_code,
                quarantine.entered_at,
                sandbox.active_node_id,
                execution.generation AS execution_generation,
                sandbox.current_local_artifact_id,
                sandbox.current_local_artifact_kind,
                sandbox.current_local_artifact_origin
           FROM sandboxes sandbox
           JOIN sandbox_quarantine_state quarantine
             ON quarantine.sandbox_id = sandbox.id
           LEFT JOIN sandbox_k8s_executions execution
             ON execution.sandbox_id = sandbox.id AND execution.active
          WHERE sandbox.id = $1
            AND sandbox.lifecycle = 'quarantined'
            AND quarantine.source = $2
            AND quarantine.entered_from_lifecycle = 'restoring'"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(SandboxQuarantineSource::SbxManager.as_str())
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("quarantine recovery lookup: {error}")))?
    .ok_or_else(|| {
        reject_quarantine_recovery_plan(
            sandbox_id,
            node_id,
            QuarantineRecoveryPlanRejectionCause::NoActiveQuarantine,
            no_recovery_authority_message(sandbox_id),
        )
    })?;

    match recovery.active_node_id {
        None => {
            return Err(reject_quarantine_recovery_plan(
                sandbox_id,
                node_id,
                QuarantineRecoveryPlanRejectionCause::NoActiveNode,
                no_recovery_authority_message(sandbox_id),
            ));
        }
        Some(active_node_id) if active_node_id != node_id => {
            return Err(reject_quarantine_recovery_plan(
                sandbox_id,
                node_id,
                QuarantineRecoveryPlanRejectionCause::WrongNode,
                no_recovery_authority_message(sandbox_id),
            ));
        }
        Some(_) => {}
    }
    let execution_generation = recovery.execution_generation.ok_or_else(|| {
        reject_quarantine_recovery_plan(
            sandbox_id,
            node_id,
            QuarantineRecoveryPlanRejectionCause::NoActiveExecution,
            format!("sandbox {sandbox_id} has no active execution"),
        )
    })?;

    let reason_code: SandboxQuarantineReasonCode =
        recovery.reason_code.parse().map_err(|error| {
            reject_quarantine_recovery_plan(
                sandbox_id,
                node_id,
                QuarantineRecoveryPlanRejectionCause::UnsupportedReason,
                format!("sandbox {sandbox_id} has unsupported quarantine reason: {error}"),
            )
        })?;
    let policy = reason_code.automatic_recovery_policy().ok_or_else(|| {
        reject_quarantine_recovery_plan(
            sandbox_id,
            node_id,
            QuarantineRecoveryPlanRejectionCause::NotAutomaticallyRecoverable,
            format!("quarantine reason {reason_code} is not automatically recoverable"),
        )
    })?;
    let exact_artifact = match (
        recovery.current_local_artifact_id,
        recovery.current_local_artifact_kind.as_deref(),
        recovery.current_local_artifact_origin.as_deref(),
    ) {
        (Some(id), Some("full"), Some("suspend")) => Some(id),
        _ => None,
    };
    let snapshot: Option<RecoverySnapshotRow> = match (policy, exact_artifact) {
        (
            SandboxQuarantineRecoveryPolicy::ExactSuspendArtifact
            | SandboxQuarantineRecoveryPolicy::ExactOrPreQuarantineSuspendArtifact,
            Some(local_artifact_id),
        ) => {
            sqlx::query_as(sqlx::AssertSqlSafe(EXACT_ARTIFACT_SNAPSHOT_QUERY.as_str()))
                .bind(sandbox_id.as_uuid())
                .bind(local_artifact_id)
                .fetch_optional(pool)
                .await
        }
        (SandboxQuarantineRecoveryPolicy::ExactSuspendArtifact, None) => {
            return Err(reject_quarantine_recovery_plan(
                sandbox_id,
                node_id,
                QuarantineRecoveryPlanRejectionCause::NoExactSuspendArtifact,
                format!("sandbox {sandbox_id} has no exact suspend artifact identity"),
            ));
        }
        (SandboxQuarantineRecoveryPolicy::ExactOrPreQuarantineSuspendArtifact, None) => {
            sqlx::query_as(sqlx::AssertSqlSafe(PRE_QUARANTINE_SNAPSHOT_QUERY.as_str()))
                .bind(sandbox_id.as_uuid())
                .bind(recovery.entered_at)
                .fetch_optional(pool)
                .await
        }
    }
    .map_err(|error| Status::internal(format!("recovery snapshot lookup: {error}")))?;
    let snapshot = snapshot.ok_or_else(|| {
        reject_quarantine_recovery_plan(
            sandbox_id,
            node_id,
            QuarantineRecoveryPlanRejectionCause::NoCompatibleSnapshot,
            format!("sandbox {sandbox_id} has no recovery-policy-compatible suspend artifact"),
        )
    })?;

    Ok(QuarantineRecoveryPlanRow {
        recovery_id: SandboxCleanupEpisodeId::from_uuid(recovery.quarantine_id),
        execution_generation,
        reason_code,
        snapshot_id: snapshot.snapshot_id,
        local_artifact_id: snapshot.local_artifact_id,
        storage_uri: snapshot.storage_uri,
        bytes_uploaded: snapshot.size_bytes.unwrap_or(0),
    })
}

/// Acknowledge node-completed activations by releasing only the exact active
/// execution attempt that produced the receipt. This is the durable counterpart
/// to the API handler's best-effort fast-path release. If a newer suspend owns
/// the lifecycle, the exact activation attempt is still released, but its stale
/// target lifecycle is not projected onto the sandbox.
/// Route-less legacy residents retain ownership until post-ack route publication:
/// clearing it earlier would make that publication CAS permanently lose its fence.
pub(super) async fn consume_activation_completion_receipts(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows_json: Json<serde_json::Value>,
) -> Result<Vec<SandboxId>, ConsumeActivationReceiptsError> {
    sqlx::query_scalar(sql!(
        "WITH input AS (
             SELECT sandbox_id, status, transition_receipt_id, transition_receipt_kind,
                    transition_receipt_target_state, transition_receipt_execution_generation,
                    transition_receipt_reason_code, transition_receipt_recovery_policy,
                    transition_receipt_activation_attempt_id, failure_reason,
                    current_local_artifact_id, current_local_artifact_kind,
                    current_local_artifact_origin, execution_generation, pod_uid
               FROM jsonb_to_recordset($2::jsonb) AS published(
                   sandbox_id UUID,
                   status TEXT,
                   transition_receipt_id UUID,
                   transition_receipt_kind TEXT,
                   transition_receipt_target_state TEXT,
                   transition_receipt_execution_generation BIGINT,
                   transition_receipt_reason_code TEXT,
                   transition_receipt_recovery_policy TEXT,
                   transition_receipt_activation_attempt_id UUID,
                   failure_reason TEXT,
                   current_local_artifact_id UUID,
                   current_local_artifact_kind TEXT,
                   current_local_artifact_origin TEXT,
                   execution_generation BIGINT,
                   pod_uid TEXT
               )
         ), released AS (
             UPDATE sandbox_k8s_executions execution
                SET activation_attempt_id = NULL,
                    activation_attempt_expires_at = NULL,
                    activation_attempt_target_state = NULL,
                    updated_at = NOW()
               FROM sandboxes sandbox, input
              WHERE sandbox.id = execution.sandbox_id
                AND input.sandbox_id = sandbox.id
                AND sandbox.active_node_id = $1
                AND execution.active
                AND execution.generation = input.transition_receipt_execution_generation
                AND execution.activation_attempt_id = input.transition_receipt_activation_attempt_id
                AND execution.route_activation_id IS NOT NULL
                AND execution.activation_attempt_target_state = input.transition_receipt_target_state
                AND input.transition_receipt_kind = $3
                AND LOWER(TRIM(input.status)) = input.transition_receipt_target_state
                AND (
                    (
                        input.transition_receipt_target_state = 'created'
                        AND LOWER(TRIM(sandbox.lifecycle)) = 'created'
                    )
                    OR (
                        input.transition_receipt_target_state IN ('started', 'paused')
                        AND execution.state = 'running'
                        AND execution.pod_ip IS NOT NULL
                    )
                )
              RETURNING execution.sandbox_id, input.transition_receipt_target_state
         )
         UPDATE sandboxes sandbox
            SET lifecycle = released.transition_receipt_target_state,
                placement = CASE
                    WHEN released.transition_receipt_target_state IN ('started', 'paused')
                    THEN 'on_node'
                    ELSE sandbox.placement
                END
           FROM released
          WHERE sandbox.id = released.sandbox_id
            AND LOWER(TRIM(sandbox.lifecycle)) NOT IN (
                'suspending', 'suspended', 'quarantined', 'deleting', 'deleted', 'cleanup_required'
            )
         RETURNING sandbox.id"
    ))
        .bind(node_id)
        .bind(rows_json)
        .bind(SandboxTransitionReceiptKind::ActivationCompleted.as_str())
        .fetch_all(conn)
        .await
        .map_err(ConsumeActivationReceiptsError::Consume)
}

/// Lock only rollback receipts that still own the active central execution.
/// Both execution and activation-attempt identities must match. Execution
/// generations are deliberately reused by retries, so generation alone is not
/// a freshness fence.
pub(super) async fn select_activation_rollback_candidates(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows_json: Json<serde_json::Value>,
) -> Result<Vec<ActivationRollbackCandidate>, SelectActivationRollbacksError> {
    type CandidateRow = (
        SandboxId,
        SandboxCleanupEpisodeId,
        i64,
        SandboxActivationAttemptId,
        String,
        String,
        SandboxLifecycle,
        Option<String>,
    );
    let rows: Vec<CandidateRow> = sqlx::query_as(sql!(
        "SELECT s.id,
                input.transition_receipt_id,
                input.transition_receipt_execution_generation,
                input.transition_receipt_activation_attempt_id,
                input.transition_receipt_target_state,
                input.transition_receipt_reason_code,
                LOWER(TRIM(s.lifecycle)),
                input.failure_reason
           FROM sandboxes s
           JOIN jsonb_to_recordset($2::jsonb) AS input(
               sandbox_id UUID,
               status TEXT,
               transition_receipt_id UUID,
               transition_receipt_kind TEXT,
               transition_receipt_target_state TEXT,
               transition_receipt_execution_generation BIGINT,
               transition_receipt_reason_code TEXT,
               transition_receipt_recovery_policy TEXT,
               transition_receipt_activation_attempt_id UUID,
               failure_reason TEXT,
               current_local_artifact_id UUID,
               current_local_artifact_kind TEXT,
               current_local_artifact_origin TEXT,
               execution_generation BIGINT,
               pod_uid TEXT
           )
             ON input.sandbox_id = s.id
           JOIN sandbox_k8s_executions execution
             ON execution.sandbox_id = s.id
            AND execution.active
            AND execution.generation = input.transition_receipt_execution_generation
            AND execution.activation_attempt_id = input.transition_receipt_activation_attempt_id
          WHERE s.active_node_id = $1
            AND input.transition_receipt_kind = $3
            AND LOWER(TRIM(input.status)) = input.transition_receipt_target_state
            AND input.transition_receipt_target_state IN ('created', 'stopped', 'suspended')
            AND LOWER(TRIM(s.lifecycle)) NOT IN (
                'quarantined', 'deleted', 'deleting', 'cleanup_required'
            )
          FOR UPDATE OF s, execution"
    ))
    .bind(node_id)
    .bind(rows_json)
    .bind(SandboxTransitionReceiptKind::ActivationRollback.as_str())
    .fetch_all(&mut *conn)
    .await
    .map_err(SelectActivationRollbacksError::Select)?;

    rows.into_iter()
        .map(
            |(
                sandbox_id,
                receipt_id,
                execution_generation,
                activation_attempt_id,
                target_state,
                reason_code,
                prior_lifecycle,
                failure_message,
            )| {
                Ok(ActivationRollbackCandidate {
                    sandbox_id,
                    receipt_id,
                    execution_generation,
                    activation_attempt_id,
                    target_state: target_state.parse().map_err(|source| {
                        SelectActivationRollbacksError::Target { sandbox_id, source }
                    })?,
                    reason_code: reason_code.parse().map_err(|source| {
                        SelectActivationRollbacksError::Reason { sandbox_id, source }
                    })?,
                    prior_lifecycle,
                    failure_message,
                })
            },
        )
        .collect()
}

/// Consume rollback receipts after their generation fence has been locked.
/// Each statement is intentionally explicit: execution retirement, port
/// invalidation, and lifecycle convergence share the caller's transaction and
/// any invariant failure rolls all of them back.
pub(super) async fn apply_activation_rollback_candidates(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    candidates: Vec<ActivationRollbackCandidate>,
) -> Result<Vec<ActivationRollbackCandidate>, ApplyActivationRollbacksError> {
    let mut applied = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let execution_state = if candidate.target_state == SandboxTransitionTargetState::Suspended {
            "suspended"
        } else {
            "replaced"
        };
        let retired = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET active = FALSE,
                    state = $3,
                    pod_ip = NULL,
                    terminated_at = COALESCE(terminated_at, NOW()),
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND generation = $2
                AND activation_attempt_id = $4
                AND active"
        ))
        .bind(candidate.sandbox_id)
        .bind(candidate.execution_generation)
        .bind(execution_state)
        .bind(candidate.activation_attempt_id)
        .execute(&mut *conn)
        .await
        .map_err(ApplyActivationRollbacksError::RetireExecution)?;
        if retired.rows_affected() == 0 {
            continue;
        }

        sqlx::query(sql!(
            "UPDATE sandbox_port_exposures
                SET apply_state = 'pending',
                    last_applied_node_id = NULL,
                    last_error = NULL,
                    last_bridge_status = NULL,
                    last_applied_bridge_generation = NULL,
                    last_observed_at = NULL,
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND desired_state = 'open'"
        ))
        .bind(candidate.sandbox_id)
        .execute(&mut *conn)
        .await
        .map_err(ApplyActivationRollbacksError::InvalidatePorts)?;
        let converged = sqlx::query(sql!(
            "UPDATE sandboxes
                SET lifecycle = $3,
                    placement = 'unplaced',
                    target_node_id = COALESCE(target_node_id, active_node_id),
                    active_node_id = NULL,
                    command_ready_pending = FALSE,
                    lifecycle_changed_at = NOW()
              WHERE id = $1
                AND active_node_id = $2
                AND lifecycle NOT IN ('quarantined', 'deleted', 'deleting', 'cleanup_required')"
        ))
        .bind(candidate.sandbox_id)
        .bind(node_id)
        .bind(candidate.target_state.as_str())
        .execute(&mut *conn)
        .await
        .map_err(ApplyActivationRollbacksError::Converge)?;
        if converged.rows_affected() != 1 {
            return Err(ApplyActivationRollbacksError::LostOwnership(
                candidate.receipt_id,
            ));
        }
        applied.push(candidate);
    }
    Ok(applied)
}

/// Clear an sbx-manager-authored pause failure after the same node publishes a
/// definitive healthy runtime state. The caller's publication transaction
/// keeps the selected sandbox and quarantine rows locked through retirement.
pub(super) async fn recover_pause_failed_from_inventory(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows_json: Json<serde_json::Value>,
) -> Result<Vec<PauseFailedQuarantineRecovery>, RecoverPauseFailedError> {
    let quarantine_source = SandboxQuarantineSource::SbxManager.as_str();
    let quarantine_reason = SandboxQuarantineReasonCode::PauseFailed.as_str();
    let candidates: Vec<(Uuid, Uuid, String)> = sqlx::query_as(sql!(
        "SELECT s.id, qs.quarantine_id, LOWER(TRIM(input.status))
           FROM sandboxes s
           JOIN jsonb_to_recordset($2::jsonb) AS input(
               sandbox_id UUID,
               status TEXT
           ) ON input.sandbox_id = s.id
           JOIN sandbox_quarantine_state qs ON qs.sandbox_id = s.id
          WHERE s.active_node_id = $1
            AND LOWER(TRIM(s.lifecycle)) = 'quarantined'
            AND LOWER(TRIM(input.status)) IN ('started', 'paused')
            AND qs.source = $3
            AND qs.reason_code = $4
          FOR UPDATE OF s, qs"
    ))
    .bind(node_id)
    .bind(rows_json)
    .bind(quarantine_source)
    .bind(quarantine_reason)
    .fetch_all(&mut *conn)
    .await
    .map_err(RecoverPauseFailedError::Select)?;

    let mut recoveries = Vec::with_capacity(candidates.len());
    for (sandbox_id, quarantine_id, target_state) in candidates {
        let target_state =
            target_state
                .parse()
                .map_err(|source| RecoverPauseFailedError::Target {
                    sandbox_id: SandboxId::from_uuid(sandbox_id),
                    source,
                })?;
        let action = match target_state {
            SandboxTransitionTargetState::Started => {
                SandboxQuarantineHistoryAction::RecoveredToStarted
            }
            SandboxTransitionTargetState::Paused => {
                SandboxQuarantineHistoryAction::RecoveredToPaused
            }
            _ => {
                return Err(RecoverPauseFailedError::UnsupportedTarget(target_state));
            }
        };
        let recovered = sqlx::query_scalar::<_, Uuid>(sql!(
            "WITH recovered AS (
                 UPDATE sandboxes
                    SET lifecycle = $1
                  WHERE id = $2
                    AND active_node_id = $3
                    AND lifecycle = 'quarantined'
                    AND EXISTS (
                        SELECT 1
                          FROM sandbox_quarantine_state
                         WHERE sandbox_id = $2
                           AND quarantine_id = $4
                           AND source = $5
                           AND reason_code = $6
                    )
                 RETURNING id
             ),
             observed AS (
                 UPDATE sandbox_observed_states observed
                    SET failure_reason = NULL
                   FROM recovered
                  WHERE observed.sandbox_id = recovered.id
                    AND observed.node_id = $3
                 RETURNING observed.sandbox_id
             ),
             recorded AS (
                 INSERT INTO sandbox_quarantine_history (
                     id, quarantine_id, sandbox_id, occurred_at, source, action,
                     reason_code, message, next_action, attempt_count, evidence
                 )
                 SELECT $7, quarantine.quarantine_id, quarantine.sandbox_id,
                        NOW(), $8, $9, quarantine.reason_code,
                        'node inventory reported a definitive healthy runtime state',
                        NULL, quarantine.attempt_count,
                        quarantine.evidence
                            || jsonb_build_object('recovered_by_node_id', $10::TEXT)
                   FROM sandbox_quarantine_state quarantine
                   JOIN observed ON observed.sandbox_id = quarantine.sandbox_id
                  WHERE quarantine.sandbox_id = $2
                    AND quarantine.quarantine_id = $4
                    AND quarantine.source = $5
                    AND quarantine.reason_code = $6
                 RETURNING sandbox_id, quarantine_id
             ),
             retired AS (
                 DELETE FROM sandbox_quarantine_state quarantine
                  USING recorded
                  WHERE quarantine.sandbox_id = recorded.sandbox_id
                    AND quarantine.quarantine_id = recorded.quarantine_id
                    AND quarantine.source = $5
                    AND quarantine.reason_code = $6
                 RETURNING quarantine.sandbox_id
             )
             SELECT sandbox_id FROM retired"
        ))
        .bind(target_state.as_str())
        .bind(sandbox_id)
        .bind(node_id)
        .bind(quarantine_id)
        .bind(quarantine_source)
        .bind(quarantine_reason)
        .bind(Uuid::now_v7())
        .bind(SandboxQuarantineSource::NodeStatePublish.as_str())
        .bind(action.as_str())
        .bind(node_id.to_string())
        .fetch_optional(&mut *conn)
        .await
        .map_err(RecoverPauseFailedError::Record)?;
        if recovered != Some(sandbox_id) {
            return Err(RecoverPauseFailedError::LostOwnership(
                SandboxId::from_uuid(sandbox_id),
            ));
        }

        recoveries.push(PauseFailedQuarantineRecovery {
            sandbox_id: SandboxId::from_uuid(sandbox_id),
            target_state,
        });
    }
    Ok(recoveries)
}

// Keep portability predicates shared, with the complete locking statement built
// once. Every publication still evaluates those predicates against its own rows.
static SELECT_SUSPENDED_PROJECTIONS: LazyLock<String> = LazyLock::new(|| {
    let eligibility = started_suspend_revert_eligibility("s");
    let clear_marker = eligibility.clear_marker;
    let eligible = eligibility.eligible;
    // Recovery may retire only the active execution whose generation the node
    // cleaned. The caller holds the sandbox row lock through finalization.
    let recovery_marker_current = exact_marker_snapshot_backup_current("s");
    let pre_quarantine_snapshot_current =
        pre_quarantine_suspend_snapshot_current("s", "recovered_snapshot", "qs.entered_at");
    let committed_suspend_marker_current = current_suspend_origin_marker_snapshot_ready("s");
    let committed_suspend_evidence = format!(
        "input.current_local_artifact_id = s.current_local_artifact_id\n\
         AND input.current_local_artifact_kind = s.current_local_artifact_kind\n\
         AND input.current_local_artifact_origin = s.current_local_artifact_origin\n\
         AND input.current_local_artifact_origin = 'suspend'\n\
         AND ({committed_suspend_marker_current})"
    );
    let node_suspend_evidence = node_published_suspend_evidence("s", "input");
    let lifecycle_eligible = suspended_projection_lifecycle_eligible_with_node_evidence(
        "s",
        &committed_suspend_evidence,
        Some(&node_suspend_evidence),
    );
    sql!(format!(
        "WITH input AS (
             SELECT published.sandbox_id, published.status, published.transition_receipt_id,
                    published.transition_receipt_kind, published.transition_receipt_target_state,
                    published.transition_receipt_execution_generation,
                    published.transition_receipt_reason_code,
                    published.transition_receipt_recovery_policy,
                    published.transition_receipt_activation_attempt_id, published.failure_reason,
                    published.current_local_artifact_id, published.current_local_artifact_kind,
                    published.current_local_artifact_origin, published.execution_generation,
                    published.pod_uid,
                    published.transition_receipt_kind = $3
                    AND published.transition_receipt_id IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1
                          FROM sandbox_quarantine_history consumed
                         WHERE consumed.id = published.transition_receipt_id
                    ) AS has_unconsumed_recovery_receipt
               FROM jsonb_to_recordset($2::jsonb) AS published(
                   sandbox_id UUID,
                   status TEXT,
                   transition_receipt_id UUID,
                   transition_receipt_kind TEXT,
                   transition_receipt_target_state TEXT,
                   transition_receipt_execution_generation BIGINT,
                   transition_receipt_reason_code TEXT,
                   transition_receipt_recovery_policy TEXT,
                   transition_receipt_activation_attempt_id UUID,
                   failure_reason TEXT,
                   current_local_artifact_id UUID,
                   current_local_artifact_kind TEXT,
                   current_local_artifact_origin TEXT,
                   execution_generation BIGINT,
                   pod_uid TEXT
               )
         )
         SELECT s.id,
                CASE WHEN LOWER(TRIM(s.lifecycle)) = 'quarantined'
                     THEN FALSE ELSE {clear_marker} END AS clear_marker,
                CASE WHEN LOWER(TRIM(s.lifecycle)) = 'quarantined'
                     THEN qs.quarantine_id END AS quarantine_id,
                CASE WHEN input.has_unconsumed_recovery_receipt
                          AND LOWER(TRIM(s.lifecycle)) IN ('quarantined', 'restoring')
                     THEN input.transition_receipt_reason_code END AS reason_code,
                CASE WHEN input.has_unconsumed_recovery_receipt
                          AND LOWER(TRIM(s.lifecycle)) IN ('quarantined', 'restoring')
                     THEN input.transition_receipt_id END AS receipt_id,
                LOWER(TRIM(s.lifecycle)) AS prior_lifecycle,
                CASE WHEN ({node_suspend_evidence})
                     THEN input.current_local_artifact_id END AS published_artifact_id,
                CASE WHEN ({node_suspend_evidence})
                     THEN input.current_local_artifact_kind END AS published_artifact_kind,
                CASE WHEN ({node_suspend_evidence})
                     THEN input.current_local_artifact_origin END AS published_artifact_origin,
                CASE WHEN ({node_suspend_evidence})
                     THEN input.execution_generation END AS published_execution_generation,
                CASE WHEN ({node_suspend_evidence})
                     THEN input.pod_uid END AS published_pod_uid,
                CASE WHEN ({node_suspend_evidence})
                     THEN EXISTS (
                         SELECT 1
                           FROM sandbox_k8s_executions execution
                          WHERE execution.sandbox_id = s.id
                            AND execution.active
                            AND execution.state = 'running'
                            AND execution.generation = input.execution_generation
                            AND execution.pod_uid IS NULL
                     ) END AS published_pod_uid_was_missing
           FROM sandboxes s
           JOIN input ON input.sandbox_id = s.id
           LEFT JOIN sandbox_quarantine_state qs ON qs.sandbox_id = s.id
          WHERE s.active_node_id = $1
            AND LOWER(TRIM(input.status)) = 'suspended'
            AND (
                (
                    {lifecycle_eligible}
                    AND (
                        NOT input.has_unconsumed_recovery_receipt
                        OR LOWER(TRIM(s.lifecycle)) != 'restoring'
                    )
                    AND {eligible}
                )
                OR (
                    input.has_unconsumed_recovery_receipt
                    AND input.transition_receipt_kind = $3
                    AND input.transition_receipt_target_state = 'suspended'
                    AND input.transition_receipt_recovery_policy IN ($5, $6)
                    AND input.transition_receipt_execution_generation IS NOT NULL
                    AND LOWER(TRIM(s.lifecycle)) = 'quarantined'
                    AND qs.quarantine_id = input.transition_receipt_id
                    AND qs.source = $4
                    AND qs.reason_code = input.transition_receipt_reason_code
                    AND qs.entered_from_lifecycle = 'restoring'
                    AND EXISTS (
                        SELECT 1
                          FROM sandbox_k8s_executions execution
                         WHERE execution.sandbox_id = s.id
                           AND execution.active
                           AND execution.generation = input.transition_receipt_execution_generation
                    )
                    AND (
                        (
                            s.current_local_artifact_id = input.current_local_artifact_id
                            AND {recovery_marker_current}
                        )
                        OR (
                            input.transition_receipt_recovery_policy = $6
                            AND s.current_local_artifact_id IS NULL
                            AND (
                                (
                                    input.current_local_artifact_id IS NOT NULL
                                    AND EXISTS (
                                        SELECT 1
                                          FROM snapshots recovered_snapshot
                                          JOIN snapshot_storage_locations recovered_location
                                            ON recovered_location.snapshot_id = recovered_snapshot.id
                                         WHERE recovered_snapshot.sandbox_id = s.id
                                           AND recovered_snapshot.local_artifact_id = input.current_local_artifact_id
                                           AND {pre_quarantine_snapshot_current}
                                    )
                                )
                                OR (
                                    input.current_local_artifact_id IS NULL
                                    AND EXISTS (
                                        SELECT 1
                                          FROM snapshots recovered_snapshot
                                          JOIN snapshot_storage_locations recovered_location
                                            ON recovered_location.snapshot_id = recovered_snapshot.id
                                         WHERE recovered_snapshot.sandbox_id = s.id
                                           AND recovered_snapshot.local_artifact_id IS NULL
                                           AND {pre_quarantine_snapshot_current}
                                    )
                                )
                            )
                        )
                    )
                )
            )
          FOR UPDATE OF s",
    ))
});

/// Select node-published suspended projections and generation-fenced recovery
/// receipts owned by this node. Lock qualifying sandbox rows through finalization;
/// an empty result is a normal no-match outcome, not a failed recovery.
pub(super) async fn select_suspended_projection_candidates(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows_json: Json<serde_json::Value>,
) -> Result<Vec<SuspendedProjectionCandidate>, SelectSuspendedProjectionsError> {
    type CandidateRow = (
        SandboxId,
        bool,
        Option<Uuid>,
        Option<String>,
        Option<SandboxCleanupEpisodeId>,
        SandboxLifecycle,
        Option<Uuid>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<bool>,
    );
    let rows: Vec<CandidateRow> =
        sqlx::query_as(sqlx::AssertSqlSafe(SELECT_SUSPENDED_PROJECTIONS.as_str()))
            .bind(node_id)
            .bind(rows_json)
            .bind(SandboxTransitionReceiptKind::QuarantineRecovery.as_str())
            .bind(SandboxQuarantineSource::SbxManager.as_str())
            .bind(SandboxQuarantineRecoveryPolicy::ExactSuspendArtifact.as_str())
            .bind(SandboxQuarantineRecoveryPolicy::ExactOrPreQuarantineSuspendArtifact.as_str())
            .fetch_all(&mut *conn)
            .await
            .map_err(SelectSuspendedProjectionsError::Select)?;

    rows.into_iter()
        .map(
            |(
                sandbox_id,
                clear_marker,
                quarantine_id,
                reason,
                receipt_id,
                prior_lifecycle,
                published_artifact_id,
                published_artifact_kind,
                published_artifact_origin,
                published_execution_generation,
                published_pod_uid,
                published_pod_uid_was_missing,
            )| {
                let published_artifact = match (
                    published_artifact_id,
                    published_artifact_kind,
                    published_artifact_origin,
                ) {
                    (None, None, None) => None,
                    (Some(id), Some(kind), Some(origin)) => Some(PublishedSuspendArtifact {
                        id: LocalArtifactId::from_uuid(id),
                        kind: kind.parse().map_err(|source| {
                            SelectSuspendedProjectionsError::ArtifactKind { sandbox_id, source }
                        })?,
                        origin: origin.parse().map_err(|source| {
                            SelectSuspendedProjectionsError::ArtifactOrigin { sandbox_id, source }
                        })?,
                    }),
                    _ => {
                        return Err(SelectSuspendedProjectionsError::PartialArtifact(sandbox_id));
                    }
                };
                let published_execution_identity = match (
                    published_execution_generation,
                    published_pod_uid,
                    published_pod_uid_was_missing,
                ) {
                    (None, None, None) => None,
                    (Some(generation), Some(pod_uid), Some(pod_uid_was_missing)) => {
                        Some(PublishedExecutionIdentity {
                            generation: ExecutionGeneration::new(generation).map_err(|source| {
                                SelectSuspendedProjectionsError::Generation { sandbox_id, source }
                            })?,
                            pod_uid: PodUid::parse(pod_uid).map_err(|source| {
                                SelectSuspendedProjectionsError::PodUid { sandbox_id, source }
                            })?,
                            pod_uid_was_missing,
                        })
                    }
                    _ => {
                        return Err(SelectSuspendedProjectionsError::PartialExecutionIdentity(
                            sandbox_id,
                        ));
                    }
                };
                let recovery = match (reason, receipt_id) {
                    (None, None) => None,
                    (Some(reason), Some(receipt_id)) => Some(QuarantineRecoveryCandidate {
                        quarantine_id: quarantine_id
                            .ok_or(SelectSuspendedProjectionsError::MissingEpisode(sandbox_id))?,
                        reason_code: reason.parse().map_err(|source| {
                            SelectSuspendedProjectionsError::RecoveryReason { sandbox_id, source }
                        })?,
                        receipt_id,
                        prior_lifecycle,
                    }),
                    _ => {
                        return Err(SelectSuspendedProjectionsError::PartialRecoveryReceipt(
                            sandbox_id,
                        ));
                    }
                };
                Ok(SuspendedProjectionCandidate {
                    sandbox_id,
                    clear_marker,
                    published_artifact,
                    published_execution_identity,
                    recovery,
                })
            },
        )
        .collect()
}

/// Record a receipt only after finalization succeeded, then retire the exact
/// central quarantine episode that authorized the node-local recovery.
pub(super) async fn record_quarantine_recovery(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    sandbox_id: SandboxId,
    recovery: &QuarantineRecoveryCandidate,
) -> Result<(), RecordQuarantineRecoveryError> {
    let history_id = recovery.receipt_id.as_uuid();
    let quarantine_id = recovery.quarantine_id;
    let inserted = sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_history (
                 id, quarantine_id, sandbox_id, occurred_at, source, action,
                 reason_code, message, next_action, attempt_count, evidence
             )
             SELECT $1, qs.quarantine_id, qs.sandbox_id, NOW(), $2, $3,
                    qs.reason_code,
                    'node validated its canonical suspend artifact after runtime cleanup',
                    NULL, qs.attempt_count,
                    qs.evidence || jsonb_build_object('recovered_by_node_id', $4::TEXT)
               FROM sandbox_quarantine_state qs
              WHERE qs.sandbox_id = $5
                AND qs.quarantine_id = $6
                AND qs.source = $8
                AND qs.reason_code = $7"
    ))
    .bind(history_id)
    .bind(SandboxQuarantineSource::NodeStatePublish.as_str())
    .bind(SandboxQuarantineHistoryAction::RecoveredToSuspended.as_str())
    .bind(node_id.to_string())
    .bind(sandbox_id)
    .bind(quarantine_id)
    .bind(recovery.reason_code.as_str())
    .bind(SandboxQuarantineSource::SbxManager.as_str())
    .execute(&mut *conn)
    .await
    .map_err(RecordQuarantineRecoveryError::Record)?
    .rows_affected();
    if inserted != 1 {
        return Err(RecordQuarantineRecoveryError::HistoryCas {
            sandbox_id,
            rows: inserted,
        });
    }
    let removed = sqlx::query(sql!(
        "DELETE FROM sandbox_quarantine_state
              WHERE sandbox_id = $1 AND quarantine_id = $2"
    ))
    .bind(sandbox_id)
    .bind(quarantine_id)
    .execute(&mut *conn)
    .await
    .map_err(RecordQuarantineRecoveryError::Retire)?
    .rows_affected();
    if removed != 1 {
        return Err(RecordQuarantineRecoveryError::RetirementCas {
            sandbox_id,
            rows: removed,
        });
    }
    Ok(())
}
