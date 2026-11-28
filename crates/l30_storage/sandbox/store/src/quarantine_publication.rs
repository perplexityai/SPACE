//! The internal API's locks of the `sandboxes` rows a node's state
//! publication recovers or projects, taken in the publication's transaction:
//! the rows (and their `sandbox_quarantine_state` episodes) leaving an
//! sbx-manager pause-failed quarantine, and the rows taking a published
//! `suspended` projection or quarantine-recovery receipt.
//!
//! The caller keeps its publication transaction and the checks it runs on
//! each locked row, and decides on and issues the recovery and projection
//! writes, such as [`crate::ingest::recover_published_pause_failed`] and
//! [`crate::ingest::finalize_suspended`].

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::{
    SandboxCleanupEpisodeId, SandboxId, SandboxLifecycle, SandboxQuarantineReasonCode,
    SandboxQuarantineRecoveryPolicy, SandboxQuarantineSource, SandboxTransitionReceiptKind,
};

use std::sync::LazyLock;

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_portability::{
    current_suspend_origin_marker_snapshot_ready, exact_marker_snapshot_backup_current,
    node_published_suspend_evidence, pre_quarantine_suspend_snapshot_current,
    started_suspend_revert_eligibility, suspended_projection_lifecycle_eligible_with_node_evidence,
};
use pplx_space_sql_locator::sql;
use sqlx::types::{Json, JsonValue};
use uuid::Uuid;

use crate::ingest::TRANSITION_RECEIPT_RECORD_COLUMNS;

/// One locked pause-failed recovery candidate.
#[derive(Debug, sqlx::FromRow)]
pub struct PauseFailedRecoveryCandidate {
    pub id: SandboxId,
    pub quarantine_id: Uuid,
    /// The lifecycle the node published, `started` or `paused`.
    pub published_lifecycle: String,
    /// The row's stored lifecycle, unparsed.
    pub lifecycle: String,
}

/// Lock, `FOR UPDATE OF s, qs`, each sandbox placed on `node_id` that the
/// node published in `rows_json` (`sandbox_id`, `published_lifecycle`) as
/// `started` or `paused` while it sits `quarantined` (compared trimmed and
/// lower-cased) under an episode of `quarantine_source` and
/// `quarantine_reason`, together with that episode's row. The locks last
/// until the caller's transaction ends. The caller must pass its open
/// transaction.
pub async fn lock_pause_failed_recovery_candidates<C>(
    tx: &mut C,
    node_id: NodeId,
    rows_json: JsonValue,
    quarantine_source: SandboxQuarantineSource,
    quarantine_reason: SandboxQuarantineReasonCode,
) -> Result<Vec<PauseFailedRecoveryCandidate>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_as(sql!(
        "SELECT s.id, qs.quarantine_id, input.published_lifecycle, s.lifecycle
           FROM sandboxes s
           JOIN jsonb_to_recordset($2::jsonb) AS input(
               sandbox_id UUID,
               published_lifecycle TEXT
           ) ON input.sandbox_id = s.id
           JOIN sandbox_quarantine_state qs ON qs.sandbox_id = s.id
          WHERE s.active_node_id = $1
            AND LOWER(TRIM(s.lifecycle)) = 'quarantined'
            AND input.published_lifecycle IN ('started', 'paused')
            AND qs.source = $3
            AND qs.reason_code = $4
          FOR UPDATE OF s, qs"
    ))
    .bind(node_id)
    .bind(Json(rows_json))
    .bind(quarantine_source.as_str())
    .bind(quarantine_reason.as_str())
    .fetch_all(tx.into_sqlx())
    .await?)
}

/// The decoded row of [`lock_suspended_projection_candidates`], in `SELECT`
/// order: the sandbox id, `clear_marker`, the quarantine episode, the
/// receipt's reason and id, the prior lifecycle, the published artifact's id,
/// kind and origin, the published execution generation and pod UID, and
/// whether the matching running execution was missing its pod UID.
pub type SuspendedProjectionCandidateRow = (
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

/// The receipt vocabulary [`lock_suspended_projection_candidates`] binds.
#[derive(Debug, Clone, Copy)]
pub struct SuspendedProjectionBinds {
    /// `$3`, the kind of a quarantine-recovery receipt.
    pub recovery_receipt_kind: SandboxTransitionReceiptKind,
    /// `$4`, the source of an episode a receipt may recover.
    pub quarantine_source: SandboxQuarantineSource,
    /// `$5`, the exact-suspend-artifact recovery policy.
    pub exact_policy: SandboxQuarantineRecoveryPolicy,
    /// `$6`, the exact-or-pre-quarantine-suspend-artifact recovery policy.
    pub exact_or_pre_quarantine_policy: SandboxQuarantineRecoveryPolicy,
}

fn suspended_projection_candidates_query() -> String {
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
    format!(
        "WITH input AS (
             SELECT published.*,
                    published.transition_receipt_kind = $3
                    AND published.transition_receipt_id IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1
                          FROM sandbox_quarantine_history consumed
                         WHERE consumed.id = published.transition_receipt_id
                    ) AS has_unconsumed_recovery_receipt
               FROM jsonb_to_recordset($2::jsonb) AS published({TRANSITION_RECEIPT_RECORD_COLUMNS})
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
            AND input.published_lifecycle = 'suspended'
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
    )
}

/// Lock, `FOR UPDATE OF s`, the sandboxes placed on `node_id` that the node
/// published in `rows_json` (a recordset of
/// [`TRANSITION_RECEIPT_RECORD_COLUMNS`]) as `suspended`, when the row may
/// take the suspended projection or the published quarantine-recovery
/// receipt is fenced to its episode and active execution generation. The
/// locks last until the caller's transaction ends. The caller must pass its
/// open transaction.
pub async fn lock_suspended_projection_candidates<C>(
    tx: &mut C,
    node_id: NodeId,
    rows_json: JsonValue,
    binds: SuspendedProjectionBinds,
) -> Result<Vec<SuspendedProjectionCandidateRow>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    static QUERY: LazyLock<String> =
        LazyLock::new(|| sql!(suspended_projection_candidates_query().as_str()));
    Ok(sqlx::query_as(sqlx::AssertSqlSafe(QUERY.as_str()))
        .bind(node_id)
        .bind(Json(rows_json))
        .bind(binds.recovery_receipt_kind.as_str())
        .bind(binds.quarantine_source.as_str())
        .bind(binds.exact_policy.as_str())
        .bind(binds.exact_or_pre_quarantine_policy.as_str())
        .fetch_all(tx.into_sqlx())
        .await?)
}
