use std::sync::LazyLock;

use pplx_space_common_core::{sql, types::SandboxId};
use pplx_space_sandbox_portability::node_published_execution_identity_matches;
use sqlx::PgConnection;

use crate::sandbox_quarantine_store;

#[derive(Debug)]
pub(crate) struct SuspendedFinalizationCandidate {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) clear_marker: bool,
    pub(crate) published_artifact: Option<sandbox_quarantine_store::PublishedSuspendArtifact>,
    pub(crate) published_execution_identity:
        Option<sandbox_quarantine_store::PublishedExecutionIdentity>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum FinalizeSuspensionError {
    #[error("finalize suspended sandbox lifecycle: {0}")]
    Finalize(#[source] sqlx::Error),
    #[error("delete retired warm-pool slot receipts: {0}")]
    RetirePoolReceipts(#[source] sqlx::Error),
}

// Keep the shared execution fence authoritative; the write and every assignment
// remain visible together. The caller already holds the selected sandbox locks.
static FINALIZE_SUSPENSION: LazyLock<String> = LazyLock::new(|| {
    let exact_execution_identity = node_published_execution_identity_matches(
        "e",
        "candidate.published_execution_generation",
        "candidate.published_pod_uid",
        "candidate.published_pod_uid_was_missing",
    );
    sql!(format!(
        "WITH candidate AS (
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
         ),
         retired_execution AS (
             UPDATE sandbox_k8s_executions e
                SET active = FALSE,
                    state = 'suspended',
                    pod_ip = NULL,
                    terminated_at = COALESCE(e.terminated_at, NOW()),
                    updated_at = NOW()
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
                SET apply_state = 'pending',
                    last_applied_node_id = NULL,
                    last_error = NULL,
                    last_bridge_status = NULL,
                    last_applied_bridge_generation = NULL,
                    last_observed_at = NULL,
                    updated_at = NOW()
               FROM eligible_candidate candidate
              WHERE spe.sandbox_id = candidate.id
                AND spe.desired_state = 'open'
             RETURNING spe.sandbox_id
         )
         UPDATE sandboxes s
            SET lifecycle = 'suspended',
                placement = 'unplaced',
                active_node_id = NULL,
                command_ready_pending = FALSE,
                target_node_id = COALESCE(s.target_node_id, s.active_node_id),
                current_local_artifact_id = CASE
                    WHEN candidate.published_artifact_id IS NOT NULL
                    THEN candidate.published_artifact_id
                    WHEN candidate.clear_marker THEN NULL
                    ELSE s.current_local_artifact_id
                END,
                current_local_artifact_kind = CASE
                    WHEN candidate.published_artifact_id IS NOT NULL
                    THEN candidate.published_artifact_kind
                    WHEN candidate.clear_marker THEN NULL
                    ELSE s.current_local_artifact_kind
                END,
                current_local_artifact_origin = CASE
                    WHEN candidate.published_artifact_id IS NOT NULL
                    THEN candidate.published_artifact_origin
                    WHEN candidate.clear_marker THEN NULL
                    ELSE s.current_local_artifact_origin
                END
           FROM eligible_candidate candidate
          WHERE s.id = candidate.id
          RETURNING s.id, candidate.clear_marker"
    ))
});

/// Apply previously locked suspension candidates inside the caller's transaction.
/// Artifact-bearing claims additionally require an exact active execution match.
/// Writes lock execution, port, sandbox and pool-receipt rows while retaining
/// caller-held locks. An empty result means no candidate passed the fence.
pub(crate) async fn apply(
    conn: &mut PgConnection,
    candidates: &[SuspendedFinalizationCandidate],
) -> Result<Vec<(SandboxId, bool)>, FinalizeSuspensionError> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
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
    let projected: Vec<(SandboxId, bool)> =
        sqlx::query_as(sqlx::AssertSqlSafe(FINALIZE_SUSPENSION.as_str()))
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
            .map_err(FinalizeSuspensionError::Finalize)?;

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
    .map_err(FinalizeSuspensionError::RetirePoolReceipts)?;

    Ok(projected)
}
