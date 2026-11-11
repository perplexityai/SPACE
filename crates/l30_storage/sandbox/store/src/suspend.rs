//! Writes the Kubernetes suspend flow makes to `sandboxes`: claiming a
//! suspend, recording its committed artifact, and finalizing the suspended
//! row, including the missing-pod finalization from the current snapshot.
//!
//! The caller keeps everything around them: the sandbox and execution row
//! locks (`SELECT ... FOR UPDATE`), the transactions, the execution-row
//! writes beside each step, and the claimed warm-pool slot delete. Every
//! function runs on the executor its caller passes, so it composes into the
//! caller's transaction when there is one.

use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, ExecutionGeneration, LocalArtifactId, PodUid, SandboxId,
    SandboxLifecycle,
};

use pplx_space_common_core::sql;

/// **Operation step.** Claim the suspend of the execution at `generation`
/// tracking `pod_uid`: flip the row from `previous` to `suspending` while
/// that execution is still the active `running` one. Returns whether the row
/// flipped.
///
/// The caller tries each lifecycle a suspend may start from (`started`,
/// `paused`) in turn; a zero-row match means another writer won or the
/// execution moved on.
pub async fn claim(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
    pod_uid: &PodUid,
    previous: SandboxLifecycle,
) -> Result<bool, sqlx::Error> {
    let claimed = sqlx::query(sql!(
        "UPDATE sandboxes s
                    SET lifecycle = 'suspending'
                   FROM sandbox_k8s_executions e
                  WHERE s.id = $1
                    AND s.lifecycle = $4
                    AND e.sandbox_id = s.id
                    AND e.generation = $2
                    AND e.active
                    AND e.pod_uid = $3
                    AND e.state = 'running'"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(generation.get())
    .bind(pod_uid.as_str())
    .bind(previous.as_str())
    .execute(executor)
    .await?;
    Ok(claimed.rows_affected() == 1)
}

/// **Operation step.** Record the artifact a node committed for the
/// in-flight suspend as the row's current local artifact marker (origin
/// `suspend`). Returns whether the row was still `suspending`.
///
/// The only guard is `lifecycle = 'suspending'`, which the statement also
/// re-assigns. The caller locks the row first and, in the same transaction,
/// fences a `deleting` artifact and marks the execution suspend-committed.
pub async fn record_committed_artifact(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    artifact_id: LocalArtifactId,
    artifact_kind: ArtifactKind,
) -> Result<bool, sqlx::Error> {
    let updated = sqlx::query(sql!(
        "UPDATE sandboxes
                SET lifecycle = 'suspending',
                    current_local_artifact_id = $2,
                    current_local_artifact_kind = $3,
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1
                AND lifecycle = 'suspending'"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(artifact_id.as_uuid())
    .bind(artifact_kind.as_str())
    .execute(executor)
    .await?;
    Ok(updated.rows_affected() == 1)
}

/// **Operation step.** Finalize a committed suspend: retire the active
/// execution, invalidate open port exposures and leave the row `suspended`
/// and unplaced, with `artifact_id` as its current local `suspend` artifact.
///
/// One statement whose CTEs also write `sandbox_k8s_executions` and the port
/// exposures. It has no lifecycle
/// predicate of its own and matches the row by id alone: it relies on the
/// caller's sandbox and execution row locks and the re-checks it made under
/// them in the same transaction.
pub async fn finalize_with_committed_artifact(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    artifact_id: LocalArtifactId,
    artifact_kind: ArtifactKind,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "WITH candidate AS MATERIALIZED (
                 SELECT id
                   FROM sandboxes
                  WHERE id = $1
             ),
             retired_execution AS (\n\
                 UPDATE sandbox_k8s_executions e\n\
                    SET active = FALSE,\n\
                        state = 'suspended',\n\
                        pod_ip = NULL,\n\
                        terminated_at = COALESCE(e.terminated_at, NOW()),\n\
                        updated_at = NOW()\n\
                   FROM candidate\n\
                  WHERE e.sandbox_id = candidate.id\n\
                    AND e.active\n\
                  RETURNING e.sandbox_id\n\
             ),
             reset_port_exposures AS (\n\
                 UPDATE sandbox_port_exposures spe\n\
                    SET apply_state = 'pending',\n\
                        last_applied_node_id = NULL,\n\
                        last_error = NULL,\n\
                        last_bridge_status = NULL,\n\
                        last_applied_bridge_generation = NULL,\n\
                        last_observed_at = NULL,\n\
                        updated_at = NOW()\n\
                   FROM candidate\n\
                  WHERE spe.sandbox_id = candidate.id\n\
                    AND spe.desired_state = 'open'\n\
                  RETURNING spe.sandbox_id\n\
             )
             UPDATE sandboxes s
                SET lifecycle = 'suspended',
                    placement = 'unplaced',
                    active_node_id = NULL,
                    command_ready_pending = FALSE,
                    current_local_artifact_id = $2,\n\
                    current_local_artifact_kind = $3,\n\
                    current_local_artifact_origin = $4
               FROM candidate
              WHERE s.id = candidate.id"
    ))
    .bind(sandbox_id)
    .bind(artifact_id.as_uuid())
    .bind(artifact_kind.as_str())
    .bind(ArtifactOrigin::Suspend.as_str())
    .execute(executor)
    .await?;
    Ok(())
}

/// **Inference.** Finalize a `suspending` row whose pod went missing, from
/// the current snapshot: leave it `suspended` and unplaced, clearing the
/// current local artifact marker when `clear_marker` (the caller's
/// `missing_suspending_pod_revert_eligibility` answer) says it is stale.
///
/// Matches the row by id alone, with no lifecycle predicate of its own: it
/// relies on the caller's sandbox row lock, its eligibility select and its
/// retirement of the active execution, all in the same transaction.
pub async fn finalize_missing_pod_from_current_snapshot(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    clear_marker: bool,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes s SET lifecycle = 'suspended',
                    placement = 'unplaced',
                    active_node_id = NULL,
                    command_ready_pending = FALSE,
                    current_local_artifact_id = CASE WHEN $2 THEN NULL ELSE s.current_local_artifact_id END,\n\
                    current_local_artifact_kind = CASE WHEN $2 THEN NULL ELSE s.current_local_artifact_kind END,\n\
                    current_local_artifact_origin = CASE WHEN $2 THEN NULL ELSE s.current_local_artifact_origin END WHERE s.id = $1"
    ))
        .bind(sandbox_id)
        .bind(clear_marker)
        .execute(executor)
        .await?;
    Ok(())
}
