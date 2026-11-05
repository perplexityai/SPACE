//! Guarded writes the Kubernetes substrate makes to `sandboxes` when it
//! forgets a sandbox's tracked pod: unplacing the row, and recovering a
//! `restoring` or `started` row to `suspended` once its execution's cleanup
//! leaves nothing running.
//!
//! The Kubernetes store keeps everything around these statements: the
//! sandbox row lock (`SELECT ... FOR UPDATE`), the transaction, the
//! execution-row writes, the warm-pool slot delete, and the event a recovery
//! emits. Every function here takes the caller's transaction because it
//! relies on the row lock the caller took in it. What else the caller must
//! have done first differs per function; see each function's doc.

use pplx_space_common_core::types::{ExecutionGeneration, SandboxId};

use std::sync::LazyLock;

use pplx_space_common_core::sql;
use pplx_space_sandbox_portability::{
    failed_repair_revert_eligibility, restore_retry_suspended_revert_snapshot_exists,
    started_suspend_revert_eligibility,
};
use sqlx::{Postgres, Transaction};

/// Unplace a sandbox whose pod tracking was torn down, clearing its active
/// and target nodes and bumping `placement_changed_at` to `NOW()`. Writes an
/// **operation step** (ends the row's placement after pod teardown).
///
/// For delete-shaped teardowns, where nothing is left to keep recoverable.
/// Matches on the id alone: no lifecycle predicate of its own. It relies on
/// the caller's row lock, and on the caller having already retired the
/// execution it tore down, or confirmed that none is active, earlier in this
/// transaction.
pub async fn unplace_after_pod_teardown(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes
                 SET placement = 'unplaced',
                     active_node_id = NULL,
                     target_node_id = NULL,
                     placement_changed_at = NOW()
                 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// [`unplace_after_pod_teardown`] for a recoverability-preserving retire:
/// `placement_changed_at` is kept, so a markerless row's ready snapshot stays
/// fresh against it. Writes an **operation step**, as
/// [`unplace_after_pod_teardown`] does.
///
/// Matches on the id alone, as [`unplace_after_pod_teardown`] does.
pub async fn unplace_after_pod_teardown_preserving_placement_clock(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes
                 SET placement = 'unplaced',
                     active_node_id = NULL,
                     target_node_id = NULL
                 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Revert a markerless `restoring` row whose execution cleanup left no
/// active execution to `suspended`, unplaced, with its placement clock,
/// artifact marker, `command_ready_pending` and restore lease cleared.
/// Writes an **inference** (the control plane concluding the restore is
/// abandoned).
///
/// Guarded on `restoring`, no evacuation marker, no active execution, and
/// [`restore_retry_suspended_revert_snapshot_exists`]. The caller holds the
/// sandbox row lock, which execution creation also takes, so no successor
/// execution can appear between this proof and commit. Callers retire the
/// execution, or confirm that none is active, earlier in this transaction.
pub async fn recover_markerless_restoring_after_execution_cleanup(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    let safe_revert_snapshot = restore_retry_suspended_revert_snapshot_exists("s");
    let query = format!(
        "UPDATE sandboxes s
                SET lifecycle = 'suspended',
                    placement = 'unplaced',
                    active_node_id = NULL,
                    target_node_id = NULL,
                    placement_changed_at = NULL,
                    current_local_artifact_id = NULL,
                    current_local_artifact_kind = NULL,
                    current_local_artifact_origin = NULL,
                    command_ready_pending = FALSE,
                    restore_lease_expires_at = NULL,
                    restore_lease_token = NULL
              WHERE s.id = $1
                AND s.lifecycle = 'restoring'
                AND s.evacuation_restore_target IS NULL
                AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.active
                )
                AND {safe_revert_snapshot}"
    );
    sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Revert an unplaced `started` row to `suspended` when no execution is
/// active and no execution row still has a `pod_ip`, and its current suspend
/// snapshot is still eligible ([`started_suspend_revert_eligibility`]).
/// Execution retirement clears `pod_ip`, so this does not prove that no pod
/// ever had an IP. Writes an **inference**. Returns whether the row moved.
///
/// The caller holds the sandbox row lock and has already retired the
/// execution it tore down in this transaction.
pub async fn recover_started_after_execution_retired(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    recover_started_with_current_suspend_snapshot(
        tx,
        sandbox_id,
        StartedSuspendRecoveryProof::ExecutionRetired,
    )
    .await
}

/// Revert an unplaced `started` row to `suspended` when its active execution
/// at `generation` is a `recovery_failed` one that tracks no pod and no newer
/// execution is active, and its snapshot passes
/// [`failed_repair_revert_eligibility`]. Writes an **inference**. Returns
/// whether the row moved.
///
/// The caller holds the sandbox row lock. The `recovery_failed` execution
/// must still be active when this runs; the caller retires it afterwards, in
/// the same transaction, only when this returns `true`.
pub async fn recover_started_after_failed_recovery(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
) -> Result<bool, sqlx::Error> {
    recover_started_with_current_suspend_snapshot(
        tx,
        sandbox_id,
        StartedSuspendRecoveryProof::FailedRecovery { generation },
    )
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartedSuspendRecoveryProof {
    ExecutionRetired,
    FailedRecovery { generation: ExecutionGeneration },
}

async fn recover_started_with_current_suspend_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    proof: StartedSuspendRecoveryProof,
) -> Result<bool, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static EXECUTION_RETIRED_QUERY: LazyLock<String> = LazyLock::new(|| {
        let eligibility = started_suspend_revert_eligibility("s");
        sql!(format!(
            "WITH args AS (
                 SELECT $2::BIGINT AS generation
             ),
             pre AS (
                 SELECT id,
                        {clear_marker} AS clear_marker
                   FROM sandboxes s
                  WHERE s.id = $1
             )
             UPDATE sandboxes s
                SET lifecycle = 'suspended',
                    placement = 'unplaced',
                    active_node_id = NULL,
                    target_node_id = NULL,
                    placement_changed_at = NULL,
                    command_ready_pending = FALSE,
                    current_local_artifact_id = CASE WHEN pre.clear_marker THEN NULL ELSE s.current_local_artifact_id END,\n\
                    current_local_artifact_kind = CASE WHEN pre.clear_marker THEN NULL ELSE s.current_local_artifact_kind END,\n\
                    current_local_artifact_origin = CASE WHEN pre.clear_marker THEN NULL ELSE s.current_local_artifact_origin END
               FROM pre, args
              WHERE s.id = pre.id
                AND s.lifecycle = 'started'
                AND s.placement = 'unplaced'
                AND s.active_node_id IS NULL
                AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.active
                 )
                 AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.pod_ip IS NOT NULL
                 )
                AND {eligible}",
            clear_marker = eligibility.clear_marker,
            eligible = eligibility.eligible,
        ))
    });
    /// Built once: every input is a literal or a bind placeholder.
    static FAILED_RECOVERY_QUERY: LazyLock<String> = LazyLock::new(|| {
        let eligibility = failed_repair_revert_eligibility("s");
        sql!(format!(
            "WITH args AS (
                 SELECT $2::BIGINT AS generation
             ),
             pre AS (
                 SELECT id,
                        {clear_marker} AS clear_marker
                   FROM sandboxes s
                  WHERE s.id = $1
             )
             UPDATE sandboxes s
                SET lifecycle = 'suspended',
                    placement = 'unplaced',
                    active_node_id = NULL,
                    target_node_id = NULL,
                    placement_changed_at = NULL,
                    command_ready_pending = FALSE,
                    current_local_artifact_id = CASE WHEN pre.clear_marker THEN NULL ELSE s.current_local_artifact_id END,\n\
                    current_local_artifact_kind = CASE WHEN pre.clear_marker THEN NULL ELSE s.current_local_artifact_kind END,\n\
                    current_local_artifact_origin = CASE WHEN pre.clear_marker THEN NULL ELSE s.current_local_artifact_origin END
               FROM pre, args
              WHERE s.id = pre.id
                AND s.lifecycle = 'started'
                AND s.placement = 'unplaced'
                AND s.active_node_id IS NULL
                AND EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.generation = args.generation
                       AND e.active
                       AND e.state = 'recovery_failed'
                       AND e.pod_uid IS NULL
                       AND e.pod_ip IS NULL
                       AND e.k8s_node_name IS NULL
                 )
                 AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions newer
                     WHERE newer.sandbox_id = s.id
                       AND newer.active
                       AND newer.generation > args.generation
                 )
                AND {eligible}",
            clear_marker = eligibility.clear_marker,
            eligible = eligibility.eligible,
        ))
    });
    let (generation, query) = match proof {
        StartedSuspendRecoveryProof::ExecutionRetired => (None, &*EXECUTION_RETIRED_QUERY),
        StartedSuspendRecoveryProof::FailedRecovery { generation } => {
            (Some(generation.get()), &*FAILED_RECOVERY_QUERY)
        }
    };
    let result = sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
        .bind(sandbox_id)
        .bind(generation)
        .execute(&mut **tx)
        .await?;
    Ok(result.rows_affected() > 0)
}
