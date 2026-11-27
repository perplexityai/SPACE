//! Guarded writes the Kubernetes substrate makes to `sandboxes` when it
//! forgets a sandbox's tracked pod: unplacing the row, releasing a stopped
//! evacuation's node, and recovering a `restoring` or `started` row to
//! `suspended` once its execution's cleanup leaves nothing running. Also the
//! read that decides whether retiring the executions a `stopped` row's stop
//! left active must refuse.
//!
//! The Kubernetes store keeps everything around these statements: the
//! sandbox row lock (`SELECT ... FOR UPDATE`), the transaction, the
//! execution-row writes, the warm-pool slot delete, and the event a recovery
//! emits. Every function here takes the caller's transaction because it
//! relies on the row lock the caller took in it. What else the caller must
//! have done first differs per function; see each function's doc.

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::{ExecutionGeneration, SandboxId};

use std::sync::LazyLock;

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_portability::{
    current_disk_artifact_backup_ready, failed_repair_revert_eligibility,
    restore_retry_suspended_revert_snapshot_exists, started_suspend_revert_eligibility,
};
use pplx_space_sql_locator::sql;

/// Unplace a sandbox whose pod tracking was torn down, clearing its active
/// and target nodes and bumping `placement_changed_at` to `NOW()`. Writes an
/// **operation step** (ends the row's placement after pod teardown). Returns
/// whether the row was unplaced.
///
/// For delete-shaped teardowns, where nothing is left to keep recoverable.
/// It relies on the caller's row lock, and on the caller having already
/// retired the execution it tore down, or confirmed that none is active,
/// earlier in this transaction.
///
/// It refuses a `stopped` row and leaves it placed. A stopped sandbox's rootfs
/// stays on the node its `active_node_id` names, and every node-drain guard
/// finds the node's stopped residents through that column, so unplacing one
/// would let a drain terminate the node holding its disk. A pod teardown
/// can't tell whether the rootfs still matches any published copy: a start
/// can boot the guest, let it write and fail with the row still `stopped`.
/// The drain's stopped evacuation asks the node that before it releases one,
/// with [`release_published_stopped_evacuation`].
/// The caller must pass its open transaction.
pub async fn unplace_after_pod_teardown<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let result = sqlx::query(sql!(
        "UPDATE sandboxes
                 SET placement = 'unplaced',
                     active_node_id = NULL,
                     target_node_id = NULL,
                     placement_changed_at = NOW()
                 WHERE id = $1
                   AND lifecycle <> 'stopped'"
    ))
    .bind(sandbox_id)
    .execute(tx.into_sqlx())
    .await?;
    Ok(result.rows_affected() > 0)
}

/// [`unplace_after_pod_teardown`] for a recoverability-preserving retire:
/// `placement_changed_at` is kept, so a markerless row's ready snapshot stays
/// fresh against it. Writes an **operation step**, as
/// [`unplace_after_pod_teardown`] does, and returns whether the row was
/// unplaced. It refuses a `stopped` row, for the same reason.
/// The caller must pass its open transaction.
pub async fn unplace_after_pod_teardown_preserving_placement_clock<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let result = sqlx::query(sql!(
        "UPDATE sandboxes
                 SET placement = 'unplaced',
                     active_node_id = NULL,
                     target_node_id = NULL
                 WHERE id = $1
                   AND lifecycle <> 'stopped'"
    ))
    .bind(sandbox_id)
    .execute(tx.into_sqlx())
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Release `source_node_id`, the node a `stopped` sandbox the drain's stopped
/// evacuation has torn down was resident on: unplace the row, clearing its
/// active and target nodes and bumping `placement_changed_at` to `NOW()`,
/// which the restore sweep's stopped arm orders on. Writes an **operation
/// step**. Returns whether the row was released.
///
/// Guarded on `stopped`, the `'stopped'` evacuation marker, a durable pinned
/// disk artifact ([`current_disk_artifact_backup_ready`]), no active
/// execution, and residence on `source_node_id`. The marker alone isn't
/// enough: the drain sets it when it claims the row, before the disk is
/// published. The caller holds the sandbox row lock, which execution creation
/// also takes, and has already confirmed with `source_node_id` that the rootfs
/// it holds is the one the artifact describes, so a row placed on any other
/// node is refused.
///
/// A row this release already unplaced matches again, so a retried teardown
/// converges, and keeps the `placement_changed_at` the first release stamped,
/// so a retry doesn't move the row back in the restore sweep's order.
/// The caller must pass its open transaction.
pub async fn release_published_stopped_evacuation<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    /// Built once: every input is a literal or a bind placeholder.
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        let durable = current_disk_artifact_backup_ready("s");
        sql!(format!(
            "UPDATE sandboxes s
                SET placement = 'unplaced',
                    active_node_id = NULL,
                    target_node_id = NULL,
                    placement_changed_at = CASE
                        WHEN s.active_node_id IS NULL THEN s.placement_changed_at
                        ELSE NOW()
                    END
              WHERE s.id = $1
                AND s.lifecycle = 'stopped'
                AND s.evacuation_restore_target = 'stopped'
                AND (
                    s.active_node_id = $2
                    OR (s.active_node_id IS NULL AND s.placement = 'unplaced')
                )
                AND {durable}
                AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.active
                )"
        ))
    });
    let result = sqlx::query(sqlx::AssertSqlSafe(QUERY.as_str()))
        .bind(sandbox_id)
        .bind(source_node_id)
        .execute(tx.into_sqlx())
        .await?;
    Ok(result.rows_affected() > 0)
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
/// The caller must pass its open transaction.
pub async fn recover_markerless_restoring_after_execution_cleanup<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
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
        .execute(tx.into_sqlx())
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
/// The caller must pass its open transaction.
pub async fn recover_started_after_execution_retired<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
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
/// The caller must pass its open transaction.
pub async fn recover_started_after_failed_recovery<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
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

async fn recover_started_with_current_suspend_snapshot<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
    proof: StartedSuspendRecoveryProof,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
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
        .execute(tx.into_sqlx())
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Whether retiring the executions a `stopped` row's stop left active must
/// refuse. Reads only.
///
/// It refuses when any of these holds:
///
/// * an active execution that doesn't predate the row's current stop: the row
///   is no longer `stopped`, or the execution was created after its
///   `lifecycle_changed_at`. One execution newer than the stop is retired all
///   the same: the one a stay-stopped runtime upgrade claimed and recorded
///   and didn't finish (its upgrade recorded, its claim targeting `stopped`),
///   once its lease has ended (a live one refuses, below). The row stays
///   `stopped` throughout such an upgrade, so its execution postdates the
///   stop, and refusing it would hold the evacuation, and the drain, forever.
///   The evacuation took it over when it opened and deleted the upgrade's pod,
///   and saw it gone, before it minted the backup the teardown requires
///   durable; that backup stands in for the one the upgrade may still owe;
/// * an active execution holding an activation attempt that hasn't expired,
///   or that has no expiry;
/// * no row carries the `'stopped'` evacuation marker. A start moves the
///   lifecycle, and `sandboxes_clear_evacuation_restore_target` clears the
///   marker on any lifecycle change to a value other than `suspending`,
///   `suspended` or `restoring`, so a start→stop cycle since the drain's last
///   read refuses here.
///
/// The caller holds the sandbox row lock that execution creation also takes,
/// and retires in the same transaction, so the answer holds until it commits.
/// The caller must pass its open transaction.
pub async fn stopped_execution_retirement_refused<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
                        SELECT 1
                          FROM sandbox_k8s_executions execution
                          JOIN sandboxes sandbox ON sandbox.id = execution.sandbox_id
                         WHERE execution.sandbox_id = $1
                           AND execution.active
                           AND NOT (
                               sandbox.lifecycle = 'stopped'
                               AND (
                                   execution.created_at <= sandbox.lifecycle_changed_at
                                   OR (
                                       execution.runtime_upgrade_operation_id IS NOT NULL
                                       AND execution.activation_attempt_target_state = 'stopped'
                                   )
                               )
                           )
                    )
                 OR EXISTS (
                        SELECT 1
                          FROM sandbox_k8s_executions execution
                         WHERE execution.sandbox_id = $1
                           AND execution.active
                           AND execution.activation_attempt_id IS NOT NULL
                           AND COALESCE(execution.activation_attempt_expires_at > NOW(), TRUE)
                    )
                 OR NOT EXISTS (
                        SELECT 1
                          FROM sandboxes
                         WHERE id = $1
                           AND evacuation_restore_target = 'stopped'
                    )"
    ))
    .bind(sandbox_id)
    .fetch_one(tx.into_sqlx())
    .await?)
}
