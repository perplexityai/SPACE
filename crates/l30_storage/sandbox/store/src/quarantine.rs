//! Writes of `sandboxes` from the activation-rollback receipts a
//! `PublishNodeState` publication consumes, after its activation-completion
//! receipts: the rollback convergence, and the read that decides whether a
//! restore's rollback retires its execution. The caller owns the transaction,
//! the locking reads that select candidates, the executions it retires, and
//! the events it emits.

use std::sync::LazyLock;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId, SandboxTransitionTargetState};
use pplx_space_sandbox_portability::restore_retry_suspended_revert_snapshot_exists;

/// Converge a sandbox whose node rolled back an activation to the receipt's
/// `target_state`, unplaced, keeping the node as its `target_node_id` when it
/// has none. Returns `true` when the row converged. **Observed projection**:
/// the node reported it gave up the activation.
///
/// `clear_placement_changed_at` also clears `placement_changed_at`, as a
/// retired restore's revert does. The write re-checks that the row is still on
/// `node_id` and outside the `quarantined`, `deleted` and `deleting`
/// lifecycles, compared exactly, and checks no execution:
/// the caller fences the receipt by first retiring the exact execution
/// generation and activation attempt it names, and its candidate selection
/// holds the row `FOR UPDATE` in the same transaction. The caller treats
/// `false` as lost ownership and rolls back.
pub async fn converge_activation_rollback(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    sandbox_id: SandboxId,
    target_state: SandboxTransitionTargetState,
    clear_placement_changed_at: bool,
) -> Result<bool, sqlx::Error> {
    let converged = sqlx::query(sql!(
        "UPDATE sandboxes
                SET lifecycle = $3,
                    placement = 'unplaced',
                    target_node_id = COALESCE(target_node_id, active_node_id),
                    active_node_id = NULL,
                    command_ready_pending = FALSE,
                    lifecycle_changed_at = NOW(),
                    placement_changed_at = CASE WHEN $4 THEN NULL ELSE placement_changed_at END
              WHERE id = $1
                AND active_node_id = $2
                AND lifecycle NOT IN ('quarantined', 'deleted', 'deleting')"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(target_state.as_str())
    .bind(clear_placement_changed_at)
    .execute(executor)
    .await?;
    Ok(converged.rows_affected() == 1)
}

/// Whether a rollback of a `restoring` row to `suspended` may retire
/// `generation`'s execution as `replaced`: not an evacuation restore, the
/// restore created the execution, and the suspended-revert proof
/// ([`restore_retry_suspended_revert_snapshot_exists`]) holds. Reads only.
///
/// Read before the rollback converges the lifecycle, so `lifecycle_changed_at`
/// still names the transition into the restore the node rolled back. The
/// caller's candidate select already holds the sandbox row `FOR UPDATE`, and
/// this separate statement reads the snapshots and executions as of after
/// that lock.
pub async fn rollback_restore_retirable(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<bool, sqlx::Error> {
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        let proof = restore_retry_suspended_revert_snapshot_exists("s");
        sql!(
            format!(
                "SELECT s.evacuation_restore_target IS NULL
                    AND EXISTS (
                        SELECT 1
                          FROM sandbox_k8s_executions e
                         WHERE e.sandbox_id = s.id
                           AND e.generation = $2
                           AND e.created_at >= s.lifecycle_changed_at
                    )
                    AND {proof}
               FROM sandboxes s
              WHERE s.id = $1"
            )
            .as_str()
        )
    });
    sqlx::query_scalar(sqlx::AssertSqlSafe(QUERY.as_str()))
        .bind(sandbox_id)
        .bind(generation)
        .fetch_one(executor)
        .await
}
