//! Writes of `sandboxes` from the activation-rollback receipts a
//! `PublishNodeState` publication consumes, after its activation-completion
//! receipts: the rollback convergence. The caller owns the transaction, the
//! locking reads that select candidates, the executions it retires, and the
//! events it emits.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId, SandboxTransitionTargetState};

/// Converge a sandbox whose node rolled back an activation to the receipt's
/// `target_state`, unplaced, keeping the node as its `target_node_id` when it
/// has none. Returns `true` when the row converged. **Observed projection**:
/// the node reported it gave up the activation.
///
/// `clear_placement_changed_at` also clears `placement_changed_at`, as a
/// retired restore's revert does. The write re-checks that the row is still on
/// `node_id` and outside the `quarantined`, `deleted`, `deleting` and
/// `cleanup_required` lifecycles, compared exactly, and checks no execution:
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
                AND lifecycle NOT IN ('quarantined', 'deleted', 'deleting', 'cleanup_required')"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(target_state.as_str())
    .bind(clear_placement_changed_at)
    .execute(executor)
    .await?;
    Ok(converged.rows_affected() == 1)
}
