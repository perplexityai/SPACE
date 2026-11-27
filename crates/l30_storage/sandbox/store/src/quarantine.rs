//! Writes of `sandboxes` from the activation-rollback receipts a
//! `PublishNodeState` publication consumes, after its activation-completion
//! receipts: the rollback convergence, and the read that decides whether a
//! restore's rollback retires its execution. The caller owns the transaction,
//! the locking reads that select candidates, the executions it retires, and
//! the events it emits.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{SandboxId, SandboxTransitionTargetState};
use pplx_space_sandbox_portability::{
    restore_pinnable_pre_claim_capture_exists, restore_retry_pinnable_revert_snapshot_exists,
    restore_retry_source_snapshot_exists, restore_retry_suspended_revert_snapshot_exists,
    restore_revert_forfeits_source_window,
};
use pplx_space_sql_locator::sql;

/// Converge a sandbox whose node rolled back an activation to the receipt's
/// `target_state`, unplaced, keeping the node as its `target_node_id` when it
/// has none. Returns `true` when the row converged. **Observed projection**:
/// the node reported it gave up the activation.
///
/// `clear_placement_changed_at` also clears `placement_changed_at`, as a
/// retired restore's revert does. When rolling a `restoring` row back toward
/// `suspended` would forfeit its pre-claim restore source window
/// ([`restore_revert_forfeits_source_window`]), `lifecycle` and
/// `lifecycle_changed_at` stay as stored and the restore lease pair is cleared
/// so the next wake retries against the original claim boundary. The write
/// re-checks that the row is still on `node_id` and outside the `quarantined`,
/// `deleted` and `deleting` lifecycles, compared exactly, and checks no
/// execution: the caller fences the receipt by first retiring the exact
/// execution generation and activation attempt it names, and its candidate
/// selection holds the row `FOR UPDATE` in the same transaction. The caller
/// treats `false` as lost ownership and rolls back.
pub async fn converge_activation_rollback(
    executor: impl Executor<'_>,
    node_id: NodeId,
    sandbox_id: SandboxId,
    target_state: SandboxTransitionTargetState,
    clear_placement_changed_at: bool,
) -> Result<bool, DbError> {
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        let forfeits_window = restore_revert_forfeits_source_window("sandboxes");
        let keep_restoring = format!(
            "(LOWER(TRIM(lifecycle)) = 'restoring' AND $3 = 'suspended' AND {forfeits_window})"
        );
        sql!(
            format!(
                "UPDATE sandboxes
                    SET lifecycle = CASE WHEN {keep_restoring} THEN lifecycle ELSE $3 END,
                        placement = 'unplaced',
                        target_node_id = COALESCE(target_node_id, active_node_id),
                        active_node_id = NULL,
                        command_ready_pending = FALSE,
                        lifecycle_changed_at = CASE
                            WHEN {keep_restoring} THEN lifecycle_changed_at
                            ELSE NOW()
                        END,
                        placement_changed_at = CASE
                            WHEN $4 AND NOT (
                                {keep_restoring}
                                AND lifecycle_changed_at IS NOT NULL
                                AND placement_changed_at < lifecycle_changed_at
                            ) THEN NULL
                            ELSE placement_changed_at
                        END,
                        restore_lease_expires_at = CASE
                            WHEN {keep_restoring} THEN NULL
                            ELSE restore_lease_expires_at
                        END,
                        restore_lease_token = CASE
                            WHEN {keep_restoring} THEN NULL
                            ELSE restore_lease_token
                        END
                  WHERE id = $1
                    AND active_node_id = $2
                    AND lifecycle NOT IN ('quarantined', 'deleted', 'deleting')"
            )
            .as_str()
        )
    });
    let converged = sqlx::query(sqlx::AssertSqlSafe(QUERY.as_str()))
        .bind(sandbox_id)
        .bind(node_id)
        .bind(target_state.as_str())
        .bind(clear_placement_changed_at)
        .execute(executor.into_sqlx())
        .await?;
    Ok(converged.rows_affected() == 1)
}

/// Whether a rollback of a `restoring` row to `suspended` may retire
/// `generation`'s execution as `replaced`: not an evacuation restore, the
/// restore created the execution, and either the suspended-revert proof
/// ([`restore_retry_suspended_revert_snapshot_exists`]) holds, or the rollback
/// keeps `lifecycle = 'restoring'` ([`restore_revert_forfeits_source_window`])
/// and the retry source proof ([`restore_retry_source_snapshot_exists`]) holds.
/// Reads only.
///
/// Read before the rollback converges the lifecycle, so `lifecycle_changed_at`
/// still names the transition into the restore the node rolled back. The
/// caller's candidate select already holds the sandbox row `FOR UPDATE`, and
/// this separate statement reads the snapshots and executions as of after
/// that lock.
pub async fn rollback_restore_retirable(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<bool, DbError> {
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        let suspended_proof = restore_retry_suspended_revert_snapshot_exists("s");
        let pinnable_revert_proof = restore_retry_pinnable_revert_snapshot_exists("s");
        let forfeits_window = restore_revert_forfeits_source_window("s");
        let retry_proof = restore_retry_source_snapshot_exists("s");
        let pinnable_pre_claim_proof = restore_pinnable_pre_claim_capture_exists("s");
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
                    AND ({suspended_proof}
                         OR {pinnable_revert_proof}
                         OR ({forfeits_window}
                             AND ({retry_proof} OR {pinnable_pre_claim_proof})))
               FROM sandboxes s
              WHERE s.id = $1"
            )
            .as_str()
        )
    });
    Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(QUERY.as_str()))
        .bind(sandbox_id)
        .bind(generation)
        .fetch_one(executor.into_sqlx())
        .await?)
}
