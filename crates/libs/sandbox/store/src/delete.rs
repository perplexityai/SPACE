//! The hard delete of a sandbox row, with the dependents it clears in the
//! same transaction: observed state, reconcile state and claimed warm-pool
//! slots are deleted, and snapshot storage locations are detached.

use pplx_space_common_core::{sql, types::SandboxId};

use pplx_space_sandbox_teardown::{
    delete_reconcile_state_for_elements, sandbox_reconcile_cleanup_refs,
};

/// Delete a central sandbox and every non-FK reconcile row in one transaction.
///
/// Writes an **operation step**: it is the last step of each caller's
/// teardown or rollback. It has no lifecycle predicate of its own; each
/// caller decides the sandbox may go and keeps the locks and transaction that
/// decision rests on. A missing `sandboxes` row is not an error, and no
/// statement checks a row count: the row delete then matches nothing, but the
/// statements keyed by the sandbox id (observed state, reconcile state,
/// claimed warm-pool slots) still remove any rows left for it.
///
/// Reconcile queue, observed-status, and condition rows are not foreign-keyed
/// to `sandboxes`, so callers must remove them explicitly. Snapshot storage
/// locations intentionally survive their snapshot: detach and timestamp them
/// first so the S3 orphan sweeper can reclaim the bytes after snapshot rows
/// cascade. Several API, controller and receipt-consumer delete paths share
/// this helper. Others still write their own statements (the API's
/// create-failure and create-rollback deletes, tombstone retention,
/// internal-api's absent-stopped publish), so a cleanup rule added here does
/// not reach them.
///
/// Every statement here is a plain `UPDATE`/`DELETE` — no `NOWAIT`, no lock
/// bound. That is fine for the callers that open a fresh transaction per
/// sandbox, because there is nothing else of theirs to block. A caller that
/// already holds unrelated row locks in the same transaction, such as one
/// draining a batch of sandboxes, has to bound its own waits first (a
/// transaction-local `lock_timeout`) and classify the resulting `55P03` as
/// retryable; otherwise one contended row stalls everything it holds.
pub async fn delete_with_dependents(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    let cleanup_refs = sandbox_reconcile_cleanup_refs(sandbox_id);
    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(&mut **tx)
    .await?;
    delete_reconcile_state_for_elements(tx, &cleanup_refs).await?;
    sqlx::query(sql!(
        "UPDATE snapshot_storage_locations
         SET snapshot_id = NULL, orphaned_at = NOW()
         WHERE snapshot_id IN (SELECT id FROM snapshots WHERE sandbox_id = $1)"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(&mut **tx)
    .await?;
    // Warm-pool slot receipts are not foreign-keyed to `sandboxes` either
    // (the reservation predates the sandbox row), and a receipt whose sandbox
    // row is gone is unreachable by the suspended-fossil drain, which joins
    // on the sandbox row. Retire it with the sandbox; the state guard inside
    // the verb leaves open (`warming`/`ready`) and `cleanup_pending` slots
    // alone.
    pplx_space_cluster_pool::WarmPoolStore::delete_claimed_slots_for_retired_executions(
        tx,
        &[sandbox_id],
    )
    .await?;
    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(sandbox_id.as_uuid())
        .execute(&mut **tx)
        .await?;
    Ok(())
}
