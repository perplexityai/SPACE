//! Recovery of an abandoned publication after its only execution disappeared.

use std::time::Duration;

use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::TxSettings as _;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_store::k8s_executions::quarantine_recovery;
use pplx_space_sandbox_store::{quarantine_recovery_candidates, row_locks};
use sqlx::{PgExecutor, PgPool};
use uuid::Uuid;

pub(super) use pplx_space_sandbox_store::quarantine_recovery_candidates::Candidate;

/// The recovery candidates after `after`, or `only` alone, up to `limit`
/// ([`quarantine_recovery_candidates::list`]).
pub(super) async fn candidates<'e>(
    executor: impl PgExecutor<'e>,
    after: Option<SandboxId>,
    only: Option<SandboxId>,
    limit: i64,
) -> Result<Vec<Candidate>, sqlx::Error> {
    quarantine_recovery_candidates::list(executor, after, only, limit, SubstrateKind::Kubernetes)
        .await
}

/// Recheck on fresh statement snapshots after locking the owners. No external
/// work occurs inside this transaction. A changed episode, execution, snapshot,
/// activity timestamp, or user operation invalidates the absence proof.
pub(super) async fn recover(pool: &PgPool, expected: &Candidate) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    tx.set_local_lock_timeout(Duration::from_secs(2)).await?;
    row_locks::lock_id(&mut tx, expected.sandbox_id).await?;
    pplx_space_sandbox_quarantine_store::locks::lock_quarantine_state(
        &mut *tx,
        expected.sandbox_id,
    )
    .await?;
    quarantine_recovery::lock_executions(&mut tx, expected.sandbox_id).await?;
    pplx_space_snapshot_store::quarantine_recovery::share_lock_snapshot(
        &mut *tx,
        expected.snapshot_id,
    )
    .await?;
    pplx_space_snapshot_store::quarantine_recovery::share_lock_storage_location(
        &mut *tx,
        expected.snapshot_id,
    )
    .await?;
    let current = candidates(&mut *tx, None, Some(expected.sandbox_id), 2).await?;
    if current.as_slice() != std::slice::from_ref(expected) {
        return Ok(false);
    }
    pplx_space_sandbox_quarantine_store::record_recovered_to_suspended(
        &mut *tx,
        expected.sandbox_id,
        Uuid::now_v7(),
        expected.snapshot_id,
    )
    .await?;
    pplx_space_sandbox_store::repair::recover_quarantined_publication_to_suspended(
        &mut tx,
        expected.sandbox_id,
        expected.artifact_id,
    )
    .await?;
    quarantine_recovery::retire_recovered_execution(&mut *tx, expected.execution_id).await?;
    pplx_space_sandbox_teardown::observed_states::delete(&mut *tx, expected.sandbox_id).await?;
    pplx_space_sandbox_store::port_exposures::invalidate_open_observations(
        &mut *tx,
        expected.sandbox_id,
    )
    .await?;
    pplx_space_sandbox_quarantine_store::delete_quarantine_state(&mut *tx, expected.sandbox_id)
        .await?;
    pplx_space_sandbox_teardown::delete_reconcile_state_for_elements(
        &mut *tx,
        &[
            pplx_space_state_reconciliation::ReconcileElementRef::sandbox_desired_state(
                expected.sandbox_id,
            ),
        ],
    )
    .await?;
    tx.commit().await?;
    Ok(true)
}
