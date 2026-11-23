//! `sandbox_observed_states`: the last state a node published for each
//! sandbox it reports, overlaid on the row's lifecycle by status reads.
//!
//! Node publications write these rows, and not every writer is in this crate
//! yet. [`crate::delete::delete_with_dependents`] removes them with the row.
//! [`delete`] is the unguarded delete, run on the executor its caller passes.

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;

/// Drop the sandbox's observed state, whatever it says (an operation step):
/// for a path that has just settled the row's lifecycle itself and must not
/// leave a node's stale report overlaying it.
/// The sandbox's `sandbox_snapshot_inventory` rows go with it, through that
/// table's foreign-key cascade.
pub async fn delete(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(executor)
    .await?;
    Ok(())
}
