//! `sandbox_observed_states`: the last state a node published for each
//! sandbox it reports, overlaid on the row's lifecycle by status reads.
//!
//! Node publications write these rows, and not every writer is in this crate
//! yet. [`crate::delete::delete_with_dependents`] removes them with the row.
//! [`delete`] is the unguarded delete, run on the executor its caller passes.
//! [`newest`] and [`window`] are unlocked reads of the rows' ids.

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use uuid::Uuid;

use crate::SandboxStoreError;

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

/// The newest observed-row id; `None` when the table is empty.
pub async fn newest(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Option<Uuid>, SandboxStoreError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT sandbox_id
             FROM sandbox_observed_states
             ORDER BY sandbox_id DESC
             LIMIT 1"
    ))
    .fetch_optional(executor)
    .await?)
}

/// Up to `limit` observed-row ids in `(after, through]`, ascending, from a
/// primary-key range scan. The ordering is a resume position, not a fairness
/// policy.
pub async fn window(
    executor: impl sqlx::PgExecutor<'_>,
    after: Uuid,
    through: Uuid,
    limit: i64,
) -> Result<Vec<Uuid>, SandboxStoreError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT sandbox_id
             FROM sandbox_observed_states
             WHERE sandbox_id > $1
               AND sandbox_id <= $2
             ORDER BY sandbox_id
             LIMIT $3"
    ))
    .bind(after)
    .bind(through)
    .bind(limit)
    .fetch_all(executor)
    .await?)
}
