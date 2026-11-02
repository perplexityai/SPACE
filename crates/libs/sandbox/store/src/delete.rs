//! Lifecycle writes of a sandbox delete: the `deleting` intent and the
//! compensation that puts the previous lifecycle back when the delete fails.
//! Removing the row is not here.

use pplx_space_common_core::types::{SandboxId, SandboxLifecycle};

use pplx_space_common_core::sql;
use sqlx::{Postgres, Transaction};

/// Record a caller's delete intent: set `sandboxes.lifecycle` to `deleting`.
/// An **intent** write. A missing row writes nothing and is not an error.
///
/// It has no lifecycle predicate and overwrites whatever lifecycle the row
/// holds. The caller supplies every guard, in the same transaction: the
/// fork-source lifecycle lock, the check that no fork of the sandbox is
/// queued or running, and the row's `FOR UPDATE` read, which returns the
/// lifecycle to restore if the delete fails. The API's delete parses that
/// read as a [`SandboxLifecycle`], which has no `deleted` or
/// `cleanup_required` variant, so those rows are refused before this write
/// runs; a caller that skips the parse would overwrite them.
///
/// Once committed, `deleting` fences some writers and not others. Fork
/// admission and the API's wake-completion compare-and-set
/// (`complete_wake_lifecycle`) refuse the row. Other writers can still move
/// it off `deleting`, among them the API's best-effort lifecycle stamps and
/// its pooled post-wake update, which refuse only `failed`, `quarantined`,
/// `deleted` and `cleanup_required` rows (see
/// [`restore_lifecycle_from_deleting`]).
pub async fn mark_deleting(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET lifecycle = 'deleting'
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Compensate a failed delete: put `lifecycle` back on a row still in
/// `deleting`. An **operation step**. Returns whether the row was restored.
///
/// Guarded by `lifecycle = 'deleting'` alone. The API's compensations run it
/// on the pool after the intent's transaction has committed, with no row lock
/// held, so `false` means no row with this id is in `deleting`: the row is
/// gone, or another writer moved it off `deleting`. Telling those apart is the
/// caller's job.
pub async fn restore_lifecycle_from_deleting(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    lifecycle: SandboxLifecycle,
) -> Result<bool, sqlx::Error> {
    let changed = sqlx::query(sql!(
        "UPDATE sandboxes
         SET lifecycle = $2
         WHERE id = $1 AND lifecycle = 'deleting'"
    ))
    .bind(sandbox_id)
    .bind(lifecycle.as_str())
    .execute(executor)
    .await?
    .rows_affected();
    Ok(changed == 1)
}
