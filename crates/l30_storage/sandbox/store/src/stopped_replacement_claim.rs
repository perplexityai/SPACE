//! Whether a stopped evacuation's re-placement has been claimed.

use pplx_space_postgres_pool::Executor;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;

use crate::SandboxStoreError;

/// Whether the evacuation restore claim has already moved `sandbox_id`'s
/// stopped evacuation to `restoring`: the row is `restoring` and still
/// carries the `stopped` evacuation marker. A markerless `restoring` row is a
/// user restore, not a re-placement. `false` for a missing row.
///
/// A plain read that takes no lock.
pub async fn claimed(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, SandboxStoreError> {
    let claimed = sqlx::query_scalar::<_, bool>(sql!(
        "SELECT COALESCE(
                        lifecycle = 'restoring' AND evacuation_restore_target = 'stopped',
                        FALSE
                    )
               FROM sandboxes
              WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(claimed.unwrap_or(false))
}
