//! How many deleted sandboxes' rows are still retained.

use pplx_space_sql_locator::sql;

use crate::SandboxStoreError;

/// The number of `sandboxes` rows in the `deleted` lifecycle: the tombstones
/// retention has not removed yet (see [`crate::DELETED_SANDBOX_RETENTION`]).
pub async fn count(executor: impl sqlx::PgExecutor<'_>) -> Result<i64, SandboxStoreError> {
    Ok(sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)
               FROM sandboxes
              WHERE lifecycle = 'deleted'"
    ))
    .fetch_one(executor)
    .await?)
}
