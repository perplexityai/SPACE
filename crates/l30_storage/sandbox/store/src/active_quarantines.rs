//! How many sandboxes sit in an active quarantine.

use pplx_space_sql_locator::sql;

use crate::SandboxStoreError;

/// The number of `sandbox_quarantine_state` episodes whose sandbox row is
/// still `quarantined`. An episode left behind by a row that has moved on is
/// not counted.
pub async fn count(executor: impl sqlx::PgExecutor<'_>) -> Result<i64, SandboxStoreError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT COUNT(*)::BIGINT
               FROM sandbox_quarantine_state quarantine
               JOIN sandboxes sandbox ON sandbox.id = quarantine.sandbox_id
              WHERE sandbox.lifecycle = 'quarantined'"
    ))
    .fetch_one(executor)
    .await?)
}
