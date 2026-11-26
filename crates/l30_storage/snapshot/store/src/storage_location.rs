//! Reads of one `snapshot_storage_locations` row.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;
use uuid::Uuid;

/// The storage connection string of location `storage_location_id`, when it
/// still belongs to `snapshot_id` and is not orphaned.
pub async fn live_connection_string(
    executor: impl Executor<'_>,
    snapshot_id: SnapshotId,
    storage_location_id: Uuid,
) -> Result<Option<String>, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT storage_connection_string
             FROM snapshot_storage_locations
             WHERE id = $1 AND snapshot_id = $2 AND orphaned_at IS NULL"
    ))
    .bind(storage_location_id)
    .bind(snapshot_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}
