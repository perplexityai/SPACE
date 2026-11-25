//! Handing snapshot prefixes to the storage sweeper.
//!
//! An orphan is a `snapshot_storage_locations` row with `snapshot_id = NULL`
//! and `orphaned_at` set. The cluster controller's snapshot-storage sweeper
//! deletes the bytes under its prefix and then the row. This verb creates
//! orphans; it doesn't delete bytes.

use pplx_space_accounts::OrgId;
use pplx_space_artifact_model::ExactStorageUri;
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;

/// Register `storage_uri` as an orphan, for bytes a caller wrote under the
/// prefix of a snapshot it then didn't register: the sweeper deletes them.
/// Returns whether it registered one.
///
/// Registers nothing when a `snapshots` row with `snapshot_id` exists, in the
/// same statement: a concurrent writer that derived the same prefix and won
/// owns those bytes.
pub async fn orphan_unowned_location(
    executor: impl sqlx::PgExecutor<'_>,
    snapshot_id: SnapshotId,
    storage_uri: &ExactStorageUri,
    org_id: OrgId,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id, orphaned_at)
         SELECT NULL, $2, $3, NOW()
          WHERE NOT EXISTS (SELECT 1 FROM snapshots WHERE id = $1)"
    ))
    .bind(snapshot_id)
    .bind(storage_uri.as_wire())
    .bind(org_id)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Whether the sweeper still has `storage_uri` to delete: an orphan row
/// names it and the sweeper hasn't removed that row yet.
///
/// A writer that derives a prefix from a stable identity asks this before
/// writing into it again. The sweeper deletes the bytes before the row, so
/// while the row exists it may delete whatever the writer copies there.
pub async fn orphan_pending(
    executor: impl sqlx::PgExecutor<'_>,
    storage_uri: &ExactStorageUri,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
               FROM snapshot_storage_locations
              WHERE snapshot_id IS NULL
                AND orphaned_at IS NOT NULL
                AND storage_connection_string = $1
         )"
    ))
    .bind(storage_uri.as_wire())
    .fetch_one(executor)
    .await
}

#[cfg(test)]
mod tests;
