//! Orphan storage locations: rows with no owning snapshot, whose bytes the
//! storage sweeper reaps.
//!
//! An orphan has both `orphaned_at` set and `snapshot_id` NULL. Every writer
//! that orphans a location sets both in one statement, so either column alone
//! identifies one today; both verbs check both, so a row that has only one
//! half is never treated as an orphan.

use std::time::Duration;

use pplx_space_sql_locator::sql;
use sqlx::FromRow;
use uuid::Uuid;

/// One orphan storage location.
#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct OrphanLocation {
    pub id: Uuid,
    /// The base row's route. Only a non-secret stand-in when
    /// `has_encrypted_fields` is set.
    pub storage_connection_string: String,
    /// Whether a `snapshot_storage_location_encrypted_fields` row exists for
    /// the location.
    pub has_encrypted_fields: bool,
}

/// Up to `limit` orphan locations with ids above `after`, in id order. With
/// `min_age`, only those orphaned longer ago than that by the database clock.
pub async fn page(
    executor: impl sqlx::PgExecutor<'_>,
    after: Option<Uuid>,
    limit: i64,
    min_age: Option<Duration>,
) -> Result<Vec<OrphanLocation>, sqlx::Error> {
    let min_age_secs = min_age.map(|d| d.as_secs().cast_signed());
    sqlx::query_as(sql!(
        "SELECT
                 ssl.id,
                 ssl.storage_connection_string,
                 EXISTS (
                     SELECT 1
                       FROM snapshot_storage_location_encrypted_fields ef
                      WHERE ef.snapshot_storage_location_id = ssl.id
                 ) AS has_encrypted_fields
             FROM snapshot_storage_locations ssl
             WHERE ssl.orphaned_at IS NOT NULL
               AND ssl.snapshot_id IS NULL
               AND ($1::UUID IS NULL OR ssl.id > $1)
               AND ($3::BIGINT IS NULL
                    OR ssl.orphaned_at < NOW() - make_interval(secs => $3::BIGINT))
             ORDER BY ssl.id ASC
             LIMIT $2"
    ))
    .bind(after)
    .bind(limit)
    .bind(min_age_secs)
    .fetch_all(executor)
    .await
}

/// Delete location `id` if it is still an orphan; its encrypted-fields row,
/// if any, cascades. `false` when the row is gone or no longer an orphan.
pub async fn delete_if_orphan(
    executor: impl sqlx::PgExecutor<'_>,
    id: Uuid,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "DELETE FROM snapshot_storage_locations
             WHERE id = $1
               AND orphaned_at IS NOT NULL
               AND snapshot_id IS NULL"
    ))
    .bind(id)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests;
