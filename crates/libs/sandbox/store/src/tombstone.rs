//! The retention reaper's write of `sandboxes`: the hard delete of rows that
//! have sat `deleted` past the tombstone retention.
//!
//! The cluster controller's tombstone-retention loop keeps everything around
//! it: the transaction, the node and template tombstones it prunes in the same
//! pass, the delete of the claimed warm-pool slots of the rows this removes,
//! and the tombstone counts it reports.

use uuid::Uuid;

use pplx_space_common_core::sql;

/// Delete up to `max_records` sandbox rows that have been `deleted` for longer
/// than `retention_secs`, oldest first, and return their ids.
///
/// Writes an **inference**: the retention age concluding that a tombstone has
/// served its purpose. No guard beyond `lifecycle = 'deleted'` and the age,
/// and no row lock first; a `deleted` row has no further transition. It is not
/// [`crate::delete::delete_with_dependents`]: it removes only the row and what
/// cascades from it, so the caller deletes the returned ids' claimed warm-pool
/// slots in the same transaction.
pub async fn prune_deleted_sandboxes(
    executor: impl sqlx::PgExecutor<'_>,
    retention_secs: i64,
    max_records: i64,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar::<_, Uuid>(sql!(
        "WITH candidates AS (
                 SELECT s.id
                   FROM sandboxes s
                  WHERE s.lifecycle = 'deleted'
                    AND COALESCE(s.updated_at, s.created_at)
                        < NOW() - ($1::BIGINT * INTERVAL '1 second')
                  ORDER BY COALESCE(s.updated_at, s.created_at) ASC,
                           s.id ASC
                  LIMIT $2
             )
             DELETE FROM sandboxes s
              USING candidates c
              WHERE s.id = c.id
              RETURNING s.id"
    ))
    .bind(retention_secs)
    .bind(max_records)
    .fetch_all(executor)
    .await
}
