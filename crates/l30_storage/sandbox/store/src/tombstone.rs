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
///
/// Like that helper, it detaches the storage locations of the rows'
/// snapshots and stamps them orphaned for the snapshot-storage sweeper, here
/// in the same statement as the delete. The census tombstone
/// ([`crate::ingest::prune_census_absent_sandboxes`]) keeps a row's snapshots
/// and their attached locations, and the locations' foreign key to
/// `snapshots` is `ON DELETE RESTRICT`: left attached, one such row fails the
/// whole batch, and every later run fails on it again. Nothing else detaches
/// them, and snapshot retention never clears them all: it removes only `full`
/// snapshots beyond each sandbox's newest `keep_n`, and no `disk` snapshot.
pub async fn prune_deleted_sandboxes(
    executor: impl sqlx::PgExecutor<'_>,
    retention_secs: i64,
    max_records: i64,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar::<_, Uuid>(sql!(
        "WITH candidates AS MATERIALIZED (
                 SELECT s.id
                   FROM sandboxes s
                  WHERE s.lifecycle = 'deleted'
                    AND COALESCE(s.updated_at, s.created_at)
                        < NOW() - ($1::BIGINT * INTERVAL '1 second')
                  ORDER BY COALESCE(s.updated_at, s.created_at) ASC,
                           s.id ASC
                  LIMIT $2
             ),
             detached_storage AS (
                 UPDATE snapshot_storage_locations ssl
                    SET snapshot_id = NULL, orphaned_at = NOW()
                  WHERE ssl.snapshot_id IN (
                      SELECT snapshots.id
                        FROM snapshots
                        JOIN candidates c ON c.id = snapshots.sandbox_id
                  )
                 RETURNING 1
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
