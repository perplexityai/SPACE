//! The retention reaper's write of `sandboxes`: the hard delete of rows that
//! have sat `deleted` past the tombstone retention.
//!
//! The cluster controller's tombstone-retention loop keeps everything around
//! it: the transaction, the node and template tombstones it prunes in the same
//! pass, the delete of the claimed warm-pool slots of the rows this removes,
//! and the tombstone counts it reports.

use uuid::Uuid;

use pplx_space_common_core::sql;

/// What one [`prune_deleted_sandboxes`] call removed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrunedDeletedSandboxes {
    /// The pruned sandbox rows' ids.
    pub sandbox_ids: Vec<Uuid>,
    /// Snapshot storage locations detached from the pruned rows' snapshots and
    /// handed to the snapshot-storage sweeper.
    pub storage_locations_detached: u64,
}

/// Delete up to `max_records` sandbox rows that have been `deleted` for longer
/// than `retention_secs`, oldest first. Returns their ids and how many storage
/// locations it detached.
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
/// them, and snapshot retention never clears them all: it keeps each
/// sandbox's newest `keep_n` `full` snapshots and every `disk` snapshot whose
/// origin isn't `stop` (named, fork-copied or origin-less).
///
/// A sandbox's snapshots go with its row, named ones (`artifact_origin =
/// 'snapshot'`) included, as on every other path that deletes the row. For a
/// census tombstone, this prune leaves them in place for the tombstone
/// retention ([`crate::DELETED_SANDBOX_RETENTION`], 7 days from the row's last
/// update); ordinary snapshot retention still applies to them meanwhile. Once
/// this prune detaches them, the sweeper deletes their objects after its
/// one-hour grace, except for locations in another bucket or with an
/// encrypted sidecar, which it leaves in place.
pub async fn prune_deleted_sandboxes(
    executor: impl sqlx::PgExecutor<'_>,
    retention_secs: i64,
    max_records: i64,
) -> Result<PrunedDeletedSandboxes, sqlx::Error> {
    let (sandbox_ids, storage_locations_detached) = sqlx::query_as::<_, (Vec<Uuid>, i64)>(sql!(
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
                 RETURNING ssl.id
             ),
             pruned AS (
                 DELETE FROM sandboxes s
                  USING candidates c
                  WHERE s.id = c.id
                  RETURNING s.id
             )
             SELECT ARRAY(SELECT id FROM pruned),
                    (SELECT COUNT(*) FROM detached_storage)"
    ))
    .bind(retention_secs)
    .bind(max_records)
    .fetch_one(executor)
    .await?;
    Ok(PrunedDeletedSandboxes {
        sandbox_ids,
        storage_locations_detached: u64::try_from(storage_locations_detached).unwrap_or(0),
    })
}
