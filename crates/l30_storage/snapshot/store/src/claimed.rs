//! Reads of the snapshots a retention claim has parked in `deleting`.
//!
//! A claim is durable: the prune transaction commits `deleting` before any
//! bytes are reaped, so a restarted reconciler finishes the claims it finds
//! here instead of waiting for fresh discovery to revisit them.

use std::sync::LazyLock;

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::deleting_snapshot_matches_artifact;
use pplx_space_snapshot_model::{ArtifactKind, LocalArtifactId, SnapshotId};
use pplx_space_sql_locator::sql;
use sqlx::{FromRow, Postgres, Transaction};
use uuid::Uuid;

/// Upper bound of the durable claims present when a recovery epoch starts.
///
/// The `OFFSET 0` fence keeps the claim filter on the `(state, type)` index.
/// Without it, `ORDER BY id DESC LIMIT 1` lets the planner walk the primary
/// key backward and filter every row, expecting a claim near the newest IDs.
/// A `deleting` estimate larger than the live claim set, such as statistics
/// sampled while a tick holds its claims, makes that plan look cheap. Claims
/// are old snapshots with low UUIDv7 IDs, so the walk reads nearly the whole
/// table and can exceed the statement timeout while fresh discovery keeps
/// working. With the fence, the cost scales with the claims, not the table.
const SCAN_EPOCH_SQL: &str = sql!(
    "SELECT claimed.id
       FROM (
             SELECT id
               FROM snapshots
              WHERE state = 'deleting'
             OFFSET 0
            ) claimed
      ORDER BY claimed.id DESC
      LIMIT 1"
);

/// One recovery page of durable claims in `($2, $3]`, oldest first.
///
/// Same `OFFSET 0` fence as [`SCAN_EPOCH_SQL`]: the `LIMIT` must not turn the
/// ID range into a primary-key walk that filters unclaimed rows.
const LIST_SQL: &str = sql!(
    "SELECT
         s.id AS snapshot_id,
         s.sandbox_id,
         ssl.id AS storage_location_id,
         ssl.storage_connection_string,
         EXISTS (
             SELECT 1
               FROM snapshot_storage_location_encrypted_fields ef
              WHERE ef.snapshot_storage_location_id = ssl.id
         ) AS has_encrypted_fields,
         EXTRACT(EPOCH FROM (NOW() - s.timestamp))::BIGINT AS age_secs
     FROM (
           SELECT id, sandbox_id, timestamp
             FROM snapshots
            WHERE state = 'deleting'
              AND id > COALESCE($2::UUID, '00000000-0000-0000-0000-000000000000'::UUID)
              AND id <= $3
           OFFSET 0
          ) s
     LEFT JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = s.id
     ORDER BY s.id ASC
     LIMIT $1"
);

/// One claimed snapshot with its optional storage location, as [`list`]
/// projects it.
///
/// The location columns come from a left join: a claim with no location
/// row attached to its snapshot has neither. The store does not check that
/// the two agree; the caller decides what a mismatch means.
#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct ClaimedSnapshot {
    pub snapshot_id: SnapshotId,
    pub sandbox_id: SandboxId,
    pub storage_location_id: Option<Uuid>,
    pub storage_connection_string: Option<String>,
    /// Whether an encrypted-fields sidecar exists for the location; always
    /// `false` without one.
    pub has_encrypted_fields: bool,
    /// Whole seconds since the snapshot's `timestamp`, by the database clock.
    pub age_secs: i64,
}

/// The highest id among the snapshots currently claimed (`deleting`), or
/// `None` when there are none. A recovery pass pages claims up to this id with
/// [`list`]. The bound is an id, not a fixed set of claims: a claim with a
/// higher id waits for a later pass, and a snapshot claimed during the pass
/// with an id at or below the bound can still be paged in it.
pub async fn scan_epoch(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Option<SnapshotId>, sqlx::Error> {
    sqlx::query_scalar(SCAN_EPOCH_SQL)
        .fetch_optional(executor)
        .await
}

/// Up to `limit` claimed snapshots with ids in `(after, through]`, oldest id
/// first. `after` is `None` for the first page.
pub async fn list(
    executor: impl sqlx::PgExecutor<'_>,
    limit: i64,
    after: Option<SnapshotId>,
    through: SnapshotId,
) -> Result<Vec<ClaimedSnapshot>, sqlx::Error> {
    sqlx::query_as(LIST_SQL)
        .bind(limit)
        .bind(after)
        .bind(through)
        .fetch_all(executor)
        .await
}

/// Whether a claim has parked a snapshot of `sandbox_id`'s local artifact
/// `artifact_id` of kind `artifact_kind` in `deleting` while no ready copy of
/// that artifact keeps a live location.
///
/// Run it after locking the sandbox row in the same transaction. Retention
/// takes that lock before it claims a snapshot, so this read sees the
/// committed claim and a caller can refuse to pin an artifact whose bytes may
/// be going.
pub async fn deleting_without_ready_copy(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    artifact_id: LocalArtifactId,
    artifact_kind: ArtifactKind,
) -> Result<bool, sqlx::Error> {
    static DELETING_WITHOUT_READY_COPY: LazyLock<String> = LazyLock::new(|| {
        let deleting_snapshot = deleting_snapshot_matches_artifact("$1", "$2", "$3");
        sql!(format!("SELECT {deleting_snapshot}"))
    });
    sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(DELETING_WITHOUT_READY_COPY.as_str()))
        .bind(sandbox_id.as_uuid())
        .bind(artifact_id.as_uuid())
        .bind(artifact_kind.as_str())
        .fetch_one(&mut **tx)
        .await
}

#[cfg(test)]
mod tests;
