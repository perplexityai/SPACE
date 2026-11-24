//! Retention's discovery read, and the locks and re-checks its claim and
//! finalize take on a snapshot.
//!
//! A usable storage location is one with `orphaned_at IS NULL`; every
//! statement here that reads a location spells that out. The claim's
//! `ready -> deleting` update stays with the retention workflow (see the
//! crate docs); finalize deletes the snapshot and its optional location
//! through [`crate::delete_snapshot_catalog`].

use std::sync::LazyLock;
use std::time::Duration;

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::snapshot_is_live_restore_source;
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;
use sqlx::{FromRow, Postgres, Transaction};
use uuid::Uuid;

/// A `ready` snapshot discovery found prunable, with its optional usable
/// storage location.
#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct Candidate {
    pub snapshot_id: SnapshotId,
    pub sandbox_id: SandboxId,
    pub storage_location_id: Option<Uuid>,
    pub storage_connection_string: Option<String>,
    pub has_encrypted_fields: bool,
    /// Seconds since the snapshot's `timestamp`, by the database clock.
    pub age_secs: i64,
}

/// Up to `limit` prunable snapshots among `ids`, in id order: `ready`, older
/// than `min_age` by the database clock when it's set, not a snapshot a
/// restore path would currently load, and either a `stop` disk row or a full
/// row with at least `keep_n` newer ready full siblings that have a usable
/// location. `ids` must be ascending; their first and last bound the scan.
///
/// The `OFFSET 0` fences keep each keyed probe on its own index rather than a
/// fleet-wide join.
pub async fn candidates(
    executor: impl sqlx::PgExecutor<'_>,
    ids: &[SnapshotId],
    keep_n: i64,
    limit: i64,
    min_age: Option<Duration>,
) -> Result<Vec<Candidate>, sqlx::Error> {
    static CANDIDATES: LazyLock<String> = LazyLock::new(|| {
        let restore_source = snapshot_is_live_restore_source("sb", "s");
        sql!(format!(
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
                 SELECT id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
                        artifact_origin
                   FROM snapshots
                  WHERE id >= $5::UUID AND id <= $6::UUID
                    AND id = ANY($3::UUID[])
                  OFFSET 0
             ) s
             JOIN LATERAL (
                 SELECT id, lifecycle, lifecycle_changed_at, placement_changed_at,
                        current_local_artifact_id, current_local_artifact_kind
                   FROM sandboxes
                  WHERE id = s.sandbox_id
                  OFFSET 0
             ) sb ON TRUE
             LEFT JOIN LATERAL (
                 SELECT id, storage_connection_string
                   FROM snapshot_storage_locations
                  WHERE snapshot_id = s.id AND orphaned_at IS NULL
                  OFFSET 0
             ) ssl ON TRUE
             WHERE s.state = 'ready'
               AND ($4::BIGINT IS NULL
                    OR s.timestamp < NOW() - make_interval(secs => $4::BIGINT))
               AND NOT {restore_source}
               AND ((s.type = 'disk' AND s.artifact_origin = 'stop')
                    OR (s.type = 'full'
                        AND EXISTS (
                            SELECT 1
                              FROM snapshots newer
                             WHERE newer.sandbox_id = s.sandbox_id
                               AND newer.state = 'ready'
                               AND newer.type = 'full'
                               AND EXISTS (
                                   SELECT 1 FROM snapshot_storage_locations newer_ssl
                                    WHERE newer_ssl.snapshot_id = newer.id
                                      AND newer_ssl.orphaned_at IS NULL
                                    OFFSET 0
                               )
                               AND (newer.timestamp, newer.id) > (s.timestamp, s.id)
                            ORDER BY newer.timestamp DESC
                            OFFSET ($1 - 1)
                            LIMIT 1
                        )))
             ORDER BY s.id ASC
             LIMIT $2",
        ))
    });
    let min_age_secs = min_age.map(|d| d.as_secs().cast_signed());
    sqlx::query_as(sqlx::AssertSqlSafe(CANDIDATES.as_str()))
        .bind(keep_n)
        .bind(limit)
        .bind(ids)
        .bind(min_age_secs)
        .bind(ids.first().copied())
        .bind(ids.last().copied())
        .fetch_all(executor)
        .await
}

/// Lock snapshot `snapshot_id` of `sandbox_id` `FOR UPDATE` and return its
/// stored state, unparsed; `None` when the sandbox has no such snapshot. The
/// lock lasts until the caller's transaction ends.
///
/// Taken before the claim reads the snapshot's storage location. It blocks a
/// concurrent location insert, whose foreign-key check needs a key share lock
/// on this row, so no location can be added before the claim commits. It does
/// not lock an existing location row against changes to its other columns,
/// such as `orphaned_at` or `snapshot_id`: the claim takes that lock
/// separately with [`lock_usable_storage_location`], and a caller needs both
/// rather than the snapshot lock alone.
pub async fn lock_sandbox_snapshot_state(
    tx: &mut Transaction<'_, Postgres>,
    snapshot_id: SnapshotId,
    sandbox_id: SandboxId,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT state
               FROM snapshots
              WHERE id = $1
                AND sandbox_id = $2
              FOR UPDATE"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .fetch_optional(&mut **tx)
    .await
}

/// Lock snapshot `snapshot_id` `FOR UPDATE` and return its stored state,
/// unparsed; `None` when there's no such row. The lock lasts until the
/// caller's transaction ends.
pub async fn lock_snapshot_state(
    tx: &mut Transaction<'_, Postgres>,
    snapshot_id: SnapshotId,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT state
               FROM snapshots
              WHERE id = $1
              FOR UPDATE"
    ))
    .bind(snapshot_id)
    .fetch_optional(&mut **tx)
    .await
}

/// A snapshot's usable storage location, as the claim locked it.
#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct LockedStorageLocation {
    pub id: Uuid,
    pub storage_connection_string: String,
    pub has_encrypted_fields: bool,
}

/// Lock `FOR UPDATE` the usable storage location of snapshot `snapshot_id`
/// and return it; `None` when it has none. The lock lasts until the caller's
/// transaction ends.
pub async fn lock_usable_storage_location(
    tx: &mut Transaction<'_, Postgres>,
    snapshot_id: SnapshotId,
) -> Result<Option<LockedStorageLocation>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT ssl.id,
                    ssl.storage_connection_string,
                    EXISTS (
                        SELECT 1
                          FROM snapshot_storage_location_encrypted_fields ef
                         WHERE ef.snapshot_storage_location_id = ssl.id
                    ) AS has_encrypted_fields
               FROM snapshot_storage_locations ssl
              WHERE ssl.snapshot_id = $1
                AND ssl.orphaned_at IS NULL
              FOR UPDATE OF ssl"
    ))
    .bind(snapshot_id)
    .fetch_optional(&mut **tx)
    .await
}

/// The claim's re-check of `ready` snapshot `snapshot_id`, one flag per
/// guard.
#[derive(Clone, Copy, Debug, Eq, PartialEq, FromRow)]
pub struct ClaimSafety {
    /// A `stop` disk row, or a full row with at least `keep_n` newer ready
    /// full siblings that have a usable location. A `disk` row of any other
    /// origin, or of none, fails it: `IS NOT DISTINCT FROM` keeps a NULL origin
    /// `false` rather than a NULL the `bool` decode rejects.
    pub floor_ok: bool,
    /// No restore path would currently load it.
    pub restore_source_ok: bool,
    /// Older than `min_age` by the database clock, or no `min_age`.
    pub age_ok: bool,
}

/// Evaluate the claim's guards on snapshot `snapshot_id`; `None` when it's no
/// longer `ready` or its sandbox is gone. Run it in the claim's transaction,
/// after its locks, so it sees writers that committed while it waited.
pub async fn claim_safety(
    executor: impl sqlx::PgExecutor<'_>,
    snapshot_id: SnapshotId,
    keep_n: i64,
    min_age: Option<Duration>,
) -> Result<Option<ClaimSafety>, sqlx::Error> {
    static CLAIM_SAFETY: LazyLock<String> = LazyLock::new(|| {
        let restore_source = snapshot_is_live_restore_source("sb", "cur");
        sql!(format!(
            "SELECT ((cur.type = 'disk' AND cur.artifact_origin IS NOT DISTINCT FROM 'stop')
                     OR (cur.type = 'full'
                         AND EXISTS (
                             SELECT 1
                               FROM snapshots newer
                              WHERE newer.sandbox_id = cur.sandbox_id
                                AND newer.state = 'ready'
                                AND newer.type = 'full'
                                AND EXISTS (
             SELECT 1
              FROM snapshot_storage_locations newer_ssl
              WHERE newer_ssl.snapshot_id = newer.id
                AND newer_ssl.orphaned_at IS NULL
         )
                                AND (newer.timestamp, newer.id) > (cur.timestamp, cur.id)
                              OFFSET ($2 - 1)
                              LIMIT 1
                         ))) AS floor_ok,
             NOT {restore_source} AS restore_source_ok,
             ($3::BIGINT IS NULL
                OR cur.timestamp < NOW() - make_interval(secs => $3::BIGINT)) AS age_ok
             FROM snapshots cur
             JOIN sandboxes sb ON sb.id = cur.sandbox_id
             WHERE cur.id = $1
               AND cur.state = 'ready'",
        ))
    });
    let min_age_secs = min_age.map(|duration| duration.as_secs().cast_signed());
    sqlx::query_as(sqlx::AssertSqlSafe(CLAIM_SAFETY.as_str()))
        .bind(snapshot_id)
        .bind(keep_n)
        .bind(min_age_secs)
        .fetch_optional(executor)
        .await
}

/// Whether snapshot `snapshot_id` still meets the keep-N floor: it's a `stop`
/// disk row, or at least `keep_n` newer ready full siblings with a usable
/// location exist.
pub async fn floor_holds(
    executor: impl sqlx::PgExecutor<'_>,
    snapshot_id: SnapshotId,
    keep_n: i64,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS (
                 SELECT 1
                   FROM snapshots cur
                  WHERE cur.id = $1
                    AND cur.type = 'disk'
                    AND cur.artifact_origin = 'stop'
             )
             OR EXISTS (
                 SELECT 1
                   FROM snapshots newer
                   JOIN snapshots cur ON cur.id = $1
                  WHERE newer.sandbox_id = cur.sandbox_id
                    AND newer.state = 'ready'
                    AND newer.type = 'full'
                    AND EXISTS (
             SELECT 1
              FROM snapshot_storage_locations newer_ssl
              WHERE newer_ssl.snapshot_id = newer.id
                AND newer_ssl.orphaned_at IS NULL
         )
                    AND (newer.timestamp, newer.id) > (cur.timestamp, cur.id)
                  OFFSET ($2 - 1)
                  LIMIT 1
             )"
    ))
    .bind(snapshot_id)
    .bind(keep_n)
    .fetch_one(executor)
    .await
}
