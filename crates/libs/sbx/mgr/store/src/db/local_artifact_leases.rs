//! Statements behind `crate::local_artifacts::LocalArtifacts` and its
//! restore and delete leases. Each runs on the connection or transaction its
//! caller holds, so a checkout's reads and its CAS share one transaction.

use crate::db::{Connection, Error, OptionalExtension, Result};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{ArtifactKind, LocalArtifactId, SnapshotName};
use pplx_space_sqlite::SqliteU64;
use rusqlite::params;

use crate::db::timestamps::EpochSeconds;

/// One sandbox's current lifecycle artifact that this node cannot prove has
/// a durable remote copy.
///
/// Identity comes from the `sandboxes` pin rather than from the artifact
/// row, because the case where there *is* no artifact row is one of the
/// cases this reports. Migration 026's CHECK keeps `current_local_artifact_id`
/// and `current_local_artifact_kind` set or unset together, so the pin alone
/// always carries a whole identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingBackup {
    pub sandbox_id: String,
    pub local_artifact_id: LocalArtifactId,
    pub artifact_kind: ArtifactKind,
}

/// Logical state of a `local_artifacts` row. Encoded on the row's
/// `state` column; transitions are CAS via
/// `LocalArtifacts::check_out_for_restore` /
/// `LocalArtifacts::check_out_for_delete`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactState {
    /// Published and available. The only state from which a checkout
    /// (restore or delete) can succeed.
    Ready,
    /// A restore is currently reading this artifact's `local_path`.
    /// Reapers must skip; deleters must wait or fail with NotReady.
    Restoring,
    /// A delete has reserved this row. The on-disk directory has
    /// usually been renamed to a `.gc-pending-<id>` sibling. Readers
    /// should treat the artifact as gone.
    Deleting,
}

impl ArtifactState {
    pub fn as_str(self) -> &'static str {
        match self {
            ArtifactState::Ready => "ready",
            ArtifactState::Restoring => "restoring",
            ArtifactState::Deleting => "deleting",
        }
    }
}

/// Error parsing an [`ArtifactState`] from its wire/DB string. The
/// CHECK constraint on `local_artifacts.state` should make this
/// unreachable for rows the schema accepted; it surfaces table
/// corruption or migration drift.
#[derive(Debug, thiserror::Error)]
#[error("unrecognized artifact state {0:?}")]
pub struct ParseArtifactStateError(String);

impl std::str::FromStr for ArtifactState {
    type Err = ParseArtifactStateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ready" => Ok(ArtifactState::Ready),
            "restoring" => Ok(ArtifactState::Restoring),
            "deleting" => Ok(ArtifactState::Deleting),
            _ => Err(ParseArtifactStateError(s.to_string())),
        }
    }
}

impl std::fmt::Display for ArtifactState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Point-in-time publication evidence for a named snapshot, not a checkout lease.
pub struct NamedArtifactPublication {
    pub snapshot_name: SnapshotName,
    pub local_path: String,
    pub artifact_kind: ArtifactKind,
    pub state: ArtifactState,
}

/// Every sandbox whose pinned artifact has no backup-completed stamp, or
/// whose pin names no row it owns. The caller,
/// `LocalArtifacts::list_pending_backups`, documents the contract.
pub fn list_pending_backups(conn: &Connection) -> Result<Vec<PendingBackup>> {
    conn.prepare(
        "SELECT s.id, s.current_local_artifact_id, s.current_local_artifact_kind
           FROM sandboxes AS s
           LEFT JOIN local_artifacts AS a
             ON a.local_artifact_id = s.current_local_artifact_id
            AND a.sandbox_id = s.id
          WHERE s.current_local_artifact_id IS NOT NULL
            AND (a.local_artifact_id IS NULL OR a.backup_completed_at IS NULL)
          ORDER BY s.id",
    )?
    .query_map([], |row| {
        Ok(PendingBackup {
            sandbox_id: row.get(0)?,
            local_artifact_id: row.get::<_, String>(1)?.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
            artifact_kind: row.get::<_, String>(2)?.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
        })
    })?
    .collect::<rusqlite::Result<_>>()
    .map_err(crate::db::Error::from)
}

/// A sandbox's named-snapshot artifacts, within the inventory read budget.
pub fn list_named_publications(
    conn: &Connection,
    sandbox_id: &str,
) -> std::result::Result<Vec<NamedArtifactPublication>, crate::db::inventory_read::InventoryReadError>
{
    use pplx_space_common_core::inventory::{InventoryBudget, MAX_ITEMS, MAX_RAW_BYTES};
    let mut statement = conn.prepare(sql!(
        "SELECT snapshot_name, local_path, artifact_kind, state
         FROM local_artifacts WHERE sandbox_id = ?1 AND artifact_origin = 'snapshot'"
    ))?;
    let mut rows = statement.query([sandbox_id])?;
    let mut budget = InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES);
    let mut publications = Vec::new();
    while let Some(row) = rows.next()? {
        crate::db::inventory_read::check_row(row, &mut budget)?;
        publications.push(NamedArtifactPublication {
            snapshot_name: row.get::<_, String>(0)?.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
            local_path: row.get(1)?,
            artifact_kind: row.get::<_, String>(2)?.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
            state: row.get::<_, String>(3)?.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
        });
    }
    Ok(publications)
}

/// `(local_path, state)` of an artifact row, as stored.
pub fn artifact_path_and_state(
    conn: &Connection,
    local_artifact_id: &str,
) -> Result<Option<(String, String)>> {
    conn.query_row(
        "SELECT local_path, state FROM local_artifacts WHERE local_artifact_id = ?1",
        params![local_artifact_id],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
    )
    .optional()
}

/// CAS `ready` → `restoring`; the number of rows changed.
pub fn mark_artifact_restoring(conn: &Connection, local_artifact_id: &str) -> Result<usize> {
    conn.execute(
        "UPDATE local_artifacts SET state = 'restoring'
         WHERE local_artifact_id = ?1 AND state = 'ready'",
        params![local_artifact_id],
    )
}

/// CAS `ready` → `deleting`; the number of rows changed.
pub fn mark_artifact_deleting(conn: &Connection, local_artifact_id: &str) -> Result<usize> {
    conn.execute(
        "UPDATE local_artifacts SET state = 'deleting'
         WHERE local_artifact_id = ?1 AND state = 'ready'",
        params![local_artifact_id],
    )
}

/// The GC checkout's evidence for an artifact `sandbox_id` owns: its
/// creation and last-touch times, whether a sandbox pins it, and whether its
/// backup completed.
pub fn gc_checkout_evidence(
    conn: &Connection,
    local_artifact_id: &str,
    sandbox_id: &str,
) -> Result<Option<(EpochSeconds, EpochSeconds, bool, bool)>> {
    conn.query_row(
        "SELECT created_at, COALESCE(last_touched_at, created_at),
            EXISTS(
                SELECT 1 FROM sandboxes
                 WHERE current_local_artifact_id = ?1
                    OR suspend_predecessor_artifact_id = ?1
            ),
            backup_completed_at IS NOT NULL
       FROM local_artifacts WHERE local_artifact_id = ?1 AND sandbox_id = ?2",
        params![local_artifact_id, sandbox_id],
        |row| {
            Ok((
                row.get::<_, EpochSeconds>(0)?,
                row.get::<_, EpochSeconds>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, bool>(3)?,
            ))
        },
    )
    .optional()
}

/// Whether a sandbox pins the artifact as its current or suspend-predecessor
/// artifact.
pub fn artifact_is_pinned(conn: &Connection, local_artifact_id: &str) -> Result<bool> {
    conn.query_row(
        sql!(
            "SELECT EXISTS(SELECT 1 FROM sandboxes
               WHERE current_local_artifact_id = ?1 OR suspend_predecessor_artifact_id = ?1)"
        ),
        [local_artifact_id],
        |row| row.get(0),
    )
}

/// The number of artifact rows at `local_path`.
pub fn count_artifacts_at_path(conn: &Connection, local_path: &str) -> Result<u64> {
    conn.query_row(
        "SELECT COUNT(*) FROM local_artifacts WHERE local_path = ?1",
        params![local_path],
        |r| Ok(r.get::<_, SqliteU64>(0)?.0),
    )
}

/// Reset every `restoring` row to `ready`; the number reset.
pub fn reset_restoring_artifacts(conn: &Connection) -> Result<usize> {
    conn.execute(
        "UPDATE local_artifacts SET state = 'ready' WHERE state = 'restoring'",
        [],
    )
}

/// `(local_artifact_id, local_path)` of every `deleting` row.
pub fn list_deleting_artifacts(conn: &Connection) -> Result<Vec<(String, String)>> {
    Ok(conn
        .prepare(
            "SELECT local_artifact_id, local_path FROM local_artifacts
         WHERE state = 'deleting'",
        )?
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<_>>()?)
}

/// CAS `deleting` → `ready`; the number of rows changed.
pub fn roll_back_deleting_artifact(conn: &Connection, local_artifact_id: &str) -> Result<usize> {
    conn.execute(
        "UPDATE local_artifacts SET state = 'ready'
         WHERE local_artifact_id = ?1 AND state = 'deleting'",
        params![local_artifact_id],
    )
}

/// Delete the row if it is still `deleting`; the number deleted.
pub fn delete_deleting_artifact(conn: &Connection, local_artifact_id: &str) -> Result<usize> {
    conn.execute(
        "DELETE FROM local_artifacts
         WHERE local_artifact_id = ?1 AND state = 'deleting'",
        params![local_artifact_id],
    )
}

/// CAS `restoring` → `ready`, touching the artifact now.
pub fn release_restoring_artifact_with_touch(
    conn: &Connection,
    local_artifact_id: &str,
) -> Result<usize> {
    conn.execute(
        "UPDATE local_artifacts
         SET state = 'ready', last_touched_at = ?1
         WHERE local_artifact_id = ?2 AND state = 'restoring'",
        params![crate::db::now()?, local_artifact_id],
    )
}

/// CAS `restoring` → `ready`, leaving the touch time alone.
pub fn release_restoring_artifact(conn: &Connection, local_artifact_id: &str) -> Result<usize> {
    conn.execute(
        "UPDATE local_artifacts SET state = 'ready'
         WHERE local_artifact_id = ?1 AND state = 'restoring'",
        params![local_artifact_id],
    )
}

/// Whether neither an artifact row nor a sandbox pin names the id.
pub fn gc_staging_is_unowned(conn: &Connection, local_artifact_id: &str) -> Result<bool> {
    conn.query_row(
        sql!(
            "SELECT NOT EXISTS(SELECT 1 FROM local_artifacts WHERE local_artifact_id = ?1)
              AND NOT EXISTS(SELECT 1 FROM sandboxes
               WHERE current_local_artifact_id = ?1 OR suspend_predecessor_artifact_id = ?1)"
        ),
        [local_artifact_id],
        |row| row.get(0),
    )
}

/// The highest artifact rowid, `None` for an empty table.
pub fn max_artifact_rowid(conn: &Connection) -> Result<Option<i64>> {
    conn.query_row("SELECT MAX(rowid) FROM local_artifacts", [], |r| r.get(0))
}

/// An orphan-scan row's `(local_artifact_id, local_path, owned)`, or the
/// error decoding them.
pub(crate) type OrphanScanRow = Result<(String, String, bool)>;

/// Up to `limit` artifact rows with rowids in `next..=through`, in rowid
/// order: each rowid, and its `(local_artifact_id, local_path, owned)` or the
/// error decoding them, so one malformed row does not fail the scan.
pub fn orphan_scan_rows(
    conn: &Connection,
    next: i64,
    through: i64,
    limit: i64,
) -> Result<Vec<(i64, OrphanScanRow)>> {
    Ok(conn
        .prepare(
            "SELECT la.rowid, la.local_artifact_id, la.local_path,
                EXISTS (SELECT 1 FROM sandboxes s WHERE s.id = la.sandbox_id)
           FROM local_artifacts la
          WHERE la.rowid >= ?1 AND la.rowid <= ?2
          ORDER BY la.rowid LIMIT ?3",
        )?
        .query_map(params![next, through, limit], |row| {
            let decoded = (|| -> rusqlite::Result<(String, String, bool)> {
                Ok((row.get(1)?, row.get(2)?, row.get(3)?))
            })()
            .map_err(Error::from);
            Ok((row.get::<_, i64>(0)?, decoded))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Delete the artifact row if its sandbox row is gone; the number deleted.
pub fn delete_orphan_artifact_row(conn: &Connection, local_artifact_id: &str) -> Result<usize> {
    conn.execute(
        "DELETE FROM local_artifacts
          WHERE local_artifact_id = ?1
            AND NOT EXISTS (
                SELECT 1
                  FROM sandboxes s
                 WHERE s.id = local_artifacts.sandbox_id
            )",
        params![local_artifact_id],
    )
}
