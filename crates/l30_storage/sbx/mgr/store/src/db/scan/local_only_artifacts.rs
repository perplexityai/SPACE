//! Every artifact the node holds without a durable remote copy.

use crate::db::{Connection, LocalArtifactRow, Result};
use pplx_space_common_core::sql;

/// Every `local_artifacts` row with no backup-completed stamp, named
/// snapshots and superseded lifecycle artifacts included, in sandbox then
/// artifact id order. A row a delete has reserved (`deleting`) is going
/// away and is left out. One row that does not decode fails the read.
pub fn local_only_artifacts(conn: &Connection) -> Result<Vec<LocalArtifactRow>> {
    let mut statement = conn.prepare(sql!(
        "SELECT local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                snapshot_name, local_path
           FROM local_artifacts
          WHERE backup_completed_at IS NULL AND state <> 'deleting'
          ORDER BY sandbox_id, local_artifact_id"
    ))?;
    Ok(statement
        .query_map([], crate::db::local_artifacts::row_to_local_artifact)?
        .collect::<rusqlite::Result<_>>()?)
}
