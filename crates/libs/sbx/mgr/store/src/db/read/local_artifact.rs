//! One `local_artifacts` row, by identity or by snapshot name, and whether
//! anything still owns an artifact identity.

use pplx_space_common_core::types::{LocalArtifactId, SnapshotName};

use crate::db::{Connection, LocalArtifactRow, Result};

/// How [`local_artifact`] finds its row.
#[derive(Clone, Copy, Debug)]
pub enum LocalArtifactKey<'a> {
    Id(LocalArtifactId),
    /// The sandbox's snapshot-origin artifact with this name.
    Named {
        sandbox_id: &'a str,
        snapshot_name: &'a SnapshotName,
    },
}

/// The artifact row, `None` without one; a row that does not decode fails
/// the read.
pub fn local_artifact(
    conn: &Connection,
    key: LocalArtifactKey<'_>,
) -> Result<Option<LocalArtifactRow>> {
    match key {
        LocalArtifactKey::Id(local_artifact_id) => {
            crate::db::get_local_artifact(conn, local_artifact_id)
        }
        LocalArtifactKey::Named {
            sandbox_id,
            snapshot_name,
        } => crate::db::get_named_local_artifact(conn, sandbox_id, snapshot_name),
    }
}

/// Whether neither an artifact row nor any sandbox's current or
/// suspend-predecessor pin names `local_artifact_id`, in one statement.
/// Staging under an unowned identity is garbage.
pub fn local_artifact_unowned(
    conn: &Connection,
    local_artifact_id: LocalArtifactId,
) -> Result<bool> {
    crate::db::local_artifact_leases::gc_staging_is_unowned(conn, &local_artifact_id.to_string())
}
