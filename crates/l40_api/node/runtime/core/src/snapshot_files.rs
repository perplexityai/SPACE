//! File completeness checks before destructive snapshot rollback.
//!
//! These checks establish file presence, not image integrity, and do not replace
//! artifact exclusion through the subsequent reads and disk replacement.

use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum SnapshotInputError {
    #[error("inspect snapshot input {path}: {source}")]
    Inspect {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("snapshot input {0} must be a nonempty regular file, not a symlink")]
    InvalidFile(PathBuf),
}

/// A full memory-only transfer is valid, but cannot replace an instance's disk.
pub fn validate_snapshot_rootfs(snapshot: &Path) -> Result<(), SnapshotInputError> {
    require_file(snapshot.join("rootfs.ext4"))
}

/// Check both inputs before stopping a VM for full-state rollback.
pub fn validate_snapshot_memory(snapshot: &Path) -> Result<(), SnapshotInputError> {
    require_file(snapshot.join("vmstate"))?;
    require_file(snapshot.join("memory"))
}

fn require_file(path: PathBuf) -> Result<(), SnapshotInputError> {
    let metadata =
        std::fs::symlink_metadata(&path).map_err(|source| SnapshotInputError::Inspect {
            path: path.clone(),
            source,
        })?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(SnapshotInputError::InvalidFile(path));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
