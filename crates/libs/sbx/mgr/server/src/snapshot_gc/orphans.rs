//! Bounded filesystem discovery for immutable GC and reserved fork-copy staging.

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use super::{FORK_COPY_ORPHAN_MIN_AGE_SECS, SWEEP_MAX_DEPTH};
use crate::service::fork_artifact::CopyOperationReservations;

#[derive(Clone, Copy)]
pub(super) enum OrphanKind {
    GcPending,
    ForkCopy,
}

impl OrphanKind {
    /// Whether the walk enters a directory that is not itself a candidate,
    /// `depth` levels below the root. GC staging sits directly in a sandbox's
    /// directory or in one of its snapshot directories, the only shapes
    /// node-mgr's `DeleteCacheArtifact` accepts, so the rest of a sandbox's
    /// directory, its jail `instance` among them, is never read.
    fn descends_into(self, depth: usize, name: &std::ffi::OsStr) -> bool {
        match self {
            Self::GcPending => {
                depth < 2
                    || matches!(
                        name.to_str(),
                        Some(
                            pplx_space_common_core::types::SNAPSHOT_DIR_NAME
                                | "template-snapshot-staging"
                        )
                    )
            }
            Self::ForkCopy => true,
        }
    }
}

/// At most one directory handle per supported namespace depth, never a queue
/// proportional to directory width. Iterator positions survive each page.
#[derive(Default)]
pub(super) struct OrphanScan {
    directories: Vec<std::fs::ReadDir>,
}

impl OrphanScan {
    pub(super) fn next_page(
        &mut self,
        root: &Path,
        kind: OrphanKind,
        active: &CopyOperationReservations,
        now: u64,
    ) -> std::io::Result<Vec<PathBuf>> {
        if self.directories.is_empty() {
            match std::fs::read_dir(root) {
                Ok(entries) => self.directories.push(entries),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(error) => return Err(error),
            }
        }
        let max_depth = match kind {
            OrphanKind::GcPending => SWEEP_MAX_DEPTH,
            OrphanKind::ForkCopy => 1,
        };
        let mut found = Vec::new();
        for _ in 0..crate::db::snapshot_gc::PAGE_SIZE {
            let Some(entries) = self.directories.last_mut() else {
                break;
            };
            let entry = match entries.next() {
                Some(Ok(entry)) => entry,
                Some(Err(error)) => {
                    tracing::warn!(%error, "snapshot reaper: unreadable directory entry retained for a later sweep");
                    continue;
                }
                None => {
                    self.directories.pop();
                    continue;
                }
            };
            let path = entry.path();
            match entry.file_type() {
                Ok(kind) if kind.is_dir() => {}
                Ok(_) => continue,
                Err(error) => {
                    tracing::warn!(%error, path = %path.display(), "snapshot reaper: unreadable path retained");
                    continue;
                }
            }
            let candidate = match kind {
                OrphanKind::GcPending => entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.contains(".gc-pending-")),
                OrphanKind::ForkCopy => {
                    if crate::service::fork_artifact::is_copy_operation_reserved(active, &path) {
                        continue;
                    }
                    match entry.metadata() {
                        Ok(metadata) => u64::try_from(metadata.mtime()).is_ok_and(|modified| {
                            now.saturating_sub(modified) >= FORK_COPY_ORPHAN_MIN_AGE_SECS
                        }),
                        Err(error) => {
                            tracing::warn!(%error, path = %path.display(), "snapshot reaper: unknown fork-copy age retained");
                            false
                        }
                    }
                }
            };
            if candidate {
                found.push(path);
            } else if self.directories.len() < max_depth
                && kind.descends_into(self.directories.len(), &entry.file_name())
            {
                match std::fs::read_dir(&path) {
                    Ok(entries) => self.directories.push(entries),
                    Err(error) => {
                        tracing::warn!(%error, path = %path.display(), "snapshot reaper: unreadable subtree retained for a later sweep")
                    }
                }
            }
        }
        Ok(found)
    }
}
