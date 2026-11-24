//! Bounded filesystem discovery for immutable GC and reserved fork-copy staging.

use std::collections::{BinaryHeap, VecDeque};
use std::ffi::{OsStr, OsString};
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
                            pplx_space_artifact_model::SNAPSHOT_DIR_NAME
                                | "template-snapshot-staging"
                        )
                    )
            }
            Self::ForkCopy => true,
        }
    }
}

struct ScanFrame {
    path: PathBuf,
    after: Option<OsString>,
}

struct QueuedEntry {
    name: OsString,
    entry: std::fs::DirEntry,
}

impl PartialEq for QueuedEntry {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for QueuedEntry {}

impl PartialOrd for QueuedEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueuedEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

struct DirBatch {
    entries: VecDeque<std::fs::DirEntry>,
    exhausted: bool,
}

/// Stream one fresh `read_dir` pass over `dir` and return up to `limit` entries
/// strictly after `after` in ascending name order, keeping at most `limit`
/// entries in memory and closing the directory handle before returning.
fn read_next_batch(dir: &Path, after: Option<&OsStr>, limit: usize) -> std::io::Result<DirBatch> {
    let entries = std::fs::read_dir(dir)?;
    let mut heap: BinaryHeap<QueuedEntry> = BinaryHeap::with_capacity(limit);
    let mut exhausted = true;
    for item in entries {
        let entry = match item {
            Ok(entry) => entry,
            Err(error) => {
                tracing::warn!(%error, "snapshot reaper: unreadable directory entry retained for a later sweep");
                continue;
            }
        };
        let name = entry.file_name();
        if after.is_some_and(|cursor| name.as_os_str() <= cursor) {
            continue;
        }
        if heap.len() < limit {
            heap.push(QueuedEntry { name, entry });
        } else {
            exhausted = false;
            if let Some(mut max) = heap.peek_mut()
                && name < max.name
            {
                *max = QueuedEntry { name, entry };
            }
        }
    }
    let entries = heap
        .into_sorted_vec()
        .into_iter()
        .map(|queued| queued.entry)
        .collect();
    Ok(DirBatch { entries, exhausted })
}

/// At most one name cursor per supported namespace depth, never a queue
/// proportional to directory width and never an open directory handle across
/// pages. Each page reopens the directory at its current cursor so entries
/// removed between ticks are not replayed from a stale listing.
#[derive(Default)]
pub(super) struct OrphanScan {
    frames: Vec<ScanFrame>,
}

impl OrphanScan {
    pub(super) fn next_page(
        &mut self,
        root: &Path,
        kind: OrphanKind,
        active: &CopyOperationReservations,
        now: u64,
    ) -> std::io::Result<Vec<PathBuf>> {
        let max_depth = match kind {
            OrphanKind::GcPending => SWEEP_MAX_DEPTH,
            OrphanKind::ForkCopy => 1,
        };
        let mut resumed_depth = self.frames.len();
        if self.frames.is_empty() {
            self.frames.push(ScanFrame {
                path: root.to_path_buf(),
                after: None,
            });
        }
        let mut batches: Vec<Option<DirBatch>> = (0..self.frames.len()).map(|_| None).collect();
        let mut found = Vec::new();
        for step in 0..crate::db::snapshot_gc::PAGE_SIZE {
            let remaining = crate::db::snapshot_gc::PAGE_SIZE - step;
            let Some(depth) = self.frames.len().checked_sub(1) else {
                break;
            };
            let (Some(frame), Some(batch_slot)) = (self.frames.last_mut(), batches.last_mut())
            else {
                break;
            };
            let batch = match batch_slot {
                Some(batch) => batch,
                None => match read_next_batch(&frame.path, frame.after.as_deref(), remaining) {
                    Ok(batch) => batch_slot.insert(batch),
                    Err(error) => {
                        let was_resumed = depth < resumed_depth;
                        let path = frame.path.clone();
                        self.frames.pop();
                        batches.pop();
                        resumed_depth = resumed_depth.min(self.frames.len());
                        if depth == 0 {
                            if error.kind() == std::io::ErrorKind::NotFound {
                                break;
                            }
                            return Err(error);
                        }
                        if was_resumed && error.kind() == std::io::ErrorKind::NotFound {
                            continue;
                        }
                        tracing::warn!(%error, path = %path.display(), "snapshot reaper: unreadable subtree retained for a later sweep");
                        continue;
                    }
                },
            };
            let Some(entry) = batch.entries.pop_front() else {
                if batch.exhausted {
                    self.frames.pop();
                    batches.pop();
                    resumed_depth = resumed_depth.min(self.frames.len());
                } else {
                    *batch_slot = None;
                }
                continue;
            };
            let name = entry.file_name();
            frame.after = Some(name.clone());
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
                OrphanKind::GcPending => name
                    .to_str()
                    .is_some_and(crate::local_artifacts::is_gc_staging_name),
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
            } else if self.frames.len() < max_depth && kind.descends_into(self.frames.len(), &name)
            {
                self.frames.push(ScanFrame { path, after: None });
                batches.push(None);
            }
        }
        Ok(found)
    }
}
