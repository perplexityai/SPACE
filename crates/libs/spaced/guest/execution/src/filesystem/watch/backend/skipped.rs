//! Directories a watch gave up on for lack of permission, and the rechecks
//! that bring them back.

use std::collections::{BTreeSet, HashSet};
use std::mem;
use std::path::{Path, PathBuf};

use notify::event::ModifyKind;
use notify::{Event, EventKind};

use super::super::MAX_SKIPPED_DIRECTORIES;
use super::PendingDirectoryDiscovery;

/// Queues the walks that verify the directories a scan newly watched, so a
/// directory created before its parent's watch is still found. Each is walked
/// again on its own, as discovery's are, unless the scan found it under a
/// recheck whose walk ran: a recovery walk can bring back a whole subtree at
/// once, and walking its recheck again covers all of it in one candidate.
/// A directory found under a nearer discovery candidate is still verified on
/// its own, so ongoing creation under a recovered subtree doesn't walk all of
/// it again. Returns false when the candidates overflow, as discovery's do.
pub(super) fn queue_verification(
    pending: &mut PendingDirectoryDiscovery,
    verify: Vec<PathBuf>,
    candidates: &BTreeSet<PathBuf>,
    walked_rechecks: &BTreeSet<PathBuf>,
) -> bool {
    let mut recovered = BTreeSet::new();
    let mut discovered = Vec::new();
    for path in verify {
        let found_under = path
            .ancestors()
            .find(|ancestor| candidates.contains(*ancestor));
        match found_under {
            Some(recheck) if walked_rechecks.contains(recheck) => {
                recovered.insert(recheck.to_path_buf());
            }
            _ => discovered.push(path),
        }
    }
    pending.extend_bounded(recovered.into_iter().chain(discovered))
}

/// The rechecks a scan walked. One the scan reports skipped was refused, so
/// its walk covered nothing under it.
pub(super) fn walked_rechecks(
    rechecks: &BTreeSet<PathBuf>,
    skipped: &[PathBuf],
) -> BTreeSet<PathBuf> {
    let refused = skipped.iter().collect::<HashSet<_>>();
    rechecks
        .iter()
        .filter(|recheck| !refused.contains(recheck))
        .cloned()
        .collect()
}

/// Queues the candidates a removal invalidated mid-scan for another scan.
/// Rechecks among them go back as rechecks, so only candidates, including
/// the verification of what a recheck brought back, can overflow the queue;
/// returns false when they do.
pub(super) fn requeue_invalidated(
    pending: &mut PendingDirectoryDiscovery,
    skipped: &mut SkippedDirectories,
    invalidated: BTreeSet<PathBuf>,
    rechecks: &BTreeSet<PathBuf>,
) -> bool {
    let (rechecks, candidates): (Vec<_>, Vec<_>) = invalidated
        .into_iter()
        .partition(|path| rechecks.contains(path));
    skipped.defer(pending.extend_best_effort(rechecks));
    pending.extend_bounded(candidates)
}

pub(super) fn is_metadata_change(event: &Event) -> bool {
    matches!(event.kind, EventKind::Modify(ModifyKind::Metadata(_)))
}

/// Directories the watch gave up on for lack of permission, remembered so a
/// permission change on one, or on a directory above it, walks it again. The
/// one remembered is the nearest the watch could still look up, worked out
/// after the refusal; matching changes above it as well keeps a permission
/// change that lands in between from leaving it unwatched. notify reports
/// IN_ATTRIB as a metadata change and can't tell a chmod from a touch, chown
/// or xattr change, so any of those walks it too. A `chmod -R` or timestamp
/// storm elsewhere costs an ordered-set lookup, not a stat.
///
/// Not every way back raises an event: a security-policy change or a mount
/// over a skipped directory does not, and nothing polls for them. Files
/// written while a directory was unwatched produce no events; the walk that
/// brings it back finds directories, not files.
#[derive(Default)]
pub(super) struct SkippedDirectories {
    pub(super) paths: BTreeSet<PathBuf>,
    // Rechecks already owed after a permission change that did not fit the
    // recheck queue yet; queued as scans free room.
    pub(super) due: BTreeSet<PathBuf>,
    pub(super) overflowed: bool,
}

impl SkippedDirectories {
    /// Past [`MAX_SKIPPED_DIRECTORIES`] remembered paths, due rechecks
    /// included, new skips are not remembered: the watch carries on without
    /// recovery for them rather than ending.
    pub(super) fn remember(&mut self, paths: impl IntoIterator<Item = PathBuf>) {
        for path in paths {
            if !self.has_room_for(&path) {
                self.note_overflow();
                return;
            }
            self.paths.insert(path);
        }
    }

    /// Keeps rechecks that did not fit the recheck queue, under the same bound.
    pub(super) fn defer(&mut self, paths: Vec<PathBuf>) {
        for path in paths {
            if !self.has_room_for(&path) {
                self.note_overflow();
                return;
            }
            self.due.insert(path);
        }
    }

    /// Moves due rechecks into `pending` as far as it has room.
    pub(super) fn queue_due(&mut self, pending: &mut PendingDirectoryDiscovery) {
        if !self.due.is_empty() {
            let due = mem::take(&mut self.due).into_iter().collect();
            self.due = pending.extend_best_effort(due).into_iter().collect();
        }
    }

    pub(super) fn has_room_for(&self, path: &Path) -> bool {
        self.paths.len() + self.due.len() < MAX_SKIPPED_DIRECTORIES
            || self.paths.contains(path)
            || self.due.contains(path)
    }

    pub(super) fn note_overflow(&mut self) {
        if !self.overflowed {
            self.overflowed = true;
            tracing::warn!(
                capacity = MAX_SKIPPED_DIRECTORIES,
                "file watcher stopped remembering unreadable directories"
            );
        }
    }

    pub(super) fn forget(&mut self, paths: &[PathBuf]) {
        for path in paths {
            self.paths.remove(path);
            self.due.remove(path);
        }
    }

    pub(super) fn forget_descendants(&mut self, removed_roots: &[PathBuf]) {
        if removed_roots.is_empty() {
            return;
        }
        for removed_root in removed_roots {
            take_at_or_below(&mut self.paths, removed_root);
        }
        self.due.retain(|path| {
            !removed_roots
                .iter()
                .any(|removed_root| path.starts_with(removed_root))
        });
    }

    /// Takes the remembered skips a metadata change names or lies above, to
    /// walk again; a walk that is still refused remembers them again.
    pub(super) fn take_hits(&mut self, event: &Event) -> Vec<PathBuf> {
        if self.paths.is_empty() || !is_metadata_change(event) {
            return Vec::new();
        }
        event
            .paths
            .iter()
            .flat_map(|path| take_at_or_below(&mut self.paths, path))
            .collect()
    }
}

/// Removes and returns `path` and everything under it. A path's descendants
/// sort directly after it, so this is a range, not a scan.
fn take_at_or_below(set: &mut BTreeSet<PathBuf>, path: &Path) -> Vec<PathBuf> {
    let taken = set
        .range::<Path, _>((std::ops::Bound::Included(path), std::ops::Bound::Unbounded))
        .take_while(|entry| entry.starts_with(path))
        .cloned()
        .collect::<Vec<_>>();
    for entry in &taken {
        set.remove(entry);
    }
    taken
}
