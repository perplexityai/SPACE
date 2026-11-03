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
///
/// The same skips are what the watch reports as not covered, apart from
/// directories remembered only to recover another; see
/// [`Self::remember_for_recovery`]. `unwatched` keeps reporting a skip while
/// `paths` and `due` give it up to be walked again, and
/// [`Self::take_changes`] reports the net difference since it last ran, so a
/// directory that is forgotten and refused again within one step is never
/// reported as watched.
#[derive(Default)]
pub(super) struct SkippedDirectories {
    /// Skips a metadata change walks again.
    pub(super) paths: BTreeSet<PathBuf>,
    /// Rechecks already owed after a permission change that did not fit the
    /// recheck queue yet; queued as scans free room.
    pub(super) due: BTreeSet<PathBuf>,
    /// Skips the watch reports, including those being walked again.
    pub(super) unwatched: BTreeSet<PathBuf>,
    /// Past [`MAX_SKIPPED_DIRECTORIES`], skips are no longer kept for recovery.
    pub(super) overflowed: bool,
    /// Past [`MAX_SKIPPED_DIRECTORIES`], skips are no longer reported.
    pub(super) incomplete: bool,
    changes: CoverageChange,
    /// The watch root, which the cap on reported skips leaves out.
    root: Option<PathBuf>,
}

/// How the watch's coverage changed, as absolute paths.
#[derive(Debug, Default, PartialEq, Eq)]
pub(in super::super) struct CoverageChange {
    /// Newly skipped directories.
    pub(in super::super) unwatched: BTreeSet<PathBuf>,
    /// Skipped directories that are watched again, or were removed with
    /// their subtree; changes under them were missed. A consumer whose globs
    /// hide a removal still learns the gap ended.
    pub(in super::super) rewatched: BTreeSet<PathBuf>,
    /// Some skips were neither remembered nor reported.
    pub(in super::super) truncated: bool,
    /// A clean, complete walk of the root listed every skip again, so an
    /// earlier truncation no longer applies. Set before anything the same
    /// step couldn't report sets `truncated` again.
    pub(in super::super) complete: bool,
}

impl CoverageChange {
    pub(super) fn is_empty(&self) -> bool {
        self.unwatched.is_empty() && self.rewatched.is_empty() && !self.truncated && !self.complete
    }
}

impl SkippedDirectories {
    pub(super) fn for_root(root: PathBuf) -> Self {
        Self {
            root: Some(root),
            ..Self::default()
        }
    }

    fn is_root(&self, path: &Path) -> bool {
        self.root.as_deref() == Some(path)
    }

    /// Reports `paths` as skips and keeps them for recovery. Past
    /// [`MAX_SKIPPED_DIRECTORIES`] reported skips, new ones aren't reported
    /// and the watch says its list is incomplete; past as many kept for
    /// recovery, they aren't walked again. The watch carries on either way.
    pub(super) fn remember(&mut self, paths: impl IntoIterator<Item = PathBuf>) {
        for path in paths {
            self.report_one(&path);
            self.keep_for_recovery(path);
        }
    }

    /// Reports `paths` as skips without keeping them for recovery, for
    /// rechecks already queued or due to be walked again.
    pub(super) fn report(&mut self, paths: impl IntoIterator<Item = PathBuf>) {
        for path in paths {
            self.report_one(&path);
        }
    }

    fn report_one(&mut self, path: &PathBuf) {
        // A skip that ended earlier in this step never stopped, whatever the
        // cap: it was counted before.
        if self.changes.rewatched.remove(path) {
            self.unwatched.insert(path.clone());
        }
        if self.unwatched.contains(path) {
            return;
        }
        // The cap doesn't apply to the root: it's never listed, and its return
        // is the one cue to rescan everything.
        if self.unwatched.len() >= MAX_SKIPPED_DIRECTORIES && !self.is_root(path) {
            self.note_truncated();
            return;
        }
        self.changes.unwatched.insert(path.clone());
        self.unwatched.insert(path.clone());
    }

    /// Keeps the ancestors blocking reported skips, whose permission change is
    /// what recovers them, without reporting the ancestors themselves. A skip
    /// whose recovery can't be kept stays listed, accurately: nothing walks it
    /// again, so it stays unwatched.
    pub(super) fn remember_for_recovery(&mut self, paths: impl IntoIterator<Item = PathBuf>) {
        for path in paths {
            self.keep_for_recovery(path);
        }
    }

    fn keep_for_recovery(&mut self, path: PathBuf) {
        if self.has_room_for(&path) {
            self.paths.insert(path);
        } else {
            self.note_overflow();
        }
    }

    /// Keeps rechecks that did not fit the recheck queue, under the same bound.
    pub(super) fn defer(&mut self, paths: Vec<PathBuf>) {
        for path in paths {
            if self.has_room_for(&path) {
                self.due.insert(path);
            } else {
                self.note_overflow();
            }
        }
    }

    /// Moves due rechecks into `pending` as far as it has room.
    pub(super) fn queue_due(&mut self, pending: &mut PendingDirectoryDiscovery) {
        if !self.due.is_empty() {
            let due = mem::take(&mut self.due).into_iter().collect();
            self.due = pending.extend_best_effort(due).into_iter().collect();
        }
    }

    /// The root always has room: its return is the one cue to rescan the
    /// whole watch.
    pub(super) fn has_room_for(&self, path: &Path) -> bool {
        self.paths.len() + self.due.len() < MAX_SKIPPED_DIRECTORIES
            || self.paths.contains(path)
            || self.due.contains(path)
            || self.is_root(path)
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

    /// Records a clean walk of the root that kept every skip it met: the
    /// skips it found are all there are, so the list is complete again.
    pub(super) fn note_complete(&mut self) {
        if self.incomplete {
            self.incomplete = false;
            self.changes.truncated = false;
            self.changes.complete = true;
        }
    }

    /// Records skips that were found but can't be reported, here or by the
    /// walk that found them.
    pub(super) fn note_truncated(&mut self) {
        if !self.incomplete {
            self.incomplete = true;
            self.changes.truncated = true;
            tracing::warn!(
                capacity = MAX_SKIPPED_DIRECTORIES,
                "file watcher stopped reporting unreadable directories"
            );
        }
    }

    /// Forgets skipped directories that are now watched.
    pub(super) fn forget(&mut self, paths: &[PathBuf]) {
        for path in paths {
            self.paths.remove(path);
            self.due.remove(path);
            if self.unwatched.remove(path) && !self.changes.unwatched.remove(path) {
                self.changes.rewatched.insert(path.clone());
            }
        }
    }

    /// Runs for every raw event, so it costs nothing without a removal.
    pub(super) fn forget_descendants(&mut self, removed_roots: &[PathBuf]) {
        if removed_roots.is_empty() {
            return;
        }
        for removed_root in removed_roots {
            take_at_or_below(&mut self.paths, removed_root);
        }
        if !self.due.is_empty() {
            self.due.retain(|path| {
                !removed_roots
                    .iter()
                    .any(|removed_root| path.starts_with(removed_root))
            });
        }
        for removed_root in removed_roots {
            for path in take_at_or_below(&mut self.unwatched, removed_root) {
                if !self.changes.unwatched.remove(&path) {
                    self.changes.rewatched.insert(path);
                }
            }
        }
    }

    /// Takes the remembered skips a metadata change names or lies above, to
    /// walk again; a walk that is still refused remembers them again. They
    /// stay reported as unwatched until a walk registers them.
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

    /// The net coverage change since the last call, if any.
    pub(super) fn take_changes(&mut self) -> Option<CoverageChange> {
        (!self.changes.is_empty()).then(|| std::mem::take(&mut self.changes))
    }
}

/// Reports the net coverage change, if any; false once nothing consumes it.
pub(super) async fn report_coverage<C, CFut>(
    root: &Path,
    skipped: &mut SkippedDirectories,
    on_coverage: &mut C,
) -> bool
where
    C: FnMut(CoverageChange) -> CFut,
    CFut: std::future::Future<Output = bool>,
{
    // A root still skipped once the step is done hides what's under it,
    // whatever this step's walks found.
    if skipped.unwatched.contains(root) && !skipped.incomplete {
        skipped.incomplete = true;
        skipped.changes.truncated = true;
        skipped.changes.complete = false;
    }
    let Some(mut change) = skipped.take_changes() else {
        return true;
    };
    // A root that turned unreadable hides every skip under it, which is what
    // an incomplete list reports, until a clean, complete walk of the root
    // lists every skip again. Its return is reported as itself: REWATCHED for
    // "." asks for a rescan of the whole watch.
    if change.unwatched.remove(root) {
        change.truncated = true;
        change.complete = false;
        skipped.incomplete = true;
    }
    // Walks record skips under the root; one outside it would name a path the
    // consumer can't resolve.
    for paths in [&mut change.unwatched, &mut change.rewatched] {
        paths.retain(|path| path.starts_with(root));
    }
    change.is_empty() || on_coverage(change).await
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
