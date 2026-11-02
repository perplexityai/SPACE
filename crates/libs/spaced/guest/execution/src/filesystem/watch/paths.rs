use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

use crate::filesystem::map_io_error;
use pplx_space_common_core::types::GuestPath;

use super::filter::WatchFilter;
use super::{MAX_LEDGER_ROOTS, MAX_SKIPPED_DIRECTORIES, MAX_WATCHED_DIRECTORIES};

const MAX_CONCURRENT_WATCH_DIRECTORY_SCANS: usize = 4;

/// The directory-scan budget of one watch manager. Each filesystem service
/// process (the direct service, or one identity worker) owns a single
/// manager, so this bounds that process's scans. Owning it per manager rather
/// than per process keeps independent managers, such as concurrent tests,
/// from exhausting each other's budget.
pub(super) fn new_watch_directory_scan_slots() -> Arc<Semaphore> {
    Arc::new(Semaphore::new(MAX_CONCURRENT_WATCH_DIRECTORY_SCANS))
}

fn try_acquire_watch_directory_scan_slot(
    scan_slots: &Arc<Semaphore>,
) -> Result<OwnedSemaphorePermit, Status> {
    Arc::clone(scan_slots).try_acquire_owned().map_err(|_| {
        crate::refused(Status::resource_exhausted(
            "too many concurrent file watcher directory scans; retry with backoff",
        ))
    })
}

pub(super) async fn normalize_watch_root(path: &GuestPath) -> Result<PathBuf, Status> {
    let root = tokio::fs::canonicalize(path.expose())
        .await
        .map_err(|error| watch_root_io_error(Path::new(path.expose()), error))?;
    let metadata = tokio::fs::symlink_metadata(&root)
        .await
        .map_err(|error| watch_root_io_error(&root, error))?;
    if !metadata.is_dir() {
        return Err(Status::invalid_argument(format!(
            "watch root {} must be a directory",
            root.display()
        )));
    }
    Ok(root)
}

pub(super) async fn default_ledger_roots() -> Result<Vec<PathBuf>, Status> {
    default_ledger_roots_for("/home/user", "/tmp").await
}

async fn default_ledger_roots_for(workdir: &str, temp_root: &str) -> Result<Vec<PathBuf>, Status> {
    let roots = [workdir, temp_root]
        .into_iter()
        .map(|path| {
            GuestPath::parse(path).map_err(|error| {
                Status::failed_precondition(format!("default watch root {path:?}: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    normalize_ledger_roots(roots).await
}

pub(super) async fn normalize_ledger_roots(roots: Vec<GuestPath>) -> Result<Vec<PathBuf>, Status> {
    if roots.len() > MAX_LEDGER_ROOTS {
        return Err(Status::resource_exhausted("too many file change roots"));
    }
    let mut normalized = Vec::with_capacity(roots.len());
    for root in roots {
        normalized.push(normalize_watch_root(&root).await?);
    }
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
}

// The mandatory root is separate from optional descendants so registration
// cannot publish a backend with no physical lifecycle watch.
#[derive(Debug)]
pub(super) struct WatchRegistrationPlan {
    pub(super) root: PathBuf,
    pub(super) descendants: Vec<PathBuf>,
    /// Where the walk gave up for lack of permission; see [`WatchDirectoryWalk`].
    pub(super) skipped: Vec<PathBuf>,
}

impl WatchRegistrationPlan {
    pub(super) fn len(&self) -> usize {
        1 + self.descendants.len()
    }
}

pub(super) fn validate_watch_root(root: &Path) -> Result<(), Status> {
    let metadata =
        std::fs::symlink_metadata(root).map_err(|error| watch_root_io_error(root, error))?;
    if !metadata.is_dir() {
        return Err(Status::invalid_argument(format!(
            "watch root {} must be a directory",
            root.display()
        )));
    }
    Ok(())
}

pub(super) fn watch_root_io_error(root: &Path, error: std::io::Error) -> Status {
    watch_io_error("watch root", root, error)
}

/// Maps `error` as the filesystem API does, naming the path it concerns.
pub(super) fn watch_io_error(prefix: &str, path: &Path, error: std::io::Error) -> Status {
    let status = map_io_error(error);
    Status::new(
        status.code(),
        format!("{prefix} {}: {}", path.display(), status.message()),
    )
}

pub(super) async fn plan_watch_registration(
    root: &Path,
    recursive: bool,
    filter: &WatchFilter,
    scan_slots: &Arc<Semaphore>,
    tasks: &tokio_util::task::TaskTracker,
) -> Result<WatchRegistrationPlan, Status> {
    if !recursive {
        return Ok(WatchRegistrationPlan {
            root: root.to_path_buf(),
            descendants: Vec::new(),
            skipped: Vec::new(),
        });
    }
    let scan_slot = try_acquire_watch_directory_scan_slot(scan_slots)?;
    let root = root.to_path_buf();
    let filter = filter.clone();
    tasks
        .spawn_blocking(move || {
            let _scan_slot = scan_slot;
            let WatchDirectoryWalk {
                mut directories,
                skipped,
            } = collect_watch_directories(&root, &root, &filter)?;
            // Required-root traversal always puts the root first.
            debug_assert_eq!(directories.first(), Some(&root));
            directories.remove(0);
            Ok(WatchRegistrationPlan {
                root,
                descendants: directories,
                skipped,
            })
        })
        .await
        .map_err(|error| {
            Status::internal(format!(
                "file watcher registration planning panicked: {error}"
            ))
        })?
}

#[derive(Debug)]
pub(super) struct WatchDirectoryWalk {
    pub(super) directories: Vec<PathBuf>,
    /// Where the walk gave up for lack of permission: a directory it could not
    /// list, a listed directory whose entries it could not inspect, or the
    /// ancestor whose missing search permission refused a lookup. A
    /// permission change on one of these is what brings the hidden part of
    /// the tree back. At most [`MAX_SKIPPED_DIRECTORIES`] per walk.
    pub(super) skipped: Vec<PathBuf>,
}

pub(super) fn collect_watch_directories(
    root: &Path,
    start: &Path,
    filter: &WatchFilter,
) -> Result<WatchDirectoryWalk, Status> {
    collect_watch_directories_with_limit(root, start, filter, MAX_WATCHED_DIRECTORIES)
}

/// Walks the tree like [`collect_watch_directories`], except that a root this
/// identity can't look up or list is reported as `None` rather than failing:
/// a watch whose root turned unreadable keeps its root watch, and the root's
/// next permission change walks it again.
pub(super) fn collect_watch_directories_or_root_refusal(
    root: &Path,
    start: &Path,
    filter: &WatchFilter,
) -> Result<Option<WatchDirectoryWalk>, Status> {
    walk_watch_directories(root, start, filter, MAX_WATCHED_DIRECTORIES, true)
}

fn collect_watch_directories_with_limit(
    root: &Path,
    start: &Path,
    filter: &WatchFilter,
    limit: usize,
) -> Result<WatchDirectoryWalk, Status> {
    walk_watch_directories(root, start, filter, limit, false)?
        .ok_or_else(|| Status::internal("file watcher root refusal was not reported"))
}

fn walk_watch_directories(
    root: &Path,
    start: &Path,
    filter: &WatchFilter,
    limit: usize,
    report_root_refusal: bool,
) -> Result<Option<WatchDirectoryWalk>, Status> {
    let mut directories = Vec::new();
    let mut skipped = Vec::new();
    let mut vanished = 0_usize;
    let mut stack = vec![start.to_path_buf()];
    while let Some(path) = stack.pop() {
        let required_root = path == root;
        if !required_root && !filter.matches_path(root, &path) {
            continue;
        }
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if !required_root && skippable_descendant_error(&error) => {
                // A refused lookup means a directory above this one is not
                // searchable: usually the parent this walk listed, but an
                // ancestor can lose search permission after the listing.
                if is_permission_denied(&error) {
                    record_skip(&mut skipped, &blocking_ancestor(root, &path));
                } else {
                    vanished = vanished.saturating_add(1);
                }
                continue;
            }
            Err(error) if required_root && report_root_refusal && is_permission_denied(&error) => {
                return Ok(None);
            }
            Err(error) if required_root => return Err(watch_root_io_error(root, error)),
            Err(error) => return Err(map_io_error(error)),
        };
        if !metadata.is_dir() {
            if required_root {
                return Err(Status::invalid_argument(format!(
                    "watch root {} must be a directory",
                    root.display()
                )));
            }
            continue;
        }
        // A filter-excluded required root is still traversed: the filter
        // selects entries, and skipping the walk would leave existing
        // subdirectories unwatched while later-created ones are discovered.
        let entries = match std::fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(error) if !required_root && skippable_descendant_error(&error) => {
                // A directory that disappeared or cannot be listed is not
                // watched, so it takes no budget; keep the live directories
                // collected from other branches.
                if is_permission_denied(&error) {
                    record_skip(&mut skipped, &path);
                } else {
                    vanished = vanished.saturating_add(1);
                }
                continue;
            }
            Err(error) if required_root && report_root_refusal && is_permission_denied(&error) => {
                return Ok(None);
            }
            Err(error) if required_root => return Err(watch_root_io_error(root, error)),
            Err(error) => return Err(map_io_error(error)),
        };
        directories.push(path.clone());
        if directories.len() > limit {
            return Err(Status::resource_exhausted(
                "file watcher directory budget exceeded",
            ));
        }
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(map_io_error(error)),
            };
            // Without d_type this stats the entry, which a listable but
            // unsearchable parent refuses; such an entry cannot be watched.
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(error) if skippable_descendant_error(&error) => {
                    // The listed directory is the one a permission change
                    // must reach, not each entry it hides.
                    if is_permission_denied(&error) {
                        record_skip(&mut skipped, &path);
                    } else {
                        vanished = vanished.saturating_add(1);
                    }
                    continue;
                }
                Err(error) => return Err(map_io_error(error)),
            };
            if file_type.is_dir() && !file_type.is_symlink() {
                let path = entry.path();
                if filter.matches_path(root, &path) {
                    // A wide directory must not accumulate an unbounded
                    // frontier before the next visited-directory check. The
                    // frontier bounds memory, with headroom for entries that
                    // turn out unwatchable; the directory budget itself is
                    // the check on visited directories above.
                    if directories.len() + stack.len() >= limit.saturating_mul(2) {
                        return Err(Status::resource_exhausted(
                            "file watcher directory budget exceeded",
                        ));
                    }
                    stack.push(path);
                }
            }
        }
    }
    if vanished > 0 || !skipped.is_empty() {
        tracing::debug!(
            vanished,
            unreadable_directories = skipped.len(),
            "file watcher skipped descendants that vanished or cannot be read"
        );
    }
    Ok(Some(WatchDirectoryWalk {
        directories,
        skipped,
    }))
}

pub(super) fn is_permission_denied(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::PermissionDenied
}

/// The directory whose missing search permission refused the lookup of `path`.
/// Looking a path up needs search permission on every directory above it, so
/// the blocking one is the nearest ancestor whose own lookup succeeds.
pub(super) fn blocking_ancestor(root: &Path, path: &Path) -> PathBuf {
    path.ancestors()
        .skip(1)
        .take_while(|ancestor| ancestor.starts_with(root))
        .find(|ancestor| std::fs::symlink_metadata(ancestor).is_ok())
        .unwrap_or(root)
        .to_path_buf()
}

// Refusals usually come in runs from one directory, so comparing with the last
// entry keeps one entry per directory without a set.
fn record_skip(skipped: &mut Vec<PathBuf>, path: &Path) {
    if skipped.len() < MAX_SKIPPED_DIRECTORIES && skipped.last().map(PathBuf::as_path) != Some(path)
    {
        skipped.push(path.to_path_buf());
    }
}

/// A descendant that vanished, or that this identity cannot read, is left out
/// of the watch with its subtree: inotify needs read access to watch a
/// directory, and one unreadable cache directory must not cost the caller the
/// rest of the tree. The watch remembers where it gave up, and a permission
/// change there (an IN_ATTRIB event) walks that part of the tree again. The
/// required root has no such fallback.
pub(super) fn skippable_descendant_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
    )
}

pub(super) fn consumer_accepts_event(
    path: impl AsRef<Path>,
    root: &Path,
    recursive: bool,
    filter: &WatchFilter,
    backend_filter_applies: bool,
) -> bool {
    let path = path.as_ref();
    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    if !recursive {
        let mut components = relative.components();
        if components.next().is_some() && components.next().is_some() {
            return false;
        }
    }
    if backend_filter_applies && !filter.has_include_globs() {
        return true;
    }
    filter.matches_relative(path, relative)
}

#[cfg(test)]
mod tests;
