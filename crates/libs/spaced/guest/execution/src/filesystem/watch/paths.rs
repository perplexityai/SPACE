use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

use crate::filesystem::map_io_error;
use pplx_space_common_core::types::GuestPath;

use super::filter::WatchFilter;
use super::{MAX_LEDGER_ROOTS, MAX_WATCHED_DIRECTORIES};

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
}

impl WatchRegistrationPlan {
    pub(super) fn len(&self) -> usize {
        1 + self.descendants.len()
    }

    pub(super) fn into_directories(self) -> Vec<PathBuf> {
        std::iter::once(self.root).chain(self.descendants).collect()
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

fn watch_root_io_error(root: &Path, error: std::io::Error) -> Status {
    let status = map_io_error(error);
    Status::new(
        status.code(),
        format!("watch root {}: {}", root.display(), status.message()),
    )
}

pub(super) async fn plan_watch_registration(
    root: &Path,
    recursive: bool,
    filter: &WatchFilter,
    scan_slots: &Arc<Semaphore>,
) -> Result<WatchRegistrationPlan, Status> {
    if !recursive {
        return Ok(WatchRegistrationPlan {
            root: root.to_path_buf(),
            descendants: Vec::new(),
        });
    }
    let scan_slot = try_acquire_watch_directory_scan_slot(scan_slots)?;
    let root = root.to_path_buf();
    let filter = filter.clone();
    tokio::task::spawn_blocking(move || {
        let _scan_slot = scan_slot;
        let mut directories = collect_watch_directories(&root, &root, &filter)?;
        // Required-root traversal always puts the root first.
        debug_assert_eq!(directories.first(), Some(&root));
        directories.remove(0);
        Ok(WatchRegistrationPlan {
            root,
            descendants: directories,
        })
    })
    .await
    .map_err(|error| {
        Status::internal(format!(
            "file watcher registration planning panicked: {error}"
        ))
    })?
}

pub(super) fn collect_watch_directories(
    root: &Path,
    start: &Path,
    filter: &WatchFilter,
) -> Result<Vec<PathBuf>, Status> {
    collect_watch_directories_with_limit(root, start, filter, MAX_WATCHED_DIRECTORIES)
}

fn collect_watch_directories_with_limit(
    root: &Path,
    start: &Path,
    filter: &WatchFilter,
    limit: usize,
) -> Result<Vec<PathBuf>, Status> {
    let mut directories = Vec::new();
    let mut stack = vec![start.to_path_buf()];
    while let Some(path) = stack.pop() {
        let required_root = path == root;
        if !required_root && !filter.matches_path(root, &path) {
            continue;
        }
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if !required_root && error.kind() == std::io::ErrorKind::NotFound => {
                continue;
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
        directories.push(path.clone());
        if directories.len() > limit {
            return Err(Status::resource_exhausted(
                "file watcher directory budget exceeded",
            ));
        }
        // A filter-excluded required root is still traversed: the filter
        // selects entries, and skipping the walk would leave existing
        // subdirectories unwatched while later-created ones are discovered.
        let entries = match std::fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(error) if !required_root && error.kind() == std::io::ErrorKind::NotFound => {
                // This path was appended immediately above, but disappeared
                // before traversal. Do not retain a stale budget entry; keep
                // the live directories collected from other branches.
                directories.pop();
                continue;
            }
            Err(error) if required_root => return Err(watch_root_io_error(root, error)),
            Err(error) => return Err(map_io_error(error)),
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(map_io_error(error)),
            };
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(map_io_error(error)),
            };
            if file_type.is_dir() && !file_type.is_symlink() {
                let path = entry.path();
                if filter.matches_path(root, &path) {
                    // A wide directory must not accumulate an unbounded
                    // frontier before the next visited-directory check.
                    if directories.len() + stack.len() >= limit {
                        return Err(Status::resource_exhausted(
                            "file watcher directory budget exceeded",
                        ));
                    }
                    stack.push(path);
                }
            }
        }
    }
    Ok(directories)
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
