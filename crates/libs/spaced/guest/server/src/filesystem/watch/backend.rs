use std::collections::{BTreeSet, HashSet};
use std::io::ErrorKind;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use notify::event::{CreateKind, ModifyKind, RenameMode};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use pplx_space_spaced_common::proto::bridge_guest::FileEventKind;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tonic::Status;

use crate::filesystem::map_io_error;

use super::MAX_WATCHED_DIRECTORIES;
use super::event::WatchEvent;
use super::filter::WatchFilter;
use super::now_unix_ms;
use super::paths::{WatchRegistrationPlan, collect_watch_directories, validate_watch_root};
use super::state::DirectoryAdjustment;

// Losing a raw event makes the watch incomplete, so overflow terminates the
// shared root. Directory discovery runs independently and never blocks this
// queue from being drained.
const RAW_EVENT_BUFFER_CAPACITY: usize = 4_096;
// Discovery candidates are coalesced while the single per-root scan is in
// flight. Keeping a separate bound prevents rename-heavy workloads from
// moving unbounded path storage behind an otherwise bounded raw-event queue.
const DIRECTORY_DISCOVERY_CANDIDATE_CAPACITY: usize = 4_096;

type DirectoryScanSpawner = Arc<
    dyn Fn(
            PathBuf,
            WatchFilter,
            Arc<BTreeSet<PathBuf>>,
        ) -> tokio::task::JoinHandle<Result<DirectoryScanResult, Status>>
        + Send
        + Sync,
>;

struct DirectoryScanResult {
    directories: Vec<PathBuf>,
    incomplete: bool,
}

#[cfg(test)]
mod tests;

// Physical watcher admission includes registration and retiring backends whose
// blocking shutdown has not returned yet, not just published consumers.
static WATCH_BACKEND_SLOTS: std::sync::LazyLock<Arc<tokio::sync::Semaphore>> =
    std::sync::LazyLock::new(|| {
        Arc::new(tokio::sync::Semaphore::new(
            super::MAX_ACTIVE_WATCHER_CONSUMERS,
        ))
    });

static WATCH_DIRECTORY_SLOTS: std::sync::LazyLock<Arc<tokio::sync::Semaphore>> =
    std::sync::LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(MAX_WATCHED_DIRECTORIES)));

fn directory_admission(count: usize) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
    WATCH_DIRECTORY_SLOTS
        .clone()
        .try_acquire_many_owned(count as u32)
        .map_err(|_| {
            crate::refused(Status::resource_exhausted(
                "watch directory registration capacity exhausted",
            ))
        })
}

struct BlockingWatcher {
    inner: Option<(
        notify::RecommendedWatcher,
        tokio::sync::OwnedSemaphorePermit,
        tokio::sync::OwnedSemaphorePermit,
    )>,
    runtime: tokio::runtime::Handle,
}

impl BlockingWatcher {
    fn reserve_directories(&mut self, count: usize) -> Result<(), Status> {
        let permit = directory_admission(count)?;
        self.inner
            .as_mut()
            .expect("watcher owns directory admission")
            .2
            .merge(permit);
        Ok(())
    }

    fn release_directories(&mut self, count: usize) {
        drop(
            self.inner
                .as_mut()
                .expect("watcher owns directory admission")
                .2
                .split(count)
                .expect("released directories were admitted"),
        );
    }

    async fn update<T: Send + 'static>(
        &mut self,
        update: impl FnOnce(&mut notify::RecommendedWatcher) -> notify::Result<T> + Send + 'static,
    ) -> notify::Result<T> {
        let mut owned = Self {
            inner: self.inner.take(),
            runtime: self.runtime.clone(),
        };
        let (owned, result) = self
            .runtime
            .spawn_blocking(move || {
                let result = update(
                    &mut owned
                        .inner
                        .as_mut()
                        .expect("watcher operation owns backend")
                        .0,
                );
                (owned, result)
            })
            .await
            .map_err(|error| {
                notify::Error::generic(&format!("watcher operation failed: {error}"))
            })?;
        *self = owned;
        result
    }
}

impl Drop for BlockingWatcher {
    fn drop(&mut self) {
        if let Some(owned) = self.inner.take() {
            self.runtime.spawn_blocking(move || drop(owned));
        }
    }
}

pub(super) struct RegisteredWatch {
    watcher: BlockingWatcher,
    raw_rx: mpsc::Receiver<notify::Result<Event>>,
    raw_overflowed: Arc<AtomicBool>,
    raw_overflow_notify: Arc<Notify>,
    directories: Vec<PathBuf>,
}

impl RegisteredWatch {
    pub(super) fn directory_count(&self) -> usize {
        self.directories.len()
    }

    pub(super) async fn prepare(plan: WatchRegistrationPlan) -> Result<Self, Status> {
        let root = plan.root.clone();
        let directories = plan.into_directories();
        let slot = WATCH_BACKEND_SLOTS
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                crate::refused(Status::resource_exhausted(
                    "too many registering or retiring watch backends",
                ))
            })?;
        let directory_slot = directory_admission(directories.len())?;
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            validate_watch_root(&root)?;
            let (raw_tx, raw_rx) = mpsc::channel(RAW_EVENT_BUFFER_CAPACITY);
            let raw_overflowed = Arc::new(AtomicBool::new(false));
            let raw_overflow_notify = Arc::new(Notify::new());
            let callback_overflowed = raw_overflowed.clone();
            let callback_notify = raw_overflow_notify.clone();
            let mut watcher = notify::RecommendedWatcher::new(
                move |result| {
                    if matches!(
                        raw_tx.try_send(result),
                        Err(mpsc::error::TrySendError::Full(_))
                    ) {
                        callback_overflowed.store(true, Ordering::Release);
                        callback_notify.notify_one();
                    }
                },
                notify::Config::default().with_follow_symlinks(false),
            )
            .map_err(|error| Status::internal(format!("create file watcher: {error}")))?;
            for path in &directories {
                watcher
                    .watch(path, RecursiveMode::NonRecursive)
                    .map_err(|error| {
                        Status::internal(format!("watch {}: {error}", path.display()))
                    })?;
            }
            validate_watch_root(&root)?;
            Ok(Self {
                watcher: BlockingWatcher {
                    inner: Some((watcher, slot, directory_slot)),
                    runtime,
                },
                raw_rx,
                raw_overflowed,
                raw_overflow_notify,
                directories,
            })
        })
        .await
        .map_err(|error| Status::internal(format!("watch registration failed: {error}")))?
    }
}

pub(super) struct NotifyTaskConfig {
    pub(super) tasks: tokio_util::task::TaskTracker,
    pub(super) root: PathBuf,
    pub(super) recursive: bool,
    pub(super) include_metadata_events: bool,
    pub(super) filter: WatchFilter,
    pub(super) registration: RegisteredWatch,
    pub(super) shutdown: CancellationToken,
}

/// `scan_slots` is the owning manager's directory-scan budget.
pub(super) fn spawn_notify_task<F, Fut, D, DFut>(
    config: NotifyTaskConfig,
    scan_slots: Arc<tokio::sync::Semaphore>,
    on_event: F,
    adjust_directories: D,
) where
    F: FnMut(Result<WatchEvent, Status>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = bool> + Send + 'static,
    D: FnMut(usize, usize, Option<bool>) -> DFut + Send + 'static,
    DFut: std::future::Future<Output = DirectoryAdjustment> + Send + 'static,
{
    spawn_notify_task_with_scanner(
        config,
        on_event,
        adjust_directories,
        Arc::new(move |root, filter, candidates| {
            let scan_slots = Arc::clone(&scan_slots);
            tokio::spawn(async move {
                let scan_slot = scan_slots
                    .acquire_owned()
                    .await
                    .map_err(|_| Status::unavailable("file watcher directory scanner closed"))?;
                tokio::task::spawn_blocking(move || {
                    let _scan_slot = scan_slot;
                    scan_directory_candidates(&root, &filter, &candidates)
                })
                .await
                .map_err(|error| {
                    Status::internal(format!("file watcher directory scan failed: {error}"))
                })?
            })
        }),
    )
}

fn spawn_notify_task_with_scanner<F, Fut, D, DFut>(
    config: NotifyTaskConfig,
    mut on_event: F,
    mut adjust_directories: D,
    scan_spawner: DirectoryScanSpawner,
) where
    F: FnMut(Result<WatchEvent, Status>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = bool> + Send + 'static,
    D: FnMut(usize, usize, Option<bool>) -> DFut + Send + 'static,
    DFut: std::future::Future<Output = DirectoryAdjustment> + Send + 'static,
{
    let NotifyTaskConfig {
        tasks,
        root,
        recursive,
        include_metadata_events,
        filter,
        registration,
        shutdown,
    } = config;
    let RegisteredWatch {
        watcher,
        mut raw_rx,
        raw_overflowed,
        raw_overflow_notify,
        directories: initial_directories,
    } = registration;

    tasks.spawn(async move {
        let mut watcher = watcher;
        let mut registered_directories: HashSet<PathBuf> = initial_directories.into_iter().collect();
        let mut pending_directory_discovery = PendingDirectoryDiscovery::default();
        let mut directory_scan: Option<DirectoryScan> = None;
        if recursive {
            // Verify the plan after registration: children created between the
            // initial walk and installing their parent watch must be discovered.
            pending_directory_discovery.queue(&root, [root.clone()]);
            start_directory_scan(&mut directory_scan, &mut pending_directory_discovery, &root, &filter, &scan_spawner);
        }
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = raw_overflow_notify.notified() => {
                    if !raw_overflowed.swap(false, Ordering::AcqRel) {
                        continue;
                    }
                    let status = Status::resource_exhausted(
                        "file watcher backend event buffer overflowed",
                    );
                    let _ = on_event(Err(status)).await;
                    shutdown.cancel();
                    break;
                }
                scan_result = async {
                    let scan = directory_scan
                        .as_mut()
                        .expect("directory scan branch requires an active scan");
                    (&mut scan.task).await
                }, if directory_scan.is_some() => {
                    let scan = directory_scan
                        .take()
                        .expect("completed directory scan must be present");
                    let scan_result = match scan_result {
                        Ok(Ok(scan_result)) => scan_result,
                        Ok(Err(status)) => {
                            let _ = on_event(Err(status)).await;
                            shutdown.cancel();
                            break;
                        }
                        Err(error) => {
                            let _ = on_event(Err(Status::internal(format!(
                                "file watcher directory scan failed: {error}"
                            )))).await;
                            shutdown.cancel();
                            break;
                        }
                    };
                    if scan_result.incomplete {
                        let _ = on_event(Err(Status::resource_exhausted(
                            "file watcher directory discovery was incomplete",
                        ))).await;
                        shutdown.cancel();
                        break;
                    }
                    let directories = scan_result
                        .directories
                        .into_iter()
                        .filter(|path| !has_ancestor_in(path, &scan.invalidated_roots))
                        .collect::<Vec<_>>();
                    if !pending_directory_discovery.extend_bounded(
                        scan.invalidated_candidate_roots,
                                    ) {
                        let _ = on_event(Err(Status::resource_exhausted(
                            "file watcher directory discovery buffer overflowed",
                        ))).await;
                        shutdown.cancel();
                        break;
                    }
                    let verify = directories.iter().filter(|path| !registered_directories.contains(*path)).cloned().collect::<Vec<_>>();
                    match apply_discovered_directories(
                        &mut watcher,

                        &mut registered_directories,
                        directories,
                        &mut adjust_directories,
                    ).await {
                        Ok(DirectoryAdjustment::Applied) => {}
                        Ok(DirectoryAdjustment::BudgetExceeded) => {
                            let _ = on_event(Err(Status::resource_exhausted(
                                "file watcher directory budget exceeded",
                            ))).await;
                            shutdown.cancel();
                            break;
                        }
                        Ok(DirectoryAdjustment::RootGone) => break,
                        Err(status) => {
                            let _ = on_event(Err(status)).await;
                            shutdown.cancel();
                            break;
                        }
                    }
                    // Revisit newly watched parents. This closes the walk→watch
                    // interval; later child creation is covered by their watch.
                    if !pending_directory_discovery.extend_bounded(verify) {
                        let _ = on_event(Err(Status::resource_exhausted("file watcher directory discovery buffer overflowed"))).await;
                        shutdown.cancel();
                        break;
                    }
                    if pending_directory_discovery.candidates.is_empty()
                        && adjust_directories(0, 0, Some(false)).await == DirectoryAdjustment::RootGone {
                        break;
                    }
                    start_directory_scan(
                        &mut directory_scan,
                        &mut pending_directory_discovery,
                        &root,
                        &filter,
                        &scan_spawner,
                    );
                }
                result = raw_rx.recv() => {
                    let Some(result) = result else { break; };
                    let results = take_raw_event_batch(result, &mut raw_rx);
                    let mut backend_error = None;
                    let mut receiver_open = true;
                    let mut released_directory_count = 0_usize;
                    for result in results {
                        match result {
                            Ok(event) => {
                                // The root has no watched parent that can recover
                                // it after removal or rename-away.
                                if removed_event_paths(&event).contains(&root.as_path()) {
                                    let _ = on_event(Err(Status::not_found("file watcher root removed"))).await;
                                    shutdown.cancel();
                                    receiver_open = false;
                                    break;
                                }
                                if recursive {
                                    let RemovedDirectories {
                                        roots,
                                        registered,
                                    } = take_removed_directories(
                                        &root,
                                        &event,

                                        &mut registered_directories,
                                    );
                                    if let Some(scan) = directory_scan.as_mut() {
                                        scan.invalidate(&roots, registered.iter().cloned());
                                    }
                                    let budgeted_count = registered.len();
                                    // An ambiguous unwatch keeps its charge until backend
                                    // retirement; errors do not prove kernel rollback.
                                    let released = if registered.is_empty() { budgeted_count } else {
                                        match watcher.update(move |watcher| Ok(registered.into_iter().filter(|path| {
                                            watcher.unwatch(path).is_err_and(|error| !matches!(error.kind, notify::ErrorKind::WatchNotFound))
                                        }).count())).await {
                                            Ok(uncertain) => budgeted_count.saturating_sub(uncertain),
                                            Err(error) => { backend_error = Some(error); break; }
                                        }
                                    };
                                    watcher.release_directories(released);
                                    released_directory_count = released_directory_count.saturating_add(released);
                                    if !pending_directory_discovery.queue(
                                        &root,
                                        directory_candidates(&event),
                                                                    ) {
                                        let _ = on_event(Err(Status::resource_exhausted(
                                            "file watcher directory discovery buffer overflowed",
                                        ))).await;
                                        shutdown.cancel();
                                        receiver_open = false;
                                        break;
                                    }
                                }
                                if !pending_directory_discovery.candidates.is_empty()
                                    && adjust_directories(0, 0, Some(true)).await == DirectoryAdjustment::RootGone {
                                    break;
                                }
                                let events = match file_events_from_notify(
                                    &root,
                                    event,
                                    include_metadata_events,
                                    &filter,
                                ) {
                                    Ok(events) => events,
                                    Err(status) => {
                                        on_event(Err(status)).await;
                                        shutdown.cancel();
                                        receiver_open = false;
                                        break;
                                    }
                                };
                                for event in events {
                                    if !on_event(Ok(event)).await {
                                        shutdown.cancel();
                                        receiver_open = false;
                                        break;
                                    }
                                }
                                if !receiver_open {
                                    break;
                                }
                            }
                            Err(error) => {
                                backend_error = Some(error);
                                break;
                            }
                        }
                    }
                    if !receiver_open {
                        break;
                    }
                    if released_directory_count > 0 {
                        match adjust_directories(0, released_directory_count, None).await {
                            DirectoryAdjustment::Applied => {}
                            DirectoryAdjustment::RootGone => break,
                            DirectoryAdjustment::BudgetExceeded => {
                                tracing::warn!(
                                    released_directory_count,
                                    "file watcher directory release was unexpectedly rejected"
                                );
                                break;
                            }
                        }
                    }
                    if let Some(error) = backend_error {
                        tracing::warn!(
                            affected_path_count = error.paths.len(),
                            root_path_bytes = encoded_path_len(&root),
                            "file watcher backend error"
                        );
                        let status = Status::resource_exhausted(format!(
                            "file watcher backend error: {error}"
                        ));
                        if !on_event(Err(status)).await {
                            tracing::debug!("file watcher error receiver closed");
                        }
                        shutdown.cancel();
                        break;
                    }
                    start_directory_scan(
                        &mut directory_scan,
                        &mut pending_directory_discovery,
                        &root,
                        &filter,
                        &scan_spawner,
                    );
                }
            }
        }
        if let Some(scan) = directory_scan {
            scan.task.abort();
        }
    });
}

fn take_raw_event_batch(
    first: notify::Result<Event>,
    receiver: &mut mpsc::Receiver<notify::Result<Event>>,
) -> Vec<notify::Result<Event>> {
    let mut results = vec![first];
    // The callback can replenish a bounded queue while it is drained. Keep
    // the local batch bounded too, and return to overflow/shutdown selection.
    while results.len() < RAW_EVENT_BUFFER_CAPACITY {
        match receiver.try_recv() {
            Ok(result) => results.push(result),
            Err(_) => break,
        }
    }
    results
}

fn file_events_from_notify(
    root: &Path,
    event: Event,
    include_metadata_events: bool,
    filter: &WatchFilter,
) -> Result<Vec<WatchEvent>, Status> {
    let observed_unix_ms = now_unix_ms()?;
    let paths = event.paths;
    match event.kind {
        EventKind::Create(CreateKind::File) => watch_events_from_paths(
            root,
            paths,
            FileEventKind::Created,
            Some(true),
            observed_unix_ms,
            filter,
        ),
        EventKind::Create(CreateKind::Folder) => watch_events_from_paths(
            root,
            paths,
            FileEventKind::Created,
            Some(false),
            observed_unix_ms,
            filter,
        ),
        EventKind::Create(CreateKind::Any | CreateKind::Other) => watch_events_from_paths(
            root,
            paths,
            FileEventKind::Created,
            None,
            observed_unix_ms,
            filter,
        ),
        EventKind::Remove(_) => watch_events_from_paths(
            root,
            paths,
            FileEventKind::Removed,
            None,
            observed_unix_ms,
            filter,
        ),
        EventKind::Modify(ModifyKind::Data(_)) | EventKind::Modify(ModifyKind::Any) => {
            watch_events_from_paths(
                root,
                paths,
                FileEventKind::Modified,
                None,
                observed_unix_ms,
                filter,
            )
        }
        EventKind::Modify(ModifyKind::Metadata(_)) if include_metadata_events => {
            watch_events_from_paths(
                root,
                paths,
                FileEventKind::Chmod,
                None,
                observed_unix_ms,
                filter,
            )
        }
        EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => {
            let mut paths = paths.into_iter();
            let mut events = Vec::new();
            if let Some(old_path) = paths.next()
                && let Some(event) = watch_event_from_path(
                    root,
                    old_path,
                    FileEventKind::Removed,
                    None,
                    observed_unix_ms,
                    filter,
                )?
            {
                events.push(event);
            }
            if let Some(new_path) = paths.next_back()
                && let Some(event) = watch_event_from_path(
                    root,
                    new_path,
                    FileEventKind::Renamed,
                    None,
                    observed_unix_ms,
                    filter,
                )?
            {
                events.push(event);
            }
            Ok(events)
        }
        EventKind::Modify(ModifyKind::Name(RenameMode::To)) => watch_events_from_paths(
            root,
            paths,
            FileEventKind::Created,
            None,
            observed_unix_ms,
            filter,
        ),
        EventKind::Modify(ModifyKind::Name(RenameMode::From)) => watch_events_from_paths(
            root,
            paths,
            FileEventKind::Removed,
            None,
            observed_unix_ms,
            filter,
        ),
        EventKind::Modify(ModifyKind::Name(_)) => watch_events_from_paths(
            root,
            paths,
            FileEventKind::Modified,
            None,
            observed_unix_ms,
            filter,
        ),
        _ => Ok(Vec::new()),
    }
}

fn watch_events_from_paths(
    root: &Path,
    paths: Vec<PathBuf>,
    kind: FileEventKind,
    is_regular_file: Option<bool>,
    observed_unix_ms: u64,
    filter: &WatchFilter,
) -> Result<Vec<WatchEvent>, Status> {
    paths
        .into_iter()
        .filter_map(|path| {
            watch_event_from_path(root, path, kind, is_regular_file, observed_unix_ms, filter)
                .transpose()
        })
        .collect()
}

fn watch_event_from_path(
    root: &Path,
    path: PathBuf,
    kind: FileEventKind,
    is_regular_file: Option<bool>,
    observed_unix_ms: u64,
    filter: &WatchFilter,
) -> Result<Option<WatchEvent>, Status> {
    let path = absolute_event_path(root, &path)?;
    if !filter.matches_path(root, &path) {
        return Ok(None);
    }
    // Path encoding is consumer loss, not backend failure. Keep the raw path
    // so each consumer's filter can decide whether the observation matters.
    let relative_path = relative_event_path(root, &path)?;
    Ok(Some(
        WatchEvent::new(path, relative_path, kind, observed_unix_ms)
            .with_is_regular_file(is_regular_file),
    ))
}

fn encoded_path_len(path: &Path) -> usize {
    path.as_os_str().as_encoded_bytes().len()
}

fn removed_event_paths(event: &Event) -> Vec<&Path> {
    match event.kind {
        EventKind::Remove(_) | EventKind::Modify(ModifyKind::Name(RenameMode::From)) => {
            event.paths.iter().map(PathBuf::as_path).collect()
        }
        EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => event
            .paths
            .first()
            .map(PathBuf::as_path)
            .into_iter()
            .collect(),
        _ => Vec::new(),
    }
}

struct RemovedDirectories {
    roots: Vec<PathBuf>,
    registered: Vec<PathBuf>,
}

fn take_descendants(directories: &mut HashSet<PathBuf>, removed_roots: &[PathBuf]) -> Vec<PathBuf> {
    let removed = directories
        .iter()
        .filter(|path| {
            removed_roots
                .iter()
                .any(|removed_root| path.starts_with(removed_root))
        })
        .cloned()
        .collect::<Vec<_>>();
    for path in &removed {
        directories.remove(path);
    }
    removed
}

fn take_removed_directories(
    root: &Path,
    event: &Event,
    registered_directories: &mut HashSet<PathBuf>,
) -> RemovedDirectories {
    let removed_roots = removed_event_paths(event)
        .into_iter()
        .filter_map(|path| absolute_event_path(root, path).ok())
        .collect::<Vec<_>>();
    let registered = take_descendants(registered_directories, &removed_roots);
    RemovedDirectories {
        roots: removed_roots,
        registered,
    }
}

fn relative_event_path(root: &Path, path: &Path) -> Result<PathBuf, Status> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| Status::internal("file watcher event escaped root"))?;
    let relative = if relative.as_os_str().is_empty() {
        Path::new(".")
    } else {
        relative
    };
    if relative
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(Status::internal(
            "file watcher event contains parent traversal",
        ));
    }
    Ok(relative.to_path_buf())
}

fn absolute_event_path(root: &Path, path: &Path) -> Result<PathBuf, Status> {
    if path.is_absolute() && path.starts_with(root) {
        return Ok(path.to_path_buf());
    }
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return Ok(canonical);
    }
    if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name())
        && let Ok(parent) = std::fs::canonicalize(parent)
    {
        return Ok(parent.join(file_name));
    }
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .map_err(map_io_error)
}

#[derive(Default)]
struct PendingDirectoryDiscovery {
    candidates: BTreeSet<PathBuf>,
}

impl PendingDirectoryDiscovery {
    fn queue(&mut self, root: &Path, candidates: impl IntoIterator<Item = PathBuf>) -> bool {
        self.extend_bounded(
            candidates
                .into_iter()
                .filter_map(|path| normalize_discovery_candidate(root, path)),
        )
    }

    fn extend_bounded(&mut self, candidates: impl IntoIterator<Item = PathBuf>) -> bool {
        for candidate in candidates {
            if self.candidates.contains(&candidate) {
                continue;
            }
            if self.candidates.len() >= DIRECTORY_DISCOVERY_CANDIDATE_CAPACITY {
                return false;
            }
            self.candidates.insert(candidate);
        }
        true
    }
}

struct DirectoryScan {
    task: tokio::task::JoinHandle<Result<DirectoryScanResult, Status>>,
    candidate_roots: Arc<BTreeSet<PathBuf>>,
    invalidated_roots: HashSet<PathBuf>,
    invalidated_candidate_roots: BTreeSet<PathBuf>,
}

impl DirectoryScan {
    fn invalidate(
        &mut self,
        removed_roots: &[PathBuf],
        known_directories: impl IntoIterator<Item = PathBuf>,
    ) {
        self.invalidated_roots.extend(known_directories);
        // A create-scan-remove race is relevant only when the removed path is
        // inside an in-flight candidate. Looking up path ancestors keeps this
        // work bounded by path depth; unrelated file removals retain no state.
        for removed_root in removed_roots {
            let Some(candidate_root) = removed_root
                .ancestors()
                .find(|ancestor| self.candidate_roots.contains(*ancestor))
            else {
                continue;
            };
            let candidate_root = candidate_root.to_path_buf();
            self.invalidated_roots.insert(candidate_root.clone());
            self.invalidated_candidate_roots.insert(candidate_root);
        }
    }
}

fn normalize_discovery_candidate(root: &Path, path: PathBuf) -> Option<PathBuf> {
    let path = if path.is_absolute() {
        path
    } else {
        root.join(path)
    };
    path.starts_with(root).then_some(path)
}

fn has_ancestor_in(path: &Path, roots: &HashSet<PathBuf>) -> bool {
    path.ancestors().any(|ancestor| roots.contains(ancestor))
}

fn directory_candidates(event: &Event) -> Vec<PathBuf> {
    match event.kind {
        EventKind::Create(CreateKind::File) => Vec::new(),
        EventKind::Create(CreateKind::Folder | CreateKind::Any | CreateKind::Other)
        | EventKind::Modify(ModifyKind::Name(RenameMode::To)) => event.paths.clone(),
        EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => {
            event.paths.last().cloned().into_iter().collect()
        }
        _ => Vec::new(),
    }
}

fn start_directory_scan(
    scan: &mut Option<DirectoryScan>,
    pending: &mut PendingDirectoryDiscovery,
    root: &Path,
    filter: &WatchFilter,
    scan_spawner: &DirectoryScanSpawner,
) {
    if scan.is_some() || pending.candidates.is_empty() {
        return;
    }
    let candidates = Arc::new(mem::take(&mut pending.candidates));
    let root = root.to_path_buf();
    let filter = filter.clone();
    *scan = Some(DirectoryScan {
        task: scan_spawner(root, filter, Arc::clone(&candidates)),
        candidate_roots: candidates,
        invalidated_roots: HashSet::new(),
        invalidated_candidate_roots: BTreeSet::new(),
    });
}

fn scan_directory_candidates(
    root: &Path,
    filter: &WatchFilter,
    candidates: &BTreeSet<PathBuf>,
) -> Result<DirectoryScanResult, Status> {
    validate_watch_root(root)?;
    let mut roots = Vec::new();
    let mut disappeared_paths = 0_usize;
    let mut inspection_failures = 0_usize;
    for candidate in candidates {
        let Ok(path) = absolute_event_path(root, candidate) else {
            continue;
        };
        if path == root {
            roots.push(path);
            continue;
        }
        if !filter.matches_path(root, &path) {
            continue;
        }
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_dir() => roots.push(path),
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {
                disappeared_paths = disappeared_paths.saturating_add(1);
            }
            Err(_) => inspection_failures = inspection_failures.saturating_add(1),
        }
    }
    let coalesced = coalesce_directory_roots(roots);

    let mut directories = Vec::new();
    let mut traversal_failures = 0_usize;
    for path in coalesced {
        match collect_watch_directories(root, &path, filter) {
            Ok(mut tree) => directories.append(&mut tree),
            Err(status) if path == root => return Err(status),
            Err(_) => traversal_failures = traversal_failures.saturating_add(1),
        }
        if directories.len() > MAX_WATCHED_DIRECTORIES {
            break;
        }
    }
    if inspection_failures > 0 || traversal_failures > 0 {
        tracing::warn!(
            inspection_failures,
            traversal_failures,
            "file watcher directory discovery skipped paths after filesystem errors"
        );
    }
    if disappeared_paths > 0 {
        tracing::debug!(
            disappeared_paths,
            "created watch paths disappeared before directory discovery"
        );
    }
    directories.sort();
    directories.dedup();
    validate_watch_root(root)?;
    Ok(DirectoryScanResult {
        directories,
        incomplete: inspection_failures > 0 || traversal_failures > 0,
    })
}

fn coalesce_directory_roots(mut roots: Vec<PathBuf>) -> Vec<PathBuf> {
    roots.sort_by(|left, right| {
        left.components()
            .count()
            .cmp(&right.components().count())
            .then_with(|| left.cmp(right))
    });
    let mut coalesced = Vec::new();
    for root in roots {
        if !coalesced
            .iter()
            .any(|parent: &PathBuf| root.starts_with(parent))
        {
            coalesced.push(root);
        }
    }
    coalesced
}

async fn apply_discovered_directories<D, DFut>(
    watcher: &mut BlockingWatcher,
    registered_directories: &mut HashSet<PathBuf>,
    directories: Vec<PathBuf>,
    adjust_directories: &mut D,
) -> Result<DirectoryAdjustment, Status>
where
    D: FnMut(usize, usize, Option<bool>) -> DFut,
    DFut: std::future::Future<Output = DirectoryAdjustment>,
{
    let unregistered = directories
        .into_iter()
        .filter(|path| !registered_directories.contains(path))
        .collect::<Vec<_>>();
    let new_count = unregistered.len();
    if new_count > 0 {
        let adjustment = adjust_directories(new_count, 0, None).await;
        if adjustment != DirectoryAdjustment::Applied {
            return Ok(adjustment);
        }
    }
    watcher.reserve_directories(new_count)?;
    let mut disappeared_directories = 0_usize;
    let mut registration_failures = 0_usize;
    for path in unregistered {
        if let Err(error) = watcher
            .update({
                let path = path.clone();
                move |watcher| watcher.watch(&path, RecursiveMode::NonRecursive)
            })
            .await
        {
            match error.kind {
                notify::ErrorKind::PathNotFound => {
                    disappeared_directories = disappeared_directories.saturating_add(1);
                }
                notify::ErrorKind::Io(ref io_error) if io_error.kind() == ErrorKind::NotFound => {
                    disappeared_directories = disappeared_directories.saturating_add(1);
                }
                _ => registration_failures = registration_failures.saturating_add(1),
            }
        } else {
            registered_directories.insert(path);
        }
    }

    // notify may add the kernel watch before a fallible metadata lookup.
    // Retain failed registrations' charges until this backend is retired.
    if disappeared_directories > 0 {
        tracing::debug!(
            disappeared_directories,
            "created watch directories disappeared before registration"
        );
    }
    if registration_failures > 0 {
        tracing::warn!(
            registration_failures,
            "file watcher failed to register discovered directories"
        );
        return Err(Status::resource_exhausted(
            "file watcher could not register discovered directories",
        ));
    }
    Ok(DirectoryAdjustment::Applied)
}
