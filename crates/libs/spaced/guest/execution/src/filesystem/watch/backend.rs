use std::collections::{BTreeSet, HashSet};
use std::io::ErrorKind;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use pplx_space_spaced_guest_protocol::proto::bridge_guest::FileEventKind;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tonic::Status;

use crate::filesystem::map_io_error;

use super::event::WatchEvent;
use super::filter::WatchFilter;
use super::now_unix_ms;
use super::paths::{
    WatchDirectoryWalk, WatchRegistrationPlan, blocking_ancestor, collect_watch_directories,
    collect_watch_directories_or_root_refusal, is_permission_denied, skippable_descendant_error,
    validate_watch_root, watch_io_error, watch_root_io_error,
};
use super::state::DirectoryAdjustment;
use super::{MAX_SKIPPED_DIRECTORIES, MAX_WATCHED_DIRECTORIES};
use skipped::{
    SkippedDirectories, is_metadata_change, queue_verification, requeue_invalidated,
    walked_rechecks,
};

// Losing a raw event makes the watch incomplete, so overflow terminates the
// shared root. Directory discovery runs independently and never blocks this
// queue from being drained.
const RAW_EVENT_BUFFER_CAPACITY: usize = 4_096;
// Discovery candidates are coalesced while the single per-root scan is in
// flight. Keeping a separate bound prevents rename-heavy workloads from
// moving unbounded path storage behind an otherwise bounded raw-event queue.
const DIRECTORY_DISCOVERY_CANDIDATE_CAPACITY: usize = 4_096;
// Rescans of skipped directories after a permission change are recovery, not
// completeness, so they are bounded apart from discovery candidates. What does
// not fit waits as due, while the skipped-directory bound has room, and is
// queued as scans free room. Verifying what a rescan brought back is
// completeness: it takes one discovery candidate per rescan that found
// something.
const DIRECTORY_RECHECK_CAPACITY: usize = 2_048;

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
    /// Where the scan gave up for lack of permission.
    skipped: Vec<PathBuf>,
    incomplete: bool,
}

mod skipped;
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
    tasks: tokio_util::task::TaskTracker,
    // Ownership survives a blocking task returning an unconsumed result.
    _ownership: tokio_util::task::task_tracker::TaskTrackerToken,
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
            tasks: self.tasks.clone(),
            _ownership: self.tasks.token(),
        };
        let operation = self.tasks.token();
        let (owned, result) = self
            .runtime
            .spawn_blocking(move || {
                let _operation = operation;
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
            let operation = self.tasks.token();
            self.runtime.spawn_blocking(move || {
                let _operation = operation;
                drop(owned);
            });
        }
    }
}

pub(super) struct RegisteredWatch {
    watcher: BlockingWatcher,
    raw_rx: mpsc::Receiver<notify::Result<Event>>,
    raw_overflowed: Arc<AtomicBool>,
    raw_overflow_notify: Arc<Notify>,
    directories: Vec<PathBuf>,
    skipped: Vec<PathBuf>,
}

impl RegisteredWatch {
    /// The directories this registration charged: the plan, less descendants
    /// whose watch was refused for lack of permission. A descendant that
    /// vanished keeps its charge until the backend is retired, as failed
    /// discovered registrations do.
    pub(super) fn directory_count(&self) -> usize {
        self.watcher
            .inner
            .as_ref()
            .expect("watcher owns directory admission")
            .2
            .num_permits()
    }

    pub(super) async fn prepare(
        plan: WatchRegistrationPlan,
        tasks: tokio_util::task::TaskTracker,
    ) -> Result<Self, Status> {
        let planned_directories = plan.len();
        let WatchRegistrationPlan {
            root,
            descendants,
            mut skipped,
        } = plan;
        let slot = WATCH_BACKEND_SLOTS
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                crate::refused(Status::resource_exhausted(
                    "too many registering or retiring watch backends",
                ))
            })?;
        let mut directory_slot = directory_admission(planned_directories)?;
        let runtime = tokio::runtime::Handle::current();
        tasks
            .clone()
            .spawn_blocking(move || {
                validate_watch_root(&root)?;
                let (raw_tx, raw_rx) = mpsc::channel(RAW_EVENT_BUFFER_CAPACITY);
                let raw_overflowed = Arc::new(AtomicBool::new(false));
                let raw_overflow_notify = Arc::new(Notify::new());
                let callback_overflowed = raw_overflowed.clone();
                let callback_notify = raw_overflow_notify.clone();
                let mut watcher = notify::RecommendedWatcher::new(
                    move |result: notify::Result<Event>| {
                        match raw_disposition(&result) {
                            RawDisposition::Drop => return,
                            RawDisposition::Lost => {
                                callback_overflowed.store(true, Ordering::Release);
                                callback_notify.notify_one();
                                return;
                            }
                            RawDisposition::Forward => {}
                        }
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
                watcher
                    .watch(&root, RecursiveMode::NonRecursive)
                    .map_err(|error| match notify_io_error(error) {
                        Ok(io_error) => watch_root_io_error(&root, io_error),
                        Err(status) => status,
                    })?;
                let mut directories = Vec::with_capacity(planned_directories);
                directories.push(root.clone());
                let mut refused = 0_usize;
                for path in descendants {
                    match watcher.watch(&path, RecursiveMode::NonRecursive) {
                        Ok(()) => directories.push(path),
                        // A descendant can vanish or turn unreadable after
                        // planning. The verification scan picks up what was
                        // created meanwhile; an unreadable one is remembered
                        // for its next permission change.
                        Err(error) => match notify_io_error(error) {
                            Ok(io_error) if is_permission_denied(&io_error) => {
                                refused = refused.saturating_add(1);
                                skipped.extend(refusal_blocker(&root, &path));
                                skipped.push(path);
                            }
                            Ok(io_error) if skippable_descendant_error(&io_error) => {}
                            Ok(io_error) => return Err(watch_io_error("watch", &path, io_error)),
                            Err(status) => return Err(status),
                        },
                    }
                }
                release_refused_charge(&mut directory_slot, refused);
                let skipped_directories = planned_directories - directories.len();
                if skipped_directories > 0 {
                    tracing::debug!(
                        skipped_directories,
                        "file watcher skipped descendants that vanished or cannot be read"
                    );
                }
                validate_watch_root(&root)?;
                Ok(Self {
                    watcher: BlockingWatcher {
                        inner: Some((watcher, slot, directory_slot)),
                        runtime,
                        _ownership: tasks.token(),
                        tasks,
                    },
                    raw_rx,
                    raw_overflowed,
                    raw_overflow_notify,
                    directories,
                    skipped,
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
    let scanner_tasks = config.tasks.clone();
    spawn_notify_task_with_scanner(
        config,
        on_event,
        adjust_directories,
        Arc::new(move |root, filter, candidates| {
            let scan_slots = Arc::clone(&scan_slots);
            let tasks = scanner_tasks.clone();
            scanner_tasks.spawn(async move {
                let scan_slot = scan_slots
                    .acquire_owned()
                    .await
                    .map_err(|_| Status::unavailable("file watcher directory scanner closed"))?;
                tasks
                    .spawn_blocking(move || {
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
        skipped: initial_skipped,
    } = registration;

    tasks.spawn(async move {
        let mut watcher = watcher;
        let mut registered_directories: HashSet<PathBuf> = initial_directories.into_iter().collect();
        let mut skipped_directories = SkippedDirectories::default();
        skipped_directories.remember(initial_skipped);
        let mut pending_directory_discovery = PendingDirectoryDiscovery::default();
        let mut directory_scan: Option<DirectoryScan> = None;
        if recursive {
            // Verify the plan after registration: children created between the
            // initial walk and installing their parent watch must be discovered.
            pending_directory_discovery.queue(&root, [root.clone()]);
            start_directory_scan(&mut directory_scan, &mut pending_directory_discovery, &mut skipped_directories, &root, &filter, &scan_spawner);
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
                    let walked_rechecks = walked_rechecks(&scan.rechecks, &scan_result.skipped);
                    // A skip the scan saw may already be stale: a permission
                    // change after its read is the only cue to look again.
                    let (recheck, still_skipped): (Vec<_>, Vec<_>) = scan_result
                        .skipped
                        .into_iter()
                        .filter(|path| !has_ancestor_in(path, &scan.invalidated_roots))
                        .partition(|path| scan.changed_since_scan(path));
                    if !requeue_invalidated(
                        &mut pending_directory_discovery,
                        &mut skipped_directories,
                        scan.invalidated_candidate_roots,
                        &scan.rechecks,
                    ) {
                        let _ = on_event(Err(Status::resource_exhausted(
                            "file watcher directory discovery buffer overflowed",
                        ))).await;
                        shutdown.cancel();
                        break;
                    }
                    let verify = directories.iter().filter(|path| !registered_directories.contains(*path)).cloned().collect::<Vec<_>>();
                    match apply_discovered_directories(
                        &root,
                        &mut watcher,
                        &mut registered_directories,
                        &mut skipped_directories,
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
                    // A directory whose watch was refused is not revisited here:
                    // the walk would find it again. A permission change on it
                    // is what walks it again; see `SkippedDirectories`.
                    let verify = verify
                        .into_iter()
                        .filter(|path| registered_directories.contains(path))
                        .collect::<Vec<_>>();
                    skipped_directories.forget(&verify);
                    if !queue_verification(&mut pending_directory_discovery, verify, &scan.candidate_roots, &walked_rechecks) {
                        let _ = on_event(Err(Status::resource_exhausted("file watcher directory discovery buffer overflowed"))).await;
                        shutdown.cancel();
                        break;
                    }
                    skipped_directories.remember(still_skipped);
                    skipped_directories.defer(pending_directory_discovery.extend_best_effort(recheck));
                    // Discovery is under way while candidates are pending,
                    // including the verification of what a recheck brought
                    // back. A recheck alone, which may find nothing, doesn't
                    // count: its end would report a loss for every touch on a
                    // directory that is still unreadable.
                    if adjust_directories(0, 0, Some(pending_directory_discovery.has_candidates())).await == DirectoryAdjustment::RootGone {
                        break;
                    }
                    start_directory_scan(
                        &mut directory_scan,
                        &mut pending_directory_discovery,
                        &mut skipped_directories,
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
                                    skipped_directories.forget_descendants(&roots);
                                    if let Some(scan) = directory_scan.as_mut() {
                                        scan.invalidate(directory_removals(&event, &roots), registered.iter().cloned());
                                        scan.note_metadata_change(&event);
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
                                    // A metadata change on a remembered skip walks it
                                    // again; one anywhere else costs nothing.
                                    let hits = skipped_directories.take_hits(&event);
                                    skipped_directories.defer(pending_directory_discovery.extend_best_effort(hits));
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
                                if pending_directory_discovery.has_candidates()
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
                        &mut skipped_directories,
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

#[derive(Debug, PartialEq, Eq)]
enum RawDisposition {
    Forward,
    Drop,
    Lost,
}

/// What the notify callback does with an event before the raw channel.
fn raw_disposition(result: &notify::Result<Event>) -> RawDisposition {
    match result {
        // notify reports opens and write-closes of a watched directory's
        // entries, this watch's own walks included, and nothing downstream
        // reports access. Dropping them here keeps a walk of a large
        // directory from filling the channel; the kernel's queue still
        // carries them.
        Ok(event) if matches!(event.kind, EventKind::Access(_)) => RawDisposition::Drop,
        // The kernel's queue overflowed (IN_Q_OVERFLOW) and its events are
        // gone, which leaves the watch incomplete just as this channel
        // overflowing does.
        Ok(event) if event.need_rescan() => RawDisposition::Lost,
        _ => RawDisposition::Forward,
    }
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

/// The removals that can invalidate a scan's results. A scan finds
/// directories, and removing a file can't change which ones exist, so a
/// build deleting files doesn't keep discarding a walk of the tree it's in.
fn directory_removals<'a>(event: &Event, removed: &'a [PathBuf]) -> &'a [PathBuf] {
    if matches!(event.kind, EventKind::Remove(RemoveKind::File)) {
        &[]
    } else {
        removed
    }
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
    // Skipped directories to walk again after a permission change. They are
    // bounded on their own and never take room from `candidates`; the next
    // scan walks both.
    rechecks: BTreeSet<PathBuf>,
}

impl PendingDirectoryDiscovery {
    /// Queues rechecks that fit and returns the rest, whose loss is
    /// recoverable rather than an incomplete watch.
    fn extend_best_effort(&mut self, rechecks: Vec<PathBuf>) -> Vec<PathBuf> {
        rechecks
            .into_iter()
            .filter(|recheck| {
                if self.candidates.contains(recheck) || self.rechecks.contains(recheck) {
                    return false;
                }
                if self.rechecks.len() >= DIRECTORY_RECHECK_CAPACITY {
                    return true;
                }
                self.rechecks.insert(recheck.clone());
                false
            })
            .collect()
    }

    fn is_empty(&self) -> bool {
        self.candidates.is_empty() && self.rechecks.is_empty()
    }

    fn has_candidates(&self) -> bool {
        !self.candidates.is_empty()
    }

    /// Takes everything queued for the next scan, and which of it are rechecks.
    fn take(&mut self) -> (BTreeSet<PathBuf>, BTreeSet<PathBuf>) {
        let mut candidates = mem::take(&mut self.candidates);
        let rechecks = mem::take(&mut self.rechecks);
        candidates.extend(rechecks.iter().cloned());
        (candidates, rechecks)
    }

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
    // Removals below a candidate whose subtree alone was discarded; past the
    // bound, the candidate is walked again instead.
    narrowed_removals: usize,
    // Candidates that are rechecks. One invalidated mid-scan is requeued as a
    // recheck, so rechecks never fill the queue discovery depends on; only the
    // verification of what one brought back takes a discovery candidate.
    rechecks: BTreeSet<PathBuf>,
    // Metadata changes seen while the scan runs, on paths its results can
    // name. A directory it found unreadable may have been made readable after
    // it looked; that event is the only cue to look again.
    metadata_changed: HashSet<PathBuf>,
    metadata_changes_overflowed: bool,
}

impl DirectoryScan {
    fn note_metadata_change(&mut self, event: &Event) {
        if !is_metadata_change(event) {
            return;
        }
        for path in &event.paths {
            // Changes on the scan's own rechecks, or above one, are tracked
            // exactly, even past the bound: there are at most as many as the
            // recheck queue holds times their depth, and losing one would
            // leave a directory made readable mid-scan unwatched.
            if at_or_above_any(&self.rechecks, path) {
                self.metadata_changed.insert(path.clone());
                continue;
            }
            if self.metadata_changes_overflowed {
                continue;
            }
            // A scan names directories inside its candidates, or an ancestor
            // of one that refused its lookup. A path's descendants sort
            // directly after it, so the first candidate past `path` is a
            // descendant if any is.
            let concerns_scan = path
                .ancestors()
                .any(|ancestor| self.candidate_roots.contains(ancestor))
                || self
                    .candidate_roots
                    .range::<Path, _>((
                        std::ops::Bound::Excluded(path.as_path()),
                        std::ops::Bound::Unbounded,
                    ))
                    .next()
                    .is_some_and(|candidate| candidate.starts_with(path));
            if !concerns_scan {
                continue;
            }
            if self.metadata_changed.len() >= DIRECTORY_DISCOVERY_CANDIDATE_CAPACITY {
                // Too many to track: recheck every other skip this scan
                // reports.
                self.metadata_changes_overflowed = true;
                let rechecks = &self.rechecks;
                self.metadata_changed
                    .retain(|path| at_or_above_any(rechecks, path));
                continue;
            }
            self.metadata_changed.insert(path.clone());
        }
    }

    /// Whether `path` may have changed after the scan looked. Past the
    /// tracking bound every skip may have, except one this scan was already
    /// rechecking, whose changes are still tracked exactly: a sustained
    /// storm then rechecks each skip once, not on every scan.
    fn changed_since_scan(&self, path: &Path) -> bool {
        path.ancestors()
            .any(|ancestor| self.metadata_changed.contains(ancestor))
            || (self.metadata_changes_overflowed && !self.rechecks.contains(path))
    }

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
            // Below the candidate, only the removed subtree's results are
            // stale: whatever replaces it arrives as its own create or rename
            // event. Walking the whole candidate again would let churn under
            // a large one, such as a recovered subtree, restart it forever.
            // Past the bound, the candidate is walked again instead.
            if candidate_root != removed_root.as_path()
                && self.narrowed_removals < DIRECTORY_DISCOVERY_CANDIDATE_CAPACITY
            {
                self.narrowed_removals += 1;
                self.invalidated_roots.insert(removed_root.clone());
                continue;
            }
            let candidate_root = candidate_root.to_path_buf();
            self.invalidated_roots.insert(candidate_root.clone());
            self.invalidated_candidate_roots.insert(candidate_root);
        }
    }
}

/// Whether `path` is one of `paths` or lies above one. A path's descendants
/// sort directly after it, so the first entry from `path` on decides.
fn at_or_above_any(paths: &BTreeSet<PathBuf>, path: &Path) -> bool {
    paths
        .range::<Path, _>((std::ops::Bound::Included(path), std::ops::Bound::Unbounded))
        .next()
        .is_some_and(|entry| entry.starts_with(path))
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
    skipped: &mut SkippedDirectories,
    root: &Path,
    filter: &WatchFilter,
    scan_spawner: &DirectoryScanSpawner,
) {
    if scan.is_some() {
        return;
    }
    skipped.queue_due(pending);
    if pending.is_empty() {
        return;
    }
    let (candidates, rechecks) = pending.take();
    let candidates = Arc::new(candidates);
    // Due rechecks take the room this scan just freed, for the next scan.
    skipped.queue_due(pending);
    let root = root.to_path_buf();
    let filter = filter.clone();
    *scan = Some(DirectoryScan {
        task: scan_spawner(root, filter, Arc::clone(&candidates)),
        candidate_roots: candidates,
        invalidated_roots: HashSet::new(),
        invalidated_candidate_roots: BTreeSet::new(),
        narrowed_removals: 0,
        rechecks,
        metadata_changed: HashSet::new(),
        metadata_changes_overflowed: false,
    });
}

fn scan_directory_candidates(
    root: &Path,
    filter: &WatchFilter,
    candidates: &BTreeSet<PathBuf>,
) -> Result<DirectoryScanResult, Status> {
    validate_watch_root(root)?;
    let mut roots = Vec::new();
    let mut skipped = Vec::new();
    let mut skipped_paths = 0_usize;
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
            Err(error) if skippable_descendant_error(&error) => {
                if is_permission_denied(&error) {
                    skipped.push(blocking_ancestor(root, &path));
                }
                skipped_paths = skipped_paths.saturating_add(1);
            }
            Err(_) => inspection_failures = inspection_failures.saturating_add(1),
        }
    }
    // Candidates the root folds in, walked on their own if the root walk is
    // refused: one reachable by lookup is watchable though its parent can't
    // be listed.
    let nested_in_root = roots
        .iter()
        .filter(|path| *path != root)
        .cloned()
        .collect::<Vec<_>>();
    let mut walks = coalesce_directory_roots(roots);

    let mut directories = Vec::new();
    let mut traversal_failures = 0_usize;
    let mut skips_overflowed = false;
    let mut walked_nested = false;
    while let Some(path) = walks.pop() {
        let walk = if path == root {
            collect_watch_directories_or_root_refusal(root, &path, filter)
        } else {
            collect_watch_directories(root, &path, filter).map(Some)
        };
        match walk {
            Ok(Some(WatchDirectoryWalk {
                directories: mut tree,
                skipped: gave_up,
            })) => {
                directories.append(&mut tree);
                // Each walk caps its own skips; this keeps the whole scan
                // within the same bound however many walks it runs.
                let room = MAX_SKIPPED_DIRECTORIES.saturating_sub(skipped.len());
                skips_overflowed |= gave_up.len() > room;
                skipped.extend(gave_up.into_iter().take(room));
            }
            // inotify keeps reporting on a root that turned unreadable, so a
            // walk refused the root itself is a skip, walked again on its
            // next permission change, not the end of the watch. The walk
            // reports that refusal itself; any other failure of the root's
            // walk, such as one reading its listing, still ends the watch.
            Ok(None) => {
                skipped.push(path);
                if !walked_nested {
                    walked_nested = true;
                    walks.extend(coalesce_directory_roots(nested_in_root.clone()));
                }
            }
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
    if skipped_paths > 0 {
        tracing::debug!(
            skipped_paths,
            "created watch paths disappeared or cannot be read before directory discovery"
        );
    }
    directories.sort();
    directories.dedup();
    skipped.sort();
    skipped.dedup();
    if skips_overflowed || skipped.len() > MAX_SKIPPED_DIRECTORIES {
        tracing::warn!(
            capacity = MAX_SKIPPED_DIRECTORIES,
            "file watcher discovery found more unreadable directories than it remembers"
        );
        skipped.truncate(MAX_SKIPPED_DIRECTORIES);
    }
    validate_watch_root(root)?;
    Ok(DirectoryScanResult {
        directories,
        skipped,
        incomplete: inspection_failures > 0 || traversal_failures > 0,
    })
}

/// The ancestor to remember for a refused watch on `path`, if the refusal came
/// from above it. A directory refused for its own mode can still be looked up,
/// and a permission change on the directory itself brings it back.
fn refusal_blocker(root: &Path, path: &Path) -> Option<PathBuf> {
    std::fs::symlink_metadata(path)
        .is_err()
        .then(|| blocking_ancestor(root, path))
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
    root: &Path,
    watcher: &mut BlockingWatcher,
    registered_directories: &mut HashSet<PathBuf>,
    skipped: &mut SkippedDirectories,
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
    let mut skipped_directories = 0_usize;
    let mut refused = Vec::new();
    let mut registration_failures = 0_usize;
    for path in unregistered {
        if let Err(error) = watcher
            .update({
                let path = path.clone();
                move |watcher| watcher.watch(&path, RecursiveMode::NonRecursive)
            })
            .await
        {
            match notify_io_error(error) {
                Ok(io_error) if is_permission_denied(&io_error) => refused.push(path),
                Ok(io_error) if skippable_descendant_error(&io_error) => {
                    skipped_directories = skipped_directories.saturating_add(1);
                }
                _ => registration_failures = registration_failures.saturating_add(1),
            }
        } else {
            registered_directories.insert(path);
        }
    }

    if skipped_directories > 0 {
        tracing::debug!(
            skipped_directories,
            "created watch directories disappeared or cannot be read before registration"
        );
    }
    // A failed registration ends this backend, which releases every charge it
    // holds; the watcher may already be gone, so nothing is released here.
    if registration_failures > 0 {
        tracing::warn!(
            registration_failures,
            "file watcher failed to register discovered directories"
        );
        return Err(Status::resource_exhausted(
            "file watcher could not register discovered directories",
        ));
    }
    // notify may add the kernel watch before a fallible metadata lookup, so a
    // vanished directory keeps its charge until this backend is retired.
    // A refused one is released; see `release_refused_charge`.
    if !refused.is_empty() {
        let count = refused.len();
        skipped.remember(refused_with_blocking_ancestors(root, refused).await);
        watcher.release_directories(count);
        let adjustment = adjust_directories(0, count, None).await;
        if adjustment != DirectoryAdjustment::Applied {
            return Ok(adjustment);
        }
    }
    Ok(DirectoryAdjustment::Applied)
}

/// What to remember for directories whose watch was refused: each directory,
/// and, when looking it up fails too, the ancestor that blocks the lookup. A
/// permission change on that ancestor is then the one that brings it back.
/// The lookups run off the event loop.
async fn refused_with_blocking_ancestors(root: &Path, refused: Vec<PathBuf>) -> Vec<PathBuf> {
    let root = root.to_path_buf();
    let fallback = refused.clone();
    tokio::task::spawn_blocking(move || {
        refused
            .into_iter()
            .flat_map(|path| refusal_blocker(&root, &path).into_iter().chain([path]))
            .collect()
    })
    .await
    .unwrap_or(fallback)
}

/// Releases the charge of `count` directories whose watch was refused with
/// PermissionDenied. Without this, every permission change that leaves a
/// directory unreadable would charge it again until the process-wide budget
/// ran out.
///
/// notify reports PermissionDenied both when adding the kernel watch fails
/// (no watch exists) and when its metadata lookup after a successful add fails
/// (an untracked watch exists), so a rare race can release one directory that
/// still holds a kernel watch. The kernel's own limit still applies: it is
/// reported as `MaxFilesWatch` and becomes ResourceExhausted.
fn release_refused_charge(permit: &mut tokio::sync::OwnedSemaphorePermit, count: usize) {
    if count > 0 {
        drop(
            permit
                .split(count)
                .expect("refused directories were admitted"),
        );
    }
}

/// The I/O error behind a failed watch registration, classified by the
/// filesystem API's mapping; `Err` carries a failure that is not plain I/O.
fn notify_io_error(error: notify::Error) -> Result<std::io::Error, Status> {
    match error.kind {
        notify::ErrorKind::Io(io_error) => Ok(io_error),
        notify::ErrorKind::PathNotFound => Ok(ErrorKind::NotFound.into()),
        notify::ErrorKind::MaxFilesWatch => Err(Status::resource_exhausted(
            "file watcher: inotify watch limit reached",
        )),
        _ => Err(Status::internal(format!("file watcher: {error}"))),
    }
}
