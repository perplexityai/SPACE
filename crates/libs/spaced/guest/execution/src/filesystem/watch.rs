use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime};

use futures_util::Stream;
use pplx_space_common_core::types::{GuestPath, RelativeGlobPattern, WatchRoot as GuestWatchRoot};
use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
    FileEvent, FileEventKind, GetModifiedFilesRequest, GetModifiedFilesResponse,
};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;
use tonic::Status;

use super::metadata::path_to_string;
use backend::{NotifyTaskConfig, RegisteredWatch, spawn_notify_task};
use event::{WatchEvent, WatchPathEncoding};
pub(super) use filter::parse_watch_globs;
use filter::{WatchFilter, WatchFilterConfig};
use ledger::Ledger;
use paths::{
    consumer_accepts_event, default_ledger_roots, normalize_ledger_roots, normalize_watch_root,
    plan_watch_registration,
};
use state::{
    DirectoryAdjustment, LedgerState, WatchConsumer, WatchConsumerRef, WatchManagerState,
    WatchRoot, WatchRootGeneration, WatchRootKey, active_consumer_count, adjust_directories_locked,
    directory_budget_exceeded, next_root_generation_locked, reap_expired_pull_watchers_locked,
    remove_consumer_locked, remove_root_for_backend, root_for_backend_mut,
};

mod backend;
mod event;
mod filter;
mod ledger;
mod paths;
mod state;
#[cfg(test)]
mod tests;

pub(super) type WatchStream =
    std::pin::Pin<Box<dyn Stream<Item = Result<FileEvent, Status>> + Send>>;

const MAX_ACTIVE_WATCHER_CONSUMERS: usize = 64;
const MAX_LEDGER_ROOTS: usize = 8;
const MAX_WATCHED_DIRECTORIES: usize = 16_384;
// Skipped directories are remembered so a permission change on one brings its
// subtree into the watch. Past this bound, further skips are not remembered.
const MAX_SKIPPED_DIRECTORIES: usize = 4_096;
// Keep burst capacity for small events, but cap path payloads independently.
// Overflow is explicit and requires the caller to rescan.
const STREAM_BUFFER_CAPACITY: usize = 1024;
const PULL_WATCHER_BUFFER_CAPACITY: usize = 4096;
const WATCH_CONSUMER_BYTE_CAPACITY: usize = 64 * 1024;
const LEDGER_BYTE_CAPACITY: usize = 1024 * 1024;
const LEDGER_PATH_CAPACITY: usize = 16_384;
const LEDGER_RETENTION: Duration = Duration::from_secs(60 * 60);
const PULL_WATCHER_TTL: Duration = Duration::from_secs(10 * 60);
const PULL_WATCHER_REAPER_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_WINDOW_MS: u64 = 60_000;

pub(super) struct QueuedFileEvent {
    event: FileEvent,
    _bytes: tokio::sync::OwnedSemaphorePermit,
}

pub(super) async fn normalize_watch_root_string(path: &GuestPath) -> Result<String, Status> {
    path_to_string(&normalize_watch_root(path).await?)
}

#[derive(Clone)]
pub(crate) struct WatchManager {
    inner: Arc<Mutex<WatchManagerState>>,
    shutdown: CancellationToken,
    tasks: tokio_util::task::TaskTracker,
    scan_slots: Arc<tokio::sync::Semaphore>,
}

impl WatchManager {
    pub(super) fn new(shutdown: CancellationToken) -> Self {
        Self {
            inner: Arc::default(),
            shutdown: shutdown.child_token(),
            tasks: tokio_util::task::TaskTracker::new(),
            scan_slots: paths::new_watch_directory_scan_slots(),
        }
    }

    pub(super) async fn shutdown(&self) {
        self.shutdown.cancel();
        {
            let mut state = self.inner.lock().await;
            state.roots.clear();
            state.pull_watchers.clear();
            state.ledger = LedgerState::Disabled;
            state.ledger_generation = state.ledger_generation.wrapping_add(1);
        }
        self.tasks.close();
        self.tasks.wait().await;
    }

    pub(super) async fn watch_stream(
        &self,
        path: GuestPath,
        recursive: bool,
        include_metadata_events: bool,
        include_globs: Vec<RelativeGlobPattern>,
        exclude_globs: Vec<RelativeGlobPattern>,
        use_default_excludes: Option<bool>,
    ) -> Result<WatchStream, Status> {
        let root = GuestWatchRoot::new(path);
        let root = normalize_watch_root(root.guest_path()).await?;
        let filter_config = WatchFilterConfig::new(
            include_globs,
            exclude_globs,
            use_default_excludes.unwrap_or(false),
        )?;
        let filter = WatchFilter::compile(filter_config.clone())?;
        let key = WatchRootKey {
            root,
            recursive,
            include_metadata_events,
            filter_config: filter_config.backend_config(),
        };
        let (tx, rx) = mpsc::channel(STREAM_BUFFER_CAPACITY);
        let shutdown = self.shutdown.child_token();
        let (backend_key, consumer_id) = self
            .add_consumer(
                key.clone(),
                WatchConsumer::Stream {
                    root: key.root.clone(),
                    recursive: key.recursive,
                    tx,
                    bytes: Arc::new(tokio::sync::Semaphore::new(WATCH_CONSUMER_BYTE_CAPACITY)),
                    shutdown: shutdown.clone(),
                    filter,
                },
            )
            .await?;

        let manager = self.clone();
        let cleanup_key = backend_key.clone();
        let cleanup_shutdown = shutdown.clone();
        self.tasks.spawn(async move {
            cleanup_shutdown.cancelled().await;
            manager.remove_consumer(&cleanup_key, consumer_id).await;
        });

        Ok(Box::pin(CancelOnDropStream {
            inner: tokio_stream::wrappers::ReceiverStream::new(rx),
            shutdown,
        }))
    }

    pub(super) async fn create_watcher(
        &self,
        root: GuestPath,
        recursive: bool,
        include_metadata_events: bool,
        include_globs: Vec<RelativeGlobPattern>,
        exclude_globs: Vec<RelativeGlobPattern>,
        use_default_excludes: Option<bool>,
    ) -> Result<(String, String), Status> {
        let root = GuestWatchRoot::new(root);
        let root = normalize_watch_root(root.guest_path()).await?;
        let normalized_root = path_to_string(&root)?;
        let filter_config = WatchFilterConfig::new(
            include_globs,
            exclude_globs,
            use_default_excludes.unwrap_or(false),
        )?;
        let filter = WatchFilter::compile(filter_config.clone())?;
        let key = WatchRootKey {
            root,
            recursive,
            include_metadata_events,
            filter_config: filter_config.backend_config(),
        };
        let (_, consumer_id) = self
            .add_consumer(
                key.clone(),
                WatchConsumer::Pull {
                    root: key.root.clone(),
                    recursive: key.recursive,
                    events: VecDeque::new(),
                    retained_bytes: 0,
                    overflowed: false,
                    last_access: Instant::now(),
                    filter,
                },
            )
            .await?;

        Ok((format!("w{consumer_id}"), normalized_root))
    }

    pub(super) async fn get_watcher_events(
        &self,
        watcher_id: &str,
    ) -> Result<Vec<FileEvent>, Status> {
        let mut state = self.inner.lock().await;
        let reference = state
            .pull_watchers
            .get(watcher_id)
            .cloned()
            .ok_or_else(|| Status::not_found("watcher not found"))?;

        let mut expired = false;
        let result = {
            let root = state
                .roots
                .get_mut(&reference.root)
                .ok_or_else(|| Status::not_found("watcher not found"))?;
            let consumer = root
                .consumers
                .get_mut(&reference.consumer_id)
                .ok_or_else(|| Status::not_found("watcher not found"))?;
            let WatchConsumer::Pull {
                events,
                retained_bytes,
                overflowed,
                last_access,
                ..
            } = consumer
            else {
                return Err(Status::not_found("watcher not found"));
            };

            if last_access.elapsed() > PULL_WATCHER_TTL {
                expired = true;
                Err(Status::not_found("watcher expired"))
            } else {
                *last_access = Instant::now();
                *retained_bytes = 0;
                if *overflowed {
                    events.clear();
                    *overflowed = false;
                    Err(Status::resource_exhausted(
                        "watcher event history is incomplete",
                    ))
                } else {
                    Ok(events.drain(..).map(|event| event.to_proto()).collect())
                }
            }
        };

        if expired {
            state.pull_watchers.remove(watcher_id);
            let root_shutdown =
                remove_consumer_locked(&mut state, &reference.root, reference.consumer_id);
            drop(state);
            cancel_root(root_shutdown);
        }
        result
    }

    pub(super) async fn remove_watcher(&self, watcher_id: &str) -> Result<(), Status> {
        let mut state = self.inner.lock().await;
        let reference = state
            .pull_watchers
            .remove(watcher_id)
            .ok_or_else(|| Status::not_found("watcher not found"))?;
        let root_shutdown =
            remove_consumer_locked(&mut state, &reference.root, reference.consumer_id);
        drop(state);
        cancel_root(root_shutdown);
        Ok(())
    }

    pub(super) async fn enable_file_changes(
        &self,
        roots: Vec<GuestPath>,
        include_globs: Vec<RelativeGlobPattern>,
        exclude_globs: Vec<RelativeGlobPattern>,
        use_default_excludes: Option<bool>,
    ) -> Result<u64, Status> {
        let generation = {
            let state = self.inner.lock().await;
            if let LedgerState::Active(ledger) = &state.ledger {
                return Ok(ledger.observed_since_unix_ms);
            }
            state.ledger_generation
        };

        if roots.len() > MAX_LEDGER_ROOTS {
            return Err(Status::resource_exhausted("too many file change roots"));
        }

        let roots = if roots.is_empty() {
            default_ledger_roots().await?
        } else {
            normalize_ledger_roots(roots).await?
        };
        if roots.len() > MAX_LEDGER_ROOTS {
            return Err(Status::resource_exhausted("too many file change roots"));
        }
        let filter_config = WatchFilterConfig::new(
            include_globs,
            exclude_globs,
            use_default_excludes.unwrap_or(true),
        )?;
        let filter = WatchFilter::compile(filter_config.clone())?;

        // Preparation is cancellable and owns no consumers. Only publication
        // and rollback below run without suspension under the state lock.
        let mut plans = Vec::with_capacity(roots.len());
        let mut planned_directories = 0;
        for root in roots {
            let key = WatchRootKey {
                root,
                recursive: true,
                include_metadata_events: true,
                filter_config: filter_config.backend_config(),
            };
            let backend_filter = WatchFilter::compile(key.filter_config.clone())?;
            let plan = plan_watch_registration(
                &key.root,
                key.recursive,
                &backend_filter,
                &self.scan_slots,
                &self.tasks,
            )
            .await?;
            planned_directories += plan.len();
            if planned_directories > MAX_WATCHED_DIRECTORIES {
                return Err(Status::resource_exhausted(
                    "file change setup directory budget exceeded",
                ));
            }
            let registration = RegisteredWatch::prepare(plan, self.tasks.clone()).await?;
            plans.push((key, backend_filter, registration));
        }

        let mut state = self.inner.lock().await;
        if self.shutdown.is_cancelled() {
            return Err(crate::refused(Status::unavailable(
                "file watching shut down",
            )));
        }
        if state.ledger_generation != generation {
            return Err(Status::aborted(
                "file change setup superseded; retry enable",
            ));
        }
        if let LedgerState::Active(ledger) = &state.ledger {
            return Ok(ledger.observed_since_unix_ms);
        }
        let observed_since_unix_ms = now_unix_ms()?;
        let mut ledger = Ledger::new(generation, observed_since_unix_ms);
        for (key, backend_filter, plan) in plans {
            match self.add_consumer_locked(
                &mut state,
                key.clone(),
                WatchConsumer::Ledger {
                    root: key.root.clone(),
                    filter: filter.clone(),
                },
                backend_filter,
                plan,
            ) {
                Ok((backend_key, consumer_id)) => {
                    ledger.root_consumers.push(WatchConsumerRef {
                        root: backend_key,
                        consumer_id,
                    });
                }
                Err(error) => {
                    for reference in ledger.root_consumers {
                        cancel_root(remove_consumer_locked(
                            &mut state,
                            &reference.root,
                            reference.consumer_id,
                        ));
                    }
                    return Err(error);
                }
            }
        }
        state.ledger = LedgerState::Active(ledger);
        Ok(observed_since_unix_ms)
    }

    pub(super) async fn disable_file_changes(&self) {
        let mut state = self.inner.lock().await;
        // Fence both pending setup and event classification from the old session.
        state.ledger_generation += 1;
        if matches!(state.ledger, LedgerState::Active(_))
            && let LedgerState::Active(ledger) = std::mem::take(&mut state.ledger)
        {
            for reference in ledger.root_consumers {
                cancel_root(remove_consumer_locked(
                    &mut state,
                    &reference.root,
                    reference.consumer_id,
                ));
            }
        }
    }

    pub(super) async fn get_modified_files(
        &self,
        request: GetModifiedFilesRequest,
    ) -> Result<GetModifiedFilesResponse, Status> {
        let filters = [
            request.window.is_some(),
            request.since.is_some(),
            request.cursor.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();
        if filters > 1 {
            return Err(Status::invalid_argument(
                "window, since, and cursor are mutually exclusive",
            ));
        }

        let mut state = self.inner.lock().await;
        let discovering = matches!(&state.ledger, LedgerState::Active(ledger)
            if ledger.root_consumers.iter().any(|reference| state.roots.get(&reference.root).is_some_and(|root| root.discovering)));
        let LedgerState::Active(ledger) = &mut state.ledger else {
            return Ok(GetModifiedFilesResponse {
                files: Vec::new(),
                observed_since: None,
                retained_since: None,
                cursor: None,
                monitoring_enabled: false,
                overflowed: matches!(state.ledger, LedgerState::Lost),
            });
        };
        let mut response = ledger.modified_files_response(request)?;
        response.overflowed |= discovering;
        Ok(response)
    }

    async fn add_consumer(
        &self,
        key: WatchRootKey,
        consumer: WatchConsumer,
    ) -> Result<(WatchRootKey, u64), Status> {
        let backend_filter = WatchFilter::compile(key.filter_config.clone())?;

        {
            let mut state = self.inner.lock().await;
            if self.shutdown.is_cancelled() {
                return Err(crate::refused(Status::unavailable(
                    "file watching shut down",
                )));
            }
            if active_consumer_count(&state) >= MAX_ACTIVE_WATCHER_CONSUMERS {
                return Err(crate::refused(Status::resource_exhausted(
                    "too many active file watcher consumers",
                )));
            }
            let backend_key = key.clone();
            if state.roots.contains_key(&backend_key) {
                let consumer_id = insert_consumer_locked(self, &mut state, &backend_key, consumer)?;
                return Ok((backend_key, consumer_id));
            }
        }

        let registration_plan = plan_watch_registration(
            &key.root,
            key.recursive,
            &backend_filter,
            &self.scan_slots,
            &self.tasks,
        )
        .await?;
        let registration = RegisteredWatch::prepare(registration_plan, self.tasks.clone()).await?;
        let mut state = self.inner.lock().await;
        self.add_consumer_locked(&mut state, key, consumer, backend_filter, registration)
    }

    fn add_consumer_locked(
        &self,
        state: &mut WatchManagerState,
        key: WatchRootKey,
        consumer: WatchConsumer,
        backend_filter: WatchFilter,
        registration: RegisteredWatch,
    ) -> Result<(WatchRootKey, u64), Status> {
        if self.shutdown.is_cancelled() {
            return Err(crate::refused(Status::unavailable(
                "file watching shut down",
            )));
        }
        let directory_count = registration.directory_count();
        if active_consumer_count(state) >= MAX_ACTIVE_WATCHER_CONSUMERS {
            return Err(crate::refused(Status::resource_exhausted(
                "too many active file watcher consumers",
            )));
        }
        let backend_key = key.clone();
        if state.roots.contains_key(&backend_key) {
            let consumer_id = insert_consumer_locked(self, state, &backend_key, consumer)?;
            return Ok((backend_key, consumer_id));
        }
        if directory_budget_exceeded(state, directory_count) {
            return Err(crate::refused(Status::resource_exhausted(
                "file watcher directory budget exceeded",
            )));
        }

        let shutdown = self.shutdown.child_token();
        let generation = next_root_generation_locked(state);
        let manager = self.clone();
        let task_key = backend_key.clone();
        let directory_manager = self.clone();
        let directory_task_key = backend_key.clone();
        spawn_notify_task(
            NotifyTaskConfig {
                tasks: self.tasks.clone(),
                root: backend_key.root.clone(),
                recursive: backend_key.recursive,
                include_metadata_events: backend_key.include_metadata_events,
                filter: backend_filter,
                registration,
                shutdown: shutdown.clone(),
            },
            Arc::clone(&self.scan_slots),
            move |event| {
                let manager = manager.clone();
                let task_key = task_key.clone();
                async move {
                    match event {
                        Ok(event) => {
                            manager
                                .dispatch_root_event(&task_key, generation, event)
                                .await
                        }
                        Err(status) => {
                            manager
                                .dispatch_root_error(&task_key, generation, status)
                                .await
                        }
                    }
                }
            },
            move |reserve_count, release_count, discovering| {
                let manager = directory_manager.clone();
                let task_key = directory_task_key.clone();
                async move {
                    manager
                        .adjust_root_directories(
                            &task_key,
                            generation,
                            reserve_count,
                            release_count,
                            discovering,
                        )
                        .await
                }
            },
        );
        state.roots.insert(
            backend_key.clone(),
            WatchRoot {
                generation,
                consumers: HashMap::new(),
                directory_count,
                discovering: backend_key.recursive,
                shutdown,
            },
        );
        let consumer_id = insert_consumer_locked(self, state, &backend_key, consumer)?;
        Ok((backend_key, consumer_id))
    }

    async fn adjust_root_directories(
        &self,
        key: &WatchRootKey,
        generation: WatchRootGeneration,
        reserve_count: usize,
        release_count: usize,
        discovering: Option<bool>,
    ) -> DirectoryAdjustment {
        let mut state = self.inner.lock().await;
        let result =
            adjust_directories_locked(&mut state, key, generation, reserve_count, release_count);
        if result == DirectoryAdjustment::Applied
            && let Some(discovering) = discovering
        {
            let root = state.roots.get_mut(key).expect("root generation checked");
            let completed = std::mem::replace(&mut root.discovering, discovering) && !discovering;
            if completed
                && let LedgerState::Active(ledger) = &mut state.ledger
                && ledger
                    .root_consumers
                    .iter()
                    .any(|reference| &reference.root == key)
            {
                ledger.record_loss(now_unix_ms().unwrap_or(ledger.observed_since_unix_ms));
            }
        }
        result
    }

    async fn remove_consumer(&self, key: &WatchRootKey, consumer_id: u64) {
        let mut state = self.inner.lock().await;
        let root_shutdown = remove_consumer_locked(&mut state, key, consumer_id);
        drop(state);
        cancel_root(root_shutdown);
    }

    async fn dispatch_root_event(
        &self,
        key: &WatchRootKey,
        generation: WatchRootGeneration,
        event: WatchEvent,
    ) -> bool {
        let mut state = self.inner.lock().await;
        let mut stream_targets = Vec::new();
        let mut ledger_record_roots = Vec::new();
        let active_ledger_session_id = match &state.ledger {
            LedgerState::Active(ledger) => Some(ledger.session_id),
            _ => None,
        };

        let Some(root) = root_for_backend_mut(&mut state, key, generation) else {
            return false;
        };
        for (consumer_id, consumer) in &mut root.consumers {
            match consumer {
                WatchConsumer::Stream {
                    root,
                    recursive,
                    tx,
                    bytes,
                    shutdown,
                    filter,
                } => {
                    let backend_filter_applies = root == &key.root;
                    if !consumer_accepts_event(
                        event.absolute_path(),
                        root,
                        *recursive,
                        filter,
                        backend_filter_applies,
                    ) {
                        continue;
                    }
                    stream_targets.push((
                        *consumer_id,
                        tx.clone(),
                        shutdown.clone(),
                        bytes.clone(),
                    ));
                }
                WatchConsumer::Pull {
                    root,
                    recursive,
                    events,
                    retained_bytes,
                    overflowed,
                    filter,
                    ..
                } => {
                    let backend_filter_applies = root == &key.root;
                    if !consumer_accepts_event(
                        event.absolute_path(),
                        root,
                        *recursive,
                        filter,
                        backend_filter_applies,
                    ) {
                        continue;
                    }
                    let bytes = event.retained_bytes();
                    if event.path_encoding() != WatchPathEncoding::Exact
                        || events.len() >= PULL_WATCHER_BUFFER_CAPACITY
                        || retained_bytes.saturating_add(bytes) > WATCH_CONSUMER_BYTE_CAPACITY
                    {
                        events.clear();
                        *retained_bytes = 0;
                        *overflowed = true;
                    } else if !*overflowed {
                        *retained_bytes += bytes;
                        events.push_back(event.clone());
                    }
                }
                WatchConsumer::Ledger { root, filter } => {
                    let backend_filter_applies = root == &key.root;
                    if !consumer_accepts_event(
                        event.absolute_path(),
                        root,
                        true,
                        filter,
                        backend_filter_applies,
                    ) {
                        continue;
                    }
                    if active_ledger_session_id.is_some() {
                        ledger_record_roots.push(root.clone());
                    }
                }
            }
        }

        drop(state);

        let mut remove_consumers = Vec::new();
        for (consumer_id, tx, shutdown, bytes) in stream_targets {
            if event.path_encoding() != WatchPathEncoding::Exact {
                send_stream_loss_and_cancel(
                    &self.tasks,
                    tx,
                    shutdown,
                    "file watch path cannot be represented exactly",
                );
                remove_consumers.push(consumer_id);
                continue;
            }
            let proto = event.to_proto();
            let size = proto.path.len() + std::mem::size_of::<QueuedFileEvent>();
            let Ok(permit) = bytes.try_acquire_many_owned(size as u32) else {
                send_stream_loss_and_cancel(
                    &self.tasks,
                    tx,
                    shutdown,
                    "file watch stream buffer full",
                );
                remove_consumers.push(consumer_id);
                continue;
            };
            match tx.try_send(Ok(QueuedFileEvent {
                event: proto,
                _bytes: permit,
            })) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    shutdown.cancel();
                    remove_consumers.push(consumer_id);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    send_stream_loss_and_cancel(
                        &self.tasks,
                        tx,
                        shutdown,
                        "file watch stream buffer full",
                    );
                    remove_consumers.push(consumer_id);
                }
            }
        }
        if !remove_consumers.is_empty() {
            let mut state = self.inner.lock().await;
            let mut root_shutdown = None;
            if root_for_backend_mut(&mut state, key, generation).is_some() {
                for consumer_id in remove_consumers {
                    root_shutdown = root_shutdown
                        .or_else(|| remove_consumer_locked(&mut state, key, consumer_id));
                }
            }
            drop(state);
            cancel_root(root_shutdown);
        }

        if let Some(session_id) = active_ledger_session_id
            && !ledger_record_roots.is_empty()
        {
            ledger_record_roots.sort();
            ledger_record_roots.dedup();
            self.record_ledger_event(session_id, ledger_record_roots, &event)
                .await;
        }
        true
    }

    async fn dispatch_root_error(
        &self,
        key: &WatchRootKey,
        generation: WatchRootGeneration,
        status: Status,
    ) -> bool {
        let mut state = self.inner.lock().await;
        let Some(root) = remove_root_for_backend(&mut state, key, generation) else {
            return false;
        };

        let had_ledger_consumer = root
            .consumers
            .values()
            .any(|consumer| matches!(consumer, WatchConsumer::Ledger { .. }));
        let mut failed_pull_watchers = Vec::new();
        for (watcher_id, reference) in &state.pull_watchers {
            if &reference.root == key {
                failed_pull_watchers.push(watcher_id.clone());
            }
        }
        for watcher_id in failed_pull_watchers {
            state.pull_watchers.remove(&watcher_id);
        }

        let mut ledger_root_shutdowns = Vec::new();
        if had_ledger_consumer
            && let LedgerState::Active(ledger) =
                std::mem::replace(&mut state.ledger, LedgerState::Lost)
        {
            state.ledger_generation += 1;
            tracing::info!(
                root_path_bytes = key.root.as_os_str().as_encoded_bytes().len(),
                code = ?status.code(),
                "file-change ledger disabled because one observed root failed"
            );
            // A partial ledger would report an apparently complete snapshot while one
            // configured root is dark, so disable the whole collection session.
            for reference in ledger.root_consumers {
                if &reference.root == key {
                    continue;
                }
                if let Some(shutdown) =
                    remove_consumer_locked(&mut state, &reference.root, reference.consumer_id)
                {
                    ledger_root_shutdowns.push(shutdown);
                }
            }
        }

        let stream_senders: Vec<_> = root
            .consumers
            .into_values()
            .filter_map(|consumer| match consumer {
                WatchConsumer::Stream { tx, .. } => Some(tx),
                WatchConsumer::Pull { .. } | WatchConsumer::Ledger { .. } => None,
            })
            .collect();
        let root_shutdown = root.shutdown;
        drop(state);

        for tx in stream_senders {
            if tx
                .try_send(Err(Status::new(
                    status.code(),
                    status.message().to_string(),
                )))
                .is_err()
            {
                tracing::debug!("file watch stream receiver closed before root error dispatch");
            }
        }
        root_shutdown.cancel();
        for shutdown in ledger_root_shutdowns {
            shutdown.cancel();
        }
        true
    }

    async fn record_ledger_event(
        &self,
        session_id: u64,
        roots: Vec<std::path::PathBuf>,
        event: &WatchEvent,
    ) {
        let is_regular_file = event.path_encoding() != WatchPathEncoding::Unsupported
            && classify_ledger_event_regular_file(event, &self.tasks).await;
        let mut state = self.inner.lock().await;
        let LedgerState::Active(ledger) = &mut state.ledger else {
            return;
        };
        if ledger.session_id != session_id {
            return;
        }
        for root in roots {
            ledger.record_event(&root, event, is_regular_file);
        }
    }
}

fn start_pull_reaper_if_needed(
    inner: &Arc<Mutex<WatchManagerState>>,
    state: &mut WatchManagerState,
    shutdown: CancellationToken,
    tasks: &tokio_util::task::TaskTracker,
) {
    if state.pull_reaper_running {
        return;
    }
    state.pull_reaper_running = true;
    let weak = Arc::downgrade(inner);
    tasks.spawn(async move {
        run_pull_reaper(weak, shutdown).await;
    });
}

async fn run_pull_reaper(inner: Weak<Mutex<WatchManagerState>>, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(PULL_WATCHER_REAPER_INTERVAL) => {}
        }
        let Some(inner) = inner.upgrade() else {
            break;
        };
        let mut state = inner.lock().await;
        if state.pull_watchers.is_empty() {
            state.pull_reaper_running = false;
            break;
        }
        let root_shutdowns = reap_expired_pull_watchers_locked(&mut state);
        drop(state);
        for shutdown in root_shutdowns {
            cancel_root(Some(shutdown));
        }
    }
}

fn cancel_root(root_shutdown: Option<CancellationToken>) {
    if let Some(shutdown) = root_shutdown {
        shutdown.cancel();
    }
}

fn insert_consumer_locked(
    manager: &WatchManager,
    state: &mut WatchManagerState,
    backend_key: &WatchRootKey,
    consumer: WatchConsumer,
) -> Result<u64, Status> {
    let next_consumer_id = &mut state.next_consumer_id;
    let Some(root) = state.roots.get_mut(backend_key) else {
        return Err(Status::internal(
            "file watcher root registration missing after creation",
        ));
    };
    *next_consumer_id += 1;
    let consumer_id = *next_consumer_id;
    let is_pull = matches!(&consumer, WatchConsumer::Pull { .. });
    root.consumers.insert(consumer_id, consumer);
    if is_pull {
        // Publish expiry ownership in the same critical section as admission;
        // request cancellation must never leave an unindexed consumer.
        state.pull_watchers.insert(
            format!("w{consumer_id}"),
            WatchConsumerRef {
                root: backend_key.clone(),
                consumer_id,
            },
        );
        start_pull_reaper_if_needed(
            &manager.inner,
            state,
            manager.shutdown.clone(),
            &manager.tasks,
        );
    }
    Ok(consumer_id)
}

fn send_stream_loss_and_cancel(
    tasks: &tokio_util::task::TaskTracker,
    tx: mpsc::Sender<Result<QueuedFileEvent, Status>>,
    shutdown: CancellationToken,
    message: &'static str,
) {
    tasks.spawn(async move {
        match tokio::time::timeout(
            Duration::from_millis(100),
            tx.send(Err(Status::resource_exhausted(message))),
        )
        .await
        {
            Ok(Ok(())) | Ok(Err(_)) | Err(_) => {}
        }
        shutdown.cancel();
    });
}

fn now_unix_ms() -> Result<u64, Status> {
    timestamp_to_unix_ms(pplx_space_spaced_guest_protocol::system_time_to_proto(
        SystemTime::now(),
    )?)
}

pub(super) fn unix_ms_to_timestamp(unix_ms: u64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: (unix_ms / 1_000) as i64,
        nanos: ((unix_ms % 1_000) * 1_000_000) as i32,
    }
}

fn timestamp_to_unix_ms(timestamp: prost_types::Timestamp) -> Result<u64, Status> {
    nonnegative_time_to_ms(timestamp.seconds, timestamp.nanos)
}

fn duration_to_ms(duration: prost_types::Duration) -> Result<u64, Status> {
    nonnegative_time_to_ms(duration.seconds, duration.nanos)
}

fn nonnegative_time_to_ms(seconds: i64, nanos: i32) -> Result<u64, Status> {
    let nanos = u64::try_from(nanos)
        .ok()
        .filter(|nanos| *nanos < 1_000_000_000)
        .ok_or_else(|| Status::invalid_argument("time nanos must be in 0..1000000000"))?;
    u64::try_from(seconds)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000))
        .and_then(|millis| millis.checked_add(nanos / 1_000_000))
        .ok_or_else(|| Status::invalid_argument("time value is outside the millisecond range"))
}

async fn classify_ledger_event_regular_file(
    event: &WatchEvent,
    tasks: &tokio_util::task::TaskTracker,
) -> bool {
    if event.kind() == FileEventKind::Removed {
        return false;
    }
    if let Some(is_regular_file) = event.is_regular_file() {
        return is_regular_file;
    }
    let path = event.absolute_path().to_owned();
    tasks
        .spawn_blocking(move || {
            std::fs::metadata(path)
                .map(|metadata| metadata.is_file())
                .unwrap_or(false)
        })
        .await
        .unwrap_or(false)
}

struct CancelOnDropStream<S> {
    inner: S,
    shutdown: CancellationToken,
}

impl<S> Stream for CancelOnDropStream<S>
where
    S: Stream<Item = Result<QueuedFileEvent, Status>> + Unpin,
{
    type Item = Result<FileEvent, Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.inner)
            .poll_next(cx)
            .map(|item| item.map(|item| item.map(|queued| queued.event)))
    }
}

impl<S> Drop for CancelOnDropStream<S> {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}
