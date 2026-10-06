use std::collections::HashMap;
use std::io;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, RawFd};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nix::sys::socket::{AddressFamily, SockFlag, SockType, socketpair};
use pplx_space_common_core::types::{CLUSTER_ID_ENV, GuestUserName};
use pplx_space_spaced_common::proto::worker::worker_filesystem_client::WorkerFilesystemClient as FilesystemClient;
use pplx_space_spaced_common::proto::worker::worker_filesystem_server::WorkerFilesystemServer as FilesystemServer;
use tokio::process::{Child, Command as ChildCommand};
use tokio::sync::{Mutex, watch};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::{Status, transport::Channel};

use crate::identity::GuestAccount;

use super::super::{FilesystemWorkerService, WORKER_PROTOCOL_VERSION};

const WORKER_MODE_ENV: &str = "SPACE_SPACED_FILESYSTEM_WORKER";
const WORKER_USER_ENV: &str = "SPACE_SPACED_FILESYSTEM_WORKER_USER";
const WORKER_FD_ENV: &str = "SPACE_SPACED_FILESYSTEM_WORKER_FD";
/// Resolved before every OTel setting, so it is how a child names itself.
const SPACE_SERVICE_NAME_ENV: &str = "SPACE_SERVICE_NAME";

/// Whether the forwarded environment would give the worker its launcher's
/// service name. Only the OTel settings outrank the worker's own default, and
/// `DD_SERVICE` is not forwarded.
fn inherits_parent_service_name(env: &dyn pplx_space_common_core::env::Environment) -> bool {
    if env
        .var("OTEL_SERVICE_NAME")
        .is_ok_and(|value| !value.trim().is_empty())
    {
        return true;
    }
    env.var("OTEL_RESOURCE_ATTRIBUTES").is_ok_and(|raw| {
        raw.split_terminator(',')
            .filter_map(|entry| entry.split_once('='))
            .any(|(key, value)| key.trim() == "service.name" && !value.trim().is_empty())
    })
}

const WORKER_ENV_ALLOWLIST: &[&str] = &[
    "RUST_LOG",
    "SPACE_LOG_FORMAT",
    "SPACE_ENV",
    "SPACE_VERSION",
    // Identity-isolated workers still need the parent's shared OTel resource
    // attributes and service override; process-local identity is resolved anew.
    "OTEL_RESOURCE_ATTRIBUTES",
    "OTEL_SERVICE_NAME",
    // Preserve the selection so the child validates it, rather than reverting
    // to the native default in this identity-isolated process.
    "SPACE_METRICS_SINK",
    // Re-exec must preserve trace selection and transport; dropping an explicit
    // off/otlp choice would silently restore the child's native default.
    "SPACE_TRACE_EXPORTER",
    // Trace verbosity is selected independently of RUST_LOG, so the re-exec
    // would otherwise drop an explicit choice and export a different span set.
    "SPACE_TRACE_FILTER",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_PROTOCOL",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
    "OTEL_TRACES_SAMPLER",
    "OTEL_TRACES_SAMPLER_ARG",
    "OTEL_BSP_SCHEDULE_DELAY",
    "OTEL_BSP_MAX_QUEUE_SIZE",
    "OTEL_BSP_MAX_EXPORT_BATCH_SIZE",
    "OTEL_BSP_EXPORT_TIMEOUT",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
    // The worker's metrics are exported by its own reader, so an explicitly
    // chosen export cadence has to cross the re-exec with the endpoint it
    // applies to; the launcher's cadence does not reach the child.
    "OTEL_METRIC_EXPORT_INTERVAL",
    "OTEL_EXPORTER_OTLP_HEADERS",
    "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
    "HOSTNAME",
    "SPACE_NODE_ID",
    // Clusters can share a DD_ENV; preserve the more specific cluster identity.
    CLUSTER_ID_ENV,
    "ENVIRONMENT",
    "KUBERNETES_SERVICE_HOST",
    "DD_AGENT_HOST",
    "DD_DOGSTATSD_PORT",
    "DD_ENV",
    "DD_VERSION",
];
const MAX_IDENTITY_WORKERS: usize = 16;
const IDENTITY_WORKER_IDLE_TTL: Duration = Duration::from_secs(10 * 60);
/// How soon the reaper looks again at a worker whose idle TTL has passed
/// while an operation still holds it.
const IDENTITY_WORKER_BUSY_RECHECK: Duration = Duration::from_secs(60);
const IDENTITY_WORKER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const IDENTITY_WORKER_START_WAIT_TIMEOUT: Duration = Duration::from_secs(15);
const IDENTITY_WORKER_MAX_BLOCKING_THREADS: usize = 16;
const IDENTITY_WORKER_MAX_CONCURRENT_REQUESTS: usize = 32;
// Re-encoding adds a 36-byte execution UUID plus protobuf tags/lengths,
// including nested Open frames. Public ingress remains capped at 4 MiB.
const WORKER_MAX_MESSAGE_BYTES: usize =
    pplx_space_spaced_common::filesystem_limits::MAX_MESSAGE_BYTES + 64;

fn clear_fd_cloexec(fd: impl AsFd) -> io::Result<()> {
    nix::fcntl::fcntl(
        fd,
        nix::fcntl::FcntlArg::F_SETFD(nix::fcntl::FdFlag::empty()),
    )?;
    Ok(())
}

#[derive(Clone)]
pub(crate) struct WorkerClient {
    pub(crate) client: FilesystemClient<tonic::transport::Channel>,
    pub(crate) user: GuestUserName,
    pub(crate) instance_id: uuid::Uuid,
    pub(crate) ledger_update_slot: Arc<tokio::sync::Semaphore>,
    activity: Arc<()>,
    stop_token: CancellationToken,
    supervisor: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// The worker process, for duplicating descriptors it opened as its user.
    pidfd: Option<Arc<std::os::fd::OwnedFd>>,
}

struct ResidentWorker {
    client: WorkerClient,
    last_access: Instant,
    ledger_enabled: bool,
}

impl ResidentWorker {
    fn new(client: WorkerClient) -> Self {
        Self {
            client,
            last_access: Instant::now(),
            ledger_enabled: false,
        }
    }

    fn is_idle(&self) -> bool {
        !self.ledger_enabled && Arc::strong_count(&self.client.activity) == 1
    }
}

struct StartingWorker {
    startup_id: uuid::Uuid,
    result: watch::Receiver<Option<Result<WorkerClient, Status>>>,
}

enum WorkerEntry {
    Starting(StartingWorker),
    Resident(Box<ResidentWorker>),
}

enum ClientAcquisition {
    Ready(Box<WorkerClient>),
    Waiting(watch::Receiver<Option<Result<WorkerClient, Status>>>),
    Start {
        startup_id: uuid::Uuid,
        result_tx: watch::Sender<Option<Result<WorkerClient, Status>>>,
        result_rx: watch::Receiver<Option<Result<WorkerClient, Status>>>,
    },
}

#[derive(Clone)]
pub(crate) struct WorkerPool {
    shutdown: CancellationToken,
    workers: Arc<Mutex<HashMap<GuestUserName, WorkerEntry>>>,
    process_slots: Arc<tokio::sync::Semaphore>,
    /// Whether the idle reaper task is running. It runs only while a worker
    /// that can expire is resident, so an unused daemon has no timer.
    reaper_running: Arc<std::sync::atomic::AtomicBool>,
}

/// Removes resident workers that are idle and past their TTL, for the caller
/// to stop outside the pool lock.
fn take_expired_workers(
    workers: &mut HashMap<GuestUserName, WorkerEntry>,
    now: Instant,
) -> Vec<WorkerClient> {
    let expired_users = workers
        .iter()
        .filter(|(_, entry)| {
            matches!(
                entry,
                WorkerEntry::Resident(worker)
                    if worker.is_idle()
                        && now.saturating_duration_since(worker.last_access)
                            > IDENTITY_WORKER_IDLE_TTL
            )
        })
        .map(|(user, _)| user.clone())
        .collect::<Vec<_>>();
    expired_users
        .into_iter()
        .filter_map(|expired_user| match workers.remove(&expired_user) {
            Some(WorkerEntry::Resident(worker)) => Some(worker.client),
            _ => None,
        })
        .collect()
}

/// When the reaper must look next: the earliest TTL expiry among workers
/// that can expire (a worker kept for the change ledger cannot), or a
/// recheck for one whose TTL passed while an operation held it. `None` when
/// no resident worker can expire.
fn next_worker_expiry(
    workers: &HashMap<GuestUserName, WorkerEntry>,
    now: Instant,
) -> Option<Instant> {
    workers
        .values()
        .filter_map(|entry| match entry {
            WorkerEntry::Resident(worker) if !worker.ledger_enabled => {
                let expiry = worker.last_access + IDENTITY_WORKER_IDLE_TTL;
                Some(if worker.is_idle() {
                    expiry
                } else {
                    expiry.max(now + IDENTITY_WORKER_BUSY_RECHECK)
                })
            }
            _ => None,
        })
        .min()
}

impl WorkerPool {
    pub(crate) async fn drain(&self) {
        self.shutdown.cancel();
        let workers: Vec<_> = self
            .workers
            .lock()
            .await
            .drain()
            .filter_map(|(_, entry)| match entry {
                WorkerEntry::Resident(resident) => Some(resident.client),
                WorkerEntry::Starting(_) => None,
            })
            .collect();
        for worker in &workers {
            worker.stop_token.cancel();
        }
        futures_util::future::join_all(workers.iter().map(WorkerClient::stop)).await;
        // Includes startup and retiring workers, not just resident map entries.
        if let Err(error) = self
            .process_slots
            .clone()
            .acquire_many_owned(MAX_IDENTITY_WORKERS as u32)
            .await
        {
            tracing::error!(%error, "identity worker drain accounting closed");
        }
    }

    pub(crate) fn new(shutdown: CancellationToken) -> Self {
        Self {
            shutdown,
            workers: Arc::new(Mutex::new(HashMap::new())),
            process_slots: Arc::new(tokio::sync::Semaphore::new(MAX_IDENTITY_WORKERS)),
            reaper_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Stops idle workers once their TTL passes, without waiting for the next
    /// acquisition: an unused daemon otherwise keeps every worker process
    /// (and its memory) resident until filesystem traffic resumes. One task
    /// at most; it sleeps until the next expiry and exits when no resident
    /// worker can expire.
    fn ensure_reaper(&self) {
        use std::sync::atomic::Ordering;
        if self.reaper_running.swap(true, Ordering::AcqRel) {
            return;
        }
        let pool = self.clone();
        drop(tokio::spawn(async move {
            loop {
                let next = {
                    let mut workers = pool.workers.lock().await;
                    let expired = take_expired_workers(&mut workers, Instant::now());
                    let next = next_worker_expiry(&workers, Instant::now());
                    drop(workers);
                    Self::stop_workers(expired);
                    next
                };
                let Some(deadline) = next else {
                    pool.reaper_running.store(false, Ordering::Release);
                    // A worker published after the look above may have seen
                    // the reaper as running and not started one: look again.
                    let expirable = next_worker_expiry(&*pool.workers.lock().await, Instant::now());
                    if expirable.is_some() && !pool.reaper_running.swap(true, Ordering::AcqRel) {
                        continue;
                    }
                    return;
                };
                tokio::select! {
                    () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {}
                    () = pool.shutdown.cancelled() => {
                        pool.reaper_running.store(false, Ordering::Release);
                        return;
                    }
                }
            }
        }));
    }

    pub(crate) async fn client_for(&self, user: GuestUserName) -> Result<WorkerClient, Status> {
        let (acquisition, mut workers_to_stop) = {
            let mut workers = self.workers.lock().await;
            let mut workers_to_stop = take_expired_workers(&mut workers, Instant::now());

            let acquisition = match workers.get_mut(&user) {
                Some(WorkerEntry::Resident(worker)) => {
                    worker.last_access = Instant::now();
                    ClientAcquisition::Ready(Box::new(worker.client.clone()))
                }
                Some(WorkerEntry::Starting(worker)) => {
                    ClientAcquisition::Waiting(worker.result.clone())
                }
                None => {
                    if workers.len() >= MAX_IDENTITY_WORKERS {
                        let evict = workers
                            .iter()
                            .filter_map(|(user, entry)| match entry {
                                WorkerEntry::Resident(worker) if worker.is_idle() => {
                                    Some((user.clone(), worker.last_access))
                                }
                                _ => None,
                            })
                            .min_by_key(|(_, last_access)| *last_access)
                            .map(|(user, _)| user);
                        let Some(evict) = evict else {
                            drop(workers);
                            Self::stop_workers(workers_to_stop);
                            return Err(Status::resource_exhausted(
                                "too many active filesystem identity workers",
                            ));
                        };
                        if let Some(WorkerEntry::Resident(worker)) = workers.remove(&evict) {
                            workers_to_stop.push(worker.client);
                        }
                    }
                    let startup_id = uuid::Uuid::now_v7();
                    let (result_tx, result_rx) = watch::channel(None);
                    workers.insert(
                        user.clone(),
                        WorkerEntry::Starting(StartingWorker {
                            startup_id,
                            result: result_rx.clone(),
                        }),
                    );
                    ClientAcquisition::Start {
                        startup_id,
                        result_tx,
                        result_rx,
                    }
                }
            };
            (acquisition, workers_to_stop)
        };

        match acquisition {
            ClientAcquisition::Ready(worker) => {
                Self::stop_workers(workers_to_stop);
                Ok(*worker)
            }
            ClientAcquisition::Waiting(result) => {
                Self::stop_workers(workers_to_stop);
                Self::wait_for_start(result).await
            }
            ClientAcquisition::Start {
                startup_id,
                result_tx,
                result_rx,
            } => {
                let pool = self.clone();
                let worker_user = user.clone();
                drop(tokio::spawn(async move {
                    for worker in workers_to_stop.drain(..) {
                        worker.stop().await;
                    }
                    let result = pool.spawn_worker(&worker_user).await;
                    let result = pool.publish_start(&worker_user, startup_id, result).await;
                    if result_tx.send(Some(result)).is_err() {
                        tracing::debug!(
                            startup = %startup_id,
                            "filesystem identity worker startup completed after callers disconnected"
                        );
                    }
                }));
                Self::wait_for_start(result_rx).await
            }
        }
    }

    pub(crate) async fn idle_ledgers(&self) -> Vec<WorkerClient> {
        self.workers
            .lock()
            .await
            .values()
            .filter_map(|entry| match entry {
                WorkerEntry::Resident(worker)
                    if worker.ledger_enabled && Arc::strong_count(&worker.client.activity) == 1 =>
                {
                    Some(worker.client.clone())
                }
                _ => None,
            })
            .collect()
    }

    pub(crate) async fn set_ledger_enabled(&self, worker: &WorkerClient, enabled: bool) -> bool {
        if let Some(mut resident) = self.resident_mut(worker).await {
            resident.ledger_enabled = enabled;
            drop(resident);
            if !enabled {
                // It can expire again.
                self.ensure_reaper();
            }
            true
        } else {
            false
        }
    }

    pub(crate) async fn ledger_enabled(&self, user: &GuestUserName) -> bool {
        matches!(self.workers.lock().await.get(user),
            Some(WorkerEntry::Resident(worker)) if worker.ledger_enabled)
    }

    pub(crate) async fn remove(&self, worker: &WorkerClient) -> Option<bool> {
        let mut workers = self.workers.lock().await;
        match workers.get(&worker.user) {
            Some(WorkerEntry::Resident(resident))
                if resident.client.instance_id == worker.instance_id =>
            {
                let ledger_enabled = resident.ledger_enabled;
                workers.remove(&worker.user);
                Some(ledger_enabled)
            }
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) async fn invalidate(&self, worker: &WorkerClient) -> bool {
        let enabled = self.remove(worker).await.unwrap_or(false);
        worker.stop().await;
        enabled
    }

    /// [`Self::insert_test_client`] for a worker whose descriptors live in
    /// this process, so the router can duplicate them as it does a spawned
    /// worker's.
    #[cfg(test)]
    pub(crate) async fn insert_test_client_in_this_process(
        &self,
        user: GuestUserName,
        channel: Channel,
    ) -> Result<(), Status> {
        self.insert_test_client(user.clone(), channel).await?;
        let pidfd = rustix::process::pidfd_open(
            rustix::process::getpid(),
            rustix::process::PidfdFlags::empty(),
        )
        .map_err(|error| Status::internal(format!("pidfd_open: {error}")))?;
        if let Some(WorkerEntry::Resident(resident)) = self.workers.lock().await.get_mut(&user) {
            resident.client.pidfd = Some(Arc::new(pidfd));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn insert_test_client(
        &self,
        user: GuestUserName,
        channel: Channel,
    ) -> Result<(), Status> {
        let worker = WorkerClient {
            client: FilesystemClient::new(channel),
            user: user.clone(),
            instance_id: uuid::Uuid::now_v7(),
            activity: Arc::new(()),
            ledger_update_slot: Arc::new(tokio::sync::Semaphore::new(1)),
            stop_token: CancellationToken::new(),
            supervisor: Arc::new(Mutex::new(None)),
            pidfd: None,
        };
        self.workers.lock().await.insert(
            user,
            WorkerEntry::Resident(Box::new(ResidentWorker::new(worker))),
        );
        Ok(())
    }

    pub(crate) async fn contains(&self, worker: &WorkerClient) -> bool {
        self.workers
            .lock()
            .await
            .get(&worker.user)
            .is_some_and(|entry| {
                matches!(
                    entry,
                    WorkerEntry::Resident(resident)
                        if resident.client.instance_id == worker.instance_id
                )
            })
    }

    #[cfg(test)]
    pub(crate) async fn is_idle_test(&self, user: &GuestUserName) -> bool {
        self.workers.lock().await.get(user).is_some_and(
            |entry| matches!(entry, WorkerEntry::Resident(resident) if resident.is_idle()),
        )
    }

    async fn resident_mut(
        &self,
        worker: &WorkerClient,
    ) -> Option<tokio::sync::MappedMutexGuard<'_, ResidentWorker>> {
        let workers = self.workers.lock().await;
        tokio::sync::MutexGuard::try_map(workers, |workers| {
            workers.get_mut(&worker.user).and_then(|entry| match entry {
                WorkerEntry::Resident(resident)
                    if resident.client.instance_id == worker.instance_id =>
                {
                    Some(resident.as_mut())
                }
                _ => None,
            })
        })
        .ok()
    }

    fn stop_workers(workers: Vec<WorkerClient>) {
        if workers.is_empty() {
            return;
        }
        drop(tokio::spawn(async move {
            for worker in workers {
                worker.stop().await;
            }
        }));
    }

    async fn wait_for_start(
        result: watch::Receiver<Option<Result<WorkerClient, Status>>>,
    ) -> Result<WorkerClient, Status> {
        wait_for_start_with_timeout(result, IDENTITY_WORKER_START_WAIT_TIMEOUT).await
    }

    async fn publish_start(
        &self,
        user: &GuestUserName,
        startup_id: uuid::Uuid,
        result: Result<WorkerClient, Status>,
    ) -> Result<WorkerClient, Status> {
        let mut discard = None;
        {
            let mut workers = self.workers.lock().await;
            let current_start_matches = matches!(
                workers.get(user),
                Some(WorkerEntry::Starting(starting)) if starting.startup_id == startup_id
            );
            if current_start_matches {
                match &result {
                    Ok(worker) => {
                        workers.insert(
                            user.clone(),
                            WorkerEntry::Resident(Box::new(ResidentWorker::new(worker.clone()))),
                        );
                        self.ensure_reaper();
                    }
                    Err(_) => {
                        workers.remove(user);
                    }
                }
            } else if let Ok(worker) = &result {
                discard = Some(worker.clone());
            }
        }
        if let Some(worker) = discard {
            worker.stop().await;
        }
        result
    }

    async fn spawn_worker(&self, user: &GuestUserName) -> Result<WorkerClient, Status> {
        if self.shutdown.is_cancelled() {
            return Err(Status::unavailable("guest shutting down"));
        }
        let process_slot = self
            .process_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                Status::resource_exhausted("identity workers still running or terminating")
            })?;
        let lookup_user = user.clone();
        let process_slot = tokio::task::spawn_blocking(move || {
            GuestAccount::resolve(lookup_user).map_err(|error| error.into_status())?;
            Ok::<_, Status>(process_slot)
        })
        .await
        .map_err(|error| {
            Status::internal(format!("worker account resolution failed: {error}"))
        })??;
        let (router_fd, worker_fd) = socketpair(
            AddressFamily::Unix,
            SockType::Stream,
            None,
            SockFlag::SOCK_CLOEXEC,
        )
        .map_err(|error| worker_io_status(io::Error::from(error)))?;
        let worker_raw_fd = worker_fd.as_raw_fd();
        let executable = std::env::current_exe().map_err(worker_io_status)?;
        let mut command = worker_command(&executable, user, worker_raw_fd);
        // The socketpair is CLOEXEC by default. Clear it only in the forked
        // worker so concurrent user process spawns cannot inherit this fd.
        // SAFETY: `pre_exec` installs a closure that runs in the forked child
        // before exec. The closure only touches the precomputed worker socket
        // fd with async-signal-safe `fcntl`.
        unsafe {
            command.pre_exec(move || clear_fd_cloexec(&worker_fd));
        }
        // Workers run in the `filesystem/` leaf, apart from the supervisor's
        // control traffic and from workloads.
        if let Some(cgroups) = crate::cgroup::GuestCgroups::for_process() {
            cgroups.place_filesystem_worker(&mut command);
        }
        // Complete fallible socket adoption before a child exists. From spawn
        // to supervisor handoff there must be no error/cancellation gap.
        let router_stream = std::os::unix::net::UnixStream::from(router_fd);
        router_stream
            .set_nonblocking(true)
            .map_err(worker_io_status)?;
        let router_stream =
            tokio::net::UnixStream::from_std(router_stream).map_err(worker_io_status)?;
        // The daemon creates its own descriptors CLOEXEC (std and tokio do,
        // and process starts open PTYs CLOEXEC), so this spawn needs no lock
        // against concurrent forks. `spawn` blocks until the worker execs, so
        // it runs off the runtime threads. A caller cancelled meanwhile drops
        // the spawned child, which `kill_on_drop` terminates.
        let child = tokio::task::spawn_blocking(move || {
            let child = command.spawn();
            // Closes this process's copy of the worker's socket.
            drop(command);
            child
        })
        .await
        .map_err(|error| Status::internal(format!("filesystem worker spawn task failed: {error}")))?
        .map_err(worker_io_status)?;

        // Taken while the child is ours and unreaped, so its pid is its own.
        let pidfd = child
            .id()
            .and_then(|pid| rustix::process::Pid::from_raw(pid as i32))
            .and_then(|pid| {
                rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty()).ok()
            })
            .map(Arc::new);
        let stop_token = self.shutdown.child_token();
        let supervisor = Arc::new(Mutex::new(Some(supervise_worker(
            child,
            stop_token.clone(),
            user.clone(),
            process_slot,
        ))));
        let channel = match tokio::time::timeout(
            IDENTITY_WORKER_CONNECT_TIMEOUT,
            connect_worker_channel(router_stream),
        )
        .await
        {
            Ok(Ok(channel)) => channel,
            Ok(Err(status)) => {
                stop_token.cancel();
                if let Some(supervisor) = supervisor.lock().await.take()
                    && let Err(error) = supervisor.await
                {
                    tracing::debug!(
                        %user,
                        %error,
                        "filesystem identity worker supervisor failed during connection setup"
                    );
                }
                return Err(status);
            }
            Err(_) => {
                stop_token.cancel();
                if let Some(supervisor) = supervisor.lock().await.take()
                    && let Err(error) = supervisor.await
                {
                    tracing::debug!(
                        %user,
                        %error,
                        "filesystem identity worker supervisor failed after startup timeout"
                    );
                }
                return Err(Status::deadline_exceeded(
                    "filesystem identity worker did not become ready before its startup deadline",
                ));
            }
        };

        let mut ready_client = FilesystemClient::new(channel.clone());
        let ready = tokio::time::timeout(
            IDENTITY_WORKER_CONNECT_TIMEOUT,
            ready_client.ready(pplx_space_spaced_common::proto::worker::ReadyRequest {}),
        )
        .await;
        if !matches!(ready, Ok(Ok(ref response)) if response.get_ref().protocol_version == WORKER_PROTOCOL_VERSION)
        {
            stop_token.cancel();
            return Err(Status::unavailable(
                "incompatible filesystem worker protocol",
            ));
        }
        Ok(WorkerClient {
            client: FilesystemClient::new(channel),
            user: user.clone(),
            instance_id: uuid::Uuid::now_v7(),
            activity: Arc::new(()),
            ledger_update_slot: Arc::new(tokio::sync::Semaphore::new(1)),
            stop_token,
            supervisor,
            pidfd,
        })
    }
}

fn worker_command(executable: &Path, user: &GuestUserName, worker_raw_fd: RawFd) -> ChildCommand {
    worker_command_from_environment(
        executable,
        user,
        worker_raw_fd,
        &pplx_space_common_core::env::ProcessEnvironment,
    )
}
fn worker_command_from_environment(
    executable: &Path,
    user: &GuestUserName,
    worker_raw_fd: RawFd,
    env: &dyn pplx_space_common_core::env::Environment,
) -> ChildCommand {
    let mut command = ChildCommand::new(executable);
    command
        .env_clear()
        .env(WORKER_MODE_ENV, "1")
        .env(WORKER_USER_ENV, user.as_str())
        .env(WORKER_FD_ENV, worker_raw_fd.to_string())
        .kill_on_drop(true);
    for name in WORKER_ENV_ALLOWLIST {
        if let Some(value) = env.var_os(name) {
            command.env(name, value);
        }
    }
    // The allowlist forwards the launcher's OTel service name, which outranks
    // the worker's own default in the shared resolver. Pin the worker's name
    // only when that would actually rename the child: a Datadog-only parent
    // forwards no OTel identity, and adding the setting there would give the
    // worker a service tag it does not have today.
    if inherits_parent_service_name(env) {
        command.env(SPACE_SERVICE_NAME_ENV, crate::WORKER_SERVICE_NAME);
    }
    command
}

impl WorkerClient {
    /// A copy of `fd`, a descriptor open in this worker, or `None` where the
    /// worker cannot be reached that way (no pidfd, or the kernel refuses).
    pub(crate) fn duplicate_descriptor(&self, fd: i32) -> Option<std::os::fd::OwnedFd> {
        let pidfd = self.pidfd.as_ref()?;
        match rustix::process::pidfd_getfd(
            pidfd.as_ref(),
            fd,
            rustix::process::PidfdGetfdFlags::empty(),
        ) {
            Ok(fd) => Some(fd),
            Err(error) => {
                tracing::debug!(%error, worker = %self.instance_id, "cannot duplicate a worker descriptor");
                None
            }
        }
    }

    pub(crate) async fn stop(&self) {
        self.stop_token.cancel();
        let mut supervisor = self.supervisor.lock().await;
        // Await by reference: cancellation of a joiner must leave the handle
        // available to the next joiner, not detach it and imply completion.
        if let Some(task) = supervisor.as_mut() {
            if let Err(error) = task.await {
                tracing::debug!(
                    worker = %self.instance_id,
                    %error,
                    "filesystem identity worker supervisor failed while stopping"
                );
            }
            *supervisor = None;
        }
    }
}

fn supervise_worker(
    mut child: Child,
    stop_token: CancellationToken,
    user: GuestUserName,
    process_slot: tokio::sync::OwnedSemaphorePermit,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _process_slot = process_slot;
        tokio::select! {
            result = child.wait() => {
                if let Err(error) = result {
                    tracing::debug!(%user, %error, "failed to reap filesystem identity worker");
                }
            }
            _ = stop_token.cancelled() => {
                if let Err(error) = child.kill().await {
                    tracing::debug!(%user, %error, "failed to terminate filesystem identity worker");
                    if let Err(wait_error) = child.wait().await {
                        tracing::debug!(%user, %wait_error, "failed to reap terminated filesystem identity worker");
                    }
                }
            }
        }
    })
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum FilesystemWorkerError {
    #[error("filesystem worker user is missing: {0}")]
    MissingUser(#[source] std::env::VarError),
    #[error("filesystem worker user is invalid: {0}")]
    InvalidUser(#[source] pplx_space_common_core::types::ProcessParseError),
    #[error("filesystem worker fd is missing: {0}")]
    MissingFd(#[source] std::env::VarError),
    #[error("filesystem worker fd is invalid: {0}")]
    InvalidFd(#[source] std::num::ParseIntError),
    #[error("failed to configure filesystem worker stream: {0}")]
    ConfigureStream(#[source] std::io::Error),
    #[error("failed to resolve filesystem worker account: {0}")]
    ResolveAccount(#[source] crate::identity::ResolveGuestAccountError),
    #[error("failed to assume filesystem worker identity: {0}")]
    AssumeIdentity(#[source] std::io::Error),
    #[error("failed to create filesystem worker runtime: {0}")]
    CreateRuntime(#[source] std::io::Error),
    #[cfg(feature = "service-entrypoint")]
    #[error("failed to initialize filesystem worker telemetry: {0}")]
    Telemetry(#[source] pplx_space_common_svc::ObservabilityError),
    #[error("failed to adopt filesystem worker stream: {0}")]
    AdoptStream(#[source] std::io::Error),
    #[error("filesystem worker gRPC server failed: {0}")]
    Serve(#[source] tonic::transport::Error),
}

pub(crate) fn run_from_environment() -> Result<bool, FilesystemWorkerError> {
    if std::env::var_os(WORKER_MODE_ENV).is_none() {
        return Ok(false);
    }
    let user = std::env::var(WORKER_USER_ENV).map_err(FilesystemWorkerError::MissingUser)?;
    let user = GuestUserName::parse(user).map_err(FilesystemWorkerError::InvalidUser)?;
    let raw_fd = std::env::var(WORKER_FD_ENV)
        .map_err(FilesystemWorkerError::MissingFd)?
        .parse::<RawFd>()
        .map_err(FilesystemWorkerError::InvalidFd)?;
    run_worker(user, raw_fd)?;
    Ok(true)
}

fn run_worker(user: GuestUserName, raw_fd: RawFd) -> Result<(), FilesystemWorkerError> {
    // SAFETY: `raw_fd` is inherited exclusively from the root router's
    // socketpair and ownership is transferred into this UnixStream exactly
    // once at worker startup.
    let stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(raw_fd) };
    stream
        .set_nonblocking(true)
        .map_err(FilesystemWorkerError::ConfigureStream)?;
    let account = GuestAccount::resolve(user).map_err(FilesystemWorkerError::ResolveAccount)?;
    account
        .assume_process_credentials()
        .map_err(FilesystemWorkerError::AssumeIdentity)?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(IDENTITY_WORKER_MAX_BLOCKING_THREADS)
        .build()
        .map_err(FilesystemWorkerError::CreateRuntime)?;
    runtime.block_on(async move {
        // The credential switch must precede all exporter threads, and OTLP
        // requires an entered runtime. Workers do not own a systemd watchdog.
        #[cfg(feature = "service-entrypoint")]
        let telemetry = pplx_space_common_svc::init_service_subprocess(crate::WORKER_SERVICE_NAME)
            .map_err(FilesystemWorkerError::Telemetry)?;
        let result = serve_worker_stream(stream, account).await;
        // Keep this current-thread runtime driving network I/O while the
        // shared guard performs its bounded metrics and trace shutdown.
        // Supervisor SIGKILL bypasses destructors and can lose pending data.
        #[cfg(feature = "service-entrypoint")]
        pplx_space_common_svc::shutdown_telemetry(telemetry).await;
        result
    })
}

async fn serve_worker_stream(
    stream: std::os::unix::net::UnixStream,
    account: GuestAccount,
) -> Result<(), FilesystemWorkerError> {
    let stream =
        tokio::net::UnixStream::from_std(stream).map_err(FilesystemWorkerError::AdoptStream)?;
    let incoming = tokio_stream::once(Ok::<_, io::Error>(stream)).chain(tokio_stream::pending::<
        Result<tokio::net::UnixStream, io::Error>,
    >());
    let mut service =
        FilesystemWorkerService::new(account.name().clone(), CancellationToken::new());
    service.account = Some(Arc::new(account));
    pplx_space_common_grpc::grpc_server_builder()
        .concurrency_limit_per_connection(IDENTITY_WORKER_MAX_CONCURRENT_REQUESTS)
        .add_service(
            // The parent supervisor owns this subprocess's lifetime.
            FilesystemServer::new(service).max_decoding_message_size(WORKER_MAX_MESSAGE_BYTES),
        )
        .serve_with_incoming(incoming)
        .await
        .map_err(FilesystemWorkerError::Serve)?;
    Ok(())
}

async fn wait_for_start_with_timeout(
    mut result: watch::Receiver<Option<Result<WorkerClient, Status>>>,
    timeout: Duration,
) -> Result<WorkerClient, Status> {
    tokio::time::timeout(timeout, async {
        loop {
            if let Some(result) = result.borrow().clone() {
                return result;
            }
            result.changed().await.map_err(|_| {
                Status::unavailable("filesystem identity worker startup was interrupted")
            })?;
        }
    })
    .await
    .map_err(|_| {
        Status::deadline_exceeded(
            "filesystem identity worker did not become ready before its startup deadline",
        )
    })?
}

async fn connect_worker_channel(stream: tokio::net::UnixStream) -> Result<Channel, Status> {
    let stream = Arc::new(Mutex::new(Some(stream)));
    let endpoint = tonic::transport::Endpoint::try_from("http://[::]:50051").map_err(|error| {
        Status::unavailable(format!("failed to create worker endpoint: {error}"))
    })?;
    endpoint
        .connect_with_connector(tower::service_fn(move |_| {
            let stream = Arc::clone(&stream);
            async move {
                let stream = stream.lock().await.take().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "worker stream already connected")
                })?;
                Ok::<_, io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .map_err(|error| Status::unavailable(format!("failed to connect worker channel: {error}")))
}

fn worker_io_status(error: std::io::Error) -> Status {
    Status::unavailable(format!(
        "failed to start filesystem identity worker: {error}"
    ))
}

#[cfg(test)]
mod tests;
