#[cfg(target_os = "linux")]
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::task::{Context, Poll};
#[cfg(target_os = "linux")]
use std::time::{Duration, Instant};

use futures_util::{Stream, StreamExt};
use pplx_space_common_core::types::GuestUserName;
use pplx_space_spaced_common::invalid_argument_status;
use pplx_space_spaced_common::proto::bridge_guest::filesystem_server::Filesystem;
use pplx_space_spaced_common::proto::bridge_guest::{
    CreateWatcherRequest, CreateWatcherResponse, DisableFileChangesRequest,
    DisableFileChangesResponse, EnableFileChangesRequest, EnableFileChangesResponse,
    GetModifiedFilesRequest, GetModifiedFilesResponse, GetWatcherEventsRequest,
    GetWatcherEventsResponse, ListRequest, MkdirRequest, MkdirResponse, NormalizeWatchRootRequest,
    NormalizeWatchRootResponse, ReadRequest, RemoveRequest, RemoveResponse, RemoveWatcherRequest,
    RemoveWatcherResponse, RenameRequest, RenameResponse, StatRequest, StatResponse, WatchRequest,
    WriteBatchRequest, WriteBatchResponse, WriteRequest, WriteResponse,
};
#[cfg(target_os = "linux")]
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::{FilesystemExecutionMode, GuestFileChunkStream, GuestFileInfoStream};

#[cfg(target_os = "linux")]
use pplx_space_spaced_common::proto::worker::worker_filesystem_client::WorkerFilesystemClient as FilesystemClient;

use super::operations::{self, Kind, Lookup, Operations};
use super::{FilesystemWorkerService, watch};
use crate::idempotency::{optional_idempotency_key, require_idempotency_key};
use pplx_space_spaced_common::proto::worker::worker_filesystem_server::WorkerFilesystem;
use pplx_space_spaced_common::proto::worker::{self, mutation_result, write_frame};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

const MAX_GUEST_FILESYSTEM_EXECUTIONS: usize = 128;
/// How long a cancelled dispatch may keep finishing before the router stops
/// waiting, reaps its worker and publishes an unknown outcome. Cancelling an
/// RPC cannot cancel a kernel operation the worker is already blocked in, so
/// this is a real deadline rather than a poll interval.
pub(crate) const DEFAULT_CANCELLATION_GRACE: std::time::Duration =
    std::time::Duration::from_secs(5);
const SHUTDOWN_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

use pplx_space_common_grpc::{PreserveInputErrors, RequestAdmission, RequestPermit};
use pplx_space_spaced_common::filesystem_limits;
use pplx_space_spaced_common::proto::bridge_guest::filesystem_server::FilesystemServer as PublicFilesystemServer;

#[derive(Clone)]
enum Backend {
    Direct(FilesystemWorkerService),
    #[cfg(target_os = "linux")]
    Workers(super::worker::WorkerPool),
}

enum Command {
    Write {
        input: tokio::sync::mpsc::Receiver<worker::WriteFrame>,
        direct_owner: Option<GuestUserName>,
    },
    Batch(worker::BatchCommand),
    Rename(worker::RenameCommand),
}

enum Executor {
    Direct(FilesystemWorkerService),
    #[cfg(target_os = "linux")]
    Worker(super::worker::WorkerClient),
}

impl Executor {
    async fn execute(&self, command: Command) -> Result<worker::MutationResult, Status> {
        match self {
            Self::Direct(service) => match command {
                Command::Write {
                    input,
                    direct_owner,
                } => {
                    service
                        .execute_write(
                            tokio_stream::wrappers::ReceiverStream::new(input).map(Ok),
                            direct_owner,
                        )
                        .await
                }
                Command::Batch(request) => service.write_batch(Request::new(request)).await,
                Command::Rename(request) => service.rename(Request::new(request)).await,
            },
            #[cfg(target_os = "linux")]
            Self::Worker(worker) => {
                let mut client = worker.client.clone();
                match command {
                    Command::Write { input, .. } => {
                        client
                            .write(tokio_stream::wrappers::ReceiverStream::new(input))
                            .await
                    }
                    Command::Batch(request) => client.write_batch(request).await,
                    Command::Rename(request) => client.rename(request).await,
                }
            }
        }
        .map(Response::into_inner)
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone)]
struct LiveWatcherRoute {
    slot: Arc<tokio::sync::OwnedSemaphorePermit>,
    worker: super::worker::WorkerClient,
    worker_watcher_id: String,
    last_access: Instant,
}

#[cfg(target_os = "linux")]
#[derive(Clone)]
enum WatcherRoute {
    Live(Box<LiveWatcherRoute>),
    Reset {
        expires_at: Instant,
        _slot: Arc<tokio::sync::OwnedSemaphorePermit>,
    },
}

#[cfg(target_os = "linux")]
const WATCHER_ROUTE_TTL: Duration = Duration::from_secs(10 * 60);
#[cfg(target_os = "linux")]
const MAX_WATCHER_ROUTES: usize = 2048;
#[cfg(target_os = "linux")]
const MAX_LEDGER_IDENTITIES: usize = 4096;

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, PartialEq, Eq)]
enum LedgerEvidence {
    Tracking,
    Lost,
}

#[derive(Clone)]
pub(crate) struct FilesystemRouter {
    default_user: GuestUserName,
    operations: Operations,
    tasks: TaskTracker,
    shutdown: CancellationToken,
    execution_slots: Arc<tokio::sync::Semaphore>,
    payload_slots: Arc<tokio::sync::Semaphore>,
    backend: Backend,
    #[cfg(target_os = "linux")]
    watcher_routes: Arc<Mutex<HashMap<String, WatcherRoute>>>,
    #[cfg(target_os = "linux")]
    watcher_slots: Arc<tokio::sync::Semaphore>,
    #[cfg(target_os = "linux")]
    ledger_evidence: Arc<Mutex<HashMap<GuestUserName, LedgerEvidence>>>,
    cancellation_grace: std::time::Duration,
}

impl FilesystemRouter {
    pub(crate) fn grpc_service(
        &self,
    ) -> RequestAdmission<PreserveInputErrors<PublicFilesystemServer<Self>>> {
        RequestAdmission::new(
            PreserveInputErrors::new(
                PublicFilesystemServer::new(self.clone())
                    .max_decoding_message_size(filesystem_limits::MAX_MESSAGE_BYTES),
            ),
            pplx_space_common_grpc::QueuedSlots::new(self.payload_slots.clone()),
            filesystem_limits::PAYLOAD_METHODS,
        )
    }

    pub(crate) async fn drain(&self) {
        self.shutdown.cancel();
        self.tasks.close();
        let drain = async {
            match &self.backend {
                Backend::Direct(service) => {
                    // A blocked Direct mutation must not delay watch cancellation.
                    tokio::join!(service.watch_manager.shutdown(), async {
                        self.tasks.wait().await;
                        service.tasks.close();
                        service.tasks.wait().await;
                    });
                }
                #[cfg(target_os = "linux")]
                Backend::Workers(pool) => {
                    self.tasks.wait().await;
                    pool.drain().await;
                }
            }
        };
        if tokio::time::timeout(SHUTDOWN_BUDGET, drain).await.is_err() {
            tracing::error!(
                "filesystem shutdown budget exceeded; execution outcome remains unknown"
            );
        }
    }

    async fn executor(&self, user: GuestUserName) -> Result<Executor, Status> {
        if self.shutdown.is_cancelled() {
            return Err(crate::refused(Status::unavailable("guest shutting down")));
        }
        match &self.backend {
            Backend::Direct(service) => {
                let mut service = service.clone();
                service.default_user = user;
                Ok(Executor::Direct(service))
            }
            #[cfg(target_os = "linux")]
            Backend::Workers(_) => Ok(Executor::Worker(self.acquire_worker(user).await?)),
        }
    }

    fn supervise(
        &self,
        executor: Executor,
        reservation: operations::Reservation,
        command: Command,
        cancelled: CancellationToken,
        permits: (tokio::sync::OwnedSemaphorePermit, Option<RequestPermit>),
    ) -> tokio::task::JoinHandle<Result<worker::MutationResult, Status>> {
        #[cfg(target_os = "linux")]
        let router = self.clone();
        let cancellation_grace = self.cancellation_grace;
        self.tasks.spawn(async move {
            // Input admission survives caller cancellation through all retained
            // payloads, outstanding file I/O, and worker termination/reaping.
            let _permits = permits;
            if cancelled.is_cancelled() {
                return Err(Status::cancelled("filesystem dispatch cancelled before execution"));
            }
            let execution = reservation.dispatch();
            let result = {
                let call = executor.execute(command);
                tokio::pin!(call);
                tokio::select! {
                    result = &mut call => result,
                    _ = cancelled.cancelled() => {
                        match tokio::time::timeout(cancellation_grace, &mut call).await {
                            Ok(result) => result,
                            Err(_) => {
                                tracing::warn!("filesystem cancellation cleanup stalled");
                                match &executor {
                                    Executor::Direct(_) => call.await,
                                    #[cfg(target_os = "linux")]
                                    Executor::Worker(worker) => {
                                        router.record_worker_failure(worker).await;
                                        Err(operations::unknown_outcome())
                                    }
                                }
                            }
                        }
                    }
                }
            };
            let result = match result {
                Ok(result) => Ok(result),
                Err(status) => {
                    // Operation errors and pre-execution refusals arrive as
                    // typed replies. A bare status does not prove the worker
                    // stopped effects, even if its code is not a transport code.
                    tracing::warn!(code = ?status.code(), "filesystem dispatch ended without a terminal reply");
                    #[cfg(target_os = "linux")]
                    if let Executor::Worker(worker) = &executor {
                        router.record_worker_failure(worker).await;
                    }
                    Err(operations::unknown_outcome())
                }
            };
            execution.complete(result.clone());
            result
        })
    }

    fn execution_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
        self.execution_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                crate::refused(Status::resource_exhausted(
                    "too many guest filesystem executions",
                ))
            })
    }

    async fn mutate(
        &self,
        user: GuestUserName,
        key: Option<pplx_space_common_core::types::IdempotencyKey>,
        kind: Kind,
        command: Command,
        admission: Option<RequestPermit>,
    ) -> Result<worker::MutationResult, Status> {
        let reservation = match self.operations.reserve(user.clone(), key, kind).await? {
            Lookup::Replay(result) => return result,
            Lookup::New(reservation) => reservation,
        };
        let permit = self.execution_permit()?;
        let executor = self.executor(user).await?;
        let cancelled = self.shutdown.child_token();
        let _on_drop = cancelled.clone().drop_guard();
        self.supervise(
            executor,
            reservation,
            command,
            cancelled,
            (permit, admission),
        )
        .await
        .map_err(|_| operations::unknown_outcome())?
    }

    async fn write_stream(
        &self,
        first: WriteRequest,
        mut incoming: Pin<Box<dyn Stream<Item = Result<WriteRequest, Status>> + Send>>,
        admission: Option<RequestPermit>,
    ) -> Result<Response<WriteResponse>, Status> {
        use pplx_space_spaced_common::proto::operations::write_request::Message;
        let Some(Message::Open(open)) = first.message else {
            return Err(Status::invalid_argument("first write message must be open"));
        };
        let direct_owner = open
            .user
            .clone()
            .map(GuestUserName::parse)
            .transpose()
            .map_err(invalid_argument_status)?;
        let user = self.effective_user(open.user)?;
        let key = if open.append {
            Some(require_idempotency_key(
                open.idempotency_key,
                "append write",
            )?)
        } else {
            optional_idempotency_key(open.idempotency_key)?
        };
        let reservation = match self
            .operations
            .reserve(user.clone(), key, Kind::Write)
            .await?
        {
            Lookup::Replay(result) => return Self::write_response(result?),
            Lookup::New(reservation) => reservation,
        };
        let permit = self.execution_permit()?;
        let executor = self.executor(user).await?;
        let cancelled = self.shutdown.child_token();
        let _on_drop = cancelled.clone().drop_guard();
        let (tx, rx) = tokio::sync::mpsc::channel(filesystem_limits::WRITE_BUFFER_FRAMES);
        tx.try_send(worker::WriteFrame {
            frame: Some(write_frame::Frame::Open(worker::WriteStart {
                execution_id: uuid::Uuid::now_v7().to_string(),
                path: open.path,
                append: open.append,
            })),
        })
        .map_err(|_| Status::internal("new worker stream unavailable"))?;
        let mut execution = self.supervise(
            executor,
            reservation,
            Command::Write {
                input: rx,
                direct_owner,
            },
            cancelled.clone(),
            (permit, admission),
        );
        // No detached pump: the outer future directly owns the input stream.
        enum PumpExit {
            Finished,
            ExecutorClosed,
        }
        let pump = async move {
            while let Some(message) = incoming.next().await {
                let message = message?;
                let Some(Message::Data(data)) = message.message else {
                    return Err(Status::invalid_argument("write expects data"));
                };
                // Bound queued bytes as well as frame count, without rejecting
                // clients that send larger (otherwise valid) protobuf messages.
                // spaced-host already sends frames of at most this size, so
                // those move through as they are. A larger one is split one
                // chunk at a time, so at most one copy waits on the channel.
                let send = |frame: Vec<u8>| {
                    tx.send(worker::WriteFrame {
                        frame: Some(write_frame::Frame::Data(frame)),
                    })
                };
                if data.len() <= filesystem_limits::WRITE_CHUNK_BYTES {
                    if send(data).await.is_err() {
                        return Ok(PumpExit::ExecutorClosed);
                    }
                } else {
                    for chunk in data.chunks(filesystem_limits::WRITE_CHUNK_BYTES) {
                        if send(chunk.to_vec()).await.is_err() {
                            return Ok(PumpExit::ExecutorClosed);
                        }
                    }
                }
            }
            match tx
                .send(worker::WriteFrame {
                    frame: Some(write_frame::Frame::Finish(worker::Finish {})),
                })
                .await
            {
                Ok(()) => Ok(PumpExit::Finished),
                Err(_) => Ok(PumpExit::ExecutorClosed),
            }
        };
        tokio::pin!(pump);
        tokio::select! {
            result = &mut execution => Self::write_response(result.map_err(|_| operations::unknown_outcome())??),
            result = &mut pump => {
                match result {
                    Err(error) => {
                        cancelled.cancel();
                        return Err(error);
                    }
                    Ok(PumpExit::ExecutorClosed) => {
                        // A closed queue can mean an early known failure, not
                        // lost execution. Preserve the authoritative result;
                        // retain the cancellation grace if it never arrives.
                        cancelled.cancel();
                    }
                    Ok(PumpExit::Finished) => {}
                }
                Self::write_response(execution.await.map_err(|_| operations::unknown_outcome())??)
            }
            _ = self.shutdown.cancelled() => Err(Status::unavailable("guest shutting down")),
        }
    }

    fn write_response(result: worker::MutationResult) -> Result<Response<WriteResponse>, Status> {
        match operations::checked_outcome(result)? {
            mutation_result::Outcome::Write(response) => Ok(Response::new(response)),
            _ => Err(operations::unknown_outcome()),
        }
    }

    pub(crate) fn new(
        execution: FilesystemExecutionMode,
        default_user: GuestUserName,
        shutdown: tokio_util::sync::CancellationToken,
        cancellation_grace: std::time::Duration,
    ) -> Self {
        let backend = match execution {
            FilesystemExecutionMode::Direct => Backend::Direct(FilesystemWorkerService::new(
                default_user.clone(),
                shutdown.child_token(),
            )),
            #[cfg(target_os = "linux")]
            FilesystemExecutionMode::IdentityWorkers => {
                Backend::Workers(super::worker::WorkerPool::new(shutdown.clone()))
            }
            #[cfg(not(target_os = "linux"))]
            FilesystemExecutionMode::IdentityWorkers => Backend::Direct(
                FilesystemWorkerService::new(default_user.clone(), shutdown.child_token()),
            ),
        };
        Self {
            default_user,
            operations: Operations::new(),
            tasks: TaskTracker::new(),
            shutdown,
            execution_slots: Arc::new(tokio::sync::Semaphore::new(MAX_GUEST_FILESYSTEM_EXECUTIONS)),
            payload_slots: Arc::new(tokio::sync::Semaphore::new(
                filesystem_limits::PAYLOAD_SLOTS,
            )),
            backend,
            #[cfg(target_os = "linux")]
            watcher_routes: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(target_os = "linux")]
            watcher_slots: Arc::new(tokio::sync::Semaphore::new(MAX_WATCHER_ROUTES)),
            #[cfg(target_os = "linux")]
            ledger_evidence: Arc::new(Mutex::new(HashMap::new())),
            cancellation_grace,
        }
    }

    fn effective_user(&self, requested_user: Option<String>) -> Result<GuestUserName, Status> {
        crate::guest_path::effective_user(requested_user, &self.default_user)
    }

    #[cfg(target_os = "linux")]
    fn worker_pool(&self) -> Result<&super::worker::WorkerPool, Status> {
        let Backend::Workers(pool) = &self.backend else {
            return Err(Status::internal("filesystem worker backend is not enabled"));
        };
        Ok(pool)
    }

    #[cfg(target_os = "linux")]
    async fn worker(
        &self,
        requested_user: Option<String>,
    ) -> Result<super::worker::WorkerClient, Status> {
        self.acquire_worker(self.effective_user(requested_user)?)
            .await
    }

    #[cfg(target_os = "linux")]
    async fn acquire_worker(
        &self,
        user: GuestUserName,
    ) -> Result<super::worker::WorkerClient, Status> {
        self.prune_watcher_routes().await;
        let pool = self.worker_pool()?;
        match pool.client_for(user.clone()).await {
            Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                let deadline = tokio::time::Instant::now() + self.cancellation_grace;
                for worker in pool.idle_ledgers().await {
                    let Ok(permit) = worker.ledger_update_slot.clone().try_acquire_owned() else {
                        continue;
                    };
                    let router = self.clone();
                    // A cancelled acquisition cannot strand loss publication or
                    // queue another probe for this generation while it settles.
                    let probe = self.tasks.spawn(async move {
                        let _permit = permit;
                        let mut client = worker.client.clone();
                        match tokio::time::timeout_at(
                            deadline,
                            client.get_modified_files(GetModifiedFilesRequest::default()),
                        )
                        .await
                        {
                            Ok(result) => {
                                if let Ok(response) =
                                    router.observe_worker_result(&worker, result).await
                                {
                                    router
                                        .observe_ledger_state(&worker, response.get_ref())
                                        .await;
                                }
                            }
                            Err(_) => router.record_worker_failure(&worker).await,
                        }
                    });
                    if tokio::time::timeout_at(deadline, probe).await.is_err() {
                        break;
                    }
                }
                pool.client_for(user).await
            }
            result => result,
        }
    }

    #[cfg(target_os = "linux")]
    async fn stream<R, T: Send + 'static>(
        &self,
        user: Option<String>,
        request: R,
        call: impl AsyncFnOnce(
            &mut FilesystemClient<tonic::transport::Channel>,
            R,
        ) -> Result<Response<tonic::Streaming<T>>, Status>,
    ) -> Result<Response<RoutedStream<T>>, Status> {
        let worker = self.worker(user).await?;
        let response = self
            .observe_worker_result(&worker, call(&mut worker.client.clone(), request).await)
            .await?;
        Ok(Response::new(Box::pin(FailureAwareStream {
            inner: Box::pin(response.into_inner()),
            router: self.clone(),
            worker,
            runtime_handle: tokio::runtime::Handle::current(),
            failure_recorded: false,
        })))
    }

    /// Read through a descriptor the user's worker opened: the worker checks
    /// access as the user, and this process streams the bytes itself instead
    /// of relaying every chunk from the worker. `None` falls back to the
    /// relayed read: a worker without the call, or a descriptor this process
    /// cannot duplicate.
    #[cfg(target_os = "linux")]
    async fn read_through_descriptor(
        &self,
        user: Option<String>,
        request: ReadRequest,
    ) -> Result<Option<GuestFileChunkStream>, Status> {
        let worker = self.worker(user).await?;
        let length = request.length;
        let response = match worker.client.clone().open_read(request).await {
            Err(status) if status.code() == tonic::Code::Unimplemented => return Ok(None),
            result => self.observe_worker_result(&worker, result).await?,
        };
        let mut lease = response.into_inner();
        let Some(opened) = self
            .observe_worker_result(&worker, lease.message().await)
            .await?
        else {
            return Err(Status::internal(
                "filesystem worker ended an open without a descriptor",
            ));
        };
        let Some(fd) = worker.duplicate_descriptor(opened.fd) else {
            return Ok(None);
        };
        // The worker keeps its copy and its admission until this read ends,
        // and the read ends with the worker: its admission is per worker, so
        // a read outliving it would escape the replacement worker's limit.
        // The worker sends nothing after the descriptor; its stream ends
        // only when the worker does. A transport failure retires the worker,
        // as it does on a relayed stream, without holding up the read's end.
        let router = self.clone();
        let leased = worker.clone();
        let worker_ended = async move {
            let ended = loop {
                match lease.message().await {
                    Ok(Some(_)) => {}
                    Ok(None) => break None,
                    Err(status) => break Some(status),
                }
            };
            if ended.as_ref().is_some_and(is_worker_transport_failure) {
                tokio::spawn(async move {
                    router.record_worker_failure(&leased).await;
                });
            }
            Status::unavailable("filesystem worker ended during the read")
        };
        Ok(Some(super::read::stream_file(
            std::fs::File::from(fd),
            length,
            worker,
            worker_ended,
        )))
    }

    #[cfg(target_os = "linux")]
    async fn observe_worker_result<T>(
        &self,
        worker: &super::worker::WorkerClient,
        result: Result<T, Status>,
    ) -> Result<T, Status> {
        if result.as_ref().is_err_and(is_worker_transport_failure) {
            self.record_worker_failure(worker).await;
        }
        result
    }

    #[cfg(target_os = "linux")]
    async fn record_worker_failure(&self, worker: &super::worker::WorkerClient) {
        let router = self.clone();
        let worker = worker.clone();
        // Removal, reaping, and loss publication are one owned transition.
        // A cancelled RPC observer must not erase an acknowledged ledger.
        if let Err(error) = self
            .tasks
            .spawn(async move {
                // Preserve evidence→pool lock order: no replacement may observe
                // a clean disabled ledger between removal and loss publication.
                let mut evidence = router.ledger_evidence.lock().await;
                match router
                    .worker_pool()
                    .expect("identity worker backend")
                    .remove(&worker)
                    .await
                {
                    Some(true) => {
                        if let Some(state) = evidence.get_mut(&worker.user) {
                            *state = LedgerEvidence::Lost;
                        }
                    }
                    Some(false)
                        if evidence.get(&worker.user) == Some(&LedgerEvidence::Tracking) =>
                    {
                        evidence.remove(&worker.user);
                    }
                    _ => {}
                }
                drop(evidence);
                router.reset_watcher_routes(&worker).await;
                // Failure is visible before a slow reap; effect completion
                // still waits until the old generation can no longer mutate.
                worker.stop().await;
            })
            .await
        {
            tracing::error!(%error, "filesystem worker loss accounting failed");
        }
    }

    #[cfg(target_os = "linux")]
    async fn reset_watcher_routes(&self, worker: &super::worker::WorkerClient) {
        let reset_expires_at = Instant::now() + WATCHER_ROUTE_TTL;
        let mut routes = self.watcher_routes.lock().await;
        for route in routes.values_mut() {
            if let WatcherRoute::Live(live) = route
                && live.worker.instance_id == worker.instance_id
            {
                *route = WatcherRoute::Reset {
                    expires_at: reset_expires_at,
                    _slot: live.slot.clone(),
                };
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn lost_ledger_response() -> Response<GetModifiedFilesResponse> {
        Response::new(GetModifiedFilesResponse {
            files: Vec::new(),
            observed_since: None,
            retained_since: None,
            cursor: None,
            monitoring_enabled: false,
            overflowed: true,
        })
    }

    #[cfg(target_os = "linux")]
    async fn finish_enable_file_changes(
        &self,
        worker: &super::worker::WorkerClient,
        user: &GuestUserName,
    ) -> bool {
        // Loss publication and acknowledgement share this lock before the pool
        // lock. A late old-worker reply must not overwrite a replacement ledger.
        let mut lost = self.ledger_evidence.lock().await;
        let Some(evidence) = lost.get_mut(user) else {
            return false;
        };
        let pool = self.worker_pool().expect("identity worker backend");
        *evidence =
            if pool.set_ledger_enabled(worker, true).await || pool.ledger_enabled(user).await {
                LedgerEvidence::Tracking
            } else {
                LedgerEvidence::Lost
            };
        true
    }

    #[cfg(target_os = "linux")]
    async fn observe_ledger_state(
        &self,
        worker: &super::worker::WorkerClient,
        response: &GetModifiedFilesResponse,
    ) {
        if !response.monitoring_enabled {
            let mut evidence = self.ledger_evidence.lock().await;
            if self
                .worker_pool()
                .expect("identity worker backend")
                .set_ledger_enabled(worker, false)
                .await
            {
                if response.overflowed {
                    // Every enabled identity reserves its evidence slot before
                    // dispatch, so worker loss cannot grow this map past its cap.
                    if let Some(state) = evidence.get_mut(&worker.user) {
                        *state = LedgerEvidence::Lost;
                    }
                } else if evidence.get(&worker.user) == Some(&LedgerEvidence::Tracking) {
                    evidence.remove(&worker.user);
                }
            }
        }
    }

    #[cfg(target_os = "linux")]
    async fn update_ledger<T: Send + 'static>(
        &self,
        worker: super::worker::WorkerClient,
        enabled: bool,
        call: impl std::future::Future<Output = Result<Response<T>, Status>> + Send + 'static,
    ) -> Result<Response<T>, Status> {
        if self.shutdown.is_cancelled() {
            return Err(crate::refused(Status::unavailable("guest shutting down")));
        }
        let permit = worker
            .ledger_update_slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                crate::refused(Status::resource_exhausted(
                    "file-change update in progress; retry with backoff",
                ))
            })?;
        let new_identity = if enabled {
            let mut evidence = self.ledger_evidence.lock().await;
            if !self.worker_pool()?.contains(&worker).await {
                return Err(Status::unavailable(
                    "filesystem worker reset before ledger update",
                ));
            }
            if !evidence.contains_key(&worker.user) {
                if evidence.len() >= MAX_LEDGER_IDENTITIES {
                    return Err(crate::refused(Status::resource_exhausted(
                        "file-change identity capacity exhausted; reenable then disable an abandoned ledger",
                    )));
                }
                evidence.insert(worker.user.clone(), LedgerEvidence::Tracking);
                true
            } else {
                false
            }
        } else {
            false
        };
        let router = self.clone();
        // One service-owned update retains the worker and its admission through
        // both the RPC and accounting. Dropping the caller cannot reorder replies.
        let task = self.tasks.spawn(async move {
            let _permit = permit;
            let budget =
                pplx_space_common_core::operation_budget::RuntimeOperationBudget::FilesystemMetadata
                    .timeout();
            let result = tokio::select! {
                biased;
                _ = router.shutdown.cancelled() => Err(Status::unavailable("guest shutting down")),
                result = tokio::time::timeout(budget, call) => result.unwrap_or_else(|_| {
                    Err(Status::deadline_exceeded("file-change update timed out"))
                }),
            };
            if result.as_ref().is_err_and(|status| {
                is_worker_transport_failure(status)
                    || status.code() == tonic::Code::DeadlineExceeded
            }) {
                // A missing reply cannot establish the worker's ledger state.
                tracing::warn!(
                    worker = %worker.instance_id,
                    error = ?result.as_ref().err(),
                    "retiring filesystem worker after inconclusive file-change update"
                );
                router.record_worker_failure(&worker).await;
            } else if result.is_ok() {
                if enabled {
                    if !router
                        .finish_enable_file_changes(&worker, &worker.user)
                        .await
                    {
                        return Err(Status::unavailable(
                            "filesystem worker reset before ledger acknowledgement",
                        ));
                    }
                } else {
                    let mut evidence = router.ledger_evidence.lock().await;
                    if router
                        .worker_pool()?
                        .set_ledger_enabled(&worker, false)
                        .await
                        && evidence.get(&worker.user) == Some(&LedgerEvidence::Tracking)
                    {
                        evidence.remove(&worker.user);
                    }
                }
            }
            if result.is_err() && new_identity {
                let mut evidence = router.ledger_evidence.lock().await;
                if evidence.get(&worker.user) == Some(&LedgerEvidence::Tracking)
                    && router
                        .worker_pool()?
                        .set_ledger_enabled(&worker, false)
                        .await
                {
                    evidence.remove(&worker.user);
                }
            }
            result
        });
        task.await
            .map_err(|error| Status::internal(format!("file-change update task failed: {error}")))?
    }

    #[cfg(target_os = "linux")]
    async fn prune_watcher_routes(&self) {
        let now = Instant::now();
        // Workers expire their own consumers. Drop routing leases before *any*
        // worker acquisition so abandoned watches cannot prevent pool eviction.
        self.watcher_routes
            .lock()
            .await
            .retain(|_, route| match route {
                WatcherRoute::Live(route) => {
                    now.duration_since(route.last_access) <= WATCHER_ROUTE_TTL
                }
                WatcherRoute::Reset { expires_at, .. } => *expires_at > now,
            });
    }

    #[cfg(target_os = "linux")]
    async fn watcher_route(&self, watcher_id: &str) -> Result<LiveWatcherRoute, Status> {
        self.prune_watcher_routes().await;
        let mut routes = self.watcher_routes.lock().await;
        let route = routes
            .get_mut(watcher_id)
            .ok_or_else(|| Status::not_found("watcher not found"))?;
        match route {
            WatcherRoute::Live(route) => {
                route.last_access = Instant::now();
                Ok((**route).clone())
            }
            WatcherRoute::Reset { .. } => Err(Status::unavailable("watcher worker reset")),
        }
    }
}

#[cfg(target_os = "linux")]
fn is_worker_transport_failure(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Cancelled | tonic::Code::Unavailable | tonic::Code::Unknown
    )
}

#[cfg(target_os = "linux")]
type RoutedStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[cfg(target_os = "linux")]
struct FailureAwareStream<T> {
    inner: RoutedStream<T>,
    router: FilesystemRouter,
    worker: super::worker::WorkerClient,
    runtime_handle: tokio::runtime::Handle,
    failure_recorded: bool,
}

#[cfg(target_os = "linux")]
impl<T> Stream for FailureAwareStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let result = this.inner.as_mut().poll_next(cx);
        if let Poll::Ready(Some(Err(status))) = &result
            && is_worker_transport_failure(status)
            && !this.failure_recorded
        {
            this.failure_recorded = true;
            let router = this.router.clone();
            let worker = this.worker.clone();
            this.runtime_handle.spawn(async move {
                router.record_worker_failure(&worker).await;
            });
        }
        result
    }
}

#[tonic::async_trait]
impl Filesystem for FilesystemRouter {
    type ListInfoStream = GuestFileInfoStream;
    // SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_1_4: also requires MIN_HOST 0.1.4.
    type ListStream = Pin<
        Box<
            dyn Stream<
                    Item = Result<
                        pplx_space_spaced_common::proto::bridge_guest::LegacyDirEntry,
                        Status,
                    >,
                > + Send,
        >,
    >;
    async fn stat(
        &self,
        request: Request<StatRequest>,
    ) -> Result<Response<pplx_space_spaced_common::proto::bridge_guest::LegacyStatResponse>, Status>
    {
        super::compat::stat(self.stat_info(request).await?)
    }
    async fn list(
        &self,
        request: Request<ListRequest>,
    ) -> Result<Response<Self::ListStream>, Status> {
        Ok(Response::new(Box::pin(
            self.list_info(request)
                .await?
                .into_inner()
                .map(|item| item.and_then(super::compat::entry)),
        )))
    }
    type ReadStream = GuestFileChunkStream;
    type WatchStream = watch::WatchStream;

    async fn stat_info(
        &self,
        request: Request<StatRequest>,
    ) -> Result<Response<StatResponse>, Status> {
        match &self.backend {
            Backend::Direct(service) => service.stat(request).await,
            #[cfg(target_os = "linux")]
            Backend::Workers(_) => {
                let mut request = request.into_inner();
                let worker = self.worker(request.user.take()).await?;
                let mut client = worker.client.clone();
                self.observe_worker_result(&worker, client.stat(request).await)
                    .await
            }
        }
    }

    async fn list_info(
        &self,
        request: Request<ListRequest>,
    ) -> Result<Response<Self::ListInfoStream>, Status> {
        match &self.backend {
            Backend::Direct(service) => service.list(request).await,
            #[cfg(target_os = "linux")]
            Backend::Workers(_) => {
                let mut request = request.into_inner();
                self.stream(request.user.take(), request, FilesystemClient::list)
                    .await
            }
        }
    }

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        match &self.backend {
            Backend::Direct(service) => service.read(request).await,
            #[cfg(target_os = "linux")]
            Backend::Workers(_) => {
                let mut request = request.into_inner();
                let user = request.user.take();
                if let Some(stream) = self
                    .read_through_descriptor(user.clone(), request.clone())
                    .await?
                {
                    return Ok(Response::new(stream));
                }
                self.stream(user, request, FilesystemClient::read).await
            }
        }
    }

    async fn write(
        &self,
        mut request: Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let admission = request.extensions_mut().remove::<RequestPermit>();
        let mut incoming = request.into_inner();
        let first = incoming
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("write requires open"))?;
        self.write_stream(first, Box::pin(incoming), admission)
            .await
    }

    async fn write_batch(
        &self,
        mut request: Request<WriteBatchRequest>,
    ) -> Result<Response<WriteBatchResponse>, Status> {
        let admission = request.extensions_mut().remove::<RequestPermit>();
        let request = request.into_inner();
        if request.entries.len() > pplx_space_common_core::spaced::MAX_WRITE_BATCH_ENTRIES {
            return Err(Status::invalid_argument("too many write_batch entries"));
        }
        let user = self.effective_user(request.user)?;
        let key = if request.entries.iter().any(|entry| entry.append) {
            Some(require_idempotency_key(
                request.idempotency_key,
                "append write_batch",
            )?)
        } else {
            optional_idempotency_key(request.idempotency_key)?
        };
        let command = Command::Batch(worker::BatchCommand {
            execution_id: uuid::Uuid::now_v7().to_string(),
            entries: request.entries,
        });
        match operations::checked_outcome(
            self.mutate(user, key, Kind::Batch, command, admission)
                .await?,
        )? {
            mutation_result::Outcome::Batch(response) => Ok(Response::new(response)),
            _ => Err(operations::unknown_outcome()),
        }
    }

    async fn mkdir(
        &self,
        request: Request<MkdirRequest>,
    ) -> Result<Response<MkdirResponse>, Status> {
        match &self.backend {
            Backend::Direct(service) => service.mkdir(request).await,
            #[cfg(target_os = "linux")]
            Backend::Workers(_) => {
                let mut request = request.into_inner();
                let worker = self.worker(request.user.take()).await?;
                let mut client = worker.client.clone();
                self.observe_worker_result(&worker, client.mkdir(request).await)
                    .await
            }
        }
    }

    async fn remove(
        &self,
        request: Request<RemoveRequest>,
    ) -> Result<Response<RemoveResponse>, Status> {
        match &self.backend {
            Backend::Direct(service) => service.remove(request).await,
            #[cfg(target_os = "linux")]
            Backend::Workers(_) => {
                let mut request = request.into_inner();
                let worker = self.worker(request.user.take()).await?;
                let mut client = worker.client.clone();
                self.observe_worker_result(&worker, client.remove(request).await)
                    .await
            }
        }
    }

    async fn rename(
        &self,
        mut request: Request<RenameRequest>,
    ) -> Result<Response<RenameResponse>, Status> {
        let admission = request.extensions_mut().remove::<RequestPermit>();
        let request = request.into_inner();
        let user = self.effective_user(request.user)?;
        let key = Some(require_idempotency_key(request.idempotency_key, "rename")?);
        let command = Command::Rename(worker::RenameCommand {
            execution_id: uuid::Uuid::now_v7().to_string(),
            old_path: request.old_path,
            new_path: request.new_path,
        });
        match operations::checked_outcome(
            self.mutate(user, key, Kind::Rename, command, admission)
                .await?,
        )? {
            mutation_result::Outcome::Rename(response) => Ok(Response::new(response)),
            _ => Err(operations::unknown_outcome()),
        }
    }

    async fn normalize_watch_root(
        &self,
        request: Request<NormalizeWatchRootRequest>,
    ) -> Result<Response<NormalizeWatchRootResponse>, Status> {
        match &self.backend {
            Backend::Direct(service) => service.normalize_watch_root(request).await,
            #[cfg(target_os = "linux")]
            Backend::Workers(_) => {
                let mut request = request.into_inner();
                let worker = self.worker(request.user.take()).await?;
                let mut client = worker.client.clone();
                self.observe_worker_result(&worker, client.normalize_watch_root(request).await)
                    .await
            }
        }
    }

    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        match &self.backend {
            Backend::Direct(service) => service.watch(request).await,
            #[cfg(target_os = "linux")]
            Backend::Workers(_) => {
                let mut request = request.into_inner();
                self.stream(request.user.take(), request, FilesystemClient::watch)
                    .await
            }
        }
    }

    async fn create_watcher(
        &self,
        request: Request<CreateWatcherRequest>,
    ) -> Result<Response<CreateWatcherResponse>, Status> {
        match &self.backend {
            Backend::Direct(service) => service.create_watcher(request).await,
            #[cfg(target_os = "linux")]
            Backend::Workers(_) => {
                self.prune_watcher_routes().await;
                let slot = self.watcher_slots.clone().try_acquire_owned()
                    .map_err(|_| crate::refused(Status::resource_exhausted("watcher route capacity exhausted; remove a watcher or retry after expiry")))?;
                let mut request = request.into_inner();
                let worker = self.worker(request.user.take()).await?;
                let mut client = worker.client.clone();
                let mut response = self
                    .observe_worker_result(&worker, client.create_watcher(request).await)
                    .await?
                    .into_inner();
                let external_id = uuid::Uuid::now_v7().to_string();
                self.prune_watcher_routes().await;
                let mut routes = self.watcher_routes.lock().await;
                routes.insert(
                    external_id.clone(),
                    WatcherRoute::Live(Box::new(LiveWatcherRoute {
                        slot: Arc::new(slot),
                        worker,
                        worker_watcher_id: response.watcher_id,
                        last_access: Instant::now(),
                    })),
                );
                response.watcher_id = external_id;
                Ok(Response::new(response))
            }
        }
    }

    async fn get_watcher_events(
        &self,
        request: Request<GetWatcherEventsRequest>,
    ) -> Result<Response<GetWatcherEventsResponse>, Status> {
        match &self.backend {
            Backend::Direct(service) => service.get_watcher_events(request).await,
            #[cfg(target_os = "linux")]
            Backend::Workers(_) => {
                let request = request.into_inner();
                let route = self.watcher_route(&request.watcher_id).await?;
                let mut client = route.worker.client.clone();
                self.observe_worker_result(
                    &route.worker,
                    client
                        .get_watcher_events(GetWatcherEventsRequest {
                            watcher_id: route.worker_watcher_id,
                        })
                        .await,
                )
                .await
            }
        }
    }

    async fn remove_watcher(
        &self,
        request: Request<RemoveWatcherRequest>,
    ) -> Result<Response<RemoveWatcherResponse>, Status> {
        match &self.backend {
            Backend::Direct(service) => service.remove_watcher(request).await,
            #[cfg(target_os = "linux")]
            Backend::Workers(_) => {
                let request = request.into_inner();
                self.prune_watcher_routes().await;
                let route = {
                    let mut routes = self.watcher_routes.lock().await;
                    match routes.get_mut(&request.watcher_id) {
                        Some(WatcherRoute::Live(route)) => {
                            route.last_access = Instant::now();
                            (**route).clone()
                        }
                        Some(WatcherRoute::Reset { .. }) => {
                            // Reset routes need only local cleanup; retaining them
                            // would keep admission occupied until expiry.
                            routes.remove(&request.watcher_id);
                            return Ok(Response::new(RemoveWatcherResponse {}));
                        }
                        None => return Err(Status::not_found("watcher not found")),
                    }
                };
                let mut client = route.worker.client.clone();
                let response = self
                    .observe_worker_result(
                        &route.worker,
                        client
                            .remove_watcher(RemoveWatcherRequest {
                                watcher_id: route.worker_watcher_id,
                            })
                            .await,
                    )
                    .await?;
                self.watcher_routes.lock().await.remove(&request.watcher_id);
                Ok(response)
            }
        }
    }

    async fn enable_file_changes(
        &self,
        request: Request<EnableFileChangesRequest>,
    ) -> Result<Response<EnableFileChangesResponse>, Status> {
        match &self.backend {
            Backend::Direct(service) => service.enable_file_changes(request).await,
            #[cfg(target_os = "linux")]
            Backend::Workers(_) => {
                let mut request = request.into_inner();
                let user = self.effective_user(request.user.take())?;
                let worker = self.acquire_worker(user).await?;
                let mut client = worker.client.clone();
                self.update_ledger(worker, true, async move {
                    client.enable_file_changes(request).await
                })
                .await
            }
        }
    }

    async fn disable_file_changes(
        &self,
        request: Request<DisableFileChangesRequest>,
    ) -> Result<Response<DisableFileChangesResponse>, Status> {
        match &self.backend {
            Backend::Direct(service) => service.disable_file_changes(request).await,
            #[cfg(target_os = "linux")]
            Backend::Workers(_) => {
                let mut request = request.into_inner();
                let worker = self.worker(request.user.take()).await?;
                let mut client = worker.client.clone();
                self.update_ledger(worker, false, async move {
                    client.disable_file_changes(request).await
                })
                .await
            }
        }
    }

    async fn get_modified_files(
        &self,
        request: Request<GetModifiedFilesRequest>,
    ) -> Result<Response<GetModifiedFilesResponse>, Status> {
        match &self.backend {
            Backend::Direct(service) => service.get_modified_files(request).await,
            #[cfg(target_os = "linux")]
            Backend::Workers(_) => {
                let mut request = request.into_inner();
                let user = self.effective_user(request.user.take())?;
                if self.ledger_evidence.lock().await.get(&user) == Some(&LedgerEvidence::Lost) {
                    return Ok(Self::lost_ledger_response());
                }
                let worker = self.acquire_worker(user.clone()).await?;
                let _update = worker
                    .ledger_update_slot
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| {
                        crate::refused(Status::resource_exhausted(
                            "file-change update in progress; retry with backoff",
                        ))
                    })?;
                let mut client = worker.client.clone();
                let result = client.get_modified_files(request).await;
                if result.as_ref().is_err_and(is_worker_transport_failure) {
                    self.record_worker_failure(&worker).await;
                    if self.ledger_evidence.lock().await.get(&user) == Some(&LedgerEvidence::Lost) {
                        return Ok(Self::lost_ledger_response());
                    }
                }
                if let Ok(response) = &result {
                    self.observe_ledger_state(&worker, response.get_ref()).await;
                }
                result
            }
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;

#[cfg(all(test, unix))]
mod ingress_tests;
