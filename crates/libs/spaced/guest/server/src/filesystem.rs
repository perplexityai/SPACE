mod compat;
mod error;
mod list;
mod metadata;
mod ownership;
mod read;
pub(super) mod watch;
mod write;

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use crate::guest_path;
use crate::identity::GuestAccount;
use crate::{GuestFileChunkStream, GuestFileInfoStream};
pub(super) use error::map_io_error;
use metadata::{
    classify_file_type, file_link_count, file_mode, file_owner, symlink_target,
    system_time_to_timestamp,
};
use pplx_space_common_core::types::{GuestPath, GuestUserName};
use pplx_space_spaced_common::proto::bridge_guest::{
    CreateWatcherRequest, CreateWatcherResponse, DisableFileChangesRequest,
    DisableFileChangesResponse, EnableFileChangesRequest, EnableFileChangesResponse, FileInfo,
    GetModifiedFilesRequest, GetModifiedFilesResponse, GetWatcherEventsRequest,
    GetWatcherEventsResponse, ListRequest, MkdirRequest, MkdirResponse, NormalizeWatchRootRequest,
    NormalizeWatchRootResponse, ReadRequest, RemoveRequest, RemoveResponse, RemoveWatcherRequest,
    RemoveWatcherResponse, RenameResponse, StatRequest, StatResponse, WatchRequest,
    WriteBatchRequest,
};
use pplx_space_spaced_common::proto::worker::worker_filesystem_server::WorkerFilesystem;
use pplx_space_spaced_common::proto::worker::{self as protocol, MutationResult, mutation_result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

mod operations;
pub(crate) mod router;
mod worker;

pub(crate) use router::FilesystemRouter;
#[cfg(target_os = "linux")]
pub(crate) use worker::FilesystemWorkerError;

#[cfg(target_os = "linux")]
pub(crate) fn run_worker_from_environment() -> Result<bool, FilesystemWorkerError> {
    worker::run_from_environment()
}

#[derive(Clone)]
pub(super) struct FilesystemWorkerService {
    default_user: GuestUserName,
    account: Option<Arc<GuestAccount>>,
    tasks: tokio_util::task::TaskTracker,
    watch_manager: watch::WatchManager,
    /// Admits file operations into a budget of slots, queueing a burst briefly.
    operation_admission: pplx_space_common_grpc::QueuedSlots,
    upload_buffers: Arc<write::UploadBufferPool>,
}

const MAX_CONCURRENT_FILE_OPERATIONS: usize = 8;
pub(super) const WORKER_PROTOCOL_VERSION: u32 = 3;

impl FilesystemWorkerService {
    pub(super) fn new(default_user: GuestUserName, shutdown: CancellationToken) -> Self {
        let operation_admission = pplx_space_common_grpc::QueuedSlots::new(Arc::new(
            Semaphore::new(MAX_CONCURRENT_FILE_OPERATIONS),
        ))
        .with_queue(
            pplx_space_spaced_common::admission_queue::WAITERS,
            pplx_space_spaced_common::admission_queue::MAX_WAIT,
        );
        Self {
            default_user,
            account: None,
            tasks: tokio_util::task::TaskTracker::new(),
            watch_manager: watch::WatchManager::new(shutdown),
            operation_admission,
            upload_buffers: Arc::new(write::UploadBufferPool::new()),
        }
    }

    async fn execute<Fut>(
        &self,
        future: impl FnOnce(Arc<GuestAccount>) -> Fut + Send + 'static,
    ) -> Result<Response<MutationResult>, Status>
    where
        Fut:
            std::future::Future<Output = Result<mutation_result::Outcome, Status>> + Send + 'static,
    {
        let permit = match self.acquire_file_operation_slot().await {
            Ok(permit) => permit,
            Err(error) => {
                return Ok(Response::new(MutationResult {
                    outcome: Some(mutation_result::Outcome::RejectedBeforeExecution(
                        operations::operation_failure(&error),
                    )),
                }));
            }
        };
        // The service task owns effects after dispatch, not the RPC waiter.
        let service = self.clone();
        let task = self.tasks.spawn(async move {
            operations::outcome(
                async {
                    let (account, _permit) = service.account(None, Some(permit)).await?;
                    future(account).await
                }
                .await,
            )
        });
        let result = task.await.map_err(|error| {
            tracing::error!(%error, "filesystem executor task failed");
            operations::unknown_outcome()
        })?;
        Ok(Response::new(result))
    }

    async fn execute_write(
        &self,
        stream: impl futures_util::Stream<Item = Result<protocol::WriteFrame, Status>>
        + Send
        + Unpin
        + 'static,
        direct_owner: Option<GuestUserName>,
    ) -> Result<Response<MutationResult>, Status> {
        let cancelled = tokio_util::sync::CancellationToken::new();
        let _cancel_on_drop = cancelled.clone().drop_guard();
        let upload_buffers = Arc::clone(&self.upload_buffers);
        self.execute(async move |account| {
            let owner = direct_owner.map(|_| account.ownership()).transpose()?;
            match write::write(stream, &account, owner, cancelled, upload_buffers).await {
                Ok(response) => Ok(mutation_result::Outcome::Write(response)),
                Err(write::WriteError::Interrupted) => {
                    Ok(mutation_result::Outcome::Interrupted(protocol::Abort {}))
                }
                Err(write::WriteError::Failed(error)) => Err(error),
            }
        })
        .await
    }

    async fn acquire_file_operation_slot(&self) -> Result<OwnedSemaphorePermit, Status> {
        self.operation_admission.acquire(1).await.ok_or_else(|| {
            Status::resource_exhausted("too many concurrent filesystem file operations")
        })
    }

    async fn account(
        &self,
        user: Option<String>,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Result<(Arc<GuestAccount>, OwnedSemaphorePermit), Status> {
        let slot = match permit {
            Some(slot) => slot,
            None => self.acquire_file_operation_slot().await?,
        };
        let user = guest_path::effective_user(user, &self.default_user)?;
        if let Some(account) = &self.account {
            if account.name() != &user {
                return Err(Status::permission_denied(
                    "filesystem worker identity is fixed",
                ));
            }
            return Ok((account.clone(), slot));
        }
        tokio::task::spawn_blocking(move || {
            let account = GuestAccount::resolve(user).map_err(|error| error.into_status())?;
            Ok((Arc::new(account), slot))
        })
        .await
        .map_err(|error| Status::internal(format!("account resolution failed: {error}")))?
    }

    async fn normalize_path(
        &self,
        path: String,
        user: Option<String>,
    ) -> Result<GuestPath, Status> {
        let (account, _slot) = self.account(user, None).await?;
        guest_path::normalize_for_account(&path, &account)
    }
}

#[tonic::async_trait]
impl WorkerFilesystem for FilesystemWorkerService {
    async fn ready(
        &self,
        _: Request<protocol::ReadyRequest>,
    ) -> Result<Response<protocol::ReadyResponse>, Status> {
        Ok(Response::new(protocol::ReadyResponse {
            protocol_version: WORKER_PROTOCOL_VERSION,
        }))
    }

    type ListStream = GuestFileInfoStream;
    type ReadStream = GuestFileChunkStream;
    type OpenReadStream = std::pin::Pin<
        Box<dyn futures_util::Stream<Item = Result<protocol::OpenedFile, Status>> + Send>,
    >;
    type WatchStream = watch::WatchStream;

    async fn stat(&self, request: Request<StatRequest>) -> Result<Response<StatResponse>, Status> {
        let request = request.into_inner();
        // Stat echoes the requested path, including relative spelling, as the
        // legacy host adapter does. List entries instead carry resolved paths.
        let response_path = request.path.clone();
        let path = self.normalize_path(request.path, request.user).await?;

        let metadata = tokio::fs::symlink_metadata(path.expose())
            .await
            .map_err(map_io_error)?;
        let file_type = metadata.file_type();
        let modified_at = metadata.modified().ok().and_then(system_time_to_timestamp);

        let (owner_uid, owner_gid) = file_owner(&metadata);
        let symlink_target = symlink_target(Path::new(path.expose()), &metadata).await?;

        tracing::debug!(
            command = "bridge_guest_stat",
            ?path,
            "spaced bridge guest command succeeded"
        );
        Ok(Response::new(StatResponse {
            info: Some(FileInfo {
                path: response_path,
                name: None,
                file_type: classify_file_type(file_type).into(),
                size_bytes: metadata.len(),
                mode: file_mode(&metadata),
                modified_at,
                owner_uid,
                owner_gid,
                symlink_target,
                // From the `metadata` already read above, so no extra stat.
                link_count: file_link_count(&metadata),
            }),
        }))
    }

    async fn list(
        &self,
        request: Request<ListRequest>,
    ) -> Result<Response<Self::ListStream>, Status> {
        let request = request.into_inner();
        let path = self.normalize_path(request.path, request.user).await?;

        let stream_slot = self.acquire_file_operation_slot().await?;
        let stream = list::list_stream(path.clone(), request.depth, stream_slot).await?;
        tracing::debug!(
            command = "bridge_guest_list",
            ?path,
            "spaced bridge guest command succeeded"
        );
        Ok(Response::new(stream))
    }

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let request = request.into_inner();
        let path = self.normalize_path(request.path, request.user).await?;
        let offset = request.offset;
        let length = request.length;
        let stream_slot = self.acquire_file_operation_slot().await?;
        let stream = read::read_stream(path.clone(), offset, length, stream_slot).await?;
        tracing::debug!(
            command = "bridge_guest_read",
            ?path,
            offset,
            length,
            "spaced bridge guest command succeeded"
        );
        Ok(Response::new(stream))
    }

    async fn open_read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::OpenReadStream>, Status> {
        use futures_util::StreamExt;
        use std::os::fd::AsRawFd;
        let request = request.into_inner();
        let path = self.normalize_path(request.path, request.user).await?;
        let stream_slot = self.acquire_file_operation_slot().await?;
        let (file, stream_slot) = read::open_for_read(path, request.offset, stream_slot).await?;
        let fd = file.as_raw_fd();
        // The file and this worker's admission stay held until the caller,
        // done with its own copy of the descriptor, ends the stream.
        let lease = (file, stream_slot);
        let held = futures_util::stream::poll_fn(move |_| {
            let _lease = &lease;
            std::task::Poll::Pending
        });
        Ok(Response::new(Box::pin(
            futures_util::stream::once(async move { Ok(protocol::OpenedFile { fd }) }).chain(held),
        )))
    }

    async fn mkdir(
        &self,
        request: Request<MkdirRequest>,
    ) -> Result<Response<MkdirResponse>, Status> {
        let request = request.into_inner();
        let assign_owner = request.user.is_some();
        let (account, _slot) = self.account(request.user, None).await?;
        let path = guest_path::normalize_for_account(&request.path, &account)?;
        let owner = assign_owner.then(|| account.ownership()).transpose()?;
        let result = if request.parents {
            create_dir_all_with_owner(Path::new(path.expose()), owner).await
        } else {
            create_dir_with_owner(Path::new(path.expose()), owner).await
        };
        match result {
            Ok(()) => {}
            Err(status) if request.exist_ok && status.code() == tonic::Code::AlreadyExists => {
                ensure_existing_directory_no_follow(Path::new(path.expose())).await?;
            }
            Err(status) => return Err(status),
        }
        tracing::debug!(
            command = "bridge_guest_mkdir",
            ?path,
            parents = request.parents,
            exist_ok = request.exist_ok,
            "spaced bridge guest command succeeded"
        );
        Ok(Response::new(MkdirResponse {}))
    }

    async fn remove(
        &self,
        request: Request<RemoveRequest>,
    ) -> Result<Response<RemoveResponse>, Status> {
        let request = request.into_inner();
        let path = self.normalize_path(request.path, request.user).await?;
        // `rm -rf` semantics: a recursive remove on a regular file or symlink
        // should still succeed by deleting that single entry. `remove_dir_all`
        // alone returns ENOTDIR in that case.
        let result = match tokio::fs::symlink_metadata(path.expose()).await {
            Ok(metadata) if metadata.is_dir() => {
                if request.recursive {
                    tokio::fs::remove_dir_all(path.expose()).await
                } else {
                    tokio::fs::remove_dir(path.expose()).await
                }
            }
            Ok(_) => tokio::fs::remove_file(path.expose()).await,
            Err(error) => Err(error),
        };
        match result {
            Ok(()) => {}
            Err(error) if request.missing_ok && error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(map_io_error(error)),
        }
        tracing::debug!(
            command = "bridge_guest_remove",
            ?path,
            recursive = request.recursive,
            missing_ok = request.missing_ok,
            "spaced bridge guest command succeeded"
        );
        Ok(Response::new(RemoveResponse {}))
    }

    async fn rename(
        &self,
        request: Request<protocol::RenameCommand>,
    ) -> Result<Response<MutationResult>, Status> {
        let request = request.into_inner();
        self.execute(async move |account| {
            uuid::Uuid::parse_str(&request.execution_id)
                .map_err(|_| Status::invalid_argument("invalid execution id"))?;
            let old_path = guest_path::normalize_for_account(&request.old_path, &account)?;
            let new_path = guest_path::normalize_for_account(&request.new_path, &account)?;
            tokio::fs::rename(old_path.expose(), new_path.expose())
                .await
                .map_err(map_io_error)?;
            Ok(mutation_result::Outcome::Rename(RenameResponse {}))
        })
        .await
    }

    async fn normalize_watch_root(
        &self,
        request: Request<NormalizeWatchRootRequest>,
    ) -> Result<Response<NormalizeWatchRootResponse>, Status> {
        let request = request.into_inner();
        let path = self.normalize_path(request.path, request.user).await?;
        let root = watch::normalize_watch_root_string(&path).await?;
        Ok(Response::new(NormalizeWatchRootResponse { root }))
    }

    async fn write(
        &self,
        request: Request<tonic::Streaming<protocol::WriteFrame>>,
    ) -> Result<Response<MutationResult>, Status> {
        self.execute_write(request.into_inner(), None).await
    }

    async fn write_batch(
        &self,
        request: Request<protocol::BatchCommand>,
    ) -> Result<Response<MutationResult>, Status> {
        let request = request.into_inner();
        self.execute(async move |account| {
            uuid::Uuid::parse_str(&request.execution_id)
                .map_err(|_| Status::invalid_argument("invalid execution id"))?;
            let request = WriteBatchRequest {
                entries: request.entries,
                idempotency_key: None,
                user: None,
            };
            write::write_batch(request, &account)
                .await
                .map(mutation_result::Outcome::Batch)
        })
        .await
    }

    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        // Bound setup payloads and completed directory plans, not the stream
        // lifetime. Blocking scans separately retain their own scan permits.
        let request = request.into_inner();
        let path = self.normalize_path(request.path, request.user).await?;
        let _setup_slot = self.acquire_file_operation_slot().await?;
        let recursive = request.recursive;
        let include_globs = watch::parse_watch_globs(request.include_globs, "include_globs")?;
        let exclude_globs = watch::parse_watch_globs(request.exclude_globs, "exclude_globs")?;
        let stream = self
            .watch_manager
            .watch_stream(
                path.clone(),
                recursive,
                request.include_metadata_events,
                include_globs,
                exclude_globs,
                request.use_default_excludes,
            )
            .await?;
        tracing::debug!(
            command = "bridge_guest_watch",
            ?path,
            recursive,
            "spaced bridge guest command succeeded"
        );
        Ok(Response::new(stream))
    }

    async fn create_watcher(
        &self,
        request: Request<CreateWatcherRequest>,
    ) -> Result<Response<CreateWatcherResponse>, Status> {
        let request = request.into_inner();
        let root = self.normalize_path(request.root, request.user).await?;
        let _setup_slot = self.acquire_file_operation_slot().await?;
        let include_globs = watch::parse_watch_globs(request.include_globs, "include_globs")?;
        let exclude_globs = watch::parse_watch_globs(request.exclude_globs, "exclude_globs")?;
        let (watcher_id, root) = self
            .watch_manager
            .create_watcher(
                root,
                request.recursive,
                request.include_metadata_events,
                include_globs,
                exclude_globs,
                request.use_default_excludes,
            )
            .await?;
        Ok(Response::new(CreateWatcherResponse { watcher_id, root }))
    }

    async fn get_watcher_events(
        &self,
        request: Request<GetWatcherEventsRequest>,
    ) -> Result<Response<GetWatcherEventsResponse>, Status> {
        let request = request.into_inner();
        if request.watcher_id.is_empty() {
            return Err(Status::invalid_argument("watcher_id is required"));
        }
        let events = self
            .watch_manager
            .get_watcher_events(&request.watcher_id)
            .await?;
        Ok(Response::new(GetWatcherEventsResponse { events }))
    }

    async fn remove_watcher(
        &self,
        request: Request<RemoveWatcherRequest>,
    ) -> Result<Response<RemoveWatcherResponse>, Status> {
        let request = request.into_inner();
        if request.watcher_id.is_empty() {
            return Err(Status::invalid_argument("watcher_id is required"));
        }
        self.watch_manager
            .remove_watcher(&request.watcher_id)
            .await?;
        Ok(Response::new(RemoveWatcherResponse {}))
    }

    async fn enable_file_changes(
        &self,
        request: Request<EnableFileChangesRequest>,
    ) -> Result<Response<EnableFileChangesResponse>, Status> {
        let request = request.into_inner();
        let (account, _setup_slot) = self.account(request.user, None).await?;
        let roots = request
            .roots
            .into_iter()
            .map(|root| guest_path::normalize_for_account(&root, &account))
            .collect::<Result<Vec<_>, _>>()?;
        let include_globs = watch::parse_watch_globs(request.include_globs, "include_globs")?;
        let exclude_globs = watch::parse_watch_globs(request.exclude_globs, "exclude_globs")?;
        let observed_since_unix_ms = self
            .watch_manager
            .enable_file_changes(
                roots,
                include_globs,
                exclude_globs,
                request.use_default_excludes,
            )
            .await?;
        Ok(Response::new(EnableFileChangesResponse {
            observed_since: Some(watch::unix_ms_to_timestamp(observed_since_unix_ms)),
        }))
    }

    async fn disable_file_changes(
        &self,
        _request: Request<DisableFileChangesRequest>,
    ) -> Result<Response<DisableFileChangesResponse>, Status> {
        self.watch_manager.disable_file_changes().await;
        Ok(Response::new(DisableFileChangesResponse {}))
    }

    async fn get_modified_files(
        &self,
        request: Request<GetModifiedFilesRequest>,
    ) -> Result<Response<GetModifiedFilesResponse>, Status> {
        let request = request.into_inner();
        let response = self.watch_manager.get_modified_files(request).await?;
        Ok(Response::new(response))
    }
}

async fn create_dir_with_owner(path: &Path, owner: Option<(u32, u32)>) -> Result<(), Status> {
    tokio::fs::create_dir(path).await.map_err(map_io_error)?;
    if let Some((uid, gid)) = owner {
        ownership::chown_directory_no_follow(path, uid, gid).await?;
    }
    Ok(())
}

async fn create_dir_all_with_owner(path: &Path, owner: Option<(u32, u32)>) -> Result<(), Status> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => {
                current.push(prefix.as_os_str());
                continue;
            }
            Component::RootDir => {
                current.push(component.as_os_str());
                continue;
            }
            Component::CurDir => continue,
            Component::ParentDir | Component::Normal(_) => current.push(component.as_os_str()),
        }

        match create_dir_with_owner(&current, owner).await {
            Ok(()) => {}
            Err(status) if status.code() == tonic::Code::AlreadyExists => {
                ensure_existing_directory_no_follow(&current).await?;
            }
            Err(status) => return Err(status),
        }
    }
    Ok(())
}

async fn ensure_existing_directory_no_follow(path: &Path) -> Result<(), Status> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(map_io_error)?;
    if metadata.file_type().is_dir() {
        return Ok(());
    }
    Err(Status::failed_precondition(format!(
        "path exists but is not a directory: {}",
        path.display()
    )))
}

#[cfg(test)]
mod tests;
