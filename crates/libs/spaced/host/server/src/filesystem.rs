mod write_relay;
use super::*;
use crate::bridge::{
    BridgeGuestOperationBudget, BridgeRpcOutcome, record_bridge_rpc_outcome,
    record_bridge_rpc_unbounded_outcome,
};
use crate::protocol::*;
use pplx_space_common_core::types::{Pii, RelativeGuestPath};

const MAX_GUEST_FILE_CHUNK_BYTES: usize = 64 * 1024;
const MAX_GUEST_FILE_LIST_ENTRIES: usize = 10_000;
const MAX_GUEST_FILE_LIST_METADATA_BYTES: usize = 4 * 1024 * 1024;
const MAX_GUEST_MODIFIED_FILES: usize = 16_384;
const MAX_GUEST_MODIFIED_FILE_KINDS: usize = 5;
const MAX_GUEST_WATCHER_EVENTS: usize = 4_096;

impl SpacedHostService {
    pub(super) fn filesystem_grpc_service(
        &self,
    ) -> pplx_space_common_grpc::RequestAdmission<
        pplx_space_common_grpc::PreserveInputErrors<FilesystemServer<Self>>,
    > {
        use pplx_space_spaced_common::filesystem_limits;
        pplx_space_common_grpc::RequestAdmission::new(
            pplx_space_common_grpc::PreserveInputErrors::new(
                FilesystemServer::new(self.clone())
                    .max_decoding_message_size(filesystem_limits::MAX_MESSAGE_BYTES),
            ),
            self.inner.filesystem_admission.clone(),
            filesystem_limits::PAYLOAD_METHODS,
        )
    }
}

fn host_file_event(
    event: pplx_space_spaced_common::proto::bridge_guest::FileEvent,
) -> Result<FileEvent, Status> {
    let path = RelativeGuestPath::parse(event.path).map_err(invalid_argument_status)?;
    Ok(FileEvent {
        kind: bridge_file_event_kind_to_host(event.kind)?,
        path: Pii::new(&path).expose_pii().expose().to_string(),
        observed_at: event.observed_at,
    })
}

fn validate_file_info(info: FileInfo) -> Result<FileInfo, Status> {
    bridge_file_type_to_host(info.file_type)?;
    Ok(info)
}

fn file_info_metadata_bytes(info: &FileInfo) -> Option<usize> {
    info.path
        .len()
        .checked_add(info.name.as_deref().map_or(0, str::len))?
        .checked_add(info.symlink_target.as_deref().map_or(0, str::len))
}

fn host_modified_file(
    file: pplx_space_spaced_common::proto::bridge_guest::ModifiedFile,
) -> Result<proto::ModifiedFile, Status> {
    if !file.root.is_empty() {
        pplx_space_common_core::types::GuestPath::parse(&file.root)
            .map_err(invalid_argument_status)?;
    }
    let path = RelativeGuestPath::parse(file.path).map_err(invalid_argument_status)?;
    Ok(proto::ModifiedFile {
        root: file.root,
        path: Pii::new(&path).expose_pii().expose().to_string(),
        last_modified_at: file.last_modified_at,
        kinds: file
            .kinds
            .into_iter()
            .map(bridge_file_event_kind_to_host)
            .collect::<Result<_, Status>>()?,
    })
}

#[tonic::async_trait]
impl Filesystem for SpacedHostService {
    type ListStream = HostFileInfoStream;
    type ReadStream = HostFileChunkStream;
    type WatchStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<FileEvent, Status>> + Send>>;

    async fn stat(&self, request: Request<StatRequest>) -> Result<Response<StatResponse>, Status> {
        let request = request.into_inner();
        let path = request.path.clone();
        let mut response = self.call_bridge_guest_stat(request).await?;
        let info = response
            .info
            .as_mut()
            .ok_or_else(|| Status::internal("bridge stat omitted file info"))?;
        bridge_file_type_to_host(info.file_type)?;
        // Stat describes the requested path, not a guest-selected response path.
        info.path = path;
        info.name = None;
        Ok(Response::new(response))
    }

    async fn list(
        &self,
        request: Request<ListRequest>,
    ) -> Result<Response<Self::ListStream>, Status> {
        let guest_stream = self
            .call_bridge_guest_list(request.into_inner())
            .await
            .map_err(|status| {
                host_relay_status(status, "bridge_guest_list", "bridge_guest_filesystem")
            })?;
        let mut entries = 0usize;
        let mut metadata_bytes = 0usize;
        Ok(Response::new(relay_stream(guest_stream, move |info| {
            if entries == MAX_GUEST_FILE_LIST_ENTRIES {
                return Err(Status::resource_exhausted(
                    "guest file list returned too many entries",
                ));
            }
            let info_bytes = file_info_metadata_bytes(&info).ok_or_else(|| {
                Status::resource_exhausted("guest file list metadata size overflowed")
            })?;
            metadata_bytes = metadata_bytes
                .checked_add(info_bytes)
                .filter(|size| *size <= MAX_GUEST_FILE_LIST_METADATA_BYTES)
                .ok_or_else(|| {
                    Status::resource_exhausted("guest file list metadata is too large")
                })?;
            entries += 1;
            validate_file_info(info)
        })))
    }

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let request = request.into_inner();
        let requested_bytes = request.length;
        let guest_stream = self
            .call_bridge_guest_read(request)
            .await
            .map_err(|status| {
                host_relay_status(status, "bridge_guest_read", "bridge_guest_filesystem")
            })?;
        let mut returned_bytes = 0u64;
        Ok(Response::new(relay_stream(guest_stream, move |chunk| {
            if chunk.data.len() > MAX_GUEST_FILE_CHUNK_BYTES {
                return Err(Status::resource_exhausted("guest file chunk is too large"));
            }
            returned_bytes = returned_bytes
                .checked_add(chunk.data.len() as u64)
                .ok_or_else(|| Status::data_loss("guest file read size overflowed"))?;
            if requested_bytes.is_some_and(|requested_bytes| returned_bytes > requested_bytes) {
                return Err(Status::data_loss(
                    "guest file read exceeded the requested length",
                ));
            }
            Ok(chunk)
        })))
    }

    async fn mkdir(
        &self,
        request: Request<MkdirRequest>,
    ) -> Result<Response<MkdirResponse>, Status> {
        self.call_bridge_guest_mkdir(request.into_inner()).await?;
        Ok(Response::new(MkdirResponse {}))
    }

    async fn remove(
        &self,
        request: Request<RemoveRequest>,
    ) -> Result<Response<RemoveResponse>, Status> {
        self.call_bridge_guest_remove(request.into_inner()).await?;
        Ok(Response::new(RemoveResponse {}))
    }

    async fn rename(
        &self,
        request: Request<RenameRequest>,
    ) -> Result<Response<RenameResponse>, Status> {
        self.call_bridge_guest_rename(request.into_inner()).await?;
        Ok(Response::new(RenameResponse {}))
    }

    async fn write(
        &self,
        request: Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let input = request.into_inner();
        let write_timeout = BridgeGuestOperationBudget::FilesystemTransfer.timeout();
        let write_started_at = Instant::now();
        let lease = match pplx_space_spaced_common::retry_status_with_context(
            "spaced bridge guest",
            "bridge_guest_write_connect",
            self.inner.bridge_retry_policy,
            RetryContext::with_timeout(write_timeout)
                .with_cancellation(self.inner.shutdown.clone()),
            || async { self.bridge_guest_filesystem_client().await },
        )
        .await
        {
            Ok(lease) => {
                record_bridge_rpc_outcome(
                    "bridge_guest_filesystem",
                    "bridge_guest_write_connect",
                    write_timeout,
                    write_started_at,
                    BridgeRpcOutcome::Ok,
                );
                lease
            }
            Err(status) => {
                record_bridge_rpc_outcome(
                    "bridge_guest_filesystem",
                    "bridge_guest_write_connect",
                    write_timeout,
                    write_started_at,
                    BridgeRpcOutcome::from_status(&status),
                );
                return Err(host_relay_status(
                    status,
                    "bridge_guest_write",
                    "bridge_guest_filesystem",
                ));
            }
        };
        let client_generation = lease.generation();
        let mut client = lease.into_client();

        let body_started_at = Instant::now();
        let input =
            futures_util::StreamExt::map(input, |message| message.and_then(map_write_request));
        let result =
            write_relay::relay(input, move |body| async move { client.write(body).await }).await;
        match result {
            Ok(response) => {
                record_bridge_rpc_unbounded_outcome(
                    "bridge_guest_filesystem",
                    "bridge_guest_write",
                    body_started_at,
                    BridgeRpcOutcome::Ok,
                );
                self.inner.bridge_guest_filesystem_client.set_connected();
                Ok(Response::new(WriteResponse {
                    bytes_written: response.into_inner().bytes_written,
                }))
            }
            Err(error) => {
                // A caller's input failure says nothing about bridge health.
                // Keep the cached channel and its generation for other RPCs.
                if matches!(&error, write_relay::RelayError::Downstream(_)) {
                    self.inner
                        .bridge_guest_filesystem_client
                        .clear_if_generation(client_generation)
                        .await;
                }
                let status = error.into_status();
                record_bridge_rpc_unbounded_outcome(
                    "bridge_guest_filesystem",
                    "bridge_guest_write",
                    body_started_at,
                    BridgeRpcOutcome::from_unbounded_status(&status),
                );
                Err(host_relay_status(
                    status,
                    "bridge_guest_write",
                    "bridge_guest_filesystem",
                ))
            }
        }
    }

    async fn write_batch(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> Result<Response<WriteBatchResponse>, Status> {
        let request = bridge_write_batch_request(request.into_inner())?;
        let timeout = BridgeGuestOperationBudget::FilesystemTransfer.timeout();
        let started_at = Instant::now();
        let lease = match pplx_space_spaced_common::retry_status_with_context(
            "spaced bridge guest",
            "bridge_guest_write_batch_connect",
            self.inner.bridge_retry_policy,
            RetryContext::with_timeout(timeout).with_cancellation(self.inner.shutdown.clone()),
            || async { self.bridge_guest_filesystem_client().await },
        )
        .await
        {
            Ok(lease) => {
                record_bridge_rpc_outcome(
                    "bridge_guest_filesystem",
                    "bridge_guest_write_batch_connect",
                    timeout,
                    started_at,
                    BridgeRpcOutcome::Ok,
                );
                lease
            }
            Err(status) => {
                record_bridge_rpc_outcome(
                    "bridge_guest_filesystem",
                    "bridge_guest_write_batch_connect",
                    timeout,
                    started_at,
                    BridgeRpcOutcome::from_status(&status),
                );
                return Err(host_relay_status(
                    status,
                    "bridge_guest_write_batch",
                    "bridge_guest_filesystem",
                ));
            }
        };
        let client_generation = lease.generation();
        let mut client = lease.into_client();
        let body_started_at = Instant::now();
        let response = client.write_batch(request).await;
        record_bridge_rpc_unbounded_outcome(
            "bridge_guest_filesystem",
            "bridge_guest_write_batch",
            body_started_at,
            BridgeRpcOutcome::from_unbounded_result(&response),
        );
        match response {
            Ok(response) => {
                self.inner.bridge_guest_filesystem_client.set_connected();
                let response = response.into_inner();
                Ok(Response::new(WriteBatchResponse {
                    files_written: response.files_written,
                }))
            }
            Err(status) => {
                self.inner
                    .bridge_guest_filesystem_client
                    .clear_if_generation(client_generation)
                    .await;
                Err(host_relay_status(
                    status,
                    "bridge_guest_write_batch",
                    "bridge_guest_filesystem",
                ))
            }
        }
    }

    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let request = bridge_watch_request(request.into_inner())?;
        let guest_stream = self
            .call_bridge_guest_filesystem(
                "bridge_guest_watch",
                BridgeGuestOperationBudget::FilesystemTransfer,
                move |mut client| {
                    let request = request.clone();
                    async move { client.watch(request).await }
                },
            )
            .await?;
        Ok(Response::new(relay_stream(guest_stream, host_file_event)))
    }

    async fn create_watcher(
        &self,
        request: Request<CreateWatcherRequest>,
    ) -> Result<Response<CreateWatcherResponse>, Status> {
        let request = bridge_create_watcher_request(request.into_inner())?;
        let response = self
            .call_bridge_guest_filesystem_once(
                "bridge_guest_create_watcher",
                BridgeGuestOperationBudget::FilesystemMutation,
                move |mut client| {
                    let request = request.clone();
                    async move { client.create_watcher(request).await }
                },
            )
            .await?;
        Ok(Response::new(CreateWatcherResponse {
            watcher_id: response.watcher_id,
            root: response.root,
        }))
    }

    async fn normalize_watch_root(
        &self,
        request: Request<NormalizeWatchRootRequest>,
    ) -> Result<Response<NormalizeWatchRootResponse>, Status> {
        let request = bridge_normalize_watch_root_request(request.into_inner())?;
        let response = self
            .call_bridge_guest_filesystem(
                "bridge_guest_normalize_watch_root",
                BridgeGuestOperationBudget::FilesystemMetadata,
                move |mut client| {
                    let request = request.clone();
                    async move { client.normalize_watch_root(request).await }
                },
            )
            .await?;
        Ok(Response::new(NormalizeWatchRootResponse {
            root: response.root,
        }))
    }

    async fn get_watcher_events(
        &self,
        request: Request<GetWatcherEventsRequest>,
    ) -> Result<Response<GetWatcherEventsResponse>, Status> {
        let request = request.into_inner();
        let response = self
            .call_bridge_guest_filesystem_once(
                "bridge_guest_get_watcher_events",
                BridgeGuestOperationBudget::FilesystemMetadata,
                move |mut client| {
                    let request = request.clone();
                    async move {
                        client
                            .get_watcher_events(BridgeGuestGetWatcherEventsRequest {
                                watcher_id: request.watcher_id,
                            })
                            .await
                    }
                },
            )
            .await?;
        if response.events.len() > MAX_GUEST_WATCHER_EVENTS {
            return Err(Status::resource_exhausted(
                "guest watcher returned too many events",
            ));
        }
        Ok(Response::new(GetWatcherEventsResponse {
            events: response
                .events
                .into_iter()
                .map(host_file_event)
                .collect::<Result<_, Status>>()?,
        }))
    }

    async fn remove_watcher(
        &self,
        request: Request<RemoveWatcherRequest>,
    ) -> Result<Response<RemoveWatcherResponse>, Status> {
        let request = request.into_inner();
        self.call_bridge_guest_filesystem_once(
            "bridge_guest_remove_watcher",
            BridgeGuestOperationBudget::FilesystemMutation,
            move |mut client| {
                let request = request.clone();
                async move {
                    client
                        .remove_watcher(BridgeGuestRemoveWatcherRequest {
                            watcher_id: request.watcher_id,
                        })
                        .await
                }
            },
        )
        .await?;
        Ok(Response::new(RemoveWatcherResponse {}))
    }

    async fn enable_file_changes(
        &self,
        request: Request<EnableFileChangesRequest>,
    ) -> Result<Response<EnableFileChangesResponse>, Status> {
        let request = bridge_enable_file_changes_request(request.into_inner())?;
        let response = self
            .call_bridge_guest_filesystem_once(
                "bridge_guest_enable_file_changes",
                BridgeGuestOperationBudget::FilesystemMutation,
                move |mut client| {
                    let request = request.clone();
                    async move { client.enable_file_changes(request).await }
                },
            )
            .await?;
        Ok(Response::new(EnableFileChangesResponse {
            observed_since: response.observed_since,
        }))
    }

    async fn disable_file_changes(
        &self,
        request: Request<DisableFileChangesRequest>,
    ) -> Result<Response<DisableFileChangesResponse>, Status> {
        let user = request
            .into_inner()
            .user
            .map(pplx_space_common_core::types::GuestUserName::parse)
            .transpose()
            .map_err(invalid_argument_status)?;
        self.call_bridge_guest_filesystem_once(
            "bridge_guest_disable_file_changes",
            BridgeGuestOperationBudget::FilesystemMutation,
            move |mut client| {
                let user = user.as_ref().map(|user| user.as_str().to_string());
                async move {
                    client
                        .disable_file_changes(BridgeGuestDisableFileChangesRequest { user })
                        .await
                }
            },
        )
        .await?;
        Ok(Response::new(DisableFileChangesResponse {}))
    }

    async fn get_modified_files(
        &self,
        request: Request<GetModifiedFilesRequest>,
    ) -> Result<Response<GetModifiedFilesResponse>, Status> {
        let request = request.into_inner();
        let user = request
            .user
            .clone()
            .map(pplx_space_common_core::types::GuestUserName::parse)
            .transpose()
            .map_err(invalid_argument_status)?;
        let response = self
            .call_bridge_guest_filesystem(
                "bridge_guest_get_modified_files",
                BridgeGuestOperationBudget::FilesystemMetadata,
                move |mut client| {
                    let request = request.clone();
                    let user = user.as_ref().map(|user| user.as_str().to_string());
                    async move {
                        client
                            .get_modified_files(BridgeGuestGetModifiedFilesRequest {
                                window: request.window,
                                since: request.since,
                                cursor: request.cursor,
                                user,
                            })
                            .await
                    }
                },
            )
            .await?;
        if response.files.len() > MAX_GUEST_MODIFIED_FILES {
            return Err(Status::resource_exhausted(
                "guest returned too many modified files",
            ));
        }
        if response
            .files
            .iter()
            .any(|file| file.kinds.len() > MAX_GUEST_MODIFIED_FILE_KINDS)
        {
            return Err(Status::resource_exhausted(
                "guest modified file has too many event kinds",
            ));
        }
        Ok(Response::new(GetModifiedFilesResponse {
            files: response
                .files
                .into_iter()
                .map(host_modified_file)
                .collect::<Result<_, Status>>()?,
            observed_since: response.observed_since,
            retained_since: response.retained_since,
            cursor: response.cursor,
            monitoring_enabled: response.monitoring_enabled,
            overflowed: response.overflowed,
        }))
    }
}

#[cfg(test)]
mod tests;
