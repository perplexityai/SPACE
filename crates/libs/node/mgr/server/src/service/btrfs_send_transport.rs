//! Narrow upload leases owned by volume-manager. The stage RPC copies COW
//! inputs before acknowledging; the lease lock survives the kernel reader.
use std::fs::File;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use pplx_space_common_svc::{LocalRole, UdsPeerPrincipal};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use super::{NodeManagerService, artifact_fs, btrfs_send_source, trusted_paths};
use crate::proto::*;

#[cfg(target_os = "linux")]
use super::btrfs_send_stream as framing;
#[cfg(all(test, target_os = "linux"))]
mod tests;

const ROOT: &str = "/space/cache/.btrfs-send-staging";
const ALLOWED: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Admin),
    UdsPeerPrincipal::Service(
        pplx_space_common_core::host::linux::identities::user_names::SPACE_VOLMGR,
    ),
];
/// Capabilities disclose nothing about any source. Sandbox-manager reads them
/// to decide whether to defer a send; lease operations stay volume-manager's.
const CAPABILITIES_ALLOWED: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Admin),
    UdsPeerPrincipal::Service(
        pplx_space_common_core::host::linux::identities::user_names::SPACE_VOLMGR,
    ),
    UdsPeerPrincipal::Service(
        pplx_space_common_core::host::linux::identities::user_names::SPACE_SBXMGR,
    ),
];
type Body = Pin<Box<dyn tokio_stream::Stream<Item = Result<BtrfsSendChunk, Status>> + Send>>;

/// `Aborted`: the stream was produced, but cannot be trusted; volume-manager
/// retries the whole transfer with a fresh send.
#[cfg(target_os = "linux")]
fn framing_status(error: framing::FramingError) -> Status {
    use framing::FramingError;
    use pplx_space_metrics::node_manager::BtrfsSendStreamRejection as Rejection;
    let reason = match error {
        FramingError::BadMagic => Rejection::BadMagic,
        FramingError::Version(_) => Rejection::Version,
        FramingError::Command { .. } => Rejection::Command,
        FramingError::CommandTooLarge { .. } => Rejection::CommandTooLarge,
        FramingError::Crc { .. } => Rejection::Crc,
        FramingError::DataAfterEnd { .. } => Rejection::DataAfterEnd,
        FramingError::Truncated { .. } => Rejection::Truncated,
    };
    pplx_space_metrics::node_manager::btrfs_send_stream_rejected(reason);
    tracing::warn!(%error, "rejected btrfs send stream before forwarding it");
    Status::aborted(format!("btrfs send stream rejected: {error}"))
}

fn lease_id(raw: &str) -> Result<Uuid, Status> {
    let id = raw
        .parse::<Uuid>()
        .map_err(|_| Status::invalid_argument("invalid send lease"))?;
    if raw != id.to_string() {
        return Err(Status::invalid_argument("noncanonical send lease"));
    }
    Ok(id)
}

fn root() -> Result<File, Status> {
    let root = trusted_paths::create_directory(Path::new(ROOT))?;
    trusted_paths::set_permissions(&root, None, 0o700)?;
    Ok(root)
}

impl NodeManagerService {
    pub(super) async fn get_btrfs_send_capabilities(
        &self,
        request: Request<GetBtrfsSendCapabilitiesRequest>,
    ) -> Result<Response<GetBtrfsSendCapabilitiesResponse>, Status> {
        self.authorize_peer(&request, "GetBtrfsSendCapabilities", CAPABILITIES_ALLOWED)?;
        Ok(Response::new(GetBtrfsSendCapabilitiesResponse {
            supported: cfg!(target_os = "linux"),
            inspect_send_source: cfg!(target_os = "linux"),
        }))
    }

    async fn send_lease_lock(&self, id: Uuid) -> tokio::sync::OwnedMutexGuard<()> {
        Self::sandbox_cgroup_family_lock(&self.send_source_locks, &id.to_string())
            .await
            .lock_owned()
            .await
    }

    pub(super) async fn btrfs_stage_send_source(
        &self,
        request: Request<BtrfsStageSendSourceRequest>,
    ) -> Result<Response<BtrfsSendLease>, Status> {
        self.authorize_peer(&request, "BtrfsStageSendSource", ALLOWED)?;
        let req = request.into_inner();
        let id = lease_id(&req.lease_id)?;
        let source_id = lease_id(&req.source_id)?;
        let artifact = crate::runtime_paths::validate_create_snapshot_dest(
            &req.sandbox_id,
            &req.artifact_dir,
        )?;
        self.run_artifact_mutation(&req.sandbox_id, move |service| async move {
            let guard = service.send_lease_lock(id).await;
            service.off_runtime("stage send source", move || {
                let _guard = guard;
                let artifact = trusted_paths::open_directory(&artifact)?;
                let source = btrfs_send_source::SendSource::open(&artifact, source_id)?;
                let root = root()?;
                let name = id.to_string();
                nix::sys::stat::mkdirat(&root, name.as_str(), nix::sys::stat::Mode::from_bits_truncate(0o700)).map_err(artifact_fs::error)?;
                let destination = artifact_fs::directory(&root, name.as_ref()).map_err(artifact_fs::error)?;
                let result = btrfs_send_source::capture_open(&destination, &source.parent, &source.source, id, source.received_uuid);
                if result.is_err() && let Err(error) = artifact_fs::remove_at(&root, name.as_ref()) {
                    tracing::warn!(%error, lease_id = %id, "partial send lease needs orphan cleanup");
                }
                result?;
                root.sync_all().map_err(artifact_fs::error)?;
                Ok(Response::new(BtrfsSendLease { lease_id: name }))
            }).await
        }).await
    }

    pub(super) async fn btrfs_delete_send_source(
        &self,
        request: Request<BtrfsSendLease>,
    ) -> Result<Response<BtrfsDeleteSendSourceResponse>, Status> {
        self.authorize_peer(&request, "BtrfsDeleteSendSource", ALLOWED)?;
        let id = lease_id(&request.into_inner().lease_id)?;
        let guard = self.send_lease_lock(id).await;
        self.off_runtime("release send lease", move || {
            let _guard = guard;
            let root = root()?;
            artifact_fs::remove_at(&root, id.to_string().as_ref())?;
            root.sync_all().map_err(artifact_fs::error)?;
            Ok(Response::new(BtrfsDeleteSendSourceResponse {}))
        })
        .await
    }

    pub(super) async fn btrfs_list_send_sources(
        &self,
        request: Request<BtrfsListSendSourcesRequest>,
    ) -> Result<Response<BtrfsListSendSourcesResponse>, Status> {
        self.authorize_peer(&request, "BtrfsListSendSources", ALLOWED)?;
        self.off_runtime("enumerate send leases", move || {
            let root = root()?;
            let lease_ids = artifact_fs::entries(&root)?
                .into_iter()
                .map(|name| {
                    let raw = name
                        .to_str()
                        .ok_or_else(|| Status::failed_precondition("non-UTF8 send lease"))?;
                    lease_id(raw).map(|id| id.to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Response::new(BtrfsListSendSourcesResponse { lease_ids }))
        })
        .await
    }

    pub(super) async fn btrfs_read_send_source(
        &self,
        request: Request<BtrfsSendLease>,
    ) -> Result<Response<Body>, Status> {
        self.authorize_peer(&request, "BtrfsReadSendSource", ALLOWED)?;
        let id = lease_id(&request.into_inner().lease_id)?;
        let guard = self.send_lease_lock(id).await;
        // A stalled upload must not consume the filesystem mutation lane
        // needed by suspend, restore, source staging, and cleanup.
        let permit = self
            .send_source_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("send readers busy; retry"))?;
        let source = self
            .off_runtime("open send lease", move || {
                let artifact =
                    trusted_paths::open_directory(&PathBuf::from(ROOT).join(id.to_string()))?;
                let source = btrfs_send_source::SendSource::open(&artifact, id)?;
                Ok((source, guard, permit))
            })
            .await?;
        #[cfg(target_os = "linux")]
        {
            use tokio::io::AsyncReadExt;
            let (read, write) =
                nix::unistd::pipe2(nix::fcntl::OFlag::O_CLOEXEC).map_err(artifact_fs::error)?;
            let mut reader = tokio::net::unix::pipe::Receiver::from_owned_fd(read)
                .map_err(artifact_fs::error)?;
            let producer = self.device_op_tasks.spawn_blocking(move || {
                let (source, _guard, _permit) = source;
                source.send(&File::from(write))
            });
            let cancel = self.pool.shutdown_token.child_token();
            let (tx, rx) = tokio::sync::mpsc::channel(4);
            self.device_op_tasks.spawn(async move {
                let mut total = 0_u64;
                // Forward only bytes that frame one valid stream: a send the
                // kernel restarted mid-pipe must fail the upload, not store it.
                let mut framing = framing::SendStreamFraming::new();
                let result = async {
                    loop {
                        let mut data = Vec::with_capacity(256 * 1024);
                        while data.len() < data.capacity() {
                        let count = tokio::select! {
                            biased;
                            _ = cancel.cancelled() => return Err(Status::cancelled("node-manager stopping")),
                            _ = tx.closed() => return Err(Status::cancelled("send consumer disconnected")),
                            count = reader.read_buf(&mut data) => count.map_err(artifact_fs::error)?,
                        };
                        if count == 0 { break; }
                        }
                        if data.is_empty() {
                            return framing.finish().map_err(framing_status);
                        }
                        framing.feed(&data).map_err(framing_status)?;
                        total += data.len() as u64;
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => return Err(Status::cancelled("node-manager stopping")),
                            sent = tx.send(Ok(BtrfsSendChunk { data, completed_bytes: None })) => {
                                sent.map_err(|_| Status::cancelled("send consumer disconnected"))?;
                            }
                        }
                    }
                }.await;
                // Close the only read end before joining a failed/cancelled
                // send. A blocked kernel write then fails with EPIPE.
                drop(reader);
                let sent = producer.await.map_err(|error| Status::internal(format!("send task: {error}"))).and_then(|result| result);
                let completion = result.and(sent).map(|()| BtrfsSendChunk { data: Vec::new(), completed_bytes: Some(total) });
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {},
                    _ = tx.send(completion) => {},
                }
            });
            Ok(Response::new(Box::pin(
                tokio_stream::wrappers::ReceiverStream::new(rx),
            )))
        }
        #[cfg(not(target_os = "linux"))]
        {
            drop(source);
            Err(Status::unimplemented("btrfs streaming requires Linux"))
        }
    }
}
