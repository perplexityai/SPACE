use super::*;
use pplx_space_volmgr_client::proto::{
    CopyObjectRequest, GetTransferJobRequest, GetTransferJobResponse, PullFileResponse,
    PushFileResponse, SubmitTransferResponse, TransferJob, TransferJobStatus,
};
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
enum CopyReply {
    Complete,
    Unsupported,
    Failed,
    Interrupted,
    Cancelled,
    Nonterminal,
    WrongJob,
}

struct Transfers {
    metadata: crate::snapshot_upload::SnapshotMeta,
    reply: CopyReply,
    calls: Mutex<Vec<String>>,
    disk_ready: std::sync::atomic::AtomicBool,
    published: Mutex<Option<crate::snapshot_upload::SnapshotMeta>>,
    job_id: String,
}

impl Transfers {
    fn pull(&self, request: PullFileRequest) -> Result<PullFileResponse, Status> {
        let artifact = Path::new(&request.local_path)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap();
        self.calls
            .lock()
            .map_err(|error| Status::internal(error.to_string()))?
            .push(format!("pull:{artifact}"));
        let bytes = if artifact == SnapshotArtifact::Metadata.file_name() {
            serde_json::to_vec(&self.metadata)
                .map_err(|error| Status::internal(error.to_string()))?
        } else {
            b"disk".to_vec()
        };
        std::fs::write(request.local_path, &bytes)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(PullFileResponse {
            bytes_downloaded: bytes.len() as u64,
        })
    }

    fn copy(&self, request: CopyObjectRequest) -> Result<SubmitTransferResponse, Status> {
        self.calls
            .lock()
            .map_err(|error| Status::internal(error.to_string()))?
            .push("copy".into());
        assert_eq!(
            request.subject_id,
            format!("sandbox:{}", self.metadata.sandbox_id)
        );
        let source = pplx_space_common_core::types::StorageUri::parse(&request.source_s3_uri)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let destination =
            pplx_space_common_core::types::StorageUri::parse(&request.destination_s3_uri)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let disk_name = if self.metadata.encrypted {
            "cow.luks"
        } else {
            "rootfs.btrfs-send"
        };
        assert_eq!(source.key_prefix, format!("snapshot/{disk_name}"));
        assert_eq!(
            destination.key_prefix,
            format!("snapshot/fork/operation/{disk_name}")
        );
        if matches!(self.reply, CopyReply::Unsupported) {
            return Err(Status::unimplemented("older volume-manager"));
        }
        Ok(SubmitTransferResponse {
            job_id: self.job_id.clone(),
        })
    }

    fn wait(&self, request: GetTransferJobRequest) -> Result<GetTransferJobResponse, Status> {
        self.calls
            .lock()
            .map_err(|error| Status::internal(error.to_string()))?
            .push("wait".into());
        assert_eq!(request.job_id, self.job_id);
        if matches!(self.reply, CopyReply::Interrupted) {
            return Err(Status::unavailable(
                "connection lost while copy remains active",
            ));
        }
        let status = match self.reply {
            CopyReply::Failed => TransferJobStatus::Failed,
            CopyReply::Cancelled => TransferJobStatus::Cancelled,
            CopyReply::Nonterminal => TransferJobStatus::Running,
            _ => {
                self.disk_ready.store(true, Ordering::SeqCst);
                TransferJobStatus::Completed
            }
        };
        Ok(GetTransferJobResponse {
            job: Some(TransferJob {
                job_id: if matches!(self.reply, CopyReply::WrongJob) {
                    Uuid::now_v7().to_string()
                } else {
                    self.job_id.clone()
                },
                status: status as i32,
                // Stored bytes are deliberately different from logical disk bytes.
                bytes_transferred: 2,
                ..Default::default()
            }),
        })
    }

    fn push(&self, request: PushFileRequest) -> Result<PushFileResponse, Status> {
        let artifact = Path::new(&request.local_path)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap();
        self.calls
            .lock()
            .map_err(|error| Status::internal(error.to_string()))?
            .push(format!("push:{artifact}"));
        let bytes = std::fs::read(&request.local_path)
            .map_err(|error| Status::internal(error.to_string()))?;
        if artifact == SnapshotArtifact::Metadata.file_name() {
            assert!(
                self.disk_ready.load(Ordering::SeqCst),
                "metadata published before disk durability"
            );
            *self
                .published
                .lock()
                .map_err(|error| Status::internal(error.to_string()))? = Some(
                serde_json::from_slice(&bytes)
                    .map_err(|error| Status::internal(error.to_string()))?,
            );
        } else {
            assert_eq!(bytes, b"disk");
            self.disk_ready.store(true, Ordering::SeqCst);
        }
        Ok(PushFileResponse {
            bytes_uploaded: bytes.len() as u64,
        })
    }
}

struct Unary<F>(F);
impl<Req, Resp, F> tonic::server::UnaryService<Req> for Unary<F>
where
    F: Fn(Req) -> Result<Resp, Status>,
{
    type Response = Resp;
    type Future = std::future::Ready<Result<Response<Resp>, Status>>;
    fn call(&mut self, request: tonic::Request<Req>) -> Self::Future {
        std::future::ready((self.0)(request.into_inner()).map(Response::new))
    }
}

struct Peer {
    client: pplx_space_volmgr_client::ConnectedVolumeManagerClient,
    transfers: Arc<Transfers>,
    shutdown: CancellationToken,
    _sockets: tempfile::TempDir,
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

async fn peer(metadata: crate::snapshot_upload::SnapshotMeta, reply: CopyReply) -> Peer {
    let sockets = tempfile::tempdir().unwrap();
    let socket = sockets.path().join("volume-manager.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let transfers = Arc::new(Transfers {
        metadata,
        reply,
        calls: Mutex::new(Vec::new()),
        disk_ready: false.into(),
        published: Mutex::new(None),
        job_id: Uuid::now_v7().to_string(),
    });
    let handler = Arc::clone(&transfers);
    let rpc = tower::service_fn(move |request: http::Request<tonic::body::Body>| {
        let handler = Arc::clone(&handler);
        async move {
            let method = request.uri().path().rsplit('/').next().unwrap();
            let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
            let response = match method {
                "PullFile" => grpc.unary(Unary(move |r| handler.pull(r)), request).await,
                "PushFile" => {
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(Unary(move |r| handler.push(r)), request)
                        .await
                }
                "SubmitCopyObject" => {
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(Unary(move |r| handler.copy(r)), request)
                        .await
                }
                "WaitTransferJob" => {
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(Unary(move |r| handler.wait(r)), request)
                        .await
                }
                _ => Status::unimplemented("unexpected transfer RPC").into_http(),
            };
            Ok::<_, std::convert::Infallible>(response)
        }
    });
    let shutdown = CancellationToken::new();
    tokio::spawn(
        tonic::transport::Server::builder().serve_with_incoming_shutdown(
            rpc,
            tokio_stream::wrappers::UnixListenerStream::new(listener),
            shutdown.clone().cancelled_owned(),
        ),
    );
    let client = pplx_space_volmgr_client::connect(socket.to_str().unwrap())
        .await
        .unwrap();
    Peer {
        client,
        transfers,
        shutdown,
        _sockets: sockets,
    }
}

async fn run(
    peer: &mut Peer,
    source: SandboxId,
    destination: SandboxId,
) -> Result<crate::proto::CreateSnapshotResponse, Status> {
    let source_config = crate::snapshot_upload::SnapshotUploadConfig::from_uri(
        "s3://example-artifacts/snapshot?region=us-east-1",
    )
    .unwrap();
    let destination_config = crate::snapshot_upload::SnapshotUploadConfig::from_uri(
        "s3://example-artifacts/snapshot/fork/operation?region=us-east-1",
    )
    .unwrap();
    let staging = tempfile::tempdir().unwrap();
    let requires_disk = peer.transfers.metadata.rootfs_size_bytes.is_none()
        || matches!(
            peer.transfers.reply,
            CopyReply::Unsupported | CopyReply::Failed
        );
    copy_artifact(
        &mut peer.client,
        CopyObjectsRequest {
            operation_id: Uuid::now_v7(),
            source_sandbox_id: source,
            target_sandbox_id: destination,
            source: &source_config,
            destination: &destination_config,
            destination_storage_uri:
                "s3://example-artifacts/snapshot/fork/operation?region=us-east-1".into(),
            destination_origin: ArtifactOrigin::Fork,
        },
        staging.path(),
        move || {
            assert!(
                requires_disk,
                "server-side copy must not require bulk disk staging admission"
            );
            Ok(CacheDiskMeasurement {
                status: pplx_space_common_core::host::disk::DiskStatus {
                    total_bytes: 1 << 40,
                    free_bytes: 1 << 39,
                    available_bytes: 1 << 39,
                },
                btrfs: None,
            })
        },
    )
    .await
}

#[tokio::test]
async fn server_copy_publishes_only_disk_metadata_after_durable_copy() {
    for encrypted in [false, true] {
        let source = "019f8000-0000-7000-8000-000000000001".parse().unwrap();
        let destination = "019f8000-0000-7000-8000-000000000002".parse().unwrap();
        let mut metadata = suspended_metadata(source);
        metadata.rootfs_size_bytes = Some(4);
        metadata.rootfs_diff_format =
            (!encrypted).then(|| crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1.into());
        metadata.encrypted = encrypted;
        let mut peer = peer(metadata, CopyReply::Complete).await;
        let response = run(&mut peer, source, destination).await.unwrap();
        assert_eq!(
            *peer.transfers.calls.lock().unwrap(),
            ["pull:metadata.json", "copy", "wait", "push:metadata.json"]
        );
        let published = peer.transfers.published.lock().unwrap();
        let metadata = published.as_ref().unwrap();
        assert_eq!(metadata.sandbox_id, destination.to_string());
        assert_eq!(metadata.size_bytes, 4);
        assert_eq!(metadata.rootfs_size_bytes, Some(4));
        assert_eq!(metadata.artifact_kind, Some(ArtifactKind::Disk));
        assert_eq!(metadata.local_artifact_id, None);
        assert_eq!(metadata.rootfs_send_source_id, None);
        if encrypted {
            assert_eq!(metadata.rootfs_encryption_source, Some(source));
            insta::assert_snapshot!(
                "fork_encrypted_disk_metadata",
                serde_json::to_string_pretty(metadata).unwrap()
            );
        } else {
            insta::assert_snapshot!(
                "fork_disk_metadata",
                serde_json::to_string_pretty(metadata).unwrap()
            );
        }
        assert_eq!(
            response.published_durable_artifact.unwrap().bytes_published,
            4 + serde_json::to_vec_pretty(metadata).unwrap().len() as u64
        );
    }
}

#[tokio::test]
async fn legacy_and_unsupported_copies_keep_the_existing_disk_transfer() {
    for (size, reply) in [
        (None, CopyReply::Complete),
        (Some(4), CopyReply::Unsupported),
        (Some(4), CopyReply::Failed),
    ] {
        let source = SandboxId::new();
        let mut metadata = suspended_metadata(source);
        metadata.rootfs_size_bytes = size;
        metadata.rootfs_diff_format =
            Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1.into());
        let mut peer = peer(metadata, reply).await;
        run(&mut peer, source, SandboxId::new()).await.unwrap();
        let calls = peer.transfers.calls.lock().unwrap();
        assert_eq!(
            calls.iter().filter(|call| *call == "copy").count(),
            usize::from(size.is_some())
        );
        assert!(calls.iter().any(|call| call == "pull:rootfs.btrfs-send"));
        assert!(calls.iter().any(|call| call == "push:rootfs.btrfs-send"));
        assert_eq!(calls.last().unwrap(), "push:metadata.json");
        assert_eq!(
            peer.transfers
                .published
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .rootfs_size_bytes,
            Some(4)
        );
    }
}

#[tokio::test]
async fn invalid_sources_and_interrupted_copy_waits_cannot_publish_metadata() {
    let source = SandboxId::new();
    let wrong_owner = suspended_metadata(SandboxId::new());
    let mut untyped = suspended_metadata(source);
    untyped.artifact_kind = None;
    for mut metadata in [wrong_owner, untyped] {
        metadata.rootfs_size_bytes = Some(4);
        let mut peer = peer(metadata, CopyReply::Interrupted).await;
        assert_eq!(
            run(&mut peer, source, SandboxId::new())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            *peer.transfers.calls.lock().unwrap(),
            ["pull:metadata.json"]
        );
        assert!(peer.transfers.published.lock().unwrap().is_none());
    }
    for (reply, expected_code) in [
        (CopyReply::Interrupted, tonic::Code::Unavailable),
        (CopyReply::Cancelled, tonic::Code::Cancelled),
        (CopyReply::Nonterminal, tonic::Code::Unavailable),
        (CopyReply::WrongJob, tonic::Code::Internal),
    ] {
        let mut metadata = suspended_metadata(source);
        metadata.rootfs_size_bytes = Some(4);
        metadata.rootfs_diff_format =
            Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1.into());
        let mut peer = peer(metadata, reply).await;
        assert_eq!(
            run(&mut peer, source, SandboxId::new())
                .await
                .unwrap_err()
                .code(),
            expected_code
        );
        let calls = peer.transfers.calls.lock().unwrap();
        assert_eq!(*calls, ["pull:metadata.json", "copy", "wait"]);
        assert!(peer.transfers.published.lock().unwrap().is_none());
    }
}
