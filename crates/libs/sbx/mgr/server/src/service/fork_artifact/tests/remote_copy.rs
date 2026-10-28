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
    jobs: Mutex<std::collections::HashMap<String, TransferJob>>,
    omit_upload_receipt: std::sync::atomic::AtomicBool,
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

    fn submit_pull(&self, request: PullFileRequest) -> Result<SubmitTransferResponse, Status> {
        let expected = request.expected_integrity.clone();
        let path = request.local_path.clone();
        self.pull(request)?;
        let receipt = identity(&std::fs::read(path).map_err(|e| Status::internal(e.to_string()))?);
        let matches = expected
            .as_ref()
            .is_none_or(|expected| expected == &receipt);
        let job_id = JobId::new().to_string();
        self.jobs
            .lock()
            .map_err(|e| Status::internal(e.to_string()))?
            .insert(
                job_id.clone(),
                TransferJob {
                    job_id: job_id.clone(),
                    kind: "pull".into(),
                    status: if matches {
                        TransferJobStatus::Completed
                    } else {
                        TransferJobStatus::Failed
                    } as i32,
                    bytes_transferred: receipt.size_bytes,
                    integrity: (matches && expected.is_some()).then_some(receipt),
                    error: (!matches).then(|| "source content does not match the catalog".into()),
                    ..Default::default()
                },
            );
        Ok(SubmitTransferResponse { job_id })
    }

    fn submit_push(&self, request: PushFileRequest) -> Result<SubmitTransferResponse, Status> {
        let receipt = identity(
            &std::fs::read(&request.local_path).map_err(|e| Status::internal(e.to_string()))?,
        );
        self.push(request)?;
        let job_id = JobId::new().to_string();
        self.jobs
            .lock()
            .map_err(|e| Status::internal(e.to_string()))?
            .insert(
                job_id.clone(),
                TransferJob {
                    job_id: job_id.clone(),
                    kind: "push".into(),
                    status: TransferJobStatus::Completed as i32,
                    bytes_transferred: receipt.size_bytes,
                    integrity: (!self.omit_upload_receipt.load(Ordering::SeqCst))
                        .then_some(receipt),
                    ..Default::default()
                },
            );
        Ok(SubmitTransferResponse { job_id })
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
        assert_eq!(source.key_prefix, "snapshot/rootfs.btrfs-send");
        assert_eq!(
            destination.key_prefix,
            "snapshot/fork/operation/rootfs.btrfs-send"
        );
        if matches!(self.reply, CopyReply::Unsupported) {
            return Err(Status::unimplemented("older volume-manager"));
        }
        Ok(SubmitTransferResponse {
            job_id: self.job_id.clone(),
        })
    }

    fn wait(&self, request: GetTransferJobRequest) -> Result<GetTransferJobResponse, Status> {
        if let Some(job) = self
            .jobs
            .lock()
            .map_err(|e| Status::internal(e.to_string()))?
            .get(&request.job_id)
            .cloned()
        {
            return Ok(GetTransferJobResponse { job: Some(job) });
        }
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
        jobs: Mutex::new(Default::default()),
        omit_upload_receipt: false.into(),
    });
    let handler = Arc::clone(&transfers);
    let rpc = tower::service_fn(move |request: http::Request<tonic::body::Body>| {
        let handler = Arc::clone(&handler);
        async move {
            let method = request.uri().path().rsplit('/').next().unwrap();
            let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
            let response = match method {
                "PullFile" => grpc.unary(Unary(move |r| handler.pull(r)), request).await,
                "SubmitPullFile" => {
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(Unary(move |r| handler.submit_pull(r)), request)
                        .await
                }
                "SubmitPushFile" => {
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(Unary(move |r| handler.submit_push(r)), request)
                        .await
                }
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
    run_with_manifest(peer, source, destination, &[]).await
}

async fn run_with_manifest(
    peer: &mut Peer,
    source: SandboxId,
    destination: SandboxId,
    manifest: &[crate::proto::SnapshotArtifactDigest],
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
            source_artifact_digests: manifest,
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
    let source = "019f8000-0000-7000-8000-000000000001".parse().unwrap();
    let destination = "019f8000-0000-7000-8000-000000000002".parse().unwrap();
    let mut metadata = suspended_metadata(source);
    metadata.rootfs_size_bytes = Some(4);
    metadata.rootfs_diff_format =
        Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1.into());
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
    insta::assert_snapshot!(
        "fork_disk_metadata",
        serde_json::to_string_pretty(metadata).unwrap()
    );
    assert_eq!(
        response.published_durable_artifact.unwrap().bytes_published,
        4 + serde_json::to_vec_pretty(metadata).unwrap().len() as u64
    );
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
        peer.transfers
            .omit_upload_receipt
            .store(true, Ordering::SeqCst);
        let response = run(&mut peer, source, SandboxId::new()).await.unwrap();
        assert!(
            response
                .published_durable_artifact
                .unwrap()
                .artifact_digests
                .is_empty()
        );
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
    let mut encrypted = suspended_metadata(source);
    encrypted.encrypted = true;
    let mut untyped = suspended_metadata(source);
    untyped.artifact_kind = None;
    for mut metadata in [wrong_owner, encrypted, untyped] {
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

fn identity(bytes: &[u8]) -> pplx_space_volmgr_client::proto::ContentIntegrity {
    let hash = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes);
    let mut digest = [0; 32];
    digest.copy_from_slice(hash.as_ref());
    pplx_space_volmgr_client::proto::ContentIntegrity {
        digest: pplx_space_common_core::types::Digest::sha256(digest).to_string(),
        size_bytes: bytes.len() as u64,
    }
}

fn source_manifest(
    metadata: &crate::snapshot_upload::SnapshotMeta,
) -> Vec<crate::proto::SnapshotArtifactDigest> {
    crate::snapshot_upload::upload_artifacts_for_meta(metadata)
        .into_iter()
        .map(|artifact| {
            let identity = match artifact.kind {
                SnapshotArtifact::Metadata => identity(&serde_json::to_vec(metadata).unwrap()),
                SnapshotArtifact::Rootfs | SnapshotArtifact::RootfsBtrfsSend => identity(b"disk"),
                _ => identity(b"unused"),
            };
            crate::proto::SnapshotArtifactDigest {
                artifact: artifact.kind.file_name().into(),
                digest: identity.digest,
                size_bytes: identity.size_bytes,
            }
        })
        .collect()
}

#[tokio::test]
async fn authenticated_copies_preserve_disk_identity_and_publish_metadata_receipts() {
    for reply in [
        CopyReply::Complete,
        CopyReply::Unsupported,
        CopyReply::Failed,
    ] {
        let source = SandboxId::new();
        let mut metadata = suspended_metadata(source);
        metadata.rootfs_size_bytes = Some(4);
        metadata.rootfs_diff_format =
            Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1.into());
        let manifest = source_manifest(&metadata);
        let mut peer = peer(metadata, reply).await;
        let target = SandboxId::new();
        // Retry uses the same trusted source and yields the same manifest.
        let first = run_with_manifest(&mut peer, source, target, &manifest)
            .await
            .unwrap();
        let second = run_with_manifest(&mut peer, source, target, &manifest)
            .await
            .unwrap();
        let output = first.published_durable_artifact.unwrap().artifact_digests;
        assert_eq!(
            second.published_durable_artifact.unwrap().artifact_digests,
            output
        );
        assert_eq!(output.len(), 2);
        assert_eq!(
            output[0],
            *manifest
                .iter()
                .find(|entry| entry.artifact == "rootfs.btrfs-send")
                .unwrap()
        );
        let uploaded = peer.transfers.published.lock().unwrap();
        let receipt = identity(&serde_json::to_vec_pretty(uploaded.as_ref().unwrap()).unwrap());
        assert_eq!(output[1].digest, receipt.digest);
        assert_eq!(output[1].size_bytes, receipt.size_bytes);
        assert_ne!(
            output[1].digest,
            manifest
                .iter()
                .find(|entry| entry.artifact == "metadata.json")
                .unwrap()
                .digest
        );
    }
}

#[tokio::test]
async fn authenticated_source_failures_never_fall_back_to_legacy_publication() {
    for problem in ["metadata", "rootfs", "partial", "missing_upload_receipt"] {
        let source = SandboxId::new();
        let mut metadata = suspended_metadata(source);
        metadata.rootfs_size_bytes = Some(4);
        metadata.rootfs_diff_format =
            Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1.into());
        let mut manifest = source_manifest(&metadata);
        if problem == "partial" {
            manifest.pop();
        }
        if matches!(problem, "metadata" | "rootfs") {
            let name = if problem == "metadata" {
                "metadata.json"
            } else {
                "rootfs.btrfs-send"
            };
            manifest
                .iter_mut()
                .find(|entry| entry.artifact == name)
                .unwrap()
                .digest = identity(b"other").digest;
        }
        let mut peer = peer(metadata, CopyReply::Unsupported).await;
        peer.transfers
            .omit_upload_receipt
            .store(problem == "missing_upload_receipt", Ordering::SeqCst);
        let error = run_with_manifest(&mut peer, source, SandboxId::new(), &manifest)
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            if problem == "missing_upload_receipt" {
                tonic::Code::Unavailable
            } else {
                tonic::Code::FailedPrecondition
            }
        );
        if problem != "missing_upload_receipt" {
            assert!(peer.transfers.published.lock().unwrap().is_none());
        }
    }
}
