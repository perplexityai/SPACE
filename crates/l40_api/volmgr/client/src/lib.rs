use pplx_space_common_core::types::JobId;

pub mod proto {
    tonic::include_proto!("space.volume.manager.v1");
}

/// Convert the domain snapshot identity to its volume-manager wire value.
pub const fn snapshot_artifact_to_proto(
    artifact: pplx_space_common_core::types::SnapshotArtifact,
) -> proto::SnapshotArtifact {
    use pplx_space_common_core::types::SnapshotArtifact as CoreArtifact;

    match artifact {
        CoreArtifact::Rootfs => proto::SnapshotArtifact::Rootfs,
        CoreArtifact::RootfsBtrfsSend => proto::SnapshotArtifact::RootfsBtrfsSend,
        CoreArtifact::Memory => proto::SnapshotArtifact::Memory,
        CoreArtifact::VmState => proto::SnapshotArtifact::VmState,
        CoreArtifact::EncryptedSnapshotVolume => proto::SnapshotArtifact::EncryptedSnapshotVolume,
        CoreArtifact::EncryptedRootfsCow => proto::SnapshotArtifact::EncryptedRootfsCow,
        CoreArtifact::Metadata => proto::SnapshotArtifact::Metadata,
    }
}

/// Convert a volume-manager wire value to the domain snapshot identity.
/// `UNSPECIFIED` represents legacy callers or non-snapshot transfers.
pub const fn snapshot_artifact_from_proto(
    artifact: proto::SnapshotArtifact,
) -> Option<pplx_space_common_core::types::SnapshotArtifact> {
    use pplx_space_common_core::types::SnapshotArtifact as CoreArtifact;

    match artifact {
        proto::SnapshotArtifact::Unspecified => None,
        proto::SnapshotArtifact::Rootfs => Some(CoreArtifact::Rootfs),
        proto::SnapshotArtifact::RootfsBtrfsSend => Some(CoreArtifact::RootfsBtrfsSend),
        proto::SnapshotArtifact::Memory => Some(CoreArtifact::Memory),
        proto::SnapshotArtifact::VmState => Some(CoreArtifact::VmState),
        proto::SnapshotArtifact::EncryptedSnapshotVolume => {
            Some(CoreArtifact::EncryptedSnapshotVolume)
        }
        proto::SnapshotArtifact::EncryptedRootfsCow => Some(CoreArtifact::EncryptedRootfsCow),
        proto::SnapshotArtifact::Metadata => Some(CoreArtifact::Metadata),
    }
}

#[cfg(test)]
mod proto_wire_tests;

use std::ffi::OsString;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use pplx_space_common_grpc::GrpcRetryPolicy;
use pplx_space_common_grpc::{
    GrpcClientTrace, trace_grpc_client_transport as wrap_grpc_client_channel,
};
use proto::{
    ExistingFilePolicy, GetTransferJobRequest, GetTransferJobResponse, PullFileRequest,
    SubmitTransferResponse, TransferJobStatus,
};

pub use proto::volume_manager_client::VolumeManagerClient;
pub type ConnectedVolumeManagerClient =
    VolumeManagerClient<GrpcClientTrace<tonic::transport::Channel>>;

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("invalid gRPC endpoint URI: {0}")]
    InvalidUri(#[from] tonic::codegen::http::uri::InvalidUri),
    #[error("volume-manager transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("volume-manager connect failed: {0}")]
    Connect(#[from] pplx_space_common_grpc::UdsConnectError),
}

/// Connect to the volume manager gRPC service over Unix socket.
pub async fn connect(socket_path: &str) -> Result<ConnectedVolumeManagerClient, ConnectError> {
    let channel = pplx_space_common_grpc::connect_uds_channel_with_retry(
        "volume-manager",
        std::path::Path::new(socket_path),
        pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY,
    )
    .await?;
    Ok(VolumeManagerClient::new(wrap_grpc_client_channel(channel)))
}

pub const VOLMGR_SOCKET: &str = pplx_space_common_core::host::linux::paths::VOLUME_MANAGER_SOCKET;
pub const RESTORE_PRIORITY: i32 = 100;
pub const VERIFIED_REPLICA_PROVENANCE_VERSION: u32 = 1;
const ENSURE_FILE_REPLICA_RETRY_POLICY: GrpcRetryPolicy = GrpcRetryPolicy {
    max_retries: usize::MAX,
    initial_delay: Duration::from_millis(100),
    backoff_multiplier: 2.0,
    max_delay: Duration::from_secs(5),
    max_elapsed: Duration::from_secs(10_800),
    jitter_factor: 0.2,
    max_server_retry_after: Duration::ZERO,
};

/// Hidden sibling written by volume-manager for a verified file replica.
/// Callers that atomically rename a materialized replica must move this
/// sidecar with it or the next ensure operation will download the file again.
pub fn verified_replica_provenance_path(local_path: impl AsRef<Path>) -> PathBuf {
    let local_path = local_path.as_ref();
    let mut path = local_path.to_path_buf();
    let mut file_name = OsString::from(".");
    file_name.push(
        local_path
            .file_name()
            .map(|name| name.to_os_string())
            .unwrap_or_else(|| OsString::from("replica")),
    );
    file_name.push(".volmgr-replica.json");
    path.set_file_name(file_name);
    path
}

/// Stable on-disk record proving that volume-manager materialized a file from
/// a specific object-store source and verified the resulting local bytes.
///
/// This wire type is shared by volume-manager and consumers that must
/// attribute surviving files after a process restart. Its fields are public
/// because the JSON record is itself the cross-process contract.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerifiedReplicaProvenance {
    pub version: u32,
    pub subject_id: String,
    pub s3_uri: String,
    pub raw: bool,
    pub file_mode: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_artifact: Option<pplx_space_common_core::types::SnapshotArtifact>,
    pub object_size: u64,
    pub object_etag: Option<String>,
    pub local_size: u64,
}

/// Typed source identity recovered from a verified-replica sidecar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedReplicaSource {
    pub subject_id: pplx_space_common_core::types::SubjectId,
    pub storage_uri: pplx_space_common_core::types::ExactStorageUri,
}

#[derive(Debug, thiserror::Error)]
pub enum ReadVerifiedReplicaProvenanceError {
    #[error("read verified replica provenance {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parse verified replica provenance {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("verified replica provenance {path} has unsupported version {version}")]
    UnsupportedVersion { path: PathBuf, version: u32 },
    #[error("verified replica provenance {path} has invalid subject: {source}")]
    Subject {
        path: PathBuf,
        #[source]
        source: pplx_space_common_core::types::SubjectIdParseError,
    },
    #[error("verified replica provenance {path} has invalid storage URI: {source}")]
    StorageUri {
        path: PathBuf,
        #[source]
        source: pplx_space_common_core::types::StorageUriParseError,
    },
}

impl ReadVerifiedReplicaProvenanceError {
    pub fn io_error_kind(&self) -> Option<std::io::ErrorKind> {
        match self {
            Self::Read { source, .. } => Some(source.kind()),
            _ => None,
        }
    }
}

/// Read and validate volume-manager's persisted provenance contract.
pub fn read_verified_replica_provenance(
    local_path: impl AsRef<Path>,
) -> Result<VerifiedReplicaSource, ReadVerifiedReplicaProvenanceError> {
    let provenance_path = verified_replica_provenance_path(local_path);
    let bytes = std::fs::read(&provenance_path).map_err(|source| {
        ReadVerifiedReplicaProvenanceError::Read {
            path: provenance_path.clone(),
            source,
        }
    })?;
    let provenance: VerifiedReplicaProvenance =
        serde_json::from_slice(&bytes).map_err(|source| {
            ReadVerifiedReplicaProvenanceError::Parse {
                path: provenance_path.clone(),
                source,
            }
        })?;
    if provenance.version != VERIFIED_REPLICA_PROVENANCE_VERSION {
        return Err(ReadVerifiedReplicaProvenanceError::UnsupportedVersion {
            path: provenance_path,
            version: provenance.version,
        });
    }
    let subject_id = provenance.subject_id.parse().map_err(|source| {
        ReadVerifiedReplicaProvenanceError::Subject {
            path: provenance_path.clone(),
            source,
        }
    })?;
    let storage_uri = pplx_space_common_core::types::ExactStorageUri::parse(&provenance.s3_uri)
        .map_err(|source| ReadVerifiedReplicaProvenanceError::StorageUri {
            path: provenance_path,
            source,
        })?;
    Ok(VerifiedReplicaSource {
        subject_id,
        storage_uri,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnsureFileReplicaWaitConfig {
    pub max_total: Duration,
}

impl Default for EnsureFileReplicaWaitConfig {
    fn default() -> Self {
        Self {
            max_total: Duration::from_secs(10_800),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsureFileReplicaOutput {
    pub job_id: JobId,
    pub bytes_present: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum EnsureFileReplicaError {
    #[error("invalid job identity in volume-manager response: {0}")]
    InvalidJobId(#[from] pplx_space_common_core::types::AuthIdParseError),
    #[error("submit ensure-file-replica job: {0}")]
    Submit(#[source] tonic::Status),
    #[error("wait for ensure-file-replica job: {0}")]
    Wait(#[source] tonic::Status),
    #[error("ensure-file-replica job {job_id} returned an empty job response")]
    EmptyJob { job_id: JobId },
    #[error("ensure-file-replica job {job_id} failed permanently: {error}")]
    Failed { job_id: JobId, error: String },
    #[error("ensure-file-replica job {job_id} was cancelled")]
    Cancelled { job_id: JobId },
    #[error(
        "ensure-file-replica job {job_id} exceeded {}s wait deadline",
        max_total.as_secs()
    )]
    DeadlineExceeded { job_id: JobId, max_total: Duration },
    #[error("ensure-file-replica job {job_id} returned unknown status code {status}")]
    UnknownStatus { job_id: JobId, status: i32 },
}

pub struct EnsureFileReplicaFuture {
    inner: Pin<
        Box<dyn Future<Output = Result<EnsureFileReplicaOutput, EnsureFileReplicaError>> + Send>,
    >,
}

impl Future for EnsureFileReplicaFuture {
    type Output = Result<EnsureFileReplicaOutput, EnsureFileReplicaError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().inner.as_mut().poll(cx)
    }
}

/// Submit an ensure-file-replica job and wait for its terminal status.
/// Queue-admission rejection is retried within the configured total wait.
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use pplx_space_volmgr_client::{
///     VOLMGR_SOCKET, ensure_file_replica,
///     proto::PullFileRequest,
/// };
///
/// let client = pplx_space_volmgr_client::connect(VOLMGR_SOCKET).await?;
/// let output = ensure_file_replica(
///     client,
///     PullFileRequest {
///         subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".into(),
///         s3_uri: "s3://space-snapshots/snapshots/sbx/snapshot.luks?region=us-east-1".into(),
///         local_path: "/space/cache/sandboxes/sbx/snapshot/snapshot.luks".into(),
///         raw: false,
///         file_mode: None,
///         ..Default::default()
///     },
/// )
/// .await?;
/// println!("{} bytes present via {}", output.bytes_present, output.job_id);
/// # Ok(())
/// # }
/// ```
pub fn ensure_file_replica(
    client: ConnectedVolumeManagerClient,
    request: PullFileRequest,
) -> EnsureFileReplicaFuture {
    ensure_file_replica_with_config(client, request, EnsureFileReplicaWaitConfig::default())
}

pub fn ensure_file_replica_with_config(
    client: ConnectedVolumeManagerClient,
    request: PullFileRequest,
    config: EnsureFileReplicaWaitConfig,
) -> EnsureFileReplicaFuture {
    EnsureFileReplicaFuture {
        inner: Box::pin(run_ensure_file_replica(client, request, config)),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CancelTransferScopeError {
    #[error("invalid job identity in volume-manager response: {0}")]
    InvalidJobId(#[from] pplx_space_common_core::types::AuthIdParseError),
    #[error("cancel transfer scope RPC failed: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("transfer job {job_id} remained non-terminal with status {status}")]
    NonTerminal { job_id: JobId, status: i32 },
}

/// Cancel currently-submitted pulls for `scope` and await their writer barrier.
///
/// The producer must be quiesced before calling this function. Cancellation
/// does not permanently close the scope against future submissions.
pub async fn cancel_transfer_scope(
    mut client: ConnectedVolumeManagerClient,
    scope: pplx_space_common_core::types::TransferScopeId,
) -> Result<Vec<proto::TransferJob>, CancelTransferScopeError> {
    let response = client
        .cancel_transfer(proto::CancelTransferRequest {
            selector: Some(proto::cancel_transfer_request::Selector::TransferScopeId(
                scope.to_string(),
            )),
        })
        .await?
        .into_inner();
    for job in &response.jobs {
        let terminal = matches!(
            proto::TransferJobStatus::try_from(job.status).ok(),
            Some(
                proto::TransferJobStatus::Completed
                    | proto::TransferJobStatus::Failed
                    | proto::TransferJobStatus::Cancelled
            )
        );
        if !terminal {
            return Err(CancelTransferScopeError::NonTerminal {
                job_id: job.job_id.parse()?,
                status: job.status,
            });
        }
    }
    Ok(response.jobs)
}

async fn run_ensure_file_replica(
    client: ConnectedVolumeManagerClient,
    request: PullFileRequest,
    config: EnsureFileReplicaWaitConfig,
) -> Result<EnsureFileReplicaOutput, EnsureFileReplicaError> {
    let submit_client = client.clone();
    let wait_client = client;
    run_ensure_file_replica_with(
        request,
        config,
        move |request| {
            let mut client = submit_client.clone();
            async move { client.submit_pull_file(request).await }
        },
        move |request| {
            let mut client = wait_client.clone();
            async move { client.wait_transfer_job(request).await }
        },
    )
    .await
}

async fn run_ensure_file_replica_with<Submit, SubmitFuture, Wait, WaitFuture>(
    mut request: PullFileRequest,
    config: EnsureFileReplicaWaitConfig,
    submit: Submit,
    wait: Wait,
) -> Result<EnsureFileReplicaOutput, EnsureFileReplicaError>
where
    Submit: FnMut(PullFileRequest) -> SubmitFuture,
    SubmitFuture: Future<Output = Result<tonic::Response<SubmitTransferResponse>, tonic::Status>>,
    Wait: FnMut(GetTransferJobRequest) -> WaitFuture,
    WaitFuture: Future<Output = Result<tonic::Response<GetTransferJobResponse>, tonic::Status>>,
{
    request.existing_file_policy = ExistingFilePolicy::VerifiedReplica as i32;
    let started_at = tokio::time::Instant::now();
    let response = submit_pull_file_with_retry(request, started_at, config.max_total, submit)
        .await
        .map_err(EnsureFileReplicaError::Submit)?;
    let job_id = response.job_id.parse::<JobId>()?;
    let response = wait_transfer_job_with_retry(&job_id, started_at, config.max_total, wait)
        .await
        .map_err(|error| match error {
            WaitTransferJobError::Rpc(status) => EnsureFileReplicaError::Wait(status),
            WaitTransferJobError::DeadlineExceeded => EnsureFileReplicaError::DeadlineExceeded {
                job_id,
                max_total: config.max_total,
            },
        })?;
    let job = response
        .job
        .ok_or_else(|| EnsureFileReplicaError::EmptyJob { job_id })?;

    match TransferJobStatus::try_from(job.status).ok() {
        Some(TransferJobStatus::Completed) => Ok(EnsureFileReplicaOutput {
            job_id,
            bytes_present: job.bytes_transferred,
        }),
        Some(TransferJobStatus::Failed) => Err(EnsureFileReplicaError::Failed {
            job_id,
            error: job.error.unwrap_or_else(|| "unknown error".to_string()),
        }),
        Some(TransferJobStatus::Cancelled) => Err(EnsureFileReplicaError::Cancelled { job_id }),
        Some(
            TransferJobStatus::Pending
            | TransferJobStatus::Running
            | TransferJobStatus::Cancelling
            | TransferJobStatus::Unspecified,
        )
        | None => Err(EnsureFileReplicaError::UnknownStatus {
            job_id,
            status: job.status,
        }),
    }
}

async fn submit_pull_file_with_retry<F, Fut>(
    request: PullFileRequest,
    started_at: tokio::time::Instant,
    max_total: Duration,
    mut submit: F,
) -> Result<SubmitTransferResponse, tonic::Status>
where
    F: FnMut(PullFileRequest) -> Fut,
    Fut: Future<Output = Result<tonic::Response<SubmitTransferResponse>, tonic::Status>>,
{
    let mut retry_index = 0;
    loop {
        match submit(request.clone()).await {
            Ok(response) => return Ok(response.into_inner()),
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::ResourceExhausted | tonic::Code::Unavailable
                ) =>
            {
                let Some(delay) = ENSURE_FILE_REPLICA_RETRY_POLICY.backoff_delay(retry_index)
                else {
                    return Err(status);
                };
                let remaining = max_total.saturating_sub(started_at.elapsed());
                if delay >= remaining {
                    return Err(status);
                }
                retry_index += 1;
                tokio::time::sleep(delay).await;
            }
            Err(status) => return Err(status),
        }
    }
}

#[derive(Debug)]
enum WaitTransferJobError {
    Rpc(tonic::Status),
    DeadlineExceeded,
}

async fn wait_transfer_job_with_retry<F, Fut>(
    job_id: &JobId,
    started_at: tokio::time::Instant,
    max_total: Duration,
    mut wait: F,
) -> Result<GetTransferJobResponse, WaitTransferJobError>
where
    F: FnMut(GetTransferJobRequest) -> Fut,
    Fut: Future<Output = Result<tonic::Response<GetTransferJobResponse>, tonic::Status>>,
{
    let mut retry_index = 0;
    loop {
        let remaining = max_total.saturating_sub(started_at.elapsed());
        if remaining.is_zero() {
            return Err(WaitTransferJobError::DeadlineExceeded);
        }
        let response = tokio::time::timeout(
            remaining,
            wait(GetTransferJobRequest {
                job_id: job_id.to_string(),
            }),
        )
        .await
        .map_err(|_| WaitTransferJobError::DeadlineExceeded)?;
        match response {
            Ok(response) => return Ok(response.into_inner()),
            Err(status) if pplx_space_common_grpc::should_reconnect_status(&status) => {
                let Some(delay) = ENSURE_FILE_REPLICA_RETRY_POLICY.backoff_delay(retry_index)
                else {
                    return Err(WaitTransferJobError::Rpc(status));
                };
                let remaining = max_total.saturating_sub(started_at.elapsed());
                if delay >= remaining {
                    return Err(WaitTransferJobError::DeadlineExceeded);
                }
                retry_index += 1;
                tokio::time::sleep(delay).await;
            }
            Err(status) => return Err(WaitTransferJobError::Rpc(status)),
        }
    }
}

#[cfg(test)]
mod wait_transfer_job_tests;

#[cfg(test)]
mod tests;
