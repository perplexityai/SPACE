use pplx_space_common_core::host::disk_admission::{
    CacheDiskMeasurement, CacheDiskProbeError, read_cache_disk_measurement,
};
use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, JobId, SandboxId, SnapshotArtifact, SubjectId,
};
use pplx_space_volmgr_client::{
    VOLMGR_SOCKET,
    proto::{
        CopyObjectRequest, ExistingFilePolicy, GetTransferJobRequest, PullFileRequest,
        PushFileRequest, TransferJobStatus,
    },
    snapshot_artifact_to_proto,
};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tonic::{Response, Status};
use uuid::Uuid;

const FORK_COPY_FILE_MODE: u32 = 0o660;
const FORK_COPY_TRANSFER_DEADLINE: Duration = Duration::from_secs(60 * 60);

struct CopyStagingAttempt {
    path: Option<PathBuf>,
}

impl CopyStagingAttempt {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    async fn cleanup(mut self) -> Result<(), Status> {
        let Some(path) = self.path.as_ref() else {
            return Ok(());
        };
        remove_staging_dir(path).await?;
        self.path = None;
        Ok(())
    }
}

impl Drop for CopyStagingAttempt {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(path = %path.display(), "fork copy staging cleanup lost its runtime");
            return;
        };
        handle.spawn(async move {
            if let Err(error) = remove_staging_dir(&path).await {
                tracing::warn!(path = %path.display(), %error, "failed to clean cancelled fork copy staging");
            }
        });
    }
}

struct CopyAdmissionPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    active: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for CopyAdmissionPermit {
    fn drop(&mut self) {
        let active = self.active.fetch_sub(1, Ordering::AcqRel) - 1;
        pplx_space_metrics::snapshot::fork_copy_queue_depth("active", active);
    }
}

struct CopyAdmissionWaiter {
    waiting: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for CopyAdmissionWaiter {
    fn drop(&mut self) {
        let waiting = self.waiting.fetch_sub(1, Ordering::AcqRel) - 1;
        pplx_space_metrics::snapshot::fork_copy_queue_depth("waiting", waiting);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyOperationReservation {
    Active(usize),
    Deleting,
}

#[derive(Default)]
pub struct CopyOperationReservations {
    entries: dashmap::DashMap<PathBuf, CopyOperationReservation>,
}

pub(crate) struct ActiveCopyOperation {
    operations: Arc<CopyOperationReservations>,
    path: PathBuf,
}

impl ActiveCopyOperation {
    pub(crate) fn register(
        operations: Arc<CopyOperationReservations>,
        path: PathBuf,
    ) -> Option<Self> {
        match operations.entries.entry(path.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => match entry.get_mut() {
                CopyOperationReservation::Active(active_count) => *active_count += 1,
                CopyOperationReservation::Deleting => return None,
            },
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(CopyOperationReservation::Active(1));
            }
        }
        Some(Self { operations, path })
    }
}

impl Drop for ActiveCopyOperation {
    fn drop(&mut self) {
        if let dashmap::mapref::entry::Entry::Occupied(mut entry) =
            self.operations.entries.entry(self.path.clone())
        {
            match entry.get_mut() {
                CopyOperationReservation::Active(active_count) if *active_count > 1 => {
                    *active_count -= 1;
                }
                CopyOperationReservation::Active(_) => {
                    entry.remove();
                }
                CopyOperationReservation::Deleting => {
                    debug_assert!(false, "active copy guard replaced by delete reservation");
                }
            }
        }
    }
}

pub(crate) struct CopyOperationDeleteLease {
    operations: Arc<CopyOperationReservations>,
    path: PathBuf,
}

impl CopyOperationDeleteLease {
    pub(crate) fn acquire(
        operations: Arc<CopyOperationReservations>,
        path: PathBuf,
    ) -> Option<Self> {
        let acquired = match operations.entries.entry(path.clone()) {
            dashmap::mapref::entry::Entry::Occupied(_) => false,
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(CopyOperationReservation::Deleting);
                true
            }
        };
        if acquired {
            Some(Self { operations, path })
        } else {
            None
        }
    }
}

impl Drop for CopyOperationDeleteLease {
    fn drop(&mut self) {
        if let dashmap::mapref::entry::Entry::Occupied(entry) =
            self.operations.entries.entry(self.path.clone())
        {
            if *entry.get() == CopyOperationReservation::Deleting {
                entry.remove();
            } else {
                debug_assert!(false, "delete reservation replaced by active copy guard");
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn is_copy_operation_active(
    operations: &CopyOperationReservations,
    path: &Path,
) -> bool {
    operations
        .entries
        .get(path)
        .is_some_and(|reservation| matches!(*reservation, CopyOperationReservation::Active(_)))
}

pub(crate) fn is_copy_operation_reserved(
    operations: &CopyOperationReservations,
    path: &Path,
) -> bool {
    operations.entries.contains_key(path)
}

pub(super) struct CopyObjectsRequest<'a> {
    pub(super) operation_id: Uuid,
    pub(super) source_sandbox_id: SandboxId,
    pub(super) target_sandbox_id: SandboxId,
    pub(super) source: &'a crate::snapshot_upload::SnapshotUploadConfig,
    pub(super) destination: &'a crate::snapshot_upload::SnapshotUploadConfig,
    pub(super) destination_storage_uri: String,
    pub(super) destination_origin: ArtifactOrigin,
}

pub(super) async fn copy_objects(
    service: &super::SandboxManagerService,
    request: CopyObjectsRequest<'_>,
) -> Result<Response<crate::proto::CreateSnapshotResponse>, Status> {
    let operation_id = request.operation_id;
    let _copy_permit = acquire_copy_permit(
        &service.fork_copy_slots,
        Arc::clone(&service.fork_copy_waiting),
        Arc::clone(&service.fork_copy_active),
    )
    .await?;
    let operation_dir = copy_staging_dir(operation_id);
    let _active_operation = ActiveCopyOperation::register(
        Arc::clone(&service.active_fork_copy_operations),
        operation_dir.clone(),
    )
    .ok_or_else(|| Status::unavailable("fork copy staging cleanup is in progress"))?;
    let copy_dir = copy_attempt_staging_dir(operation_id, Uuid::now_v7());
    let staging_attempt = CopyStagingAttempt::new(copy_dir.clone());
    let prepare_dir = copy_dir.clone();
    // Cancellation must not release admission or start cleanup while the
    // blocking task is still creating the staging directory.
    let (_copy_permit, _active_operation, staging_attempt) =
        tokio::task::spawn_blocking(move || {
            crate::transfer_directory::prepare(&prepare_dir)
                .map(|()| (_copy_permit, _active_operation, staging_attempt))
        })
        .await
        .map_err(|error| Status::internal(format!("prepare fork copy directory task: {error}")))?
        .map_err(|error| Status::internal(format!("prepare fork copy directory: {error}")))?;

    let copy_result = async {
        let mut volmgr = pplx_space_volmgr_client::connect(VOLMGR_SOCKET)
            .await
            .map_err(|error| Status::unavailable(format!("connect to vol-mgr: {error}")))?;
        copy_artifact(&mut volmgr, request, &copy_dir, read_cache_disk_measurement)
            .await
            .map(Response::new)
    }
    .await;

    let cleanup_result = staging_attempt.cleanup().await;
    // This succeeds only when no concurrent attempt still owns a child.
    let _ = tokio::fs::remove_dir(&operation_dir).await;
    match (copy_result, cleanup_result) {
        (Ok(response), Ok(())) => Ok(response),
        (Ok(response), Err(cleanup_error)) => {
            tracing::warn!(
                operation_id = %operation_id,
                error = %cleanup_error,
                "failed to clean fork copy staging after successful publication"
            );
            Ok(response)
        }
        (Err(copy_error), Ok(())) => Err(copy_error),
        (Err(copy_error), Err(cleanup_error)) => {
            tracing::warn!(
                operation_id = %operation_id,
                error = %cleanup_error,
                "failed to clean fork copy staging after copy failure"
            );
            Err(copy_error)
        }
    }
}

async fn copy_artifact<Probe>(
    volmgr: &mut pplx_space_volmgr_client::ConnectedVolumeManagerClient,
    request: CopyObjectsRequest<'_>,
    copy_dir: &Path,
    measure_disk: Probe,
) -> Result<crate::proto::CreateSnapshotResponse, Status>
where
    Probe: FnOnce() -> Result<CacheDiskMeasurement, CacheDiskProbeError> + Send + 'static,
{
    let CopyObjectsRequest {
        operation_id,
        source_sandbox_id,
        target_sandbox_id,
        source,
        destination,
        destination_storage_uri,
        destination_origin,
    } = request;
    let metadata_path = copy_dir.join(SnapshotArtifact::Metadata.file_name());
    pull_object(
        volmgr,
        operation_id,
        source_sandbox_id,
        source,
        SnapshotArtifact::Metadata,
        &metadata_path,
    )
    .await?;
    let metadata_bytes = tokio::fs::read(&metadata_path)
        .await
        .map_err(|error| Status::internal(format!("read fork metadata: {error}")))?;
    let metadata: crate::snapshot_upload::SnapshotMeta = serde_json::from_slice(&metadata_bytes)
        .map_err(|error| Status::failed_precondition(format!("parse fork metadata: {error}")))?;
    if metadata.sandbox_id != source_sandbox_id.to_string() {
        return Err(Status::failed_precondition(
            "snapshot metadata belongs to a different source sandbox",
        ));
    }
    let rootfs_artifact = rootfs_artifact(&metadata)?;
    let copied_disk_size = if let Some(size) = metadata.rootfs_size_bytes {
        match copy_remote_disk(
            volmgr,
            source_sandbox_id,
            source,
            destination,
            rootfs_artifact,
        )
        .await
        {
            Ok(()) => Some(size),
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::Unimplemented
                        | tonic::Code::InvalidArgument
                        | tonic::Code::FailedPrecondition
                        | tonic::Code::Aborted
                ) =>
            {
                tracing::warn!(%operation_id, %error, "server-side disk copy unavailable; using disk transfer");
                None
            }
            Err(error) => return Err(error),
        }
    } else {
        None
    };
    let rootfs_bytes = if let Some(size) = copied_disk_size {
        size
    } else {
        admit_copy_staging(
            metadata.rootfs_size_bytes.unwrap_or(metadata.size_bytes),
            measure_disk,
        )
        .await?;
        let rootfs_path = copy_dir.join(rootfs_artifact.file_name());
        pull_object(
            volmgr,
            operation_id,
            source_sandbox_id,
            source,
            rootfs_artifact,
            &rootfs_path,
        )
        .await?;
        let size = tokio::fs::metadata(&rootfs_path)
            .await
            .map_err(|error| Status::internal(format!("stat staged fork rootfs: {error}")))?
            .len();
        if metadata
            .rootfs_size_bytes
            .is_some_and(|expected| expected != size)
        {
            return Err(Status::failed_precondition(
                "fork disk length differs from source metadata",
            ));
        }
        push_object(
            volmgr,
            source_sandbox_id,
            destination,
            rootfs_artifact,
            &rootfs_path,
        )
        .await?;
        size
    };
    let projected = project_disk_metadata(
        metadata,
        source_sandbox_id,
        target_sandbox_id,
        rootfs_bytes,
        destination_origin,
    )?;
    let projected_bytes = serde_json::to_vec_pretty(&projected)
        .map_err(|error| Status::internal(format!("serialize fork metadata: {error}")))?;
    write_projected_metadata(&metadata_path, &projected_bytes)
        .map_err(|error| Status::internal(format!("write fork metadata: {error}")))?;

    // The sidecar is the publication marker and must be visible only after
    // the disk payload is durable at the destination.
    let metadata_uploaded = push_object(
        volmgr,
        source_sandbox_id,
        destination,
        SnapshotArtifact::Metadata,
        &metadata_path,
    )
    .await?;

    Ok(crate::proto::CreateSnapshotResponse {
        snapshot_name: String::new(),
        r#type: crate::proto::SnapshotType::Disk as i32,
        published_artifact: None,
        published_durable_artifact: Some(crate::proto::PublishedDurableArtifact {
            storage_uri: destination_storage_uri,
            bytes_published: rootfs_bytes.saturating_add(metadata_uploaded),
        }),
    })
}

async fn copy_remote_disk(
    volmgr: &mut pplx_space_volmgr_client::ConnectedVolumeManagerClient,
    sandbox_id: SandboxId,
    source: &crate::snapshot_upload::SnapshotUploadConfig,
    destination: &crate::snapshot_upload::SnapshotUploadConfig,
    artifact: SnapshotArtifact,
) -> Result<(), Status> {
    bounded_transfer(
        async {
            let submitted = volmgr
                .submit_copy_object(CopyObjectRequest {
                    subject_id: SubjectId::Sandbox(sandbox_id).to_string(),
                    source_s3_uri: artifact_uri(source, artifact),
                    destination_s3_uri: artifact_uri(destination, artifact),
                })
                .await
                .map_err(|error| match error.code() {
                    tonic::Code::Unimplemented
                    | tonic::Code::InvalidArgument
                    | tonic::Code::FailedPrecondition
                    | tonic::Code::PermissionDenied
                    | tonic::Code::Unauthenticated => error,
                    _ => Status::unavailable(format!("submit fork disk copy: {error}")),
                })?
                .into_inner();
            let job_id: JobId = submitted
                .job_id
                .parse()
                .map_err(|error| Status::internal(format!("invalid disk copy job ID: {error}")))?;
            // A disconnected wait is not proof that the durable writer stopped.
            // Only a known terminal failure permits a competing disk transfer.
            let response = volmgr
                .wait_transfer_job(GetTransferJobRequest {
                    job_id: job_id.to_string(),
                })
                .await
                .map_err(|error| Status::unavailable(format!("wait for fork disk copy: {error}")))?
                .into_inner();
            let job = response
                .job
                .ok_or_else(|| Status::internal("disk copy response has no job"))?;
            if job.job_id != job_id.to_string() {
                return Err(Status::internal(
                    "disk copy response has a different job ID",
                ));
            }
            match TransferJobStatus::try_from(job.status).ok() {
                Some(TransferJobStatus::Completed) => Ok(()),
                Some(TransferJobStatus::Failed) => {
                    Err(Status::aborted("server-side disk copy failed"))
                }
                Some(TransferJobStatus::Cancelled) => {
                    Err(Status::cancelled("server-side disk copy cancelled"))
                }
                _ => Err(Status::unavailable(
                    "disk copy has not reached a terminal state",
                )),
            }
        },
        FORK_COPY_TRANSFER_DEADLINE,
        "copy",
    )
    .await
}

async fn acquire_copy_permit(
    slots: &pplx_space_common_grpc::QueuedSlots,
    waiting_count: Arc<std::sync::atomic::AtomicUsize>,
    active_count: Arc<std::sync::atomic::AtomicUsize>,
) -> Result<CopyAdmissionPermit, Status> {
    let started_at = Instant::now();
    let busy = || {
        pplx_space_common_grpc::refused_before_start(
            Status::resource_exhausted("fork copy admission is busy; retry the same operation"),
            Duration::from_secs(1),
        )
    };
    let permit = match slots.request(1) {
        pplx_space_common_grpc::SlotRequest::Admitted(permit) => permit,
        pplx_space_common_grpc::SlotRequest::Queued(queued) => {
            let waiting = waiting_count.fetch_add(1, Ordering::AcqRel) + 1;
            pplx_space_metrics::snapshot::fork_copy_queue_depth("waiting", waiting);
            let _waiter = CopyAdmissionWaiter {
                waiting: waiting_count,
            };
            queued.admitted().await.ok_or_else(busy)?
        }
        pplx_space_common_grpc::SlotRequest::Rejected => return Err(busy()),
    };
    pplx_space_metrics::snapshot::fork_copy_queue_wait(started_at);
    let active = active_count.fetch_add(1, Ordering::AcqRel) + 1;
    pplx_space_metrics::snapshot::fork_copy_queue_depth("active", active);
    Ok(CopyAdmissionPermit {
        _permit: permit,
        active: active_count,
    })
}

async fn admit_copy_staging<Probe>(required_bytes: u64, measure_disk: Probe) -> Result<(), Status>
where
    Probe: FnOnce() -> Result<CacheDiskMeasurement, CacheDiskProbeError> + Send + 'static,
{
    use pplx_space_common_core::host::disk_admission::CacheDiskAdmissionConfig;

    let config = CacheDiskAdmissionConfig::from_env();
    if !config.enabled {
        return Ok(());
    }
    let measurement = tokio::task::spawn_blocking(measure_disk)
        .await
        .map_err(|error| Status::unavailable(format!("probe fork copy disk admission: {error}")))?
        .map_err(|error| Status::unavailable(format!("probe fork copy disk admission: {error}")))?;
    let decision = copy_disk_admission_decision(config, measurement, required_bytes);
    if decision.allows_new_allocation() {
        return Ok(());
    }
    Err(Status::resource_exhausted(format!(
        "fork copy disk admission denied: {}",
        decision.reason
    )))
}

fn copy_disk_admission_decision(
    config: pplx_space_common_core::host::disk_admission::CacheDiskAdmissionConfig,
    measurement: pplx_space_common_core::host::disk_admission::CacheDiskMeasurement,
    required_bytes: u64,
) -> pplx_space_common_core::host::disk_admission::CacheDiskAdmissionDecision {
    let reservation_bytes = required_bytes.max(config.allocation_reservation_bytes);
    pplx_space_common_core::host::disk_admission::evaluate_cache_disk_measurement(
        config,
        measurement,
        reservation_bytes,
    )
}

fn write_projected_metadata(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    crate::snapshot_scheduler::write_file_atomic(path, bytes)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(FORK_COPY_FILE_MODE))
}

fn project_disk_metadata(
    mut metadata: crate::snapshot_upload::SnapshotMeta,
    source_sandbox_id: SandboxId,
    destination_sandbox_id: SandboxId,
    rootfs_bytes: u64,
    destination_origin: ArtifactOrigin,
) -> Result<crate::snapshot_upload::SnapshotMeta, Status> {
    if metadata.sandbox_id != source_sandbox_id.to_string() {
        return Err(Status::failed_precondition(
            "snapshot metadata belongs to a different source sandbox",
        ));
    }
    validate_disk_projection_source(&metadata)?;
    metadata.sandbox_id = destination_sandbox_id.to_string();
    metadata.size_bytes = rootfs_bytes;
    metadata.rootfs_size_bytes = Some(rootfs_bytes);
    // The copy has the stream, never the source's node-local bundle.
    metadata.rootfs_send_source_id = None;
    metadata.local_artifact_id = None;
    metadata.artifact_kind = Some(ArtifactKind::Disk);
    metadata.artifact_origin = Some(destination_origin);
    metadata.artifact_snapshot_name = None;
    Ok(metadata)
}

fn rootfs_artifact(
    metadata: &crate::snapshot_upload::SnapshotMeta,
) -> Result<SnapshotArtifact, Status> {
    validate_disk_projection_source(metadata)?;
    if metadata.rootfs_diff_format.as_deref()
        == Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1)
    {
        Ok(SnapshotArtifact::RootfsBtrfsSend)
    } else {
        Ok(SnapshotArtifact::Rootfs)
    }
}

fn validate_disk_projection_source(
    metadata: &crate::snapshot_upload::SnapshotMeta,
) -> Result<(), Status> {
    if metadata.encrypted {
        return Err(Status::failed_precondition(
            "encrypted suspended artifacts cannot be forked as disk-only artifacts",
        ));
    }
    match metadata.artifact_kind {
        // A suspended artifact contains a valid rootfs plus runtime state. A disk-only fork
        // deliberately projects just that rootfs into a new disk artifact; memory is not copied.
        Some(ArtifactKind::Disk | ArtifactKind::Full) => Ok(()),
        None => Err(Status::failed_precondition(
            "fork source metadata must declare a disk or full artifact kind",
        )),
    }
}

pub(crate) fn copy_staging_root_under(cache_root: &Path) -> PathBuf {
    cache_root.join(pplx_space_common_core::host::linux::paths::FORK_COPY_STAGING_DIR_NAME)
}

fn copy_staging_root() -> PathBuf {
    copy_staging_root_under(Path::new(
        pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR,
    ))
}

fn copy_staging_dir(operation_id: Uuid) -> std::path::PathBuf {
    copy_staging_root().join(operation_id.to_string())
}

fn copy_attempt_staging_dir(operation_id: Uuid, attempt_id: Uuid) -> std::path::PathBuf {
    copy_staging_dir(operation_id).join(attempt_id.to_string())
}

async fn remove_staging_dir(path: &std::path::Path) -> Result<(), Status> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Status::internal(format!(
            "remove fork copy staging {}: {error}",
            path.display()
        ))),
    }
}

async fn pull_object(
    volmgr: &mut pplx_space_volmgr_client::ConnectedVolumeManagerClient,
    operation_id: Uuid,
    sandbox_id: SandboxId,
    config: &crate::snapshot_upload::SnapshotUploadConfig,
    artifact: SnapshotArtifact,
    local_path: &std::path::Path,
) -> Result<u64, Status> {
    let response = bounded_transfer(
        volmgr.pull_file(PullFileRequest {
            subject_id: format!("sandbox:{sandbox_id}"),
            s3_uri: artifact_uri(config, artifact),
            local_path: local_path.to_string_lossy().into_owned(),
            raw: false,
            file_mode: Some(FORK_COPY_FILE_MODE),
            existing_file_policy: ExistingFilePolicy::VerifiedReplica as i32,
            snapshot_artifact: snapshot_artifact_to_proto(artifact) as i32,
            transfer_scope_id: Some(operation_id.to_string()),
            ..Default::default()
        }),
        FORK_COPY_TRANSFER_DEADLINE,
        "pull",
    )
    .await
    .map_err(|error| map_pull_error(artifact.file_name(), error))?
    .into_inner();
    Ok(response.bytes_downloaded)
}

fn map_pull_error(artifact: &str, error: Status) -> Status {
    if error.code() == tonic::Code::NotFound {
        Status::not_found(format!("fork source {artifact} is no longer available"))
    } else {
        Status::unavailable(format!("pull fork {artifact}: {error}"))
    }
}

async fn push_object(
    volmgr: &mut pplx_space_volmgr_client::ConnectedVolumeManagerClient,
    sandbox_id: SandboxId,
    config: &crate::snapshot_upload::SnapshotUploadConfig,
    artifact: SnapshotArtifact,
    local_path: &std::path::Path,
) -> Result<u64, Status> {
    let response = bounded_transfer(
        volmgr.push_file(PushFileRequest {
            subject_id: format!("sandbox:{sandbox_id}"),
            local_path: local_path.to_string_lossy().into_owned(),
            s3_uri: artifact_uri(config, artifact),
        }),
        FORK_COPY_TRANSFER_DEADLINE,
        "push",
    )
    .await
    .map_err(|error| Status::unavailable(format!("push fork artifact: {error}")))?
    .into_inner();
    Ok(response.bytes_uploaded)
}

async fn bounded_transfer<T>(
    future: impl std::future::Future<Output = Result<T, Status>>,
    deadline: Duration,
    operation: &'static str,
) -> Result<T, Status> {
    let started_at = Instant::now();
    match tokio::time::timeout(deadline, future).await {
        Ok(Ok(value)) => {
            pplx_space_metrics::snapshot::fork_copy_transfer(operation, "success", started_at);
            Ok(value)
        }
        Ok(Err(error)) => {
            pplx_space_metrics::snapshot::fork_copy_transfer(operation, "error", started_at);
            Err(error)
        }
        Err(_) => {
            pplx_space_metrics::snapshot::fork_copy_transfer(operation, "timeout", started_at);
            Err(Status::deadline_exceeded(format!(
                "fork copy {operation} exceeded {} seconds",
                deadline.as_secs()
            )))
        }
    }
}

fn artifact_uri(
    config: &crate::snapshot_upload::SnapshotUploadConfig,
    artifact: SnapshotArtifact,
) -> String {
    let prefix = config.key_prefix.trim_matches('/');
    let key = if prefix.is_empty() {
        artifact.file_name().to_string()
    } else {
        format!("{prefix}/{}", artifact.file_name())
    };
    crate::snapshot_upload::build_artifact_uri(&key, config)
}

#[cfg(test)]
mod tests;
