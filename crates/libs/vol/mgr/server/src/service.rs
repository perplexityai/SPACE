use pplx_space_common_core::types::JobId;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use crate::artifact::{ArtifactEntryRole, ArtifactId};
use pplx_space_common_core::types::{
    MaterializationMode, SnapshotArtifact, StorageUri, SubjectId, TransferScopeId,
    parse_snapshot_object_key,
};
use pplx_space_common_jobs::{
    BackgroundWorker, JobExecutor, JobFailure, JobFailureReason, JobPriority, PriorityBand,
    WorkerConfig,
};
use pplx_space_common_svc::core::host::linux::identities::{user_names, users};
use pplx_space_common_svc::core::types::OrgId;
use pplx_space_common_svc::{
    AuthorizedUdsPeer, UdsPeerAccount, UdsPeerAuthorizer, UdsPeerPrincipal,
};
use tonic::{Request, Response, Status};

use crate::executor::{
    ArtifactEntryPayload, ArtifactPayload, FileMode, PullExistingPolicy,
    ShadowComparisonController, TransferDedupOptions, TransferEncryption, TransferExecutor,
    TransferExecutorConfig, TransferPayload, dedup_key, parse_subject_id_with_legacy_fallback,
    snapshot_artifact_from_entry_role, snapshot_artifact_from_local_path,
};
use crate::staging::{StagingDir, StagingError};
use crate::{VolumeAttachment, db};
use pplx_space_volmgr_client::proto::cancel_transfer_request::Selector as CancelTransferSelector;
use pplx_space_volmgr_client::proto::{
    AttachVolumeRequest, AttachVolumeResponse, BatchDecryptionConfig, BatchEncryptionConfig,
    CancelTransferRequest, CancelTransferResponse, DetachVolumeRequest, DetachVolumeResponse,
    ExistingFilePolicy, GetTransferJobRequest, GetTransferJobResponse, ListTransferJobsRequest,
    ListTransferJobsResponse, ListVolumesRequest, ListVolumesResponse, PullFileBatchEntry,
    PullFileRequest, PullFileResponse, PullFilesAsyncRequest, PullFilesAsyncResponse,
    PushFileBatchEntry, PushFileRequest, PushFileResponse, PushFilesAsyncRequest,
    PushFilesAsyncResponse, SnapshotArtifact as ProtoSnapshotArtifact, SubmitTransferResponse,
    TransferJob, TransferJobStatus, VolumeReadDirRequest, VolumeReadDirResponse, VolumeReadRequest,
    VolumeReadResponse, VolumeSpec, VolumeStatRequest, VolumeStatResponse,
    volume_manager_server::VolumeManager,
};
use pplx_space_volmgr_client::snapshot_artifact_from_proto;

const PUSH_KIND: &str = "push";
const PULL_KIND: &str = "pull";
const PUSH_ARTIFACT_KIND: &str = "push_artifact";
const PULL_ARTIFACT_KIND: &str = "pull_artifact";
// Socket-group membership grants connectivity, not authority. Keep this list
// to workload identities so local operator and observer accounts fail closed.
const ROOT_PEERS: &[UdsPeerPrincipal] = &[UdsPeerPrincipal::Root];
const SANDBOX_MANAGER_PEERS: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
];
const TRANSFER_MANAGER_PEERS: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
    UdsPeerPrincipal::Service(user_names::SPACE_TMPLMGR),
];

fn transfer_scope_job_ids(
    conn: &rusqlite::Connection,
    scope: TransferScopeId,
) -> rusqlite::Result<Vec<pplx_space_common_jobs::JobId>> {
    let mut stmt = conn.prepare(
        "SELECT job_id
           FROM jobs
          WHERE kind = 'pull'
            AND status IN ('pending', 'running', 'cancelling')
            AND json_extract(payload, '$.transfer_scope_id') = ?1
          ORDER BY created_at DESC, job_id DESC",
    )?;
    stmt.query_map([scope.to_string()], |row| row.get(0))?
        .collect()
}

#[derive(Clone)]
pub struct VolumeManagerService {
    db: Arc<Mutex<rusqlite::Connection>>,
    worker: BackgroundWorker<TransferExecutor>,
    shadow_comparisons: ShadowComparisonController,
    staging: StagingDir,
    transfer_admission: TransferAdmission,
    peer_authorizer: UdsPeerAuthorizer,
}

#[derive(Clone)]
pub struct TransferAdmission {
    state: Arc<Mutex<TransferAdmissionState>>,
    idle: Arc<tokio::sync::Notify>,
}

#[derive(Default)]
struct TransferAdmissionState {
    accepting: bool,
    admitted: usize,
}

struct TransferAdmissionLease {
    admission: TransferAdmission,
}

impl TransferAdmission {
    pub fn open() -> Self {
        Self {
            state: Arc::new(Mutex::new(TransferAdmissionState {
                accepting: true,
                admitted: 0,
            })),
            idle: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn enter(&self) -> Result<TransferAdmissionLease, Status> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("volume-manager transfer admission mutex poisoned"))?;
        if !state.accepting {
            return Err(Status::unavailable(
                "volume-manager is handing off; retry transfer submission",
            ));
        }
        state.admitted += 1;
        Ok(TransferAdmissionLease {
            admission: self.clone(),
        })
    }

    /// Atomically rejects new state mutations. Existing polling remains
    /// available while admitted mutations reach a durable boundary.
    pub fn close(&self) {
        match self.state.lock() {
            Ok(mut state) => state.accepting = false,
            Err(error) => {
                tracing::error!(%error, "volume-manager transfer admission mutex poisoned")
            }
        }
    }

    /// Waits until every submission admitted before [`Self::close`] has
    /// either durably registered its job or returned an error.
    pub async fn wait_for_idle(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            // `Notify::notified()` is not registered until first poll. Enable
            // it before inspecting the counter so a final lease drop between
            // the state read and await cannot lose `notify_waiters()`.
            notified.as_mut().enable();
            let admitted = match self.state.lock() {
                Ok(state) => state.admitted,
                Err(error) => {
                    tracing::error!(%error, "volume-manager transfer admission mutex poisoned");
                    return false;
                }
            };
            if admitted == 0 {
                return true;
            }
            if tokio::time::timeout_at(deadline, &mut notified)
                .await
                .is_err()
            {
                return false;
            }
        }
    }
}

impl Drop for TransferAdmissionLease {
    fn drop(&mut self) {
        let Ok(mut state) = self.admission.state.lock() else {
            tracing::error!("volume-manager transfer admission mutex poisoned while leaving");
            return;
        };
        state.admitted = state.admitted.saturating_sub(1);
        if state.admitted == 0 {
            self.admission.idle.notify_waiters();
        }
    }
}

pub struct VolumeManagerServiceConfig {
    pub push_concurrency: usize,
    pub pull_concurrency: usize,
    pub pull_priority_bands: Vec<PriorityBand>,
    pub transfer_timeout: Duration,
    pub max_attempts: i32,
    pub executor: TransferExecutorConfig,
}

impl VolumeManagerService {
    pub fn new(
        db_path: &str,
        staging_dir: impl Into<PathBuf>,
        config: VolumeManagerServiceConfig,
    ) -> Result<Self, NewServiceError> {
        let conn = db::open(db_path).map_err(NewServiceError::OpenDb)?;
        let db = Arc::new(Mutex::new(conn));
        let staging = StagingDir::ensure(staging_dir)?;
        let worker_config = WorkerConfig::new(1, config.transfer_timeout, config.max_attempts)
            .with_kind_concurrency(PUSH_KIND, config.push_concurrency)
            .with_kind_concurrency(PULL_KIND, config.pull_concurrency)
            .with_kind_active_job_limit(PULL_KIND, config.pull_concurrency)
            .with_kind_priority_bands(PULL_KIND, config.pull_priority_bands);
        let executor = TransferExecutor::new(config.executor, staging.root().to_path_buf());
        let shadow_comparisons = executor.shadow_comparison_controller();
        let worker = BackgroundWorker::new(Arc::clone(&db), executor, worker_config);
        Ok(Self {
            db,
            worker,
            shadow_comparisons,
            staging,
            transfer_admission: TransferAdmission::open(),
            peer_authorizer: UdsPeerAuthorizer::new(
                crate::SERVICE_NAME,
                [
                    UdsPeerAccount::with_linux_user(users::SPACE_SBXMGR),
                    UdsPeerAccount::with_linux_user(users::SPACE_TMPLMGR),
                ],
            ),
        })
    }

    fn authorize_sandbox_subject(
        &self,
        peer: &AuthorizedUdsPeer,
        subject: &SubjectId,
        operation: &str,
    ) -> Result<(), Status> {
        if matches!(peer.principal(), UdsPeerPrincipal::Root)
            || matches!(
                (peer.principal(), subject),
                (
                    UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
                    SubjectId::Sandbox(_)
                )
            )
        {
            return Ok(());
        }
        self.reject_subject(peer, subject, operation)
    }

    fn authorize_transfer_subject(
        &self,
        peer: &AuthorizedUdsPeer,
        subject: &SubjectId,
        operation: &str,
    ) -> Result<(), Status> {
        if matches!(peer.principal(), UdsPeerPrincipal::Root)
            || matches!(
                (peer.principal(), subject),
                (
                    UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
                    SubjectId::Sandbox(_)
                ) | (
                    UdsPeerPrincipal::Service(user_names::SPACE_TMPLMGR),
                    SubjectId::Template(_)
                )
            )
        {
            return Ok(());
        }
        self.reject_subject(peer, subject, operation)
    }

    fn authorize_artifact_job(
        &self,
        peer: &AuthorizedUdsPeer,
        requested_subject: &SubjectId,
        job: &pplx_space_common_jobs::Job,
        operation: &str,
    ) -> Result<(), Status> {
        if artifact_job_matches_subject_class(job, requested_subject) {
            return self.authorize_job(peer, job, operation);
        }
        tracing::warn!(
            operation,
            job_id = %job.job_id,
            peer_principal = peer.principal().as_str(),
            requested_subject_kind = %requested_subject.kind(),
            "rejecting artifact job outside the authorized subject class"
        );
        Err(Status::permission_denied(
            "artifact job is not authorized for this local peer and subject class",
        ))
    }

    fn authorize_job(
        &self,
        peer: &AuthorizedUdsPeer,
        job: &pplx_space_common_jobs::Job,
        operation: &str,
    ) -> Result<(), Status> {
        if matches!(peer.principal(), UdsPeerPrincipal::Root) {
            return Ok(());
        }
        let Some(subject_id) = job.subject_id.as_deref() else {
            tracing::warn!(
                operation,
                job_id = %job.job_id,
                peer_principal = peer.principal().as_str(),
                "rejecting access to transfer job without an authorization subject"
            );
            return Err(Status::permission_denied(
                "transfer job is not authorized for this local peer",
            ));
        };
        let subject = parse_subject(subject_id).map_err(|error| {
            tracing::warn!(
                operation,
                job_id = %job.job_id,
                peer_principal = peer.principal().as_str(),
                %error,
                "rejecting access to transfer job with an invalid authorization subject"
            );
            Status::permission_denied("transfer job is not authorized for this local peer")
        })?;
        self.authorize_transfer_subject(peer, &subject, operation)
    }

    fn reject_subject(
        &self,
        peer: &AuthorizedUdsPeer,
        subject: &SubjectId,
        operation: &str,
    ) -> Result<(), Status> {
        tracing::warn!(
            operation,
            peer_uid = peer.identity().uid(),
            peer_gid = peer.identity().gid(),
            peer_pid = ?peer.identity().pid(),
            peer_principal = peer.principal().as_str(),
            subject_kind = %subject.kind(),
            "rejecting local RPC for unauthorized subject kind"
        );
        Err(Status::permission_denied(format!(
            "local peer {} is not authorized for {operation} on {} subjects",
            peer.principal().as_str(),
            subject.kind()
        )))
    }

    /// Returns a clone of the worker handle for calling `recover()` and
    /// `drain()` from the service binary.
    pub fn worker(&self) -> BackgroundWorker<TransferExecutor> {
        self.worker.clone()
    }

    pub fn shadow_comparison_controller(&self) -> ShadowComparisonController {
        self.shadow_comparisons.clone()
    }

    pub fn transfer_admission(&self) -> TransferAdmission {
        self.transfer_admission.clone()
    }

    /// Count durable transfer rows that have not reached a terminal state.
    pub fn active_transfer_count(&self) -> Result<u64, Status> {
        let conn = self.lock_db()?;
        pplx_space_common_jobs::db::count_active_jobs(&conn)
            .map_err(|error| Status::internal(format!("count active transfers: {error}")))
    }

    /// Sweep the staging dir, removing any file whose owning push job is
    /// no longer in `pending`/`running`. Call once at startup, **before**
    /// [`BackgroundWorker::recover`], so that:
    ///   * files left by jobs that completed or permanently failed before
    ///     a prior process exit are reclaimed; and
    ///   * files still owned by jobs that the worker is about to re-spawn
    ///     are preserved.
    pub fn cleanup_staging_orphans(&self) -> Result<usize, Status> {
        let keep = self.collect_active_staged_paths()?;
        Ok(crate::staging::cleanup_orphans(self.staging.root(), &keep))
    }

    /// Collect every `staged_path` referenced by a push job currently in
    /// `pending` or `running`. Goes through the worker's SQLite connection
    /// directly (rather than `worker.list_jobs`, which is bounded by a
    /// caller-supplied limit) because the keep-set **must be complete**:
    /// if a single staged_path were missing, `cleanup_orphans` would
    /// delete a file the worker is about to re-execute, reproducing the
    /// exact "entity not found" failure this module exists to prevent.
    fn collect_active_staged_paths(&self) -> Result<HashSet<PathBuf>, Status> {
        let conn = self.lock_db()?;
        let mut stmt = conn
            .prepare(
                "SELECT payload FROM jobs
                 WHERE kind = 'push' AND status IN ('pending', 'running')",
            )
            .map_err(|e| Status::internal(format!("prepare staged-paths query: {e}")))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| Status::internal(format!("execute staged-paths query: {e}")))?;

        let mut out = HashSet::new();
        for row in rows {
            let payload =
                row.map_err(|e| Status::internal(format!("read staged-paths row: {e}")))?;
            if let Ok(p) = serde_json::from_str::<TransferPayload>(&payload)
                && let Some(staged) = p.staged_path
            {
                out.insert(PathBuf::from(staged));
            }
        }
        Ok(out)
    }

    fn lock_db(&self) -> Result<MutexGuard<'_, rusqlite::Connection>, Status> {
        self.db
            .lock()
            .map_err(|e| Status::internal(format!("lock vol-mgr db: {e}")))
    }

    fn materialization_assignment(
        &self,
        subject: &SubjectId,
        artifacts: impl IntoIterator<Item = Option<SnapshotArtifact>>,
    ) -> MaterializationAssignment {
        let mut has_memory = false;
        let mut has_rootfs = false;
        for artifact in artifacts.into_iter().flatten() {
            has_memory |= artifact == SnapshotArtifact::Memory;
            has_rootfs |= artifact == SnapshotArtifact::Rootfs;
        }

        let rootfs_assignment = if has_rootfs {
            rootfs_materialization_mode(subject)
        } else {
            MaterializationMode::Disabled
        };
        MaterializationAssignment {
            mode: artifact_materialization_mode(subject, has_memory, has_rootfs, rootfs_assignment),
            rootfs_mode: has_rootfs.then_some(rootfs_assignment),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MaterializationAssignment {
    mode: MaterializationMode,
    rootfs_mode: Option<MaterializationMode>,
}

impl From<MaterializationMode> for MaterializationAssignment {
    fn from(mode: MaterializationMode) -> Self {
        Self {
            mode,
            rootfs_mode: None,
        }
    }
}

fn artifact_materialization_mode(
    subject: &SubjectId,
    has_memory: bool,
    has_rootfs: bool,
    rootfs_assignment: MaterializationMode,
) -> MaterializationMode {
    if !matches!(subject, SubjectId::Sandbox(_)) {
        MaterializationMode::Disabled
    } else if has_memory {
        MaterializationMode::Enabled
    } else if has_rootfs {
        rootfs_assignment
    } else {
        MaterializationMode::Disabled
    }
}

fn rootfs_materialization_mode(subject: &SubjectId) -> MaterializationMode {
    if matches!(subject, SubjectId::Sandbox(_)) {
        MaterializationMode::Enabled
    } else {
        MaterializationMode::Disabled
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NewServiceError {
    #[error("open volume-manager db: {0}")]
    OpenDb(#[source] rusqlite::Error),
    #[error(transparent)]
    Staging(#[from] StagingError),
}

fn attachment_to_proto(a: VolumeAttachment) -> VolumeSpec {
    VolumeSpec {
        name: a.name,
        s3_bucket: a.s3_bucket,
        s3_prefix: a.s3_prefix,
        s3_region: a.s3_region,
        read_only: a.read_only,
        byok_key_id: a.byok_key_id,
    }
}

fn proto_to_attachment(spec: VolumeSpec) -> VolumeAttachment {
    VolumeAttachment {
        name: spec.name,
        s3_bucket: spec.s3_bucket,
        s3_prefix: spec.s3_prefix,
        s3_region: spec.s3_region,
        read_only: spec.read_only,
        byok_key_id: spec.byok_key_id,
    }
}

fn db_err(e: rusqlite::Error) -> Status {
    Status::internal(format!("volume-manager db error: {e}"))
}

fn parse_uri(raw: &str) -> Result<StorageUri, Status> {
    StorageUri::parse(raw).map_err(|e| Status::invalid_argument(format!("invalid s3_uri: {e}")))
}

/// Parse the wire `subject_id` field into a typed [`SubjectId`].
///
/// Accepts two shapes via [`parse_subject_id_with_legacy_fallback`]:
///
/// - The kinded `"<kind>:<id>"` form — used by future template-manager
///   pulls (`"template:<name>"`) and by any caller that wants the kind
///   tag explicit on the wire.
/// - A bare sandbox UUID, including messages from older sandbox-manager
///   binaries during a rollout. Resolves to [`SubjectId::Sandbox`] without
///   requiring a coordinated client upgrade.
fn parse_subject(raw: &str) -> Result<SubjectId, Status> {
    parse_subject_id_with_legacy_fallback(raw)
        .map_err(|e| Status::invalid_argument(format!("invalid subject_id: {e}")))
}

fn artifact_job_matches_subject_class(
    job: &pplx_space_common_jobs::Job,
    requested_subject: &SubjectId,
) -> bool {
    job.subject_id
        .as_deref()
        .and_then(|subject_id| parse_subject(subject_id).ok())
        .is_some_and(|persisted_subject| persisted_subject.kind() == requested_subject.kind())
}

fn job_bytes_to_u64(bytes: i64) -> Result<u64, Status> {
    u64::try_from(bytes).map_err(|source| {
        Status::internal(format!(
            "job byte count must be non-negative: {bytes}: {source}"
        ))
    })
}

fn pull_job_failure_status(job: &pplx_space_common_jobs::Job) -> Status {
    let message = format!(
        "pull failed: {}",
        job.error.as_deref().unwrap_or("unknown error")
    );
    match job.failure {
        Some(JobFailure::Permanent { reason }) => match reason {
            Some(JobFailureReason::NotFound) => Status::not_found(message),
            Some(JobFailureReason::PermissionDenied) => Status::permission_denied(message),
            Some(JobFailureReason::InvalidInput) => Status::invalid_argument(message),
            Some(JobFailureReason::Internal) => Status::internal(message),
            None => Status::failed_precondition(message),
        },
        Some(JobFailure::Retriable) => Status::unavailable(message),
        None => Status::internal(message),
    }
}

fn parse_existing_file_policy(raw: i32) -> Result<PullExistingPolicy, Status> {
    match ExistingFilePolicy::try_from(raw) {
        Ok(ExistingFilePolicy::LegacySkipPresent) => Ok(PullExistingPolicy::LegacySkipPresent),
        Ok(ExistingFilePolicy::VerifiedReplica) => Ok(PullExistingPolicy::VerifiedReplica),
        Err(_) => Err(Status::invalid_argument(format!(
            "invalid existing_file_policy: {raw}"
        ))),
    }
}

fn transfer_job_status_code(status: TransferJobStatus) -> i32 {
    match status {
        TransferJobStatus::Unspecified => 0,
        TransferJobStatus::Pending => 1,
        TransferJobStatus::Running => 2,
        TransferJobStatus::Completed => 3,
        TransferJobStatus::Failed => 4,
        TransferJobStatus::Cancelling => 5,
        TransferJobStatus::Cancelled => 6,
    }
}

fn job_to_proto(job: pplx_space_common_jobs::Job) -> Result<TransferJob, Status> {
    let payload = serde_json::from_str::<TransferPayload>(&job.payload).ok();
    let local_path = payload
        .as_ref()
        .map(|p| p.local_path.clone())
        .unwrap_or_default();
    let s3_uri = payload
        .as_ref()
        .map(|p| p.s3_uri.clone())
        .unwrap_or_default();
    let transfer_scope_id = payload
        .as_ref()
        .and_then(|payload| payload.transfer_scope_id)
        .map(|scope| scope.to_string());
    let status = match job.status.as_str() {
        "pending" => TransferJobStatus::Pending,
        "running" => TransferJobStatus::Running,
        "completed" => TransferJobStatus::Completed,
        "failed" => TransferJobStatus::Failed,
        "cancelling" => TransferJobStatus::Cancelling,
        "cancelled" => TransferJobStatus::Cancelled,
        _ => TransferJobStatus::Unspecified,
    };
    Ok(TransferJob {
        job_id: job.job_id.to_string(),
        status: transfer_job_status_code(status),
        kind: job.kind,
        subject_id: job.subject_id.unwrap_or_default(),
        local_path,
        s3_uri,
        bytes_transferred: job_bytes_to_u64(job.bytes_processed)?,
        error: job.error,
        created_at: job.created_at,
        started_at: job.started_at,
        completed_at: job.completed_at,
        transfer_scope_id,
    })
}

async fn wait_for_transfer_job<E: JobExecutor>(
    worker: &BackgroundWorker<E>,
    job_id: &JobId,
) -> Result<GetTransferJobResponse, Status> {
    let job = worker
        .wait_for_job(job_id, worker.worst_case_runtime())
        .await?;
    Ok(GetTransferJobResponse {
        job: Some(job_to_proto(job)?),
    })
}

async fn submit_push(
    worker: &BackgroundWorker<TransferExecutor>,
    staging: &StagingDir,
    req: &PushFileRequest,
) -> Result<pplx_space_common_jobs::JobId, Status> {
    // Reflink the caller's source into vol-mgr-owned staging first. The
    // upload reads bytes from the staged copy, so an unlink or rewrite of
    // the original between submit time and worker dequeue (e.g. a later
    // cleanup pass over the same paths) can no longer race the upload.
    // Done before submit so failures here surface synchronously and we
    // never enqueue a job that can't reach its data.
    let staged = stage_push_source(staging, &req.local_path).await?;
    register_push_job(worker, req, staged).await
}

/// Stage a push source file into vol-mgr-owned staging. Split out of
/// [`submit_push`] so batched callers can pre-stage every source up front
/// — fail-fast before any irreversible side effect like a credmgr
/// `IssueSandboxArtifactDek` round-trip — and then call [`register_push_job`] per file
/// with the already-staged path.
async fn stage_push_source(staging: &StagingDir, local_path: &str) -> Result<PathBuf, Status> {
    let src = Path::new(local_path);
    staging.stage(src).await.map_err(|e| match e {
        StagingError::Stage { .. } => Status::failed_precondition(e.to_string()),
        StagingError::CreateDir { .. } => Status::internal(e.to_string()),
    })
}

/// Best-effort unlink of a pre-staged path. Used to roll back the
/// partial result of a `stage_push_source` batch when a later file in
/// the batch fails to stage.
async fn unstage_push_source(path: &Path) {
    if let Err(e) = tokio::fs::remove_file(path).await
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(
            staged_path = %path.display(),
            error = %e,
            "failed to remove pre-staged file during batch rollback",
        );
    }
}

/// Register the upload job for an already-staged push request. Mirrors
/// the second half of the single-file [`submit_push`] flow — dedup-key
/// computation, worker submit, post-submit orphan cleanup — taking the
/// pre-staged path explicitly so batched callers can hand in paths from
/// [`stage_push_source`].
async fn register_push_job(
    worker: &BackgroundWorker<TransferExecutor>,
    req: &PushFileRequest,
    staged: PathBuf,
) -> Result<pplx_space_common_jobs::JobId, Status> {
    let staged_str = staged.to_string_lossy().into_owned();

    // Single-file pushes are always plaintext: envelope encryption is a
    // property of an artifact (`PushFilesAsync`), never a single file.
    let payload = TransferPayload {
        subject_id: req.subject_id.clone(),
        local_path: req.local_path.clone(),
        s3_uri: req.s3_uri.clone(),
        transfer_scope_id: None,
        snapshot_artifact: snapshot_artifact_from_local_path(Path::new(&req.local_path)),
        staged_path: Some(staged_str.clone()),
        raw: false,
        file_mode: None,
        existing_file_policy: PullExistingPolicy::LegacySkipPresent,
        materialization_mode: MaterializationMode::Disabled,
        rootfs_materialization_mode: None,
        encryption: None,
        artifact_id: None,
        entry_role: None,
    }
    .to_json();
    // `raw` is meaningless on push (we always upload exactly the bytes the
    // caller gave us, optionally pre-compressed by vol-mgr's `compress`
    // config); pass `false` so push jobs share the same dedup namespace.
    let dk = dedup_key(
        "push",
        &req.subject_id,
        &req.local_path,
        &req.s3_uri,
        false,
        TransferDedupOptions::default(),
    );

    // Submit. On error the freshly reflinked file is an orphan in the
    // staging dir — the startup GC would reclaim it eventually, but a
    // flapping submitter could fill the staging dir in the meantime.
    // Best-effort unlink on the spot, then propagate the original error.
    let duplicate_policy = push_duplicate_policy(req);
    let job_id = match worker.submit_with_duplicate_policy(
        PUSH_KIND,
        &payload,
        Some(&req.subject_id),
        Some(&dk),
        duplicate_policy,
    ) {
        Ok(id) => id,
        Err(submit_err) => {
            if let Err(remove_err) = tokio::fs::remove_file(&staged).await
                && remove_err.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(
                    staged_path = %staged_str,
                    subject_id = %req.subject_id,
                    error = %remove_err,
                    "failed to remove staged file after submit error"
                );
            }
            return Err(submit_err);
        }
    };

    // Dedup detection: if the worker returned an existing job (same
    // dedup_key in `pending`/`running`), our just-reflinked file is an
    // orphan — the running job's payload still points at its own staged
    // path. Compare and clean up. On btrfs the reflink we just wasted is
    // O(1), so the cost of this defensive stage-then-discard is tiny.
    if let Ok(Some(job)) = worker.get_job(&job_id)
        && let Ok(persisted) = serde_json::from_str::<TransferPayload>(&job.payload)
        && persisted.staged_path.as_deref() != Some(staged_str.as_str())
        && let Err(e) = tokio::fs::remove_file(&staged).await
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(
            job_id = %job_id,
            staged_path = %staged_str,
            subject_id = %req.subject_id,
            error = %e,
            "failed to remove orphan staged file after dedup hit"
        );
    }

    Ok(job_id)
}

fn push_duplicate_policy(req: &PushFileRequest) -> pplx_space_common_jobs::DuplicatePolicy {
    if is_immutable_suspend_snapshot_push(req) {
        // Snapshot object keys include an immutable backup-attempt identity.
        // A retry may therefore rejoin a completed transfer across the
        // caller's submit-to-persist gap. Generic PushFile callers can reuse
        // paths and destinations for changed bytes and remain active-only.
        pplx_space_common_jobs::DuplicatePolicy::ReuseExistingOrCompleted
    } else {
        pplx_space_common_jobs::DuplicatePolicy::ReuseExisting
    }
}

fn is_immutable_suspend_snapshot_push(req: &PushFileRequest) -> bool {
    let Some(subject_sandbox) = req.subject_id.strip_prefix("sandbox:") else {
        return false;
    };
    if subject_sandbox.parse::<uuid::Uuid>().is_err() {
        return false;
    }
    let Ok(storage) = StorageUri::parse(&req.s3_uri) else {
        return false;
    };
    let Some(object) = parse_snapshot_object_key(&storage.key_prefix) else {
        return false;
    };
    object.sandbox_id == subject_sandbox
        && object.snapshot_id.parse::<uuid::Uuid>().is_ok()
        && snapshot_artifact_from_entry_role(object.file_name).is_some()
}

fn submit_pull<E: JobExecutor>(
    worker: &BackgroundWorker<E>,
    req: &PullFileRequest,
    assignment: impl Into<MaterializationAssignment>,
) -> Result<pplx_space_common_jobs::JobId, Status> {
    let assignment = assignment.into();
    let file_mode = req
        .file_mode
        .map(FileMode::new)
        .transpose()
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
    let existing_file_policy = parse_existing_file_policy(req.existing_file_policy)?;
    let requested_snapshot_artifact =
        parse_snapshot_artifact(req.snapshot_artifact, "snapshot_artifact")?;
    let transfer_scope_id = req
        .transfer_scope_id
        .as_deref()
        .map(str::parse::<TransferScopeId>)
        .transpose()
        .map_err(|error| Status::invalid_argument(format!("transfer_scope_id: {error}")))?;
    // Single-file pulls are always plaintext; encrypted transfers go
    // through `PullFilesAsync` as an artifact.
    // Pull writes to `local_path` (it doesn't read from it), so there's
    // no submit-time-vs-execute-time race to defend against: nothing in
    // the staging area would help.
    let payload = TransferPayload {
        subject_id: req.subject_id.clone(),
        local_path: req.local_path.clone(),
        s3_uri: req.s3_uri.clone(),
        transfer_scope_id,
        snapshot_artifact: requested_snapshot_artifact,
        staged_path: None,
        raw: req.raw,
        file_mode,
        existing_file_policy,
        materialization_mode: assignment.mode,
        rootfs_materialization_mode: assignment.rootfs_mode,
        encryption: None,
        artifact_id: None,
        entry_role: None,
    }
    .to_json();
    let mut dk = dedup_key(
        PULL_KIND,
        &req.subject_id,
        &req.local_path,
        &req.s3_uri,
        req.raw,
        TransferDedupOptions {
            file_mode,
            existing_file_policy,
            snapshot_artifact: requested_snapshot_artifact,
            materialization_mode: assignment.mode,
            encryption: None,
        },
    );
    if let Some(rootfs_mode) = assignment.rootfs_mode {
        dk.push_str("\0rootfs-materialization=");
        dk.push_str(rootfs_mode.as_str());
    }
    if let Some(scope) = transfer_scope_id {
        dk.push_str("\0scope=");
        dk.push_str(&scope.to_string());
    }
    worker.submit_with_priority(
        PULL_KIND,
        &payload,
        Some(&req.subject_id),
        Some(&dk),
        JobPriority::new(req.priority),
    )
}

fn effective_pull_snapshot_artifact(
    req: &PullFileRequest,
) -> Result<Option<SnapshotArtifact>, Status> {
    Ok(
        parse_snapshot_artifact(req.snapshot_artifact, "snapshot_artifact")?
            .or_else(|| snapshot_artifact_from_local_path(Path::new(&req.local_path))),
    )
}

fn pull_artifact_dedup_key(
    artifact_dedup: &str,
    assignment: impl Into<MaterializationAssignment>,
) -> String {
    let assignment = assignment.into();
    let mut key = format!("{artifact_dedup}{}", assignment.mode.dedup_suffix());
    if let Some(rootfs_mode) = assignment.rootfs_mode {
        key.push_str("\0rootfs-materialization=");
        key.push_str(rootfs_mode.as_str());
    }
    key
}

fn subject_class_artifact_dedup_key(subject: &SubjectId, artifact_id: &ArtifactId) -> String {
    format!("subject-kind={}\0artifact-id={artifact_id}", subject.kind())
}

#[derive(Clone, Copy, Debug)]
struct ArtifactRetryIdentity<'a> {
    artifact_id: &'a str,
    dedup: &'a str,
}

impl<'a> ArtifactRetryIdentity<'a> {
    fn scoped(artifact_id: &'a str, dedup: &'a str) -> Self {
        Self { artifact_id, dedup }
    }

    #[cfg(test)]
    fn legacy(artifact_id: &'a str) -> Self {
        Self::scoped(artifact_id, artifact_id)
    }
}

/// Within one subject class, an artifact id identifies one immutable file set
/// and key across every materialization layout. Validate all durable mode
/// records against that identity, but only reuse a job whose layout contract
/// matches the request. A live job from before rootfs-scoped assignments
/// either satisfies a dense rootfs request or blocks an incompatible retry
/// until it becomes terminal.
fn classify_pull_artifact_retry<E: JobExecutor>(
    worker: &BackgroundWorker<E>,
    identity: ArtifactRetryIdentity<'_>,
    assignment: impl Into<MaterializationAssignment>,
    entries: &[ArtifactEntryPayload],
    kek: Option<&OrgId>,
    wrapped_dek: Option<&[u8]>,
    mut is_authorized: impl FnMut(&pplx_space_common_jobs::Job) -> bool,
) -> Result<Option<pplx_space_common_jobs::Job>, Status> {
    let ArtifactRetryIdentity { artifact_id, dedup } = identity;
    let assignment = assignment.into();
    let mut reusable = None;
    let mut incompatible_legacy_live = None;
    for existing_mode in [
        MaterializationMode::Disabled,
        MaterializationMode::Shadow,
        MaterializationMode::Enabled,
    ] {
        let rootfs_modes = if assignment.rootfs_mode.is_some() {
            [
                None,
                Some(MaterializationMode::Disabled),
                Some(MaterializationMode::Shadow),
                Some(MaterializationMode::Enabled),
            ]
            .as_slice()
        } else {
            [None].as_slice()
        };
        for &rootfs_mode in rootfs_modes {
            let existing_assignment = MaterializationAssignment {
                mode: existing_mode,
                rootfs_mode,
            };
            let dedup_key = pull_artifact_dedup_key(dedup, existing_assignment);
            let Some(existing) = worker.find_job_by_dedup_key(PULL_ARTIFACT_KIND, &dedup_key)?
            else {
                continue;
            };
            if !is_authorized(&existing) {
                continue;
            }
            let Some((job, _)) =
                classify_artifact_retry(existing, artifact_id, entries, kek, wrapped_dek)?
            else {
                continue;
            };
            if existing_assignment == assignment
                || (existing_assignment.rootfs_mode.is_none()
                    && assignment.rootfs_mode == Some(MaterializationMode::Disabled)
                    && existing_assignment.mode == assignment.mode)
            {
                reusable = Some(job);
            } else if existing_assignment.rootfs_mode.is_none()
                && assignment.rootfs_mode.is_some()
                && !job.is_terminal()
            {
                incompatible_legacy_live = Some(job);
            }
        }
    }
    if reusable.is_some() {
        return Ok(reusable);
    }
    if let Some(job) = incompatible_legacy_live {
        return Err(Status::unavailable(format!(
            "artifact_id {artifact_id} has legacy materialization job {} still {}; retry after it reaches a terminal state",
            job.job_id, job.status
        )));
    }
    Ok(None)
}

#[tonic::async_trait]
impl VolumeManager for VolumeManagerService {
    async fn attach_volume(
        &self,
        request: Request<AttachVolumeRequest>,
    ) -> Result<Response<AttachVolumeResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "attach_volume", SANDBOX_MANAGER_PEERS)?;
        let _admission = self.transfer_admission.enter()?;
        let req = request.into_inner();
        let spec = req
            .volume
            .ok_or_else(|| Status::invalid_argument("volume spec required"))?;
        let attachment = proto_to_attachment(spec);
        let sandbox_id = req
            .sandbox_id
            .parse::<db::AttachmentSandboxKey>()
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox_id: {error}")))?;
        let conn = self.lock_db()?;
        db::upsert_attachment(&conn, &sandbox_id, &attachment).map_err(db_err)?;
        tracing::info!(
            sandbox_id = %req.sandbox_id,
            volume_name = %attachment.name,
            bucket = %attachment.s3_bucket,
            "volume attached"
        );
        Ok(Response::new(AttachVolumeResponse {}))
    }

    async fn detach_volume(
        &self,
        request: Request<DetachVolumeRequest>,
    ) -> Result<Response<DetachVolumeResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "detach_volume", SANDBOX_MANAGER_PEERS)?;
        let _admission = self.transfer_admission.enter()?;
        let req = request.into_inner();
        let sandbox_id = req
            .sandbox_id
            .parse::<db::AttachmentSandboxKey>()
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox_id: {error}")))?;
        let conn = self.lock_db()?;
        db::delete_attachment(&conn, &sandbox_id, &req.volume_name).map_err(db_err)?;
        tracing::info!(
            sandbox_id = %req.sandbox_id,
            volume_name = %req.volume_name,
            "volume detached"
        );
        Ok(Response::new(DetachVolumeResponse {}))
    }

    async fn list_volumes(
        &self,
        request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "list_volumes", SANDBOX_MANAGER_PEERS)?;
        let req = request.into_inner();
        let sandbox_id = req
            .sandbox_id
            .parse::<db::AttachmentSandboxKey>()
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox_id: {error}")))?;
        let conn = self.lock_db()?;
        let attachments = db::list_attachments(&conn, &sandbox_id).map_err(db_err)?;
        Ok(Response::new(ListVolumesResponse {
            volumes: attachments.into_iter().map(attachment_to_proto).collect(),
        }))
    }

    async fn volume_read_dir(
        &self,
        request: Request<VolumeReadDirRequest>,
    ) -> Result<Response<VolumeReadDirResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "volume_read_dir", ROOT_PEERS)?;
        Err(Status::unimplemented(
            "VolumeReadDir is not yet implemented",
        ))
    }

    async fn volume_stat(
        &self,
        request: Request<VolumeStatRequest>,
    ) -> Result<Response<VolumeStatResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "volume_stat", ROOT_PEERS)?;
        Err(Status::unimplemented("VolumeStat is not yet implemented"))
    }

    type VolumeReadStream = tokio_stream::Once<Result<VolumeReadResponse, Status>>;

    async fn volume_read(
        &self,
        request: Request<VolumeReadRequest>,
    ) -> Result<Response<Self::VolumeReadStream>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "volume_read", ROOT_PEERS)?;
        Err(Status::unimplemented("VolumeRead is not yet implemented"))
    }

    async fn push_file(
        &self,
        request: Request<PushFileRequest>,
    ) -> Result<Response<PushFileResponse>, Status> {
        let peer = self.peer_authorizer.authorize_request(
            &request,
            "push_file",
            TRANSFER_MANAGER_PEERS,
        )?;
        let admission = self.transfer_admission.enter()?;
        let req = request.into_inner();
        let subject = parse_subject(&req.subject_id)?;
        self.authorize_sandbox_subject(&peer, &subject, "push_file")?;
        parse_uri(&req.s3_uri)?;
        let job_id = submit_push(&self.worker, &self.staging, &req).await?;
        drop(admission);
        let job = self
            .worker
            .wait_for_job(&job_id, self.worker.worst_case_runtime())
            .await?;
        if job.status == "completed" {
            Ok(Response::new(PushFileResponse {
                bytes_uploaded: job_bytes_to_u64(job.bytes_processed)?,
            }))
        } else {
            Err(Status::internal(format!(
                "push failed: {}",
                job.error.as_deref().unwrap_or("unknown error")
            )))
        }
    }

    async fn pull_file(
        &self,
        request: Request<PullFileRequest>,
    ) -> Result<Response<PullFileResponse>, Status> {
        let peer = self.peer_authorizer.authorize_request(
            &request,
            "pull_file",
            TRANSFER_MANAGER_PEERS,
        )?;
        let admission = self.transfer_admission.enter()?;
        let req = request.into_inner();
        let subject = parse_subject(&req.subject_id)?;
        self.authorize_transfer_subject(&peer, &subject, "pull_file")?;
        parse_uri(&req.s3_uri)?;
        let artifact = effective_pull_snapshot_artifact(&req)?;
        let assignment = self.materialization_assignment(&subject, [artifact]);
        let job_id = submit_pull(&self.worker, &req, assignment)?;
        drop(admission);
        let job = self
            .worker
            .wait_for_job(&job_id, self.worker.worst_case_runtime())
            .await?;
        if job.status == "completed" {
            Ok(Response::new(PullFileResponse {
                bytes_downloaded: job_bytes_to_u64(job.bytes_processed)?,
            }))
        } else {
            Err(pull_job_failure_status(&job))
        }
    }

    async fn submit_push_file(
        &self,
        request: Request<PushFileRequest>,
    ) -> Result<Response<SubmitTransferResponse>, Status> {
        let peer = self.peer_authorizer.authorize_request(
            &request,
            "submit_push_file",
            TRANSFER_MANAGER_PEERS,
        )?;
        let _admission = self.transfer_admission.enter()?;
        let req = request.into_inner();
        let subject = parse_subject(&req.subject_id)?;
        self.authorize_sandbox_subject(&peer, &subject, "submit_push_file")?;
        parse_uri(&req.s3_uri)?;
        let job_id = submit_push(&self.worker, &self.staging, &req).await?;
        Ok(Response::new(SubmitTransferResponse {
            job_id: job_id.to_string(),
        }))
    }

    async fn submit_pull_file(
        &self,
        request: Request<PullFileRequest>,
    ) -> Result<Response<SubmitTransferResponse>, Status> {
        let peer = self.peer_authorizer.authorize_request(
            &request,
            "submit_pull_file",
            TRANSFER_MANAGER_PEERS,
        )?;
        let _admission = self.transfer_admission.enter()?;
        let req = request.into_inner();
        let subject = parse_subject(&req.subject_id)?;
        self.authorize_transfer_subject(&peer, &subject, "submit_pull_file")?;
        parse_uri(&req.s3_uri)?;
        let artifact = effective_pull_snapshot_artifact(&req)?;
        let assignment = self.materialization_assignment(&subject, [artifact]);
        let job_id = submit_pull(&self.worker, &req, assignment)?;
        Ok(Response::new(SubmitTransferResponse {
            job_id: job_id.to_string(),
        }))
    }

    async fn get_transfer_job(
        &self,
        request: Request<GetTransferJobRequest>,
    ) -> Result<Response<GetTransferJobResponse>, Status> {
        let peer = self.peer_authorizer.authorize_request(
            &request,
            "get_transfer_job",
            TRANSFER_MANAGER_PEERS,
        )?;
        let req = request.into_inner();
        let job = self
            .worker
            .get_job(&parse_job_id(&req.job_id)?)?
            .ok_or_else(|| Status::not_found(format!("transfer job not found: {}", req.job_id)))?;
        self.authorize_job(&peer, &job, "get_transfer_job")?;
        Ok(Response::new(GetTransferJobResponse {
            job: Some(job_to_proto(job)?),
        }))
    }

    async fn wait_transfer_job(
        &self,
        request: Request<GetTransferJobRequest>,
    ) -> Result<Response<GetTransferJobResponse>, Status> {
        let peer = self.peer_authorizer.authorize_request(
            &request,
            "wait_transfer_job",
            TRANSFER_MANAGER_PEERS,
        )?;
        let request = request.into_inner();
        let job_id = parse_job_id(&request.job_id)?;
        let job = self.worker.get_job(&job_id)?.ok_or_else(|| {
            Status::not_found(format!("transfer job not found: {}", request.job_id))
        })?;
        self.authorize_job(&peer, &job, "wait_transfer_job")?;
        Ok(Response::new(
            wait_for_transfer_job(&self.worker, &job_id).await?,
        ))
    }

    async fn list_transfer_jobs(
        &self,
        request: Request<ListTransferJobsRequest>,
    ) -> Result<Response<ListTransferJobsResponse>, Status> {
        let peer = self.peer_authorizer.authorize_request(
            &request,
            "list_transfer_jobs",
            TRANSFER_MANAGER_PEERS,
        )?;
        let req = request.into_inner();
        match req.subject_id.as_deref() {
            Some(subject_id) => {
                let subject = parse_subject(subject_id)?;
                self.authorize_transfer_subject(&peer, &subject, "list_transfer_jobs")?;
            }
            None if !matches!(peer.principal(), UdsPeerPrincipal::Root) => {
                return Err(Status::permission_denied(
                    "subject_id filter is required for this local peer",
                ));
            }
            None => {}
        }
        let status_str = req
            .status_filter
            .and_then(|v| TransferJobStatus::try_from(v).ok())
            .and_then(|s| match s {
                TransferJobStatus::Pending => Some("pending"),
                TransferJobStatus::Running => Some("running"),
                TransferJobStatus::Completed => Some("completed"),
                TransferJobStatus::Failed => Some("failed"),
                TransferJobStatus::Cancelling => Some("cancelling"),
                TransferJobStatus::Cancelled => Some("cancelled"),
                TransferJobStatus::Unspecified => None,
            });
        let jobs = self
            .worker
            .list_jobs(req.subject_id.as_deref(), status_str, req.limit)?;
        Ok(Response::new(ListTransferJobsResponse {
            jobs: jobs
                .into_iter()
                .map(job_to_proto)
                .collect::<Result<Vec<_>, Status>>()?,
        }))
    }

    async fn cancel_transfer(
        &self,
        request: Request<CancelTransferRequest>,
    ) -> Result<Response<CancelTransferResponse>, Status> {
        let peer = self.peer_authorizer.authorize_request(
            &request,
            "cancel_transfer",
            TRANSFER_MANAGER_PEERS,
        )?;
        let _admission = self.transfer_admission.enter()?;
        let request = request.into_inner();
        let job_ids = match request.selector {
            Some(CancelTransferSelector::JobId(job_id)) => {
                let job_id = parse_job_id(&job_id)?;
                let job = self.worker.get_job(&job_id)?.ok_or_else(|| {
                    Status::not_found(format!("transfer job not found: {job_id}"))
                })?;
                self.authorize_job(&peer, &job, "cancel_transfer")?;
                if !matches!(job.kind.as_str(), PULL_KIND | PULL_ARTIFACT_KIND) {
                    return Err(Status::failed_precondition(format!(
                        "transfer job {job_id} is not a pull"
                    )));
                }
                vec![job_id]
            }
            Some(CancelTransferSelector::TransferScopeId(scope)) => {
                let scope: TransferScopeId = scope.parse().map_err(|error| {
                    Status::invalid_argument(format!("transfer_scope_id: {error}"))
                })?;
                let conn = self.lock_db()?;
                transfer_scope_job_ids(&conn, scope).map_err(|error| {
                    Status::internal(format!("query transfer scope jobs: {error}"))
                })?
            }
            None => {
                return Err(Status::invalid_argument(
                    "cancel transfer selector is required",
                ));
            }
        };

        // Validate the complete scope before cancelling any member so a
        // mixed or legacy scope cannot be partially mutated.
        for job_id in &job_ids {
            let job = self
                .worker
                .get_job(job_id)?
                .ok_or_else(|| Status::not_found(format!("transfer job not found: {job_id}")))?;
            self.authorize_job(&peer, &job, "cancel_transfer")?;
        }

        // Request every cancellation concurrently so a large restore scope
        // does not keep later transfers writing while the first one drains.
        let cancelled =
            futures_util::future::join_all(job_ids.into_iter().map(|job_id| async move {
                self.worker
                    .cancel_job_and_wait(&job_id, self.worker.worst_case_runtime())
                    .await
            }))
            .await
            .into_iter()
            .collect::<Result<Vec<_>, Status>>()?;
        Ok(Response::new(CancelTransferResponse {
            jobs: cancelled
                .into_iter()
                .map(job_to_proto)
                .collect::<Result<Vec<_>, _>>()?,
        }))
    }

    async fn push_files_async(
        &self,
        request: Request<PushFilesAsyncRequest>,
    ) -> Result<Response<PushFilesAsyncResponse>, Status> {
        let peer = self.peer_authorizer.authorize_request(
            &request,
            "push_files_async",
            TRANSFER_MANAGER_PEERS,
        )?;
        let _admission = self.transfer_admission.enter()?;
        let req = request.into_inner();
        if req.files.is_empty() {
            return Err(Status::invalid_argument("files must not be empty"));
        }
        let artifact_id = parse_artifact_id(&req.artifact_id)?;
        let subject = parse_subject(&req.subject_id)?;
        self.authorize_sandbox_subject(&peer, &subject, "push_files_async")?;
        // Encrypted artifacts bind the wrap AAD to (org, sandbox), so they
        // require a sandbox subject; plaintext pushes accept any valid
        // subject (matching pull — vol-mgr stays artifact-generic).
        let sandbox_id = if req.encryption.is_some() {
            match &subject {
                SubjectId::Sandbox(id) => Some(*id),
                SubjectId::Template(_) => {
                    return Err(Status::invalid_argument(
                        "PushFilesAsync / PullFilesAsync require a sandbox subject",
                    ));
                }
            }
        } else {
            None
        };
        let dedup = artifact_id.to_string();

        // Validate every entry URI up front, and — for encrypted artifacts —
        // every role (non-empty, bounded, NUL-free, and unique so two
        // entries can't share a per-chunk AAD). Fail synchronously here
        // rather than after a DEK is issued or a job submitted.
        for (idx, entry) in req.files.iter().enumerate() {
            parse_uri(&entry.s3_uri).map_err(|e| {
                Status::invalid_argument(format!("files[{idx}].s3_uri: {}", e.message()))
            })?;
        }
        if req.encryption.is_some() {
            validate_entry_roles(req.files.iter().map(|f| f.role.as_str()))?;
        }

        // Parse the expected owner once (push carries no wrapped DEK — vol-mgr
        // mints it). Reused for the retry-equivalence check and the issue.
        let kek = req
            .encryption
            .map(parse_batch_encryption_config)
            .transpose()?;
        let subject_id = req.subject_id;

        // The artifact's entries as they will persist, minus `staged_path`
        // (filled after staging). Built now so the retry-equivalence check can
        // compare against the persisted record.
        let mut entries: Vec<ArtifactEntryPayload> = req
            .files
            .into_iter()
            .enumerate()
            .map(|(index, entry)| push_batch_entry_payload(entry, index))
            .collect::<Result<_, Status>>()?;

        // Idempotent recovery, before any filesystem work or DEK issue: an
        // equivalent retry returns the same job + persisted wrap; a retry whose
        // contents diverge from the persisted artifact is rejected; a retry
        // after a terminally-failed attempt falls through to mint a fresh job
        // (returning the failed job would report failure as success and leak
        // its wrap back as if the upload had landed).
        if let Some(existing) = self
            .worker
            .find_job_by_dedup_key(PUSH_ARTIFACT_KIND, &dedup)?
            && let Some((existing, persisted)) =
                classify_artifact_retry(existing, &dedup, &entries, kek.as_ref(), None)?
        {
            return Ok(Response::new(PushFilesAsyncResponse {
                job_id: existing.job_id.to_string(),
                wrapped_dek: persisted.encryption.map(|e| e.wrapped_dek),
            }));
        }

        // Pre-stage every source into vol-mgr-owned staging before issuing
        // the DEK: a missing/unreadable source fails fast, and the upload is
        // insulated from a later unlink of the caller's original between
        // submit and execute. On any staging failure, unstage what we
        // already staged.
        let mut staged_paths: Vec<PathBuf> = Vec::with_capacity(entries.len());
        for (idx, entry) in entries.iter().enumerate() {
            match stage_push_source(&self.staging, &entry.local_path).await {
                Ok(staged) => staged_paths.push(staged),
                Err(status) => {
                    for already in &staged_paths {
                        unstage_push_source(already).await;
                    }
                    return Err(Status::new(
                        status.code(),
                        format!("files[{idx}].local_path: {}", status.message()),
                    ));
                }
            }
        }

        // Issue exactly one DEK for the artifact when encryption is set.
        // credmgr binds it to the sandbox's durable owner and this node's
        // assignment to the sandbox. The plaintext DEK is dropped here (the
        // `Zeroizing` wrapper scrubs it); every entry re-unwraps from the
        // persisted wrap at execute time, so the caller never sees the
        // plaintext bytes.
        let encryption = match kek {
            None => None,
            Some(organization_id) => {
                // `kek.is_some()` iff `req.encryption.is_some()`, which is the
                // exact condition under which `sandbox_id` was resolved above.
                let sandbox_id =
                    sandbox_id.expect("encrypted artifact ⇒ sandbox subject resolved above");
                let issued = match crate::credentials::issue_dek(&sandbox_id, organization_id).await
                {
                    Ok(issued) => issued,
                    Err(e) => {
                        for already in &staged_paths {
                            unstage_push_source(already).await;
                        }
                        return Err(issue_dek_error_to_status(e));
                    }
                };
                Some(TransferEncryption {
                    organization_id,
                    wrapped_dek: issued.wrapped_dek,
                })
            }
        };

        // Fill staged paths and submit the durable artifact record as ONE job.
        // The submit persists the record — including the wrap — BEFORE any
        // upload runs, so recovery never depends on this response.
        for (entry, staged) in entries.iter_mut().zip(staged_paths.iter()) {
            entry.staged_path = Some(staged.to_string_lossy().into_owned());
        }
        let payload = ArtifactPayload {
            subject_id: subject_id.clone(),
            artifact_id: dedup.clone(),
            entries,
            materialization_mode: MaterializationMode::Disabled,
            rootfs_materialization_mode: None,
            encryption,
        };
        let job_id = self.worker.submit(
            PUSH_ARTIFACT_KIND,
            &payload.to_json(),
            Some(&subject_id),
            Some(&dedup),
        )?;

        // Race guard for two concurrent first submits of the same artifact_id:
        // `submit` dedups onto the winner's pending/running job, so the
        // persisted payload is the winner's and our staged copies are orphans.
        // Compare the full staged-path set (not just the first entry) to decide
        // whether we won; unstage our copies on a dedup loss; return the wrap
        // that is actually persisted (== what execute will use).
        let persisted = self
            .worker
            .get_job(&job_id)?
            .ok_or_else(|| Status::internal("submitted artifact job vanished"))?;
        let persisted_payload = ArtifactPayload::from_json(&persisted.payload)
            .map_err(|e| Status::internal(format!("read artifact payload: {}", e.message())))?;
        let ours_won = persisted_payload.entries.len() == staged_paths.len()
            && persisted_payload
                .entries
                .iter()
                .zip(&staged_paths)
                .all(|(e, s)| e.staged_path.as_deref() == Some(s.to_string_lossy().as_ref()));
        if !ours_won {
            for staged in &staged_paths {
                unstage_push_source(staged).await;
            }
        }

        Ok(Response::new(PushFilesAsyncResponse {
            job_id: job_id.to_string(),
            wrapped_dek: persisted_payload.encryption.map(|e| e.wrapped_dek),
        }))
    }

    async fn pull_files_async(
        &self,
        request: Request<PullFilesAsyncRequest>,
    ) -> Result<Response<PullFilesAsyncResponse>, Status> {
        let peer = self.peer_authorizer.authorize_request(
            &request,
            "pull_files_async",
            TRANSFER_MANAGER_PEERS,
        )?;
        let _admission = self.transfer_admission.enter()?;
        let req = request.into_inner();
        if req.files.is_empty() {
            return Err(Status::invalid_argument("files must not be empty"));
        }
        let artifact_id = parse_artifact_id(&req.artifact_id)?;
        let subject = parse_subject(&req.subject_id)?;
        self.authorize_transfer_subject(&peer, &subject, "pull_files_async")?;
        // Encrypted artifacts require a sandbox subject so the wrap AAD's
        // `(org, sandbox, artifact_id)` reproduces at execute time; plaintext
        // pulls accept any subject.
        let subject = if req.encryption.is_some() {
            match subject {
                SubjectId::Sandbox(id) => SubjectId::Sandbox(id),
                SubjectId::Template(_) => {
                    return Err(Status::invalid_argument(
                        "PushFilesAsync / PullFilesAsync require a sandbox subject",
                    ));
                }
            }
        } else {
            subject
        };
        for (idx, entry) in req.files.iter().enumerate() {
            parse_uri(&entry.s3_uri).map_err(|e| {
                Status::invalid_argument(format!("files[{idx}].s3_uri: {}", e.message()))
            })?;
        }
        if req.encryption.is_some() {
            validate_entry_roles(req.files.iter().map(|f| f.role.as_str()))?;
        }

        // Batch entries don't carry `raw`; the standard transparent-
        // decompress path runs on every entry. Raw-byte pulls
        // (template-manager) keep using single-file `SubmitPullFile`.
        let entries: Vec<ArtifactEntryPayload> = req
            .files
            .into_iter()
            .enumerate()
            .map(|(index, entry)| pull_batch_entry_payload(entry, index))
            .collect::<Result<_, Status>>()?;

        let dedup = artifact_id.to_string();
        let scoped_dedup = subject_class_artifact_dedup_key(&subject, &artifact_id);
        let assignment = self.materialization_assignment(
            &subject,
            entries.iter().map(effective_entry_snapshot_artifact),
        );
        let job_dedup = pull_artifact_dedup_key(&scoped_dedup, assignment);
        let encryption = req
            .encryption
            .map(parse_batch_decryption_config)
            .transpose()?;
        let subject_id = req.subject_id;

        // Idempotent recovery: an equivalent retry returns the same job;
        // contents that diverge from the persisted artifact (entries or the
        // supplied wrap) are rejected; a terminally-failed prior attempt falls
        // through to mint a fresh job so a corrected retry can make progress
        // (returning the failed job would let a `GetTransferJob` poll see the
        // old failure and never advance).
        let kek_id = encryption.as_ref().map(|e| &e.organization_id);
        let wrap = encryption.as_ref().map(|e| e.wrapped_dek.as_slice());
        let existing = classify_pull_artifact_retry(
            &self.worker,
            ArtifactRetryIdentity::scoped(&dedup, &scoped_dedup),
            assignment,
            &entries,
            kek_id,
            wrap,
            |job| artifact_job_matches_subject_class(job, &subject),
        )?;
        // During rollout, recover a same-class job written with the old
        // subject-agnostic key. A collision owned by the other manager class
        // is ignored so the class-scoped submission below can make progress.
        let existing = match existing {
            Some(existing) => Some(existing),
            None => classify_pull_artifact_retry(
                &self.worker,
                ArtifactRetryIdentity::scoped(&dedup, &dedup),
                assignment,
                &entries,
                kek_id,
                wrap,
                |job| artifact_job_matches_subject_class(job, &subject),
            )?,
        };
        if let Some(existing) = existing {
            self.authorize_artifact_job(&peer, &subject, &existing, "pull_files_async")?;
            return Ok(Response::new(PullFilesAsyncResponse {
                job_id: existing.job_id.to_string(),
            }));
        }

        let payload = ArtifactPayload {
            subject_id: subject_id.clone(),
            artifact_id: dedup.clone(),
            entries,
            materialization_mode: assignment.mode,
            rootfs_materialization_mode: assignment.rootfs_mode,
            encryption,
        };
        let job_id = self.worker.submit(
            PULL_ARTIFACT_KIND,
            &payload.to_json(),
            Some(&subject_id),
            Some(&job_dedup),
        )?;
        let persisted = self
            .worker
            .get_job(&job_id)?
            .ok_or_else(|| Status::internal("submitted artifact job vanished"))?;
        self.authorize_artifact_job(&peer, &subject, &persisted, "pull_files_async")?;
        Ok(Response::new(PullFilesAsyncResponse {
            job_id: job_id.to_string(),
        }))
    }
}

/// Parse a push-side [`BatchEncryptionConfig`] into the owner the caller
/// expects for the sandbox. credmgr chooses the KEK.
fn parse_batch_encryption_config(cfg: BatchEncryptionConfig) -> Result<OrgId, Status> {
    parse_expected_org(&cfg.organization_id)
}

/// Parse a pull-side [`BatchDecryptionConfig`] into the typed
/// [`TransferEncryption`] stored on the artifact's entries. The
/// caller-supplied `wrapped_dek` is the wrap recorded from the paired
/// `PushFilesAsync`; vol-mgr asks credmgr to unwrap it for the
/// artifact's sandbox at execute time.
fn parse_batch_decryption_config(cfg: BatchDecryptionConfig) -> Result<TransferEncryption, Status> {
    let organization_id = parse_expected_org(&cfg.organization_id)?;
    if cfg.wrapped_dek.is_empty() {
        return Err(Status::invalid_argument(
            "encryption.wrapped_dek must not be empty",
        ));
    }
    Ok(TransferEncryption {
        organization_id,
        wrapped_dek: cfg.wrapped_dek,
    })
}

fn parse_expected_org(raw: &str) -> Result<OrgId, Status> {
    raw.trim()
        .parse()
        .map_err(|e| Status::invalid_argument(format!("encryption.organization_id: {e}")))
}

/// Parse the caller-minted artifact id at the gRPC boundary.
fn parse_artifact_id(raw: &str) -> Result<ArtifactId, Status> {
    raw.trim()
        .parse()
        .map_err(|e| Status::invalid_argument(format!("artifact_id: {e}")))
}

fn parse_snapshot_artifact(raw: i32, field: &str) -> Result<Option<SnapshotArtifact>, Status> {
    ProtoSnapshotArtifact::try_from(raw)
        .map(snapshot_artifact_from_proto)
        .map_err(|_| Status::invalid_argument(format!("{field}: unknown enum value {raw}")))
}

fn push_batch_entry_payload(
    entry: PushFileBatchEntry,
    index: usize,
) -> Result<ArtifactEntryPayload, Status> {
    let snapshot_artifact = parse_snapshot_artifact(
        entry.snapshot_artifact,
        &format!("files[{index}].snapshot_artifact"),
    )?
    .or_else(|| snapshot_artifact_from_entry_role(&entry.role));
    Ok(ArtifactEntryPayload {
        snapshot_artifact,
        role: entry.role,
        local_path: entry.local_path,
        s3_uri: entry.s3_uri,
        staged_path: None,
        file_mode: None,
    })
}

fn pull_batch_entry_payload(
    entry: PullFileBatchEntry,
    index: usize,
) -> Result<ArtifactEntryPayload, Status> {
    let snapshot_artifact = parse_snapshot_artifact(
        entry.snapshot_artifact,
        &format!("files[{index}].snapshot_artifact"),
    )?
    .or_else(|| snapshot_artifact_from_entry_role(&entry.role));
    let file_mode = entry
        .file_mode
        .map(FileMode::new)
        .transpose()
        .map_err(|e| Status::invalid_argument(format!("files[{index}].file_mode: {e}")))?;
    Ok(ArtifactEntryPayload {
        snapshot_artifact,
        role: entry.role,
        local_path: entry.local_path,
        s3_uri: entry.s3_uri,
        staged_path: None,
        file_mode,
    })
}

/// Whether an idempotent retry's intended artifact matches the one already
/// persisted under the same `artifact_id`. Compares the entry tuples
/// (role, snapshot artifact, local_path, s3_uri, file_mode) and the expected
/// owning organization. The server-minted `wrapped_dek` is
/// compared only when the caller supplied one (pull): the push caller never
/// sends a wrap, so there is nothing to compare on that side. `staged_path` is
/// excluded — it is server-assigned and differs per attempt.
fn artifact_request_matches(
    persisted: &ArtifactPayload,
    entries: &[ArtifactEntryPayload],
    kek: Option<&OrgId>,
    wrapped_dek: Option<&[u8]>,
) -> bool {
    if persisted.entries.len() != entries.len() {
        return false;
    }
    let entries_match = persisted.entries.iter().zip(entries).all(|(p, r)| {
        p.role == r.role
            && effective_entry_snapshot_artifact(p) == effective_entry_snapshot_artifact(r)
            && p.local_path == r.local_path
            && p.s3_uri == r.s3_uri
            && p.file_mode == r.file_mode
    });
    if !entries_match {
        return false;
    }
    match (persisted.encryption.as_ref(), kek) {
        (None, None) => true,
        (Some(p), Some(org)) => {
            p.organization_id == *org && wrapped_dek.is_none_or(|w| p.wrapped_dek == w)
        }
        _ => false,
    }
}

fn effective_entry_snapshot_artifact(entry: &ArtifactEntryPayload) -> Option<SnapshotArtifact> {
    entry
        .snapshot_artifact
        .or_else(|| snapshot_artifact_from_entry_role(&entry.role))
}

/// Decide how to handle a [`BackgroundWorker::find_job_by_dedup_key`] hit for an
/// artifact retry:
///
/// - `Ok(Some((job, payload)))` — reuse the existing job: an equivalent retry
///   of a still-live or completed artifact. Idempotent.
/// - `Ok(None)` — the prior attempt is terminally `failed`; the caller mints a
///   fresh job for the same dedup key so a corrected retry can make progress.
///   Returning the failed job would report a failure as a success (and, on
///   push, leak its wrap back as if the upload had landed).
/// - `Err(FailedPrecondition)` — the same `artifact_id` is in use for a
///   *different* file set or KEK; surface loudly rather than acting on stale
///   persisted state (e.g. an upstream bug that derives the id from too few
///   inputs, or a stale id resurrected after a crash).
fn classify_artifact_retry(
    existing: pplx_space_common_jobs::Job,
    artifact_id: &str,
    entries: &[ArtifactEntryPayload],
    kek: Option<&OrgId>,
    wrapped_dek: Option<&[u8]>,
) -> Result<Option<(pplx_space_common_jobs::Job, ArtifactPayload)>, Status> {
    if existing.status == "failed" {
        return Ok(None);
    }
    let persisted = ArtifactPayload::from_json(&existing.payload)
        .map_err(|e| Status::internal(format!("recover artifact payload: {}", e.message())))?;
    if !artifact_request_matches(&persisted, entries, kek, wrapped_dek) {
        return Err(Status::failed_precondition(format!(
            "artifact_id {artifact_id} is already in use for a different file set or key"
        )));
    }
    Ok(Some((existing, persisted)))
}

/// Validate the per-entry roles of an encrypted artifact: each must be a
/// well-formed [`ArtifactEntryRole`] (non-empty, bounded, NUL-free), and
/// they must be **unique** across the artifact. Uniqueness is
/// load-bearing for non-interchangeability — two entries sharing a role
/// would derive the same per-chunk AAD under the one artifact DEK and so
/// become cryptographically swappable.
fn validate_entry_roles<'a>(roles: impl Iterator<Item = &'a str>) -> Result<(), Status> {
    let mut seen = HashSet::new();
    for (idx, raw) in roles.enumerate() {
        let role: ArtifactEntryRole = raw
            .parse()
            .map_err(|e| Status::invalid_argument(format!("files[{idx}].role: {e}")))?;
        if !seen.insert(role) {
            return Err(Status::invalid_argument(format!(
                "files[{idx}].role: duplicate role {raw:?} in artifact (entries must be distinct)"
            )));
        }
    }
    Ok(())
}

fn issue_dek_error_to_status(e: crate::credentials::IssueDekError) -> Status {
    use crate::credentials::IssueDekError;
    match e {
        IssueDekError::Connect(msg) => Status::unavailable(format!("credmgr connect: {msg}")),
        IssueDekError::Transient { code, message } => Status::new(code, message),
        IssueDekError::Permanent { code, message } => Status::new(code, message),
    }
}

#[cfg(test)]
mod tests;

fn parse_job_id(raw: &str) -> Result<pplx_space_common_jobs::JobId, Status> {
    raw.parse()
        .map_err(|error| Status::invalid_argument(format!("job_id: {error}")))
}
