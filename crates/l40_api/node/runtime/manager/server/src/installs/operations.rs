//! Durable requests and operation views for the shared install/delete worker.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::executor::{ExecutorParts, InstallExecutor};
use super::release_source::VerifiableRelease;
use super::source::{KernelVersion, RuntimeLine, SourceError, VerifiableKernel};
use super::staging::{self, PrepareStagingError};
use super::store::{self, JobOutcome, JobResult, StoreError};
use pplx_space_common_core::types::{ComponentIdentity, Digest, JobId, RuntimeIdentity};
use pplx_space_common_jobs::{BackgroundWorker, Job, JobStatus, ParseJobStatusError, WorkerConfig};
use pplx_space_node_runtime_core::catalog::{CatalogError, PinnedBootArtifact};
use tokio_util::sync::CancellationToken;

pub(super) const INSTALL_JOB_KIND: &str = "artifact-install";
const WORKER_LABEL: &str = "space-rtmgr.install";
/// One attempt's bound: a guest kernel at the slowest link a node should
/// have, with room to spare.
const JOB_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// How long a cancel waits for the job to stop.
const CANCEL_TIMEOUT: Duration = Duration::from_secs(30);
/// How long shutdown waits for a running job to yield.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Bounds on kernel operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstallLimits {
    /// The largest kernel or release archive an install downloads, and the
    /// largest binary a release archive may unpack.
    pub max_bytes: u64,
    /// How long a download may stall before the attempt fails.
    pub download_idle_timeout: Duration,
    /// Attempts per operation. Transfer failures retry with the shared
    /// worker's backoff; bytes that do not match what the source published
    /// never do.
    pub max_attempts: i32,
}

/// An install job's request: the shared job's payload.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub(crate) enum InstallOperation {
    InstallBootArtifact {
        pin: PinnedBootArtifact,
    },
    UninstallBootArtifact {
        identity: ComponentIdentity,
    },
    InstallKernel {
        kernel: VerifiableKernel,
    },
    UninstallKernel {
        version: KernelVersion,
        digest: Digest,
    },
    InstallRelease {
        release: VerifiableRelease,
        digest: Digest,
    },
    UninstallRelease {
        release: RuntimeIdentity,
        digest: Digest,
    },
}

impl InstallOperation {
    pub(crate) fn artifact(&self) -> super::publisher::NodeArtifactRef {
        use super::publisher::NodeArtifactRef;
        match self {
            Self::InstallBootArtifact { pin } => NodeArtifactRef::Boot(pin.identity().clone()),
            Self::UninstallBootArtifact { identity } => NodeArtifactRef::Boot(identity.clone()),
            Self::InstallKernel { kernel } => {
                NodeArtifactRef::Kernel(kernel.version.clone(), kernel.digest)
            }
            Self::UninstallKernel { version, digest } => {
                NodeArtifactRef::Kernel(version.clone(), *digest)
            }
            Self::InstallRelease { release, digest } => {
                NodeArtifactRef::Release(release.release.clone(), *digest)
            }
            Self::UninstallRelease { release, digest } => {
                NodeArtifactRef::Release(release.clone(), *digest)
            }
        }
    }

    /// What the operation is about, for job listings: a kernel version, or
    /// a release as `<runtime>-<version>`.
    pub(crate) fn subject(&self) -> String {
        match self {
            Self::InstallBootArtifact { pin } => {
                format!("{}-{}", pin.identity().kind, pin.identity().version)
            }
            Self::UninstallBootArtifact { identity } => {
                format!("{}-{}", identity.kind, identity.version)
            }
            Self::InstallKernel { kernel } => kernel.version.to_string(),
            Self::UninstallKernel { version, .. } => version.to_string(),
            Self::InstallRelease { release, .. } => release_subject(&release.release),
            Self::UninstallRelease { release, .. } => release_subject(release),
        }
    }

    /// Whether the operation removes what it names.
    pub(crate) const fn is_removal(&self) -> bool {
        matches!(
            self,
            Self::UninstallKernel { .. }
                | Self::UninstallRelease { .. }
                | Self::UninstallBootArtifact { .. }
        )
    }

    /// Repeats of one identity join the active job. A source location is
    /// transfer evidence; it does not create a second component identity.
    fn dedup_key(&self) -> String {
        match self {
            Self::InstallBootArtifact { pin } => format!(
                "install-component:{}:{}:{}",
                pin.identity().kind,
                pin.identity().version,
                pin.identity().digest
            ),
            Self::UninstallBootArtifact { identity } => format!(
                "uninstall-component:{}:{}:{}",
                identity.kind, identity.version, identity.digest
            ),
            Self::InstallKernel { kernel } => format!(
                "install-component:kernel:{}:{}",
                kernel.version, kernel.digest
            ),
            Self::UninstallKernel { version, digest } => format!("uninstall:{version}:{digest}"),
            Self::InstallRelease { release, digest } => {
                format!(
                    "install-release:{}:{digest}",
                    release_subject(&release.release)
                )
            }
            Self::UninstallRelease { release, digest } => {
                format!("uninstall-release:{}:{digest}", release_subject(release))
            }
        }
    }
}

fn release_subject(release: &RuntimeIdentity) -> String {
    format!("{}-{}", release.kind, release.version)
}

/// Where an operation is, as the caller sees it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OperationState {
    Queued,
    Downloading,
    Publishing,
    Removing,
    Cancelling,
    Succeeded(JobOutcome),
    Failed,
    Cancelled,
}

impl OperationState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Downloading => "downloading",
            Self::Publishing => "publishing",
            Self::Removing => "removing",
            Self::Cancelling => "cancelling",
            Self::Succeeded(_) => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub(crate) const fn is_finished(self) -> bool {
        matches!(self, Self::Succeeded(_) | Self::Failed | Self::Cancelled)
    }
}

impl std::fmt::Display for OperationState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Succeeded(outcome) => write!(formatter, "succeeded ({outcome})"),
            state => formatter.write_str(state.as_str()),
        }
    }
}

/// A kernel or release directory an operation published, found, or removed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OnDisk {
    pub(crate) path: PathBuf,
    /// The image's SHA-256.
    pub(crate) digest: Digest,
}

/// One kernel operation: its job and what it did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Operation {
    pub(crate) job_id: JobId,
    pub(crate) request: InstallOperation,
    pub(crate) state: OperationState,
    /// Set once the operation reached a kernel or release directory.
    pub(crate) on_disk: Option<OnDisk>,
    /// The last attempt's failure, as the worker recorded it.
    pub(crate) error: Option<String>,
    pub(crate) created_at: Option<jiff::Timestamp>,
    pub(crate) updated_at: Option<jiff::Timestamp>,
}

#[derive(Debug, thiserror::Error)]
pub enum OpenInstallerError {
    #[error("unsupported node architecture")]
    UnsupportedArchitecture,
    #[error("cannot prepare staging")]
    Staging(#[from] PrepareStagingError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("cannot load catalog")]
    Catalog(#[from] CatalogError),
    #[error("installer setup failed")]
    Task(#[from] tokio::task::JoinError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SubmitOperationError {
    #[error("no enabled source for this artifact")]
    Disabled,
    #[error("artifact identity is not pinned for this node or source")]
    NotPinned,
    #[error("artifact is not offered by the configured source")]
    NotOffered,
    #[error("artifact exceeds the configured install limit")]
    TooLarge,
    #[error(transparent)]
    Pins(#[from] CatalogError),
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error("worker refused operation")]
    Submit(#[source] Box<tonic::Status>),
    #[error(transparent)]
    Lookup(#[from] LookupOperationError),
    #[error("operation submission failed")]
    Task(#[from] tokio::task::JoinError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LookupOperationError {
    #[error("the worker could not read its jobs")]
    Worker(#[source] Box<tonic::Status>),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("job {job_id} carries an unreadable request")]
    Payload {
        job_id: JobId,
        #[source]
        source: serde_json::Error,
    },
    #[error("job {job_id} has an unknown status")]
    Status {
        job_id: JobId,
        #[source]
        source: ParseJobStatusError,
    },
    #[error("job {job_id} finished without recording what it did")]
    MissingResult { job_id: JobId },
    #[error("job {job_id} vanished")]
    Vanished { job_id: JobId },
    #[error("operation lookup task failed")]
    Task(#[from] tokio::task::JoinError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CancelOperationError {
    #[error("no operation {0}")]
    NotFound(JobId),
    #[error("operation {0} is past downloading and can no longer be cancelled")]
    TooLate(JobId),
    #[error(transparent)]
    Lookup(#[from] LookupOperationError),
    #[error("the worker could not cancel operation {job_id}")]
    Worker {
        job_id: JobId,
        #[source]
        source: Box<tonic::Status>,
    },
}

#[derive(Clone)]
pub(crate) struct Installer {
    worker: BackgroundWorker<InstallExecutor>,
    pub(super) executor: InstallExecutor,
}

impl Installer {
    /// Open the store in `state_dir`, clear staging, and re-queue every job a
    /// restart interrupted.
    pub(crate) async fn open(
        parts: ExecutorParts,
        state_dir: &Path,
    ) -> Result<Self, OpenInstallerError> {
        let limits = parts.limits;
        let prepared = parts.staging.clone();
        tokio::task::spawn_blocking(move || staging::prepare(&prepared)).await??;
        // An unsafe catalog refuses to start installs at all, rather than
        // failing each one later.
        let pins = parts.pins.clone();
        tokio::task::spawn_blocking(move || pins.catalog()).await??;
        let db = store::open(state_dir).await?;
        let orphaned = store::delete_orphaned_job_results(&db)?;
        if orphaned > 0 {
            tracing::info!(orphaned, "dropped results of pruned kernel jobs");
        }
        let db = Arc::new(Mutex::new(db));
        let executor = InstallExecutor::new(parts, db.clone());
        let config = WorkerConfig::new(1, JOB_TIMEOUT, limits.max_attempts)
            .with_kind_concurrency(INSTALL_JOB_KIND, 1)
            .with_recovery_job_kind(INSTALL_JOB_KIND);
        let worker = BackgroundWorker::new(db, executor.clone(), config);
        worker.recover_and_log(WORKER_LABEL);
        Ok(Self { worker, executor })
    }

    /// Source resolution is bounded by the source's own deadline. The worker
    /// owns all transfers and node-manager mutation calls.
    pub(crate) async fn install(
        &self,
        line: RuntimeLine,
        version: KernelVersion,
        expected_digest: Digest,
    ) -> Result<Operation, SubmitOperationError> {
        let context = self.executor.context();
        let source = context
            .source
            .get(line.runtime)
            .ok_or(SubmitOperationError::Disabled)?;
        let listed = source
            .list(line, context.host_arch)
            .await?
            .into_iter()
            .find(|kernel| kernel.version == version)
            .ok_or(SubmitOperationError::NotOffered)?;
        let kernel = listed.download_request(expected_digest);
        if kernel.size > context.limits.max_bytes {
            return Err(SubmitOperationError::TooLarge);
        }
        let checking = self.executor.clone();
        let checked = kernel.clone();
        tokio::task::spawn_blocking(move || {
            let catalog = checking.context().pins.catalog()?;
            catalog
                .kernel(
                    checked.version.artifact_version(),
                    checking.context().host_arch,
                )
                .filter(|pin| {
                    pin.sha256 == checked.digest
                        && pin.size == checked.size
                        && pin.runtime == checked.line.runtime
                        && pin.runtime_line.as_str() == checked.line.release.to_string()
                })
                .ok_or(SubmitOperationError::NotPinned)?;
            Ok::<_, SubmitOperationError>(())
        })
        .await??;
        self.enqueue(InstallOperation::InstallKernel { kernel })
            .await
    }

    pub(crate) async fn uninstall(
        &self,
        version: KernelVersion,
        digest: Digest,
    ) -> Result<Operation, SubmitOperationError> {
        self.enqueue(InstallOperation::UninstallKernel { version, digest })
            .await
    }

    pub(crate) async fn install_release(
        &self,
        release: RuntimeIdentity,
        digest: Digest,
    ) -> Result<Operation, SubmitOperationError> {
        let context = self.executor.context();
        let releases = context
            .releases
            .get(release.kind)
            .ok_or(SubmitOperationError::Disabled)?;
        let archive = releases.source.archive(&release, context.host_arch);
        let checking = self.executor.clone();
        let release = tokio::task::spawn_blocking(move || {
            let architecture = checking.context().host_arch;
            let catalog = checking.context().pins.catalog()?;
            let pin = catalog
                .release(&release, architecture)
                .filter(|pin| pin.digest() == digest)
                .ok_or(SubmitOperationError::NotPinned)?;
            Ok::<_, SubmitOperationError>(VerifiableRelease {
                release,
                architecture,
                archive,
                sha256: pin.archive_sha256,
            })
        })
        .await??;
        self.enqueue(InstallOperation::InstallRelease { release, digest })
            .await
    }

    pub(crate) async fn uninstall_release(
        &self,
        release: RuntimeIdentity,
        digest: Digest,
    ) -> Result<Operation, SubmitOperationError> {
        if self.executor.context().releases.get(release.kind).is_none() {
            return Err(SubmitOperationError::Disabled);
        }
        self.enqueue(InstallOperation::UninstallRelease { release, digest })
            .await
    }

    pub(crate) fn boot_source(&self) -> Option<&Path> {
        self.executor.context().boot_source.as_deref()
    }

    pub(crate) async fn install_boot_artifact(
        &self,
        identity: ComponentIdentity,
    ) -> Result<Operation, SubmitOperationError> {
        if self.boot_source().is_none() {
            return Err(SubmitOperationError::Disabled);
        }
        let checking = self.clone();
        let pin = tokio::task::spawn_blocking(move || {
            let catalog = checking.catalog()?;
            let pin = catalog
                .boot_artifact(&identity, checking.executor.context().host_arch)
                .ok_or(SubmitOperationError::NotPinned)?;
            if pin.size() > checking.executor.context().limits.max_bytes {
                return Err(SubmitOperationError::TooLarge);
            }
            Ok::<_, SubmitOperationError>(pin.clone())
        })
        .await??;
        self.enqueue(InstallOperation::InstallBootArtifact { pin })
            .await
    }

    pub(crate) async fn uninstall_boot_artifact(
        &self,
        identity: ComponentIdentity,
    ) -> Result<Operation, SubmitOperationError> {
        self.enqueue(InstallOperation::UninstallBootArtifact { identity })
            .await
    }

    async fn enqueue(&self, request: InstallOperation) -> Result<Operation, SubmitOperationError> {
        let submitting = self.clone();
        // The blocking thread has no current span; carry the caller's, so the
        // job saves the request's trace context.
        let span = tracing::Span::current();
        let job_id =
            tokio::task::spawn_blocking(move || span.in_scope(|| submitting.submit(&request)))
                .await?
                .map_err(SubmitOperationError::Submit)?;
        Ok(self.lookup(job_id).await?)
    }

    pub(crate) fn catalog(
        &self,
    ) -> Result<pplx_space_node_runtime_core::catalog::RuntimeCatalog, CatalogError> {
        self.executor.context().pins.catalog()
    }

    pub(crate) fn releases_enabled(&self) -> bool {
        !self.executor.context().releases.is_empty()
    }
    fn submit(&self, operation: &InstallOperation) -> Result<JobId, Box<tonic::Status>> {
        self.worker
            .submit_json(
                INSTALL_JOB_KIND,
                operation,
                Some(&operation.subject()),
                Some(&operation.dedup_key()),
            )
            .map_err(Box::new)
    }

    /// The operation `job_id` names, which must exist.
    async fn lookup(&self, job_id: JobId) -> Result<Operation, LookupOperationError> {
        self.get(job_id)
            .await?
            .ok_or(LookupOperationError::Vanished { job_id })
    }

    pub(crate) async fn get(
        &self,
        job_id: JobId,
    ) -> Result<Option<Operation>, LookupOperationError> {
        let reading = self.clone();
        tokio::task::spawn_blocking(move || reading.get_blocking(job_id)).await?
    }

    fn get_blocking(&self, job_id: JobId) -> Result<Option<Operation>, LookupOperationError> {
        let job = self
            .worker
            .get_job(&job_id)
            .map_err(|status| LookupOperationError::Worker(Box::new(status)))?;
        let Some(job) = job.filter(|job| job.kind == INSTALL_JOB_KIND) else {
            return Ok(None);
        };
        let result = store::get_job_result(&self.executor.db(), &job_id)?;
        operation(&job, result).map(Some)
    }

    /// Cancel an operation that has not started publishing or removing, and
    /// return how it ended.
    ///
    /// The commit gate is held across the whole cancel, waiting for the job
    /// to stop included: the shared worker requests cancellation and waits
    /// in one call, and the decision to cancel must stay exclusive with a
    /// job entering its publication or removal until the job has seen the request. A job
    /// waiting on the gate watches its cancellation, so this cannot deadlock.
    pub(crate) async fn cancel(&self, job_id: JobId) -> Result<Operation, CancelOperationError> {
        let _gate = self.executor.context().commit_gate.lock().await;
        let current = self
            .get(job_id)
            .await?
            .ok_or(CancelOperationError::NotFound(job_id))?;
        if current.state.is_finished() {
            return Ok(current);
        }
        // Eligibility follows the durable commit step, not the worker's
        // status: a job queued for a retry after its node-manager call landed still has
        // that mutation to resolve, and cancellation cannot undo that mutation; one whose outcome is recorded is done
        // even while the worker has yet to mark it so.
        let reading = self.executor.clone();
        let committed =
            tokio::task::spawn_blocking(move || store::get_job_result(&reading.db(), &job_id))
                .await
                .map_err(LookupOperationError::from)?
                .map_err(LookupOperationError::from)?;
        if committed.is_some() {
            return Err(CancelOperationError::TooLate(job_id));
        }
        self.worker
            .cancel_job_and_wait(&job_id, CANCEL_TIMEOUT)
            .await
            .map_err(|status| CancelOperationError::Worker {
                job_id,
                source: Box::new(status),
            })?;
        Ok(self.lookup(job_id).await?)
    }

    /// Prune finished jobs on the worker's schedule until `shutdown`.
    pub(crate) async fn prune_finished_jobs(self, shutdown: CancellationToken) {
        self.worker
            .run_terminal_job_pruner(WORKER_LABEL, shutdown)
            .await;
    }

    /// Stop running jobs at a safe boundary. They stay queued, and the next
    /// start re-runs them.
    pub(crate) async fn shutdown(&self) {
        if !self.worker.yield_for_restart(SHUTDOWN_TIMEOUT).await {
            tracing::warn!("a kernel job did not yield before shutdown; the next start re-runs it");
        }
    }
}

/// The caller's view of a job and what it did.
fn operation(job: &Job, result: Option<JobResult>) -> Result<Operation, LookupOperationError> {
    let request: InstallOperation =
        serde_json::from_str(&job.payload).map_err(|source| LookupOperationError::Payload {
            job_id: job.job_id,
            source,
        })?;
    let status =
        job.status
            .parse::<JobStatus>()
            .map_err(|source| LookupOperationError::Status {
                job_id: job.job_id,
                source,
            })?;
    let in_progress = match &result {
        Some(JobResult::InProgress { phase, .. }) => Some(*phase),
        _ => None,
    };
    let state = match status {
        JobStatus::Pending => OperationState::Queued,
        JobStatus::Running => match in_progress {
            Some(store::JobPhase::Publishing) => OperationState::Publishing,
            Some(store::JobPhase::Removing) => OperationState::Removing,
            None if request.is_removal() => OperationState::Removing,
            None => OperationState::Downloading,
        },
        JobStatus::Cancelling => OperationState::Cancelling,
        JobStatus::Completed => match &result {
            Some(JobResult::Done { outcome, .. }) => OperationState::Succeeded(*outcome),
            _ => return Err(LookupOperationError::MissingResult { job_id: job.job_id }),
        },
        JobStatus::Failed => OperationState::Failed,
        JobStatus::Cancelled => OperationState::Cancelled,
    };
    let created_at = job_timestamp(&job.created_at);
    let updated_at = job
        .completed_at
        .as_deref()
        .or(job.started_at.as_deref())
        .and_then(job_timestamp)
        .or(created_at);
    Ok(Operation {
        job_id: job.job_id,
        request,
        state,
        on_disk: result.map(|result| OnDisk {
            path: result.path().to_path_buf(),
            digest: result.digest(),
        }),
        error: job.error.clone(),
        created_at,
        updated_at,
    })
}

/// The shared jobs table records UTC times as SQLite `datetime('now')` text.
fn job_timestamp(value: &str) -> Option<jiff::Timestamp> {
    value
        .parse::<jiff::civil::DateTime>()
        .ok()?
        .to_zoned(jiff::tz::TimeZone::UTC)
        .ok()
        .map(|zoned| zoned.timestamp())
}

#[cfg(test)]
mod tests;
