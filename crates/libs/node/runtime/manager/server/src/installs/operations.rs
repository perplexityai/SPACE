//! Kernel operations as callers see them: install and uninstall requests,
//! queued as jobs on the shared background worker, and the operation each
//! becomes.
//!
//! Both kinds are one job kind that runs one at a time, so they never
//! overlap and later requests queue. Requests are refused up front only on
//! cheap evidence (the listing, the install records, whether a directory
//! exists); anything that needs the bytes, such as comparing an external
//! kernel's image, is the job's work. See [`super::executor`] for what a job
//! does and how a re-run after a restart resolves it.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_common_core::types::{Arch, Digest, JobId, RuntimeKind, StorageUri};
use pplx_space_common_jobs::{BackgroundWorker, Job, JobStatus, ParseJobStatusError, WorkerConfig};
use tokio_util::sync::CancellationToken;

use super::executor::KernelExecutor;
use super::seed::{SeedError, SeedReport};
use super::source::{KernelSource, KernelVersion, RuntimeLine, SourceError, VerifiableKernel};
use super::staging::{self, ImageDigestError, PrepareStagingError};
use super::store::{self, JobOutcome, JobResult, KernelInstall, StoreError};

pub(super) const KERNEL_JOB_KIND: &str = "kernel";
const WORKER_LABEL: &str = "space-rtmgr.kernel";
pub(super) const STAGING_DIR: &str = "staging";
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
    /// The largest kernel an install downloads.
    pub max_bytes: u64,
    /// How long a download may stall before the attempt fails.
    pub download_idle_timeout: Duration,
    /// Attempts per operation. Transfer failures retry with the shared
    /// worker's backoff; bytes that do not match what the source published
    /// never do.
    pub max_attempts: i32,
}

/// A kernel job's request: the shared job's payload.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub(crate) enum KernelOperation {
    Install { kernel: VerifiableKernel },
    Uninstall { version: KernelVersion },
}

impl KernelOperation {
    pub(crate) fn version(&self) -> &KernelVersion {
        match self {
            Self::Install { kernel } => &kernel.version,
            Self::Uninstall { version } => version,
        }
    }

    /// Repeats of one request join the active job rather than queueing a
    /// second; the same version from another line is a different request.
    fn dedup_key(&self) -> String {
        match self {
            Self::Install { kernel } => format!(
                "install:{}:{}:{}",
                kernel.line.runtime, kernel.line.release, kernel.version
            ),
            Self::Uninstall { version } => format!("uninstall:{version}"),
        }
    }
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

/// A kernel directory an operation published, found, or removed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct KernelOnDisk {
    pub(crate) path: PathBuf,
    /// The image's SHA-256.
    pub(crate) digest: Digest,
}

/// One kernel operation: its job and what it did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Operation {
    pub(crate) job_id: JobId,
    pub(crate) request: KernelOperation,
    pub(crate) state: OperationState,
    /// Set once the operation reached a kernel directory.
    pub(crate) kernel: Option<KernelOnDisk>,
    /// The last attempt's failure, as the worker recorded it.
    pub(crate) error: Option<String>,
    pub(crate) created_at: Option<jiff::Timestamp>,
    pub(crate) updated_at: Option<jiff::Timestamp>,
}

/// Why a kernel version is already taken by something else.
#[derive(Debug, thiserror::Error)]
pub(crate) enum KernelConflict {
    #[error("kernel {version} is already installed by the runtime manager from {location}")]
    InstalledFromElsewhere {
        version: KernelVersion,
        location: Box<StorageUri>,
    },
    #[error(
        "kernel {version} is recorded as installed at {} but is missing; restoring it is maintenance work",
        path.display()
    )]
    RecordedButMissing {
        version: KernelVersion,
        path: PathBuf,
    },
    #[error(
        "kernel {version} is provided externally (provider unknown) at {} with different bytes",
        path.display()
    )]
    ExternalDiffers {
        version: KernelVersion,
        path: PathBuf,
    },
    #[error("kernel {version} at {} cannot be compared", path.display())]
    Unreadable {
        version: KernelVersion,
        path: PathBuf,
        #[source]
        source: ImageDigestError,
    },
    #[error(
        "{} appeared while kernel {version} downloaded; nothing was replaced",
        path.display()
    )]
    Appeared {
        version: KernelVersion,
        path: PathBuf,
    },
    #[error(
        "an earlier attempt was publishing kernel {version} to {}, which now holds other bytes; left in place",
        path.display()
    )]
    PublishedOtherBytes {
        version: KernelVersion,
        path: PathBuf,
    },
    #[error(
        "kernel {version} at {} no longer holds the bytes the runtime manager installed; left in place",
        path.display()
    )]
    Changed {
        version: KernelVersion,
        path: PathBuf,
    },
    #[error(
        "kernel {version} is recorded at {}, outside the install root {}; restore the root it was installed under",
        path.display(),
        root.display()
    )]
    OutsideRoot {
        version: KernelVersion,
        path: PathBuf,
        root: PathBuf,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum OpenInstallerError {
    #[error("kernel installs are unsupported on this architecture")]
    UnsupportedArchitecture,
    #[error("cannot prepare kernel staging")]
    Staging(#[from] PrepareStagingError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("kernel installer setup task failed")]
    Task(#[from] tokio::task::JoinError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InstallKernelError {
    #[error("the kernel source publishes kernels for {published}, not {}", line.runtime)]
    RuntimeNotPublished {
        line: RuntimeLine,
        published: RuntimeKind,
    },
    #[error("{source_root} offers no kernel {version} for {line} on {architecture}")]
    NotOffered {
        version: KernelVersion,
        line: RuntimeLine,
        architecture: Arch,
        source_root: Box<StorageUri>,
    },
    #[error("the source publishes no MD5 for {location}, so it cannot be verified")]
    Unverifiable { location: Box<StorageUri> },
    #[error("{location} is {size} bytes, over the {limit}-byte install limit")]
    TooLarge {
        location: Box<StorageUri>,
        size: u64,
        limit: u64,
    },
    #[error(transparent)]
    Conflict(#[from] KernelConflict),
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("the worker refused the install")]
    Submit(#[source] Box<tonic::Status>),
    #[error(transparent)]
    Lookup(#[from] LookupOperationError),
    #[error("install check task failed")]
    Task(#[from] tokio::task::JoinError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum UninstallKernelError {
    #[error("kernel {version} is not installed")]
    NotInstalled { version: KernelVersion },
    #[error(
        "kernel {version} at {} is external (provider unknown); the runtime manager never removes it",
        path.display()
    )]
    External {
        version: KernelVersion,
        path: PathBuf,
    },
    #[error(
        "kernel {version} is selected by {}; switch the alias away before uninstalling it",
        alias.display()
    )]
    Selected {
        version: KernelVersion,
        alias: PathBuf,
    },
    #[error(
        "kernel {version} is recorded at {}, outside the install root {}; restore the root it was installed under",
        path.display(),
        root.display()
    )]
    OutsideRoot {
        version: KernelVersion,
        path: PathBuf,
        root: PathBuf,
    },
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("the worker refused the uninstall")]
    Submit(#[source] Box<tonic::Status>),
    #[error(transparent)]
    Lookup(#[from] LookupOperationError),
    #[error("uninstall check task failed")]
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
    worker: BackgroundWorker<KernelExecutor>,
    executor: KernelExecutor,
}

impl Installer {
    /// Open the store in `state_dir`, clear staging, and re-queue every job a
    /// restart interrupted.
    pub(crate) async fn open(
        kernels_root: PathBuf,
        state_dir: &Path,
        limits: InstallLimits,
        source: Arc<dyn KernelSource>,
    ) -> Result<Self, OpenInstallerError> {
        let host_arch =
            super::source::host_arch().ok_or(OpenInstallerError::UnsupportedArchitecture)?;
        let staging = state_dir.join(STAGING_DIR);
        let (prepared_staging, prepared_root) = (staging.clone(), kernels_root.clone());
        tokio::task::spawn_blocking(move || staging::prepare(&prepared_staging, &prepared_root))
            .await??;
        let db = store::open(state_dir).await?;
        let orphaned = store::delete_orphaned_job_results(&db)?;
        if orphaned > 0 {
            tracing::info!(orphaned, "dropped results of pruned kernel jobs");
        }
        let db = Arc::new(Mutex::new(db));
        let executor =
            KernelExecutor::new(kernels_root, staging, limits, host_arch, source, db.clone());
        // Before the worker starts, so no job runs meanwhile.
        let finishing = executor.clone();
        let finished = tokio::task::spawn_blocking(move || finishing.finish_ended_jobs()).await??;
        if finished > 0 {
            tracing::info!(finished, "finished renames of ended kernel jobs");
        }
        let config = WorkerConfig::new(1, JOB_TIMEOUT, limits.max_attempts)
            .with_kind_concurrency(KERNEL_JOB_KIND, 1)
            .with_recovery_job_kind(KERNEL_JOB_KIND);
        let worker = BackgroundWorker::new(db, executor.clone(), config);
        worker.recover_and_log(WORKER_LABEL);
        Ok(Self { worker, executor })
    }

    /// Queue an install of `version` from the kernels the source offers for
    /// `line`, refusing up front what the listing or the install records
    /// already rule out.
    pub(crate) async fn install(
        &self,
        line: RuntimeLine,
        version: KernelVersion,
    ) -> Result<Operation, InstallKernelError> {
        let context = self.executor.context();
        let published = context.source.runtime();
        if line.runtime != published {
            return Err(InstallKernelError::RuntimeNotPublished { line, published });
        }
        let listed = context
            .source
            .list(line, context.host_arch)
            .await?
            .into_iter()
            .find(|kernel| kernel.version == version)
            .ok_or_else(|| InstallKernelError::NotOffered {
                version: version.clone(),
                line,
                architecture: context.host_arch,
                source_root: Box::new(context.source.root().clone()),
            })?;
        let kernel = listed
            .verifiable()
            .ok_or_else(|| InstallKernelError::Unverifiable {
                location: Box::new(listed.location.clone()),
            })?;
        if kernel.size > context.limits.max_bytes {
            return Err(InstallKernelError::TooLarge {
                location: Box::new(kernel.location),
                size: kernel.size,
                limit: context.limits.max_bytes,
            });
        }
        let checking = self.executor.clone();
        let checked = kernel.clone();
        if let Some(conflict) =
            tokio::task::spawn_blocking(move || checking.recorded_conflict(&checked)).await??
        {
            return Err(conflict.into());
        }
        let job_id = self
            .submit(&KernelOperation::Install { kernel })
            .map_err(InstallKernelError::Submit)?;
        Ok(self.lookup(job_id).await?)
    }

    /// Queue removal of `version`, which the runtime manager must have
    /// installed and no alias may select.
    pub(crate) async fn uninstall(
        &self,
        version: KernelVersion,
    ) -> Result<Operation, UninstallKernelError> {
        let checking = self.executor.clone();
        let checked = version.clone();
        tokio::task::spawn_blocking(move || checking.removable(&checked)).await??;
        let job_id = self
            .submit(&KernelOperation::Uninstall { version })
            .map_err(UninstallKernelError::Submit)?;
        Ok(self.lookup(job_id).await?)
    }

    fn submit(&self, operation: &KernelOperation) -> Result<JobId, Box<tonic::Status>> {
        self.worker
            .submit_json(
                KERNEL_JOB_KIND,
                operation,
                Some(operation.version().as_str()),
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
        let Some(job) = job.filter(|job| job.kind == KERNEL_JOB_KIND) else {
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
    /// job entering its rename until the job has seen the request. A job
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
        // status: a job queued for a retry after its rename landed still has
        // that rename to resolve, and cancelling it would strand the kernel
        // without its install record; one whose outcome is recorded is done
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

    /// Kernels the runtime manager installed, for listings. Blocking.
    pub(crate) fn kernel_installs(&self) -> Result<Vec<KernelInstall>, StoreError> {
        store::list_kernel_installs(&self.executor.db())
    }

    pub(crate) fn kernels_root(&self) -> &Path {
        &self.executor.context().kernels_root
    }

    /// Record the kernels `manifest` says the node image pre-installed in
    /// the install root, holding the commit gate so no job publishes or
    /// removes meanwhile.
    pub(crate) async fn import_seed(&self, manifest: PathBuf) -> Result<SeedReport, SeedError> {
        let context = self.executor.context();
        let _gate = context.commit_gate.lock().await;
        let importing = self.executor.clone();
        tokio::task::spawn_blocking(move || {
            let context = importing.context();
            super::seed::import(
                &importing.db(),
                &context.kernels_root,
                context.host_arch,
                &manifest,
                jiff::Timestamp::now(),
            )
        })
        .await?
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
    let request: KernelOperation =
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
        JobStatus::Running => match (&request, in_progress) {
            (_, Some(store::JobPhase::Publishing)) => OperationState::Publishing,
            (_, Some(store::JobPhase::Removing)) | (KernelOperation::Uninstall { .. }, None) => {
                OperationState::Removing
            }
            (KernelOperation::Install { .. }, None) => OperationState::Downloading,
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
        kernel: result.map(|result| KernelOnDisk {
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
