//! The kernel job: download, verify, and publish an install; rename away and
//! forget an uninstall.
//!
//! An install downloads into private staging under the state directory,
//! checks the bytes against the source's size and MD5 and that they are a
//! boot image in this node's format (an ELF `vmlinux` on x86_64, the Linux
//! `Image` on aarch64), and publishes the kernel's directory into the
//! install root with one rename that never replaces anything. An uninstall
//! renames a kernel the runtime manager installed out of the root, once its
//! image is shown to still hold the installed bytes; anything else at the path
//! is left alone. A reinstall likewise checks the installed image, not just its
//! directory, before it reports the kernel present.
//!
//! The shared worker re-runs a job a restart interrupted, so the job is
//! idempotent. Before it renames anything it records the step it is taking
//! (publishing or removing) with the target and digest, and a re-run
//! resolves that step from the disk: a publication whose rename landed is
//! recorded if the published bytes still match, one whose rename did not is
//! downloaded again, a removal whose rename landed is finished, and a target
//! holding anything else is left for maintenance and the job fails. The
//! install record that makes a kernel space-managed is written in the same
//! transaction as the job's outcome.
//!
//! Records and job results keep absolute paths. One that names a directory
//! outside the configured install root, as after a restart with another
//! root, is refused rather than reported present, renamed, or forgotten.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt as _;
use pplx_space_common_core::types::{Arch, ArtifactVersion, Digest, JobId};
use pplx_space_common_jobs::{Job, JobError, JobExecutor};
use rusqlite::Connection;
use tokio_util::sync::CancellationToken;

use super::describe;
use super::operations::{InstallLimits, KernelConflict, KernelOperation, UninstallKernelError};
use super::source::{ContentMd5, KernelSource, KernelVersion, SourceError, VerifiableKernel};
use super::staging::{
    self, FinishedImage, ImageDigests, KERNEL_IMAGE, StagedImage, StagingError, WRITE_BATCH_BYTES,
};
use super::store::{self, JobOutcome, JobPhase, JobResult, KernelInstall, StoreError};

/// The aliases a versioned kernel root selects through. A kernel either
/// names is in use as a default and is never uninstalled.
const SELECTION_ALIASES: &[&str] = &["default", "legacy-default"];

#[derive(Clone)]
pub(crate) struct KernelExecutor {
    context: Arc<ExecutorContext>,
}

pub(super) struct ExecutorContext {
    pub(super) kernels_root: PathBuf,
    pub(super) staging: PathBuf,
    pub(super) limits: InstallLimits,
    pub(super) host_arch: Arch,
    pub(super) source: Arc<dyn KernelSource>,
    db: Arc<Mutex<Connection>>,
    /// Held while a cancel decides and while a job enters publishing or
    /// removal, so a cancel either stops the job first or is refused.
    pub(super) commit_gate: tokio::sync::Mutex<()>,
}

/// What already occupies a kernel version.
enum Existing {
    Absent,
    Satisfied {
        outcome: JobOutcome,
        path: PathBuf,
        digest: Digest,
    },
    Taken(KernelConflict),
}

/// How a download ended short of verified bytes.
#[derive(Debug, thiserror::Error)]
enum DownloadError {
    #[error("cancelled")]
    Cancelled,
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error("the download stalled for {}s", idle.as_secs())]
    Stalled { idle: Duration },
    #[error("the source sent {received} of the {listed} bytes it listed")]
    Short { received: u64, listed: u64 },
    #[error("the source sent more than the {listed} bytes it listed")]
    Overlong { listed: u64 },
    #[error("the download's MD5 {found} is not the {published} the source published")]
    Md5Mismatch {
        found: ContentMd5,
        published: ContentMd5,
    },
    #[error("the download is not an {architecture} kernel boot image")]
    NotKernel { architecture: Arch },
    #[error("the downloaded image announces release {found}, not the {listed} the source listed")]
    WrongRelease {
        found: ArtifactVersion,
        listed: KernelVersion,
    },
    #[error(transparent)]
    Staging(#[from] StagingError),
    #[error("download task failed")]
    Task(#[from] tokio::task::JoinError),
}

impl DownloadError {
    /// Transfer and local failures may clear up on another attempt; bytes
    /// that do not match what the source published will not.
    fn into_job_error(self) -> JobError {
        match self {
            Self::Cancelled => JobError::YieldedForRestart,
            Self::Overlong { .. }
            | Self::Md5Mismatch { .. }
            | Self::NotKernel { .. }
            | Self::WrongRelease { .. } => JobError::permanent(describe(&self)),
            Self::Source(_)
            | Self::Stalled { .. }
            | Self::Short { .. }
            | Self::Staging(_)
            | Self::Task(_) => JobError::retriable(describe(&self)),
        }
    }
}

fn retriable(error: &(dyn std::error::Error + 'static)) -> JobError {
    JobError::retriable(describe(error))
}

fn permanent(error: &(dyn std::error::Error + 'static)) -> JobError {
    JobError::permanent(describe(error))
}

#[async_trait::async_trait]
impl JobExecutor for KernelExecutor {
    async fn execute(&self, job: &Job, cancellation: CancellationToken) -> Result<i64, JobError> {
        let operation: KernelOperation = job.payload_as()?;
        let staged_root = self.context.staging.join(job.job_id.to_string());
        // Whatever an earlier attempt staged is not trusted.
        staging::remove_dir_if_present(&staged_root).map_err(|error| retriable(&error))?;
        let outcome = match operation {
            KernelOperation::Install { kernel } => {
                self.run_install(job.job_id, &kernel, &staged_root, &cancellation)
                    .await
            }
            KernelOperation::Uninstall { version } => {
                self.run_uninstall(job.job_id, &version, &staged_root, &cancellation)
                    .await
            }
        };
        if let Err(error) = staging::remove_dir_if_present(&staged_root) {
            tracing::warn!(job_id = %job.job_id, error = %describe(&error), "failed to clear staging; the next start clears it");
        }
        outcome
    }

    async fn cleanup_cancelled(&self, job: &Job) -> Result<(), JobError> {
        let staged_root = self.context.staging.join(job.job_id.to_string());
        staging::remove_dir_if_present(&staged_root).map_err(|error| retriable(&error))
    }
}

impl KernelExecutor {
    pub(super) fn new(
        kernels_root: PathBuf,
        staging: PathBuf,
        limits: InstallLimits,
        host_arch: Arch,
        source: Arc<dyn KernelSource>,
        db: Arc<Mutex<Connection>>,
    ) -> Self {
        Self {
            context: Arc::new(ExecutorContext {
                kernels_root,
                staging,
                limits,
                host_arch,
                source,
                db,
                commit_gate: tokio::sync::Mutex::new(()),
            }),
        }
    }

    pub(super) fn context(&self) -> &ExecutorContext {
        &self.context
    }

    pub(super) fn db(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.context
            .db
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn target(&self, version: &KernelVersion) -> PathBuf {
        self.context.kernels_root.join(version.as_str())
    }

    /// The conflict a recorded `path` for `version` is when it is not this
    /// root's directory for it. Records and job results keep absolute paths,
    /// so after a restart with another install root they name the old one;
    /// nothing outside the configured root is reported, renamed, or forgotten.
    fn outside_root(&self, version: &KernelVersion, path: &Path) -> Option<KernelConflict> {
        (path != self.target(version)).then(|| KernelConflict::OutsideRoot {
            version: version.clone(),
            path: path.to_path_buf(),
            root: self.context.kernels_root.clone(),
        })
    }

    /// The conflict the install records alone show for `kernel`, if any.
    /// Cheap enough for a request handler: it reads one row and stats one
    /// path. Blocking.
    pub(super) fn recorded_conflict(
        &self,
        kernel: &VerifiableKernel,
    ) -> Result<Option<KernelConflict>, StoreError> {
        let recorded = store::get_kernel_install(&self.db(), &kernel.version)?;
        Ok(recorded.and_then(|recorded| {
            self.outside_root(&kernel.version, &recorded.path)
                .or_else(|| conflict_with_record(kernel, &recorded))
        }))
    }

    async fn run_install(
        &self,
        job_id: JobId,
        kernel: &VerifiableKernel,
        staged_root: &Path,
        cancellation: &CancellationToken,
    ) -> Result<i64, JobError> {
        let resolving = self.clone();
        let resolved = kernel.clone();
        let existing =
            tokio::task::spawn_blocking(move || resolving.resolve_install(job_id, &resolved))
                .await
                .map_err(|error| retriable(&error))??;
        match existing {
            Existing::Absent => {}
            Existing::Satisfied {
                outcome,
                path,
                digest,
            } => {
                self.record(
                    job_id,
                    JobResult::Done {
                        outcome,
                        path,
                        digest,
                    },
                )
                .await?;
                return Ok(0);
            }
            Existing::Taken(conflict) => return Err(permanent(&conflict)),
        }

        let staged = staged_root.join(kernel.version.as_str());
        let digest = self
            .download(kernel, staged_root, &staged, cancellation)
            .await
            .map_err(DownloadError::into_job_error)?;

        let target = self.target(&kernel.version);
        self.enter_phase(job_id, JobPhase::Publishing, &target, digest, cancellation)
            .await?;
        let publishing = self.clone();
        let published = kernel.clone();
        tokio::task::spawn_blocking(move || {
            publishing.publish(job_id, &published, &staged, &target, digest)
        })
        .await
        .map_err(|error| retriable(&error))??;
        Ok(i64::try_from(kernel.size).unwrap_or(i64::MAX))
    }

    async fn run_uninstall(
        &self,
        job_id: JobId,
        version: &KernelVersion,
        staged_root: &Path,
        cancellation: &CancellationToken,
    ) -> Result<i64, JobError> {
        let resolving = self.clone();
        let resolved = version.clone();
        let install =
            tokio::task::spawn_blocking(move || resolving.resolve_uninstall(job_id, &resolved))
                .await
                .map_err(|error| retriable(&error))??;
        let Some(install) = install else {
            // A re-run found the removal already done.
            return Ok(0);
        };
        self.enter_phase(
            job_id,
            JobPhase::Removing,
            &install.path,
            install.digest,
            cancellation,
        )
        .await?;
        let removing = self.clone();
        let staged_root = staged_root.to_path_buf();
        tokio::task::spawn_blocking(move || removing.remove(job_id, &install, &staged_root))
            .await
            .map_err(|error| retriable(&error))??;
        Ok(0)
    }

    /// Record that the job is about to rename, unless it was cancelled
    /// first. After this a cancel is refused.
    async fn enter_phase(
        &self,
        job_id: JobId,
        phase: JobPhase,
        path: &Path,
        digest: Digest,
        cancellation: &CancellationToken,
    ) -> Result<(), JobError> {
        let _gate = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(JobError::YieldedForRestart),
            gate = self.context.commit_gate.lock() => gate,
        };
        if cancellation.is_cancelled() {
            return Err(JobError::YieldedForRestart);
        }
        self.record(
            job_id,
            JobResult::InProgress {
                phase,
                path: path.to_path_buf(),
                digest,
            },
        )
        .await
    }

    async fn record(&self, job_id: JobId, result: JobResult) -> Result<(), JobError> {
        let recording = self.clone();
        tokio::task::spawn_blocking(move || {
            store::put_job_result(&recording.db(), &job_id, &result, jiff::Timestamp::now())
        })
        .await
        .map_err(|error| retriable(&error))?
        .map_err(|error| retriable(&error))
    }

    /// What this job still has to do, given what an earlier attempt left.
    /// Blocking.
    fn resolve_install(
        &self,
        job_id: JobId,
        kernel: &VerifiableKernel,
    ) -> Result<Existing, JobError> {
        let earlier =
            store::get_job_result(&self.db(), &job_id).map_err(|error| retriable(&error))?;
        // An interrupted step under another root is left for maintenance.
        if let Some(JobResult::InProgress { path, .. }) = &earlier
            && let Some(conflict) = self.outside_root(&kernel.version, path)
        {
            return Err(permanent(&conflict));
        }
        match earlier {
            Some(JobResult::Done {
                outcome,
                path,
                digest,
            }) => {
                return Ok(Existing::Satisfied {
                    outcome,
                    path,
                    digest,
                });
            }
            Some(JobResult::InProgress {
                phase: JobPhase::Publishing,
                path: target,
                digest,
            }) => match staging::image_digests(&target.join(KERNEL_IMAGE), kernel.size) {
                // The rename landed; finish recording it.
                Ok(found) if found.sha256 == digest => {
                    self.record_install(job_id, kernel, &target, digest)
                        .map_err(|error| retriable(&error))?;
                    return Ok(Existing::Satisfied {
                        outcome: JobOutcome::Installed,
                        path: target,
                        digest,
                    });
                }
                // The rename never landed: start over.
                Err(_) if std::fs::symlink_metadata(&target).is_err() => {
                    store::clear_job_result(&self.db(), &job_id)
                        .map_err(|error| retriable(&error))?;
                }
                _ => {
                    return Err(permanent(&KernelConflict::PublishedOtherBytes {
                        version: kernel.version.clone(),
                        path: target,
                    }));
                }
            },
            Some(JobResult::InProgress {
                phase: JobPhase::Removing,
                ..
            }) => {
                return Err(JobError::permanent(
                    "an install job recorded a removal; its record is inconsistent",
                ));
            }
            None => {}
        }
        self.existing(job_id, kernel)
            .map_err(|error| retriable(&error))
    }

    /// What already occupies the kernel's version. Blocking: an external
    /// kernel's image is hashed.
    fn existing(&self, job_id: JobId, kernel: &VerifiableKernel) -> Result<Existing, StoreError> {
        let version = &kernel.version;
        let recorded = store::get_kernel_install(&self.db(), version)?;
        if let Some(recorded) = recorded {
            if let Some(conflict) = self
                .outside_root(version, &recorded.path)
                .or_else(|| conflict_with_record(kernel, &recorded))
            {
                return Ok(Existing::Taken(conflict));
            }
            // Verified local presence means the recorded bytes, not just a
            // directory by that name.
            if let Some(conflict) = changed_since_recorded(&recorded) {
                return Ok(Existing::Taken(conflict));
            }
            let outcome = if recorded.job_id == Some(job_id) {
                JobOutcome::Installed
            } else {
                JobOutcome::AlreadyInstalled
            };
            return Ok(Existing::Satisfied {
                outcome,
                path: recorded.path,
                digest: recorded.digest,
            });
        }
        let target = self.target(version);
        if std::fs::symlink_metadata(&target).is_err() {
            return Ok(Existing::Absent);
        }
        Ok(
            match staging::image_digests(&target.join(KERNEL_IMAGE), kernel.size) {
                Ok(found) if found.md5 == kernel.md5 => {
                    // An earlier job that renamed these bytes in and then
                    // failed to record them left them without their record:
                    // finish its publication as this install.
                    let stale =
                        store::find_unfinished_publication(&self.db(), &target, found.sha256)?;
                    if let Some(stale) = stale {
                        self.record_install(job_id, kernel, &target, found.sha256)?;
                        store::clear_job_result(&self.db(), &stale)?;
                        tracing::info!(%job_id, %stale, version = %version, "adopted a kernel an earlier job published");
                        Existing::Satisfied {
                            outcome: JobOutcome::Installed,
                            path: target,
                            digest: found.sha256,
                        }
                    } else {
                        Existing::Satisfied {
                            outcome: JobOutcome::ProvidedExternally,
                            path: target,
                            digest: found.sha256,
                        }
                    }
                }
                Ok(_) => Existing::Taken(KernelConflict::ExternalDiffers {
                    version: version.clone(),
                    path: target,
                }),
                Err(source) => Existing::Taken(KernelConflict::Unreadable {
                    version: version.clone(),
                    path: target,
                    source,
                }),
            },
        )
    }

    /// Stream the kernel into `staged`, writing in batches on the blocking
    /// pool, then hold it to the source's size, MD5, and version and to this
    /// node's architecture. Returns its SHA-256.
    async fn download(
        &self,
        kernel: &VerifiableKernel,
        staged_root: &Path,
        staged: &Path,
        cancellation: &CancellationToken,
    ) -> Result<Digest, DownloadError> {
        let (created_root, created) = (staged_root.to_path_buf(), staged.to_path_buf());
        let mut writer =
            tokio::task::spawn_blocking(move || StagedImage::create(&created_root, &created))
                .await??;

        let mut chunks = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(DownloadError::Cancelled),
            opened = self.context.source.open(kernel) => opened?,
        };
        let idle = self.context.limits.download_idle_timeout;
        let image = staged.join(KERNEL_IMAGE);
        let mut batch: Vec<u8> = Vec::with_capacity(WRITE_BATCH_BYTES);
        loop {
            let next = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(DownloadError::Cancelled),
                next = tokio::time::timeout(idle, chunks.next()) => {
                    next.map_err(|_| DownloadError::Stalled { idle })?
                }
            };
            let Some(chunk) = next else { break };
            let chunk = chunk?;
            let received = writer.written() + batch.len() as u64 + chunk.len() as u64;
            if received > kernel.size {
                return Err(DownloadError::Overlong {
                    listed: kernel.size,
                });
            }
            batch.extend_from_slice(&chunk);
            if batch.len() >= WRITE_BATCH_BYTES {
                writer = write_batch(writer, std::mem::take(&mut batch), image.clone()).await?;
                batch.reserve(WRITE_BATCH_BYTES);
            }
        }
        if !batch.is_empty() {
            writer = write_batch(writer, batch, image.clone()).await?;
        }
        if writer.written() != kernel.size {
            return Err(DownloadError::Short {
                received: writer.written(),
                listed: kernel.size,
            });
        }

        let finished = staged.to_path_buf();
        let FinishedImage {
            digests: ImageDigests { md5, sha256 },
            release,
        } = tokio::task::spawn_blocking(move || writer.finish(&finished)).await??;
        if md5 != kernel.md5 {
            return Err(DownloadError::Md5Mismatch {
                found: md5,
                published: kernel.md5,
            });
        }
        // The version is the install's name and what listings report, so an
        // image that names another release is refused. One with no banner
        // has nothing to contradict.
        if let Some(found) = release
            && found.as_str() != kernel.version.release()
        {
            return Err(DownloadError::WrongRelease {
                found,
                listed: kernel.version.clone(),
            });
        }
        let architecture = self.context.host_arch;
        let probed = image.clone();
        let boots =
            tokio::task::spawn_blocking(move || staging::is_boot_image(&probed, architecture))
                .await??;
        if boots {
            Ok(sha256)
        } else {
            Err(DownloadError::NotKernel { architecture })
        }
    }

    /// Rename the verified staging directory into the install root and
    /// record the install with the job's outcome. Blocking.
    fn publish(
        &self,
        job_id: JobId,
        kernel: &VerifiableKernel,
        staged: &Path,
        target: &Path,
        digest: Digest,
    ) -> Result<(), JobError> {
        if let Err(error) = staging::rename_noreplace(staged, target) {
            // Nothing was renamed, so a later attempt or request may try again.
            store::clear_job_result(&self.db(), &job_id).map_err(|error| retriable(&error))?;
            return Err(if error.is_already_exists() {
                permanent(&KernelConflict::Appeared {
                    version: kernel.version.clone(),
                    path: target.to_path_buf(),
                })
            } else {
                retriable(&error)
            });
        }
        staging::sync_dir(&self.context.kernels_root).map_err(|error| retriable(&error))?;
        self.record_install(job_id, kernel, target, digest)
            .map_err(|error| retriable(&error))?;
        tracing::info!(%job_id, version = %kernel.version, %digest, path = %target.display(), "installed kernel");
        Ok(())
    }

    /// Finish the renames that jobs which already ended recorded and did not
    /// resolve, as a re-run of each would: a landed publication is recorded,
    /// a landed removal forgotten, and a step whose rename never landed is
    /// dropped. A job ends this way when the writes after its rename keep
    /// failing until its attempts run out; what it did to the disk stands
    /// regardless. Active jobs resolve their own steps when the worker runs
    /// them. Returns how many steps were finished. Blocking.
    pub(super) fn finish_ended_jobs(&self) -> Result<usize, StoreError> {
        let mut finished = 0;
        // Bound first: a guard in the loop header would hold the store's
        // lock through the body, which takes it again.
        let steps = store::list_unfinished_steps(&self.db())?;
        for step in steps {
            let job = pplx_space_common_jobs::db::get_job(&self.db(), &step.job_id)?;
            let request = match &job {
                Some(job) if !job.is_terminal() => continue,
                Some(job) => job.payload_as::<KernelOperation>().ok(),
                None => None,
            };
            if self.finish_step(&step, request.as_ref())? {
                finished += 1;
            }
        }
        Ok(finished)
    }

    /// Resolve one ended job's step from the disk. Returns whether it was
    /// resolved; one that cannot be is left for a later install of the same
    /// bytes to adopt, or for maintenance.
    fn finish_step(
        &self,
        step: &store::UnfinishedStep,
        request: Option<&KernelOperation>,
    ) -> Result<bool, StoreError> {
        let landed = std::fs::symlink_metadata(&step.path).is_ok();
        match (step.phase, request) {
            (JobPhase::Publishing, Some(KernelOperation::Install { kernel })) => {
                if self.outside_root(&kernel.version, &step.path).is_some() {
                    return Ok(false);
                }
                if !landed {
                    store::clear_job_result(&self.db(), &step.job_id)?;
                    return Ok(true);
                }
                match staging::image_digests(&step.path.join(KERNEL_IMAGE), kernel.size) {
                    Ok(found) if found.sha256 == step.digest => {
                        self.record_install(step.job_id, kernel, &step.path, step.digest)?;
                        tracing::info!(job_id = %step.job_id, version = %kernel.version, "recorded a kernel an ended job published");
                        Ok(true)
                    }
                    _ => {
                        tracing::warn!(job_id = %step.job_id, path = %step.path.display(), "an ended job's publication target holds other bytes; left in place");
                        Ok(false)
                    }
                }
            }
            (JobPhase::Removing, request) => {
                let version = match request {
                    Some(KernelOperation::Uninstall { version }) => Some(version.clone()),
                    _ => step
                        .path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .and_then(|name| name.parse::<KernelVersion>().ok()),
                };
                let Some(version) = version else {
                    return Ok(false);
                };
                if self.outside_root(&version, &step.path).is_some() {
                    return Ok(false);
                }
                if landed {
                    // The removal never renamed anything; the kernel stays.
                    store::clear_job_result(&self.db(), &step.job_id)?;
                } else {
                    self.forget(step.job_id, &version, &step.path, step.digest)?;
                    tracing::info!(job_id = %step.job_id, version = %version, "forgot a kernel an ended job removed");
                }
                Ok(true)
            }
            // A pruned install job's request is gone; a later install of the
            // same bytes adopts what it published.
            (JobPhase::Publishing, _) => Ok(false),
        }
    }

    /// The install record and the job's outcome, in one transaction. A
    /// re-run finds the record an earlier attempt wrote and keeps it.
    fn record_install(
        &self,
        job_id: JobId,
        kernel: &VerifiableKernel,
        target: &Path,
        digest: Digest,
    ) -> Result<(), StoreError> {
        let now = jiff::Timestamp::now();
        let mut db = self.db();
        let transaction = db.transaction()?;
        store::insert_kernel_install(
            &transaction,
            &KernelInstall {
                version: kernel.version.clone(),
                path: target.to_path_buf(),
                line: kernel.line,
                location: kernel.location.clone(),
                size: kernel.size,
                md5: kernel.md5,
                digest,
                job_id: Some(job_id),
                installed_at: now,
            },
        )?;
        store::put_job_result(
            &transaction,
            &job_id,
            &JobResult::Done {
                outcome: JobOutcome::Installed,
                path: target.to_path_buf(),
                digest,
            },
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// The install `version` names, if it may be removed now. Blocking.
    pub(super) fn removable(
        &self,
        version: &KernelVersion,
    ) -> Result<KernelInstall, UninstallKernelError> {
        let target = self.target(version);
        let Some(recorded) = store::get_kernel_install(&self.db(), version)? else {
            return Err(if std::fs::symlink_metadata(&target).is_ok() {
                UninstallKernelError::External {
                    version: version.clone(),
                    path: target,
                }
            } else {
                UninstallKernelError::NotInstalled {
                    version: version.clone(),
                }
            });
        };
        if recorded.path != target {
            return Err(UninstallKernelError::OutsideRoot {
                version: version.clone(),
                path: recorded.path,
                root: self.context.kernels_root.clone(),
            });
        }
        if let Some(alias) = self.selecting_alias(&recorded.path) {
            return Err(UninstallKernelError::Selected {
                version: version.clone(),
                alias,
            });
        }
        Ok(recorded)
    }

    /// The install this job still has to remove, or `None` if an earlier
    /// attempt finished. Blocking.
    fn resolve_uninstall(
        &self,
        job_id: JobId,
        version: &KernelVersion,
    ) -> Result<Option<KernelInstall>, JobError> {
        let earlier =
            store::get_job_result(&self.db(), &job_id).map_err(|error| retriable(&error))?;
        // An interrupted step under another root is left for maintenance.
        if let Some(JobResult::InProgress { path, .. }) = &earlier
            && let Some(conflict) = self.outside_root(version, path)
        {
            return Err(permanent(&conflict));
        }
        match earlier {
            Some(JobResult::Done { .. }) => return Ok(None),
            Some(JobResult::InProgress {
                phase: JobPhase::Removing,
                path,
                digest,
            }) if std::fs::symlink_metadata(&path).is_err() => {
                // The rename landed; finish forgetting the kernel.
                self.forget(job_id, version, &path, digest)
                    .map_err(|error| retriable(&error))?;
                return Ok(None);
            }
            _ => {}
        }
        self.removable(version)
            .map(Some)
            .map_err(|error| match error {
                UninstallKernelError::Store(_) | UninstallKernelError::Task(_) => retriable(&error),
                _ => permanent(&error),
            })
    }

    /// Rename the kernel out of the root, then forget it. Blocking.
    fn remove(
        &self,
        job_id: JobId,
        install: &KernelInstall,
        staged_root: &Path,
    ) -> Result<(), JobError> {
        // Remove only the bytes the runtime manager installed: whatever else
        // now sits at the path is left alone.
        if let Some(conflict) = changed_since_recorded(install) {
            return Err(permanent(&conflict));
        }
        staging::create_private_dir(staged_root).map_err(|error| retriable(&error))?;
        staging::rename_noreplace(&install.path, &staged_root.join(install.version.as_str()))
            .and_then(|()| staging::sync_dir(&self.context.kernels_root))
            .map_err(|error| retriable(&error))?;
        self.forget(job_id, &install.version, &install.path, install.digest)
            .map_err(|error| retriable(&error))?;
        tracing::info!(%job_id, version = %install.version, "uninstalled kernel");
        Ok(())
    }

    /// Drop the install record and record the uninstall, in one transaction.
    fn forget(
        &self,
        job_id: JobId,
        version: &KernelVersion,
        path: &Path,
        digest: Digest,
    ) -> Result<(), StoreError> {
        let mut db = self.db();
        let transaction = db.transaction()?;
        store::delete_kernel_install(&transaction, version)?;
        store::put_job_result(
            &transaction,
            &job_id,
            &JobResult::Done {
                outcome: JobOutcome::Uninstalled,
                path: path.to_path_buf(),
                digest,
            },
            jiff::Timestamp::now(),
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// The alias in the install root that resolves to `path`, if any.
    fn selecting_alias(&self, path: &Path) -> Option<PathBuf> {
        let installed = std::fs::canonicalize(path).ok()?;
        SELECTION_ALIASES
            .iter()
            .map(|alias| self.context.kernels_root.join(alias))
            .find(|alias| std::fs::canonicalize(alias).is_ok_and(|resolved| resolved == installed))
    }
}

/// How a recorded install conflicts with installing `kernel`, if it does.
fn conflict_with_record(
    kernel: &VerifiableKernel,
    recorded: &KernelInstall,
) -> Option<KernelConflict> {
    if recorded.location != kernel.location || recorded.md5 != kernel.md5 {
        return Some(KernelConflict::InstalledFromElsewhere {
            version: kernel.version.clone(),
            location: Box::new(recorded.location.clone()),
        });
    }
    let image_present = std::fs::symlink_metadata(recorded.path.join(KERNEL_IMAGE))
        .is_ok_and(|metadata| metadata.is_file() && metadata.len() == recorded.size);
    if !image_present {
        return Some(KernelConflict::RecordedButMissing {
            version: kernel.version.clone(),
            path: recorded.path.clone(),
        });
    }
    None
}

/// How the recorded install's image differs from what was installed, if it
/// does. Blocking: the image is hashed.
fn changed_since_recorded(recorded: &KernelInstall) -> Option<KernelConflict> {
    let image = recorded.path.join(KERNEL_IMAGE);
    match staging::image_digests(&image, recorded.size) {
        Ok(found) if found.sha256 == recorded.digest => None,
        Ok(_) => Some(KernelConflict::Changed {
            version: recorded.version.clone(),
            path: recorded.path.clone(),
        }),
        Err(source) => Some(KernelConflict::Unreadable {
            version: recorded.version.clone(),
            path: recorded.path.clone(),
            source,
        }),
    }
}

async fn write_batch(
    mut writer: StagedImage,
    batch: Vec<u8>,
    image: PathBuf,
) -> Result<StagedImage, DownloadError> {
    Ok(tokio::task::spawn_blocking(move || {
        writer.append(&batch, &image)?;
        Ok::<_, StagingError>(writer)
    })
    .await??)
}
