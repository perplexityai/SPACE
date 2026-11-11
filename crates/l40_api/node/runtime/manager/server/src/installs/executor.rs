//! One durable install/delete pipeline. Artifact adapters only prepare bytes.
//!
//! Intent precedes every node-manager call. Retries use the same immutable
//! identity and node-manager's idempotent mutations. A lost response never
//! turns into a second ownership record or a different artifact request.

use super::describe;
use super::operations::{InstallLimits, InstallOperation};
use super::publisher::{ArtifactPublisher, NodeArtifactRef, PinSource};
use super::release::ReleaseSources;
use super::source::KernelSources;
use super::staging;
use super::store::{self, JobOutcome, JobPhase, JobResult};
use pplx_space_common_core::types::{Arch, Digest, JobId};
use pplx_space_common_jobs::{Job, JobError, JobExecutor};
use pplx_space_node_runtime_core::artifact_store::{StagedName, kernel_staging, release_staging};
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) struct InstallExecutor {
    context: Arc<ExecutorContext>,
}

pub(crate) struct ExecutorParts {
    pub(crate) kernels_root: PathBuf,
    pub(crate) boot_source: Option<PathBuf>,
    pub(crate) staging: PathBuf,
    pub(crate) limits: InstallLimits,
    pub(crate) host_arch: Arch,
    pub(crate) source: KernelSources,
    pub(crate) releases: ReleaseSources,
    pub(crate) publisher: Arc<dyn ArtifactPublisher>,
    pub(crate) pins: Arc<dyn PinSource>,
}

pub(super) struct ExecutorContext {
    pub(super) kernels_root: PathBuf,
    pub(super) boot_source: Option<PathBuf>,
    pub(super) staging: PathBuf,
    pub(super) limits: InstallLimits,
    pub(super) host_arch: Arch,
    pub(super) source: KernelSources,
    pub(super) releases: ReleaseSources,
    pub(super) publisher: Arc<dyn ArtifactPublisher>,
    pub(super) pins: Arc<dyn PinSource>,
    db: Arc<Mutex<Connection>>,
    // Cancellation and durable mutation intent are mutually exclusive.
    pub(super) commit_gate: tokio::sync::Mutex<()>,
}

pub(super) fn retriable(error: &(dyn std::error::Error + 'static)) -> JobError {
    JobError::retriable(describe(error))
}
pub(super) fn permanent(error: &(dyn std::error::Error + 'static)) -> JobError {
    JobError::permanent(describe(error))
}

impl InstallExecutor {
    pub(super) fn new(parts: ExecutorParts, db: Arc<Mutex<Connection>>) -> Self {
        let ExecutorParts {
            kernels_root,
            boot_source,
            staging,
            limits,
            host_arch,
            source,
            releases,
            publisher,
            pins,
        } = parts;
        Self {
            context: Arc::new(ExecutorContext {
                kernels_root,
                boot_source,
                staging,
                limits,
                host_arch,
                source,
                releases,
                publisher,
                pins,
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

    fn target_names(&self, artifact: &NodeArtifactRef) -> Result<(PathBuf, Vec<String>), JobError> {
        use pplx_space_node_runtime_core::artifact_store::{
            kernel_directory_names, release_directory_names,
        };
        match artifact {
            NodeArtifactRef::Boot(identity) => {
                let root = pplx_space_node_runtime_core::artifact_layout::boot_artifacts_root(
                    &self.context.kernels_root,
                );
                let name =
                    pplx_space_node_runtime_core::artifact_store::boot_artifact_directory(identity);
                Ok((root, vec![name]))
            }
            NodeArtifactRef::Kernel(version, digest) => Ok((
                self.context.kernels_root.clone(),
                kernel_directory_names(version.artifact_version(), *digest).to_vec(),
            )),
            NodeArtifactRef::Release(release, digest) => {
                let context = self
                    .context
                    .releases
                    .get(release.kind)
                    .ok_or_else(|| JobError::permanent("release management is disabled"))?;
                Ok((
                    context.root.clone(),
                    release_directory_names(release, *digest).to_vec(),
                ))
            }
        }
    }

    pub(super) fn target(&self, artifact: &NodeArtifactRef) -> Result<PathBuf, JobError> {
        let (root, names) = self.target_names(artifact)?;
        pplx_space_node_runtime_core::artifact_store::existing_artifact_path(&root, &names)
            .map_err(|error| retriable(&error))
    }

    fn staged_root(&self, request: &InstallOperation, job_id: JobId) -> Option<PathBuf> {
        let namespace = match request {
            InstallOperation::InstallBootArtifact { pin } => {
                pplx_space_node_runtime_core::artifact_store::boot_artifact_staging(
                    pin.identity().kind,
                )
            }
            InstallOperation::InstallKernel { kernel } => kernel_staging(kernel.line.runtime),
            InstallOperation::InstallRelease { release, .. } => {
                release_staging(release.release.kind)
            }
            _ => return None,
        };
        Some(
            self.context
                .staging
                .join(namespace)
                .join(job_id.to_string()),
        )
    }

    /// Freeze the expected digest before contacting node-manager. A changed
    /// catalog cannot silently change an already accepted install request.
    fn expected_digest(request: &InstallOperation) -> Digest {
        match request {
            InstallOperation::InstallBootArtifact { pin } => pin.identity().digest,
            InstallOperation::UninstallBootArtifact { identity } => identity.digest,
            InstallOperation::InstallKernel { kernel } => kernel.digest,
            InstallOperation::UninstallKernel { digest, .. }
            | InstallOperation::InstallRelease { digest, .. }
            | InstallOperation::UninstallRelease { digest, .. } => *digest,
        }
    }

    /// Presence is evidence from bytes, not an ownership record. This is only
    /// a download optimization: node-manager rechecks the store and its pins.
    pub(super) fn matches_on_disk(
        &self,
        artifact: &NodeArtifactRef,
        digest: Digest,
    ) -> Result<bool, JobError> {
        let target = self.target(artifact)?;
        match std::fs::symlink_metadata(&target) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(retriable(&error)),
            Ok(metadata) if !metadata.is_dir() => {
                return Err(JobError::permanent("artifact target is not a directory"));
            }
            Ok(_) => {}
        }
        let found = match artifact {
            NodeArtifactRef::Boot(identity) => {
                let catalog = self
                    .context
                    .pins
                    .catalog()
                    .map_err(|error| permanent(&error))?;
                let pin = catalog
                    .boot_artifact(identity, self.context.host_arch)
                    .filter(|pin| pin.size() <= self.context.limits.max_bytes)
                    .ok_or_else(|| {
                        JobError::permanent("boot artifact is not pinned within the install limit")
                    })?;
                staging::image_digests(
                    &target.join(pplx_space_node_runtime_core::artifact_store::BOOT_ARTIFACT_IMAGE),
                    pin.size(),
                )
                .map(|digests| digests.sha256)
                .map_err(|error| permanent(&error))?
            }
            NodeArtifactRef::Kernel(version, _) => {
                let catalog = self
                    .context
                    .pins
                    .catalog()
                    .map_err(|error| permanent(&error))?;
                let pin = catalog
                    .kernel(version.artifact_version(), self.context.host_arch)
                    .filter(|pin| pin.sha256 == digest && pin.size <= self.context.limits.max_bytes)
                    .ok_or_else(|| {
                        JobError::permanent("component is not pinned within the install limit")
                    })?;
                staging::image_digests(&target.join(staging::KERNEL_IMAGE), pin.size)
                    .map(|digests| digests.sha256)
                    .map_err(|error| permanent(&error))?
            }
            NodeArtifactRef::Release(release, _) => {
                super::release::release_digest(release.kind, &target, self.context.limits.max_bytes)
                    .map_err(|error| permanent(&error))?
            }
        };
        if found != digest {
            return Err(JobError::permanent(format!(
                "{} holds different bytes; left in place",
                target.display()
            )));
        }
        Ok(true)
    }

    async fn run(
        &self,
        job_id: JobId,
        request: &InstallOperation,
        staged: Option<&Path>,
        cancellation: &CancellationToken,
    ) -> Result<i64, JobError> {
        let artifact = request.artifact();
        let checking = self.clone();
        let checked = request.clone();
        let (target, digest, earlier) = tokio::task::spawn_blocking(move || {
            let digest = Self::expected_digest(&checked);
            let earlier = store::get_job_result(&checking.db(), &job_id)
                .map_err(|error| retriable(&error))?;
            // Keep the durable operation locator after removal or when another
            // layout appears. Only the two exact digest locators are compatible.
            let artifact = checked.artifact();
            let target = if let Some(result) = &earlier {
                let (root, names) = checking.target_names(&artifact)?;
                if !names.iter().any(|name| root.join(name) == result.path()) {
                    return Err(JobError::permanent(
                        "operation store path changed; restore its original configuration",
                    ));
                }
                result.path().to_path_buf()
            } else {
                checking.target(&artifact)?
            };
            Ok::<_, JobError>((target, digest, earlier))
        })
        .await
        .map_err(|error| retriable(&error))??;
        if let Some(result) = &earlier {
            if result.digest() != digest {
                return Err(JobError::permanent(
                    "operation identity changed; restore its original configuration",
                ));
            }
            if matches!(result, JobResult::Done { .. }) {
                return Ok(0);
            }
        }
        let mut received = 0;
        if !request.is_removal() {
            let checking = self.clone();
            let checked = artifact.clone();
            let present =
                tokio::task::spawn_blocking(move || checking.matches_on_disk(&checked, digest))
                    .await
                    .map_err(|error| retriable(&error))??;
            if !present {
                let root = staged
                    .ok_or_else(|| JobError::permanent("install has no staging directory"))?;
                let prepared_digest = match request {
                    InstallOperation::InstallBootArtifact { pin } => {
                        self.prepare_boot_artifact(pin, root, cancellation).await?;
                        received = pin.size();
                        pin.identity().digest
                    }
                    InstallOperation::InstallKernel { kernel } => {
                        let digest = self
                            .prepare_kernel(
                                kernel,
                                root,
                                &root.join(kernel.version.as_str()),
                                cancellation,
                            )
                            .await
                            .map_err(super::kernel::DownloadError::into_job_error)?;
                        received = kernel.size;
                        digest
                    }
                    InstallOperation::InstallRelease { release, .. } => {
                        let context = self
                            .context
                            .releases
                            .get(release.release.kind)
                            .ok_or_else(|| JobError::permanent("release source disabled"))?;
                        let (digest, size) = self
                            .prepare_release(context, release, digest, root, cancellation)
                            .await
                            .map_err(super::release::DownloadError::into_job_error)?;
                        received = size;
                        digest
                    }
                    _ => return Err(JobError::permanent("removal cannot prepare an artifact")),
                };
                if prepared_digest != digest {
                    return Err(JobError::permanent("prepared artifact identity changed"));
                }
            }
        }
        let phase = if request.is_removal() {
            JobPhase::Removing
        } else {
            JobPhase::Publishing
        };
        self.enter_phase(job_id, phase, &target, digest, cancellation)
            .await?;
        let outcome = if request.is_removal() {
            self.context
                .publisher
                .remove(&artifact)
                .await
                .map(|_| JobOutcome::Uninstalled)
        } else {
            let name = StagedName::parse(job_id.to_string()).map_err(|error| permanent(&error))?;
            self.context
                .publisher
                .publish(&artifact, &name)
                .await
                .and_then(|published| {
                    if published.digest != digest || published.path != target {
                        return Err(super::publisher::PublisherError::InvalidResponse {
                            field: "publication identity or path",
                        });
                    }
                    Ok(if published.already_present {
                        JobOutcome::AlreadyInstalled
                    } else {
                        JobOutcome::Installed
                    })
                })
        }
        .map_err(|error| {
            if error.is_permanent() {
                permanent(&error)
            } else {
                retriable(&error)
            }
        })?;
        self.record(
            job_id,
            JobResult::Done {
                outcome,
                path: target,
                digest,
            },
        )
        .await?;
        Ok(i64::try_from(received).unwrap_or(i64::MAX))
    }

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

    pub(super) async fn record(&self, job_id: JobId, result: JobResult) -> Result<(), JobError> {
        let recording = self.clone();
        tokio::task::spawn_blocking(move || {
            store::put_job_result(&recording.db(), &job_id, &result, jiff::Timestamp::now())
        })
        .await
        .map_err(|error| retriable(&error))?
        .map_err(|error| retriable(&error))
    }
}

#[async_trait::async_trait]
impl JobExecutor for InstallExecutor {
    async fn execute(&self, job: &Job, cancellation: CancellationToken) -> Result<i64, JobError> {
        let request: InstallOperation = job.payload_as()?;
        let staged = self.staged_root(&request, job.job_id);
        if let Some(staged) = staged.clone() {
            tokio::task::spawn_blocking(move || {
                staging::remove_dir_if_present(&staged)?;
                staging::create_namespace(&staged)
            })
            .await
            .map_err(|error| retriable(&error))?
            .map_err(|error| retriable(&error))?;
        }
        let result = self
            .run(job.job_id, &request, staged.as_deref(), &cancellation)
            .await;
        if let Err(error) = self.cleanup_cancelled(job).await {
            // Cleanup must not turn a committed mutation into a retry or
            // replace the original failure classification.
            tracing::warn!(job_id = %job.job_id, %error, "staging cleanup failed; preserving operation result");
        }
        result
    }

    async fn cleanup_cancelled(&self, job: &Job) -> Result<(), JobError> {
        let request: InstallOperation = job.payload_as()?;
        if let Some(staged) = self.staged_root(&request, job.job_id) {
            tokio::task::spawn_blocking(move || staging::remove_dir_if_present(&staged))
                .await
                .map_err(|error| retriable(&error))?
                .map_err(|error| retriable(&error))?;
        }
        Ok(())
    }
}
