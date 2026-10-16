//! The sparse-shadow rollout rung: compare a sparse materialization with a dense one, and schedule
//! those comparisons.

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShadowComparisonOutcome {
    Match,
    Mismatch,
    Error,
    SkippedBusy,
    SkippedDisk,
}

impl ShadowComparisonOutcome {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Match => "match",
            Self::Mismatch => "mismatch",
            Self::Error => "error",
            Self::SkippedBusy => "skipped_busy",
            Self::SkippedDisk => "skipped_disk",
        }
    }
}

impl std::fmt::Display for ShadowComparisonOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ShadowComparisonObservation {
    pub(super) logical_bytes: u64,
    pub(super) allocated_bytes: u64,
    pub(super) candidate_hole_bytes: u64,
}

pub(super) fn sparse_shadow_path(path: &Path) -> PathBuf {
    let mut shadow = path.to_path_buf();
    let mut name = shadow.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".sparse-shadow-{}.tmp", uuid::Uuid::now_v7()));
    shadow.set_file_name(name);
    shadow
}

pub(super) fn sparse_shadow_source_path(staging_dir: &Path) -> PathBuf {
    staging_dir.join(format!("sparse-shadow-source-{}.tmp", uuid::Uuid::now_v7()))
}

#[derive(Debug, thiserror::Error)]
pub(super) enum SparseShadowCmpError {
    #[error("sparse shadow comparison cancelled")]
    Cancelled,
    #[error("sparse shadow bytes differ from dense output")]
    Mismatch,
    #[error("{operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
}

impl SparseShadowCmpError {
    pub(super) fn io(operation: &'static str, source: std::io::Error) -> Self {
        Self::Io { operation, source }
    }
}

pub(super) fn compare_completed_files(
    dense: &Path,
    sparse: &Path,
    shutdown_token: &CancellationToken,
) -> Result<(), SparseShadowCmpError> {
    let mut dense_file = std::fs::File::open(dense)
        .map_err(|error| SparseShadowCmpError::io("open dense comparison input", error))?;
    let mut sparse_file = std::fs::File::open(sparse)
        .map_err(|error| SparseShadowCmpError::io("open sparse comparison input", error))?;
    let dense_len = dense_file
        .metadata()
        .map_err(|error| SparseShadowCmpError::io("stat dense comparison input", error))?
        .len();
    let sparse_len = sparse_file
        .metadata()
        .map_err(|error| SparseShadowCmpError::io("stat sparse comparison input", error))?
        .len();
    if dense_len != sparse_len {
        return Err(SparseShadowCmpError::Mismatch);
    }

    let mut dense_buffer = vec![0; 1024 * 1024];
    let mut sparse_buffer = vec![0; 1024 * 1024];
    let mut compared_through = 0_u64;
    let mut advised_through = 0_u64;
    while compared_through < dense_len {
        if shutdown_token.is_cancelled() {
            return Err(SparseShadowCmpError::Cancelled);
        }
        let remaining = dense_len.saturating_sub(compared_through);
        let read_len = dense_buffer
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let dense_chunk = dense_buffer.get_mut(..read_len).ok_or_else(|| {
            SparseShadowCmpError::io(
                "prepare dense comparison buffer",
                std::io::Error::other("comparison read exceeds dense buffer"),
            )
        })?;
        let sparse_chunk = sparse_buffer.get_mut(..read_len).ok_or_else(|| {
            SparseShadowCmpError::io(
                "prepare sparse comparison buffer",
                std::io::Error::other("comparison read exceeds sparse buffer"),
            )
        })?;
        dense_file
            .read_exact(dense_chunk)
            .map_err(|error| SparseShadowCmpError::io("read dense comparison input", error))?;
        sparse_file
            .read_exact(sparse_chunk)
            .map_err(|error| SparseShadowCmpError::io("read sparse comparison input", error))?;
        if dense_chunk != sparse_chunk {
            return Err(SparseShadowCmpError::Mismatch);
        }
        compared_through = compared_through.saturating_add(read_len as u64);
        if compared_through.saturating_sub(advised_through) >= STREAM_WRITEBACK_WINDOW_BYTES {
            advise_shadow_read_range(
                &dense_file,
                dense,
                advised_through,
                compared_through.saturating_sub(advised_through),
                "dense sparse-shadow comparison input",
            );
            advise_shadow_read_range(
                &sparse_file,
                sparse,
                advised_through,
                compared_through.saturating_sub(advised_through),
                "sparse shadow comparison input",
            );
            advised_through = compared_through;
        }
    }
    if advised_through < compared_through {
        let length = compared_through.saturating_sub(advised_through);
        advise_shadow_read_range(
            &dense_file,
            dense,
            advised_through,
            length,
            "dense sparse-shadow comparison input",
        );
        advise_shadow_read_range(
            &sparse_file,
            sparse,
            advised_through,
            length,
            "sparse shadow comparison input",
        );
    }
    Ok(())
}

pub(super) fn advise_shadow_read_range(
    file: &std::fs::File,
    path: &Path,
    offset: u64,
    length: u64,
    operation: &'static str,
) {
    if let Err(error) = page_cache::advise_range_dontneed(file, offset, length) {
        tracing::warn!(
            %error,
            path_len = path.as_os_str().len(),
            offset,
            length,
            operation,
            "failed to evict consumed sparse-shadow read range"
        );
    }
}

pub(super) fn compare_sparse_shadow(
    path: &Path,
    block_bytes: usize,
    shutdown_token: &CancellationToken,
    writeback_coordinator: Arc<StreamWritebackCoordinator>,
) -> Result<ShadowComparisonObservation, SparseShadowCmpError> {
    let shadow = sparse_shadow_path(path);
    let result = (|| {
        if shutdown_token.is_cancelled() {
            return Err(SparseShadowCmpError::Cancelled);
        }
        let mut reader = std::fs::File::open(path)
            .map_err(|error| SparseShadowCmpError::io("open dense shadow source", error))?;
        let output = std::fs::File::create(&shadow)
            .map_err(|error| SparseShadowCmpError::io("create sparse shadow output", error))?;
        let candidate_observer = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let writeback_timing = Arc::new(StreamPullTiming::default());
        let mut writer = DecryptedFileSink::new(
            output,
            MaterializationMode::Enabled,
            block_bytes,
            Arc::clone(&candidate_observer),
            Some(writeback_coordinator),
            Some(writeback_timing),
            JobPriority::default(),
        )
        .map_err(|error| SparseShadowCmpError::io("create sparse shadow writer", error))?;
        let mut buffer = vec![0; 1024 * 1024];
        let mut read_through = 0_u64;
        let mut advised_through = 0_u64;
        loop {
            if shutdown_token.is_cancelled() {
                return Err(SparseShadowCmpError::Cancelled);
            }
            let read = reader
                .read(&mut buffer)
                .map_err(|error| SparseShadowCmpError::io("read dense shadow source", error))?;
            if read == 0 {
                break;
            }
            let chunk = buffer.get(..read).ok_or_else(|| {
                SparseShadowCmpError::io(
                    "read dense shadow source",
                    std::io::Error::other("read exceeded shadow buffer"),
                )
            })?;
            writer
                .write_all(chunk)
                .map_err(|error| SparseShadowCmpError::io("write sparse shadow output", error))?;
            read_through = read_through.saturating_add(read as u64);
            if read_through.saturating_sub(advised_through) >= STREAM_WRITEBACK_WINDOW_BYTES {
                advise_shadow_read_range(
                    &reader,
                    path,
                    advised_through,
                    read_through.saturating_sub(advised_through),
                    "read dense sparse-shadow source",
                );
                advised_through = read_through;
            }
        }
        writer
            .complete()
            .map_err(|error| SparseShadowCmpError::io("finalize sparse shadow output", error))?;
        if advised_through < read_through {
            advise_shadow_read_range(
                &reader,
                path,
                advised_through,
                read_through.saturating_sub(advised_through),
                "read dense sparse-shadow source",
            );
        }
        let candidate_hole_bytes = candidate_observer.load(std::sync::atomic::Ordering::Relaxed);
        let output = std::fs::File::open(&shadow)
            .map_err(|error| SparseShadowCmpError::io("open sparse shadow output", error))?;
        page_cache::advise_dontneed(&reader, path, "read dense sparse-shadow source");
        page_cache::advise_dontneed(&output, &shadow, "write sparse shadow comparison file");
        drop(reader);
        drop(output);

        compare_completed_files(path, &shadow, shutdown_token)?;

        let shadow_metadata = std::fs::metadata(&shadow)
            .map_err(|error| SparseShadowCmpError::io("stat sparse shadow output", error))?;
        let shadow_logical_bytes = shadow_metadata.len();
        #[cfg(unix)]
        let shadow_allocated_bytes = {
            use std::os::unix::fs::MetadataExt;
            shadow_metadata.blocks().saturating_mul(512)
        };
        #[cfg(not(unix))]
        let shadow_allocated_bytes = shadow_logical_bytes;
        Ok(ShadowComparisonObservation {
            logical_bytes: shadow_logical_bytes,
            allocated_bytes: shadow_allocated_bytes,
            candidate_hole_bytes,
        })
    })();

    match std::fs::remove_file(&shadow) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => tracing::warn!(
            path = %shadow.display(),
            %error,
            "failed to remove sparse shadow comparison file"
        ),
    }
    result
}

pub(super) struct ShadowComparisonLifecycle {
    pub(super) admission: std::sync::Mutex<()>,
    pub(super) shutdown_token: CancellationToken,
    pub(super) tasks: TaskTracker,
}

#[derive(Clone)]
pub struct ShadowComparisonController {
    pub(super) inner: Arc<ShadowComparisonLifecycle>,
}

impl ShadowComparisonController {
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(ShadowComparisonLifecycle {
                admission: std::sync::Mutex::new(()),
                shutdown_token: CancellationToken::new(),
                tasks: TaskTracker::new(),
            }),
        }
    }

    pub(super) fn spawn<F>(&self, future: F) -> Option<tokio::task::JoinHandle<F::Output>>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let _admission = self
            .inner
            .admission
            .lock()
            .expect("shadow comparison admission lock poisoned");
        (!self.inner.shutdown_token.is_cancelled()).then(|| self.inner.tasks.spawn(future))
    }

    pub(super) fn is_accepting(&self) -> bool {
        !self.inner.shutdown_token.is_cancelled()
    }

    pub(super) fn shutdown_token(&self) -> CancellationToken {
        self.inner.shutdown_token.clone()
    }

    pub async fn shutdown(&self, timeout: std::time::Duration) -> bool {
        {
            let _admission = self
                .inner
                .admission
                .lock()
                .expect("shadow comparison admission lock poisoned");
            self.inner.shutdown_token.cancel();
            self.inner.tasks.close();
        }
        tokio::time::timeout(timeout, self.inner.tasks.wait())
            .await
            .is_ok()
    }
}

pub(super) struct SparseShadowTask {
    pub(super) job_id: JobId,
    pub(super) subject_id: String,
    pub(super) local_path: PathBuf,
    pub(super) snapshot_path: PathBuf,
    pub(super) block_bytes: usize,
    pub(super) artifact: &'static str,
}

pub(super) struct SparseShadowAdmission {
    pub(super) _shadow_permit: tokio::sync::OwnedSemaphorePermit,
    pub(super) _codec_permit: CodecPermit,
}

pub(super) fn sparse_shadow_disk_floor_bytes(
    status: pplx_space_common_core::host::disk::DiskStatus,
) -> u64 {
    pplx_space_common_core::host::disk_admission::CacheDiskAdmissionConfig::default()
        .hard_floor_bytes(status.total_bytes)
}

pub(super) fn sparse_shadow_has_disk_headroom(
    status: pplx_space_common_core::host::disk::DiskStatus,
    logical_bytes: u64,
) -> bool {
    status.available_bytes >= sparse_shadow_disk_floor_bytes(status).saturating_add(logical_bytes)
}

pub(super) async fn remove_sparse_shadow_snapshot(path: &Path, reason: &'static str) {
    if let Err(error) = tokio::fs::remove_file(path).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(
            shadow_source_path = %path.display(),
            %error,
            reason,
            "failed to remove sparse shadow snapshot"
        );
    }
}

impl TransferExecutor {
    pub(super) fn try_acquire_sparse_shadow_admission(&self) -> Option<SparseShadowAdmission> {
        let shadow_permit = Arc::clone(&self.shadow_comparison_permits)
            .try_acquire_owned()
            .ok()?;
        let codec_permit = self.codec.try_acquire_shared()?;
        Some(SparseShadowAdmission {
            _shadow_permit: shadow_permit,
            _codec_permit: codec_permit,
        })
    }

    pub(super) async fn schedule_sparse_shadow_comparison(
        &self,
        job_id: &JobId,
        kind: &str,
        subject_id: &str,
        local_path: &Path,
        snapshot_artifact: Option<SnapshotArtifact>,
        mode: MaterializationMode,
    ) -> Option<tokio::task::JoinHandle<Option<ShadowComparisonObservation>>> {
        if mode != MaterializationMode::Shadow {
            return None;
        }
        if !self.shadow_comparisons.is_accepting() {
            return None;
        }

        let snapshot_artifact =
            snapshot_artifact.or_else(|| snapshot_artifact_from_local_path(local_path));
        let artifact = transfer_artifact_tag(snapshot_artifact);
        let block_bytes = materialization_block_bytes(snapshot_artifact);
        let Some(admission) = self.try_acquire_sparse_shadow_admission() else {
            pplx_space_metrics::volume_manager::transfer_materialization_shadow_comparison(
                artifact,
                ShadowComparisonOutcome::SkippedBusy.as_str(),
            );
            tracing::info!(
                job_id = %job_id,
                subject_id,
                local_path = %local_path.display(),
                "background sparse shadow comparison skipped because the node is busy"
            );
            return None;
        };
        let Ok(metadata) = tokio::fs::metadata(local_path).await else {
            pplx_space_metrics::volume_manager::transfer_materialization_shadow_comparison(
                artifact,
                ShadowComparisonOutcome::Error.as_str(),
            );
            tracing::error!(
                job_id = %job_id,
                subject_id,
                local_path = %local_path.display(),
                "could not inspect completed artifact before sparse shadow comparison"
            );
            return None;
        };
        let logical_bytes = metadata.len();
        #[cfg(unix)]
        let disk_status = self
            .shadow_staging_dir
            .to_str()
            .and_then(pplx_space_common_core::host::disk::statvfs_status);
        #[cfg(not(unix))]
        let disk_status: Option<pplx_space_common_core::host::disk::DiskStatus> = None;
        let disk_floor_bytes = disk_status.map_or(0, sparse_shadow_disk_floor_bytes);
        let has_disk_headroom = disk_status
            .is_some_and(|status| sparse_shadow_has_disk_headroom(status, logical_bytes));
        if !has_disk_headroom {
            pplx_space_metrics::volume_manager::transfer_materialization_shadow_comparison(
                artifact,
                ShadowComparisonOutcome::SkippedDisk.as_str(),
            );
            tracing::warn!(
                job_id = %job_id,
                subject_id,
                local_path = %local_path.display(),
                logical_bytes,
                available_bytes = disk_status.map(|status| status.available_bytes),
                disk_floor_bytes,
                "background sparse shadow comparison skipped for disk headroom"
            );
            return None;
        }
        let snapshot_path = sparse_shadow_source_path(&self.shadow_staging_dir);
        let snapshot_started_at = Instant::now();
        if let Err(error) = crate::staging::reflink_only(local_path, &snapshot_path).await {
            remove_sparse_shadow_snapshot(&snapshot_path, "reflink failed").await;
            pplx_space_metrics::volume_manager::transfer_materialization_shadow_comparison(
                artifact,
                ShadowComparisonOutcome::Error.as_str(),
            );
            tracing::error!(
                job_id = %job_id,
                subject_id,
                local_path = %local_path.display(),
                shadow_source_path = %snapshot_path.display(),
                %error,
                "could not snapshot completed artifact for background sparse shadow comparison"
            );
            return None;
        }
        emit_transfer_phase_duration(
            kind,
            artifact,
            TransferPhase::ShadowReflink,
            MaterializationMode::Shadow,
            snapshot_started_at,
        );

        tracing::info!(
            job_id = %job_id,
            subject_id,
            local_path = %local_path.display(),
            shadow_source_path = %snapshot_path.display(),
            snapshot_elapsed_ms = snapshot_started_at.elapsed().as_millis(),
            "background sparse shadow comparison scheduled"
        );

        let cleanup_path = snapshot_path.clone();
        let handle = self.spawn_sparse_shadow_comparison(
            SparseShadowTask {
                job_id: *job_id,
                subject_id: subject_id.to_string(),
                local_path: local_path.to_path_buf(),
                snapshot_path,
                block_bytes,
                artifact,
            },
            admission,
        );
        if handle.is_none() {
            remove_sparse_shadow_snapshot(&cleanup_path, "task admission closed").await;
        }
        handle
    }

    pub(super) fn spawn_sparse_shadow_comparison(
        &self,
        task: SparseShadowTask,
        admission: SparseShadowAdmission,
    ) -> Option<tokio::task::JoinHandle<Option<ShadowComparisonObservation>>> {
        let shutdown_token = self.shadow_comparisons.shutdown_token();
        let writeback_coordinator = Arc::clone(&self.stream_writeback);
        self.shadow_comparisons.spawn(async move {
            let SparseShadowTask {
                job_id,
                subject_id,
                local_path,
                snapshot_path,
                block_bytes,
                artifact,
            } = task;
            let _admission = admission;
            if shutdown_token.is_cancelled() {
                remove_sparse_shadow_snapshot(&snapshot_path, "shutdown before comparison").await;
                return None;
            }
            let comparison_path = snapshot_path.clone();
            let blocking_shutdown_token = shutdown_token.clone();
            let result = tokio::task::spawn_blocking(move || {
                compare_sparse_shadow(
                    &comparison_path,
                    block_bytes,
                    &blocking_shutdown_token,
                    writeback_coordinator,
                )
            })
            .await;
            remove_sparse_shadow_snapshot(&snapshot_path, "comparison finished").await;

            match result {
                Ok(Ok(observation)) => {
                    pplx_space_metrics::volume_manager::transfer_materialization_shadow_comparison(
                        artifact,
                        ShadowComparisonOutcome::Match.as_str(),
                    );
                    pplx_space_metrics::volume_manager::transfer_materialization_allocation(
                        "pull",
                        artifact,
                        MaterializationMode::Shadow,
                        MaterializationLayout::Sparse,
                        observation.logical_bytes,
                        observation.allocated_bytes,
                        observation.candidate_hole_bytes,
                    );
                    tracing::info!(
                        job_id = %job_id,
                        subject_id,
                        local_path = %local_path.display(),
                        shadow_logical_bytes = observation.logical_bytes,
                        shadow_allocated_bytes = observation.allocated_bytes,
                        candidate_hole_bytes = observation.candidate_hole_bytes,
                        "background sparse shadow comparison completed"
                    );
                    Some(observation)
                }
                Ok(Err(SparseShadowCmpError::Cancelled)) => {
                    tracing::info!(
                        job_id = %job_id,
                        subject_id,
                        local_path = %local_path.display(),
                        "background sparse shadow comparison cancelled during shutdown"
                    );
                    None
                }
                Ok(Err(error)) => {
                    let outcome = if matches!(error, SparseShadowCmpError::Mismatch) {
                        ShadowComparisonOutcome::Mismatch
                    } else {
                        ShadowComparisonOutcome::Error
                    };
                    pplx_space_metrics::volume_manager::transfer_materialization_shadow_comparison(
                        artifact,
                        outcome.as_str(),
                    );
                    tracing::error!(
                        job_id = %job_id,
                        subject_id,
                        local_path = %local_path.display(),
                        ?error,
                        comparison = outcome.as_str(),
                        "background sparse shadow comparison failed"
                    );
                    None
                }
                Err(error) => {
                    pplx_space_metrics::volume_manager::transfer_materialization_shadow_comparison(
                        artifact,
                        ShadowComparisonOutcome::Error.as_str(),
                    );
                    tracing::error!(
                        job_id = %job_id,
                        subject_id,
                        local_path = %local_path.display(),
                        %error,
                        "background sparse shadow comparison task panicked"
                    );
                    None
                }
            }
        })
    }
}
