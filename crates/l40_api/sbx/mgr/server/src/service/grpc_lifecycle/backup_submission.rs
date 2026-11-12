//! Durable artifact-backup submission shared by suspend, teardown suspend,
//! `EnqueueArtifactBackup`, and reupload: upload-config resolution, snapshot
//! metadata, and the restore-visible backup handoff.

use super::*;

/// Sidecar metadata of the suspend capture `local_artifact_id`, which this
/// host takes, so it records this host's CPU as the capture CPU.
pub(super) fn suspend_artifact_metadata(
    sandbox: &crate::db::SandboxRow,
    format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
    local_artifact_id: LocalArtifactId,
    encrypted: bool,
    rootfs_send_source_id: Option<uuid::Uuid>,
) -> Result<crate::snapshot_upload::SnapshotMeta, Status> {
    let mut metadata = captured_suspend_snapshot_metadata(sandbox, format)?;
    metadata.local_artifact_id = Some(local_artifact_id);
    metadata.artifact_kind = Some(ArtifactKind::Full);
    metadata.artifact_origin = Some(ArtifactOrigin::Suspend);
    metadata.encrypted = encrypted;
    metadata.rootfs_send_source_id = rootfs_send_source_id;
    Ok(metadata)
}

/// The execution metadata an artifact backup records. A disk artifact carries
/// no vCPU state, so it has no capture CPU, whatever its metadata says.
pub(super) fn artifact_backup_execution(
    execution: pplx_space_common_core::types::SnapshotExecutionMetadata,
    artifact_kind: pplx_space_common_core::types::ArtifactKind,
) -> pplx_space_common_core::types::SnapshotExecutionMetadata {
    match artifact_kind {
        pplx_space_common_core::types::ArtifactKind::Full => execution,
        pplx_space_common_core::types::ArtifactKind::Disk => execution.with_host_cpu(None),
    }
}

/// [`suspend_snapshot_metadata`] for a suspend this host just captured. The VM
/// state was captured here, so this host's CPU is recorded as its capture CPU:
/// a host with a different CPU may not be able to load the vCPU state.
pub(super) fn captured_suspend_snapshot_metadata(
    sandbox: &crate::db::SandboxRow,
    format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
) -> Result<crate::snapshot_upload::SnapshotMeta, Status> {
    let mut metadata = suspend_snapshot_metadata(sandbox, format)?;
    metadata.execution = metadata.execution.map(|execution| {
        execution.with_host_cpu(pplx_space_common_core::types::HostCpu::current().cloned())
    });
    Ok(metadata)
}

/// Snapshot metadata built from the sandbox's current state. It records no
/// capture CPU: a caller rebuilding metadata for an artifact captured earlier
/// can't know which host captured it.
pub(in crate::service) fn suspend_snapshot_metadata(
    sandbox: &crate::db::SandboxRow,
    format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
) -> Result<crate::snapshot_upload::SnapshotMeta, Status> {
    let snapshot_execution = sandbox_snapshot_execution_metadata(sandbox)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let execution = snapshot_execution.execution().clone();
    let rootfs_diff_format = match format {
        pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend => {
            Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1.to_string())
        }
        _ => None,
    };
    Ok(crate::snapshot_upload::SnapshotMeta {
        version: crate::snapshot_upload::SNAPSHOT_METADATA_VERSION,
        execution: Some(snapshot_execution),
        sandbox_id: sandbox.id.clone(),
        size_bytes: 0,
        rootfs_size_bytes: None,
        created_at: jiff::Timestamp::now().to_string(),
        source: crate::snapshot_upload::SnapshotSource {
            runtime: execution.runtime().kind.alias().to_string(),
            runtime_version: None,
            state_format_version: None,
            template_id: sandbox.template.clone(),
            content_hash: sandbox.template_hash.clone(),
            rootfs_size_mib: sandbox.rootfs_size_mb,
            disk_io_max_operations_per_sec: Some(sandbox.disk_io_limits.max_operations_per_sec()),
            disk_io_max_bandwidth_mib_per_sec: Some(
                sandbox.disk_io_limits.max_bandwidth_mib_per_sec(),
            ),
            rootfs_version: None,
            kernel_version: None,
            kernel_digest: None,
        },
        sandbox: Some(crate::snapshot_upload::SnapshotSandbox {
            owner: sandbox.user.to_string(),
            snapshots_enabled: sandbox.snapshots_enabled,
            process_env: sandbox.process_env.clone(),
            auto_lifecycle: sandbox.auto_lifecycle.clone(),
        }),
        rootfs_diff_format,
        rootfs_send_source_id: None,
        // Set by the suspend publication once it knows whether the sandbox's
        // live rootfs is encrypted (BYOK); plaintext suspends leave it false.
        encrypted: false,
        rootfs_encryption_source: None,
        local_artifact_id: None,
        artifact_kind: None,
        artifact_origin: None,
        artifact_snapshot_name: None,
    })
}

fn suspend_upload_payload(
    sandbox: &crate::db::SandboxRow,
    local_artifact_id: LocalArtifactId,
    job: &pplx_space_common_jobs::Job,
) -> Result<crate::snapshot_upload::SnapshotUploadJobs, ArtifactBackupSubmissionError> {
    let payload = job
        .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
        .map_err(|error| ArtifactBackupSubmissionError::Other(error.to_string()))?;
    // Older payloads may omit the execution fence after completing handoff.
    // Exact immutable artifact identity plus staged metadata/transfers proves
    // that existing handoff; it never authorizes a new unfenced registration.
    let completed_legacy_handoff = payload.execution_generation.is_none()
        && payload.includes_metadata_job
        && !payload.job_ids.is_empty();
    if payload.meta.local_artifact_id != Some(local_artifact_id)
        || payload.meta.sandbox_id != sandbox.id
        || (payload.execution_generation != node_local_registration_generation(sandbox)
            && !completed_legacy_handoff)
    {
        return Err(ArtifactBackupSubmissionError::TerminalHandoff(
            "persisted upload has a different capture owner".to_owned(),
        ));
    }
    // Do not create another attempt on every cleanup tick. The cleanup
    // owner retains the artifact and ends a terminal handoff explicitly.
    if job.is_terminal()
        && (job.status != pplx_space_common_jobs::JobStatus::Completed.as_str()
            || job.failure.is_some())
    {
        return Err(ArtifactBackupSubmissionError::TerminalHandoff(format!(
            "persisted suspend upload requires resolution: {}",
            job.error.as_deref().unwrap_or("upload failed")
        )));
    }
    Ok(payload)
}

#[cfg(test)]
impl SandboxManagerService {
    pub(in crate::service) async fn submit_artifact_backup_for_test(
        &self,
        sandbox: &crate::db::SandboxRow,
        artifact_attempt: (
            LocalArtifactId,
            pplx_space_common_core::types::BackupAttemptId,
        ),
        snapshot_dir: &str,
        request_override: &crate::snapshot_upload::SnapshotUploadConfig,
        restore_visible: bool,
    ) -> Result<SubmitSuspendSnapshotOutcome, String> {
        let (local_artifact_id, backup_attempt_id) = artifact_attempt;
        self.submit_artifact_backup(
            sandbox,
            ArtifactBackupSubmission {
                local_artifact_id,
                artifact_kind: pplx_space_common_core::types::ArtifactKind::Full,
                artifact_origin: pplx_space_common_core::types::ArtifactOrigin::Suspend,
                snapshot_name: None,
                backup_attempt_id,
                snapshot_dir,
                request_override: Some(request_override),
                format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Full,
                handoff: if restore_visible {
                    ArtifactBackupHandoff::RestoreVisible
                } else {
                    ArtifactBackupHandoff::DurableWorker
                },
                execution_generation: None,
                capture_vouched: false,
            },
        )
        .await
        .map_err(|error| error.to_string())
    }
}

impl SandboxManagerService {
    /// Retry handoff for at most fifteen minutes from durable cleanup admission.
    /// Each remote attempt has thirty seconds; neither retries nor restarts renew
    /// the clock. Terminal failure retains the capture and finishes cleanup.
    pub(in crate::service) async fn ensure_suspend_backup_before_cleanup(
        &self,
        sandbox_id: &str,
        published: Option<&(LocalArtifactId, String)>,
        job_id: Option<&JobId>,
        now: impl Fn() -> jiff::Timestamp,
    ) -> Result<Option<crate::db::cleanup_jobs::CleanupOutcome>, Status> {
        let (row, artifact, path) = {
            let db = self.db_conn()?;
            let row = crate::db::read::sandbox(&db, sandbox_id)
                .map_err(|error| {
                    Status::internal(format!("read suspend cleanup evidence: {error}"))
                })?
                .ok_or_else(|| Status::aborted("suspend cleanup owner disappeared"))?;
            // Restore retirement also uses MarkSuspended, but its persisted
            // lifecycle is stopping. Only a newly published suspend owns handoff.
            // Use durable state so rediscovery/restart preserves this distinction.
            if row.state.lifecycle() != SandboxState::Suspending {
                return Ok(None);
            }
            let artifact = match published {
                Some(artifact) => Some(artifact.clone()),
                None => match row.current_local_artifact_id {
                    Some(id) => crate::db::read::local_artifact(
                        &db,
                        crate::db::read::LocalArtifactKey::Id(id),
                    )
                    .map_err(|error| {
                        Status::internal(format!("read suspend cleanup evidence: {error}"))
                    })?
                    .map(|artifact| (artifact.local_artifact_id, artifact.local_path)),
                    None => None,
                },
            };
            // Missing publication is classified by the existing finalizer.
            let Some((artifact, path)) = artifact else {
                return Ok(None);
            };
            if let Some(status) = crate::db::read::sandbox_view(&db, sandbox_id)
                .teardown()
                .status()
                .map_err(|error| {
                    Status::internal(format!("read suspend cleanup evidence: {error}"))
                })?
            {
                if status.backup_completed
                    && status
                        .artifact
                        .as_ref()
                        .is_some_and(|record| record.local_artifact_id == artifact)
                {
                    return Ok(None);
                }
                if status
                    .operation
                    .as_ref()
                    .is_some_and(|operation| operation.status.is_active())
                {
                    return Err(Status::unavailable(
                        "teardown suspend awaits remote durability",
                    ));
                }
            }
            (row, artifact, path)
        };
        let created = if let Some(job_id) = job_id {
            let created = crate::db::read::cleanup_job_created_at(&*self.db_conn()?, job_id)
                .map_err(|error| Status::internal(format!("read handoff admission time: {error}")))?
                .ok_or_else(|| Status::aborted("suspend cleanup job disappeared"))?;
            Some(created.get())
        } else {
            // Foreground-only probes cannot renew or replace a durable job.
            None
        };
        let age_at = |now: jiff::Timestamp| {
            created.map_or(0, |created| now.as_second().saturating_sub(created).max(0))
        };
        let age = age_at(now());
        // Even an expired retry must accept an already persisted handoff: a
        // restart or scheduling delay cannot invalidate completed ownership.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs((15 * 60 - age).clamp(1, 30).cast_unsigned()),
            self.enqueue_node_local_suspend_backup(&row, artifact, &path, None),
        )
        .await
        .unwrap_or_else(|_| {
            Err(ArtifactBackupSubmissionError::Other(
                "backup handoff attempt timed out".to_owned(),
            ))
        });
        // Charge the awaited attempt against the same immutable admission.
        let age = age_at(now());
        match result {
            Ok(_) => Ok(None),
            Err(error) if error.handoff_is_terminal() || age >= 15 * 60 => {
                let reason = format!(
                    "suspend backup handoff ended at age {age}s; requires operator recovery: {error}"
                );
                self.settle_failed_suspend_handoff(&row, artifact, job_id, &reason)
            }
            Err(error) => {
                if age >= 5 * 60 {
                    tracing::error!(sandbox_id, %artifact, age_secs = age, %error,
                        "suspend backup handoff pending over five minutes; capture retained for retry");
                }
                Err(artifact_backup_fence_status(error.to_string()))
            }
        }
    }

    pub(super) fn settle_failed_suspend_handoff(
        &self,
        row: &crate::db::SandboxRow,
        artifact: LocalArtifactId,
        job_id: Option<&JobId>,
        reason: &str,
    ) -> Result<Option<crate::db::cleanup_jobs::CleanupOutcome>, Status> {
        use crate::db::cleanup_jobs::CleanupOutcome;
        let applied = {
            // The upload worker persists transfer IDs through this same mutex.
            // Hold it from the final evidence read through quarantine so its
            // successful handoff cannot land between those two decisions.
            let db = self.db_conn()?;
            if let Some(upload) = crate::db::read::local_artifact_upload(&db, artifact)
                .map_err(|error| Status::internal(format!("re-read suspend handoff: {error}")))?
                && let Ok(payload) = suspend_upload_payload(row, artifact, &upload)
                && !payload.job_ids.is_empty()
            {
                return Ok(None);
            }
            crate::db::quarantine_refused_suspend_backup(&db, row, artifact, reason, job_id)
                .map_err(|error| Status::internal(format!("settle suspend handoff: {error}")))?
        };
        if applied {
            tracing::error!(sandbox_id = %row.id, %artifact, %reason,
                "suspend backup handoff ended; capture retained");
            mark_sandbox_dirty(&self.db, &row.id);
        }
        Ok(Some(if applied {
            CleanupOutcome::Quarantined(reason.to_owned())
        } else {
            CleanupOutcome::Superseded
        }))
    }

    /// Resolve the org-level S3 override for this sandbox. Returns one
    /// of three classifications:
    ///
    /// - [`OrgSnapshotConfigResolution::Resolved`] — override present
    ///   and parses cleanly.
    /// - [`OrgSnapshotConfigResolution::Absent`] — no cluster-api wired
    ///   (local dev), sandbox row not yet visible (benign create
    ///   race), or org left the override unset / empty. Callers fall
    ///   back to the env-derived default.
    /// - [`OrgSnapshotConfigResolution::LookupFailed`] — cluster-api
    ///   unreachable, RPC errored with non-`NotFound`, connect failed
    ///   with bearer/endpoint/transport error, or the override URI
    ///   itself failed to parse. The suspend RPC collapses this to
    ///   silent fall-through (the local snapshot is authoritative);
    ///   backup admission surfaces it as `Internal` so the reconciler
    ///   retries rather than escalating to mark-as-failed.
    pub(super) async fn resolve_org_snapshot_upload_config(
        &self,
        sandbox_id: &str,
    ) -> OrgSnapshotConfigResolution {
        use pplx_space_node_api_client::cluster::{ConnectClusterApiError, connect_from_env};

        let mut client = match connect_from_env().await {
            Ok(c) => c,
            Err(
                ConnectClusterApiError::MissingEndpoint | ConnectClusterApiError::MissingToken(_),
            ) => {
                // Node is not wired to a cluster API (e.g. local dev) —
                // definitively no override.
                return OrgSnapshotConfigResolution::Absent;
            }
            Err(e) => {
                // Transport errors and bearer/endpoint config bugs are
                // not "absent" — they're a lookup failure. Callers that
                // need to retry (reupload) treat this as `Internal`.
                return OrgSnapshotConfigResolution::LookupFailed(format!(
                    "cluster-api connect failed: {e}"
                ));
            }
        };

        let req = pplx_space_node_api_client::proto::GetSandboxSnapshotStorageUriRequest {
            sandbox_id: sandbox_id.to_string(),
        };
        let uri = match client.get_sandbox_snapshot_storage_uri(req).await {
            Ok(resp) => resp.into_inner().storage_uri,
            Err(status) if status.code() == tonic::Code::NotFound => {
                // For suspend this is a benign create race (the local
                // SandboxRow exists before the cluster row is committed).
                // For reupload it indicates state divergence — the call
                // site classifies. Returning the distinct
                // `ClusterRowMissing` variant keeps suspend's silent
                // fall-through while letting reupload reject up front.
                return OrgSnapshotConfigResolution::ClusterRowMissing;
            }
            Err(status) => {
                return OrgSnapshotConfigResolution::LookupFailed(format!(
                    "GetSandboxSnapshotStorageUri failed: code={:?} message={}",
                    status.code(),
                    status.message(),
                ));
            }
        };
        let uri = match uri {
            Some(u) if !u.is_empty() => u,
            _ => return OrgSnapshotConfigResolution::Absent,
        };

        match crate::snapshot_upload::SnapshotUploadConfig::from_uri(&uri) {
            Ok(cfg) => OrgSnapshotConfigResolution::Resolved(cfg),
            Err(e) => {
                // Malformed override is operator-fixable, not transient,
                // but it's also not "absent" — there is an override, it's
                // broken. Surfacing as `LookupFailed` keeps suspend's
                // silent fall-through behavior intact (the call site logs
                // and substitutes the env default) while letting reupload
                // return `Internal` so the reconciler doesn't mark the
                // sandbox failed on a config bug an operator can fix.
                OrgSnapshotConfigResolution::LookupFailed(format!(
                    "org snapshot_base_object_storage_uri did not parse: {e}"
                ))
            }
        }
    }

    pub(super) async fn submit_artifact_backup(
        &self,
        sandbox: &crate::db::SandboxRow,
        submission: ArtifactBackupSubmission<'_>,
    ) -> Result<SubmitSuspendSnapshotOutcome, ArtifactBackupSubmissionError> {
        let ArtifactBackupSubmission {
            local_artifact_id,
            artifact_kind,
            artifact_origin,
            snapshot_name,
            backup_attempt_id,
            snapshot_dir,
            request_override,
            format,
            handoff,
            execution_generation,
            capture_vouched,
        } = submission;
        let config: &crate::snapshot_upload::SnapshotUploadConfig = match request_override {
            Some(c) => c,
            None => match self.snapshot_upload_config.as_ref() {
                Some(c) => c,
                None => return Ok(SubmitSuspendSnapshotOutcome::NoUploadConfig),
            },
        };

        // Encrypted-rootfs sandboxes are supported here: the publication staged
        // the encrypted `snapshot.luks` (vmstate+memory) and `cow.luks` (rootfs
        // delta), and `metadata.json` carries `encrypted: true` so the artifact
        // set below resolves to the encrypted shape. Nothing to refuse.
        let meta_path = std::path::Path::new(snapshot_dir)
            .join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME);
        let mut meta = match std::fs::read(&meta_path) {
            Ok(bytes) => serde_json::from_slice::<crate::snapshot_upload::SnapshotMeta>(&bytes)
                .map_err(|err| {
                    format!(
                        "published snapshot metadata is unparseable at {}: {err}",
                        meta_path.display()
                    )
                })?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                suspend_snapshot_metadata(sandbox, format).map_err(|error| error.to_string())?
            }
            Err(err) => {
                return Err(format!(
                    "read published snapshot metadata at {} failed: {err}",
                    meta_path.display()
                )
                .into());
            }
        };
        meta.local_artifact_id = Some(local_artifact_id);
        meta.artifact_kind = Some(artifact_kind);
        meta.artifact_origin = Some(artifact_origin);
        meta.artifact_snapshot_name = snapshot_name
            .map(pplx_space_common_core::types::SnapshotName::parse)
            .transpose()
            .map_err(|error| format!("invalid artifact snapshot name: {error}"))?;
        // Normalize legacy metadata before persisting the durable intent. The
        // worker may execute after this process exits and must not need to
        // rediscover capture-time execution identity from the replacement
        // host.
        meta.execution = Some(artifact_backup_execution(
            meta.execution_metadata(host_architecture().map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?,
            artifact_kind,
        ));

        // Every backup attempt gets a fresh UUIDv7 identity. It names the
        // per-attempt S3 prefix before any vol-mgr submission; the cluster row is
        // inserted in state `creating` against this same id and flipped to
        // `ready`/`failed` by the durable worker tail.
        let mut snapshot_id = backup_attempt_id.to_string();
        // Prepare the exact immutable artifact locally, but perform no remote
        // side effects here. The durable worker job below must exist before
        // vol-mgr submission, ClusterApi.BeginArtifactBackup, or runtime
        // teardown so cancellation at any later point has an owner that can
        // resume the complete pipeline.
        let retained = self
            .inspect_retained_upload(&meta, std::path::Path::new(snapshot_dir), format)
            .await?;
        let mut jobs = crate::snapshot_upload::prepare_suspend_snapshot(
            &meta,
            &snapshot_id,
            std::path::Path::new(snapshot_dir),
            config,
            artifact_kind,
            format,
            retained,
        )
        .map_err(|error| format!("prepare snapshot upload intent failed: {error}"))?;
        jobs.execution_generation = execution_generation;
        // Ordered by only on evidence: the caller's, or the record the
        // capturing teardown published beside the bytes, naming this
        // generation for this artifact.
        jobs.execution_generation_names_capture = execution_generation.is_some_and(|generation| {
            capture_vouched
                || crate::capture_record::read_capture_order(
                    std::path::Path::new(snapshot_dir),
                    local_artifact_id,
                )
                .ok()
                .flatten()
                    == Some(generation)
        });
        let mut storage_uri = jobs.storage_uri.clone();
        let dedup_key = crate::snapshot_upload_jobs::artifact_backup_dedup_key(local_artifact_id);
        let local_artifact_id = local_artifact_id.to_string();

        #[cfg(test)]
        crate::auto_lifecycle::AutoLifecycleCoordinator::rendezvous(
            &self.auto_lifecycle.backup_admission_barrier,
        )
        .await;
        let job_id = match self.snapshot_upload_worker.submit_json(
            crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            &jobs,
            Some(&sandbox.id),
            Some(&dedup_key),
        ) {
            Ok(id) => id,
            Err(e) => {
                let reason = format!("snapshot_upload worker rejected the job: {e}");
                emit_sandbox_event(
                    &sandbox.id,
                    SandboxEventType::ArtifactBackupFailed,
                    SandboxEventSource::SbxManager,
                    SandboxEventDetails::default()
                        .snapshot_id(snapshot_id.clone())
                        .backup_attempt_id(snapshot_id.clone())
                        .local_artifact_id(local_artifact_id.to_string())
                        .artifact_kind(artifact_kind.as_str())
                        .artifact_origin(artifact_origin.as_str())
                        .object_uri(storage_uri.clone())
                        .reason("snapshot_upload_worker_submit")
                        .rootfs_diff_format(meta.rootfs_diff_format.as_deref().unwrap_or("full"))
                        .error(reason.clone()),
                );
                crate::snapshot_upload::cleanup_metadata_staging_dir(&jobs);
                return Err(reason.into());
            }
        };

        // `submit_json` returns the artifact's active upload when it has one.
        // That job owns its immutable backup attempt: join its
        // payload instead of replacing it with this caller's newly prepared
        // snapshot ID and transfer set.
        let persisted_after_submit = self
            .snapshot_upload_worker
            .get_job(&job_id)
            .map_err(|error| format!("read snapshot-upload job {job_id}: {error}"))?
            .ok_or_else(|| format!("snapshot-upload job {job_id} disappeared after submit"))?
            .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
            .map_err(|error| format!("decode snapshot-upload job {job_id}: {error}"))?;
        if persisted_after_submit.snapshot_id != jobs.snapshot_id {
            crate::snapshot_upload::cleanup_metadata_staging_dir(&jobs);
            jobs = persisted_after_submit;
            snapshot_id.clone_from(&jobs.snapshot_id);
            storage_uri.clone_from(&jobs.storage_uri);
        }
        tracing::info!(
            sandbox_id = %sandbox.id,
            snapshot_upload_job_id = %job_id,
            "local artifact backup intent persisted for durable processing"
        );

        if matches!(
            handoff,
            ArtifactBackupHandoff::RestoreVisible | ArtifactBackupHandoff::TeardownFence
        ) {
            self.establish_restore_visible_artifact_backup(&job_id)
                .await?;
        }
        if handoff == ArtifactBackupHandoff::TeardownFence {
            tracing::info!(
                sandbox_id = %sandbox.id,
                snapshot_upload_job_id = %job_id,
                "snapshot artifact staged in vol-mgr before runtime teardown"
            );
        }
        emit_sandbox_event(
            &sandbox.id,
            SandboxEventType::ArtifactBackupQueued,
            SandboxEventSource::SbxManager,
            SandboxEventDetails::default()
                .snapshot_id(snapshot_id.clone())
                .backup_attempt_id(snapshot_id.clone())
                .local_artifact_id(local_artifact_id.to_string())
                .artifact_kind(artifact_kind.as_str())
                .artifact_origin(artifact_origin.as_str())
                .object_uri(storage_uri)
                .snapshot_upload_job_id(job_id.to_string())
                .rootfs_diff_format(meta.rootfs_diff_format.as_deref().unwrap_or("full")),
        );
        Ok(SubmitSuspendSnapshotOutcome::Enqueued {
            job_id,
            backup_attempt_id: snapshot_id,
        })
    }

    pub(super) async fn establish_restore_visible_artifact_backup(
        &self,
        job_id: &JobId,
    ) -> Result<crate::snapshot_upload::SnapshotUploadJobs, ArtifactBackupSubmissionError> {
        let persisted = self
            .snapshot_upload_worker
            .get_job(job_id)
            .map_err(|error| format!("read snapshot-upload job {job_id}: {error}"))?
            .ok_or_else(|| format!("snapshot-upload job {job_id} disappeared"))?;
        let mut payload = persisted
            .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
            .map_err(|error| format!("decode snapshot-upload job {job_id}: {error}"))?;
        if persisted.is_terminal() && persisted.failure.is_some() {
            // The worker can refuse registration before this caller reads
            // the job. Its persisted error has no typed generation; recover
            // the fence verdict without restarting the failed upload.
            if payload.execution_generation.is_some()
                && crate::snapshot_upload_jobs::prepared_upload_requires_begin(&payload)
                && let crate::snapshot_upload_jobs::RegistrationProbe::Superseded(generation) =
                    crate::snapshot_upload_jobs::prepared_begin_refused_as_superseded(&payload)
                        .await
            {
                return Err(ArtifactBackupSubmissionError::Begin {
                    error: pplx_space_common_jobs::JobError::permanent(
                        persisted.error.as_deref().unwrap_or("unknown error"),
                    ),
                    superseded_generation: Some(generation),
                });
            }
            return Err(format!(
                "snapshot-upload job {job_id} terminated before restore-visible handoff: {}",
                persisted.error.as_deref().unwrap_or("unknown error")
            )
            .into());
        }
        if crate::snapshot_upload_jobs::prepared_upload_requires_begin(&payload) {
            crate::snapshot_upload_jobs::begin_prepared_upload_naming_supersession(&payload)
                .await
                .map_err(|refusal| ArtifactBackupSubmissionError::Begin {
                    error: refusal.error,
                    superseded_generation: refusal.superseded_generation,
                })?;
        }
        if payload.job_ids.is_empty() {
            match crate::snapshot_upload::submit_prepared_snapshot(
                &payload,
                &self.node_manager,
                &self.volume_manager_socket_path,
            )
            .await
            {
                Ok(submitted) => {
                    match crate::snapshot_upload_jobs::persist_submitted_transfer_ids(
                        &self.db, job_id, &submitted,
                    ) {
                        Ok(()) => payload = submitted,
                        Err(error) => {
                            // The durable worker may have persisted the same
                            // deterministic transfers or completed between our
                            // read and write. Join its payload instead of
                            // failing a valid handoff.
                            payload = self
                                .persisted_submitted_artifact_backup(job_id)
                                .map_err(|reload_error| {
                                    format!(
                                        "persist staged snapshot transfer IDs: {error}; {reload_error}"
                                    )
                                })?;
                        }
                    }
                }
                Err(error) => {
                    // A concurrent worker can move/delete the prepared
                    // metadata staging file after durably recording transfer
                    // IDs. Re-read before treating the local staging error as
                    // authoritative.
                    payload = self.persisted_submitted_artifact_backup(job_id).map_err(
                        |reload_error| {
                            format!("stage snapshot artifacts in vol-mgr: {error}; {reload_error}")
                        },
                    )?;
                }
            }
        }
        Ok(payload)
    }

    fn persisted_submitted_artifact_backup(
        &self,
        job_id: &JobId,
    ) -> Result<crate::snapshot_upload::SnapshotUploadJobs, String> {
        let job = self
            .snapshot_upload_worker
            .get_job(job_id)
            .map_err(|error| format!("re-read snapshot-upload job {job_id}: {error}"))?
            .ok_or_else(|| format!("snapshot-upload job {job_id} disappeared"))?;
        let payload = job
            .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
            .map_err(|error| format!("decode snapshot-upload job {job_id}: {error}"))?;
        if payload.job_ids.is_empty() {
            return Err(format!(
                "snapshot-upload job {job_id} still has no durable transfer IDs"
            ));
        }
        Ok(payload)
    }

    /// Hand a node-local suspend's capture to a durable upload, registered
    /// under the sandbox's Kubernetes execution (see
    /// [`node_local_registration_generation`]). Any failure, a refusal of a
    /// superseded execution included, is a backup-fence failure. The caller
    /// preserves the published capture for publication/backup reconciliation.
    pub(super) async fn enqueue_node_local_suspend_backup(
        &self,
        sandbox: &crate::db::SandboxRow,
        local_artifact_id: LocalArtifactId,
        snapshot_dir: &str,
        format: Option<pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat>,
    ) -> Result<SubmitSuspendSnapshotOutcome, ArtifactBackupSubmissionError> {
        if let Some(job) = self
            .snapshot_upload_worker
            .find_job_by_dedup_key(
                crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                &crate::snapshot_upload_jobs::artifact_backup_dedup_key(local_artifact_id),
            )
            .map_err(|error| ArtifactBackupSubmissionError::Other(error.to_string()))?
        {
            let payload = suspend_upload_payload(sandbox, local_artifact_id, &job)?;
            // Transfer IDs are the durable handoff; do not reintroduce a remote
            // registration gate after they were persisted, including on restart.
            let payload = if payload.job_ids.is_empty() {
                self.establish_restore_visible_artifact_backup(&job.job_id)
                    .await?
            } else {
                payload
            };
            return Ok(SubmitSuspendSnapshotOutcome::Enqueued {
                job_id: job.job_id,
                backup_attempt_id: payload.snapshot_id,
            });
        }
        // Existing transfer ownership is independent of the local sidecar.
        // Recovery reads its format only when it must create a new upload.
        let format = match format {
            Some(format) => format,
            None => reupload_format_from_sidecar(snapshot_dir)
                .map_err(|error| ArtifactBackupSubmissionError::Other(error.to_string()))?,
        };
        self.enqueue_suspend_artifact_backup(
            sandbox,
            local_artifact_id,
            snapshot_dir,
            format,
            node_local_registration_generation(sandbox),
            // The current execution just took this capture.
            true,
        )
        .await
    }

    pub(super) async fn enqueue_suspend_artifact_backup(
        &self,
        sandbox: &crate::db::SandboxRow,
        local_artifact_id: LocalArtifactId,
        snapshot_dir: &str,
        format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
        registration_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
        capture_vouched: bool,
    ) -> Result<SubmitSuspendSnapshotOutcome, ArtifactBackupSubmissionError> {
        let request_override = match self.resolve_org_snapshot_upload_config(&sandbox.id).await {
            OrgSnapshotConfigResolution::Resolved(config) => Some(config),
            OrgSnapshotConfigResolution::Absent
            | OrgSnapshotConfigResolution::ClusterRowMissing => None,
            OrgSnapshotConfigResolution::LookupFailed(reason) => {
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    reason,
                    "could not resolve org snapshot override; falling back to env-derived snapshot upload config",
                );
                None
            }
        };
        self.submit_artifact_backup(
            sandbox,
            ArtifactBackupSubmission {
                local_artifact_id,
                artifact_kind: pplx_space_common_core::types::ArtifactKind::Full,
                artifact_origin: pplx_space_common_core::types::ArtifactOrigin::Suspend,
                snapshot_name: None,
                backup_attempt_id: pplx_space_common_core::types::BackupAttemptId::new(),
                snapshot_dir,
                request_override: request_override.as_ref(),
                format,
                handoff: ArtifactBackupHandoff::TeardownFence,
                execution_generation: registration_generation,
                capture_vouched,
            },
        )
        .await
    }
}
