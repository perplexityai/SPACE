//! Durable artifact-backup submission shared by suspend, teardown suspend,
//! `EnqueueArtifactBackup`, and reupload: upload-config resolution, snapshot
//! metadata, and the restore-visible backup handoff.

use super::*;

pub(super) fn suspend_snapshot_metadata(
    sandbox: &crate::db::SandboxRow,
    format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
) -> Result<crate::snapshot_upload::SnapshotMeta, Status> {
    let snapshot_execution = sandbox_snapshot_execution_metadata(&sandbox.id)
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
        local_artifact_id: None,
        artifact_kind: None,
        artifact_origin: None,
        artifact_snapshot_name: None,
    })
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
        dedup_key: &str,
        restore_visible: bool,
    ) -> Result<SubmitSuspendSnapshotOutcome, String> {
        let (local_artifact_id, backup_attempt_id) = artifact_attempt;
        self.submit_artifact_backup(
            sandbox,
            ArtifactBackupSubmission {
                execution_generation: None,
                local_artifact_id,
                artifact_kind: pplx_space_common_core::types::ArtifactKind::Full,
                artifact_origin: pplx_space_common_core::types::ArtifactOrigin::Suspend,
                snapshot_name: None,
                backup_attempt_id,
                snapshot_dir,
                request_override: Some(request_override),
                format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Full,
                dedup_key: Some(dedup_key),
                handoff: if restore_visible {
                    ArtifactBackupHandoff::RestoreVisible
                } else {
                    ArtifactBackupHandoff::DurableWorker
                },
            },
        )
        .await
        .map_err(|error| error.to_string())
    }
}

impl SandboxManagerService {
    pub(in crate::service) fn require_node_local_backup_handoff(
        &self,
        sandbox_id: &str,
        local_artifact_id: LocalArtifactId,
        result: Result<SubmitSuspendSnapshotOutcome, Status>,
    ) -> Result<(), Status> {
        match result {
            Ok(SubmitSuspendSnapshotOutcome::Enqueued { job_id, .. }) => {
                tracing::info!(
                    sandbox_id,
                    local_artifact_id = %local_artifact_id,
                    job_id = %job_id,
                    "persisted suspend artifact backup before runtime teardown",
                );
                Ok(())
            }
            Ok(SubmitSuspendSnapshotOutcome::NoUploadConfig) => Ok(()),
            Err(status) => {
                let reason =
                    format!("persist suspend artifact backup before runtime teardown: {status}");
                self.record_suspend_snapshot_publication_failure_after_begin(sandbox_id, &reason);
                Err(status)
            }
        }
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
            dedup_key,
            handoff,
            execution_generation,
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
        meta.execution = Some(
            meta.execution_metadata(host_architecture().map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?,
        );

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
        let mut storage_uri = jobs.storage_uri.clone();
        let source_artifact_id = local_artifact_id;
        let local_artifact_id = local_artifact_id.to_string();

        let job_id = match self.snapshot_upload_worker.submit_json_guarded(
            crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            &jobs,
            Some(&sandbox.id),
            dedup_key,
            |db| {
                if let Some(generation) = execution_generation
                    && !crate::db::teardown_suspend_owns_backup_source(
                        db,
                        &sandbox.id,
                        generation,
                        source_artifact_id,
                        snapshot_dir,
                    )
                    .map_err(|error| {
                        Status::internal(format!("read suspend backup ownership: {error}"))
                    })?
                {
                    return Err(Status::aborted(
                        "generation suspend no longer owns backup source",
                    ));
                }
                Ok(())
            },
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

        // `submit_json` may return an existing active job for a caller-supplied
        // dedup key. That job owns its immutable backup attempt: join its
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
        if persisted.is_terminal() && persisted.failure.is_some() {
            return Err(format!(
                "snapshot-upload job {job_id} terminated before restore-visible handoff: {}",
                persisted.error.as_deref().unwrap_or("unknown error")
            )
            .into());
        }
        let mut payload = persisted
            .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
            .map_err(|error| format!("decode snapshot-upload job {job_id}: {error}"))?;
        if crate::snapshot_upload_jobs::prepared_upload_requires_begin(&payload) {
            crate::snapshot_upload_jobs::begin_prepared_upload(&payload)
                .await
                .map_err(ArtifactBackupSubmissionError::Begin)?;
        }
        if payload.job_ids.is_empty() {
            match crate::snapshot_upload::submit_prepared_snapshot(&payload, &self.node_manager)
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

    pub(super) async fn enqueue_suspend_artifact_backup(
        &self,
        sandbox: &crate::db::SandboxRow,
        local_artifact_id: LocalArtifactId,
        snapshot_dir: &str,
        format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
        dedup_key: Option<&str>,
        execution_generation: Option<ExecutionGeneration>,
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
                dedup_key,
                handoff: ArtifactBackupHandoff::TeardownFence,
                execution_generation,
            },
        )
        .await
    }
}
