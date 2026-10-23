//! A teardown backup the cluster refused: carrying its verdict on the drive's
//! error, finding the refused upload, re-asking the cluster, and marking or
//! clearing the row's terminal teardown-backup marker.

use super::*;

const TEARDOWN_BACKUP_FAILED_ARTIFACT_METADATA: &str =
    "x-space-teardown-backup-failed-artifact-bin";
const TEARDOWN_BACKUP_SUPERSEDED_GENERATION_METADATA: &str =
    "x-space-teardown-backup-superseded-generation";

pub(in crate::service::grpc_lifecycle) fn teardown_artifact_backup_status(
    error: ArtifactBackupSubmissionError,
    artifact: LocalArtifactId,
) -> Status {
    let (permanent, superseded_generation) = match &error {
        ArtifactBackupSubmissionError::Begin {
            error,
            superseded_generation,
        } => (
            error.kind() == pplx_space_common_jobs::JobFailureKind::Permanent,
            *superseded_generation,
        ),
        _ => (false, None),
    };
    let mut status = artifact_backup_fence_status(error.to_string());
    if permanent {
        status = teardown_backup_failure_status(status, artifact);
        // The refusal reached the drive before the upload job failed, so the
        // driver can't recover the verdict from the job; carry it here.
        if let Some(generation) = superseded_generation {
            status = teardown_backup_superseded_status(status, generation);
        }
    }
    status
}

/// `status`, carrying the generation the cluster refused the teardown
/// backup's registration for as superseded.
pub(super) fn teardown_backup_superseded_status(
    mut status: Status,
    generation: pplx_space_common_core::types::ExecutionGeneration,
) -> Status {
    status.metadata_mut().insert(
        TEARDOWN_BACKUP_SUPERSEDED_GENERATION_METADATA,
        tonic::metadata::MetadataValue::from(generation.get()),
    );
    status
}

/// The generation a teardown backup's own registration was refused for as
/// superseded, when that refusal reached the drive directly.
pub(super) fn teardown_backup_superseded_generation(
    status: &Status,
) -> Option<pplx_space_common_core::types::ExecutionGeneration> {
    status
        .metadata()
        .get(TEARDOWN_BACKUP_SUPERSEDED_GENERATION_METADATA)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

// The shared publication path returns tonic Status. Keep backup provenance on
// its teardown-only errors until the driver has verified control-plane absence.
pub(super) fn teardown_backup_failure_status(
    mut status: Status,
    artifact: LocalArtifactId,
) -> Status {
    status.metadata_mut().insert_bin(
        TEARDOWN_BACKUP_FAILED_ARTIFACT_METADATA,
        tonic::metadata::MetadataValue::from_bytes(artifact.as_uuid().as_bytes()),
    );
    status
}

pub(super) fn teardown_failed_backup_artifact(status: &Status) -> Option<LocalArtifactId> {
    let value = status
        .metadata()
        .get_bin(TEARDOWN_BACKUP_FAILED_ARTIFACT_METADATA)?
        .to_bytes()
        .ok()?;
    Some(LocalArtifactId::from_uuid(
        uuid::Uuid::from_slice(&value).ok()?,
    ))
}

/// The payload of `job`, a terminal upload, when its registration was bound to
/// `execution_generation` and never admitted, or `None`. Only that payload's
/// registration can carry the cluster's verdict about this execution.
pub(super) fn teardown_backup_payload_of(
    job: &pplx_space_common_jobs::Job,
    execution_generation: pplx_space_common_core::types::ExecutionGeneration,
) -> Option<crate::snapshot_upload::SnapshotUploadJobs> {
    match job.payload_as::<crate::snapshot_upload::SnapshotUploadJobs>() {
        // Transfer IDs mean the registration was admitted, so a re-sent
        // Begin is an unfenced replay that can't carry a refusal. Their
        // absence doesn't prove the opposite; the probe tolerates a replay.
        Ok(payload)
            if payload.execution_generation == Some(execution_generation)
                && payload.job_ids.is_empty() =>
        {
            Some(payload)
        }
        Ok(_) => None,
        Err(error) => {
            tracing::warn!(target: super::LOG_TARGET, job_id = %job.job_id, %error,
                "could not decode the refused teardown backup's upload job");
            None
        }
    }
}

impl SandboxManagerService {
    /// The terminal upload job that staged `staged_artifact` for this
    /// generation's teardown suspend and has no transfer IDs, or `None`.
    /// Only that payload's registration can carry the cluster's verdict about
    /// this execution.
    pub(in crate::service::grpc_lifecycle) fn teardown_backup_payload_bound_to(
        &self,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        staged_artifact: LocalArtifactId,
    ) -> Option<crate::snapshot_upload::SnapshotUploadJobs> {
        let job = match self.snapshot_upload_worker.find_job_by_dedup_key(
            crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            &crate::snapshot_upload_jobs::artifact_backup_dedup_key(staged_artifact),
        ) {
            // A live job still owns its attempt: its own Begin will meet the
            // same refusal and make the job terminal, and the next drive probes.
            Ok(job) => job.filter(|job| job.is_terminal())?,
            Err(error) => {
                tracing::warn!(target: super::LOG_TARGET, %staged_artifact, %error,
                    "could not read the refused teardown backup's upload job");
                return None;
            }
        };
        teardown_backup_payload_of(&job, execution_generation)
    }

    /// Record that a teardown suspend's durable backup is over, on the row
    /// the shim keeps polling.
    ///
    /// A permanent job failure the teardown can't retry ends the artifact's
    /// upload for good: [`teardown_keyed_upload_use`] refuses a fresh
    /// attempt for it, so every later poll rejoins the same terminal job. A
    /// permanent failure that still gets a new attempt is not marked.
    /// The suspend therefore can never publish, the row can never leave
    /// `suspending`, and nothing on this node distinguishes it from a suspend
    /// still uploading — a distinction the cluster-deleted eviction needs
    /// before it may reclaim anything. The marker supplies exactly that
    /// terminality proof and nothing else; whether the sandbox is *gone*
    /// stays the cluster's call.
    ///
    /// Best-effort: a failed stamp leaves the pre-existing stuck row, which is
    /// the behavior without this marker at all, and the next poll retries.
    pub(super) fn record_teardown_suspend_backup_permanent_failure(
        &self,
        sandbox_id: &str,
        staged_artifact: LocalArtifactId,
    ) {
        let marked = self.db_conn().and_then(|db| {
            crate::db::mark_teardown_suspend_backup_permanently_failed(
                &db,
                sandbox_id,
                staged_artifact,
            )
            .map_err(|error| Status::internal(format!("db error: {error}")))
        });
        match marked {
            Ok(true) => {
                tracing::warn!(
                    target: super::LOG_TARGET,
                    sandbox_id,
                    %staged_artifact,
                    "generation suspend backup was permanently refused; marked the suspending \
                     row terminal so a cluster-deleted report can reclaim it"
                );
                mark_sandbox_dirty(&self.db, sandbox_id);
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(
                    target: super::LOG_TARGET,
                    sandbox_id,
                    %staged_artifact,
                    %error,
                    "failed to mark a permanently refused generation suspend backup; the \
                     suspending row stays unreclaimable until a later attempt marks it"
                );
            }
        }
    }

    /// Undo [`Self::record_teardown_suspend_backup_permanent_failure`] for a
    /// refusal the cluster no longer makes, so a cluster-deleted report can't
    /// reclaim the artifact under its new upload. Whether the caller may start
    /// that upload: not when the clear failed, which leaves any marker for the
    /// next drive to retry, or when the row no longer stages the artifact as
    /// `suspending`, as after a cluster-deleted eviction claimed it.
    pub(super) fn clear_teardown_suspend_backup_permanent_failure(
        &self,
        sandbox_id: &str,
        staged_artifact: LocalArtifactId,
    ) -> bool {
        use crate::db::TeardownBackupMarkerClear;
        let cleared = self.db_conn().and_then(|db| {
            crate::db::clear_teardown_suspend_backup_permanently_failed(
                &db,
                sandbox_id,
                staged_artifact,
            )
            .map_err(|error| Status::internal(format!("db error: {error}")))
        });
        match cleared {
            Ok(TeardownBackupMarkerClear::Cleared) => {
                mark_sandbox_dirty(&self.db, sandbox_id);
                true
            }
            Ok(TeardownBackupMarkerClear::Unmarked) => true,
            Ok(TeardownBackupMarkerClear::MovedOn) => false,
            Err(error) => {
                tracing::warn!(target: super::LOG_TARGET, sandbox_id, %staged_artifact, %error,
                    "failed to clear a lapsed teardown backup refusal's terminal marker; \
                     rejoining its upload until a later drive clears it");
                false
            }
        }
    }

    /// The cluster's current verdict on the registration that `job`, the
    /// newest upload under the artifact's key, failed on, when that failure is
    /// a refusal of the artifact for this generation's capture; otherwise
    /// `None`. A refusal names a retired execution or rejects the artifact, and
    /// only the first can stop holding. The recorded scope doesn't say which,
    /// so re-asking tells them apart.
    pub(super) async fn teardown_backup_refusal_verdict(
        &self,
        backup: &TeardownSuspendBackup<'_>,
        job: &pplx_space_common_jobs::Job,
    ) -> Option<crate::snapshot_upload_jobs::RegistrationProbe> {
        // The new attempt an admission starts registers under this generation,
        // so the fence that admitted the probe runs on it too.
        if backup.registration_generation() != Some(backup.execution_generation)
            || !matches!(
                job.failure,
                Some(pplx_space_common_jobs::JobFailure::Permanent { .. })
            )
        {
            return None;
        }
        let payload = teardown_backup_payload_of(job, backup.execution_generation)?;
        if payload.failure_scope != crate::snapshot_upload::UploadFailureScope::Artifact {
            return None;
        }
        Some(crate::snapshot_upload_jobs::prepared_begin_refused_as_superseded(&payload).await)
    }
}
