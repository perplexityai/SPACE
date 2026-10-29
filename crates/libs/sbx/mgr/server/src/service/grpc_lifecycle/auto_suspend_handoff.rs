//! Recovery of a newly captured idle suspend. Historical unowned captures are
//! deliberately not candidates for this path.

use super::*;
use crate::snapshot_upload::{HANDOFF_RETRY_POLICY, SnapshotUploadJobs};
use pplx_space_common_grpc::GrpcRetryPolicy;
use pplx_space_common_jobs::JobStatus;

const DURABLE_HANDOFF_POLICY: GrpcRetryPolicy = GrpcRetryPolicy {
    initial_delay: Duration::from_secs(5),
    max_delay: Duration::from_secs(60),
    max_elapsed: Duration::MAX,
    ..HANDOFF_RETRY_POLICY
};

fn capture_fence(error: impl std::error::Error + Send + Sync + 'static) -> Status {
    let mut status = artifact_backup_fence_status(format!("verify immutable capture: {error}"));
    status.set_source(Arc::new(error));
    // Missing capture files are not a missing transport socket. Refuse this
    // admission before any RPC; preserve the typed cause without retrying it.
    pplx_space_common_grpc::refused_before_start(status, Duration::ZERO)
}

impl SandboxManagerService {
    pub(super) fn check_auto_suspend_handoff_owner(
        &self,
        payload: &SnapshotUploadJobs,
    ) -> Result<(), Status> {
        let Some(owner) = &payload.auto_suspend else {
            return Ok(());
        };
        let current = self.lookup_sandbox(&payload.meta.sandbox_id)?;
        if !owner.owns(&current)
            || !due_stage(
                &current.auto_lifecycle,
                &current.auto_lifecycle_state,
                now_secs(),
            )
            .is_some_and(|(_, _, action, due_at)| {
                action == AutoLifecycleIdleAction::Suspend && due_at == owner.due_at
            })
        {
            return Err(artifact_backup_fence_status(
                "auto-suspend handoff owner was superseded".to_owned(),
            ));
        }
        Ok(())
    }

    /// Check the immutable capture at admission/completion while the caller
    /// holds the artifact lock. Transport retries need only re-read ownership.
    pub(super) async fn verify_auto_suspend_handoff_capture(
        &self,
        payload: &SnapshotUploadJobs,
    ) -> Result<(), Status> {
        if payload.auto_suspend.is_none() {
            return Ok(());
        }
        self.check_auto_suspend_handoff_owner(payload)?;
        let bytes = tokio::fs::read(
            payload
                .snapshot_dir
                .join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME),
        )
        .await
        .map_err(capture_fence)?;
        let capture: crate::snapshot_upload::SnapshotMeta =
            serde_json::from_slice(&bytes).map_err(capture_fence)?;
        if capture.local_artifact_id != payload.meta.local_artifact_id
            || capture.sandbox_id != payload.meta.sandbox_id
            || capture.created_at != payload.meta.created_at
            || capture.rootfs_send_source_id != payload.meta.rootfs_send_source_id
        {
            return Err(artifact_backup_fence_status(
                "prepared capture identity changed".to_owned(),
            ));
        }
        self.check_auto_suspend_handoff_owner(payload)
    }

    pub(super) fn auto_suspend_submission_policy(
        &self,
        payload: &SnapshotUploadJobs,
    ) -> GrpcRetryPolicy {
        let Some(owner) = &payload.auto_suspend else {
            return HANDOFF_RETRY_POLICY;
        };
        let remaining = owner
            .foreground_deadline_at
            .saturating_sub(now_secs())
            .min(120);
        if owner.failures == 0 && remaining > 0 {
            GrpcRetryPolicy {
                max_elapsed: Duration::from_secs(remaining),
                ..HANDOFF_RETRY_POLICY
            }
        } else {
            // The durable scheduler supplies backoff, not a fresh foreground budget.
            GrpcRetryPolicy {
                max_retries: 0,
                max_elapsed: Duration::from_secs(5),
                ..HANDOFF_RETRY_POLICY
            }
        }
    }

    /// Called under wake/artifact locks, before allocating another capture.
    pub(in crate::service) async fn resume_auto_suspend_handoff(
        &self,
        sandbox_id: &str,
    ) -> Result<bool, Status> {
        let job = {
            let db = self.db_conn()?;
            crate::db::read::auto_suspend_upload(&db, sandbox_id)
                .map_err(|error| Status::internal(format!("read auto-suspend handoff: {error}")))?
        };
        let Some(job) = job else {
            return Ok(false);
        };
        let payload: SnapshotUploadJobs = job.payload_as().map_err(|error| {
            artifact_backup_fence_status(format!("decode auto-suspend handoff: {error}"))
        })?;
        let current = self.lookup_sandbox(sandbox_id)?;
        if !payload
            .auto_suspend
            .as_ref()
            .is_some_and(|owner| owner.same_idle_decision(&current))
        {
            return Ok(false);
        }
        self.check_auto_suspend_handoff_owner(&payload)?;
        let local_artifact_id = payload.meta.local_artifact_id.ok_or_else(|| {
            artifact_backup_fence_status("auto-suspend capture identity is missing".to_owned())
        })?;
        let snapshot_dir = payload
            .snapshot_dir
            .to_str()
            .ok_or_else(|| {
                artifact_backup_fence_status("auto-suspend capture path is invalid".to_owned())
            })?
            .to_owned();
        let CleanupLockTryResult::Acquired(cleanup_fence) = self.try_cleanup_fence(sandbox_id)
        else {
            return Err(node_local_backup_status(
                ArtifactBackupSubmissionError::Admission(
                    crate::snapshot_upload::SnapshotSubmissionError::Admission {
                        artifact: pplx_space_common_core::types::SnapshotArtifact::Metadata,
                        source: Status::unavailable("cleanup admission is busy"),
                    },
                ),
            ));
        };
        self.check_auto_suspend_handoff_owner(&payload)?;
        if !matches!(
            job.status.parse::<JobStatus>(),
            Ok(JobStatus::Pending | JobStatus::Running | JobStatus::Completed)
        ) {
            return Err(artifact_backup_fence_status(format!(
                "auto-suspend upload {} is {}",
                job.job_id, job.status
            )));
        }
        {
            let db = self.db_conn()?;
            if !crate::db::begin_suspend(&db, sandbox_id).map_err(|error| {
                Status::internal(format!("resume auto-suspend admission: {error}"))
            })? {
                return Err(artifact_backup_fence_status(
                    "auto-suspend admission lost ownership".to_owned(),
                ));
            }
        }
        let submitted = self
            .establish_restore_visible_artifact_backup(&job.job_id)
            .await;
        let payload = match submitted {
            Ok(payload) => payload,
            Err(error) => {
                self.record_suspend_snapshot_publication_failure_after_begin(
                    sandbox_id,
                    "backup handoff deferred",
                );
                return Err(node_local_backup_status(error));
            }
        };
        if let Err(error) = self.check_auto_suspend_handoff_owner(&payload) {
            self.record_suspend_snapshot_publication_failure_after_begin(
                sandbox_id,
                "backup handoff superseded",
            );
            return Err(error);
        }
        let current = self.lookup_sandbox(sandbox_id)?;
        self.finalize_suspended_runtime(
            &current,
            local_artifact_id,
            &snapshot_dir,
            None,
            Some(cleanup_fence),
        )
        .await?;
        mark_sandbox_dirty(&self.db, sandbox_id);
        Ok(true)
    }

    /// Retains the original idle epoch and only moves its scheduling deadline.
    pub(in crate::service) fn defer_auto_suspend_handoff(
        &self,
        sandbox: &crate::db::SandboxRow,
        error: &Status,
    ) -> Result<(), Status> {
        let db = self.db_conn()?;
        let current = crate::db::read::sandbox(&db, &sandbox.id)
            .map_err(|cause| Status::internal(format!("read handoff owner: {cause}")))?;
        let Some(sandbox) = current.as_ref() else {
            return Ok(());
        };
        let job = crate::db::read::auto_suspend_upload(&db, &sandbox.id)
            .map_err(|cause| Status::internal(format!("read deferred handoff: {cause}")))?
            .ok_or_else(|| {
                artifact_backup_fence_status("transient handoff has no durable job".to_owned())
            })?;
        let payload: SnapshotUploadJobs = job.payload_as().map_err(|cause| {
            artifact_backup_fence_status(format!("decode deferred handoff: {cause}"))
        })?;
        let Some(owner) = payload
            .auto_suspend
            .as_ref()
            .filter(|owner| owner.owns(sandbox))
        else {
            return Ok(());
        };
        let failures = owner.failures.saturating_add(1);
        let delay = DURABLE_HANDOFF_POLICY
            .backoff_delay(failures.saturating_sub(1).min(16) as usize)
            .unwrap_or(Duration::from_secs(60));
        let delay =
            pplx_space_common_grpc::retry_after(error).map_or(delay, |hint| delay.max(hint));
        let now = now_secs();
        let mut state = sandbox.auto_lifecycle_state.clone();
        state.last_auto_error = Some(error.to_string());
        state.auto_error_streak = failures;
        state.next_auto_event_at = Some(now.saturating_add(delay.as_secs().max(1)));
        if !crate::db::snapshot_upload_jobs::schedule_auto_suspend_retry(
            &db,
            &sandbox.id,
            &job.job_id,
            failures,
            &state,
        )
        .map_err(|cause| Status::internal(format!("persist handoff retry: {cause}")))?
        {
            return Err(artifact_backup_fence_status(
                "handoff job disappeared before retry".to_owned(),
            ));
        }
        if now.saturating_sub(owner.due_at) >= 300 {
            tracing::error!(sandbox_id = %sandbox.id, job_id = %job.job_id, due_at = owner.due_at,
                retry_at = ?state.next_auto_event_at, failures, %error, "auto-suspend backup handoff remains overdue");
        }
        Ok(())
    }

    pub(super) fn invalidate_auto_suspend_handoff(&self, sandbox_id: &str) -> Result<(), Status> {
        let db = self.db_conn()?;
        let Some(row) = crate::db::read::sandbox(&db, sandbox_id)
            .map_err(|error| Status::internal(format!("read lifecycle epoch: {error}")))?
        else {
            return Ok(());
        };
        let mut state = row.auto_lifecycle_state;
        state.handoff_epoch = state.handoff_epoch.wrapping_add(1);
        crate::db::update_auto_lifecycle_state(&db, sandbox_id, &state)
            .map_err(|error| Status::internal(format!("invalidate suspend handoff: {error}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
