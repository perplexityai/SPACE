//! Suspend snapshot capture and publication after the suspend locks are held:
//! staging, encoding, monitoring re-arm, failure recording, and finalizing the
//! suspended runtime.

use super::*;

pub(in crate::service) fn encrypted_snapshot_prepare_error_status(
    error: crate::snapshot_encryption::EncryptedSnapshotPrepareError,
) -> Status {
    let reason = format!("prepare encrypted snapshot volume failed: {error}");
    error.into_status(reason)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::service) enum SuspendSnapshotPublicationFailurePersistence {
    RolledBackToPaused,
    Quarantined,
    StateChanged,
    PersistFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::service) enum SuspendSnapshotStagingDisposition {
    Preserve,
    Cleanup,
}

#[derive(Debug, thiserror::Error)]
pub(in crate::service) enum EncryptedCaptureSealError {
    #[error("capture record does not belong to the admitted attempt")]
    RecordIdentityMismatch,
    #[error("capture owner unavailable: {0}")]
    OwnerUnavailable(#[source] Status),
    #[error("read capture owner: {0}")]
    OwnerRead(#[source] crate::db::Error),
    #[error(transparent)]
    Record(#[from] crate::service::suspend_snapshot_publication::SuspendSnapshotPublicationError),
    #[error("{0}")]
    Release(String),
    #[error("local artifact metadata creation failed: {0}")]
    Metadata(#[from] crate::snapshot_upload::SnapshotMetadataWriteError),
    #[error("remove encrypted capture record failed: {0}")]
    RemoveRecord(#[source] std::io::Error),
}

/// Whether a failed btrfs-send encode demotes this suspend to the Full codec.
/// Node-manager answers `FailedPrecondition` only when it refused before
/// touching Firecracker's `rootfs.ext4`; an explicit `format=BtrfsSend` keeps
/// that refusal visible to its caller.
pub(in crate::service) fn btrfs_send_encode_falls_back_to_full(
    status: &Status,
    requested_format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && requested_format != pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend
}

/// Re-arm watchdog monitoring after a pre-submission failure leaves a live
/// paused runtime. Encrypted capture disarms this guard before submission:
/// its producer may still stop the VM, so only refusal recovery may re-arm it.
/// Reload endpoint authority on re-arm instead of restoring a stale PID.
/// The snapshot scheduler stays off for paused sandboxes and is restored by
/// the normal resume/start path.
pub(super) struct SuspendMonitoringGuard {
    armed: bool,
    watchdog: Arc<crate::watchdog::SandboxWatchdog>,
    runtime_live: Arc<dyn Fn(&str) -> bool + Send + Sync>,
    sandbox_id: String,
}

impl SuspendMonitoringGuard {
    fn new(service: &SandboxManagerService, sandbox: &crate::db::SandboxRow) -> Self {
        Self {
            armed: true,
            watchdog: Arc::clone(&service.watchdog),
            runtime_live: Arc::clone(&service.sandbox_runtime_live),
            sandbox_id: sandbox.id.clone(),
        }
    }

    /// The suspend reached its intentional teardown (or otherwise owns the
    /// monitoring lifecycle from here); do not re-arm on drop.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SuspendMonitoringGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Drop can't await; re-add on the runtime if one is still available
        // (it is during task cancellation and normal returns; absent only
        // during runtime shutdown, where re-arming is moot).
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let watchdog = Arc::clone(&self.watchdog);
            let runtime_live = Arc::clone(&self.runtime_live);
            let sandbox_id = self.sandbox_id.clone();
            handle.spawn(async move {
                if runtime_live(&sandbox_id) {
                    watchdog.rearm_sandbox_from_db(&sandbox_id).await;
                }
            });
        }
    }
}

#[cfg(test)]
impl SandboxManagerService {
    /// Build and immediately drop an armed [`SuspendMonitoringGuard`], the way
    /// an abandoned suspend would, so tests can assert the drop-time re-arm.
    pub(in crate::service) fn drop_armed_suspend_monitoring_guard_for_test(
        &self,
        sandbox: &crate::db::SandboxRow,
    ) {
        drop(SuspendMonitoringGuard::new(self, sandbox));
    }

    /// Build a guard, disarm it (as the intentional-teardown path does), then
    /// drop it — must not re-arm.
    pub(in crate::service) fn drop_disarmed_suspend_monitoring_guard_for_test(
        &self,
        sandbox: &crate::db::SandboxRow,
    ) {
        let mut guard = SuspendMonitoringGuard::new(self, sandbox);
        guard.disarm();
        drop(guard);
    }
}

/// What a suspend publication failure knows about this suspend's own capture
/// when it quarantines the row. It decides only the failure reason the
/// quarantine records, never whether or when the row quarantines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::service) enum CaptureEvidence {
    /// Classification just found no usable capture of this suspend.
    ClassifiedNone,
    /// The failure keeps no capture of this suspend: it came before the
    /// capture returned, or while the capture's staging was still set to be
    /// discarded.
    NoneKept,
    /// The capture may be kept and nothing classified it since: it may be a
    /// usable copy on disk.
    Unclassified,
}

impl SandboxManagerService {
    pub(in crate::service) fn record_suspend_snapshot_publication_failure_after_begin(
        &self,
        sandbox_id: &str,
        reason: &str,
    ) -> SuspendSnapshotPublicationFailurePersistence {
        let runtime_live = (self.sandbox_runtime_live)(sandbox_id);
        self.record_suspend_snapshot_publication_failure_after_begin_with_liveness(
            sandbox_id,
            reason,
            runtime_live,
            CaptureEvidence::Unclassified,
        )
    }

    /// Record a failure that keeps no capture of this suspend: it came before
    /// the capture returned, or while the capture's staging guard is armed,
    /// so the staging is discarded on the way out.
    pub(in crate::service) fn record_suspend_snapshot_publication_failure_without_capture(
        &self,
        sandbox_id: &str,
        reason: &str,
    ) -> SuspendSnapshotPublicationFailurePersistence {
        let runtime_live = (self.sandbox_runtime_live)(sandbox_id);
        self.record_suspend_snapshot_publication_failure_after_begin_with_liveness(
            sandbox_id,
            reason,
            runtime_live,
            CaptureEvidence::NoneKept,
        )
    }

    fn record_suspend_snapshot_publication_failure_after_begin_with_liveness(
        &self,
        sandbox_id: &str,
        reason: &str,
        runtime_live: bool,
        capture: CaptureEvidence,
    ) -> SuspendSnapshotPublicationFailurePersistence {
        if runtime_live {
            let rolled_back = {
                let Ok(db) = self.db_conn() else {
                    return SuspendSnapshotPublicationFailurePersistence::PersistFailed;
                };
                crate::db::rollback_suspending_to_paused(&db, sandbox_id)
            };
            match rolled_back {
                Ok(true) => {
                    tracing::warn!(
                        sandbox_id,
                        reason,
                        "rolled interrupted suspend publication back to paused"
                    );
                    mark_sandbox_dirty(&self.db, sandbox_id);
                    return SuspendSnapshotPublicationFailurePersistence::RolledBackToPaused;
                }
                Ok(false) => {
                    tracing::warn!(
                        sandbox_id,
                        reason,
                        "suspend publication failure raced with another state transition"
                    );
                    return SuspendSnapshotPublicationFailurePersistence::StateChanged;
                }
                Err(error) => {
                    tracing::warn!(
                        sandbox_id,
                        reason,
                        %error,
                        "failed to roll interrupted suspend publication back to paused"
                    );
                }
            }
        }

        // When the runtime is gone and no capture of this suspend is kept,
        // the retained instance may hold the sandbox's only copy. That's what
        // the failure reason then records: the shared retention policy reads
        // it to refuse an ordinary delete and to admit an operator's discard,
        // and the quarantine keeps the cause as its message. Any other failure
        // records the cause, as before: its capture may be a usable copy, or,
        // when a live runtime's rollback failed, the runtime still holds the
        // sandbox.
        let quarantined = {
            let Ok(db) = self.db_conn() else {
                return SuspendSnapshotPublicationFailurePersistence::PersistFailed;
            };
            match capture {
                CaptureEvidence::ClassifiedNone | CaptureEvidence::NoneKept if !runtime_live => {
                    crate::db::mark_suspending_quarantined_retaining_only_copy(
                        &db, sandbox_id, reason,
                    )
                }
                CaptureEvidence::ClassifiedNone
                | CaptureEvidence::NoneKept
                | CaptureEvidence::Unclassified => {
                    crate::db::mark_suspending_quarantined(&db, sandbox_id, reason)
                }
            }
        };
        match quarantined {
            Ok(true) => {
                mark_sandbox_dirty(&self.db, sandbox_id);
                SuspendSnapshotPublicationFailurePersistence::Quarantined
            }
            Ok(false) => {
                tracing::warn!(
                    sandbox_id,
                    reason,
                    "suspend publication failure did not quarantine because state changed"
                );
                SuspendSnapshotPublicationFailurePersistence::StateChanged
            }
            Err(error) => {
                tracing::warn!(
                    sandbox_id,
                    reason,
                    %error,
                    "failed to persist suspend publication failure"
                );
                SuspendSnapshotPublicationFailurePersistence::PersistFailed
            }
        }
    }

    /// Seal an encrypted capture's volume into its staging directory, write
    /// its sidecar from `metadata` and drop its record. This is the only
    /// encrypted-specific publication step: the live suspend and recovery
    /// both run it, and the sealed staging directory then publishes like a
    /// plaintext one.
    pub(in crate::service) async fn seal_encrypted_capture(
        &self,
        sandbox: &crate::db::SandboxRow,
        staging_dir: &std::path::Path,
        metadata: &crate::snapshot_upload::SnapshotMeta,
    ) -> Result<(), EncryptedCaptureSealError> {
        // node-manager seals and stages both ciphertext artifacts in one
        // privileged step: only it can read the sandbox-private instance dir.
        crate::snapshot_encryption::release_encrypted_snapshot_volume(
            &sandbox.id,
            sandbox.node_execution_fence(),
            &staging_dir.to_string_lossy(),
            &self.node_manager,
        )
        .await
        .map_err(EncryptedCaptureSealError::Release)?;
        crate::snapshot_upload::write_snapshot_metadata(
            metadata,
            staging_dir,
            &crate::snapshot_upload::encrypted_vm_artifacts(),
            None,
        )?;
        remove_encrypted_capture_record(staging_dir)
            .map_err(EncryptedCaptureSealError::RemoveRecord)
    }

    /// Seal only the admitted capture, within its fixed retry window and with
    /// runtime evidence consistent with the producer's confirmed stop. Both
    /// periodic and teardown callers hold the row's lifecycle/cleanup guards.
    pub(in crate::service) async fn seal_recorded_encrypted_capture(
        &self,
        sandbox: &crate::db::SandboxRow,
        cache_dir: &std::path::Path,
    ) -> Result<Option<LocalArtifactId>, EncryptedCaptureSealError> {
        if !sandbox.volume_encryption {
            return Ok(None);
        }
        let attempt = {
            let db = self
                .db_conn()
                .map_err(EncryptedCaptureSealError::OwnerUnavailable)?;
            crate::db::read::suspend_attempt(&db, &sandbox.id)
                .map_err(EncryptedCaptureSealError::OwnerRead)?
        };
        let Some(attempt) = attempt else {
            return Ok(None);
        };
        if !crate::service::suspend_snapshot_publication::suspend_attempt_retry_open(attempt) {
            return Ok(None);
        }
        let staging_dir =
            suspend_snapshot_staging_dir(&cache_dir.join(&sandbox.id), attempt.artifact_id);
        if crate::service::suspend_snapshot_publication::read_capture_refusal(&staging_dir)?
            .is_some()
            || crate::service::suspend_snapshot_publication::read_capture_failure(&staging_dir)?
                .is_some()
        {
            // Classification must resolve failed or contradictory refusal/completion
            // evidence before any seal can remove the completion receipt.
            return Ok(None);
        }
        let Some(metadata) = read_encrypted_capture_record(&staging_dir)? else {
            return Ok(None);
        };
        // Both periodic and teardown recovery enter here. A receipt cannot
        // authorize sealing while a runtime still holds the memory mapping.
        // Let classification bound contradictory evidence by admission time.
        let runtime = self.capture_runtime_observation(&sandbox.id);
        if runtime != crate::service::suspend_snapshot_publication::RuntimeObservation::Gone {
            if runtime == crate::service::suspend_snapshot_publication::RuntimeObservation::Live {
                crate::service::suspend_snapshot_publication::report_live_capture_receipt(
                    &sandbox.id,
                );
            }
            return Ok(None);
        }
        if metadata.local_artifact_id != Some(attempt.artifact_id)
            || metadata.sandbox_id != sandbox.id
        {
            return Err(EncryptedCaptureSealError::RecordIdentityMismatch);
        }
        self.seal_encrypted_capture(sandbox, &staging_dir, &metadata)
            .await?;
        Ok(Some(attempt.artifact_id))
    }

    pub(in crate::service) fn record_suspend_snapshot_publication_failure_after_staging(
        &self,
        sandbox_id: &str,
        sandbox_dir: &std::path::Path,
        current_staging_dir: &std::path::Path,
        reason: &str,
        verdicts: &crate::retained_source::RetainedSourceVerdicts,
    ) -> SuspendSnapshotStagingDisposition {
        let runtime_live = (self.sandbox_runtime_live)(sandbox_id);
        let row = {
            let Ok(db) = self.db_conn() else {
                return SuspendSnapshotStagingDisposition::Preserve;
            };
            match crate::db::read::sandbox(&db, sandbox_id) {
                Ok(Some(row)) if row.state.is_unowned(SandboxState::Suspending) => row,
                Ok(Some(row)) => {
                    tracing::warn!(
                        sandbox_id,
                        reason,
                        state = %row.state,
                        "suspend publication failure raced with another state transition"
                    );
                    return SuspendSnapshotStagingDisposition::Cleanup;
                }
                Ok(None) => {
                    tracing::warn!(
                        sandbox_id,
                        reason,
                        "suspend publication failure raced with sandbox deletion"
                    );
                    return SuspendSnapshotStagingDisposition::Cleanup;
                }
                Err(error) => {
                    tracing::warn!(
                        sandbox_id,
                        reason,
                        %error,
                        "failed to load sandbox after suspend publication failure"
                    );
                    if current_staging_dir.exists() {
                        return SuspendSnapshotStagingDisposition::Preserve;
                    }
                    return SuspendSnapshotStagingDisposition::Cleanup;
                }
            }
        };

        let attempt = match self.db_conn().and_then(|db| {
            crate::db::read::suspend_attempt(&db, sandbox_id)
                .map_err(|e| Status::internal(e.to_string()))
        }) {
            Ok(attempt) => attempt,
            Err(_) => return SuspendSnapshotStagingDisposition::Preserve,
        };
        let decision = match crate::service::suspend_snapshot_publication::classify_suspend_attempt(
            sandbox_dir,
            row.current_local_artifact_id,
            attempt,
            row.volume_encryption,
            if row.volume_encryption {
                self.capture_runtime_observation(sandbox_id)
            } else {
                runtime_live.into()
            },
            verdicts,
        ) {
            Ok(decision) => decision,
            // Evidence that can still complete is not a failed publication.
            // The row stays suspending, so periodic recovery or the teardown
            // driver retries it as it does any interrupted publication.
            Err(error) if error.is_pending() => {
                tracing::warn!(
                    sandbox_id,
                    reason,
                    %error,
                    "suspend publication is waiting on its artifact; recovery will retry"
                );
                return SuspendSnapshotStagingDisposition::Preserve;
            }
            Err(error) => {
                tracing::warn!(sandbox_id, reason, %error, "suspend publication evidence unavailable; recovery will retry");
                return SuspendSnapshotStagingDisposition::Preserve;
            }
        };

        match decision {
            SuspendSnapshotPublicationDecision::RuntimeLiveWithoutUsableArtifact => {
                self.record_suspend_snapshot_publication_failure_after_begin_with_liveness(
                    sandbox_id,
                    reason,
                    true,
                    CaptureEvidence::Unclassified,
                );
                SuspendSnapshotStagingDisposition::Cleanup
            }
            decision @ (SuspendSnapshotPublicationDecision::StagedComplete(_)
            | SuspendSnapshotPublicationDecision::CanonicalReady(_)) => {
                let preserve_current_staging = matches!(
                    &decision,
                    SuspendSnapshotPublicationDecision::StagedComplete(staged)
                        if staged.staging_dir == current_staging_dir
                );
                let teardown_generation = {
                    let Ok(db) = self.db_conn() else {
                        return SuspendSnapshotStagingDisposition::Preserve;
                    };
                    match crate::db::read::sandbox_view(&db, sandbox_id)
                        .teardown()
                        .status()
                    {
                        Ok(Some(status)) => status
                            .operation
                            .filter(|operation| operation.status.is_active())
                            .map(|operation| operation.execution_generation),
                        Ok(_) => None,
                        Err(error) => {
                            tracing::warn!(
                                sandbox_id,
                                %error,
                                "could not prove suspend durability mode after publish failure"
                            );
                            return SuspendSnapshotStagingDisposition::Preserve;
                        }
                    }
                };
                match self.apply_suspend_snapshot_publication_recovery_decision(
                    row,
                    sandbox_dir.to_path_buf(),
                    decision,
                    SuspendSnapshotPublicationRecoveryOrigin::LivePublishFailure,
                    teardown_generation,
                    verdicts,
                ) {
                    Ok(record) => {
                        if record.outcome.changed() {
                            mark_sandbox_dirty(&self.db, sandbox_id);
                        }
                        match record.outcome {
                            crate::db::StartupRecoveryOutcome::CleanupRequired { .. } => {
                                SuspendSnapshotStagingDisposition::Cleanup
                            }
                            crate::db::StartupRecoveryOutcome::Deferred => {
                                // The complete artifact still has a retry owner:
                                // periodic publication recovery scans suspending
                                // rows, not operator-attention quarantines.
                                if preserve_current_staging {
                                    SuspendSnapshotStagingDisposition::Preserve
                                } else {
                                    SuspendSnapshotStagingDisposition::Cleanup
                                }
                            }
                            crate::db::StartupRecoveryOutcome::State(_) => {
                                SuspendSnapshotStagingDisposition::Cleanup
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(sandbox_id, reason, %error, "complete suspend artifact awaits persistence; recovery will retry");
                        SuspendSnapshotStagingDisposition::Preserve
                    }
                }
            }
            SuspendSnapshotPublicationDecision::RuntimeGoneWithoutUsableArtifact => {
                self.record_suspend_snapshot_publication_failure_after_begin_with_liveness(
                    sandbox_id,
                    reason,
                    false,
                    CaptureEvidence::ClassifiedNone,
                );
                SuspendSnapshotStagingDisposition::Cleanup
            }
            decision @ (SuspendSnapshotPublicationDecision::BlockedAttempt(_)
            | SuspendSnapshotPublicationDecision::CaptureRefused) => {
                let refused =
                    matches!(decision, SuspendSnapshotPublicationDecision::CaptureRefused);
                if let Err(error) = self.apply_suspend_snapshot_publication_recovery_decision(
                    row,
                    sandbox_dir.to_path_buf(),
                    decision,
                    SuspendSnapshotPublicationRecoveryOrigin::LivePublishFailure,
                    None,
                    verdicts,
                ) {
                    tracing::warn!(sandbox_id, %error, "cannot persist capture recovery; retaining evidence");
                    return SuspendSnapshotStagingDisposition::Preserve;
                }
                if refused {
                    SuspendSnapshotStagingDisposition::Cleanup
                } else {
                    SuspendSnapshotStagingDisposition::Preserve
                }
            }
        }
    }

    pub(in crate::service) async fn complete_suspend_snapshot_publication(
        &self,
        publication: SuspendSnapshotPublicationContext,
    ) -> Result<(), Status> {
        let SuspendSnapshotPublicationContext {
            cleanup_fence,
            sandbox,
            requested_format,
            suspend_format: mut published_format,
            mut create_codec,
            local_artifact_id,
            event_details: suspend_event_details,
            started_at: create_started_at,
            publish_node_state,
            durability,
        } = publication;
        let mut cleanup_fence = Some(cleanup_fence);
        // Backstop: if this publication is abandoned before its intentional
        // teardown while the runtime is still live — a failure that rolls back
        // to paused, or a dropped handler future mid-capture — re-arm the
        // watchdog on drop so the paused-but-live sandbox's sidecars keep being
        // monitored. Disarmed just before the deliberate monitoring removal on
        // the success path. Harmless no-op on paths that never unsubscribed.
        let mut monitoring_guard = SuspendMonitoringGuard::new(self, &sandbox);
        let create_kind = "suspend";
        let mut node_client = connect_node(&self.node_manager).await.map_err(|e| {
            crate::metrics::snapshot::create_failed(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                "sync_guest",
                "node_mgr_unreachable",
                create_started_at,
            );
            let error = format!("cannot connect to node-manager: {e}");
            self.record_suspend_snapshot_publication_failure_without_capture(&sandbox.id, &error);
            emit_suspend_failed_event(
                &sandbox.id,
                &suspend_event_details,
                "sync_guest",
                "node_mgr_unreachable",
                error.clone(),
            );
            Status::internal(error)
        })?;

        let sync_started_at = std::time::Instant::now();
        if let Err(e) = node_client
            .sync_guest(SyncGuestRequest {
                sandbox_id: sandbox.id.clone(),
            })
            .await
        {
            let reason = format!("host rootfs sync failed: {e}");
            self.record_suspend_snapshot_publication_failure_without_capture(&sandbox.id, &reason);
            crate::metrics::snapshot::create_failed(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                "sync_guest",
                "host_rootfs_sync_failed",
                create_started_at,
            );
            emit_suspend_failed_event(
                &sandbox.id,
                &suspend_event_details,
                "sync_guest",
                "host_rootfs_sync_failed",
                reason.clone(),
            );
            return Err(Status::internal(reason));
        }
        crate::metrics::snapshot::create_step(
            create_kind,
            create_codec,
            sandbox.volume_encryption,
            "sync_guest",
            sync_started_at,
        );

        let sandbox_dir = std::path::PathBuf::from(format!("{CACHE_DIR}/{}", sandbox.id));
        let snapshot_dir = canonical_suspend_snapshot_dir(&sandbox_dir);
        let staging_parent = suspend_snapshot_staging_parent_dir(&sandbox_dir);
        let staging_dir = suspend_snapshot_staging_dir(&sandbox_dir, local_artifact_id);
        let record_artifact_stage_failure = |reason: &str| {
            crate::metrics::snapshot::create_failed(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                "artifact_stage",
                "artifact_stage_failed",
                create_started_at,
            );
            emit_suspend_failed_event(
                &sandbox.id,
                &suspend_event_details,
                "artifact_stage",
                "artifact_stage_failed",
                reason.to_owned(),
            );
        };
        if let Err(e) = node_client
            .prepare_sandbox_artifact_dir(PrepareSandboxArtifactDirRequest {
                sandbox_id: sandbox.id.clone(),
            })
            .await
        {
            let reason = format!("prepare local artifact directory failed: {e}");
            self.record_suspend_snapshot_publication_failure_without_capture(&sandbox.id, &reason);
            record_artifact_stage_failure(&reason);
            return Err(Status::internal(reason));
        }
        std::fs::create_dir_all(&staging_parent).map_err(|err| {
            let reason = format!(
                "create local artifact staging parent {} failed: {err}",
                staging_parent.display()
            );
            self.record_suspend_snapshot_publication_failure_without_capture(&sandbox.id, &reason);
            record_artifact_stage_failure(&reason);
            Status::internal(reason)
        })?;
        std::fs::create_dir(&staging_dir).map_err(|err| {
            let reason = format!(
                "create local artifact staging directory {} failed: {err}",
                staging_dir.display()
            );
            self.record_suspend_snapshot_publication_failure_without_capture(&sandbox.id, &reason);
            record_artifact_stage_failure(&reason);
            Status::internal(reason)
        })?;
        let mut staging_guard = LocalArtifactStagingGuard::new(staging_dir.clone());
        let result = async {
            let staging_dir_string = staging_dir.to_string_lossy().into_owned();

            // BYOK: bring up the encrypted snapshot volume so the jailed Firecracker
            // writes vmstate+memory straight into it (compressed+encrypted at t0).
            // Keyed off the sandbox's immutable volume_encryption flag; `None` for
            // plaintext sandboxes — the plaintext producer below is then
            // byte-identical to before. A failure here is fail-closed.
            // An encrypted capture's sidecar metadata depends only on the row.
            // Build it and write the capture's record now, while a failure can
            // still roll back to paused: once the capture stops the runtime, a
            // failed seal is recoverable only through that record.
            let encrypted_capture_metadata = if sandbox.volume_encryption {
                let metadata = match suspend_artifact_metadata(
                    &sandbox,
                    published_format,
                    local_artifact_id,
                    true,
                    None,
                ) {
                    Ok(metadata) => metadata,
                    Err(status) => {
                        let reason =
                            format!("local artifact metadata construction failed: {status}");
                        self.record_suspend_snapshot_publication_failure_without_capture(
                            &sandbox.id,
                            &reason,
                        );
                        return Err(status);
                    }
                };
                if let Err(error) = prepare_encrypted_capture_record(&staging_dir, &metadata) {
                    let reason = format!("record encrypted capture failed: {error}");
                    self.record_suspend_snapshot_publication_failure_without_capture(
                        &sandbox.id,
                        &reason,
                    );
                    record_artifact_stage_failure(&reason);
                    return Err(Status::internal(reason));
                }
                Some(metadata)
            } else {
                None
            };
            let enc_snapshot = if sandbox.volume_encryption {
                // A restored VM's encrypted memory file is the base into which
                // Firecracker 1.15 writes the next Diff snapshot. Never format
                // an empty replacement when that base is expected: a missing
                // mount/backing must fail this suspend instead of publishing a
                // sparse diff as though it were standalone.
                let fresh = crate::snapshot_encryption::capture_volume_is_fresh(&sandbox);
                match crate::snapshot_encryption::prepare_encrypted_snapshot_volume(
                    &sandbox.id,
                    sandbox.node_execution_fence(),
                    fresh,
                    None,
                    &self.node_manager,
                )
                .await
                {
                    Ok(value) => Some(value),
                    Err(error) => {
                        let status = encrypted_snapshot_prepare_error_status(error);
                        let reason = status.message().to_string();
                        self.record_suspend_snapshot_publication_failure_without_capture(
                            &sandbox.id,
                            &reason,
                        );
                        record_artifact_stage_failure(&reason);
                        return Err(status);
                    }
                }
            } else {
                None
            };
            let encrypted = enc_snapshot.is_some();
            let enc_snapshot_mount_dir = enc_snapshot.as_ref().map(|e| e.mount_dir.clone());

            // The encrypted capture stops the runtime inside create_snapshot to
            // release Firecracker's mmap pin on the volume before it is sealed.
            // Stop watching first so the watchdog does not race that teardown with
            // sidecar-restart attempts. Idempotent: the removes near the end of
            // suspend are a no-op once already removed. Plaintext keeps its
            // existing ordering (removed only there). After submission, recovery
            // re-arms only when producer refusal has settled the row to paused.
            if encrypted {
                self.watchdog.remove_sandbox(&sandbox.id).await;
                self.snapshot_scheduler.remove_sandbox(&sandbox.id).await;
            }

            let fc_create_started_at = std::time::Instant::now();
            self.ensure_teardown_suspend_owner_for_durability(&sandbox.id, durability)?;
            let request = crate::service::snapshot_capture::bind(
                CaptureVmStateRequest {
                    execution_fence: sandbox.node_execution_fence(),
                    sandbox_id: sandbox.id.clone(),
                    artifacts: crate::service::snapshot_capture::artifacts(),
                    storage: Some(pplx_space_node_mgr_client::proto::VmStateStorage {
                        directory: Some(staging_dir_string.clone()),
                        encrypted_volume_mount: enc_snapshot_mount_dir,
                    }),
                    environment_reference: None,
                },
                sandbox.execution_metadata.as_ref(),
            );
            if encrypted {
                // All pre-submission checks passed. From this point only a
                // settled refusal may re-arm monitoring through recovery.
                monitoring_guard.disarm();
            }
            if let Err(e) = node_client.capture_vm_state(request).await {
                let reason = format!("snapshot failed: {e}");
                if enc_snapshot.is_some() {
                    // Only producer-written refusal proves the Diff base was
                    // untouched. All status codes alone remain ambiguous.
                    if self.record_suspend_snapshot_publication_failure_after_staging(
                        &sandbox.id,
                        &sandbox_dir,
                        &staging_dir,
                        &reason,
                        &crate::retained_source::RetainedSourceVerdicts::default(),
                    ) == SuspendSnapshotStagingDisposition::Preserve
                    {
                        staging_guard.disarm();
                    }
                } else {
                    // The plaintext capture's staging guard is armed, so a
                    // failed capture keeps nothing of this suspend.
                    self.record_suspend_snapshot_publication_failure_after_begin_with_liveness(
                        &sandbox.id,
                        &reason,
                        (self.sandbox_runtime_live)(&sandbox.id),
                        CaptureEvidence::NoneKept,
                    );
                }
                crate::metrics::snapshot::create_failed(
                    create_kind,
                    create_codec,
                    sandbox.volume_encryption,
                    "fc_create",
                    "fc_create_failed",
                    create_started_at,
                );
                emit_suspend_failed_event(
                    &sandbox.id,
                    &suspend_event_details,
                    "fc_create",
                    "fc_create_failed",
                    reason.clone(),
                );
                return Err(Status::internal(reason));
            }
            crate::metrics::snapshot::create_step(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                "fc_create",
                fc_create_started_at,
            );

            // BYOK: Firecracker wrote vmstate+memory into the encrypted snapshot
            // volume. Seal it (unmount+close) and stage the two encrypted artifacts
            // — `snapshot.luks` (vmstate+memory) and the rootfs delta `cow.luks` —
            // into the publication dir. Both are already-encrypted LUKS2 containers,
            // carried to durable storage as opaque blobs; reflink keeps the copy
            // instant and sparse. The plaintext rootfs codec branch below is skipped
            // for encrypted snapshots (there is no plaintext `rootfs.ext4` to diff).
            if let Some(metadata) = &encrypted_capture_metadata
                && let Err(error) = self
                    .seal_encrypted_capture(&sandbox, &staging_dir, metadata)
                    .await
            {
                let reason = format!("seal/stage encrypted snapshot volume failed: {error}");
                if self.record_suspend_snapshot_publication_failure_after_staging(
                    &sandbox.id,
                    &sandbox_dir,
                    &staging_dir,
                    &reason,
                    &crate::retained_source::RetainedSourceVerdicts::default(),
                ) == SuspendSnapshotStagingDisposition::Preserve
                {
                    staging_guard.disarm();
                }
                if matches!(error, EncryptedCaptureSealError::Metadata(_)) {
                    crate::metrics::snapshot::create_failed(
                        create_kind,
                        create_codec,
                        sandbox.volume_encryption,
                        "publish",
                        "metadata_failed",
                        create_started_at,
                    );
                    emit_suspend_failed_event(
                        &sandbox.id,
                        &suspend_event_details,
                        "publish",
                        "metadata_failed",
                        reason.clone(),
                    );
                } else {
                    record_artifact_stage_failure(&reason);
                }
                return Err(Status::internal(reason));
            }

            // Codec branch: when the resolved format is `BtrfsSend`,
            // replace the just-written `rootfs.ext4` with a `btrfs send
            // -p <template> <instance.ro>` stream. Two failure shapes:
            //
            // - `FailedPrecondition` from the encoder means the sandbox
            //   is btrfs-send-*ineligible* (instance not a subvolume,
            //   template build absent or plain-dir, parent_uuid lineage
            //   broken, received_uuid mismatch). The encoder bails before any
            //   side-effect; FC's `rootfs.ext4` from `create_snapshot` is
            //   still intact on disk. If the caller didn't *explicitly*
            //   request btrfs-send (i.e. it was the env-var/template
            //   default that picked it), silently fall back to Full and
            //   continue — the user has no API surface today to opt out
            //   so we mustn't surface this as a hard error. Explicit
            //   `format=BtrfsSend` on the wire keeps the loud-fail
            //   behavior so callers who *care* still see the precondition
            //   miss.
            // - Any other code (`Internal`, `Unavailable`, …) is a real
            //   encode failure (subvolume snapshot failed, btrfs-send
            //   exited non-zero, etc.). Always loud-fail; no auto-Full
            //   fallback here because the staged snapshot shape may be
            //   partial. If the runtime is still live, the failure handler
            //   rolls the DB row back to `paused` so a later suspend can
            //   start with a fresh staging directory.
            let mut captured_rootfs = None;
            if !encrypted
                && published_format
                    == pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend
            {
                let btrfs_step_started_at = std::time::Instant::now();
                let encode_result = self
                    .encode_rootfs_as_btrfs_send(&sandbox, &staging_dir_string)
                    .await;
                match encode_result {
                    Ok(captured) => {
                        captured_rootfs = Some(captured);
                        crate::metrics::snapshot::create_step(
                            create_kind,
                            create_codec,
                            sandbox.volume_encryption,
                            "btrfs_send",
                            btrfs_step_started_at,
                        );
                    }
                    Err(status)
                        if btrfs_send_encode_falls_back_to_full(&status, requested_format) =>
                    {
                        // Reactive fallback: the caller didn't explicitly
                        // pick btrfs-send (env-var default did), the
                        // sandbox is ineligible, and `rootfs.ext4` from
                        // FC is still intact. Demote to Full for the rest
                        // of this RPC so the upload picks the Full
                        // artifact set and the sidecar carries no
                        // `rootfs_diff_format` marker.
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            template = %sandbox.template,
                            template_hash = sandbox.template_hash.as_deref().unwrap_or(""),
                            from_codec = crate::metrics::snapshot::CODEC_BTRFS_SEND_V1,
                            to_codec = crate::metrics::snapshot::CODEC_FULL,
                            reason = %status.message(),
                            "btrfs-send precondition not met; suspending with Full codec instead"
                        );
                        crate::metrics::snapshot::create_failed(
                            create_kind,
                            create_codec,
                            sandbox.volume_encryption,
                            "btrfs_send",
                            "fallback_to_full",
                            btrfs_step_started_at,
                        );
                        create_codec = crate::metrics::snapshot::CODEC_FULL;
                        published_format =
                            pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Full;
                    }
                    Err(status) => {
                        // Loud-fail path. `encode_rootfs_as_btrfs_send`
                        // Status messages name the failed check explicitly
                        // (template not subvolume, received_uuid mismatch,
                        // parent_uuid lineage broken, btrfs send exited
                        // with …); this log preserves that detail
                        // server-side.
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            template = %sandbox.template,
                            template_hash = sandbox.template_hash.as_deref().unwrap_or(""),
                            codec = crate::metrics::snapshot::CODEC_BTRFS_SEND_V1,
                            code = %status.code(),
                            error = %status.message(),
                            "btrfs-send rootfs encode failed; suspend snapshot wire-shape \
                             will be unusable on this attempt"
                        );
                        // If the runtime is still present, leave the caller
                        // a paused sandbox it can retry with `format=full`.
                        let reason = format!("btrfs-send encode failed: {status}");
                        self.record_suspend_snapshot_publication_failure_without_capture(
                            &sandbox.id,
                            &reason,
                        );
                        crate::metrics::snapshot::create_failed(
                            create_kind,
                            create_codec,
                            sandbox.volume_encryption,
                            "btrfs_send",
                            "encode_failed",
                            create_started_at,
                        );
                        emit_suspend_failed_event(
                            &sandbox.id,
                            &suspend_event_details,
                            "btrfs_send",
                            "encode_failed",
                            reason,
                        );
                        return Err(status);
                    }
                }
            }

            let publish_started_at = std::time::Instant::now();
            let mut verdicts = crate::retained_source::RetainedSourceVerdicts::default();
            let mut retained_rootfs_bytes = None;
            if let Some(crate::btrfs_via_nodemgr::CapturedRootfs::Deferred {
                source_id,
                rootfs_bytes,
            }) = captured_rootfs
            {
                retained_rootfs_bytes = Some(rootfs_bytes);
                // This attempt owns the source through its publication rename,
                // so node-manager's capture-time answer covers both names.
                for artifact_dir in [&staging_dir, &snapshot_dir] {
                    verdicts.record(
                        crate::retained_source::RetainedSource {
                            sandbox_id: sandbox.id.clone(),
                            artifact_dir: artifact_dir.clone(),
                            source_id,
                        },
                        crate::retained_source::RetainedSourceCheck::Present { rootfs_bytes },
                    );
                }
            }
            // An encrypted capture's sidecar was written by its seal.
            if !encrypted {
                let metadata = match suspend_artifact_metadata(
                    &sandbox,
                    published_format,
                    local_artifact_id,
                    false,
                    // The capture response is the only identity and size
                    // source: the retained bundle is not readable here.
                    captured_rootfs.and_then(crate::btrfs_via_nodemgr::CapturedRootfs::source_id),
                ) {
                    Ok(metadata) => metadata,
                    Err(status) => {
                        let reason =
                            format!("local artifact metadata construction failed: {status}");
                        self.record_suspend_snapshot_publication_failure_without_capture(
                            &sandbox.id,
                            &reason,
                        );
                        return Err(status);
                    }
                };
                if let Err(error) = crate::snapshot_upload::write_snapshot_metadata(
                    &metadata,
                    &staging_dir,
                    &crate::snapshot_upload::vm_artifacts_for_format(published_format),
                    retained_rootfs_bytes,
                ) {
                    let reason = format!("local artifact metadata creation failed: {error}");
                    self.record_suspend_snapshot_publication_failure_without_capture(
                        &sandbox.id,
                        &reason,
                    );
                    crate::metrics::snapshot::create_failed(
                        create_kind,
                        create_codec,
                        sandbox.volume_encryption,
                        "publish",
                        "metadata_failed",
                        create_started_at,
                    );
                    emit_suspend_failed_event(
                        &sandbox.id,
                        &suspend_event_details,
                        "publish",
                        "metadata_failed",
                        reason.clone(),
                    );
                    return Err(Status::internal(reason));
                }
            }
            // A teardown suspend's capture records the execution that took it,
            // so a backup requested again by artifact id stays fenced however
            // long the capture outlives its upload job and its teardown. A
            // capture that can't be recorded isn't published, as one whose
            // metadata can't be written isn't, and the failure keeps the staged
            // capture as a publication failure after staging does.
            let published = publish_recorded_suspend_snapshot_dir(
                &staging_dir,
                &snapshot_dir,
                local_artifact_id,
                durability.execution_generation(),
                &verdicts,
            );
            let publish_error = match published {
                Ok(garbage) => {
                    for path in garbage {
                        drop(
                            crate::service::recovery::staging_cleanup::reclaim_garbage_now(
                                sandbox.id.clone(),
                                path,
                                self.node_manager.clone(),
                            ),
                        );
                    }
                    None
                }
                Err(error) => Some(error),
            };
            if let Some(error) = publish_error {
                let (reason, failure) = match &error {
                    RecordedPublicationError::Record(_) => (error.to_string(), "metadata_failed"),
                    RecordedPublicationError::Publish(_) => (
                        format!("local artifact publication failed: {error}"),
                        "rename_failed",
                    ),
                };
                // A live rollback here leaves `monitoring_guard` to re-arm the
                // watchdog on return.
                if self.record_suspend_snapshot_publication_failure_after_staging(
                    &sandbox.id,
                    &sandbox_dir,
                    &staging_dir,
                    &reason,
                    &verdicts,
                ) == SuspendSnapshotStagingDisposition::Preserve
                {
                    staging_guard.disarm();
                }
                crate::metrics::snapshot::create_failed(
                    create_kind,
                    create_codec,
                    sandbox.volume_encryption,
                    "publish",
                    failure,
                    create_started_at,
                );
                emit_suspend_failed_event(
                    &sandbox.id,
                    &suspend_event_details,
                    "publish",
                    failure,
                    reason.clone(),
                );
                return Err(Status::internal(reason));
            }
            staging_guard.disarm();
            crate::metrics::snapshot::create_step(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                "publish",
                publish_started_at,
            );

            let snapshot_dir_string = snapshot_dir.to_string_lossy().into_owned();
            if let SuspendDurability::RemoteBeforeTeardown {
                execution_generation,
            } = durability
            {
                let stage_result = {
                    let db = self.db_conn()?;
                    crate::db::stage_teardown_suspend_artifact(
                        &db,
                        &sandbox.id,
                        execution_generation,
                        local_artifact_id,
                        &snapshot_dir_string,
                    )
                };
                if let Err(error) = stage_result {
                    let reason =
                        format!("persist durable suspend artifact before teardown: {error}");
                    self.record_suspend_snapshot_publication_failure_after_begin(
                        &sandbox.id,
                        &reason,
                    );
                    return Err(Status::internal(reason));
                }

                let staged_artifact = crate::db::LocalArtifactRow {
                    local_artifact_id,
                    sandbox_id: sandbox.id.clone(),
                    artifact_kind: pplx_space_common_core::types::ArtifactKind::Full,
                    artifact_origin: pplx_space_common_core::types::ArtifactOrigin::Suspend,
                    snapshot_name: None,
                    local_path: snapshot_dir_string.clone(),
                };
                // Capture/publication are finished. Remote durability can take
                // hours; do not prevent Stop/Delete admission while waiting for it.
                // Finalization reacquires the fence and checks the original owner.
                drop(cleanup_fence.take());
                self.ensure_teardown_suspend_backup(
                    &sandbox,
                    TeardownSuspendBackup {
                        execution_generation,
                        artifact: &staged_artifact,
                        format: published_format,
                        origin: TeardownBackupOrigin::Captured,
                    },
                )
                .await?;
            }

            // Node-local auto-suspend must hand the exact published artifact to a
            // durable owner before runtime cleanup releases its rootfs. A missing
            // upload destination is an intentional local-only configuration; when
            // a destination exists, failure to persist the worker intent leaves
            // the runtime intact and makes this suspend retryable.
            let upload_queue_started_at = std::time::Instant::now();
            if durability == SuspendDurability::NodeLocal {
                self.prepare_node_local_suspend_cleanup(
                    &sandbox,
                    local_artifact_id,
                    &snapshot_dir_string,
                    published_format,
                )
                .await?;
            }
            crate::metrics::snapshot::create_step(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                "upload_queue",
                upload_queue_started_at,
            );

            let cleanup_started_at = std::time::Instant::now();
            self.ensure_teardown_suspend_owner_for_durability(&sandbox.id, durability)?;
            if let Err(error) = self
                .finalize_suspended_runtime(
                    &sandbox,
                    local_artifact_id,
                    &snapshot_dir_string,
                    Some(&mut monitoring_guard),
                    cleanup_fence.take(),
                )
                .await
            {
                crate::metrics::snapshot::create_failed(
                    create_kind,
                    create_codec,
                    sandbox.volume_encryption,
                    "cleanup",
                    "cleanup_failed",
                    create_started_at,
                );
                emit_suspend_failed_event(
                    &sandbox.id,
                    &suspend_event_details,
                    "cleanup",
                    "cleanup_failed",
                    format!("cleanup after suspend failed: {error}"),
                );
                return Err(error);
            }
            crate::metrics::snapshot::create_step(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                "cleanup",
                cleanup_started_at,
            );

            if publish_node_state {
                mark_sandbox_dirty(&self.db, &sandbox.id);
            }

            crate::metrics::snapshot::create_completed(
                create_kind,
                create_codec,
                sandbox.volume_encryption,
                create_started_at,
            );
            Ok(())
        }
        .await;
        if staging_guard.armed {
            match self.state_writer_lease("SuspendStagingCleanup") {
                Ok(writer) => {
                    let _writer = writer;
                    let _fence = cleanup_fence;
                    staging_guard.cleanup(&mut node_client).await;
                }
                Err(error) => {
                    tracing::warn!(%error, "suspend staging cleanup admission refused; retaining residue");
                }
            }
        }
        result
    }

    /// Publication is already validated under wake, artifact and cleanup fences.
    /// A live plaintext VM can roll back, but must keep the newly published
    /// artifact: publication retired its predecessor. Otherwise cleanup owns
    /// handoff retries and the bounded terminal decision.
    pub(super) async fn prepare_node_local_suspend_cleanup(
        &self,
        sandbox: &crate::db::SandboxRow,
        artifact: LocalArtifactId,
        path: &str,
        format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
    ) -> Result<(), Status> {
        if !sandbox.volume_encryption
            && (self.sandbox_runtime_live)(&sandbox.id)
            && let Err(error) = tokio::time::timeout(
                Duration::from_secs(30),
                self.enqueue_node_local_suspend_backup(sandbox, artifact, path, Some(format)),
            )
            .await
            .unwrap_or_else(|_| {
                Err(ArtifactBackupSubmissionError::Other(
                    "backup handoff attempt timed out".to_owned(),
                ))
            })
            && (self.sandbox_runtime_live)(&sandbox.id)
        {
            let applied = crate::db::rollback_published_suspend_to_paused(
                &*self.db_conn()?,
                sandbox,
                artifact,
                path,
            )
            .map_err(|error| {
                Status::internal(format!("retain published suspend on rollback: {error}"))
            })?;
            if !applied {
                return Err(Status::aborted("published suspend rollback superseded"));
            }
            mark_sandbox_dirty(&self.db, &sandbox.id);
            return Err(artifact_backup_fence_status(error.to_string()));
        }
        crate::db::pin_suspended_artifact_for_cleanup(
            &*self.db_conn()?,
            &sandbox.id,
            artifact,
            path,
        )
        .map_err(|error| Status::internal(format!("persist published suspend cleanup: {error}")))
    }

    pub(super) async fn finalize_suspended_runtime(
        &self,
        sandbox: &crate::db::SandboxRow,
        local_artifact_id: LocalArtifactId,
        snapshot_dir: &str,
        monitoring_guard: Option<&mut SuspendMonitoringGuard>,
        cleanup_fence: Option<OwnedCleanupFenceGuard>,
    ) -> Result<(), Status> {
        let fence = match cleanup_fence {
            Some(fence) => fence,
            None => match self.try_cleanup_fence(&sandbox.id) {
                CleanupLockTryResult::Acquired(fence) => fence,
                CleanupLockTryResult::Busy => {
                    return Err(Status::unavailable(
                        "sandbox cleanup is active; retry suspend completion",
                    ));
                }
            },
        };
        let current = self.lookup_sandbox(&sandbox.id)?;
        if current.uuid8 != sandbox.uuid8
            || current.space_api_route_identity.execution_generation
                != sandbox.space_api_route_identity.execution_generation
            || !CleanupCompletion::MarkSuspended.owns_row_intent(&current)
        {
            return Err(Status::aborted("suspend completion superseded"));
        }
        if let Some(guard) = monitoring_guard {
            guard.disarm();
        }
        self.watchdog.remove_sandbox(&sandbox.id).await;
        self.snapshot_scheduler.remove_sandbox(&sandbox.id).await;
        let (sandbox_id, _) = Self::cleanup_identity(&sandbox.id, Some(&sandbox.uuid8))?;
        let admission = CleanupAdmission::persisted(
            sandbox_id,
            sandbox.space_api_route_identity.execution_generation,
            None,
            fence,
        );
        self.run_cleanup_to_completion_using_admission(
            SynchronousCleanupRequest {
                sandbox_id: &sandbox.id,
                uuid8: sandbox.uuid8.as_str(),
                keep_filesystem: true,
                context: "cleanup after suspend",
                completion: CleanupCompletion::MarkSuspended,
                published_artifact: Some((local_artifact_id, snapshot_dir.to_string())),
                immediate_kill: Self::cleanup_immediate_kill(CleanupCompletion::MarkSuspended),
                guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
            },
            Some(admission),
            None,
            super::cleanup_executor::CleanupWait::Attempt,
        )
        .await?;
        emit_sandbox_event(
            &sandbox.id,
            SandboxEventType::LocalArtifactPublished,
            SandboxEventSource::SbxManager,
            SandboxEventDetails::default()
                .local_artifact_id(local_artifact_id.to_string())
                .artifact_kind("full")
                .artifact_origin("suspend"),
        );
        Ok(())
    }

    /// Persist a published local artifact backup intent on the durable worker.
    /// The public enqueue and teardown handoffs additionally stage immutable
    /// vol-mgr transfer handles and establish the Cluster API `creating` row
    /// before returning; the compatibility reupload handoff returns after the
    /// local intent. In every case the worker polls and finalizes across
    /// process restarts.
    ///
    /// `request_override`, when `Some`, takes precedence over the env-derived
    /// default in [`SnapshotUploadConfig::from_env`].
    /// Encode the post-FC-freeze rootfs as a `btrfs send -p` stream
    /// by delegating to node-manager (the privileged daemon —
    /// sbxmgr is non-root in prod and lacks `CAP_SYS_ADMIN` for the
    /// btrfs ioctls). Strict preconditions hold inside nodemgr; any
    /// mismatch comes back as `FailedPrecondition` so the caller
    /// can retry with `format=full`. Preconditions checked
    /// nodemgr-side:
    ///
    /// 1. The local template directory at
    ///    `/space/cache/firecracker/templates/<name>/<content_hash>` is
    ///    a btrfs subvolume with `received_uuid` matching
    ///    `deterministic_template_received_uuid(name, content_hash)`.
    /// 2. The sandbox `instance/` directory is a btrfs subvolume whose
    ///    `parent_uuid` equals the local template's `uuid`. Set by
    ///    `node-mgr::launch_prepare` when the template was a subvolume
    ///    at sandbox-create time (the default for tmplmgr-installed
    ///    templates; non-subvolume legacy templates take the
    ///    create+reflink path and end up without a parent-UUID
    ///    lineage).
    ///
    /// Nodemgr then takes a read-only btrfs snapshot of the
    /// instance, runs `btrfs send -p <template> <instance.ro>`
    /// writing to `<snapshot_dir>/rootfs.btrfs-send`, removes the
    /// now-superseded `rootfs.ext4` so the upload set doesn't carry
    /// both, and tears down the `instance.ro` snapshot.
    ///
    /// The `sandbox.template_hash` precondition is checked here
    /// (sbxmgr-side) before the gRPC call so we surface the no-hash
    /// case without a round-trip; nodemgr's RPC would reject an
    /// empty `content_hash` field with the same FailedPrecondition.
    async fn encode_rootfs_as_btrfs_send(
        &self,
        sandbox: &crate::db::SandboxRow,
        snapshot_dir: &str,
    ) -> Result<crate::btrfs_via_nodemgr::CapturedRootfs, Status> {
        let content_hash = sandbox.template_hash.as_deref().ok_or_else(|| {
            Status::failed_precondition(
                "btrfs-send codec requires sandbox.template_hash to be set; \
                 retry with format=full",
            )
        })?;

        // The parent's partition follows the sandbox's binding, read as the
        // snapshot sidecar records it, else as the row does. Node-manager
        // falls back to the legacy build when the sandbox descends from it.
        let environment = super::super::tail_support::codec_environment(
            &sandbox.id,
            super::super::tail_support::sandbox_snapshot_execution_metadata(sandbox),
            sandbox.execution_metadata.as_ref(),
        );
        let captured = crate::btrfs_via_nodemgr::encode_rootfs_send(
            &sandbox.id,
            &sandbox.template,
            content_hash,
            environment,
            snapshot_dir,
            sandbox
                .node_execution_fence()
                .as_ref()
                .ok_or_else(|| Status::failed_precondition("execution identity is missing"))?
                .try_into()?,
            &self.node_manager,
        )
        .await
        .map_err(|e| e.into_status())?;

        tracing::info!(
            sandbox_id = %sandbox.id,
            template = %sandbox.template,
            content_hash,
            stream_bytes = captured.stream_bytes(),
            deferred = matches!(captured, crate::btrfs_via_nodemgr::CapturedRootfs::Deferred { .. }),
            "btrfs-send codec: rootfs encoded as btrfs send -p stream (via nodemgr)"
        );
        Ok(captured)
    }

    /// Node-manager's current answer for a retained rootfs about to be backed
    /// up. Only a present source proceeds; streaming is chosen per backup, so
    /// a retained source still uploads after volume-manager loses streaming.
    pub(super) async fn inspect_retained_upload(
        &self,
        meta: &crate::snapshot_upload::SnapshotMeta,
        snapshot_dir: &std::path::Path,
        format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
    ) -> Result<Option<crate::snapshot_upload::RetainedUpload>, RetainedUploadError> {
        use crate::retained_source::{RetainedSourceCheck, RootfsArtifact};
        if meta.encrypted
            || format != pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend
        {
            return Ok(None);
        }
        let Some(RootfsArtifact::RetainedSource(source)) =
            RootfsArtifact::resolve_meta(meta, snapshot_dir)?
        else {
            return Ok(None);
        };
        match self.retained_source_inspector.inspect(&source).await {
            RetainedSourceCheck::Present { rootfs_bytes } => {
                Ok(Some(crate::snapshot_upload::RetainedUpload {
                    source,
                    rootfs_bytes,
                    stream: crate::btrfs_via_nodemgr::volume_manager_streams_btrfs_send().await,
                }))
            }
            check => Err(RetainedUploadError::NotBackable {
                retained: source,
                check,
            }),
        }
    }
}

/// Why a retained rootfs cannot be prepared for upload.
#[derive(Debug, thiserror::Error)]
pub(in crate::service) enum RetainedUploadError {
    #[error(transparent)]
    Resolve(#[from] crate::retained_source::RootfsResolveError),
    #[error(
        "retained send source {} in {} cannot be backed up: {check}",
        retained.source_id,
        retained.artifact_dir.display()
    )]
    NotBackable {
        retained: crate::retained_source::RetainedSource,
        check: crate::retained_source::RetainedSourceCheck,
    },
}
