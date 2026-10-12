use super::cleanup_support::CleanupLockTryResult;
use super::recovery::{STARTUP_RECOVERY_CONCURRENCY, StartupRecoveryError};
use super::spaced_bridge::SpacedEndpointProofPolicy;
use super::*;
use crate::db::{SandboxRow, StartupRecoveryOutcome, StartupRecoveryRecord};
use crate::runtime_reconciliation::{FirecrackerVmStateObservation, observe_firecracker_vm_state};
use futures_util::{StreamExt, stream};
use pplx_space_common_core::types::SandboxQuarantineReasonCode;

const RESIDENT_SETUP_TIMEOUT: Duration = Duration::from_secs(11);
const ATTESTED_SETUP_TIMEOUT: Duration = Duration::from_secs(5);

impl SandboxManagerService {
    /// Each candidate takes at most three bounded runtime probes and eleven
    /// seconds of attested setup. Ambiguous evidence preserves the resident VM.
    pub(super) async fn recover_interrupted_resident_operations(
        &self,
    ) -> Result<Vec<StartupRecoveryRecord>, StartupRecoveryError> {
        let rows = {
            let db = self.startup_recovery_db()?;
            crate::db::list_sandboxes_for_recovery(&db)?
        };
        let failed = AtomicBool::new(false);
        stream::iter(rows.into_iter().filter(|row| {
            matches!(
                row.state,
                SandboxState::Paused | SandboxState::Resuming | SandboxState::Checkpointing
            )
        }))
        .take_while(|_| std::future::ready(!failed.load(Ordering::Relaxed)))
        .map(|row| {
            let failed = &failed;
            async move {
                let prepare_row = row.clone();
                let result = self
                    .recover_resident_with_probe(
                        row,
                        Path::new(RUN_DIR),
                        |id| async move {
                            observe_firecracker_vm_state(
                                &id,
                                "startup resident recovery",
                                &self.node_manager,
                            )
                            .await
                        },
                        |target| self.prepare_startup_resident(&prepare_row, target),
                    )
                    .await;
                if result.is_err() {
                    failed.store(true, Ordering::Relaxed);
                }
                result
            }
        })
        .buffer_unordered(STARTUP_RECOVERY_CONCURRENCY)
        // A failed peer must not drop another peer's in-flight guest setup.
        // Stop admission on failure, then fail startup after admitted work finishes.
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect()
    }

    pub(super) async fn prepare_startup_resident(
        &self,
        row: &SandboxRow,
        target: SandboxState,
    ) -> Result<(), Status> {
        prepare_resident_peer(
            &row.id,
            row.state,
            target,
            &row.process_env,
            self.connect_spaced_host_with_policy(row, SpacedEndpointProofPolicy::ValidateOnly),
        )
        .await
    }

    /// Discovery already bounds this batch. Cancellation stops admission, never
    /// drops an owned mutation; an error in one row must not cancel its peers.
    pub(super) async fn recover_periodic_resident_operations(
        &self,
        rows: Vec<SandboxRow>,
        shutdown: &CancellationToken,
    ) {
        stream::iter(rows.into_iter().map(|row| async move {
            if shutdown.is_cancelled() {
                return;
            }
            let prepare_row = row.clone();
            if let Err(error) = self
                .recover_resident_with_probe(
                    row,
                    Path::new(RUN_DIR),
                    |id| async move {
                        observe_firecracker_vm_state(
                            &id,
                            "periodic resident recovery",
                            &self.node_manager,
                        )
                        .await
                    },
                    |target| self.prepare_startup_resident(&prepare_row, target),
                )
                .await
            {
                tracing::warn!(sandbox_id = %prepare_row.id, %error, "periodic resident recovery failed; will retry");
            }
        }))
        .buffer_unordered(STARTUP_RECOVERY_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    }

    pub(super) async fn recover_resident_with_probe<Probe, ProbeFuture, Prepare, PrepareFuture>(
        &self,
        row: SandboxRow,
        run_dir: &Path,
        probe: Probe,
        prepare_resume: Prepare,
    ) -> Result<StartupRecoveryRecord, StartupRecoveryError>
    where
        Probe: FnMut(SandboxId) -> ProbeFuture,
        ProbeFuture: std::future::Future<Output = FirecrackerVmStateObservation>,
        Prepare: FnOnce(SandboxState) -> PrepareFuture,
        PrepareFuture: std::future::Future<Output = Result<(), Status>>,
    {
        let reason_code = match row.state {
            SandboxState::Paused => SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
            SandboxState::Resuming => SandboxQuarantineReasonCode::ResidentResumeFailed,
            SandboxState::Checkpointing => SandboxQuarantineReasonCode::CheckpointResumeFailed,
            _ => return Err(rusqlite::Error::InvalidQuery.into()),
        };
        // Resume uses the wake lock; teardown/activation uses the cleanup
        // fence. Hold both through setup so a stale recovery cannot configure
        // the guest belonging to a replacement execution.
        let wake_lock = self.auto_lifecycle.wake_lock(&row.id);
        let Ok(_wake) = wake_lock.try_lock() else {
            return Ok(StartupRecoveryRecord {
                id: row.id,
                previous_state: row.state,
                outcome: StartupRecoveryOutcome::Deferred,
            });
        };
        let CleanupLockTryResult::Acquired(_cleanup) = self.try_cleanup_fence(&row.id) else {
            return Ok(StartupRecoveryRecord {
                id: row.id,
                previous_state: row.state,
                outcome: StartupRecoveryOutcome::Deferred,
            });
        };
        {
            let db = self.startup_recovery_db()?;
            if !crate::db::startup_resident_witness_current(&db, &row)? {
                return Ok(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::Deferred,
                });
            }
        }
        let mut target = SandboxState::Quarantined;
        let mut detail = "startup recovery lacks a retained runtime identity".to_string();
        match row.id.parse::<SandboxId>() {
            Err(_) => {
                // Corrupt evidence belongs to this row, not the service. The
                // durable operator quarantine prevents endless startup retries.
                detail =
                    "startup recovery found an invalid sandbox ID; operator repair required".into();
            }
            Ok(id) if crate::db::sandbox_has_runtime_fields(&row) => {
                use crate::runtime_reconciliation::observe_stable_resident_state;
                use pplx_space_node_runtime_core::fc_api::FirecrackerVmState;
                match observe_stable_resident_state(id, probe).await {
                    // A stable pause needs no replay. Failure to reach the node
                    // is not proof of lost memory while its PID witness exists.
                    Ok(FirecrackerVmState::Paused) if row.state == SandboxState::Paused => {
                        return Ok(StartupRecoveryRecord {
                            id: row.id,
                            previous_state: row.state,
                            outcome: StartupRecoveryOutcome::Deferred,
                        });
                    }
                    Ok(FirecrackerVmState::Running) if row.state == SandboxState::Paused => {
                        detail = "paused lifecycle contradicts the running resident VM".into();
                    }
                    Ok(FirecrackerVmState::Running) => target = SandboxState::Started,
                    Ok(FirecrackerVmState::Paused) if row.state != SandboxState::Checkpointing => {
                        target = SandboxState::Paused
                    }
                    Ok(FirecrackerVmState::Paused) => {
                        detail = "startup checkpoint pause has no durable lifecycle origin".into();
                    }
                    Ok(FirecrackerVmState::NotStarted) => {
                        detail = "Firecracker reported NotStarted; resident memory is unavailable"
                            .into();
                    }
                    Err(error) if row.state == SandboxState::Paused => {
                        if !matches!(
                            tokio::fs::try_exists(run_dir.join(&row.id).join("fc.pid")).await,
                            Ok(false)
                        ) {
                            return Ok(StartupRecoveryRecord {
                                id: row.id,
                                previous_state: row.state,
                                outcome: StartupRecoveryOutcome::Deferred,
                            });
                        }
                        detail = format!(
                            "paused runtime has no PID witness and cannot establish resident state: {error}"
                        );
                    }
                    Err(error) => detail = error.to_string(),
                }
            }
            Ok(_) => {}
        }
        if target != SandboxState::Quarantined {
            {
                let db = self.startup_recovery_db()?;
                if !crate::db::startup_resident_witness_current(&db, &row)? {
                    return Ok(StartupRecoveryRecord {
                        id: row.id,
                        previous_state: row.state,
                        outcome: StartupRecoveryOutcome::Deferred,
                    });
                }
            }
            // State observation alone does not attest runtime ownership or
            // prove required guest setup finished before the manager restarted.
            match tokio::time::timeout(RESIDENT_SETUP_TIMEOUT, prepare_resume(target)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    target = SandboxState::Quarantined;
                    // Guest status messages are untrusted, even on an attested
                    // relay. Persist only the finite status code and category.
                    detail = format!("startup resident setup failed (status {:?})", error.code());
                }
                Err(_) => {
                    target = SandboxState::Quarantined;
                    detail = "startup resident setup timed out".into();
                }
            }
        }
        let changed = {
            let db = self.startup_recovery_db()?;
            let settlement = match target {
                SandboxState::Started => crate::db::StartupResidentSettlement::Started,
                SandboxState::Paused => crate::db::StartupResidentSettlement::Paused,
                _ => crate::db::StartupResidentSettlement::Quarantined {
                    reason_code,
                    message: &detail,
                },
            };
            crate::db::settle_startup_resident(&db, &row, settlement)?
        };
        if changed {
            self.watchdog.record_lifecycle_edge(&row.id).await;
            if target != SandboxState::Started {
                self.snapshot_scheduler.remove_sandbox(&row.id).await;
            }
            mark_sandbox_dirty(&self.db, &row.id);
        }
        Ok(StartupRecoveryRecord {
            id: row.id,
            previous_state: row.state,
            outcome: if changed {
                StartupRecoveryOutcome::State(target)
            } else {
                StartupRecoveryOutcome::Deferred
            },
        })
    }
}

trait ResidentRecoveryPeer {
    async fn configure(&mut self, env: &BTreeMap<String, String>) -> Result<(), Status>;
    async fn synchronize_clock(&mut self) -> Result<(), Status>;
}

impl ResidentRecoveryPeer for pplx_space_spaced_host_client::SpacedHostClient {
    async fn configure(&mut self, env: &BTreeMap<String, String>) -> Result<(), Status> {
        self.configure_process_env(env.clone().into_iter().collect())
            .await
    }

    async fn synchronize_clock(&mut self) -> Result<(), Status> {
        let response = self
            .notify_resume(pplx_space_spaced_host_client::proto::ResumeReason::SuspendResume)
            .await?;
        if response.clock_sync_status()
            == pplx_space_spaced_host_client::proto::ClockSyncStatus::Synced
        {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                "guest clock synchronization did not succeed",
            ))
        }
    }
}

async fn prepare_resident_peer<P: ResidentRecoveryPeer>(
    sandbox_id: &str,
    source: SandboxState,
    target: SandboxState,
    env: &BTreeMap<String, String>,
    connect: impl std::future::Future<Output = Result<P, Status>>,
) -> Result<(), Status> {
    let mut peer = tokio::time::timeout(ATTESTED_SETUP_TIMEOUT, async {
        let mut peer = connect.await?;
        if source == SandboxState::Resuming && target == SandboxState::Started && !env.is_empty() {
            peer.configure(env).await?;
        }
        Ok::<_, Status>(peer)
    })
    .await
    .map_err(|_| {
        Status::deadline_exceeded("startup endpoint attestation or environment setup timed out")
    })??;
    if target == SandboxState::Started {
        // Every recovered thaw, including a checkpoint, can have missed its
        // notification. Keep normal resume's observable, nonterminal clock
        // failure policy, and use the same authenticated connection.
        match tokio::time::timeout(RESUME_CLOCK_SYNC_TIMEOUT, peer.synchronize_clock()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(
                sandbox_id,
                status_code = ?error.code(),
                "startup resume clock synchronization failed"
            ),
            Err(_) => tracing::warn!(sandbox_id, "startup resume clock synchronization timed out"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
