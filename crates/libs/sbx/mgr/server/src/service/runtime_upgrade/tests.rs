//! The upgrade's phases against the real store and scripted node work. The
//! fake keeps a model of the instance disk, its preserved copy and the
//! checkpoint, so each test can check what a failure leaves behind.

use super::*;
use crate::db::fixture::insert_test_sandbox;
use crate::db::{ActivationKind, ActivationRouteAdmission};
use pplx_space_common_core::types::{LocalArtifactId, SandboxCleanupEpisodeId, SandboxId};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

struct Environments {
    source: ExecutionMetadata,
    target: ExecutionMetadata,
    /// Another kernel: a cold start can't boot the source disk under it.
    incompatible: ExecutionMetadata,
}

fn environments() -> Environments {
    let compiled = pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(
        pplx_space_common_core::types::Arch::X86_64,
    )
    .unwrap();
    let executions: Vec<_> = compiled
        .iter()
        .map(|environment| environment.definition().execution_requirements().unwrap())
        .collect();
    let source = executions[0].clone();
    let boots_source = |candidate: &ExecutionMetadata| {
        crate::service::create_support::bind_disk_execution(Some(candidate), source.clone()).is_ok()
    };
    let target = executions[1..]
        .iter()
        .find(|candidate| boots_source(candidate))
        .unwrap()
        .clone();
    let incompatible = executions[1..]
        .iter()
        .find(|candidate| !boots_source(candidate))
        .unwrap()
        .clone();
    // The target keeps the kernel but not the snapshot format: the cold
    // start must honor a consumer whose format differs from the producer's.
    assert_ne!(target.state_format(), source.state_format());
    Environments {
        source,
        target,
        incompatible,
    }
}

fn reference(execution: &ExecutionMetadata) -> EnvironmentReference {
    execution.environment().unwrap()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Boot {
    /// Claim the sandbox, write the disk and become started.
    Succeed,
    /// Claim the sandbox, write the disk, then fail; the activation's cleanup
    /// returns the sandbox to its old state after `cleanup_after`.
    FailAfterWriting,
    /// Fail before claiming anything.
    FailBeforeClaim,
    /// Claim the sandbox, then fail as it is deleted.
    FailAsDeleted,
    /// Fail like `FailAfterWriting`; auto-lifecycle then stops the sandbox
    /// the cleanup returned to suspended.
    FailAfterAutoStop,
    /// Become started, then report an error anyway.
    SucceedThenFail,
    /// Claim the sandbox and write the disk, then never answer, as a crashed
    /// process would; the row stays starting.
    Hang,
}

struct Fake {
    db: crate::db::SharedConnection,
    resolved: Mutex<ExecutionMetadata>,
    disk: Mutex<LocalDisk>,
    instance: Mutex<String>,
    preserved: Mutex<Option<String>>,
    checkpoint_present: AtomicBool,
    boot: Mutex<Boot>,
    cleanup_after: Mutex<Duration>,
    fail_preserve: AtomicBool,
    fail_restore: AtomicBool,
    fail_retire: AtomicBool,
    /// Stop the sandbox behind the upgrade's back while it inspects the disk.
    stop_during_inspect: AtomicBool,
    calls: Mutex<Vec<&'static str>>,
}

impl Fake {
    fn new(db: crate::db::SharedConnection, envs: &Environments, disk: LocalDisk) -> Self {
        Self {
            db,
            resolved: Mutex::new(envs.target.clone()),
            disk: Mutex::new(disk),
            instance: Mutex::new("old disk".into()),
            preserved: Mutex::new(None),
            checkpoint_present: AtomicBool::new(true),
            boot: Mutex::new(Boot::Succeed),
            cleanup_after: Mutex::new(Duration::from_millis(30)),
            fail_preserve: AtomicBool::new(false),
            fail_restore: AtomicBool::new(false),
            fail_retire: AtomicBool::new(false),
            stop_during_inspect: AtomicBool::new(false),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn called(&self, call: &'static str) {
        self.calls.lock().unwrap().push(call);
    }

    fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().unwrap().clone()
    }

    fn phase(&self, operation_id: RuntimeUpgradeOperationId) -> Option<RuntimeUpgradePhase> {
        crate::db::read::runtime_upgrade(&self.db.lock().unwrap(), operation_id)
            .unwrap()
            .map(|record| record.phase)
    }

    fn instance(&self) -> String {
        self.instance.lock().unwrap().clone()
    }
}

#[tonic::async_trait]
impl RuntimeUpgradeEffects for Arc<Fake> {
    async fn inspect_local_disk(&self, sandbox: &SandboxRow) -> Result<LocalDisk, Status> {
        self.called("inspect");
        if self.stop_during_inspect.load(Ordering::SeqCst) {
            self.db
                .lock()
                .unwrap()
                .execute(
                    "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
                    crate::db::fixture::params![sandbox.id],
                )
                .unwrap();
        }
        Ok(self.disk.lock().unwrap().clone())
    }

    async fn preserve_disk(&self, _sandbox_id: &str, name: &str) -> Result<(), Status> {
        self.called("preserve");
        assert!(name.starts_with("d-operation-runtime-upgrade-"));
        if self.fail_preserve.load(Ordering::SeqCst) {
            return Err(Status::internal("scripted snapshot failure"));
        }
        *self.preserved.lock().unwrap() = Some(self.instance());
        Ok(())
    }

    async fn restore_disk(&self, sandbox_id: &str, _name: &str) -> Result<(), Status> {
        self.called("restore");
        // Nothing may replace the disk under a runtime that could still use it.
        let row = crate::db::read::sandbox(&self.db.lock().unwrap(), sandbox_id)
            .unwrap()
            .unwrap();
        assert!(
            matches!(row.state, SandboxState::Stopped | SandboxState::Suspended)
                && row.cleanup_completion.is_none(),
            "restore ran while the sandbox was {}",
            row.state
        );
        if self.fail_restore.load(Ordering::SeqCst) {
            return Err(Status::unavailable("scripted restore failure"));
        }
        let preserved = self
            .preserved
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| Status::not_found("no preserved copy"))?;
        *self.instance.lock().unwrap() = preserved;
        Ok(())
    }

    async fn discard_disk(&self, _sandbox_id: &str, _name: &str) -> Result<(), Status> {
        self.called("discard");
        *self.preserved.lock().unwrap() = None;
        Ok(())
    }

    async fn resolve_target(
        &self,
        target: EnvironmentReference,
    ) -> Result<ExecutionMetadata, Status> {
        self.called("resolve");
        let resolved = self.resolved.lock().unwrap().clone();
        assert_eq!(Some(target), resolved.environment());
        Ok(resolved)
    }

    async fn cold_boot(
        &self,
        operation_id: RuntimeUpgradeOperationId,
        start: StartRequest,
    ) -> Result<StartResponse, Status> {
        self.called("boot");
        assert!(start.blocking);
        let sandbox_id = start.sandbox_id.clone();
        let boot = *self.boot.lock().unwrap();
        if boot == Boot::FailBeforeClaim {
            return Err(Status::unavailable("scripted node-manager outage"));
        }
        let (state, cleanup_id) = {
            let db = self.db.lock().unwrap();
            let row = crate::db::read::sandbox(&db, &sandbox_id).unwrap().unwrap();
            // The boot runs on the consumer the upgrade saved.
            assert_eq!(
                row.execution_metadata.as_ref(),
                Some(&*self.resolved.lock().unwrap())
            );
            // A concurrent resume can't claim the sandbox mid-move.
            let kinds: &[ActivationKind] = if row.state == SandboxState::Suspended {
                &[ActivationKind::Restore, ActivationKind::ColdStart]
            } else {
                &[ActivationKind::ColdStart]
            };
            for &kind in kinds {
                assert!(
                    matches!(
                        crate::db::test_fixtures::begin_activation(
                            &db,
                            &sandbox_id,
                            row.state,
                            kind,
                            None,
                            None,
                        ),
                        Err(BeginActivationError::RuntimeUpgrade(
                            crate::db::RuntimeUpgradeFenceError::Fenced { .. }
                        ))
                    ),
                    "{kind:?} passed the upgrade's fence"
                );
            }
            let episode = crate::db::begin_activation_with_route_evidence(
                &db,
                &sandbox_id,
                row.state,
                ActivationKind::ColdStart,
                None,
                None,
                ActivationRouteAdmission {
                    runtime_upgrade: Some(operation_id),
                    ..ActivationRouteAdmission::default()
                },
            )
            .map_err(|error| begin_activation_status(error, "claim"))?
            .expect("the upgrade's own boot claims the sandbox");
            (row.state, episode.id)
        };
        *self.instance.lock().unwrap() = "disk written by the new boot".into();
        match boot {
            Boot::Succeed | Boot::SucceedThenFail => {
                let db = self.db.lock().unwrap();
                assert!(
                    crate::db::mark_start_succeeded(&db, &sandbox_id, Some(cleanup_id)).unwrap()
                );
                if boot == Boot::SucceedThenFail {
                    return Err(Status::internal("the gateway push failed after readiness"));
                }
                Ok(StartResponse {
                    sandbox_id,
                    pid: 42,
                    ..Default::default()
                })
            }
            Boot::FailAfterWriting | Boot::FailAfterAutoStop => {
                let db = self.db.clone();
                let delay = *self.cleanup_after.lock().unwrap();
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    roll_back_activation(&db, &sandbox_id, state, cleanup_id);
                    if boot == Boot::FailAfterAutoStop {
                        db.lock()
                            .unwrap()
                            .execute(
                                "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
                                crate::db::fixture::params![sandbox_id],
                            )
                            .unwrap();
                    }
                });
                Err(Status::deadline_exceeded("guest never became ready"))
            }
            Boot::Hang => Err(Status::cancelled("the process died mid-boot")),
            Boot::FailAsDeleted => {
                crate::db::delete_sandbox(&self.db.lock().unwrap(), &sandbox_id).unwrap();
                Err(Status::aborted("the sandbox was deleted"))
            }
            Boot::FailBeforeClaim => unreachable!(),
        }
    }

    async fn stop(
        &self,
        sandbox_id: &str,
        execution: pplx_space_common_core::types::SandboxActivationAttemptId,
    ) -> Result<(), Status> {
        self.called("stop");
        // Only ever the verified boot's own execution.
        assert_eq!(
            crate::db::read::sandbox(&self.db.lock().unwrap(), sandbox_id)
                .unwrap()
                .unwrap()
                .node_execution_id,
            Some(execution)
        );
        self.db
            .lock()
            .unwrap()
            .execute(
                "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
                crate::db::fixture::params![sandbox_id],
            )
            .unwrap();
        Ok(())
    }

    async fn exclude_disk_mutations(&self, sandbox_id: &str) -> Box<dyn Send> {
        self.called("exclude");
        Box::new(Exclusion {
            fake: Arc::clone(self),
            sandbox_id: sandbox_id.to_string(),
        })
    }

    async fn retire_checkpoint(&self, record: &RuntimeUpgradeRecord) -> Result<(), Status> {
        self.called("retire");
        if self.fail_retire.load(Ordering::SeqCst) {
            return Err(Status::unavailable("scripted retirement failure"));
        }
        // Retirement only ever follows a verified boot.
        assert_eq!(
            self.phase(record.operation_id),
            Some(RuntimeUpgradePhase::Verified)
        );
        self.checkpoint_present.store(false, Ordering::SeqCst);
        Ok(())
    }
}

/// Records, when released, whether the fence was in place by then: the fence
/// must be inserted while other disk writers are held off.
struct Exclusion {
    fake: Arc<Fake>,
    sandbox_id: String,
}

impl Drop for Exclusion {
    fn drop(&mut self) {
        let fenced =
            crate::db::read::runtime_upgrade_fence(&self.fake.db.lock().unwrap(), &self.sandbox_id)
                .unwrap()
                .is_some();
        self.fake.called(if fenced {
            "release fenced"
        } else {
            "release unfenced"
        });
    }
}

fn roll_back_activation(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
    state: SandboxState,
    cleanup_id: SandboxCleanupEpisodeId,
) {
    let db = db.lock().unwrap();
    assert!(crate::db::rollback_starting_episode(&db, sandbox_id, state, cleanup_id).unwrap());
}

struct Harness {
    db: crate::db::SharedConnection,
    fake: Arc<Fake>,
    upgrader: RuntimeUpgrader<Arc<Fake>>,
    envs: Environments,
    sandbox_id: String,
    checkpoint_artifact: Option<LocalArtifactId>,
}

fn harness(state: SandboxState) -> Harness {
    let envs = environments();
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &sandbox_id, state);
    crate::db::upsert_sandbox_execution_metadata(&conn, &sandbox_id, &envs.source).unwrap();
    let checkpoint_artifact = (state == SandboxState::Suspended).then(|| {
        let artifact = LocalArtifactId::new();
        assert!(
            crate::db::adopt_suspended_local_artifact(
                &conn,
                &sandbox_id,
                artifact,
                "/space/cache/test/snapshot",
            )
            .unwrap()
        );
        artifact
    });
    let db = crate::db::SharedConnection::new(conn);
    let disk = LocalDisk {
        execution: envs.source.clone(),
        checkpoint_execution: (state == SandboxState::Suspended).then(|| envs.source.clone()),
        checkpoint_dir_identity: (state == SandboxState::Suspended).then(|| "1:2".to_string()),
    };
    let fake = Arc::new(Fake::new(db.clone(), &envs, disk));
    let upgrader = RuntimeUpgrader::new(db.clone(), Arc::clone(&fake))
        .with_settle_timing(Duration::from_secs(5), Duration::from_millis(5));
    Harness {
        db,
        fake,
        upgrader,
        envs,
        sandbox_id,
        checkpoint_artifact,
    }
}

impl Harness {
    fn request(
        &self,
        operation_id: RuntimeUpgradeOperationId,
        target_state: RuntimeUpgradeTargetState,
    ) -> RuntimeUpgradeRequest {
        RuntimeUpgradeRequest {
            operation_id,
            sandbox_id: self.sandbox_id.clone(),
            target: reference(&self.envs.target),
            target_state,
            authorization: RuntimeUpgradeAuthorization {
                id: "block-e0".into(),
                reason: "runtime advisory".into(),
                memory_loss: true,
            },
            start: StartRequest {
                sandbox_id: self.sandbox_id.clone(),
                blocking: true,
                ..Default::default()
            },
        }
    }

    fn row(&self) -> SandboxRow {
        crate::db::read::sandbox(&self.db.lock().unwrap(), &self.sandbox_id)
            .unwrap()
            .unwrap()
    }

    fn record(&self, operation_id: RuntimeUpgradeOperationId) -> Option<RuntimeUpgradeRecord> {
        crate::db::read::runtime_upgrade(&self.db.lock().unwrap(), operation_id).unwrap()
    }

    fn fence(&self) -> Option<(RuntimeUpgradeOperationId, RuntimeUpgradePhase)> {
        let db = self.db.lock().unwrap();
        crate::db::read::runtime_upgrade_fence(&db, &self.sandbox_id)
            .unwrap()
            .map(|operation_id| {
                let record = crate::db::read::runtime_upgrade(&db, operation_id).unwrap();
                (operation_id, record.unwrap().phase)
            })
    }

    /// Put the row back in `phase`, as a crash between two steps leaves it.
    fn force_phase(&self, operation_id: RuntimeUpgradeOperationId, phase: RuntimeUpgradePhase) {
        self.db
            .lock()
            .unwrap()
            .execute(
                "UPDATE sandbox_runtime_upgrades SET phase = ?2 WHERE operation_id = ?1",
                crate::db::fixture::params![operation_id.to_string(), phase.as_str()],
            )
            .unwrap();
    }

    /// The old pair is whole: the old disk, the checkpoint, the old consumer,
    /// and nothing fencing a restore or start onto it.
    fn assert_old_pair_restorable(&self, state: SandboxState) {
        assert_eq!(self.fake.instance(), "old disk");
        assert!(self.fake.checkpoint_present.load(Ordering::SeqCst));
        assert_eq!(*self.fake.preserved.lock().unwrap(), None);
        let row = self.row();
        assert_eq!(row.state, state);
        assert_eq!(row.execution_metadata, Some(self.envs.source.clone()));
        assert_eq!(row.current_local_artifact_id, self.checkpoint_artifact);
        assert_eq!(self.fence(), None);
        let kind = if state == SandboxState::Suspended {
            ActivationKind::Restore
        } else {
            ActivationKind::ColdStart
        };
        assert!(
            crate::db::test_fixtures::begin_activation(
                &self.db.lock().unwrap(),
                &self.sandbox_id,
                state,
                kind,
                None,
                None
            )
            .unwrap()
            .is_some(),
            "the old pair must be activatable again"
        );
    }
}

fn operation() -> RuntimeUpgradeOperationId {
    RuntimeUpgradeOperationId::from_uuid(uuid::Uuid::now_v7())
}

#[tokio::test]
async fn a_suspended_sandbox_boots_on_the_target_and_retires_its_checkpoint() {
    let h = harness(SandboxState::Suspended);
    let operation_id = operation();

    let outcome = h
        .upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
        .await
        .unwrap();

    assert!(matches!(
        outcome,
        RuntimeUpgradeOutcome::Upgraded { start: Some(ref start) } if start.pid == 42
    ));
    assert_eq!(
        h.fake.calls(),
        [
            "exclude",
            "inspect",
            "release fenced",
            "preserve",
            "resolve",
            "boot",
            "retire",
            "discard"
        ]
    );
    let row = h.row();
    assert_eq!(row.state, SandboxState::Started);
    assert_eq!(row.execution_metadata, Some(h.envs.target.clone()));
    assert!(!h.fake.checkpoint_present.load(Ordering::SeqCst));
    assert_eq!(*h.fake.preserved.lock().unwrap(), None);
    let record = h.record(operation_id).unwrap();
    assert_eq!(record.phase, RuntimeUpgradePhase::Retired);
    assert_eq!(record.checkpoint_artifact_id, h.checkpoint_artifact);
    assert!(record.authorization.memory_loss);
    assert_eq!(h.fence(), None);
}

#[tokio::test]
async fn a_stopped_sandbox_upgrade_ends_stopped_when_asked() {
    let h = harness(SandboxState::Stopped);
    let operation_id = operation();

    h.upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Stopped))
        .await
        .unwrap();

    assert_eq!(
        h.fake.calls(),
        [
            "exclude",
            "inspect",
            "release fenced",
            "preserve",
            "resolve",
            "boot",
            "retire",
            "discard",
            "stop"
        ]
    );
    assert_eq!(h.row().state, SandboxState::Stopped);
    assert_eq!(h.row().execution_metadata, Some(h.envs.target.clone()));
}

#[tokio::test]
async fn a_failed_cold_boot_leaves_the_old_pair_intact_and_restorable() {
    for state in [SandboxState::Suspended, SandboxState::Stopped] {
        let h = harness(state);
        *h.fake.boot.lock().unwrap() = Boot::FailAfterWriting;
        let operation_id = operation();

        let error = h
            .upgrader
            .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
            .await
            .unwrap_err();

        assert_eq!(error.code(), tonic::Code::DeadlineExceeded, "{error}");
        assert!(error.message().contains("rolled back"), "{error}");
        assert_eq!(
            h.fake.calls(),
            [
                "exclude",
                "inspect",
                "release fenced",
                "preserve",
                "resolve",
                "boot",
                "restore",
                "discard"
            ],
            "{state}"
        );
        assert_eq!(h.record(operation_id), None);
        h.assert_old_pair_restorable(state);
    }
}

#[tokio::test]
async fn a_failed_boot_of_an_unbound_sandbox_binds_the_execution_its_disk_recorded() {
    let h = harness(SandboxState::Stopped);
    h.db.lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET execution_metadata_json = NULL WHERE id = ?1",
            crate::db::fixture::params![h.sandbox_id],
        )
        .unwrap();
    *h.fake.boot.lock().unwrap() = Boot::FailAfterWriting;

    let error = h
        .upgrader
        .upgrade(h.request(operation(), RuntimeUpgradeTargetState::Started))
        .await
        .unwrap_err();

    assert!(error.message().contains("rolled back"), "{error}");
    // The boot rewrote the disk's record to the target; unbound again, the
    // next cold start would bind that.
    assert_eq!(
        h.row().execution_metadata,
        Some(h.fake.disk.lock().unwrap().execution.clone())
    );
}

#[tokio::test]
async fn a_boot_that_fails_before_claiming_restores_the_untouched_disk() {
    let h = harness(SandboxState::Suspended);
    *h.fake.boot.lock().unwrap() = Boot::FailBeforeClaim;

    let error = h
        .upgrader
        .upgrade(h.request(operation(), RuntimeUpgradeTargetState::Started))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable);
    h.assert_old_pair_restorable(SandboxState::Suspended);
}

#[tokio::test]
async fn a_rollback_waits_for_the_failed_boot_to_settle_and_keeps_the_fence_until_it_does() {
    let h = harness(SandboxState::Suspended);
    *h.fake.cleanup_after.lock().unwrap() = Duration::from_millis(200);
    *h.fake.boot.lock().unwrap() = Boot::FailAfterWriting;
    let short = RuntimeUpgrader::new(h.db.clone(), Arc::clone(&h.fake))
        .with_settle_timing(Duration::from_millis(20), Duration::from_millis(5));
    let operation_id = operation();

    let error = short
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
        .await
        .unwrap_err();

    // The cleanup hadn't returned the sandbox yet: nothing was restored, and
    // the fence still refuses every other activation.
    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    assert!(!h.fake.calls().contains(&"restore"));
    assert_eq!(h.fake.instance(), "disk written by the new boot");
    assert_eq!(
        h.fence(),
        Some((operation_id, RuntimeUpgradePhase::Booting))
    );

    // Once it has, a retry of the same operation rolls back and starts over.
    tokio::time::sleep(Duration::from_millis(300)).await;
    *h.fake.boot.lock().unwrap() = Boot::Succeed;
    h.upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
        .await
        .unwrap();
    assert_eq!(
        h.fake.calls(),
        [
            "exclude",
            "inspect",
            "release fenced",
            "preserve",
            "resolve",
            "boot", // first attempt
            "restore",
            "discard", // rollback on retry
            "exclude",
            "inspect",
            "release fenced",
            "preserve",
            "resolve",
            "boot",
            "retire",
            "discard"
        ]
    );
    assert_eq!(
        h.record(operation_id).unwrap().phase,
        RuntimeUpgradePhase::Retired
    );
}

#[tokio::test]
async fn a_failed_restore_keeps_the_fence_for_a_later_rollback() {
    let h = harness(SandboxState::Stopped);
    *h.fake.boot.lock().unwrap() = Boot::FailAfterWriting;
    h.fake.fail_restore.store(true, Ordering::SeqCst);
    let operation_id = operation();

    let error = h
        .upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    // The only rollback disk is never discarded while the fence stands.
    assert_eq!(*h.fake.preserved.lock().unwrap(), Some("old disk".into()));
    assert_eq!(
        h.fence(),
        Some((operation_id, RuntimeUpgradePhase::Booting))
    );
    assert_eq!(h.row().execution_metadata, Some(h.envs.target.clone()));

    h.fake.fail_restore.store(false, Ordering::SeqCst);
    let record = h.record(operation_id).unwrap();
    assert_eq!(
        h.upgrader.settle(&record, false).await.unwrap(),
        Settled::RolledBack
    );
    h.assert_old_pair_restorable(SandboxState::Stopped);
}

#[tokio::test]
async fn retirement_waits_for_verification_across_a_crash() {
    // Crashed mid-boot: the sandbox came back to its old state, so the
    // upgrade rolls back and never retires the checkpoint.
    let h = harness(SandboxState::Suspended);
    *h.fake.boot.lock().unwrap() = Boot::Hang;
    let operation_id = operation();
    let short = RuntimeUpgrader::new(h.db.clone(), Arc::clone(&h.fake))
        .with_settle_timing(Duration::from_millis(20), Duration::from_millis(5));
    let error = short
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
        .await
        .unwrap_err();
    // The row is still starting, so nothing is restored and the fence holds.
    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    assert!(!h.fake.calls().contains(&"restore"));
    // The interrupted boot's activation is still open; finish it as startup
    // recovery would.
    let episode = crate::db::cleanup_episode(&h.db.lock().unwrap(), &h.sandbox_id)
        .unwrap()
        .unwrap();
    roll_back_activation(&h.db, &h.sandbox_id, SandboxState::Suspended, episode.id);
    let record = h.record(operation_id).unwrap();
    assert_eq!(record.phase, RuntimeUpgradePhase::Booting);
    assert_eq!(
        h.upgrader.settle(&record, false).await.unwrap(),
        Settled::RolledBack
    );
    assert!(!h.fake.calls().contains(&"retire"));
    h.assert_old_pair_restorable(SandboxState::Suspended);

    // Crashed after a verified boot but before recording it: the sandbox is
    // started on the target, which only guest readiness produces, so the
    // upgrade records the verification and then retires.
    let h = harness(SandboxState::Suspended);
    let operation_id = operation();
    h.upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
        .await
        .unwrap();
    h.force_phase(operation_id, RuntimeUpgradePhase::Booting);
    h.fake.checkpoint_present.store(true, Ordering::SeqCst);
    let record = h.record(operation_id).unwrap();
    assert_eq!(
        h.upgrader.settle(&record, false).await.unwrap(),
        Settled::Retired
    );
    assert!(!h.fake.checkpoint_present.load(Ordering::SeqCst));
    assert_eq!(
        h.record(operation_id).unwrap().phase,
        RuntimeUpgradePhase::Retired
    );
}

#[tokio::test]
async fn a_verified_upgrade_whose_retirement_failed_finishes_on_settle() {
    let h = harness(SandboxState::Suspended);
    let operation_id = operation();
    h.upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
        .await
        .unwrap();
    h.force_phase(operation_id, RuntimeUpgradePhase::Verified);
    // A verified upgrade no longer fences the sandbox.
    assert_eq!(h.fence(), None);

    // A retry with the same operation settles it without booting again.
    let before = h.fake.calls().len();
    let outcome = h
        .upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
        .await
        .unwrap();

    assert!(matches!(
        outcome,
        RuntimeUpgradeOutcome::Upgraded { start: None }
    ));
    assert_eq!(h.fake.calls()[before..], ["retire", "discard"]);
    assert_eq!(
        h.record(operation_id).unwrap().phase,
        RuntimeUpgradePhase::Retired
    );
}

#[tokio::test]
async fn an_upgrade_interrupted_before_its_boot_rolls_back_on_settle() {
    for phase in [RuntimeUpgradePhase::Fenced, RuntimeUpgradePhase::Preserved] {
        let h = harness(SandboxState::Suspended);
        let operation_id = crate::db::test_fixtures::fence_runtime_upgrade(
            &h.db.lock().unwrap(),
            &h.sandbox_id,
            reference(&h.envs.target),
            None,
        )
        .operation_id;
        if phase == RuntimeUpgradePhase::Preserved {
            *h.fake.preserved.lock().unwrap() = Some("old disk".into());
            assert!(
                crate::db::advance_runtime_upgrade(
                    &h.db.lock().unwrap(),
                    operation_id,
                    RuntimeUpgradePhase::Fenced,
                    phase
                )
                .unwrap()
            );
        }
        let record = h.record(operation_id).unwrap();

        assert_eq!(
            h.upgrader.settle(&record, false).await.unwrap(),
            Settled::RolledBack
        );

        // No boot ran, so nothing is restored over the untouched disk.
        assert_eq!(h.fake.calls(), ["discard"], "{phase}");
        assert_eq!(h.record(operation_id), None);
        h.assert_old_pair_restorable(SandboxState::Suspended);
    }
}

#[tokio::test]
async fn failures_before_the_boot_roll_back_without_booting() {
    // The preserved copy can't be made.
    let h = harness(SandboxState::Suspended);
    h.fake.fail_preserve.store(true, Ordering::SeqCst);
    let error = h
        .upgrader
        .upgrade(h.request(operation(), RuntimeUpgradeTargetState::Started))
        .await
        .unwrap_err();
    assert!(error.message().contains("preserve the disk"), "{error}");
    assert_eq!(
        h.fake.calls(),
        [
            "exclude",
            "inspect",
            "release fenced",
            "preserve",
            "discard"
        ]
    );
    h.assert_old_pair_restorable(SandboxState::Suspended);

    // The edge is certified, but the disk was written for another kernel
    // than the target boots.
    let h = harness(SandboxState::Suspended);
    h.fake.disk.lock().unwrap().execution = h.envs.incompatible.clone();
    let error = h
        .upgrader
        .upgrade(h.request(operation(), RuntimeUpgradeTargetState::Started))
        .await
        .unwrap_err();
    assert!(error.message().contains("disk boot inputs"), "{error}");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert_eq!(
        h.fake.calls(),
        [
            "exclude",
            "inspect",
            "release fenced",
            "preserve",
            "resolve",
            "discard"
        ]
    );
    h.assert_old_pair_restorable(SandboxState::Suspended);
}

#[tokio::test]
async fn only_a_certified_upgrade_edge_is_followed() {
    // No certified edge leads from the source to a target with another kernel.
    for state in [SandboxState::Suspended, SandboxState::Stopped] {
        let h = harness(state);
        let operation_id = operation();
        let mut request = h.request(operation_id, RuntimeUpgradeTargetState::Started);
        request.target = reference(&h.envs.incompatible);

        let error = h.upgrader.upgrade(request).await.unwrap_err();

        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
        assert!(
            error.message().contains("not a certified upgrade edge"),
            "{error}"
        );
        assert_eq!(
            h.fake.calls(),
            ["exclude", "inspect", "release unfenced"],
            "{state}"
        );
        assert_eq!(h.record(operation_id), None);
        h.assert_old_pair_restorable(state);
    }

    // A historical contract no compiled environment describes has no edges.
    let h = harness(SandboxState::Stopped);
    let custom = ExecutionMetadata::new(
        pplx_space_common_core::types::RuntimeIdentity {
            kind: pplx_space_common_core::types::RuntimeKind::Firecracker,
            version: "1.14.0".parse().unwrap(),
        },
        h.envs.source.guest(),
        h.envs.source.state_format().cloned(),
        vec![],
    )
    .unwrap();
    h.db.lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET execution_metadata_json = NULL WHERE id = ?1",
            crate::db::fixture::params![h.sandbox_id],
        )
        .unwrap();
    h.fake.disk.lock().unwrap().execution = custom;
    let error = h
        .upgrader
        .upgrade(h.request(operation(), RuntimeUpgradeTargetState::Started))
        .await
        .unwrap_err();
    assert!(
        error.message().contains("no compiled environment"),
        "{error}"
    );
    assert!(!h.fake.calls().contains(&"preserve"));
}

#[tokio::test]
async fn a_verified_boot_is_never_rolled_back_whatever_the_sandbox_does_next() {
    // The boot is verified, then retirement fails and the caller goes away.
    // Before anything retries, the user stops or suspends the new execution.
    for (source, after) in [
        (SandboxState::Stopped, SandboxState::Stopped),
        (SandboxState::Suspended, SandboxState::Stopped),
        (SandboxState::Suspended, SandboxState::Suspended),
    ] {
        let h = harness(source);
        h.fake.fail_retire.store(true, Ordering::SeqCst);
        let operation_id = operation();
        let error = h
            .upgrader
            .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
            .await
            .unwrap_err();
        assert!(
            error.message().contains("scripted retirement failure"),
            "{error}"
        );
        assert_eq!(
            h.record(operation_id).unwrap().phase,
            RuntimeUpgradePhase::Verified
        );
        h.db.lock()
            .unwrap()
            .execute(
                "UPDATE sandboxes SET state = ?2 WHERE id = ?1",
                crate::db::fixture::params![h.sandbox_id, after.as_str()],
            )
            .unwrap();
        h.fake.fail_retire.store(false, Ordering::SeqCst);
        let record = h.record(operation_id).unwrap();

        assert_eq!(
            h.upgrader.settle(&record, false).await.unwrap(),
            Settled::Retired,
            "{source} -> {after}"
        );

        assert!(!h.fake.calls().contains(&"restore"), "{source} -> {after}");
        assert_eq!(h.fake.instance(), "disk written by the new boot");
        assert_eq!(h.row().execution_metadata, Some(h.envs.target.clone()));
        assert_eq!(h.fence(), None);
    }
}

#[tokio::test]
async fn an_upgrade_never_stops_an_execution_it_did_not_boot() {
    for reach_target in [true, false] {
        // The boot is verified, retirement fails, and before any retry the
        // user stops the sandbox and starts it again: a new execution.
        let h = harness(SandboxState::Stopped);
        h.fake.fail_retire.store(true, Ordering::SeqCst);
        let operation_id = operation();
        h.upgrader
            .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Stopped))
            .await
            .unwrap_err();
        let record = h.record(operation_id).unwrap();
        assert_eq!(record.phase, RuntimeUpgradePhase::Verified);
        assert_eq!(record.boot_execution_id, h.row().node_execution_id);
        assert!(record.boot_execution_id.is_some());
        h.db.lock()
            .unwrap()
            .execute(
                "UPDATE sandboxes SET state = 'started', node_execution_id = ?2 WHERE id = ?1",
                crate::db::fixture::params![
                    h.sandbox_id,
                    pplx_space_common_core::types::SandboxActivationAttemptId::new().to_string()
                ],
            )
            .unwrap();
        h.fake.fail_retire.store(false, Ordering::SeqCst);
        let before = h.fake.calls().len();

        assert_eq!(
            h.upgrader.settle(&record, reach_target).await.unwrap(),
            Settled::Retired,
            "{reach_target}"
        );

        assert_eq!(
            h.fake.calls()[before..],
            ["retire", "discard"],
            "{reach_target}"
        );
        assert_eq!(h.row().state, SandboxState::Started);
    }
}

#[tokio::test]
async fn a_paused_boot_still_gets_the_stop_it_was_asked_for() {
    for reach_target in [true, false] {
        let h = harness(SandboxState::Stopped);
        h.fake.fail_retire.store(true, Ordering::SeqCst);
        let operation_id = operation();
        // The verified boot's own execution, paused before its stop.
        *h.fake.boot.lock().unwrap() = Boot::Succeed;
        h.upgrader
            .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
            .await
            .unwrap_err();
        h.db.lock()
            .unwrap()
            .execute(
                "UPDATE sandboxes SET state = 'paused' WHERE id = ?1;",
                crate::db::fixture::params![h.sandbox_id],
            )
            .unwrap();
        h.db.lock()
            .unwrap()
            .execute(
                "UPDATE sandbox_runtime_upgrades SET target_state = 'stopped' WHERE operation_id = ?1",
                crate::db::fixture::params![operation_id.to_string()],
            )
            .unwrap();
        h.fake.fail_retire.store(false, Ordering::SeqCst);
        let record = h.record(operation_id).unwrap();
        let before = h.fake.calls().len();

        let settled = h.upgrader.settle(&record, reach_target).await;

        if reach_target {
            assert_eq!(settled.unwrap(), Settled::Retired);
            assert_eq!(h.fake.calls()[before..], ["retire", "discard", "stop"]);
        } else {
            assert_eq!(settled.unwrap_err().code(), tonic::Code::Unavailable);
            assert_eq!(
                h.record(operation_id).unwrap().phase,
                RuntimeUpgradePhase::Verified
            );
        }
    }
}

#[tokio::test]
async fn a_start_that_fails_after_its_guest_is_ready_completes_the_upgrade() {
    let h = harness(SandboxState::Suspended);
    *h.fake.boot.lock().unwrap() = Boot::SucceedThenFail;
    let operation_id = operation();

    let error = h
        .upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
        .await
        .unwrap_err();

    assert!(
        error.message().contains("runtime upgrade completed"),
        "{error}"
    );
    assert!(!h.fake.calls().contains(&"restore"));
    assert_eq!(
        h.record(operation_id).unwrap().phase,
        RuntimeUpgradePhase::Retired
    );
    // A retry confirms it without booting again.
    let before = h.fake.calls().len();
    assert!(matches!(
        h.upgrader
            .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
            .await
            .unwrap(),
        RuntimeUpgradeOutcome::Upgraded { start: None }
    ));
    assert_eq!(h.fake.calls().len(), before);
}

#[tokio::test]
async fn a_sandbox_that_changes_while_it_is_inspected_is_not_fenced() {
    let h = harness(SandboxState::Suspended);
    h.fake.stop_during_inspect.store(true, Ordering::SeqCst);
    let operation_id = operation();

    let error = h
        .upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Aborted, "{error}");
    assert_eq!(h.record(operation_id), None);
    assert_eq!(h.fake.calls(), ["exclude", "inspect", "release unfenced"]);
}

#[tokio::test]
async fn a_sandbox_deleted_during_its_boot_is_reported_as_deleted() {
    let h = harness(SandboxState::Stopped);
    *h.fake.boot.lock().unwrap() = Boot::FailAsDeleted;
    let operation_id = operation();

    let error = h
        .upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
        .await
        .unwrap_err();

    assert!(
        error.message().contains("deleted during the upgrade"),
        "{error}"
    );
    assert_eq!(h.record(operation_id), None);
}

#[tokio::test]
async fn a_failed_boot_auto_lifecycle_then_stops_still_rolls_back() {
    let h = harness(SandboxState::Suspended);
    *h.fake.boot.lock().unwrap() = Boot::FailAfterAutoStop;

    let error = h
        .upgrader
        .upgrade(h.request(operation(), RuntimeUpgradeTargetState::Started))
        .await
        .unwrap_err();

    assert!(error.message().contains("rolled back"), "{error}");
    assert_eq!(h.fake.instance(), "old disk");
    assert_eq!(h.fence(), None);
    let row = h.row();
    assert_eq!(row.state, SandboxState::Stopped);
    assert_eq!(row.execution_metadata, Some(h.envs.source.clone()));
}

#[test]
fn a_refused_stop_is_a_no_op_only_once_the_boot_is_at_rest_or_replaced() {
    use pplx_space_common_core::types::SandboxActivationAttemptId;
    let boot = SandboxActivationAttemptId::new();
    let refusal = Status::aborted("cleanup lifecycle changed from started to pausing");
    let other = Some(SandboxActivationAttemptId::new());
    assert!(!moved_on_or_retry(SandboxState::Started, other, boot, &refusal).unwrap());
    assert!(!moved_on_or_retry(SandboxState::Started, None, boot, &refusal).unwrap());
    // A user stop that won the race leaves nothing to stop, though stopping
    // doesn't clear the execution.
    assert!(!moved_on_or_retry(SandboxState::Stopped, Some(boot), boot, &refusal).unwrap());
    for state in [
        SandboxState::Started,
        SandboxState::Pausing,
        SandboxState::Quarantined,
    ] {
        assert_eq!(
            moved_on_or_retry(state, Some(boot), boot, &refusal)
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable,
            "{state}"
        );
    }
}

#[tokio::test]
async fn a_must_end_stopped_upgrade_waits_out_a_checkpoint_of_its_boot() {
    let h = harness(SandboxState::Stopped);
    let operation_id = operation();
    h.fake.fail_retire.store(true, Ordering::SeqCst);
    h.upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Stopped))
        .await
        .unwrap_err();
    h.fake.fail_retire.store(false, Ordering::SeqCst);
    // A scheduled checkpoint of the boot's execution is in flight; it will
    // return the sandbox to started.
    let set_state = |state: &str| {
        h.db.lock()
            .unwrap()
            .execute(
                "UPDATE sandboxes SET state = ?2 WHERE id = ?1",
                crate::db::fixture::params![h.sandbox_id, state],
            )
            .unwrap();
    };
    set_state("checkpointing");

    let error = h
        .upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Stopped))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    assert_eq!(
        h.fake.phase(operation_id),
        Some(RuntimeUpgradePhase::Verified)
    );
    assert!(!h.fake.calls().contains(&"stop"));

    set_state("started");
    h.upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Stopped))
        .await
        .unwrap();
    assert_eq!(h.row().state, SandboxState::Stopped);
    assert!(h.fake.calls().contains(&"stop"));
}

#[tokio::test]
async fn a_suspended_sandbox_needs_memory_loss_consent() {
    let h = harness(SandboxState::Suspended);
    let operation_id = operation();
    let mut request = h.request(operation_id, RuntimeUpgradeTargetState::Started);
    request.authorization.memory_loss = false;

    let error = h.upgrader.upgrade(request).await.unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("memory loss"), "{error}");
    assert_eq!(h.fake.calls(), ["exclude", "inspect", "release unfenced"]);
    assert_eq!(h.record(operation_id), None);
    h.assert_old_pair_restorable(SandboxState::Suspended);
}

#[tokio::test]
async fn work_already_on_the_target_is_left_alone() {
    let h = harness(SandboxState::Stopped);
    crate::db::upsert_sandbox_execution_metadata(
        &h.db.lock().unwrap(),
        &h.sandbox_id,
        &h.envs.target,
    )
    .unwrap();
    let outcome = h
        .upgrader
        .upgrade(h.request(operation(), RuntimeUpgradeTargetState::Started))
        .await
        .unwrap();
    assert!(matches!(outcome, RuntimeUpgradeOutcome::AlreadyOnTarget));
    assert_eq!(h.fake.calls(), ["exclude", "release unfenced"]);

    let h = harness(SandboxState::Suspended);
    h.fake.disk.lock().unwrap().checkpoint_execution = Some(h.envs.target.clone());
    let outcome = h
        .upgrader
        .upgrade(h.request(operation(), RuntimeUpgradeTargetState::Started))
        .await
        .unwrap();
    assert!(matches!(outcome, RuntimeUpgradeOutcome::AlreadyOnTarget));
    assert_eq!(h.fake.calls(), ["exclude", "inspect", "release unfenced"]);
}

#[tokio::test]
async fn a_retry_must_name_the_same_upgrade() {
    let h = harness(SandboxState::Stopped);
    let operation_id = operation();
    h.upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
        .await
        .unwrap();

    let mut request = h.request(operation_id, RuntimeUpgradeTargetState::Started);
    request.target = reference(&h.envs.incompatible);
    assert_eq!(
        h.upgrader.upgrade(request).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    let mut request = h.request(operation_id, RuntimeUpgradeTargetState::Started);
    request.authorization.id = "another-campaign".into();
    assert_eq!(
        h.upgrader.upgrade(request).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );

    // The same request is answered from the record, without a second boot.
    let before = h.fake.calls().len();
    let outcome = h
        .upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Started))
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        RuntimeUpgradeOutcome::Upgraded { start: None }
    ));
    assert_eq!(h.fake.calls().len(), before);
}

#[tokio::test]
async fn a_second_upgrade_of_a_fenced_sandbox_is_refused() {
    let h = harness(SandboxState::Stopped);
    let first = crate::db::test_fixtures::fence_runtime_upgrade(
        &h.db.lock().unwrap(),
        &h.sandbox_id,
        reference(&h.envs.target),
        None,
    )
    .operation_id;

    let error = h
        .upgrader
        .upgrade(h.request(operation(), RuntimeUpgradeTargetState::Started))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Aborted);
    assert!(error.message().contains(&first.to_string()), "{error}");
    assert!(!h.fake.calls().contains(&"preserve"));

    // Nor is it already on the target once the first has saved its consumer:
    // that upgrade may still roll back.
    crate::db::upsert_sandbox_execution_metadata(
        &h.db.lock().unwrap(),
        &h.sandbox_id,
        &h.envs.target,
    )
    .unwrap();
    let error = h
        .upgrader
        .upgrade(h.request(operation(), RuntimeUpgradeTargetState::Started))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Aborted);
    assert!(error.message().contains(&first.to_string()), "{error}");
}

#[test]
fn the_fence_refusal_is_a_retryable_unavailable() {
    let operation_id = operation();
    let status = begin_activation_status(
        BeginActivationError::RuntimeUpgrade(crate::db::RuntimeUpgradeFenceError::Fenced {
            operation_id,
        }),
        "claim",
    );
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(status.message().contains(&operation_id.to_string()));
    assert_eq!(
        begin_activation_status(
            BeginActivationError::RuntimeUpgrade(crate::db::RuntimeUpgradeFenceError::NotFencing {
                operation_id
            }),
            "claim"
        )
        .code(),
        tonic::Code::Aborted
    );
}

#[test]
fn requests_are_parsed_strictly() {
    use crate::proto::{
        RuntimeUpgradeAuthorization as Authorization, UpgradeRuntimeRequest, WakeTargetState,
    };
    let envs = environments();
    let sandbox_id = SandboxId::new().to_string();
    let valid = || UpgradeRuntimeRequest {
        sandbox_id: sandbox_id.clone(),
        operation_id: uuid::Uuid::now_v7().to_string(),
        target_environment: reference(&envs.target).to_string(),
        authorization: Some(Authorization {
            authorization_id: "block-e0".into(),
            reason: "runtime advisory".into(),
            memory_loss: true,
        }),
        start: Some(StartRequest {
            sandbox_id: sandbox_id.clone(),
            blocking: true,
            ..Default::default()
        }),
        target_state: WakeTargetState::Stopped as i32,
    };
    let parsed = parse_request(valid()).unwrap();
    assert_eq!(parsed.target_state, RuntimeUpgradeTargetState::Stopped);
    assert_eq!(parsed.target, reference(&envs.target));
    assert!(parsed.authorization.memory_loss);

    type Mutation = Box<dyn Fn(&mut UpgradeRuntimeRequest)>;
    let refusals: Vec<(&str, Mutation)> = vec![
        (
            "sandbox id",
            Box::new(|r| r.sandbox_id = "not-a-uuid".into()),
        ),
        ("operation id", Box::new(|r| r.operation_id = "op".into())),
        ("target", Box::new(|r| r.target_environment = "e0".into())),
        ("no authorization", Box::new(|r| r.authorization = None)),
        (
            "empty authorization id",
            Box::new(|r| r.authorization.as_mut().unwrap().authorization_id = " ".into()),
        ),
        (
            "empty reason",
            Box::new(|r| r.authorization.as_mut().unwrap().reason = String::new()),
        ),
        (
            "oversized reason",
            Box::new(|r| r.authorization.as_mut().unwrap().reason = "x".repeat(1025)),
        ),
        (
            "paused target",
            Box::new(|r| r.target_state = WakeTargetState::Paused as i32),
        ),
        ("unknown target", Box::new(|r| r.target_state = 99)),
        ("no start", Box::new(|r| r.start = None)),
        (
            "another sandbox",
            Box::new(|r| r.start.as_mut().unwrap().sandbox_id = SandboxId::new().to_string()),
        ),
        (
            "non-blocking",
            Box::new(|r| r.start.as_mut().unwrap().blocking = false),
        ),
        (
            "checkpoint",
            Box::new(|r| r.start.as_mut().unwrap().checkpoint = Some("suspend".into())),
        ),
        (
            "warm",
            Box::new(|r| r.start.as_mut().unwrap().from_warm = true),
        ),
        (
            "restore as new",
            Box::new(|r| {
                r.start.as_mut().unwrap().restore_artifact_as_new = Some(Default::default())
            }),
        ),
        (
            "storage uri",
            Box::new(|r| {
                r.start.as_mut().unwrap().suspend_restore_storage_uri = Some("s3://b/k".into())
            }),
        ),
        (
            "source node",
            Box::new(|r| {
                r.start.as_mut().unwrap().suspend_restore_source_node = Some("node".into())
            }),
        ),
        (
            "expected artifact",
            Box::new(|r| {
                r.start.as_mut().unwrap().expected_local_artifact_id =
                    Some(LocalArtifactId::new().to_string())
            }),
        ),
        (
            "stopped start",
            Box::new(|r| r.start.as_mut().unwrap().target_state = WakeTargetState::Stopped as i32),
        ),
    ];
    for (case, mutate) in refusals {
        let mut request = valid();
        mutate(&mut request);
        let error = parse_request(request).map(drop).unwrap_err();
        assert_eq!(
            error.code(),
            tonic::Code::InvalidArgument,
            "{case}: {error}"
        );
    }
}

#[tokio::test]
async fn a_replay_of_a_retired_upgrade_leaves_a_later_execution_alone() {
    let h = harness(SandboxState::Stopped);
    let operation_id = operation();
    h.upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Stopped))
        .await
        .unwrap();
    assert_eq!(h.row().state, SandboxState::Stopped);
    // The user starts the upgraded sandbox; then the caller replays.
    h.db.lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET state = 'started' WHERE id = ?1",
            crate::db::fixture::params![h.sandbox_id],
        )
        .unwrap();
    let before = h.fake.calls().len();

    let outcome = h
        .upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Stopped))
        .await
        .unwrap();

    assert!(matches!(
        outcome,
        RuntimeUpgradeOutcome::Upgraded { start: None }
    ));
    assert!(h.fake.calls()[before..].is_empty());
    assert_eq!(h.row().state, SandboxState::Started);
}

#[tokio::test]
async fn a_verified_upgrade_that_must_end_stopped_retires_only_once_stopped() {
    let h = harness(SandboxState::Stopped);
    let operation_id = operation();
    h.upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Stopped))
        .await
        .unwrap();
    // Crash after the verified boot, before its stop: the row is verified and
    // the sandbox started.
    h.db.lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET state = 'started' WHERE id = ?1",
            crate::db::fixture::params![h.sandbox_id],
        )
        .unwrap();
    h.force_phase(operation_id, RuntimeUpgradePhase::Verified);
    let record = h.record(operation_id).unwrap();
    let before = h.fake.calls().len();

    // Recovery can't stop the sandbox: it retires the old checkpoint, then
    // leaves the upgrade verified for the stop.
    let error = h.upgrader.settle(&record, false).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    assert_eq!(h.fake.calls()[before..], ["retire", "discard"]);
    assert_eq!(
        h.record(operation_id).unwrap().phase,
        RuntimeUpgradePhase::Verified
    );
    assert_eq!(
        h.fence(),
        None,
        "a verified upgrade no longer fences the sandbox"
    );

    // The caller's retry stops it, then retires.
    assert_eq!(
        h.upgrader.settle(&record, true).await.unwrap(),
        Settled::Retired
    );
    assert_eq!(
        h.fake.calls()[before..],
        ["retire", "discard", "retire", "discard", "stop"]
    );
    assert_eq!(h.row().state, SandboxState::Stopped);

    // Once stopped by anyone, recovery retires it.
    let h = harness(SandboxState::Stopped);
    let operation_id = operation();
    h.upgrader
        .upgrade(h.request(operation_id, RuntimeUpgradeTargetState::Stopped))
        .await
        .unwrap();
    h.force_phase(operation_id, RuntimeUpgradePhase::Verified);
    let record = h.record(operation_id).unwrap();
    assert_eq!(
        h.upgrader.settle(&record, false).await.unwrap(),
        Settled::Retired
    );
    assert_eq!(h.row().state, SandboxState::Stopped);
}

mod service {
    use super::*;
    use crate::service::{SandboxManagerService, SandboxManagerServiceOptions};
    use pplx_space_common_core::host::linux::identities::users;
    use pplx_space_common_svc::PeerIdentity;

    fn root<T>(message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request
            .extensions_mut()
            .insert(PeerIdentity::new(0, 0, None));
        request
    }

    /// A service whose only sandbox is fenced by a runtime upgrade.
    fn fenced(state: SandboxState) -> (SandboxManagerService, String, RuntimeUpgradeOperationId) {
        let envs = environments();
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, state);
        crate::db::upsert_sandbox_execution_metadata(&conn, &sandbox_id, &envs.source).unwrap();
        let operation_id = crate::db::test_fixtures::fence_runtime_upgrade(
            &conn,
            &sandbox_id,
            reference(&envs.target),
            None,
        )
        .operation_id;
        let service =
            SandboxManagerService::new_with_options(conn, SandboxManagerServiceOptions::for_test());
        (service, sandbox_id, operation_id)
    }

    fn assert_fenced(result: Result<(), Status>, operation_id: RuntimeUpgradeOperationId) {
        let error = result.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
        assert!(
            error.message().contains(&operation_id.to_string()),
            "{error}"
        );
    }

    #[tokio::test]
    async fn the_fence_refuses_disk_mutations_and_export() {
        let (service, sandbox_id, operation_id) = fenced(SandboxState::Stopped);
        assert_fenced(
            service
                .export_sandbox(root(crate::proto::ExportSandboxRequest {
                    sandbox_id: sandbox_id.clone(),
                    use_delta: false,
                }))
                .await
                .map(drop),
            operation_id,
        );
        assert_fenced(
            service
                .rollback(root(crate::proto::RollbackRequest {
                    sandbox_id: sandbox_id.clone(),
                    snapshot_name: "d-1m-20260922T120000Z".into(),
                    ..Default::default()
                }))
                .await
                .map(drop),
            operation_id,
        );
        assert_fenced(
            service
                .delete_snapshots(root(crate::proto::DeleteSnapshotsRequest {
                    sandbox_id: sandbox_id.clone(),
                    snapshot_name: None,
                    all: true,
                }))
                .await
                .map(drop),
            operation_id,
        );
    }

    /// Warm start and checkpoint restore write the checkpoint directory and
    /// the row before their activation claim; the fence refuses them first.
    #[tokio::test]
    async fn the_fence_refuses_restores_that_stage_before_their_claim() {
        for (state, start) in [
            (
                SandboxState::Stopped,
                StartRequest {
                    from_warm: true,
                    ..Default::default()
                },
            ),
            (
                SandboxState::Suspended,
                StartRequest {
                    checkpoint: Some("suspend".into()),
                    ..Default::default()
                },
            ),
        ] {
            let (service, sandbox_id, operation_id) = fenced(state);
            let start = StartRequest {
                sandbox_id: sandbox_id.clone(),
                ..start
            };
            let error = service
                .start_sandbox_with_upgrade_authority(
                    start,
                    false,
                    crate::service::activation_support::IdentityFreeDirectActivationAuthority::LocalProducer,
                    None,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::Unavailable, "{state}: {error}");
            assert!(
                error.message().contains(&operation_id.to_string()),
                "{error}"
            );
            let row = service.lookup_sandbox(&sandbox_id).unwrap();
            assert_eq!(row.state, state, "the row must not move");
            assert_eq!(row.current_local_artifact_id, None);
        }
    }

    /// Until a verified upgrade retires the old checkpoint, the canonical
    /// checkpoint may be the old runtime's, over a disk the boot wrote.
    #[tokio::test]
    async fn a_verified_upgrade_refuses_restores_until_it_retires() {
        for (state, start) in [
            (
                SandboxState::Stopped,
                StartRequest {
                    from_warm: true,
                    ..Default::default()
                },
            ),
            (
                SandboxState::Suspended,
                StartRequest {
                    checkpoint: Some("suspend".into()),
                    ..Default::default()
                },
            ),
        ] {
            let (service, sandbox_id, operation_id) = fenced(state);
            service
                .db_conn()
                .unwrap()
                .execute(
                    "UPDATE sandbox_runtime_upgrades SET phase = 'verified' WHERE operation_id = ?1",
                    crate::db::fixture::params![operation_id.to_string()],
                )
                .unwrap();
            let error = service
                .start_sandbox_with_upgrade_authority(
                    StartRequest {
                        sandbox_id: sandbox_id.clone(),
                        ..start
                    },
                    false,
                    crate::service::activation_support::IdentityFreeDirectActivationAuthority::LocalProducer,
                    None,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::Unavailable, "{state}: {error}");
            assert!(error.message().contains("retiring"), "{error}");
            // A full rollback would restage the checkpoint retirement owes.
            let error = service
                .rollback(root(crate::proto::RollbackRequest {
                    sandbox_id: sandbox_id.clone(),
                    snapshot_name: "f-1m-20260922T120000Z".into(),
                    ..Default::default()
                }))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::Unavailable, "{state}: {error}");
            assert!(error.message().contains("retiring"), "{error}");
        }
    }

    #[test]
    fn users_can_never_reach_the_preserved_copy() {
        assert!(
            crate::service::grpc_spaced_process_snapshots::is_internal_snapshot_name(
                &preserved_disk_name(operation())
            )
        );
    }

    #[tokio::test]
    async fn only_the_upgrade_itself_may_mark_a_start_and_only_a_cold_one() {
        let (service, sandbox_id, operation_id) = fenced(SandboxState::Suspended);
        for start in [
            StartRequest {
                sandbox_id: sandbox_id.clone(),
                checkpoint: Some("suspend".into()),
                ..Default::default()
            },
            StartRequest {
                sandbox_id: sandbox_id.clone(),
                from_warm: true,
                ..Default::default()
            },
        ] {
            let error = service
                .start_sandbox_with_upgrade_authority(
                    start,
                    false,
                    crate::service::activation_support::IdentityFreeDirectActivationAuthority::LocalProducer,
                    Some(operation_id),
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::InvalidArgument, "{error}");
        }
    }

    #[test]
    fn the_upgrade_callers_may_also_start_and_stop() {
        let authorizer = crate::service::activation_support::sandbox_peer_authorizer();
        let identity = |user: pplx_space_common_core::host::linux::identities::LinuxUser| {
            PeerIdentity::new(user.uid, user.primary_group.gid, None)
        };
        let allowed = [
            PeerIdentity::new(0, 0, None),
            identity(users::SPACE_NODEAPI),
        ];
        for peer in allowed {
            for (operation, principals) in [
                (
                    "UpgradeRuntime",
                    crate::service::ALLOW_SANDBOX_RUNTIME_UPGRADE,
                ),
                ("Start", crate::service::ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR),
                ("Stop", crate::service::ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR),
            ] {
                authorizer
                    .authorize_identity(peer.clone(), operation, principals)
                    .unwrap_or_else(|error| panic!("{operation}: {error}"));
            }
        }
        for user in [
            users::SPACE_NODECTL,
            users::SPACE_TMPLMGR,
            users::SPACE_SBXMGR,
        ] {
            let error = authorizer
                .authorize_identity(
                    identity(user),
                    "UpgradeRuntime",
                    crate::service::ALLOW_SANDBOX_RUNTIME_UPGRADE,
                )
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::PermissionDenied);
        }
    }
}
