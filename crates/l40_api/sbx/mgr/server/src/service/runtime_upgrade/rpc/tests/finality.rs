//! An attempt's final outcome across what can come between it and the
//! attempt's retry: an interrupted disk discard and a copy eviction, a change
//! of the attempt finality switch, another operation that moves the sandbox,
//! a request that reuses the operation id, and a sandbox that moves between a
//! refusal and its record.

use super::*;

/// [`node_reaching_the_boot`], whose disk discard after the boot's rollback
/// fails: the preserved disk's delete is refused once the upgrade copied it.
fn node_failing_the_discard(
    target: &ExecutionMetadata,
    calls: &Arc<Mutex<Vec<String>>>,
) -> FakeNodeManager {
    let deleted = calls.clone();
    node_reaching_the_boot(target, calls).unary(
        "DeleteBtrfsSnapshot",
        move |request: DeleteBtrfsSnapshotRequest| {
            let copied = deleted
                .lock()
                .unwrap()
                .iter()
                .any(|call| call.starts_with("create "));
            recording(&deleted, format!("delete {}", request.snapshot_name));
            if copied {
                Err::<DeleteBtrfsSnapshotResponse, _>(Status::unavailable("node-manager went away"))
            } else {
                Err(Status::not_found("absent"))
            }
        },
    )
}

/// A suspended sandbox's pod upgrade, restaged from object storage when
/// `staged`, as kubelet's retry after a copy eviction sends it.
fn suspended_upgrade(
    f: &Fixture,
    target: &ExecutionMetadata,
    operation_id: RuntimeUpgradeOperationId,
    checkpoint: LocalArtifactId,
    staged: bool,
) -> UpgradeRuntimeRequest {
    let mut request = pod_upgrade(
        &f.sandbox_id,
        target,
        operation_id,
        WakeTargetState::Started,
    );
    request.checkpoint_local_artifact_id = Some(checkpoint.to_string());
    if staged {
        request.stage_from = Some(crate::proto::RuntimeUpgradeStageFrom {
            suspend_restore_storage_uri: format!(
                "s3://space-test/suspend/{}/{checkpoint}?region=us-east-1",
                f.sandbox_id
            ),
            suspend_restore_source_node: None,
        });
    }
    request
}

fn service_with_finality(f: &Fixture, finality: bool) -> SandboxManagerService {
    SandboxManagerService {
        runtime_upgrade_attempt_finality: finality,
        ..f.service.clone()
    }
}

fn assert_own_rollback(f: &Fixture, operation_id: RuntimeUpgradeOperationId) {
    let own = report_for_attempt(&f.service, &f.sandbox_id, operation_id, POD_UID);
    assert_eq!(
        own.status,
        RuntimeUpgradeStatus::RolledBack as i32,
        "{own:?}"
    );
    assert_eq!(own.attempt_id.as_deref(), Some(POD_UID));
    assert!(
        own.failure
            .as_deref()
            .is_some_and(|failure| failure.contains("cold boot failed")),
        "{own:?}"
    );
}

/// The rollback is final for its attempt from the moment a caller can read
/// it, before its disk discard: the discard fails, the caller reads the
/// attempt's rollback, the sandbox's copy is evicted, and the attempt's staged
/// retry gets the rollback back without staging or booting, while a caller
/// waiting on the attempt still reads the original failure.
#[tokio::test]
async fn a_rollback_read_before_its_disk_discard_stays_final_after_an_eviction() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (source, _) = environments();
    let target = certified_target(&source);
    let f = fixture(
        SandboxState::Suspended,
        false,
        node_failing_the_discard(&target, &calls),
    );
    let checkpoint = f.adopt_checkpoint();
    let operation_id = operation();

    upgrade(
        &f.service,
        suspended_upgrade(&f, &target, operation_id, checkpoint, false),
    )
    .await
    .expect_err("the harness can't boot a sandbox");
    let unfinished: i64 = f
        .service
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM sandbox_runtime_upgrade_outcomes WHERE operation_id = ?1",
            crate::db::fixture::params![operation_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        unfinished, 0,
        "the rollback's discard didn't finish: {calls:?}"
    );
    assert_own_rollback(&f, operation_id);

    crate::db::delete_sandbox(&f.service.db.lock().unwrap(), &f.sandbox_id).unwrap();
    let disk_calls = calls.lock().unwrap().len();
    let node_calls = f._node.calls().len();
    let retry = upgrade(
        &f.service,
        suspended_upgrade(&f, &target, operation_id, checkpoint, true),
    )
    .await
    .expect_err("the attempt's retry gets its rollback back");

    assert_eq!(retry.code(), tonic::Code::FailedPrecondition, "{retry}");
    assert!(retry.message().contains("cold boot failed"), "{retry}");
    assert_eq!(calls.lock().unwrap().len(), disk_calls, "{calls:?}");
    assert_eq!(f._node.calls().len(), node_calls, "{:?}", f._node.calls());
    assert!(
        f.service.lookup_sandbox(&f.sandbox_id).is_err(),
        "nothing was staged"
    );
    assert_own_rollback(&f, operation_id);
}

/// A rollback with finality off is final for no attempt, then or after the
/// switch is turned on: the attempt's retry under finality on starts over
/// and boots again rather than getting the old rollback back.
#[tokio::test]
async fn a_rollback_with_finality_off_stays_final_for_no_attempt_once_it_is_on() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (source, _) = environments();
    let target = certified_target(&source);
    let f = fixture(
        SandboxState::Suspended,
        false,
        node_reaching_the_boot(&target, &calls),
    );
    let checkpoint = f.adopt_checkpoint();
    let operation_id = operation();
    let request = || suspended_upgrade(&f, &target, operation_id, checkpoint, false);

    let off = upgrade(&service_with_finality(&f, false), request())
        .await
        .expect_err("the harness can't boot a sandbox");
    assert!(off.message().contains("rolled back"), "{off}");
    let named = report_for_attempt(&f.service, &f.sandbox_id, operation_id, POD_UID);
    assert_eq!(
        named.status,
        RuntimeUpgradeStatus::Absent as i32,
        "nothing is final for the attempt: {named:?}"
    );
    assert_eq!(named.attempt_id, None);
    let boots = || {
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.starts_with("create "))
            .count()
    };
    let booted = boots();

    let on = upgrade(&f.service, request())
        .await
        .expect_err("the harness can't boot a sandbox");

    assert!(on.message().contains("rolled back"), "{on}");
    assert!(boots() > booted, "the retry ran again: {calls:?}");
    assert_own_rollback(&f, operation_id);
}

/// A rollback final for its attempt stays final once the switch is turned
/// off: the attempt's caller may have settled on it, so its retry gets it
/// back and never boots, and a caller still waiting on the attempt reads it
/// as that attempt's, so it settles rather than waiting on a retry that's
/// refused.
#[tokio::test]
async fn a_final_rollback_stays_final_once_finality_is_turned_off() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (source, _) = environments();
    let target = certified_target(&source);
    let f = fixture(
        SandboxState::Suspended,
        false,
        node_reaching_the_boot(&target, &calls),
    );
    let checkpoint = f.adopt_checkpoint();
    let operation_id = operation();
    let request = || suspended_upgrade(&f, &target, operation_id, checkpoint, false);
    upgrade(&f.service, request())
        .await
        .expect_err("the harness can't boot a sandbox");
    assert_own_rollback(&f, operation_id);
    let disk_calls = calls.lock().unwrap().len();
    let node_calls = f._node.calls().len();

    let retry = upgrade(&service_with_finality(&f, false), request())
        .await
        .expect_err("the attempt's retry gets its rollback back");

    assert_eq!(retry.code(), tonic::Code::FailedPrecondition, "{retry}");
    assert!(retry.message().contains("cold boot failed"), "{retry}");
    assert_eq!(calls.lock().unwrap().len(), disk_calls, "{calls:?}");
    assert_eq!(f._node.calls().len(), node_calls, "{:?}", f._node.calls());
    // Read from the attempt's own record, not the live report: the sandbox's
    // row, and the operation's report with it, are gone after an eviction.
    crate::db::delete_sandbox(&f.service.db.lock().unwrap(), &f.sandbox_id).unwrap();
    let off = service_with_finality(&f, false);
    let own = report_for_attempt(&off, &f.sandbox_id, operation_id, POD_UID);
    assert_eq!(
        own.status,
        RuntimeUpgradeStatus::RolledBack as i32,
        "{own:?}"
    );
    assert_eq!(own.attempt_id.as_deref(), Some(POD_UID), "{own:?}");
    assert!(
        own.failure
            .as_deref()
            .is_some_and(|failure| failure.contains("cold boot failed")),
        "{own:?}"
    );
}

/// A final rollback holds back only the retries that still name its attempt.
/// A shim from before attempt ids sends the same pod's retry with none, so
/// the node runs it as a new attempt and boots again, with the switch on or
/// off, while the same retry naming the attempt is held back: honoring a
/// rollback already final takes both a sandbox-manager that reads the
/// attempt's record and a shim that still sends its id. The attempt's own
/// record still answers a read that names it.
#[tokio::test]
async fn a_retry_naming_no_attempt_runs_again_after_its_pods_final_rollback() {
    for finality in [true, false] {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (source, _) = environments();
        let target = certified_target(&source);
        let f = fixture(
            SandboxState::Suspended,
            false,
            node_reaching_the_boot(&target, &calls),
        );
        let checkpoint = f.adopt_checkpoint();
        let operation_id = operation();
        let request = || suspended_upgrade(&f, &target, operation_id, checkpoint, false);
        upgrade(&f.service, request())
            .await
            .expect_err("the harness can't boot a sandbox");
        assert_own_rollback(&f, operation_id);
        let boots = || {
            calls
                .lock()
                .unwrap()
                .iter()
                .filter(|call| call.starts_with("create "))
                .count()
        };
        let booted = boots();
        // Naming the pod's attempt, the retry is held back.
        let named = upgrade(&service_with_finality(&f, finality), request())
            .await
            .expect_err("the attempt's rollback is final");
        assert!(
            named.message().contains("only a new attempt retries it"),
            "{named}"
        );
        assert_eq!(boots(), booted, "finality {finality}: {calls:?}");
        // The same pod's retry, as a shim that sends no attempt id sends it,
        // straight to the service: `upgrade` would name the pod's attempt.
        let mut older_shims = request();
        older_shims.attempt_id = None;

        let retry = service_with_finality(&f, finality)
            .upgrade_runtime(internal_request(older_shims))
            .await
            .expect_err("the harness can't boot a sandbox");

        assert!(retry.message().contains("rolled back"), "{retry}");
        assert!(
            boots() > booted,
            "finality {finality}: the retry ran again: {calls:?}"
        );
        assert_own_rollback(&f, operation_id);
    }
}

/// With finality off, an attempt's rollback holds back nothing: once another
/// operation moved the sandbox to the target, the attempt's retry, under the
/// same attempt id, finds it there.
#[tokio::test]
async fn with_finality_off_a_rolled_back_attempt_finds_its_sandbox_on_target_later() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (source, _) = environments();
    let target = certified_target(&source);
    let f = fixture(
        SandboxState::Stopped,
        false,
        node_reaching_the_boot(&target, &calls),
    );
    let service = service_with_finality(&f, false);
    let operation_id = operation();
    let mut request = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation_id,
        WakeTargetState::Stopped,
    );
    request.attempt_id = Some(POD_UID.to_string());
    let rolled_back = upgrade(&service, request.clone())
        .await
        .expect_err("the harness can't boot a sandbox");
    assert!(
        rolled_back.message().contains("rolled back"),
        "{rolled_back}"
    );
    // Another operation moved the sandbox to the target since.
    f.put_on(&target);

    let retried = upgrade(&service, request).await.unwrap();

    assert_eq!(
        retried.outcome,
        RuntimeUpgradeOutcome::AlreadyOnTarget as i32
    );
    assert_eq!(retried.attempt_id, None);
}

/// After the sandbox's row is deleted, its attempts' final outcomes still
/// hold the operation to its request: the same attempt with another
/// authorization or target state, and a fresh attempt for another sandbox,
/// are told they name another operation, and the original attempt still
/// reads its rollback.
#[tokio::test]
async fn a_final_rollback_holds_its_operation_to_its_request_after_an_eviction() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (source, _) = environments();
    let target = certified_target(&source);
    let f = fixture(
        SandboxState::Suspended,
        false,
        node_reaching_the_boot(&target, &calls),
    );
    let checkpoint = f.adopt_checkpoint();
    let operation_id = operation();
    upgrade(
        &f.service,
        suspended_upgrade(&f, &target, operation_id, checkpoint, false),
    )
    .await
    .expect_err("the harness can't boot a sandbox");
    crate::db::delete_sandbox(&f.service.db.lock().unwrap(), &f.sandbox_id).unwrap();
    let disk_calls = calls.lock().unwrap().len();

    let mut reauthorized = suspended_upgrade(&f, &target, operation_id, checkpoint, true);
    reauthorized
        .authorization
        .as_mut()
        .unwrap()
        .authorization_id = "another-authorization".into();
    let mut restated = suspended_upgrade(&f, &target, operation_id, checkpoint, true);
    restated.target_state = WakeTargetState::Stopped as i32;
    let elsewhere = SandboxId::new().to_string();
    {
        let db = f.service.db.lock().unwrap();
        crate::db::fixture::insert_test_sandbox(&db, &elsewhere, SandboxState::Stopped);
        crate::db::upsert_sandbox_execution_metadata(&db, &elsewhere, &f.source).unwrap();
    }
    // The operation's target and authorization, so only the sandbox differs.
    let mut another = pod_upgrade(&elsewhere, &target, operation_id, WakeTargetState::Started);
    another.attempt_id = Some(OTHER_POD_UID.to_string());
    for request in [reauthorized, restated, another] {
        let refused = upgrade(&f.service, request).await.unwrap_err();
        assert_eq!(refused.code(), tonic::Code::InvalidArgument, "{refused}");
    }
    assert_eq!(calls.lock().unwrap().len(), disk_calls, "{calls:?}");
    let read = f
        .service
        .get_runtime_upgrade(&internal_request(GetRuntimeUpgradeRequest {
            sandbox_id: elsewhere.clone(),
            operation_id: operation_id.to_string(),
            attempt_id: Some(OTHER_POD_UID.to_string()),
        }))
        .unwrap_err();
    assert_eq!(read.code(), tonic::Code::InvalidArgument, "{read}");
    assert_eq!(
        report(&f.service, &elsewhere, operation_id).status,
        RuntimeUpgradeStatus::Absent as i32,
        "the other sandbox holds no report of the operation"
    );
    assert_own_rollback(&f, operation_id);
}

/// An activation refused because another pod's VM runs the sandbox is
/// recorded only while the sandbox still is as that refusal read it: one
/// that stopped since is at rest, unclaimed and already on the target, so the
/// attempt's retry would now start it, and a record would answer the retry
/// with the old refusal instead.
#[tokio::test]
async fn a_refused_activation_is_recorded_only_on_the_sandbox_it_was_decided_on() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let operation_id = operation();
    f.put_on(&target);
    assert!(
        crate::db::record_runtime_upgrade_already_on_target(
            &f.service.db.lock().unwrap(),
            &crate::db::RuntimeUpgradeRequestIdentity {
                operation_id,
                sandbox_id: f.sandbox_id.clone(),
                target_environment: target.environment().unwrap(),
                target_state: crate::db::RuntimeUpgradeTargetState::Started,
                authorization_id: authorization().authorization_id,
            },
            Some(OTHER_POD_UID),
        )
        .unwrap()
    );
    run_as(&f, &pod_identity(&f.sandbox_id, OTHER_POD_UID, 4));
    let request = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation_id,
        WakeTargetState::Started,
    );
    let start = request.start.clone().unwrap();
    let activate = || {
        f.service.activate_once(
            tonic::metadata::MetadataMap::new(),
            tonic::Extensions::new(),
            operation_id,
            start.clone(),
            None,
        )
    };
    let recorded = || {
        crate::db::read::runtime_upgrade_attempt_outcome(
            &f.service.db.lock().unwrap(),
            operation_id,
            POD_UID,
        )
        .unwrap()
    };
    let decided = activate()
        .await
        .expect_err("another pod's VM runs the sandbox");
    assert!(
        decided.as_final().is_some(),
        "another pod's execution is a final refusal: {decided:?}"
    );
    let stop = || {
        f.service
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
                crate::db::fixture::params![f.sandbox_id],
            )
            .unwrap();
    };
    stop();

    f.service
        .record_refused_attempt(operation_id, POD_UID, &start, &decided);
    assert!(
        recorded().is_none(),
        "the sandbox stopped since the refusal"
    );

    // Decided on the sandbox as it is, the same refusal is recorded.
    run_as(&f, &pod_identity(&f.sandbox_id, OTHER_POD_UID, 4));
    let decided = activate()
        .await
        .expect_err("another pod's VM runs the sandbox");
    f.service
        .record_refused_attempt(operation_id, POD_UID, &start, &decided);
    assert_eq!(
        recorded().map(|outcome| outcome.outcome),
        Some(crate::db::RecordedAttemptOutcome::ActivationRefused)
    );
}

/// The same for a replay of a finished upgrade that another pod's VM runs:
/// its refusal is recorded only while the sandbox is as the replay read it.
/// One that stopped since is at rest on the target, so the attempt's retry
/// would now start it.
#[tokio::test]
async fn a_refused_replay_is_recorded_only_on_the_sandbox_it_was_decided_on() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let operation_id = operation();
    retire(
        &f,
        &target,
        operation_id,
        crate::db::RuntimeUpgradeTargetState::Started,
    );
    let service = f.live_service();
    let request = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation_id,
        WakeTargetState::Started,
    );
    let start = request.start.clone().unwrap();
    let replay = || async {
        run_as(&f, &pod_identity(&f.sandbox_id, OTHER_POD_UID, 4));
        Box::pin(service.upgrade_runtime_locked(
            tonic::metadata::MetadataMap::new(),
            tonic::Extensions::new(),
            crate::service::runtime_upgrade::parse_request(request.clone()).unwrap(),
            None,
        ))
        .await
        .expect_err("another pod's VM runs the sandbox")
    };
    let recorded = || {
        crate::db::read::runtime_upgrade_attempt_outcome(
            &f.service.db.lock().unwrap(),
            operation_id,
            POD_UID,
        )
        .unwrap()
    };
    let decided = replay().await;
    assert!(
        decided.as_final().is_some(),
        "another pod's execution is a final refusal: {decided:?}"
    );
    f.service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
            crate::db::fixture::params![f.sandbox_id],
        )
        .unwrap();

    service.record_refused_attempt(operation_id, POD_UID, &start, &decided);
    assert!(
        recorded().is_none(),
        "the sandbox stopped since the refusal"
    );

    // Decided on the sandbox as it is, the same refusal is recorded.
    let decided = replay().await;
    service.record_refused_attempt(operation_id, POD_UID, &start, &decided);
    assert_eq!(
        recorded().map(|outcome| outcome.outcome),
        Some(crate::db::RecordedAttemptOutcome::ActivationRefused)
    );
}

/// Recovery's rollback of an attempt interrupted after its fence is final for
/// that attempt the same way an RPC's is: a caller waiting on the attempt reads
/// the rollback, and the attempt's retry gets it back without fencing again.
#[tokio::test]
async fn a_rollback_made_by_recovery_is_final_for_its_attempt() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (source, _) = environments();
    let target = certified_target(&source);
    let f = fixture(
        SandboxState::Stopped,
        false,
        node_reaching_the_boot(&target, &calls),
    );
    let operation_id = operation();
    {
        let db = f.service.db.lock().unwrap();
        let sandbox = crate::db::read::sandbox(&db, &f.sandbox_id)
            .unwrap()
            .unwrap();
        let begun = crate::db::begin_runtime_upgrade(
            &db,
            crate::db::NewRuntimeUpgrade {
                operation_id,
                sandbox_id: &f.sandbox_id,
                observed: crate::db::ObservedSandbox::of(&sandbox),
                target_environment: target.environment().unwrap(),
                target_state: crate::db::RuntimeUpgradeTargetState::Started,
                authorization: &crate::db::RuntimeUpgradeAuthorization {
                    id: authorization().authorization_id,
                    reason: authorization().reason,
                    memory_loss: authorization().memory_loss,
                },
                checkpoint_dir_identity: None,
                previous_execution: &f.source,
                attempt_id: Some(POD_UID),
            },
        )
        .unwrap();
        assert!(
            matches!(begun, crate::db::BeginRuntimeUpgrade::Fenced(_)),
            "{begun:?}"
        );
    }

    f.service.recover_runtime_upgrades().await;

    let own = report_for_attempt(&f.service, &f.sandbox_id, operation_id, POD_UID);
    assert_eq!(
        own.status,
        RuntimeUpgradeStatus::RolledBack as i32,
        "{own:?}"
    );
    assert_eq!(own.attempt_id.as_deref(), Some(POD_UID));
    let node_calls = f._node.calls().len();
    let retry = upgrade(
        &f.service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Started,
        ),
    )
    .await
    .expect_err("the attempt was rolled back");
    assert_eq!(retry.code(), tonic::Code::FailedPrecondition, "{retry}");
    assert_eq!(f._node.calls().len(), node_calls, "the retry ran again");
}

/// An activation refused while finality is on stays final once it's turned
/// off: the attempt's retry gets the recorded refusal back, a precondition
/// only a new attempt clears, rather than being refused for naming an
/// attempt the node no longer reads or being answered afresh.
#[tokio::test]
async fn a_refused_activation_stays_final_once_finality_is_turned_off() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);
    run_as(&f, &pod_identity(&f.sandbox_id, OTHER_POD_UID, 4));
    let operation_id = operation();
    let request = || {
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Started,
        )
    };
    let refused = upgrade(&f.live_service(), request()).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
    // The sandbox runs this pod's execution now, so only the recorded
    // refusal holds its retry back.
    run_as(&f, &pod_identity(&f.sandbox_id, POD_UID, 4));
    let off = SandboxManagerService {
        runtime_upgrade_attempt_finality: false,
        ..f.live_service()
    };

    let retried = upgrade(&off, request()).await.unwrap_err();

    assert_eq!(retried.code(), tonic::Code::FailedPrecondition, "{retried}");
    assert!(
        retried.message().contains("only a new attempt retries it")
            && retried.message().contains(refused.message()),
        "{retried}"
    );
    f.assert_no_node_calls();
}

/// An attempt that found its sandbox on the target while finality was off
/// records no attempt id, so once finality is on, neither the operation's
/// report nor one that names the attempt reads the outcome as that
/// attempt's, and a replay keeps it that way.
#[tokio::test]
async fn already_on_target_with_finality_off_names_no_attempt_once_it_is_on() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);
    let operation_id = operation();
    let request = || {
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Stopped,
        )
    };
    let found = upgrade(&service_with_finality(&f, false), request())
        .await
        .unwrap();
    assert_eq!(found.outcome, RuntimeUpgradeOutcome::AlreadyOnTarget as i32);

    let on = service_with_finality(&f, true);
    for reported in [
        report(&on, &f.sandbox_id, operation_id),
        report_for_attempt(&on, &f.sandbox_id, operation_id, POD_UID),
    ] {
        assert_eq!(
            reported.status,
            RuntimeUpgradeStatus::AlreadyOnTarget as i32,
            "{reported:?}"
        );
        assert_eq!(reported.attempt_id, None, "{reported:?}");
    }
    let replayed = upgrade(&on, request()).await.unwrap();
    assert_eq!(
        replayed.outcome,
        RuntimeUpgradeOutcome::AlreadyOnTarget as i32
    );
    assert_eq!(report(&on, &f.sandbox_id, operation_id).attempt_id, None);
    f.assert_no_node_calls();
}

/// A suspended sandbox's attempt is refused as its disk's inspection would
/// refuse it, whichever read finds the problem first: with the staged
/// checkpoint switch on, its absent unencrypted rootfs is found while its
/// staged disk is finished, and that refusal is final for the attempt as the
/// inspection's is.
#[tokio::test]
async fn an_absent_rootfs_found_finishing_a_staged_disk_is_final_for_the_attempt() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let checkpoint = f.adopt_checkpoint();
    f.write_sidecar(checkpoint_sidecar(&f, false, Some(checkpoint)));
    std::fs::remove_file(
        f.service
            .cache_root()
            .join(&f.sandbox_id)
            .join("instance/rootfs.ext4"),
    )
    .unwrap();
    let staged = SandboxManagerService {
        staged_checkpoint_disk: true,
        ..f.service.clone()
    };
    let operation_id = operation();

    let refused = upgrade(
        &staged,
        suspended_upgrade(&f, &target, operation_id, checkpoint, false),
    )
    .await
    .unwrap_err();

    assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
    assert!(refused.message().contains("is absent"), "{refused}");
    let own = report_for_attempt(&f.service, &f.sandbox_id, operation_id, POD_UID);
    assert_eq!(
        consumer_action(Ok(own.clone()), POD_UID),
        ConsumerAction::Release,
        "{own:?}"
    );
}

/// A suspended sandbox's checkpoint metadata that reads but doesn't parse is
/// found at the first read of the checkpoint, before the disk is inspected,
/// and is final for the attempt there as the inspection's refusal of it is.
#[tokio::test]
async fn checkpoint_metadata_that_doesnt_parse_is_final_at_the_first_read() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let checkpoint = f.adopt_checkpoint();
    std::fs::write(f.checkpoint().join("runtime.json"), b"not runtime metadata").unwrap();
    let operation_id = operation();

    let refused = upgrade(
        &f.service,
        suspended_upgrade(&f, &target, operation_id, checkpoint, false),
    )
    .await
    .unwrap_err();

    assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
    let own = report_for_attempt(&f.service, &f.sandbox_id, operation_id, POD_UID);
    assert_eq!(
        consumer_action(Ok(own.clone()), POD_UID),
        ConsumerAction::Release,
        "{own:?}"
    );
}
