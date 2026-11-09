use super::*;
use pplx_space_common_core::types::SandboxActivationAttemptId;

#[tokio::test]
async fn completed_restart_receipt_survives_later_lifecycle_changes() {
    use crate::service::activation_support::internal_request;
    let conn = crate::db::test_conn();
    let sandbox = SandboxId::new();
    let id = sandbox.to_string();
    crate::db::fixture::insert_test_sandbox(&conn, &id, SandboxState::Stopped);
    let episode = crate::db::test_fixtures::state_write_fixtures::begin_cold_start(&conn, &id);
    assert!(crate::db::mark_start_succeeded(&conn, &id, Some(episode.id)).unwrap());
    let execution = crate::db::read::sandbox(&conn, &id)
        .unwrap()
        .unwrap()
        .node_execution_id
        .unwrap();
    let intent = Intent {
        operation: Uuid::now_v7(),
        sandbox,
        execution,
        action: Action::Restart,
        force: false,
        configuration_revision: 1,
    };
    let Admission::Accepted(receipt) = store::admit(
        &conn,
        intent.clone(),
        jiff::Timestamp::now().as_second() + 360,
    )
    .unwrap() else {
        panic!("fixture admission refused");
    };
    assert!(store::settle(&conn, &receipt, Outcome::Restarted, None).unwrap());
    let receipt = crate::db::read::guest_update(&conn, intent.operation)
        .unwrap()
        .unwrap();
    assert!(store::advance(&conn, &receipt, Phase::Complete, None).unwrap());
    crate::db::test_fixtures::state_write_fixtures::force_state(&conn, &id, SandboxState::Paused);
    let service = SandboxManagerService::for_test(conn);
    let request = |force| {
        internal_request(crate::proto::RestartGuestRequest {
            sandbox_id: id.clone(),
            operation_id: intent.operation.to_string(),
            expected_execution_id: execution.to_string(),
            force,
        })
    };
    let receipt = service
        .restart_guest_rpc(request(false))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(receipt.outcome.as_deref(), Some("restarted"));
    assert_eq!(
        service
            .restart_guest_rpc(request(true))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

#[test]
fn journal_outcome_matrix_checks_operation_execution_and_candidate_identity() {
    let operation = Uuid::now_v7();
    let execution = SandboxActivationAttemptId::new();
    let committed = model::Selection {
        slot: model::Slot::A,
        digest: model::ArtifactDigest::of_bytes(b"old"),
    };
    let target = model::Selection {
        slot: model::Slot::B,
        digest: model::ArtifactDigest::of_bytes(b"new"),
    };
    let attempt = model::Attempt {
        operation,
        execution: execution.to_string().parse().unwrap(),
        boot: Uuid::now_v7(),
        incarnation: Uuid::now_v7(),
        configuration_revision: 1,
        target: target.clone(),
        confirmation_deadline_ms: 30_000,
    };
    let record = Record {
        intent: Intent {
            operation,
            sandbox: SandboxId::new(),
            execution,
            action: Action::Apply {
                promotion: Box::new(model::policy::Promotion {
                    channel: "stable".into(),
                    target_version: "0.8.1".parse().unwrap(),
                    guest_update_required_since: "0.8.1".parse().unwrap(),
                    intent: pplx_space_spaced_update::policy::SelectionIntent::Upgrade,
                    target: target.digest,
                    minimum_host: "0.8.1".parse().unwrap(),
                    maximum_guest: "0.8.1".parse().unwrap(),
                }),
                authorization: Uuid::now_v7(),
            },
            force: false,
            configuration_revision: 1,
        },
        phase: Phase::Activating,
        prior: None,
        observed: None,
        deadline: i64::MAX,
        outcome: None,
    };
    let mut seen = wire::Status {
        boot_id: attempt.boot.to_string(),
        incarnation: attempt.incarnation.to_string(),
        running: Some(committed.clone().into()),
        committed: Some(committed.into()),
        attempt: Some(attempt.clone().into()),
        ..Default::default()
    };
    for (phase, progress, outcome) in [
        (
            wire::Phase::Staged,
            wire::TrialProgress::Unspecified,
            Decision::Arm,
        ),
        (
            wire::Phase::Armed,
            wire::TrialProgress::Starting,
            Decision::Wait,
        ),
        (
            wire::Phase::Trial,
            wire::TrialProgress::AwaitingHost,
            Decision::Confirm,
        ),
        (
            wire::Phase::RolledBack,
            wire::TrialProgress::Starting,
            Decision::Settle(Outcome::CandidateFailed),
        ),
        (
            wire::Phase::RolledBack,
            wire::TrialProgress::AwaitingHost,
            Decision::Settle(Outcome::AwaitingHost),
        ),
    ] {
        seen.phase = phase as i32;
        seen.progress = progress as i32;
        assert_eq!(maintenance_decision(&record, &seen).unwrap(), outcome);
        let mut foreign = seen.clone();
        foreign.attempt.as_mut().unwrap().execution_id = Uuid::now_v7().to_string();
        let Err(_) = maintenance_decision(&record, &foreign) else {
            panic!("operation must be rejected");
        };
    }
    // A durable abort on the original daemon is a deferral, not a failed
    // candidate. A restarted generation cannot support an allowance refund.
    let mut unstarted = record.clone();
    seen.phase = wire::Phase::RolledBack as i32;
    seen.progress = wire::TrialProgress::Starting as i32;
    unstarted.prior = Some(identity(&seen).unwrap());
    assert_eq!(
        maintenance_decision(&unstarted, &seen).unwrap(),
        Decision::Settle(Outcome::Deferred)
    );
    let mut restarted = seen.clone();
    restarted.incarnation = Uuid::now_v7().to_string();
    assert_eq!(
        maintenance_decision(&unstarted, &restarted).unwrap(),
        Decision::Settle(Outcome::CandidateFailed)
    );
    seen.phase = wire::Phase::Committed as i32;
    let Err(_) = maintenance_decision(&record, &seen) else {
        panic!("operation must be rejected");
    };
    seen.committed = Some(target.clone().into());
    assert_eq!(
        maintenance_decision(&record, &seen).unwrap(),
        Decision::Settle(Outcome::Committed)
    );
    seen.phase = wire::Phase::Trial as i32;
    seen.incarnation = Uuid::now_v7().to_string();
    let Err(_) = maintenance_decision(&record, &seen) else {
        panic!("operation must be rejected");
    };
    seen.incarnation = attempt.incarnation.to_string();
    seen.phase = wire::Phase::RolledBack as i32;
    seen.committed = Some(
        model::Selection {
            slot: model::Slot::A,
            digest: model::ArtifactDigest::of_bytes(b"old"),
        }
        .into(),
    );
    seen.running = Some(target.into());
    assert_eq!(
        maintenance_decision(&record, &seen).unwrap(),
        Decision::Wait
    );
    // A reconciled committed executable can outlive the manager's receipt
    // (for example after journal fallback). Retire stale authority instead of
    // refusing every later lifecycle operation.
    seen.running = seen.committed.clone();
    seen.attempt = None;
    seen.phase = wire::Phase::Stable as i32;
    assert_eq!(
        maintenance_decision(&record, &seen).unwrap(),
        Decision::Settle(Outcome::RecoveryRequired)
    );
}

#[test]
fn lifecycle_shortening_compares_against_the_fixed_activation_deadline() {
    let finish_by = 100 + ACTIVATION_BUDGET;
    for (due, expected) in [
        (None, false),
        (Some(400), false),
        (Some(340), true),
        (Some(120), true),
    ] {
        // Advancing the clock alone must not move the admission boundary.
        assert_eq!(lifecycle_preempts(due, finish_by), expected);
    }
}

#[tokio::test]
async fn lifecycle_waiter_cancels_staging_without_waiting_for_transfer_deadline() {
    let coordinator = crate::auto_lifecycle::AutoLifecycleCoordinator::default();
    let artifact = coordinator.artifact_lock("test-capture");
    let mut priority = artifact.subscribe_capture_requests();
    let owner = artifact.try_lock().unwrap();
    let mut waiting = Box::pin(artifact.lock_for_capture());
    assert!(futures_util::poll!(&mut waiting).is_pending());
    let outcome = yield_staging_to_capture::<()>(&mut priority, std::future::pending());
    tokio::pin!(outcome);
    let std::task::Poll::Ready(result) = futures_util::poll!(&mut outcome) else {
        panic!("capture is still waiting for the transfer deadline");
    };
    assert_eq!(result.unwrap_err().code(), tonic::Code::Aborted);
    drop(owner);
    let capture = waiting.await;
    artifact.try_lock().unwrap_err();
    drop(capture);
    artifact.try_lock().unwrap();
}

#[tokio::test(start_paused = true)]
async fn capture_excludes_new_admission_and_invalid_execution_never_probes() {
    use crate::service::activation_support::internal_request;
    let conn = crate::db::test_conn();
    let sandbox = SandboxId::new();
    let id = sandbox.to_string();
    crate::db::fixture::insert_test_sandbox(&conn, &id, SandboxState::Stopped);
    let episode = crate::db::test_fixtures::state_write_fixtures::begin_cold_start(&conn, &id);
    assert!(crate::db::mark_start_succeeded(&conn, &id, Some(episode.id)).unwrap());
    let execution = crate::db::read::sandbox(&conn, &id)
        .unwrap()
        .unwrap()
        .node_execution_id
        .unwrap();
    let service = SandboxManagerService::for_test(conn);
    let intent = Intent {
        operation: Uuid::now_v7(),
        sandbox,
        execution,
        action: Action::Restart,
        force: false,
        configuration_revision: 1,
    };
    let capture = service
        .auto_lifecycle
        .artifact_lock(&id)
        .lock_for_capture()
        .await;
    assert_eq!(
        service
            .admit_guest_update(intent.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Aborted
    );
    drop(capture);
    let request = internal_request(crate::proto::RestartGuestRequest {
        sandbox_id: id.clone(),
        operation_id: Uuid::now_v7().to_string(),
        expected_execution_id: "invalid".into(),
        force: false,
    });
    assert_eq!(
        service.restart_guest_rpc(request).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    let request = internal_request(crate::proto::ApplyGuestUpdateRequest {
        sandbox_id: id.clone(),
        operation_id: Uuid::now_v7().to_string(),
        expected_execution_id: "invalid".into(),
        force: false,
        promotion: None,
    });
    assert!(
        service
            .apply_guest_update_rpc(request)
            .await
            .unwrap_err()
            .message()
            .contains("expected_execution_id")
    );
    // A brief read must wait, then admit, without granting a capture race.
    service.shutdown_token.cancel();
    let reader = service.auto_lifecycle.artifact_lock(&id).lock().await;
    let mut admission = Box::pin(service.admit_guest_update(intent));
    assert!(futures_util::poll!(&mut admission).is_pending());
    drop(reader);
    assert_eq!(
        admission.await.unwrap().into_inner().phase,
        crate::proto::GuestUpdateOperationPhase::Pending as i32
    );
    service.task_tracker.close();
    service.task_tracker.wait().await;
}
