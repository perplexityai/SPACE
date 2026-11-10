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
    let mut pending = record.clone();
    pending.phase = Phase::Pending;
    for held in [false, true] {
        for belongs in [false, true] {
            let mut observed = wire::Status {
                customer_admission_held: held,
                ..Default::default()
            };
            assert_eq!(pending_guest_untouched(&pending, &observed), !held);
            let mut fence: wire::Attempt = attempt.clone().into();
            if !belongs {
                fence.operation_id = Uuid::now_v7().to_string();
            }
            observed.attempt = Some(fence);
            assert_eq!(
                pending_guest_untouched(&pending, &observed),
                !held && !belongs
            );
            assert!(!pending_guest_untouched(&record, &observed));
        }
    }
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
    // A spontaneous restart before delivery satisfies a restart operation,
    // even when an earlier apply still owns the stable guest journal.
    let mut restart = record.clone();
    restart.intent.action = Action::Restart;
    restart.intent.operation = Uuid::now_v7();
    restart.deadline = 0;
    let mut stable = seen.clone();
    stable.running = stable.committed.clone();
    restart.prior = Some(identity(&stable).unwrap());
    stable.incarnation = Uuid::now_v7().to_string();
    for phase in [wire::Phase::Committed, wire::Phase::RolledBack] {
        stable.phase = phase as i32;
        assert_eq!(
            maintenance_decision(&restart, &stable).unwrap(),
            Decision::Settle(Outcome::Restarted)
        );
    }
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
    let finish_by = 100 + ACTIVATION_BUDGET.as_secs();
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

#[test]
fn confirmation_errors_preserve_transport_reconciliation() {
    for (code, terminal) in [
        (tonic::Code::Ok, false),
        (tonic::Code::Cancelled, false),
        (tonic::Code::Unknown, false),
        (tonic::Code::InvalidArgument, true),
        (tonic::Code::DeadlineExceeded, false),
        (tonic::Code::NotFound, false),
        (tonic::Code::AlreadyExists, false),
        (tonic::Code::PermissionDenied, true),
        (tonic::Code::ResourceExhausted, false),
        (tonic::Code::FailedPrecondition, true),
        (tonic::Code::Aborted, false),
        (tonic::Code::OutOfRange, false),
        (tonic::Code::Unimplemented, false),
        (tonic::Code::Internal, false),
        (tonic::Code::Unavailable, false),
        (tonic::Code::DataLoss, false),
        (tonic::Code::Unauthenticated, true),
    ] {
        assert_eq!(confirmation_must_settle(code), terminal);
    }
}

#[tokio::test]
async fn untouched_pending_receipt_settles_without_a_host_connection() {
    for outcome in [
        Outcome::Deferred,
        Outcome::Cancelled,
        Outcome::NotRequired,
        Outcome::Committed,
    ] {
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
        let operation = Uuid::now_v7();
        let intent = Intent {
            operation,
            sandbox,
            execution,
            action: Action::Restart,
            force: false,
            configuration_revision: 1,
        };
        let Admission::Accepted(receipt) = store::admit(&conn, intent, i64::MAX).unwrap() else {
            panic!("fixture refused");
        };
        let service = SandboxManagerService::for_test(conn);
        let selection: wire::Selection = model::Selection {
            slot: model::Slot::A,
            digest: model::ArtifactDigest::of_bytes(b"committed"),
        }
        .into();
        let seen = wire::Status {
            boot_id: Uuid::now_v7().to_string(),
            incarnation: Uuid::now_v7().to_string(),
            committed: Some(selection.clone()),
            running: Some(selection),
            phase: wire::Phase::Stable as i32,
            ..Default::default()
        };
        // No listener exists: any Hold/Configure/Release fails this test.
        let mut client = Client::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        );
        service
            .finish_pending_guest_update(*receipt, outcome, seen, &mut client)
            .await
            .unwrap();
        let completed = service.update_record(operation).unwrap();
        assert_eq!(completed.phase, Phase::Complete);
        assert_eq!(completed.outcome, Some(outcome));
    }
}

#[tokio::test]
async fn lifecycle_retires_old_execution_without_waiting_for_the_node_permit() {
    let conn = crate::db::test_conn();
    let sandbox = SandboxId::new();
    let id = sandbox.to_string();
    crate::db::fixture::insert_test_sandbox(&conn, &id, SandboxState::Stopped);
    let episode = crate::db::test_fixtures::state_write_fixtures::begin_cold_start(&conn, &id);
    assert!(crate::db::mark_start_succeeded(&conn, &id, Some(episode.id)).unwrap());
    let intent = Intent {
        operation: Uuid::now_v7(),
        sandbox,
        execution: crate::db::read::sandbox(&conn, &id)
            .unwrap()
            .unwrap()
            .node_execution_id
            .unwrap(),
        action: Action::Restart,
        force: false,
        configuration_revision: 1,
    };
    assert!(matches!(
        store::admit(&conn, intent.clone(), i64::MAX).unwrap(),
        Admission::Accepted(_)
    ));
    crate::db::test_fixtures::state_write_fixtures::force_state(&conn, &id, SandboxState::Stopped);
    let next = crate::db::test_fixtures::state_write_fixtures::begin_cold_start(&conn, &id);
    assert!(crate::db::mark_start_succeeded(&conn, &id, Some(next.id)).unwrap());
    let service = SandboxManagerService::for_test(conn);
    let _permit = service
        .guest_update_sem
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    service
        .settle_guest_update_for_lifecycle(&id)
        .await
        .unwrap();
    assert_eq!(
        service.update_record(intent.operation).unwrap().outcome,
        Some(Outcome::Superseded)
    );
}

#[test]
fn malformed_promotion_fields_are_invalid_arguments() {
    for field in 0..5 {
        let mut value = wire::Promotion {
            channel: "stable".into(),
            target_version: "0.8.1".into(),
            guest_update_required_since: "0.8.1".into(),
            target_digest: model::ArtifactDigest::of_bytes(b"candidate").to_string(),
            minimum_host: "0.8.1".into(),
            maximum_guest: "0.8.1".into(),
            intent: wire::SelectionIntent::Upgrade as i32,
        };
        *match field {
            0 => &mut value.target_version,
            1 => &mut value.guest_update_required_since,
            2 => &mut value.target_digest,
            3 => &mut value.minimum_host,
            _ => &mut value.maximum_guest,
        } = "invalid".into();
        assert_eq!(
            policy::promotion(value).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[test]
fn fresh_authority_requires_the_current_template_attestation() {
    let promotion = model::policy::Promotion {
        channel: "stable".into(),
        target: model::ArtifactDigest::of_bytes(b"candidate"),
        target_version: "0.8.2".parse().unwrap(),
        guest_update_required_since: "0.8.2".parse().unwrap(),
        intent: model::policy::SelectionIntent::Upgrade,
        minimum_host: "0.8.1".parse().unwrap(),
        maximum_guest: "0.8.2".parse().unwrap(),
    };
    let authorization = Uuid::now_v7();
    let action = Action::Apply {
        promotion: Box::new(promotion.clone()),
        authorization,
    };
    for (templates, template, expected) in [
        (vec!["current", "other"], Some("current"), true),
        (vec!["other"], Some("current"), false),
        (vec![], Some("current"), false),
        (vec!["current"], None, false),
    ] {
        let mut policy = policy::Policy {
            authorization,
            promotion: promotion.clone(),
            qualified_templates: templates.into_iter().map(str::to_owned).collect(),
        };
        assert_eq!(policy::authorizes(&policy, &action, template), expected);
        policy.authorization = Uuid::now_v7();
        assert!(!policy::authorizes(&policy, &action, template));
        policy.authorization = authorization;
        policy.promotion.maximum_guest = "0.8.3".parse().unwrap();
        assert!(!policy::authorizes(&policy, &action, template));
    }
}
