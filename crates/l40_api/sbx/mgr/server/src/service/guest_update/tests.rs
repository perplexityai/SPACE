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
