use super::*;
use pplx_space_spaced_update::ArtifactDigest;

fn intent(conn: &Connection) -> Intent {
    let sandbox = SandboxId::new();
    let execution = SandboxActivationAttemptId::new();
    crate::db::fixture::insert_test_sandbox(conn, &sandbox.to_string(), SandboxState::Started);
    conn.execute(
        "UPDATE sandboxes SET node_execution_id = ?2, node_execution_retired = 0 WHERE id = ?1",
        params![sandbox.to_string(), execution.to_string()],
    )
    .unwrap();
    Intent {
        operation: Uuid::now_v7(),
        sandbox,
        execution,
        action: Action::Apply {
            promotion: Box::new(Promotion {
                channel: "stable".into(),
                target_version: "0.8.1".parse().unwrap(),
                guest_update_required_since: "0.8.1".parse().unwrap(),
                intent: pplx_space_spaced_update::policy::SelectionIntent::Upgrade,
                target: ArtifactDigest::of_bytes(b"bundle"),
                minimum_host: "0.8.1".parse().unwrap(),
                maximum_guest: "0.8.1".parse().unwrap(),
            }),
            authorization: Uuid::now_v7(),
        },
        force: false,
        configuration_revision: 1,
    }
}
fn accepted(conn: &Connection, intent: Intent) -> Record {
    match admit(conn, intent, jiff::Timestamp::now().as_second() + 120).unwrap() {
        Admission::Accepted(value) => *value,
        other => panic!("unexpected {other:?}"),
    }
}
fn refused(result: Admission, reason: Refusal) {
    assert!(
        matches!(result, Admission::Refused(value) if value == reason),
        "{result:?}"
    );
}
fn reload(conn: &Connection, op: Uuid) -> Record {
    record(conn, op).unwrap().unwrap()
}

#[test]
fn admission_receipts_fence_concurrency_and_replacement() {
    let conn = test_conn();
    let request = intent(&conn);
    let receipt = accepted(&conn, request.clone());
    assert_eq!(receipt, accepted(&conn, request.clone()));
    let mut conflict = request.clone();
    conflict.force = true;
    refused(
        admit(&conn, conflict, receipt.deadline).unwrap(),
        Refusal::ConflictingId,
    );
    let mut other = request.clone();
    other.operation = Uuid::now_v7();
    refused(
        admit(&conn, other, receipt.deadline).unwrap(),
        Refusal::OtherOperation,
    );
    let replacement = SandboxActivationAttemptId::new();
    conn.execute(
        "UPDATE sandboxes SET node_execution_id = ?2 WHERE id = ?1",
        params![request.sandbox.to_string(), replacement.to_string()],
    )
    .unwrap();
    assert!(!advance(&conn, &receipt, Phase::Activating, None).unwrap());
    assert!(!settle(&conn, &receipt, Outcome::Committed, None).unwrap());
    assert!(supersede(&conn, request.operation).unwrap());
    assert_eq!(
        reload(&conn, request.operation).outcome,
        Some(Outcome::Superseded)
    );
    assert!(!supersede(&conn, request.operation).unwrap());
    assert!(unfinished(&conn, None).unwrap().is_empty());
}

#[test]
fn lifecycle_retirement_revokes_authority_even_when_execution_id_is_retained() {
    use crate::state::SandboxStatus;
    let states: [SandboxStatus; 5] = [
        SandboxState::Stopping.into(),
        SandboxState::Stopped.into(),
        SandboxState::Deleting.into(),
        SandboxState::Paused.into(),
        SandboxState::Suspending.into(),
    ];
    for state in states {
        let conn = test_conn();
        let request = intent(&conn);
        let receipt = accepted(&conn, request.clone());
        crate::db::test_fixtures::state_write_fixtures::force_state(
            &conn,
            &request.sandbox.to_string(),
            state,
        );
        assert!(
            !advance(&conn, &receipt, Phase::Activating, None).unwrap(),
            "{state}"
        );
        let retired =
            !state.is_unowned(SandboxState::Paused) && !state.is_unowned(SandboxState::Suspending);
        assert_eq!(supersede(&conn, request.operation).unwrap(), retired);
        if retired {
            assert_eq!(
                reload(&conn, request.operation).outcome,
                Some(Outcome::Superseded)
            );
        }
    }
}

#[test]
fn returning_to_a_consumed_authorization_does_not_replenish_its_allowance() {
    let conn = test_conn();
    let mut request = intent(&conn);
    let original = request.action.clone();
    for fresh in [false, true] {
        if fresh && let Action::Apply { authorization, .. } = &mut request.action {
            *authorization = Uuid::now_v7();
        }
        let receipt = accepted(&conn, request.clone());
        assert!(settle(&conn, &receipt, Outcome::CandidateFailed, None).unwrap());
        assert!(
            advance(
                &conn,
                &reload(&conn, request.operation),
                Phase::Complete,
                None
            )
            .unwrap()
        );
        request.operation = Uuid::now_v7();
    }
    request.action = original;
    refused(
        admit(&conn, request, jiff::Timestamp::now().as_second() + 120).unwrap(),
        Refusal::InvalidPolicy,
    );
}

#[test]
fn activation_allowance_survives_new_operations_and_only_new_authority_resets_it() {
    let conn = test_conn();
    let mut request = intent(&conn);
    for _ in 0..2 {
        let receipt = accepted(&conn, request.clone());
        assert!(advance(&conn, &receipt, Phase::Activating, None).unwrap());
        assert!(!advance(&conn, &receipt, Phase::Activating, None).unwrap());
        assert!(
            settle(
                &conn,
                &reload(&conn, request.operation),
                Outcome::Committed,
                None
            )
            .unwrap()
        );
        assert!(
            advance(
                &conn,
                &reload(&conn, request.operation),
                Phase::Complete,
                None
            )
            .unwrap()
        );
        request.operation = Uuid::now_v7();
    }
    refused(
        admit(
            &conn,
            request.clone(),
            jiff::Timestamp::now().as_second() + 120,
        )
        .unwrap(),
        Refusal::Exhausted,
    );
    if let Action::Apply { authorization, .. } = &mut request.action {
        *authorization = Uuid::now_v7();
    }
    let next = accepted(&conn, request);
    assert!(advance(&conn, &next, Phase::Activating, None).unwrap());
}

#[test]
fn uncertain_host_wait_retries_once_without_repeating_force_or_quarantined_artifacts() {
    for (outcome, force, retry) in [
        (Outcome::AwaitingHost, false, true),
        (Outcome::AwaitingHost, true, false),
        (Outcome::CandidateFailed, false, false),
    ] {
        let conn = test_conn();
        let mut request = intent(&conn);
        request.force = force;
        let receipt = accepted(&conn, request.clone());
        assert!(advance(&conn, &receipt, Phase::Activating, None).unwrap());
        assert!(settle(&conn, &reload(&conn, request.operation), outcome, None).unwrap());
        assert!(
            advance(
                &conn,
                &reload(&conn, request.operation),
                Phase::Complete,
                None
            )
            .unwrap()
        );
        let record = reload(&conn, request.operation);
        assert_eq!(
            record.phase,
            if retry {
                Phase::Pending
            } else {
                Phase::Complete
            }
        );
        assert!(!retry_ready(&conn, &record).unwrap());
        if retry {
            assert!(!advance(&conn, &record, Phase::Activating, None).unwrap());
            // Move only the retry clock; no real-time sleeps or allowance reset.
            conn.execute(
                "UPDATE guest_update_policy SET retry_not_before = 0 WHERE sandbox_id = ?1",
                params![request.sandbox.to_string()],
            )
            .unwrap();
            assert!(retry_ready(&conn, &record).unwrap());
            assert!(advance(&conn, &record, Phase::Activating, None).unwrap());
            assert!(settle(&conn, &reload(&conn, request.operation), outcome, None).unwrap());
            assert!(
                advance(
                    &conn,
                    &reload(&conn, request.operation),
                    Phase::Complete,
                    None
                )
                .unwrap()
            );
            assert_eq!(reload(&conn, request.operation).phase, Phase::Complete);
            request.operation = Uuid::now_v7();
            refused(
                admit(&conn, request, jiff::Timestamp::now().as_second() + 120).unwrap(),
                Refusal::Exhausted,
            );
        } else {
            request.operation = Uuid::now_v7();
            refused(
                admit(&conn, request, jiff::Timestamp::now().as_second() + 120).unwrap(),
                if outcome == Outcome::CandidateFailed {
                    Refusal::Quarantined
                } else {
                    Refusal::Backoff
                },
            );
        }
    }
}

#[test]
fn down_migration_refuses_unfinished_and_delete_cascades_both_tables() {
    let conn = test_conn();
    let receipt = accepted(&conn, intent(&conn));
    let down = include_str!(
        "../../../../../../../../db/sandbox-manager/20261108230838_guest_updates.down.sql"
    );
    let tx = immediate_transaction(&conn).unwrap();
    let Err(_) = tx.execute_batch(down) else {
        panic!("operation must be rejected");
    };
    drop(tx);
    assert!(record(&conn, receipt.intent.operation).unwrap().is_some());
    let tx = immediate_transaction(&conn).unwrap();
    delete_sandbox_child_rows(&tx, &receipt.intent.sandbox.to_string()).unwrap();
    tx.commit().unwrap();
    assert!(record(&conn, receipt.intent.operation).unwrap().is_none());
    let remaining: u32 = conn
        .query_row("SELECT count(*) FROM guest_update_policy", [], |r| r.get(0))
        .unwrap();
    assert_eq!(remaining, 0);
    conn.execute_batch(down).unwrap();
}

#[test]
fn explicit_rollback_preserves_forward_release_history() {
    use pplx_space_spaced_update::policy::SelectionIntent;
    let conn = test_conn();
    let mut request = intent(&conn);
    for (version, intent, allowed) in [
        ("0.8.5", SelectionIntent::Upgrade, true),
        ("0.8.1", SelectionIntent::Rollback, true),
        ("0.8.3", SelectionIntent::Upgrade, false),
        ("0.8.6", SelectionIntent::Upgrade, true),
    ] {
        request.operation = Uuid::now_v7();
        let Action::Apply {
            promotion,
            authorization,
        } = &mut request.action
        else {
            unreachable!();
        };
        *authorization = Uuid::now_v7();
        promotion.intent = intent;
        promotion.target_version = version.parse().unwrap();
        promotion.guest_update_required_since = version.parse().unwrap();
        promotion.maximum_guest = "0.8.9".parse().unwrap();
        promotion.target = ArtifactDigest::of_bytes(version.as_bytes());
        if allowed {
            let receipt = accepted(&conn, request.clone());
            assert!(settle(&conn, &receipt, Outcome::Committed, None).unwrap());
            assert!(
                advance(
                    &conn,
                    &reload(&conn, request.operation),
                    Phase::Complete,
                    None
                )
                .unwrap()
            );
        } else {
            refused(
                admit(
                    &conn,
                    request.clone(),
                    jiff::Timestamp::now().as_second() + 120,
                )
                .unwrap(),
                Refusal::InvalidPolicy,
            );
        }
    }
}
