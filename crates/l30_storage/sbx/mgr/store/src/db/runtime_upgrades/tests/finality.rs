//! Which attempt outcomes are final: the ones a rollback or refusal records,
//! with the request, while attempt finality is on. A row that doesn't record
//! its request is final for no attempt.

use super::*;

/// An attempt outcome that doesn't record its request, as a build from before
/// the request columns writes when a rollback finishes.
fn insert_legacy_outcome(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    attempt_id: &str,
    sandbox_id: &str,
) {
    conn.execute(
        "INSERT INTO sandbox_runtime_upgrade_attempt_outcomes
             (operation_id, attempt_id, sandbox_id, outcome, failure, recorded_at)
         VALUES (?1, ?2, ?3, 'rolled_back', 'an earlier release rolled it back', '0')",
        params![operation_id.to_string(), attempt_id, sandbox_id],
    )
    .unwrap();
}

/// A rollback with finality on records its attempt's final outcome in the
/// transaction that makes the rollback reportable, before its disk is
/// discarded, so the report names the attempt only while the outcome agrees.
/// With finality off it records none, and the report names no attempt.
#[test]
fn a_rollback_is_final_for_its_attempt_only_with_finality_on() {
    for finality in [true, false] {
        let conn = test_conn();
        let envs = environments();
        let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
        let operation_id = operation();
        fenced(begin_for_attempt(
            &conn,
            operation_id,
            &id,
            envs.target_reference,
            false,
            "attempt-a",
        ));

        assert!(roll_back_runtime_upgrade(&conn, operation_id, "boot failed", finality).unwrap());

        let own =
            crate::db::read::runtime_upgrade_attempt_outcome(&conn, operation_id, "attempt-a")
                .unwrap();
        let named = finality.then(|| "attempt-a".to_string());
        assert_eq!(
            own.map(|outcome| (outcome.outcome, outcome.failure)),
            finality.then(|| (
                RecordedAttemptOutcome::RolledBack,
                Some("boot failed".into())
            )),
            "finality {finality}: recorded before the disk is discarded"
        );
        let rolled_back = RuntimeUpgradeStatus::RolledBack {
            failure: Some("boot failed".to_string()),
        };
        assert_eq!(
            report_with_attempt(&conn, operation_id),
            Some((rolled_back.clone(), named.clone())),
            "finality {finality}: the live row"
        );
        assert!(finish_runtime_upgrade_rollback(&conn, operation_id).unwrap());
        assert_eq!(
            report_with_attempt(&conn, operation_id),
            Some((rolled_back, named)),
            "finality {finality}: the recorded outcome"
        );
        let retained = crate::db::read::retained_runtime_upgrade_requests(&conn, operation_id)
            .unwrap()
            .len();
        assert_eq!(retained, usize::from(finality), "finality {finality}");
    }
}

/// An attempt outcome without its request is final for no attempt: no reader answers from it, it holds back no
/// request, and the attempt's final outcome replaces it.
#[test]
fn a_legacy_attempt_outcome_is_final_for_no_attempt() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let elsewhere = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    insert_legacy_outcome(&conn, operation_id, "attempt-a", &elsewhere);

    assert_eq!(
        crate::db::read::runtime_upgrade_attempt_outcome(&conn, operation_id, "attempt-a").unwrap(),
        None
    );
    assert_eq!(
        crate::db::read::retained_runtime_upgrade_requests(&conn, operation_id).unwrap(),
        Vec::new()
    );
    // It names another sandbox, and still holds back nothing.
    fenced(begin_for_attempt(
        &conn,
        operation_id,
        &id,
        envs.target_reference,
        false,
        "attempt-a",
    ));
    // Nor does the report name the attempt for a rollback the row agrees with.
    conn.execute(
        "UPDATE sandbox_runtime_upgrades
            SET phase = 'rolled_back', failure = 'an earlier release rolled it back',
                updated_at = '0'
          WHERE operation_id = ?1",
        params![operation_id.to_string()],
    )
    .unwrap();
    assert_eq!(
        report_with_attempt(&conn, operation_id).map(|(_, attempt)| attempt),
        Some(None)
    );
    conn.execute(
        "UPDATE sandbox_runtime_upgrades SET phase = 'fenced' WHERE operation_id = ?1",
        params![operation_id.to_string()],
    )
    .unwrap();
    assert!(roll_back_runtime_upgrade(&conn, operation_id, "boot failed", true).unwrap());
    let own = crate::db::read::runtime_upgrade_attempt_outcome(&conn, operation_id, "attempt-a")
        .unwrap()
        .unwrap();
    assert_eq!(own.failure.as_deref(), Some("boot failed"));
    let rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sandbox_runtime_upgrade_attempt_outcomes
              WHERE operation_id = ?1",
            params![operation_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        rows, 1,
        "the final outcome replaced the one without a request"
    );
}

/// A final attempt outcome keeps what its operation asked for after the
/// sandbox's row, and its report, are gone: the store won't begin, refuse or
/// find on target the operation for another sandbox, target state or
/// authorization, under any attempt id.
#[test]
fn a_final_attempt_outcome_holds_its_operation_to_its_request() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    fenced(begin_for_attempt(
        &conn,
        operation_id,
        &id,
        envs.target_reference,
        false,
        "attempt-a",
    ));
    assert!(roll_back_runtime_upgrade(&conn, operation_id, "boot failed", true).unwrap());
    assert!(finish_runtime_upgrade_rollback(&conn, operation_id).unwrap());
    delete_sandbox(&conn, &id).unwrap();
    assert_eq!(
        crate::db::read::runtime_upgrade_report(&conn, operation_id).unwrap(),
        None
    );
    // The same sandbox id comes back, so only the retained request refuses.
    insert_test_sandbox(&conn, &id, SandboxState::Stopped);
    let elsewhere = sandbox(&conn, SandboxState::Stopped, &envs.source);

    assert_eq!(
        begin_for_attempt(
            &conn,
            operation_id,
            &elsewhere,
            envs.target_reference,
            false,
            "attempt-b",
        ),
        BeginRuntimeUpgrade::OperationNamesAnother
    );
    let asked = RuntimeUpgradeRequestIdentity {
        target_environment: envs.target_reference,
        ..on_target(operation_id, &id)
    };
    let others = [
        RuntimeUpgradeRequestIdentity {
            sandbox_id: elsewhere.clone(),
            ..asked.clone()
        },
        RuntimeUpgradeRequestIdentity {
            target_state: RuntimeUpgradeTargetState::Started,
            ..asked.clone()
        },
        RuntimeUpgradeRequestIdentity {
            authorization_id: "another-authorization".to_string(),
            ..asked.clone()
        },
    ];
    for other in &others {
        assert!(
            !record_runtime_upgrade_refusal(&conn, other, "attempt-b", "refused", None).unwrap(),
            "{other:?}"
        );
        assert!(
            !record_runtime_upgrade_already_on_target(&conn, other, Some("attempt-b")).unwrap(),
            "{other:?}"
        );
    }
    assert_eq!(
        crate::db::read::runtime_upgrade_report(&conn, operation_id).unwrap(),
        None
    );
    // The request it recorded is still the operation's.
    assert!(record_runtime_upgrade_refusal(&conn, &asked, "attempt-b", "refused", None).unwrap());
}

/// A start that ran and stopped again while the refusal was decided leaves the
/// state, local artifact and execution metadata as they were, but not the
/// activation, so a disk the start laid down doesn't inherit the refusal.
#[test]
fn a_refusal_isnt_recorded_after_a_start_ran_in_between() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    let identity = RuntimeUpgradeRequestIdentity {
        target_environment: envs.target_reference,
        ..on_target(operation_id, &id)
    };
    let decided = get_sandbox(&conn, &id).unwrap().unwrap();
    conn.execute(
        "UPDATE sandboxes SET node_execution_id = ?2 WHERE id = ?1",
        params![id, SandboxActivationAttemptId::new().to_string()],
    )
    .unwrap();

    assert!(
        !record_runtime_upgrade_refusal(
            &conn,
            &identity,
            "attempt-a",
            "refused",
            Some(ObservedSandbox::of(&decided)),
        )
        .unwrap()
    );
    assert_eq!(
        crate::db::read::runtime_upgrade_attempt_outcome(&conn, operation_id, "attempt-a").unwrap(),
        None
    );
}

/// A refusal decided from the sandbox's row is recorded only while the row is
/// still as the refusal read it, compared in the transaction that records it.
#[test]
fn a_refusal_is_recorded_only_while_the_sandbox_is_as_it_was_decided_on() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    let identity = RuntimeUpgradeRequestIdentity {
        target_environment: envs.target_reference,
        ..on_target(operation_id, &id)
    };
    let decided = get_sandbox(&conn, &id).unwrap().unwrap();
    conn.execute(
        "UPDATE sandboxes SET state = 'started' WHERE id = ?1",
        params![id],
    )
    .unwrap();

    assert!(
        !record_runtime_upgrade_refusal(
            &conn,
            &identity,
            "attempt-a",
            "refused",
            Some(ObservedSandbox::of(&decided)),
        )
        .unwrap()
    );
    assert_eq!(
        crate::db::read::runtime_upgrade_report(&conn, operation_id).unwrap(),
        None
    );
    assert_eq!(
        crate::db::read::runtime_upgrade_attempt_outcome(&conn, operation_id, "attempt-a").unwrap(),
        None
    );

    let now = get_sandbox(&conn, &id).unwrap().unwrap();
    assert!(
        record_runtime_upgrade_refusal(
            &conn,
            &identity,
            "attempt-a",
            "refused",
            Some(ObservedSandbox::of(&now)),
        )
        .unwrap()
    );
}
