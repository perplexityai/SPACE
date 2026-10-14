//! `update_state_from`, the expected-state generic writer of the node-local
//! SQLite `sandboxes.state`: with every state expected it answers each cell of
//! the generic guards' matrix, and a from-set that excludes the row's state
//! refuses with the row unchanged. Each runs standalone and inside a caller's
//! transaction. An edge binds the from-set and target a call site writes.

use super::state_write_fixtures::{assert_diff, assert_unchanged, snapshot, test_start};
use super::update_state_matrix::{
    Expected, PLAIN, Receipt, Shape, Writer, applied_diff, cell, index, seed,
    sweep_lifecycle_pairs_and_cleanup_shapes, sweep_quarantine_episode_and_missing_row, write_from,
};
use crate::db::{
    EDGES, StateSet, StateWriteGuard, StateWriteOutcome, StateWriteRefusal, edges, test_conn,
    update_state_from,
};
use crate::state::SandboxState;

const IN_TRANSACTION: [bool; 2] = [false, true];

#[test]
fn expecting_every_state_answers_every_lifecycle_pair_and_cleanup_shape() {
    for in_transaction in IN_TRANSACTION {
        sweep_lifecycle_pairs_and_cleanup_shapes(Writer { in_transaction });
    }
}

#[test]
fn expecting_every_state_answers_a_quarantine_episode_and_a_missing_row() {
    for in_transaction in IN_TRANSACTION {
        sweep_quarantine_episode_and_missing_row(Writer { in_transaction });
    }
}

/// The from-sets each case passes for a row in `from`, and whether each one
/// contains it: two that exclude it and two accepting controls.
fn from_sets(from: SandboxState) -> [(&'static str, StateSet, bool); 4] {
    [
        ("every other state", StateSet::ALL.without(from), false),
        ("no state", StateSet::EMPTY, false),
        ("only the row's state", StateSet::of(&[from]), true),
        ("every state", StateSet::ALL, true),
    ]
}

/// What writing `to` over a plain row in `from` returns when the from-set
/// does or does not contain `from`, or `None` for a target the generic
/// writers reject before reading the row. The generic guards answer before
/// the expectation does, so their refusals do not depend on the from-set.
fn expected_outcome(
    from: SandboxState,
    to: SandboxState,
    from_set_contains_row: bool,
) -> Option<StateWriteOutcome> {
    let refused = |refusal| StateWriteOutcome::Refused {
        found: from,
        refusal,
    };
    match cell(PLAIN[index(from)], to) {
        Expected::Applied if from_set_contains_row => Some(StateWriteOutcome::Applied { from }),
        Expected::Applied => Some(refused(StateWriteRefusal::FromState)),
        Expected::Quarantine => Some(refused(StateWriteRefusal::Guard(
            StateWriteGuard::Quarantined,
        ))),
        Expected::Superseded => Some(refused(StateWriteRefusal::Guard(
            StateWriteGuard::CleanupOwned,
        ))),
        Expected::Rejected => None,
        Expected::Missing => panic!("a seeded row cannot be missing"),
    }
}

#[test]
fn a_from_set_without_the_row_state_refuses_and_keeps_every_row() {
    for in_transaction in IN_TRANSACTION {
        for receipt_kind in Receipt::ALL {
            for from in SandboxState::all() {
                for to in SandboxState::all() {
                    let receipt = receipt_kind.target(to);
                    for (label, from_set, contains) in from_sets(from) {
                        let case = format!(
                            "in transaction: {in_transaction}, {receipt} receipt, \
                             expecting {label}: {from} -> {to}"
                        );
                        let conn = test_conn();
                        let id = seed(&conn, Shape::Plain, from, receipt);
                        let before = snapshot(&conn);
                        let start = test_start();
                        let result = write_from(&conn, &id, from_set, to, in_transaction);
                        match (expected_outcome(from, to, contains), result) {
                            (Some(expected), Ok(outcome)) => {
                                assert_eq!(outcome, expected, "{case}");
                                if matches!(outcome, StateWriteOutcome::Applied { .. }) {
                                    let applied = applied_diff(&id, from, to, receipt);
                                    assert_diff(&case, &before, &snapshot(&conn), start, &applied);
                                } else {
                                    assert_unchanged(&case, &before, &conn);
                                }
                            }
                            (None, Err(rusqlite::Error::InvalidQuery)) => {
                                assert_unchanged(&case, &before, &conn);
                            }
                            (expected, result) => {
                                panic!("{case}: expected {expected:?}, got {result:?}")
                            }
                        }
                    }
                }
            }
        }
    }
}

fn writable(edge: &crate::db::Edge) -> bool {
    !edge.from().is_empty()
        && !matches!(edge.to(), SandboxState::Quarantined | SandboxState::Failed)
}

#[test]
fn an_edge_writes_its_target_only_from_its_sources() {
    for edge in EDGES.iter().filter(|edge| writable(edge)) {
        let to = edge.to();
        for from in SandboxState::all() {
            let receipt = Receipt::ForTarget.target(to);
            let case = format!("{}: {from} -> {to}", edge.name());
            let conn = test_conn();
            let id = seed(&conn, Shape::Plain, from, receipt);
            let before = snapshot(&conn);
            let start = test_start();
            let expected = expected_outcome(from, to, edge.from().contains(from)).unwrap();
            let outcome = update_state_from(&conn, &id, edge).unwrap();
            assert_eq!(outcome, expected, "{case}");
            if matches!(outcome, StateWriteOutcome::Applied { .. }) {
                let applied = applied_diff(&id, from, to, receipt);
                assert_diff(&case, &before, &snapshot(&conn), start, &applied);
            } else {
                assert_unchanged(&case, &before, &conn);
            }
        }
    }
}

/// Inserts and quarantine or failure targets are refused before the row is
/// read. Each is tried over `started`.
#[test]
fn insert_and_rejected_target_edges_are_refused() {
    let refused: Vec<_> = EDGES.iter().filter(|edge| !writable(edge)).collect();
    assert!(refused.iter().any(|edge| edge.from().is_empty()));
    for edge in refused {
        let to = edge.to();
        let from = SandboxState::Started;
        let case = format!("{}: {from} -> {to}", edge.name());
        let conn = test_conn();
        let id = seed(
            &conn,
            Shape::Plain,
            from,
            Receipt::ForAnotherState.target(to),
        );
        let before = snapshot(&conn);
        match update_state_from(&conn, &id, edge) {
            Err(rusqlite::Error::InvalidQuery) => assert_unchanged(&case, &before, &conn),
            other => panic!("{case}: expected InvalidQuery, got {other:?}"),
        }
    }
}

/// A write that matches no row although the row was read in a source state
/// is an invariant failure, not a refusal. A trigger that skips the update
/// stands in for SQL that drifted from the from-set.
#[test]
fn a_missed_write_from_a_source_state_is_an_invariant_error() {
    let conn = test_conn();
    let id = seed(
        &conn,
        Shape::Plain,
        SandboxState::Pausing,
        SandboxState::Started,
    );
    conn.execute_batch(
        "CREATE TEMP TRIGGER skip_state_write BEFORE UPDATE OF state ON main.sandboxes
         BEGIN SELECT RAISE(IGNORE); END;",
    )
    .unwrap();
    let before = snapshot(&conn);
    match update_state_from(&conn, &id, &edges::PAUSE_ROLLBACK) {
        Err(rusqlite::Error::StatementChangedRows(0)) => {}
        other => panic!("expected an invariant error, got {other:?}"),
    }
    assert_unchanged("skipped pause rollback", &before, &conn);
}

/// Standalone, a `deleting` write and the teardown jobs it fails commit
/// together: when failing the job fails, the state write rolls back too.
#[test]
fn a_standalone_deletion_rolls_back_when_its_job_failure_does() {
    let conn = test_conn();
    let id = seed(
        &conn,
        Shape::Plain,
        SandboxState::Stopped,
        SandboxState::Stopped,
    );
    conn.execute_batch(
        "CREATE TEMP TRIGGER refuse_job_update BEFORE UPDATE ON main.jobs
         BEGIN SELECT RAISE(ABORT, 'job update refused'); END;",
    )
    .unwrap();
    let before = snapshot(&conn);
    assert!(conn.is_autocommit());
    match update_state_from(&conn, &id, &edges::DELETE_ADMISSION) {
        Err(rusqlite::Error::SqliteFailure(_, Some(message))) => {
            assert!(message.contains("job update refused"), "{message}");
        }
        other => panic!("expected the job failure to fail the write, got {other:?}"),
    }
    assert_unchanged("deletion with a failing job update", &before, &conn);
}
