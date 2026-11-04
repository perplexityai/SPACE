//! The statuses [`require_state_write_from`] maps `update_state_from`'s
//! node-local SQLite `sandboxes.state` writes to, and what
//! [`update_state_from_best_effort`] returns and persists. Every outcome is
//! listed by a match without a wildcard, so a new outcome or refusal does not
//! compile until it has a status here.

use std::sync::Arc;

use pplx_space_common_core::types::{
    SandboxId, SandboxQuarantineNextAction, SandboxQuarantineReasonCode, SandboxQuarantineSource,
};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::Code;

use super::{
    StateWriteRejected, require_state_write_from, state_write_result, update_state_from_best_effort,
};
use crate::db::test_fixtures::state_write_fixtures::{assert_unchanged, snapshot};
use crate::db::{Edge, StateSet, StateWriteGuard, StateWriteOutcome, StateWriteRefusal, edges};
use crate::service::drain::{CriticalTaskGuard, DrainState};
use crate::service::tests::service_tests::insert_test_sandbox;
use crate::state::SandboxState;

const CONTEXT: &str = "complete synthetic transition";

/// Every refusal. Each names its successor, so a new refusal does not compile
/// until it joins the chain.
fn refusals() -> Vec<StateWriteRefusal> {
    let next = |refusal| match refusal {
        StateWriteRefusal::FromState => {
            Some(StateWriteRefusal::Guard(StateWriteGuard::Quarantined))
        }
        StateWriteRefusal::Guard(StateWriteGuard::Quarantined) => {
            Some(StateWriteRefusal::Guard(StateWriteGuard::CleanupOwned))
        }
        StateWriteRefusal::Guard(StateWriteGuard::CleanupOwned) => None,
    };
    std::iter::successors(Some(StateWriteRefusal::FromState), |refusal| next(*refusal)).collect()
}

/// The messages the retired generic `update_state` caller path returned,
/// copied from 8f12bd6: `"{context}: {SandboxStateWriteNotApplied}"` over
/// `SandboxStateWriteOutcome::as_str`. Clients still receive them for a guard
/// refusal and a missing row.
const BLOCKED_BY_QUARANTINE: &str =
    "complete synthetic transition: sandbox state write was not applied: blocked_by_quarantine";
const SUPERSEDED_BY_CLEANUP_EPISODE: &str = "complete synthetic transition: sandbox state write \
                                             was not applied: superseded_by_cleanup_episode";
const MISSING: &str = "complete synthetic transition: sandbox state write was not applied: missing";

fn outcomes() -> Vec<StateWriteOutcome> {
    let mut outcomes = vec![StateWriteOutcome::Missing];
    for state in SandboxState::all() {
        outcomes.push(StateWriteOutcome::Applied { from: state });
        for refusal in refusals() {
            outcomes.push(StateWriteOutcome::Refused {
                found: state,
                refusal,
            });
        }
    }
    outcomes
}

fn expected_rejection(outcome: StateWriteOutcome) -> Result<SandboxState, StateWriteRejected> {
    match outcome {
        StateWriteOutcome::Applied { from } => Ok(from),
        StateWriteOutcome::Refused { found, refusal } => Err(match refusal {
            StateWriteRefusal::FromState => StateWriteRejected::FromState { found },
            StateWriteRefusal::Guard(StateWriteGuard::Quarantined) => StateWriteRejected::Guard {
                refusal: StateWriteGuard::Quarantined,
                found,
            },
            StateWriteRefusal::Guard(StateWriteGuard::CleanupOwned) => StateWriteRejected::Guard {
                refusal: StateWriteGuard::CleanupOwned,
                found,
            },
        }),
        StateWriteOutcome::Missing => Err(StateWriteRejected::Missing),
    }
}

fn expected_status(outcome: StateWriteOutcome) -> Result<(), (Code, String)> {
    match outcome {
        StateWriteOutcome::Applied { .. } => Ok(()),
        StateWriteOutcome::Refused { found, refusal } => Err(match refusal {
            StateWriteRefusal::FromState => (
                Code::Aborted,
                format!("{CONTEXT}: sandbox state write was superseded: sandbox is {found}"),
            ),
            StateWriteRefusal::Guard(StateWriteGuard::Quarantined) => {
                (Code::Aborted, BLOCKED_BY_QUARANTINE.to_owned())
            }
            StateWriteRefusal::Guard(StateWriteGuard::CleanupOwned) => {
                (Code::Aborted, SUPERSEDED_BY_CLEANUP_EPISODE.to_owned())
            }
        }),
        StateWriteOutcome::Missing => Err((Code::Aborted, MISSING.to_owned())),
    }
}

fn code_and_message<T>(result: Result<T, tonic::Status>) -> Result<T, (Code, String)> {
    result.map_err(|status| (status.code(), status.message().to_owned()))
}

#[test]
fn every_state_write_outcome_has_its_rejection_and_status() {
    for outcome in outcomes() {
        assert_eq!(
            state_write_result(outcome),
            expected_rejection(outcome),
            "{outcome:?}"
        );
        assert_eq!(
            code_and_message(require_state_write_from(Ok(outcome), CONTEXT)),
            expected_status(outcome),
            "{outcome:?}"
        );
        if let Err(rejected) = state_write_result(outcome) {
            assert_eq!(
                Err((
                    rejected.into_status(CONTEXT).code(),
                    rejected.into_status(CONTEXT).message().to_owned()
                )),
                expected_status(outcome),
                "{outcome:?}"
            );
        }
    }
}

#[test]
fn a_database_error_is_internal() {
    assert_eq!(
        code_and_message(require_state_write_from(
            Err(crate::db::Error::invalid_query()),
            CONTEXT,
        )),
        Err((
            Code::Internal,
            "complete synthetic transition: Query is not read-only".to_owned()
        ))
    );
}

/// A row each generic guard refuses `target` over, or no row.
struct GuardedRow {
    name: &'static str,
    seed: fn(&crate::db::Connection, &str),
    target: SandboxState,
    rejected: fn(SandboxState) -> StateWriteRejected,
    message: &'static str,
}

const GUARDED_ROWS: [GuardedRow; 4] = [
    GuardedRow {
        name: "quarantined row",
        seed: |conn, id| {
            insert_test_sandbox(conn, id, SandboxState::Started);
            crate::db::mark_quarantined(
                conn,
                id,
                SandboxQuarantineSource::SbxManager,
                SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed,
                "synthetic snapshot publication failure",
                SandboxQuarantineNextAction::RequiresOperatorAttention,
            )
            .unwrap();
        },
        target: SandboxState::Started,
        rejected: |found| StateWriteRejected::Guard {
            refusal: StateWriteGuard::Quarantined,
            found,
        },
        message: BLOCKED_BY_QUARANTINE,
    },
    GuardedRow {
        name: "stopping row revived",
        seed: |conn, id| insert_test_sandbox(conn, id, SandboxState::Stopping),
        target: SandboxState::Started,
        rejected: |found| StateWriteRejected::Guard {
            refusal: StateWriteGuard::CleanupOwned,
            found,
        },
        message: SUPERSEDED_BY_CLEANUP_EPISODE,
    },
    GuardedRow {
        name: "cleanup row with a delete_row intent",
        seed: |conn, id| {
            insert_test_sandbox(conn, id, SandboxState::Started);
            crate::db::mark_cleanup_required(
                conn,
                id,
                false,
                crate::db::CleanupCompletion::DeleteRow,
                "synthetic cleanup",
            )
            .unwrap();
        },
        target: SandboxState::Stopped,
        rejected: |found| StateWriteRejected::Guard {
            refusal: StateWriteGuard::CleanupOwned,
            found,
        },
        message: SUPERSEDED_BY_CLEANUP_EPISODE,
    },
    GuardedRow {
        name: "missing row",
        seed: |_, _| {},
        target: SandboxState::Started,
        rejected: |_| StateWriteRejected::Missing,
        message: MISSING,
    },
];

/// A guard refusal or a missing row maps to the status the retired generic
/// caller path returned, even with the row's state expected.
#[test]
fn guard_refusals_and_a_missing_row_keep_their_statuses() {
    for row in GUARDED_ROWS {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        (row.seed)(&conn, &id);
        let found = crate::db::get_sandbox(&conn, &id)
            .unwrap()
            .map(|sandbox| sandbox.state);
        let expected = found.unwrap_or(SandboxState::Started);
        let before = snapshot(&conn);

        let outcome =
            crate::db::update_state_from_states(&conn, &id, StateSet::of(&[expected]), row.target)
                .unwrap();
        assert_eq!(
            state_write_result(outcome),
            Err((row.rejected)(expected)),
            "{}",
            row.name
        );
        assert_eq!(
            code_and_message(require_state_write_from(Ok(outcome), CONTEXT)),
            Err((Code::Aborted, row.message.to_owned())),
            "{}",
            row.name
        );
        assert_unchanged(row.name, &before, &conn);
    }
}

fn dirty_sandboxes(conn: &crate::db::Connection) -> Vec<String> {
    crate::db::list_node_dirty_refs(conn, 100)
        .unwrap()
        .into_iter()
        .filter(|dirty| dirty.element_kind == "sandbox_inventory")
        .map(|dirty| dirty.element_value)
        .collect()
}

#[test]
fn best_effort_write_applies_and_marks_only_that_sandbox_dirty() {
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&db.lock().unwrap(), &id, SandboxState::Pausing);
    let before = dirty_sandboxes(&db.lock().unwrap());

    assert_eq!(
        update_state_from_best_effort(&db, &id, &edges::PAUSE_ROLLBACK),
        Some(StateWriteOutcome::Applied {
            from: SandboxState::Pausing
        })
    );
    let conn = db.lock().unwrap();
    assert_eq!(
        crate::db::get_sandbox(&conn, &id).unwrap().unwrap().state,
        SandboxState::Started
    );
    assert!(!before.contains(&id));
    assert_eq!(dirty_sandboxes(&conn), [before, vec![id]].concat());
}

/// A refused write returns its outcome and changes nothing, so the sandbox is
/// not marked dirty.
#[test]
fn best_effort_refusals_change_nothing() {
    for (name, state, outcome) in [
        (
            "row outside the source states",
            Some(SandboxState::Stopped),
            StateWriteOutcome::Refused {
                found: SandboxState::Stopped,
                refusal: StateWriteRefusal::FromState,
            },
        ),
        (
            "cleanup-owned row",
            Some(SandboxState::Stopping),
            StateWriteOutcome::Refused {
                found: SandboxState::Stopping,
                refusal: StateWriteRefusal::Guard(StateWriteGuard::CleanupOwned),
            },
        ),
        ("missing row", None, StateWriteOutcome::Missing),
    ] {
        let db = crate::db::SharedConnection::new(crate::db::test_conn());
        let id = SandboxId::new().to_string();
        if let Some(state) = state {
            insert_test_sandbox(&db.lock().unwrap(), &id, state);
        }
        let before = snapshot(&db.lock().unwrap());

        assert_eq!(
            update_state_from_best_effort(&db, &id, &edges::PAUSE_ROLLBACK),
            Some(outcome),
            "{name}"
        );
        assert_unchanged(name, &before, &db.lock().unwrap());
    }
}

#[test]
fn best_effort_database_error_is_none_and_changes_nothing() {
    let edge: &'static Edge = &edges::ORPHAN_SUSPENDED_INSERT;
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&db.lock().unwrap(), &id, SandboxState::Stopped);
    let before = snapshot(&db.lock().unwrap());

    assert_eq!(update_state_from_best_effort(&db, &id, edge), None);
    assert_unchanged("insert edge", &before, &db.lock().unwrap());
}

/// A poisoned connection is `None`, changes nothing and signals the failure:
/// the dirty-outbox worker wakes, sees the poisoned connection and exits,
/// which fails its critical-task guard. The dirty-outbox and poison `Notify`s
/// are process-global statics, so this relies on nextest running each test in
/// its own process, as the poisoned-outbox test in `service/reconcile/tests.rs`
/// does.
#[tokio::test(start_paused = true)]
async fn best_effort_write_over_a_poisoned_connection_signals_the_failure() {
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&db.lock().unwrap(), &id, SandboxState::Pausing);
    let before = snapshot(&db.lock().unwrap());
    let shutdown = CancellationToken::new();
    let worker_failed = CancellationToken::new();
    let tracker = TaskTracker::new();
    crate::node_state_sync::start_dirty_outbox_worker(
        db.clone(),
        shutdown.clone(),
        &tracker,
        CriticalTaskGuard::new(
            "node dirty outbox",
            shutdown.clone(),
            worker_failed.clone(),
            Arc::new(DrainState::default()),
        ),
    );
    tokio::task::yield_now().await;
    let poisoner = db.clone();
    std::thread::spawn(move || {
        let _conn = poisoner.lock().unwrap();
        panic!("synthetic panic while holding the database");
    })
    .join()
    .unwrap_err();

    assert_eq!(
        update_state_from_best_effort(&db, &id, &edges::PAUSE_ROLLBACK),
        None
    );
    // The clock is paused and this task only yields, so the worker's poll
    // timer cannot fire: only the failure signal wakes it.
    for _ in 0..100 {
        if worker_failed.is_cancelled() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(worker_failed.is_cancelled());
    let conn = db.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_unchanged("poisoned connection", &before, &conn);
}
