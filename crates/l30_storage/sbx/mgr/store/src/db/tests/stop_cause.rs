//! The cause a stop no one asked for leaves on its row: recorded while the
//! row is `stopping`, kept through the stop's cleanup and anything short of
//! running again, and gone once a writer moves the row to `started`.

use super::state_write_fixtures::{
    admit_stop, begin_cold_start, complete_stop, force_state, insert_row,
};
use crate::db::{
    CleanupCompletion, StopCause, get_sandbox, list_sandboxes, mark_cleanup_failed,
    record_stop_cause, test_conn,
};
use crate::state::SandboxState;

#[test]
fn a_stop_cause_lasts_until_the_sandbox_runs_again() {
    let conn = test_conn();
    let cause = |id: &str| get_sandbox(&conn, id).unwrap().unwrap().stop_cause;

    // A row no stop owns refuses a cause.
    let running = insert_row(&conn, SandboxState::Started);
    assert!(!record_stop_cause(&conn, &running, Some(StopCause::VmExited)).unwrap());
    assert_eq!(cause(&running), None);

    // A crash stop whose cleanup fails and is retried still ends `stopped`
    // with its cause.
    let id = insert_row(&conn, SandboxState::Started);
    admit_stop(&conn, &id);
    assert!(record_stop_cause(&conn, &id, Some(StopCause::PodTerminated)).unwrap());
    assert!(
        mark_cleanup_failed(
            &conn,
            &id,
            true,
            CleanupCompletion::MarkStopped,
            "synthetic failure"
        )
        .unwrap()
    );
    assert_eq!(cause(&id), Some(StopCause::PodTerminated));
    force_state(&conn, &id, SandboxState::Stopping);
    complete_stop(&conn, &id);
    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().state,
        SandboxState::Stopped
    );
    assert_eq!(cause(&id), Some(StopCause::PodTerminated));

    // A start that doesn't reach `started` keeps it; one that does ends it.
    begin_cold_start(&conn, &id);
    assert_eq!(cause(&id), Some(StopCause::PodTerminated));
    force_state(&conn, &id, SandboxState::Started);
    assert_eq!(cause(&id), None);

    // A delete keeps it: the pod teardown's Delete can reach the row before
    // its `stopped` is published.
    let deleted = insert_row(&conn, SandboxState::Started);
    admit_stop(&conn, &deleted);
    assert!(record_stop_cause(&conn, &deleted, Some(StopCause::PodTerminated)).unwrap());
    force_state(&conn, &deleted, SandboxState::Deleting);
    assert_eq!(cause(&deleted), Some(StopCause::PodTerminated));

    // A requested stop records nothing, so it reads as a plain stop.
    let requested = insert_row(&conn, SandboxState::Started);
    admit_stop(&conn, &requested);
    complete_stop(&conn, &requested);
    assert_eq!(cause(&requested), None);
}

/// A label a newer sbx-manager wrote, read after a rollback, is no cause, and
/// neither the row nor the node's list becomes unreadable.
#[test]
fn an_unknown_stop_cause_reads_as_none() {
    let conn = test_conn();
    let id = insert_row(&conn, SandboxState::Stopped);
    conn.execute(
        "UPDATE sandboxes SET stop_cause = 'cause_from_a_newer_node' WHERE id = ?1",
        [&id],
    )
    .unwrap();

    assert_eq!(get_sandbox(&conn, &id).unwrap().unwrap().stop_cause, None);
    let listed = list_sandboxes(&conn).unwrap();
    assert!(
        listed
            .iter()
            .any(|row| row.id == id && row.stop_cause.is_none())
    );
}
