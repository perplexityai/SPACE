//! The node's attribution of a sandbox's last stop: recorded when a stop
//! newly owns the row, a requested one included, kept through the stop's
//! cleanup, a failed start and a delete, and gone once a writer moves the row
//! to `started`.

use super::state_write_fixtures::{
    admit_stop, begin_cold_start, complete_stop, force_state, insert_row,
};
use crate::db::{
    CleanupCompletion, StopAttribution, StopCause, get_sandbox, list_sandboxes,
    mark_cleanup_failed, record_stop_attribution, test_conn,
};
use crate::state::SandboxState;

#[test]
fn a_stop_attribution_lasts_until_the_sandbox_runs_again() {
    let conn = test_conn();
    let attribution = |id: &str| -> Option<StopAttribution> {
        get_sandbox(&conn, id).unwrap().unwrap().stop_attribution
    };

    // A row no stop owns refuses one.
    let running = insert_row(&conn, SandboxState::Started);
    assert!(!record_stop_attribution(&conn, &running, Some(StopCause::VmExited)).unwrap());
    assert_eq!(attribution(&running), None);

    // A crash stop whose cleanup fails and is retried still ends `stopped`
    // with its attribution, time included.
    let id = insert_row(&conn, SandboxState::Started);
    admit_stop(&conn, &id);
    assert!(record_stop_attribution(&conn, &id, Some(StopCause::PodTerminated)).unwrap());
    let crashed = attribution(&id).expect("the stop is attributed");
    assert_eq!(crashed.cause, Some(StopCause::PodTerminated));
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
    assert_eq!(attribution(&id), Some(crashed));
    force_state(&conn, &id, SandboxState::Stopping);
    complete_stop(&conn, &id);
    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().state,
        SandboxState::Stopped
    );
    assert_eq!(attribution(&id), Some(crashed));

    // A start that fails back to `stopped` keeps the attribution and its
    // time, though it moves the row's state change time.
    begin_cold_start(&conn, &id);
    force_state(&conn, &id, SandboxState::Stopped);
    assert_eq!(attribution(&id), Some(crashed));
    // One that reaches `started` ends it.
    force_state(&conn, &id, SandboxState::Started);
    assert_eq!(attribution(&id), None);

    // A delete keeps it: only running again, or a stop that newly owns the
    // row, replaces it.
    let deleted = insert_row(&conn, SandboxState::Started);
    admit_stop(&conn, &deleted);
    assert!(record_stop_attribution(&conn, &deleted, Some(StopCause::PodTerminated)).unwrap());
    force_state(&conn, &deleted, SandboxState::Deleting);
    assert_eq!(
        attribution(&deleted).map(|a| a.cause),
        Some(Some(StopCause::PodTerminated))
    );

    // A requested stop is attributed too, with no cause.
    let requested = insert_row(&conn, SandboxState::Started);
    admit_stop(&conn, &requested);
    assert!(record_stop_attribution(&conn, &requested, None).unwrap());
    complete_stop(&conn, &requested);
    assert_eq!(attribution(&requested).map(|a| a.cause), Some(None));
}

/// A label a newer sbx-manager wrote, read after a rollback, is no cause, and
/// neither the row nor the node's list becomes unreadable.
#[test]
fn an_unknown_stop_cause_reads_as_none() {
    let conn = test_conn();
    let id = insert_row(&conn, SandboxState::Stopped);
    conn.execute(
        "UPDATE sandboxes
            SET stop_cause = 'cause_from_a_newer_node', stop_cause_at = 1793900000000
          WHERE id = ?1",
        [&id],
    )
    .unwrap();

    let expected = Some(StopAttribution {
        cause: None,
        stopped_at_ms: 1_793_900_000_000,
    });
    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().stop_attribution,
        expected
    );
    let listed = list_sandboxes(&conn).unwrap();
    assert!(
        listed
            .iter()
            .any(|row| row.id == id && row.stop_attribution == expected)
    );
}
