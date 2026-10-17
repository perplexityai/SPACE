//! The artifact pin a `stopped` row carries into a current-instance disk
//! backup. That backup reuses any pin it finds and mints a fresh `d-stop-*`
//! artifact only for a pinless `stopped` row (`enqueue_artifact_backup`'s
//! `(SandboxState::Stopped, None)` arm). A leftover disk pin is therefore what
//! the backup publishes, and a leftover full pin makes it refuse. Each case
//! that expects a pin to be dropped seeds one first, so a writer that left it
//! in place would fail the case.

use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, LocalArtifactId, SandboxQuarantineReasonCode,
};
use rusqlite::Connection;

use super::state_write_fixtures::{
    Cell, ExpectedDiff, admit_stop, assert_accepts, backdate, begin_cold_start, complete_stop,
    force_state, insert_row, insert_suspended_with_artifact, seed_runtime, text, unpinned,
};
use crate::db::{
    StateWriteOutcome, complete_cleanup_episode, drop_stopped_artifact_pin, get_local_artifact,
    get_sandbox, mark_restore_succeeded, publish_stopped_artifact, request_cleanup_episode,
    rollback_starting_episode, stop_suspended_for_auto_lifecycle, test_conn,
};
use crate::state::SandboxState;

/// A `stopped` row pinning a published stop-time disk artifact, as the first
/// disk backup of a stopped sandbox leaves it.
fn insert_stopped_with_disk_pin(conn: &Connection) -> (String, LocalArtifactId) {
    let id = insert_row(conn, SandboxState::Stopped);
    let artifact = LocalArtifactId::new();
    publish_stopped_artifact(
        conn,
        &id,
        get_sandbox(conn, &id).unwrap().unwrap().node_execution_id,
        artifact,
        &format!("/cache/{id}/snapshots/d-stop-{artifact}"),
    )
    .unwrap();
    assert_disk_pinned(conn, &id, artifact);
    (id, artifact)
}

/// A `started` row restored from its suspend artifact, which it still pins.
fn insert_started_with_suspend_pin(conn: &Connection) -> (String, LocalArtifactId) {
    let (id, artifact) = insert_suspended_with_artifact(conn);
    force_state(conn, &id, SandboxState::Restoring);
    assert!(mark_restore_succeeded(conn, &id, SandboxState::Started, None).unwrap());
    assert_suspend_pinned(conn, &id, artifact);
    (id, artifact)
}

fn assert_pinned(
    conn: &Connection,
    id: &str,
    artifact: LocalArtifactId,
    kind: ArtifactKind,
    origin: ArtifactOrigin,
) {
    let row = get_sandbox(conn, id).unwrap().unwrap();
    assert_eq!(row.current_local_artifact_id, Some(artifact));
    assert_eq!(row.current_local_artifact_kind, Some(kind));
    assert_eq!(row.current_local_artifact_origin, Some(origin));
}

fn assert_disk_pinned(conn: &Connection, id: &str, artifact: LocalArtifactId) {
    assert_pinned(conn, id, artifact, ArtifactKind::Disk, ArtifactOrigin::Stop);
}

fn assert_suspend_pinned(conn: &Connection, id: &str, artifact: LocalArtifactId) {
    assert_pinned(
        conn,
        id,
        artifact,
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
    );
}

/// The row is `stopped` with no pin, so its next disk backup mints, and the
/// unpinned artifact is still catalogued: dropping the pin deletes nothing.
fn assert_stopped_unpinned(conn: &Connection, id: &str, dropped: LocalArtifactId) {
    let row = get_sandbox(conn, id).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Stopped);
    assert_eq!(row.current_local_artifact_id, None);
    assert_eq!(row.current_local_artifact_kind, None);
    assert_eq!(row.current_local_artifact_origin, None);
    assert!(get_local_artifact(conn, dropped).unwrap().is_some());
}

/// A start whose runtime may have written the rootfs, then a stop before the
/// start completed: the next disk backup must capture the rootfs as it now
/// stands, not the artifact taken before the start.
#[test]
fn a_start_that_is_stopped_before_it_completes_leaves_no_earlier_disk_pin() {
    let conn = test_conn();
    let (id, earlier) = insert_stopped_with_disk_pin(&conn);
    force_state(&conn, &id, SandboxState::Starting);
    seed_runtime(&conn, &id);

    admit_stop(&conn, &id);
    complete_stop(&conn, &id);

    assert_stopped_unpinned(&conn, &id, earlier);
}

/// A launched start that fails and is cleaned up back to `stopped` may have
/// written its rootfs, so the pin taken before the start may no longer name its
/// bytes.
#[test]
fn a_failed_launched_start_leaves_no_earlier_disk_pin() {
    let conn = test_conn();
    let (id, earlier) = insert_stopped_with_disk_pin(&conn);
    let episode = begin_cold_start(&conn, &id);
    seed_runtime(&conn, &id);
    request_cleanup_episode(
        &conn,
        &id,
        episode.id,
        SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
        "guest never became ready",
    )
    .unwrap();

    complete_cleanup_episode(&conn, &id, episode.id)
        .unwrap()
        .unwrap();

    assert_stopped_unpinned(&conn, &id, earlier);
}

/// A start refused before anything launched never touched the rootfs, so the
/// rollback keeps the pin and the next disk backup reuses an artifact of the
/// same bytes instead of minting a duplicate.
#[test]
fn a_start_refused_before_launch_keeps_its_disk_pin() {
    let conn = test_conn();
    let (id, artifact) = insert_stopped_with_disk_pin(&conn);
    let episode = begin_cold_start(&conn, &id);

    assert!(rollback_starting_episode(&conn, &id, SandboxState::Stopped, episode.id).unwrap());

    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().state,
        SandboxState::Stopped
    );
    assert_disk_pinned(&conn, &id, artifact);
}

/// A sandbox restored from a suspend artifact keeps that pin while it runs. A
/// stop must not carry it into `stopped`: the guest may have written since the
/// restore, and a `stopped` row's disk backup refuses a full artifact
/// outright.
#[test]
fn a_restored_sandbox_that_is_stopped_leaves_no_suspend_pin() {
    let conn = test_conn();
    let (id, suspend_artifact) = insert_started_with_suspend_pin(&conn);

    admit_stop(&conn, &id);
    complete_stop(&conn, &id);

    assert_stopped_unpinned(&conn, &id, suspend_artifact);
}

/// An idle auto-lifecycle Stop of a suspended sandbox leaves it `stopped`
/// without the full suspend pin, so a later disk backup mints one of the
/// rootfs instead of failing on the artifact kind. The write changes nothing
/// else.
#[test]
fn an_auto_lifecycle_stop_of_a_suspended_sandbox_leaves_no_suspend_pin() {
    let conn = test_conn();
    let (id, suspend_artifact) = insert_suspended_with_artifact(&conn);
    backdate(&conn, &id);
    let mut cells = vec![
        ("state", text("stopped")),
        ("state_changed_at", Cell::NowText),
        ("updated_at", Cell::NowText),
    ];
    cells.extend(unpinned());

    assert_accepts(
        "auto-lifecycle stop of a suspended row",
        &conn,
        || {
            let outcome = stop_suspended_for_auto_lifecycle(&conn, &id).unwrap();
            assert!(
                matches!(outcome, StateWriteOutcome::Applied { .. }),
                "{outcome:?}"
            );
        },
        ExpectedDiff::new().sandbox(&id, cells),
    );
    assert_stopped_unpinned(&conn, &id, suspend_artifact);
}

/// The auto-lifecycle Stop drops the pin only with its own state write. A
/// sandbox that another writer already moved to `stopped`, and whose disk
/// backup has since pinned the rootfs, keeps that pin when the stale Stop is
/// refused.
#[test]
fn a_refused_auto_lifecycle_stop_keeps_a_stopped_disk_pin() {
    let conn = test_conn();
    let (id, artifact) = insert_stopped_with_disk_pin(&conn);

    let outcome = stop_suspended_for_auto_lifecycle(&conn, &id).unwrap();

    assert!(
        matches!(
            outcome,
            StateWriteOutcome::Refused {
                found: SandboxState::Stopped,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_disk_pinned(&conn, &id, artifact);
}

/// The writer a snapshot rollback calls before it rewrites a stopped
/// sandbox's rootfs: it leaves the row pinless, so the next disk backup mints
/// one of the rolled-back bytes, and changes nothing else.
#[test]
fn dropping_a_stopped_pin_leaves_the_row_pinless() {
    let conn = test_conn();
    let (id, artifact) = insert_stopped_with_disk_pin(&conn);
    backdate(&conn, &id);
    let mut cells = vec![("updated_at", Cell::NowText)];
    cells.extend(unpinned());

    assert_accepts(
        "drop a stopped row's pin",
        &conn,
        || assert!(drop_stopped_artifact_pin(&conn, &id).unwrap()),
        ExpectedDiff::new().sandbox(&id, cells),
    );
    assert_stopped_unpinned(&conn, &id, artifact);
    assert!(!drop_stopped_artifact_pin(&conn, &id).unwrap());
}

/// Only a `stopped` row's pin is dropped: a suspended sandbox's pin names the
/// suspend artifact its restore needs.
#[test]
fn dropping_a_stopped_pin_leaves_a_suspended_pin() {
    let conn = test_conn();
    let (id, suspend_artifact) = insert_suspended_with_artifact(&conn);

    assert!(!drop_stopped_artifact_pin(&conn, &id).unwrap());

    assert_suspend_pinned(&conn, &id, suspend_artifact);
}

/// A disk backup that read a stopped, pinless row and then lost a race with a
/// start must not pin its snapshot, because the start may have written the
/// rootfs after the snapshot was taken. The publish refuses while the row
/// carries the execution a start began, even one rolled back before launch as
/// here, and publishing for that execution still succeeds.
#[test]
fn a_stopped_publish_refuses_a_row_started_since_it_was_read() {
    let conn = test_conn();
    let id = insert_row(&conn, SandboxState::Stopped);
    let read = get_sandbox(&conn, &id).unwrap().unwrap().node_execution_id;
    let episode = begin_cold_start(&conn, &id);
    assert!(rollback_starting_episode(&conn, &id, SandboxState::Stopped, episode.id).unwrap());
    let current = get_sandbox(&conn, &id).unwrap().unwrap().node_execution_id;
    assert_ne!(read, current);
    let stale = LocalArtifactId::new();

    assert!(
        publish_stopped_artifact(&conn, &id, read, stale, &format!("/cache/{id}/stale")).is_err()
    );

    let row = get_sandbox(&conn, &id).unwrap().unwrap();
    assert_eq!(row.current_local_artifact_id, None);
    assert!(get_local_artifact(&conn, stale).unwrap().is_none());
    let fresh = LocalArtifactId::new();
    publish_stopped_artifact(&conn, &id, current, fresh, &format!("/cache/{id}/fresh")).unwrap();
    assert_disk_pinned(&conn, &id, fresh);
}
