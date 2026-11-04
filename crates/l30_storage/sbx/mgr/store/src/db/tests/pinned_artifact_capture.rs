//! The pinned artifact's capture time, which the node publishes so the cluster
//! can tell a pin taken during the current stop from one taken before it.

use super::*;

fn insert_stopped(conn: &Connection, id: &str, uuid8: &str) {
    insert_test_sandbox(
        conn,
        TestSandboxInsert::new(id, uuid8, SandboxState::Stopped),
    );
}

fn insert_disk_artifact(conn: &Connection, sandbox_id: &str, created_at: &str) -> LocalArtifactId {
    let id = LocalArtifactId::new();
    insert_local_artifact(
        conn,
        sandbox_id,
        id,
        ArtifactKind::Disk,
        ArtifactOrigin::Stop,
        None,
        &format!("/cache/{sandbox_id}/d-stop-{id}"),
    )
    .unwrap();
    conn.execute(
        "UPDATE local_artifacts SET created_at = ?1 WHERE local_artifact_id = ?2",
        params![created_at, id.to_string()],
    )
    .unwrap();
    id
}

fn pin(conn: &Connection, sandbox_id: &str, artifact: LocalArtifactId) {
    conn.execute(
        "UPDATE sandboxes
            SET current_local_artifact_id = ?1,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = ?2",
        params![artifact.to_string(), sandbox_id],
    )
    .unwrap();
}

fn created_at(conn: &Connection, artifact: LocalArtifactId) -> String {
    conn.query_row(
        "SELECT created_at FROM local_artifacts WHERE local_artifact_id = ?1",
        params![artifact.to_string()],
        |row| row.get(0),
    )
    .unwrap()
}

/// A new artifact's capture time is the node's clock at the moment it was
/// recorded, in the same whole epoch seconds `state_changed_at` uses, so
/// the two are comparable.
#[test]
fn a_minted_artifact_is_stamped_with_the_node_clock_in_epoch_seconds() {
    let conn = test_conn();
    let sandbox = SandboxId::new().to_string();
    insert_stopped(&conn, &sandbox, "mint0001");
    let before = now_epoch_i64().unwrap();
    let artifact = LocalArtifactId::new();
    insert_local_artifact(
        &conn,
        &sandbox,
        artifact,
        ArtifactKind::Disk,
        ArtifactOrigin::Stop,
        None,
        "/cache/mint",
    )
    .unwrap();
    let after = now_epoch_i64().unwrap();

    let stamped: i64 = created_at(&conn, artifact).parse().unwrap();
    assert!(
        (before..=after).contains(&stamped),
        "{before} <= {stamped} <= {after}"
    );
}

/// Each sandbox reports the capture time of the artifact it pins and of no
/// other: not of an unpinned artifact it also owns, not of an artifact
/// belonging to a different sandbox that its pin happens to name, and not of
/// anything when it pins nothing.
#[test]
fn only_the_pinned_artifact_of_the_same_sandbox_reports_a_capture_time() {
    let conn = test_conn();
    let pinned = SandboxId::new().to_string();
    let unpinned = SandboxId::new().to_string();
    let foreign = SandboxId::new().to_string();
    insert_stopped(&conn, &pinned, "pinned01");
    insert_stopped(&conn, &unpinned, "unpinned");
    insert_stopped(&conn, &foreign, "foreign1");

    let current = insert_disk_artifact(&conn, &pinned, "100");
    let other = insert_disk_artifact(&conn, &pinned, "200");
    pin(&conn, &pinned, current);
    insert_disk_artifact(&conn, &unpinned, "300");
    pin(&conn, &foreign, other);

    let captures = pinned_local_artifact_created_at(
        &conn,
        &[pinned.clone(), unpinned.clone(), foreign.clone()],
    )
    .unwrap();

    // 100 is the pinned artifact's time; 200 belongs to the sandbox's other one.
    assert_eq!(
        captures.get(&pinned),
        Some(&timestamps::EpochSeconds::parse("100").unwrap())
    );
    assert!(!captures.contains_key(&unpinned), "{captures:?}");
    assert!(!captures.contains_key(&foreign), "{captures:?}");
    assert_eq!(captures.len(), 1, "{captures:?}");
}

/// A capture time that can't be decoded as a timestamp is reported as absent
/// rather than guessed, and doesn't fail the read for the other sandboxes in
/// it.
#[test]
fn a_malformed_capture_time_is_absent_and_spares_the_rest() {
    let conn = test_conn();
    let malformed = SandboxId::new().to_string();
    let readable = SandboxId::new().to_string();
    insert_stopped(&conn, &malformed, "malform1");
    insert_stopped(&conn, &readable, "readabl1");
    let bad = insert_disk_artifact(&conn, &malformed, "yesterday");
    pin(&conn, &malformed, bad);
    let good = insert_disk_artifact(&conn, &readable, "7");
    pin(&conn, &readable, good);

    let captures =
        pinned_local_artifact_created_at(&conn, &[malformed.clone(), readable.clone()]).unwrap();

    assert!(!captures.contains_key(&malformed), "{captures:?}");
    assert_eq!(
        captures.get(&readable),
        Some(&timestamps::EpochSeconds::parse("7").unwrap())
    );
}

/// An artifact an older node recorded, before the comparable marker existed,
/// keeps the column's default and reports no capture time, while one this
/// node records is marked. The older node may have kept a pin across a
/// rollback of the rootfs, so its time proves nothing.
#[test]
fn only_an_artifact_recorded_as_comparable_reports_a_capture_time() {
    let conn = test_conn();
    let legacy = SandboxId::new().to_string();
    let current = SandboxId::new().to_string();
    insert_stopped(&conn, &legacy, "legacy01");
    insert_stopped(&conn, &current, "current1");
    let old = LocalArtifactId::new();
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             local_path, created_at
         ) VALUES (?1, ?2, 'disk', 'stop', '/cache/legacy', '9')",
        params![old.to_string(), legacy],
    )
    .unwrap();
    pin(&conn, &legacy, old);
    let new = insert_disk_artifact(&conn, &current, "9");
    pin(&conn, &current, new);

    let captures =
        pinned_local_artifact_created_at(&conn, &[legacy.clone(), current.clone()]).unwrap();

    assert!(!captures.contains_key(&legacy), "{captures:?}");
    assert_eq!(
        captures.get(&current),
        Some(&timestamps::EpochSeconds::parse("9").unwrap())
    );
}

/// Republishing the same suspend artifact moves its path and touch time but
/// keeps its capture time: `created_at` is write-once even on the one path
/// that reuses an artifact identity, so nothing can make an old pin read as
/// newer than it is.
#[test]
fn reusing_a_suspend_artifact_identity_keeps_its_capture_time() {
    let conn = test_conn();
    let sandbox = SandboxId::new().to_string();
    insert_stopped(&conn, &sandbox, "suspend1");
    let artifact = LocalArtifactId::new();
    upsert_suspend_local_artifact(&conn, &sandbox, artifact, "/cache/first").unwrap();
    conn.execute(
        "UPDATE local_artifacts SET created_at = '5' WHERE local_artifact_id = ?1",
        params![artifact.to_string()],
    )
    .unwrap();

    upsert_suspend_local_artifact(&conn, &sandbox, artifact, "/cache/second").unwrap();

    let row = get_local_artifact(&conn, artifact).unwrap().unwrap();
    assert_eq!(row.local_path, "/cache/second");
    assert_eq!(created_at(&conn, artifact), "5");
}

/// **A start that boots, writes and fails in the second its pin was captured
/// leaves no pin to accept.** The pin a real stopped publish records is
/// comparable and captured no earlier than the stop, so with both times in one
/// second it reads as current. A start that launches and fails is cleaned up
/// back to `stopped`, and that cleanup drops the pin, so afterwards the
/// sandbox reports no pin and no capture time, and its next backup mints from
/// the rootfs as it stands.
#[test]
fn a_same_second_failed_launched_start_leaves_no_pin_to_accept() {
    use super::state_write_fixtures::{begin_cold_start, insert_row, seed_runtime};
    let conn = test_conn();
    let id = insert_row(&conn, SandboxState::Stopped);
    let pin = LocalArtifactId::new();
    publish_stopped_artifact(
        &conn,
        &id,
        get_sandbox(&conn, &id).unwrap().unwrap().node_execution_id,
        pin,
        &format!("/cache/{id}/snapshots/d-stop-{pin}"),
    )
    .unwrap();
    let stopped_since: String = conn
        .query_row(
            "SELECT state_changed_at FROM sandboxes WHERE id = ?1",
            params![id],
            |row| row.get(0),
        )
        .unwrap();
    let captured = pinned_local_artifact_created_at(&conn, std::slice::from_ref(&id)).unwrap()[&id];
    assert!(
        captured.get()
            >= timestamps::EpochSeconds::parse(&stopped_since)
                .unwrap()
                .get(),
        "a stop-time mint is captured no earlier than the stop"
    );
    // The tie: both times in the same second.
    conn.execute(
        "UPDATE local_artifacts SET created_at = ?1 WHERE local_artifact_id = ?2",
        params![stopped_since, pin.to_string()],
    )
    .unwrap();
    assert!(
        pinned_local_artifact_created_at(&conn, std::slice::from_ref(&id))
            .unwrap()
            .contains_key(&id)
    );

    let episode = begin_cold_start(&conn, &id);
    seed_runtime(&conn, &id);
    request_cleanup_episode(
        &conn,
        &id,
        episode.id,
        SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
        "guest wrote and the start failed",
    )
    .unwrap();
    complete_cleanup_episode(&conn, &id, episode.id)
        .unwrap()
        .unwrap();

    let row = get_sandbox(&conn, &id).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Stopped);
    assert_eq!(row.current_local_artifact_id, None);
    assert!(
        pinned_local_artifact_created_at(&conn, std::slice::from_ref(&id))
            .unwrap()
            .is_empty(),
        "no pin, so no capture time for the guard to accept"
    );
}

/// **A pin minted as its row enters `stopped` reads as captured in the stop's
/// own second, never before it.** Both stamps come from one clock read: with a
/// clock that moves on every read, a writer that read it twice would record
/// the stop a second after the capture, and the evacuation would refuse a
/// pin of the rootfs as it stands. Covers the stop-time publish from
/// `stopping` and the restored disk a stopped evacuation materializes.
#[test]
fn a_pin_minted_with_its_stop_shares_the_stop_stamp() {
    use super::state_write_fixtures::insert_row;
    for shape in ["stopped publish", "restored stopped disk"] {
        let conn = test_conn();
        let pin = LocalArtifactId::new();
        let id = match shape {
            "stopped publish" => insert_row(&conn, SandboxState::Stopping),
            _ => insert_row(&conn, SandboxState::Restoring),
        };
        let execution = get_sandbox(&conn, &id).unwrap().unwrap().node_execution_id;
        let _clock = crate::db::test_clock::ticking_from(1_800_000_000);
        match shape {
            "stopped publish" => publish_stopped_artifact(
                &conn,
                &id,
                execution,
                pin,
                &format!("/cache/{id}/snapshots/d-stop-{pin}"),
            ),
            _ => publish_restored_stopped_artifact(
                &conn,
                &id,
                pin,
                &format!("/cache/{id}/snapshots/d-stop-{pin}"),
            ),
        }
        .unwrap();

        let (state, stopped_since): (String, String) = conn
            .query_row(
                "SELECT state, state_changed_at FROM sandboxes WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, "stopped", "{shape}");
        let captured =
            pinned_local_artifact_created_at(&conn, std::slice::from_ref(&id)).unwrap()[&id];
        assert_eq!(
            captured,
            timestamps::EpochSeconds::parse(&stopped_since).unwrap(),
            "{shape}: the capture and the stop come from one clock read"
        );
    }
}
