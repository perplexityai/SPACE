use super::*;
use crate::db::fixture::insert_test_sandbox;

/// A suspended sandbox pinning a checkpoint artifact; returns its id and pin.
fn pinned_suspended(conn: &Connection) -> (String, String) {
    let id = SandboxId::new().to_string();
    insert_test_sandbox(conn, &id, SandboxState::Suspended);
    let artifact = LocalArtifactId::new();
    insert_local_artifact(
        conn,
        &id,
        artifact,
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
        "/space/cache/test/snapshot",
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET current_local_artifact_id = ?1,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = ?2",
        params![artifact.to_string(), id],
    )
    .unwrap();
    (id, artifact.to_string())
}

/// The suspended eviction claim deletes every local artifact of the sandbox,
/// so it waits for an upload that is pending, running or cancelling, leaving
/// the row
/// unchanged, and proceeds once the upload finished either way.
#[test]
fn a_suspended_eviction_claim_waits_for_a_pending_upload() {
    for (status, claimable) in [
        ("pending", false),
        ("running", false),
        ("cancelling", false),
        ("completed", true),
        ("failed", true),
    ] {
        let conn = test_conn();
        let (id, pin) = pinned_suspended(&conn);
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, payload, subject_id, created_at)
             VALUES (?1, 'snapshot_upload', ?2, '{}', ?3, '1')",
            params![uuid::Uuid::now_v7().to_string(), status, id],
        )
        .unwrap();
        let before = get_sandbox(&conn, &id).unwrap().unwrap();

        let claimed = claim_suspended_sandbox_for_eviction(&conn, &id, Some(&pin), None).unwrap();

        assert_eq!(claimed, claimable, "{status}");
        if !claimable {
            let after = get_sandbox(&conn, &id).unwrap().unwrap();
            assert_eq!(
                (after.state, &after.updated_at),
                (SandboxState::Suspended, &before.updated_at),
                "{status}"
            );
        }
    }
}

/// The stale-copy claim re-asserts the generations its caller checked the
/// activation against, not a row version: a generation retained after the
/// read fails it, with the row unchanged, even within the same second, and
/// an unchanged witness claims the row. An in-flight upload fails it too.
#[test]
fn a_stale_upgrade_copy_claim_needs_the_generations_it_was_decided_against() {
    // (the row's change after the witness was read, whether it still claims)
    for (change, claimable) in [
        (None, true),
        (
            Some("UPDATE sandboxes SET last_observed_execution_generation = 9 WHERE id = ?1"),
            false,
        ),
        (
            Some("UPDATE sandboxes SET last_observed_route_generation = 9 WHERE id = ?1"),
            false,
        ),
        (
            Some(
                "INSERT INTO jobs (job_id, kind, status, payload, subject_id, created_at)
                 VALUES ('upload-in-flight', 'snapshot_upload', 'pending', '{}', ?1, '1')",
            ),
            false,
        ),
    ] {
        let conn = test_conn();
        let (id, pin) = pinned_suspended(&conn);
        let generations = activation_generations(&conn, &id).unwrap().unwrap();
        let before = get_sandbox(&conn, &id).unwrap().unwrap();
        if let Some(change) = change {
            conn.execute(change, params![id]).unwrap();
        }

        let claimed =
            claim_stale_upgrade_copy_for_eviction(&conn, &id, &pin, &generations).unwrap();

        assert_eq!(claimed, claimable, "{change:?}");
        if !claimable {
            let after = get_sandbox(&conn, &id).unwrap().unwrap();
            assert_eq!(after.state, SandboxState::Suspended, "{change:?}");
            assert_eq!(after.updated_at, before.updated_at, "{change:?}");
        }
    }
    let conn = test_conn();
    assert!(activation_generations(&conn, "absent").unwrap().is_none());
}
