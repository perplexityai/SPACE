use super::*;
use crate::db::fixture::insert_test_sandbox;

#[test]
fn prefix_selection_ignores_unrelated_metadata_and_detects_ambiguity_before_decode() {
    let conn = test_conn();
    let healthy = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    let malformed = "019d84bd-280d-7331-99c6-b63388f3a3a2";
    for id in [healthy, malformed, "019d84bd-280d-7331-99c6-b63399f3a3a2"] {
        insert_test_sandbox(&conn, id, SandboxState::Started);
    }
    conn.execute(
        "UPDATE sandboxes SET process_env_json = 'malformed' WHERE id = ?1",
        [malformed],
    )
    .unwrap();

    for (selector, count) in [("77f3", 1), ("019d84bd-280d", 2), ("deadbeef", 0)] {
        assert_eq!(find_by_prefix(&conn, selector).unwrap().len(), count);
    }
    assert!(get_sandbox(&conn, malformed).is_err());
}

#[test]
fn host_process_and_artifact_path_lookups_use_nonunique_indexes() {
    let conn = test_conn();
    for (query, index) in [
        (
            "EXPLAIN QUERY PLAN SELECT id FROM sandboxes
             WHERE netgtw_pid = 42 AND uid = 1000
               AND state IN ('starting', 'started', 'stopping', 'pausing', 'paused',
                             'resuming', 'checkpointing', 'suspending', 'restoring') LIMIT 2",
            "idx_sandboxes_netgtw_process",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT COUNT(*) FROM local_artifacts WHERE local_path = '/artifact'",
            "idx_local_artifacts_path",
        ),
    ] {
        let details = conn
            .prepare(query)
            .unwrap()
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(
            details
                .iter()
                .any(|line| line.contains("SEARCH") && line.contains(index)),
            "{details:?}"
        );
    }

    for id in [
        "019d84bd-280d-7331-99c6-b63377f3a3a2",
        "019d84bd-280d-7331-99c6-b63388f3a3a2",
    ] {
        insert_test_sandbox(&conn, id, SandboxState::Started);
        conn.execute(
            "UPDATE sandboxes SET netgtw_pid = 42, uid = 1000 WHERE id = ?1",
            [id],
        )
        .unwrap();
        insert_local_artifact(
            &conn,
            id,
            LocalArtifactId::new(),
            ArtifactKind::Full,
            ArtifactOrigin::Suspend,
            None,
            "/artifact",
        )
        .unwrap();
    }
    assert_eq!(list_by_netgtw_pid_uid(&conn, 42, 1000).unwrap().len(), 2);
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM local_artifacts WHERE local_path = '/artifact'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        2
    );
}
