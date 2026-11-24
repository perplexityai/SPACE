use super::*;
use rusqlite::params;

fn test_db() -> SharedConnection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    crate::tests::initialize_schema(&conn);

    conn.execute(
        "INSERT INTO jobs (job_id, kind, payload) VALUES ('019d84bd-280d-7331-99c6-000000000001', ?1, '{}')",
        params![BUILD_JOB_KIND],
    )
    .unwrap();
    SharedConnection::new(database::Connection::from_raw(conn))
}

#[test]
fn stores_and_reads_build_result() {
    let db = test_db();
    let result = BuildJobResult {
        template_id: "seed".to_string(),
        state: BuildTerminalState::Succeeded,
        content_hash: Some("sha256:abc".to_string()),
        error: None,
    };

    store_build_result(
        &db,
        &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
        &result,
    )
    .unwrap();
    let stored = {
        let conn = db.lock().unwrap();
        get_build_result(
            &conn,
            &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
        )
        .unwrap()
        .unwrap()
    };

    assert_eq!(stored.template_id, "seed");
    assert_eq!(stored.state, BuildTerminalState::Succeeded);
    assert_eq!(stored.content_hash.as_deref(), Some("sha256:abc"));
    assert_eq!(stored.error, None);
}

fn insert_build_job(conn: &database::Connection, job_id: &str, subject_id: &str, status: &str) {
    let conn = conn.raw();
    conn.execute(
        "INSERT INTO jobs (job_id, kind, payload, subject_id, status)
         VALUES (?1, ?2, '{}', ?3, ?4)",
        params![job_id, BUILD_JOB_KIND, subject_id, status],
    )
    .unwrap();
}

#[test]
fn count_other_active_builds_excludes_self_and_terminal_rows() {
    let db = test_db();
    let conn = db.lock().unwrap();
    insert_build_job(
        &conn,
        "019d84bd-280d-7331-99c6-000000000002",
        "tpl-a",
        "running",
    );
    insert_build_job(
        &conn,
        "019d84bd-280d-7331-99c6-000000000003",
        "tpl-a",
        "pending",
    );
    insert_build_job(
        &conn,
        "019d84bd-280d-7331-99c6-000000000004",
        "tpl-a",
        "completed",
    );
    insert_build_job(
        &conn,
        "019d84bd-280d-7331-99c6-000000000005",
        "tpl-a",
        "failed",
    );
    insert_build_job(
        &conn,
        "019d84bd-280d-7331-99c6-000000000006",
        "tpl-b",
        "running",
    );

    // From 019d84bd-280d-7331-99c6-000000000002's perspective only the pending sibling counts:
    // terminal rows and other templates never gate the prune.
    assert_eq!(
        count_other_active_builds_for_subject(
            &conn,
            "tpl-a",
            &"019d84bd-280d-7331-99c6-000000000002".parse().unwrap()
        )
        .unwrap(),
        1
    );
    // From the pending sibling's perspective, the running job counts.
    assert_eq!(
        count_other_active_builds_for_subject(
            &conn,
            "tpl-a",
            &"019d84bd-280d-7331-99c6-000000000003".parse().unwrap()
        )
        .unwrap(),
        1
    );
    assert_eq!(
        count_other_active_builds_for_subject(
            &conn,
            "tpl-b",
            &"019d84bd-280d-7331-99c6-000000000006".parse().unwrap()
        )
        .unwrap(),
        0
    );
}
