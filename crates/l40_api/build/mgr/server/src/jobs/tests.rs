use super::*;

fn test_db() -> Arc<Mutex<Connection>> {
    let mut conn = Connection::open_in_memory().unwrap();
    crate::tests::initialize_schema(&mut conn);

    conn.execute(
        "INSERT INTO jobs (job_id, kind, payload) VALUES ('019d84bd-280d-7331-99c6-000000000001', ?1, '{}')",
        params![BUILD_JOB_KIND],
    )
    .unwrap();
    Arc::new(Mutex::new(conn))
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

#[test]
fn leased_result_write_requires_current_owner() {
    let db = test_db();
    let result = BuildJobResult {
        template_id: "seed".to_string(),
        state: BuildTerminalState::Succeeded,
        content_hash: Some("sha256:abc".to_string()),
        error: None,
    };
    {
        let conn = db.lock().unwrap();
        conn.execute(
            "UPDATE jobs
             SET status = 'running',
                 lease_owner = 'worker-1',
                 lease_expires_at = datetime('now', '+60 seconds')
             WHERE job_id = '019d84bd-280d-7331-99c6-000000000001'",
            [],
        )
        .unwrap();
    }

    assert!(
        !store_build_result_if_lease_owner(
            &db,
            &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
            "worker-2",
            &result
        )
        .unwrap()
    );
    {
        let conn = db.lock().unwrap();
        assert!(
            get_build_result(
                &conn,
                &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap()
            )
            .unwrap()
            .is_none()
        );
    }

    assert!(
        store_build_result_if_lease_owner(
            &db,
            &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
            "worker-1",
            &result
        )
        .unwrap()
    );
    {
        let conn = db.lock().unwrap();
        let stored = get_build_result(
            &conn,
            &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.template_id, "seed");
        assert_eq!(stored.state, BuildTerminalState::Succeeded);
    }

    let stale_update = BuildJobResult {
        template_id: "seed".to_string(),
        state: BuildTerminalState::Failed,
        content_hash: None,
        error: Some("stale worker".to_string()),
    };
    {
        let conn = db.lock().unwrap();
        conn.execute(
            "UPDATE jobs SET lease_owner = 'worker-2' WHERE job_id = '019d84bd-280d-7331-99c6-000000000001'",
            [],
        )
        .unwrap();
    }

    assert!(
        !store_build_result_if_lease_owner(
            &db,
            &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
            "worker-1",
            &stale_update
        )
        .unwrap()
    );
    {
        let conn = db.lock().unwrap();
        let stored = get_build_result(
            &conn,
            &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.state, BuildTerminalState::Succeeded);
        assert_eq!(stored.content_hash.as_deref(), Some("sha256:abc"));
        assert_eq!(stored.error, None);
    }
}

#[test]
fn expired_running_build_reaper_marks_null_lease_rows_failed() {
    let db = test_db();
    {
        let conn = db.lock().unwrap();
        conn.execute(
            "UPDATE jobs
             SET status = 'running',
                 lease_owner = NULL,
                 lease_expires_at = NULL
             WHERE job_id = '019d84bd-280d-7331-99c6-000000000001'",
            [],
        )
        .unwrap();

        let failed = fail_expired_running_builds(&conn, "interrupted").unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(
            failed[0].0.to_string(),
            "019d84bd-280d-7331-99c6-000000000001"
        );

        let (status, error_kind, error): (String, String, String) = conn
            .query_row(
                "SELECT status, error_kind, error FROM jobs WHERE job_id = '019d84bd-280d-7331-99c6-000000000001'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(status, "failed");
        assert_eq!(error_kind, "permanent");
        assert_eq!(error, "interrupted");
    }
}

#[test]
fn terminal_state_round_trips_wire_string() {
    for state in [
        BuildTerminalState::Succeeded,
        BuildTerminalState::Failed,
        BuildTerminalState::PublishFailed,
    ] {
        assert_eq!(state.as_str().parse::<BuildTerminalState>().unwrap(), state);
    }
}

fn insert_build_job(conn: &Connection, job_id: &str, subject_id: &str, status: &str) {
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

#[test]
fn expired_reaper_leaves_live_leases_untouched() {
    // The decision must be made by the same statement that fails the
    // row: a healthy build whose worker is heartbeating keeps running
    // and keeps its staging dir. (The select-then-update interleaving —
    // heartbeat landing between the two — cannot be reproduced in a
    // single-threaded test; the atomic UPDATE…RETURNING makes it
    // structurally impossible, and this test pins the WHERE clause that
    // the statement enforces at decision time.)
    let db = test_db();
    let conn = db.lock().unwrap();
    insert_build_job(
        &conn,
        "019d84bd-280d-7331-99c6-000000000009",
        "tpl-a",
        "running",
    );
    insert_build_job(
        &conn,
        "019d84bd-280d-7331-99c6-000000000007",
        "tpl-a",
        "running",
    );
    conn.execute(
        "UPDATE jobs SET lease_owner = 'live-worker',
             lease_expires_at = datetime('now', '+90 seconds')
         WHERE job_id = '019d84bd-280d-7331-99c6-000000000009'",
        [],
    )
    .unwrap();
    conn.execute(
        "UPDATE jobs SET lease_owner = 'dead-worker',
             lease_expires_at = datetime('now', '-1 second')
         WHERE job_id = '019d84bd-280d-7331-99c6-000000000007'",
        [],
    )
    .unwrap();

    let failed = fail_expired_running_builds(&conn, "interrupted").unwrap();
    assert_eq!(
        failed,
        vec![(
            "019d84bd-280d-7331-99c6-000000000007"
                .parse::<pplx_space_common_jobs::JobId>()
                .unwrap(),
            Some("tpl-a".to_string())
        )]
    );
    let (status, owner): (String, Option<String>) = conn
        .query_row(
            "SELECT status, lease_owner FROM jobs WHERE job_id = '019d84bd-280d-7331-99c6-000000000009'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(status, "running");
    assert_eq!(owner.as_deref(), Some("live-worker"));
}

#[test]
fn expired_running_build_reaper_returns_job_and_subject_ids() {
    let db = test_db();
    let conn = db.lock().unwrap();
    insert_build_job(
        &conn,
        "019d84bd-280d-7331-99c6-000000000008",
        "tpl-a",
        "running",
    );
    conn.execute(
        "UPDATE jobs
         SET lease_owner = 'dead-worker',
             lease_expires_at = datetime('now', '-1 second')
         WHERE job_id = '019d84bd-280d-7331-99c6-000000000008'",
        [],
    )
    .unwrap();

    let failed = fail_expired_running_builds(&conn, "interrupted").unwrap();
    assert_eq!(
        failed,
        vec![(
            "019d84bd-280d-7331-99c6-000000000008"
                .parse::<pplx_space_common_jobs::JobId>()
                .unwrap(),
            Some("tpl-a".to_string())
        )]
    );
    // Idempotent: nothing left to fail.
    assert_eq!(
        fail_expired_running_builds(&conn, "interrupted").unwrap(),
        vec![]
    );
}
