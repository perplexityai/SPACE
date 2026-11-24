use super::*;

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

const JOB: &str = "019d84bd-280d-7331-99c6-000000000001";

fn test_db() -> Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    let service = sqlx::migrate!("../../../../../db/build-manager");
    let common = sqlx::migrate!("../../../../../db/common");
    for migration in common
        .iter()
        .chain(service.iter())
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        conn.execute_batch(migration.sql.as_ref()).unwrap();
    }
    Connection(conn)
}

fn job_id(id: &str) -> JobId {
    id.parse().unwrap()
}

fn insert_build_job(conn: &Connection, job_id: &str, subject_id: &str, status: &str) {
    let conn = &conn.0;
    conn.execute(
        "INSERT INTO jobs (job_id, kind, payload, subject_id, status)
         VALUES (?1, ?2, '{}', ?3, ?4)",
        params![job_id, BUILD_JOB_KIND, subject_id, status],
    )
    .unwrap();
}

fn lease(conn: &Connection, job_id: &str, owner: Option<&str>, expires_in: Option<&str>) {
    let conn = &conn.0;
    conn.execute(
        "UPDATE jobs
         SET lease_owner = ?2,
             lease_expires_at = CASE WHEN ?3 IS NULL THEN NULL ELSE datetime('now', ?3) END
         WHERE job_id = ?1",
        params![job_id, owner, expires_in],
    )
    .unwrap();
}

type JobRow = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn job_row(conn: &Connection, job_id: &str) -> JobRow {
    let conn = &conn.0;
    conn.query_row(
        "SELECT status, error_kind, error, completed_at, lease_owner, lease_expires_at
         FROM jobs WHERE job_id = ?1",
        params![job_id],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        },
    )
    .unwrap()
}

fn succeeded() -> BuildJobResult {
    BuildJobResult {
        template_id: "seed".to_string(),
        state: BuildTerminalState::Succeeded,
        content_hash: Some("sha256:abc".to_string()),
        error: None,
    }
}

#[test]
fn leased_result_write_requires_current_owner() {
    let conn = test_db();
    insert_build_job(&conn, JOB, "seed", "running");
    lease(&conn, JOB, Some("worker-1"), Some("+60 seconds"));

    // Losing case: another worker's write inserts nothing.
    assert!(
        !record_build_result_as_lease_owner(&conn, &job_id(JOB), "worker-2", &succeeded()).unwrap()
    );
    assert!(get_build_result(&conn, &job_id(JOB)).unwrap().is_none());

    assert!(
        record_build_result_as_lease_owner(&conn, &job_id(JOB), "worker-1", &succeeded()).unwrap()
    );
    let stored = get_build_result(&conn, &job_id(JOB)).unwrap().unwrap();
    assert_eq!(stored.template_id, "seed");
    assert_eq!(stored.state, BuildTerminalState::Succeeded);

    // Losing case: once the lease moves on, the old owner's conflicting
    // write leaves the recorded result as it was.
    lease(&conn, JOB, Some("worker-2"), Some("+60 seconds"));
    let stale = BuildJobResult {
        template_id: "seed".to_string(),
        state: BuildTerminalState::Failed,
        content_hash: None,
        error: Some("stale worker".to_string()),
    };
    assert!(!record_build_result_as_lease_owner(&conn, &job_id(JOB), "worker-1", &stale).unwrap());
    let stored = get_build_result(&conn, &job_id(JOB)).unwrap().unwrap();
    assert_eq!(stored.state, BuildTerminalState::Succeeded);
    assert_eq!(stored.content_hash.as_deref(), Some("sha256:abc"));
    assert_eq!(stored.error, None);
}

#[test]
fn leased_result_write_requires_running_job() {
    let conn = test_db();
    insert_build_job(&conn, JOB, "seed", "failed");
    lease(&conn, JOB, Some("worker-1"), Some("+60 seconds"));

    assert!(
        !record_build_result_as_lease_owner(&conn, &job_id(JOB), "worker-1", &succeeded()).unwrap()
    );
    assert!(get_build_result(&conn, &job_id(JOB)).unwrap().is_none());
}

#[test]
fn leased_result_write_requires_named_job() {
    // Losing case for the guard's job id: worker-1 holds a running job's
    // lease, but not the lease of the job it names.
    let conn = test_db();
    let other = "019d84bd-280d-7331-99c6-000000000002";
    insert_build_job(&conn, JOB, "seed", "running");
    insert_build_job(&conn, other, "seed", "running");
    lease(&conn, JOB, Some("worker-1"), Some("+60 seconds"));
    lease(&conn, other, Some("worker-2"), Some("+60 seconds"));

    assert!(
        !record_build_result_as_lease_owner(&conn, &job_id(other), "worker-1", &succeeded())
            .unwrap()
    );
    assert!(get_build_result(&conn, &job_id(other)).unwrap().is_none());
}

#[test]
fn expired_running_build_reaper_marks_null_lease_rows_failed() {
    let conn = test_db();
    insert_build_job(&conn, JOB, "seed", "running");

    let failed = fail_expired_running_builds(&conn, "interrupted").unwrap();
    assert_eq!(failed, vec![(job_id(JOB), Some("seed".to_string()))]);

    let (status, error_kind, error, completed_at, owner, expires_at) = job_row(&conn, JOB);
    assert_eq!(status, "failed");
    assert_eq!(error_kind.as_deref(), Some("permanent"));
    assert_eq!(error.as_deref(), Some("interrupted"));
    assert!(completed_at.is_some());
    assert_eq!((owner, expires_at), (None, None));
}

#[test]
fn expired_reaper_leaves_live_leases_untouched() {
    // The decision is made by the same statement that fails the row: a
    // healthy build whose worker is heartbeating keeps running. (The
    // select-then-update interleaving cannot be reproduced in a
    // single-threaded test; this pins the WHERE clause the statement
    // enforces at decision time.)
    let conn = test_db();
    let live = "019d84bd-280d-7331-99c6-000000000009";
    let dead = "019d84bd-280d-7331-99c6-000000000007";
    insert_build_job(&conn, live, "tpl-a", "running");
    insert_build_job(&conn, dead, "tpl-a", "running");
    lease(&conn, live, Some("live-worker"), Some("+90 seconds"));
    lease(&conn, dead, Some("dead-worker"), Some("-1 second"));
    let live_before = job_row(&conn, live);

    let failed = fail_expired_running_builds(&conn, "interrupted").unwrap();
    assert_eq!(failed, vec![(job_id(dead), Some("tpl-a".to_string()))]);
    assert_eq!(job_row(&conn, live), live_before);
}

#[test]
fn expired_reaper_ignores_other_kinds_and_jobs_not_running() {
    let conn = test_db();
    let other_kind = "019d84bd-280d-7331-99c6-00000000000a";
    let finished = "019d84bd-280d-7331-99c6-00000000000b";
    let cancelling = "019d84bd-280d-7331-99c6-00000000000c";
    conn.0
        .execute(
            "INSERT INTO jobs (job_id, kind, payload, subject_id, status)
         VALUES (?1, 'other', '{}', 'tpl-a', 'running')",
            params![other_kind],
        )
        .unwrap();
    lease(&conn, other_kind, Some("dead-worker"), Some("-1 second"));
    insert_build_job(&conn, finished, "tpl-a", "completed");
    // Requesting cancellation leaves the lease in place.
    insert_build_job(&conn, cancelling, "tpl-a", "cancelling");
    lease(&conn, cancelling, Some("dead-worker"), Some("-1 second"));
    let other_before = job_row(&conn, other_kind);
    let finished_before = job_row(&conn, finished);
    let cancelling_before = job_row(&conn, cancelling);

    assert_eq!(
        fail_expired_running_builds(&conn, "interrupted").unwrap(),
        vec![]
    );
    assert_eq!(job_row(&conn, other_kind), other_before);
    assert_eq!(job_row(&conn, finished), finished_before);
    assert_eq!(job_row(&conn, cancelling), cancelling_before);
}

#[test]
fn expired_running_build_reaper_is_idempotent() {
    let conn = test_db();
    insert_build_job(&conn, JOB, "tpl-a", "running");
    lease(&conn, JOB, Some("dead-worker"), Some("-1 second"));

    assert_eq!(
        fail_expired_running_builds(&conn, "interrupted").unwrap(),
        vec![(job_id(JOB), Some("tpl-a".to_string()))]
    );
    let after_first = job_row(&conn, JOB);
    assert_eq!(after_first.0, "failed");
    assert_eq!((&after_first.4, &after_first.5), (&None, &None));
    // Nothing left to fail, and the failed row stays as the first pass left it.
    assert_eq!(
        fail_expired_running_builds(&conn, "interrupted").unwrap(),
        vec![]
    );
    assert_eq!(job_row(&conn, JOB), after_first);
}
