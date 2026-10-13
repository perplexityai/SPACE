use super::*;
use crate::fixture_job_id;

fn mem_conn() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    init_test_schema(&conn);
    conn
}

fn init_test_schema(conn: &Connection) {
    for migration in sqlx::migrate!("../../../../db/common")
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        conn.execute_batch(migration.sql.as_ref()).unwrap();
    }
}

#[test]
fn permanent_failure_reason_round_trips_through_job_row() {
    let conn = mem_conn();
    insert_job(
        &conn,
        InsertJob {
            job_id: &fixture_job_id("missing-object"),
            kind: "pull",
            payload: "{}",
            subject_id: None,
            dedup_key: None,
            max_attempts: 1,
            traceparent: None,
            priority: JobPriority::default(),
        },
    )
    .unwrap();

    fail_job_permanent(
        &conn,
        &fixture_job_id("missing-object"),
        "object missing",
        Some(JobFailureReason::NotFound),
    )
    .unwrap();

    let job = get_job(&conn, &fixture_job_id("missing-object"))
        .unwrap()
        .expect("failed job must remain queryable");
    assert_eq!(
        job.failure,
        Some(JobFailure::Permanent {
            reason: Some(JobFailureReason::NotFound),
        })
    );
    assert_eq!(job.error.as_deref(), Some("object missing"));
}

#[test]
fn failure_reason_requires_permanent_failure_kind() {
    let conn = mem_conn();
    insert_job(
        &conn,
        InsertJob {
            job_id: &fixture_job_id("invalid-failure"),
            kind: "pull",
            payload: "{}",
            subject_id: None,
            dedup_key: None,
            max_attempts: 1,
            traceparent: None,
            priority: JobPriority::default(),
        },
    )
    .unwrap();

    let error = conn
        .execute(
            "UPDATE jobs SET error_kind = 'retriable', failure_reason = 'not_found' WHERE job_id = ?1",
            [fixture_job_id("invalid-failure")],
        )
        .expect_err("retriable failures cannot carry a permanent domain reason");
    assert!(matches!(error, rusqlite::Error::SqliteFailure(_, _)));
}

#[test]
fn find_job_by_dedup_key_recovers_terminal_rows_that_find_active_job_skips() {
    // The recovery invariant for idempotent artifact batches: once the
    // original job completes, `find_active_job` (pending/running only)
    // stops matching, so a naive retry would mint a fresh job + DEK. The
    // status-agnostic `find_job_by_dedup_key` keeps recovering the same
    // row so the retry returns the persisted wrap instead.
    let conn = mem_conn();
    insert_job(
        &conn,
        InsertJob {
            job_id: &fixture_job_id("job-1"),
            kind: "push_artifact",
            payload: r#"{"wrapped_dek":[1,2,3]}"#,
            subject_id: Some("sandbox:x"),
            dedup_key: Some("artifact-1"),
            max_attempts: 5,
            traceparent: None,
            priority: JobPriority::default(),
        },
    )
    .unwrap();

    // While pending, both finders see it.
    assert_eq!(
        find_active_job(&conn, "push_artifact", "artifact-1").unwrap(),
        Some(fixture_job_id("job-1"))
    );
    assert_eq!(
        find_job_by_dedup_key(&conn, "push_artifact", "artifact-1")
            .unwrap()
            .map(|j| j.job_id),
        Some(fixture_job_id("job-1"))
    );

    // After completion, `find_active_job` misses; the status-agnostic
    // getter still recovers the row (with its persisted payload).
    assert!(mark_job_running(&conn, &fixture_job_id("job-1")).unwrap());
    complete_job(&conn, &fixture_job_id("job-1"), 123).unwrap();
    assert_eq!(
        find_active_job(&conn, "push_artifact", "artifact-1").unwrap(),
        None
    );
    let recovered = find_job_by_dedup_key(&conn, "push_artifact", "artifact-1")
        .unwrap()
        .expect("completed row must still be recoverable by dedup key");
    assert_eq!(recovered.job_id, fixture_job_id("job-1"));
    assert_eq!(recovered.status, "completed");
    assert_eq!(recovered.payload, r#"{"wrapped_dek":[1,2,3]}"#);
}

#[test]
fn find_job_by_dedup_key_scoped_by_kind_and_key() {
    let conn = mem_conn();
    insert_job(
        &conn,
        InsertJob {
            job_id: &fixture_job_id("job-1"),
            kind: "push_artifact",
            payload: "{}",
            subject_id: None,
            dedup_key: Some("artifact-1"),
            max_attempts: 5,
            traceparent: None,
            priority: JobPriority::default(),
        },
    )
    .unwrap();
    // Same key, different kind → no match (push and pull are separate).
    assert!(
        find_job_by_dedup_key(&conn, "pull_artifact", "artifact-1")
            .unwrap()
            .is_none()
    );
    // Same kind, different key → no match.
    assert!(
        find_job_by_dedup_key(&conn, "push_artifact", "artifact-2")
            .unwrap()
            .is_none()
    );
}

#[test]
fn prune_terminal_jobs_uses_completed_at_and_longer_failed_retention() {
    let conn = mem_conn();
    insert_pending_with_subject(&conn, fixture_job_id("completed-old"), None);
    insert_pending_with_subject(&conn, fixture_job_id("completed-long-running"), None);
    insert_pending_with_subject(&conn, fixture_job_id("failed-old"), None);
    insert_pending_with_subject(&conn, fixture_job_id("failed-long-running"), None);
    insert_pending_with_subject(&conn, fixture_job_id("legacy-completed-old"), None);
    insert_pending_with_subject(&conn, fixture_job_id("legacy-failed-recent"), None);
    insert_pending_with_subject(&conn, fixture_job_id("pending-old"), None);
    insert_pending_with_subject(&conn, fixture_job_id("running-old"), None);

    assert!(mark_job_running(&conn, &fixture_job_id("completed-old")).unwrap());
    complete_job(&conn, &fixture_job_id("completed-old"), 1).unwrap();
    assert!(mark_job_running(&conn, &fixture_job_id("completed-long-running")).unwrap());
    complete_job(&conn, &fixture_job_id("completed-long-running"), 1).unwrap();
    fail_job_permanent(&conn, &fixture_job_id("failed-old"), "failed", None).unwrap();
    fail_job_permanent(
        &conn,
        &fixture_job_id("failed-long-running"),
        "failed",
        None,
    )
    .unwrap();
    assert!(mark_job_running(&conn, &fixture_job_id("legacy-completed-old")).unwrap());
    complete_job(&conn, &fixture_job_id("legacy-completed-old"), 1).unwrap();
    fail_job_permanent(
        &conn,
        &fixture_job_id("legacy-failed-recent"),
        "failed",
        None,
    )
    .unwrap();
    mark_job_running(&conn, &fixture_job_id("running-old")).unwrap();

    set_created_at_modifier(&conn, fixture_job_id("completed-old"), "-30 days");
    set_completed_at_modifier(&conn, fixture_job_id("completed-old"), "-5 days");
    set_created_at_modifier(&conn, fixture_job_id("completed-long-running"), "-30 days");
    set_completed_at_modifier(&conn, fixture_job_id("completed-long-running"), "-3 days");
    set_created_at_modifier(&conn, fixture_job_id("failed-old"), "-30 days");
    set_completed_at_modifier(&conn, fixture_job_id("failed-old"), "-11 days");
    set_created_at_modifier(&conn, fixture_job_id("failed-long-running"), "-30 days");
    set_completed_at_modifier(&conn, fixture_job_id("failed-long-running"), "-5 days");
    set_created_at_modifier(&conn, fixture_job_id("legacy-completed-old"), "-5 days");
    clear_completed_at(&conn, fixture_job_id("legacy-completed-old"));
    set_created_at_modifier(&conn, fixture_job_id("legacy-failed-recent"), "-5 days");
    clear_completed_at(&conn, fixture_job_id("legacy-failed-recent"));
    set_created_at_modifier(&conn, fixture_job_id("pending-old"), "-11 days");
    set_created_at_modifier(&conn, fixture_job_id("running-old"), "-11 days");

    let pruned = prune_terminal_jobs(
        &conn,
        Duration::from_secs(4 * 24 * 60 * 60),
        Duration::from_secs(10 * 24 * 60 * 60),
    )
    .unwrap();

    assert_eq!(pruned, 3);
    assert!(
        get_job(&conn, &fixture_job_id("completed-old"))
            .unwrap()
            .is_none()
    );
    assert!(
        get_job(&conn, &fixture_job_id("failed-old"))
            .unwrap()
            .is_none()
    );
    assert!(
        get_job(&conn, &fixture_job_id("legacy-completed-old"))
            .unwrap()
            .is_none()
    );
    assert!(
        get_job(&conn, &fixture_job_id("completed-long-running"))
            .unwrap()
            .is_some()
    );
    assert!(
        get_job(&conn, &fixture_job_id("failed-long-running"))
            .unwrap()
            .is_some()
    );
    assert!(
        get_job(&conn, &fixture_job_id("legacy-failed-recent"))
            .unwrap()
            .is_some()
    );
    assert!(
        get_job(&conn, &fixture_job_id("pending-old"))
            .unwrap()
            .is_some()
    );
    assert!(
        get_job(&conn, &fixture_job_id("running-old"))
            .unwrap()
            .is_some()
    );
}

fn insert_pending_with_subject(conn: &Connection, job_id: JobId, subject_id: Option<&str>) {
    insert_job(
        conn,
        InsertJob {
            job_id: &job_id,
            kind: "build",
            payload: "{}",
            subject_id,
            dedup_key: None,
            max_attempts: 1,
            traceparent: None,
            priority: JobPriority::default(),
        },
    )
    .unwrap();
}

fn set_created_at(conn: &Connection, job_id: JobId, created_at: &str) {
    conn.execute(
        "UPDATE jobs SET created_at = ?2 WHERE job_id = ?1",
        params![job_id, created_at],
    )
    .unwrap();
}

fn set_created_at_modifier(conn: &Connection, job_id: JobId, modifier: &str) {
    conn.execute(
        "UPDATE jobs SET created_at = datetime('now', ?2) WHERE job_id = ?1",
        params![job_id, modifier],
    )
    .unwrap();
}

fn set_completed_at_modifier(conn: &Connection, job_id: JobId, modifier: &str) {
    conn.execute(
        "UPDATE jobs SET completed_at = datetime('now', ?2) WHERE job_id = ?1",
        params![job_id, modifier],
    )
    .unwrap();
}

fn clear_completed_at(conn: &Connection, job_id: JobId) {
    conn.execute(
        "UPDATE jobs SET completed_at = NULL WHERE job_id = ?1",
        params![job_id],
    )
    .unwrap();
}

fn claim(conn: &Connection, job_id: JobId, subject_concurrency: SubjectConcurrency) -> Option<Job> {
    try_claim_pending_job(
        conn,
        &job_id,
        "worker-1",
        Duration::from_secs(30),
        subject_concurrency,
    )
    .unwrap()
}

#[test]
fn serialized_claim_blocks_same_subject_while_one_runs() {
    let conn = mem_conn();
    insert_pending_with_subject(&conn, fixture_job_id("job-1"), Some("tpl-a"));
    insert_pending_with_subject(&conn, fixture_job_id("job-2"), Some("tpl-a"));
    set_created_at(&conn, fixture_job_id("job-2"), "2999-01-01 00:00:01");

    assert!(
        claim(
            &conn,
            fixture_job_id("job-1"),
            SubjectConcurrency::Serialized
        )
        .is_some()
    );
    assert!(
        claim(
            &conn,
            fixture_job_id("job-2"),
            SubjectConcurrency::Serialized
        )
        .is_none()
    );

    assert!(complete_leased_job(&conn, &fixture_job_id("job-1"), "worker-1", 0).unwrap());
    assert!(
        claim(
            &conn,
            fixture_job_id("job-2"),
            SubjectConcurrency::Serialized
        )
        .is_some()
    );
}

#[test]
fn serialized_claim_enforces_fifo_among_pending() {
    let conn = mem_conn();
    insert_pending_with_subject(&conn, fixture_job_id("job-1"), Some("tpl-a"));
    insert_pending_with_subject(&conn, fixture_job_id("job-2"), Some("tpl-a"));
    set_created_at(&conn, fixture_job_id("job-1"), "2020-01-01 00:00:00");
    set_created_at(&conn, fixture_job_id("job-2"), "2020-01-01 00:00:01");

    // The newer pending job cannot jump the queue while the older one is
    // still pending; the older job claims fine.
    assert!(
        claim(
            &conn,
            fixture_job_id("job-2"),
            SubjectConcurrency::Serialized
        )
        .is_none()
    );
    assert!(
        claim(
            &conn,
            fixture_job_id("job-1"),
            SubjectConcurrency::Serialized
        )
        .is_some()
    );

    // Equal created_at: job_id breaks the tie (UUIDv7 ids are
    // time-ordered, so lexicographic order is submission order).
    insert_pending_with_subject(&conn, fixture_job_id("job-3"), Some("tpl-b"));
    insert_pending_with_subject(&conn, fixture_job_id("job-4"), Some("tpl-b"));
    set_created_at(&conn, fixture_job_id("job-3"), "2020-01-01 00:00:02");
    set_created_at(&conn, fixture_job_id("job-4"), "2020-01-01 00:00:02");
    assert!(
        claim(
            &conn,
            fixture_job_id("job-4"),
            SubjectConcurrency::Serialized
        )
        .is_none()
    );
    assert!(
        claim(
            &conn,
            fixture_job_id("job-3"),
            SubjectConcurrency::Serialized
        )
        .is_some()
    );
}

#[test]
fn serialized_claim_ignores_null_and_distinct_subjects() {
    let conn = mem_conn();
    insert_pending_with_subject(&conn, fixture_job_id("job-1"), Some("tpl-a"));
    insert_pending_with_subject(&conn, fixture_job_id("job-2"), Some("tpl-b"));
    insert_pending_with_subject(&conn, fixture_job_id("job-3"), None);
    insert_pending_with_subject(&conn, fixture_job_id("job-4"), None);

    // Distinct subjects run in parallel.
    assert!(
        claim(
            &conn,
            fixture_job_id("job-1"),
            SubjectConcurrency::Serialized
        )
        .is_some()
    );
    assert!(
        claim(
            &conn,
            fixture_job_id("job-2"),
            SubjectConcurrency::Serialized
        )
        .is_some()
    );
    // NULL subjects never serialize, even against each other.
    assert!(
        claim(
            &conn,
            fixture_job_id("job-3"),
            SubjectConcurrency::Serialized
        )
        .is_some()
    );
    assert!(
        claim(
            &conn,
            fixture_job_id("job-4"),
            SubjectConcurrency::Serialized
        )
        .is_some()
    );
}

#[test]
fn parallel_claim_is_unchanged() {
    let conn = mem_conn();
    insert_pending_with_subject(&conn, fixture_job_id("job-1"), Some("tpl-a"));
    insert_pending_with_subject(&conn, fixture_job_id("job-2"), Some("tpl-a"));

    assert!(claim(&conn, fixture_job_id("job-1"), SubjectConcurrency::Parallel).is_some());
    assert!(claim(&conn, fixture_job_id("job-2"), SubjectConcurrency::Parallel).is_some());
}

#[test]
fn serialized_listing_skips_blocked_subjects() {
    let conn = mem_conn();
    insert_pending_with_subject(&conn, fixture_job_id("job-a1"), Some("tpl-a"));
    insert_pending_with_subject(&conn, fixture_job_id("job-a2"), Some("tpl-a"));
    insert_pending_with_subject(&conn, fixture_job_id("job-b1"), Some("tpl-b"));
    set_created_at(&conn, fixture_job_id("job-a2"), "2999-01-01 00:00:01");
    assert!(
        claim(
            &conn,
            fixture_job_id("job-a1"),
            SubjectConcurrency::Serialized
        )
        .is_some()
    );

    let serialized: Vec<JobId> = list_due_pending_jobs(&conn, 10, SubjectConcurrency::Serialized)
        .unwrap()
        .into_iter()
        .map(|(job_id, _, _)| job_id)
        .collect();
    assert_eq!(serialized, vec![fixture_job_id("job-b1")]);

    let parallel = list_due_pending_jobs(&conn, 10, SubjectConcurrency::Parallel).unwrap();
    assert_eq!(parallel.len(), 2);
}

#[test]
fn completion_evidence_is_atomic_and_fenced() {
    for leased in [false, true] {
        let conn = mem_conn();
        let id = fixture_job_id("content-proof");
        insert_job(
            &conn,
            InsertJob {
                job_id: &id,
                kind: "push",
                payload: "{\"source\":\"owned\"}",
                subject_id: None,
                dedup_key: None,
                max_attempts: 3,
                traceparent: None,
                priority: JobPriority::default(),
            },
        )
        .unwrap();
        conn.execute(
            "UPDATE jobs SET status='running',attempts=2,lease_owner='current' WHERE job_id=?1",
            [id],
        )
        .unwrap();
        let complete = |owner, attempt, payload| {
            if leased {
                complete_leased_job_with_payload(
                    &conn,
                    &id,
                    owner,
                    Some(attempt),
                    14,
                    Some(payload),
                )
            } else {
                complete_job_attempt_with_payload(&conn, &id, attempt, 14, Some(payload))
            }
        };
        assert!(!complete("previous", 1, "wrong-proof").unwrap());
        assert!(!complete("current", 1, "stale-attempt-proof").unwrap());
        let unchanged = get_job(&conn, &id).unwrap().unwrap();
        assert_eq!(unchanged.status, "running");
        assert_eq!(unchanged.payload, "{\"source\":\"owned\"}");
        assert!(
            complete(
                "current",
                2,
                "{\"source\":\"owned\",\"proof\":\"verified\"}"
            )
            .unwrap()
        );
        let completed = get_job(&conn, &id).unwrap().unwrap();
        assert_eq!(completed.status, "completed");
        assert_eq!(completed.bytes_processed, 14);
        assert!(completed.payload.contains("verified"));
        assert!(!complete("current", 2, "replacement-proof").unwrap());
        assert_eq!(
            get_job(&conn, &id).unwrap().unwrap().payload,
            completed.payload
        );
    }
}
