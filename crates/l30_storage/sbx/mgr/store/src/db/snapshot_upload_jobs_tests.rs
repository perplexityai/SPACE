//! The teardown's attempt budget: which permanently failed snapshot uploads
//! under one artifact's key count as attempts.

use super::snapshot_upload_jobs::{
    SNAPSHOT_UPLOAD_JOB_KIND, count_permanent_upload_failures_outside_scope,
};
use super::test_conn;
use pplx_space_common_core::types::{ExecutionGeneration, JobId};

fn insert_failed_upload(conn: &super::Connection, dedup_key: &str, payload: &str) {
    let job_id = JobId::new();
    pplx_space_common_jobs::db::insert_job(
        conn.raw(),
        pplx_space_common_jobs::db::InsertJob {
            job_id: &job_id,
            kind: SNAPSHOT_UPLOAD_JOB_KIND,
            payload,
            subject_id: Some("sandbox-1"),
            dedup_key: Some(dedup_key),
            max_attempts: 1,
            traceparent: None,
            priority: Default::default(),
        },
    )
    .unwrap();
    pplx_space_common_jobs::db::fail_job_permanent(conn.raw(), &job_id, "refused", None).unwrap();
}

/// A refusal of this node that the teardown can re-send isn't an attempt's
/// failure; one it can't re-send, and every attempt-scoped failure, are. So a
/// source-node refusal followed by an attempt-scoped failure leaves the second
/// attempt of the budget.
#[test]
fn only_a_resendable_source_node_refusal_stays_out_of_the_attempt_budget() {
    let conn = test_conn();
    let generation = ExecutionGeneration::new(7).unwrap();
    let count = |key| {
        count_permanent_upload_failures_outside_scope(&conn, key, "artifact", generation).unwrap()
    };
    for (key, payload, counted) in [
        (
            "resendable",
            r#"{"source_node_refused":true,"job_ids":[],"execution_generation":7}"#,
            0,
        ),
        (
            "no-generation",
            r#"{"source_node_refused":true,"job_ids":[]}"#,
            0,
        ),
        (
            "replayed",
            r#"{"source_node_refused":true,"job_ids":["j"],"execution_generation":7}"#,
            1,
        ),
        (
            "other-generation",
            r#"{"source_node_refused":true,"job_ids":[],"execution_generation":6}"#,
            1,
        ),
        ("attempt", r#"{"job_ids":[]}"#, 1),
        (
            "artifact",
            r#"{"failure_scope":"artifact","job_ids":[]}"#,
            0,
        ),
        ("not-json", "not json", 1),
    ] {
        insert_failed_upload(&conn, key, payload);
        assert_eq!(count(key), counted, "{key}");
    }
    insert_failed_upload(&conn, "resendable", r#"{"job_ids":[]}"#);
    assert_eq!(
        count("resendable"),
        1,
        "the attempt after a source-node refusal is the budget's first"
    );
}
