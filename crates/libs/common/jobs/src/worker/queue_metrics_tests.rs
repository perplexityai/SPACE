use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_metrics::test_capture::capture_values;
use rusqlite::Connection;
use tokio_util::sync::CancellationToken;

use super::*;

#[derive(Clone)]
struct NoopExecutor;

#[async_trait::async_trait]
impl JobExecutor for NoopExecutor {
    async fn execute(&self, _job: &Job, _cancellation: CancellationToken) -> Result<i64, JobError> {
        Ok(0)
    }
}

fn test_worker() -> BackgroundWorker<NoopExecutor> {
    let connection = Connection::open_in_memory().unwrap();
    for migration in sqlx::migrate!("../../../../db/common")
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        connection.execute_batch(migration.sql.as_ref()).unwrap();
    }
    BackgroundWorker::new(
        Arc::new(Mutex::new(connection)),
        NoopExecutor,
        WorkerConfig::new(1, Duration::from_secs(5), 1),
    )
}

fn oldest_pending_age(worker: &BackgroundWorker<NoopExecutor>, kind: &str) -> Option<f64> {
    let tag = format!("kind:{kind}");
    capture_values(|| worker.emit_queue_metrics())
        .into_iter()
        .filter(|value| value.name == "jobs.queue.oldest_pending_age_seconds")
        .find(|value| value.tags.contains(&tag))
        .map(|value| value.value)
}

/// prod-2 2026-10-20: a sandbox_cleanup job stuck ~17 h finally drained, but
/// the gauge was only emitted for kinds with a pending row, so the series
/// stopped instead of dropping to 0 and the dashboard kept showing 17 h.
/// A drained kind must report 0.
#[test]
fn drained_kind_reports_zero_oldest_pending_age() {
    let worker = test_worker();
    let job_id = JobId::new();
    {
        let connection = worker.db.lock().unwrap();
        db::insert_job(
            &connection,
            db::InsertJob {
                job_id: &job_id,
                kind: "sandbox_cleanup",
                payload: "{}",
                subject_id: None,
                dedup_key: None,
                max_attempts: 1,
                traceparent: None,
                priority: JobPriority::default(),
            },
        )
        .unwrap();
        connection
            .execute(
                "UPDATE jobs SET created_at = datetime('now', '-17 hours')",
                [],
            )
            .unwrap();
    }
    let stuck = oldest_pending_age(&worker, "sandbox_cleanup").expect("pending kind is reported");
    assert!(stuck >= 17.0 * 3600.0 - 5.0, "stuck age {stuck}");

    {
        let connection = worker.db.lock().unwrap();
        db::mark_job_running(&connection, &job_id).unwrap();
        assert!(db::complete_job(&connection, &job_id, 0).unwrap());
    }
    let drained = oldest_pending_age(&worker, "sandbox_cleanup");
    eprintln!("sandbox_cleanup oldest_pending_age: while stuck={stuck}s, after drain={drained:?}");
    assert_eq!(
        drained,
        Some(0.0),
        "a drained kind must report 0, not stop reporting (which freezes the last value)"
    );
}
