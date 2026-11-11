use std::sync::{Arc, Mutex};
use std::time::Duration;

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

fn running_job(worker: &BackgroundWorker<NoopExecutor>) -> JobId {
    let job_id = JobId::new();
    let connection = worker.db.lock().unwrap();
    db::insert_job(
        &connection,
        db::InsertJob {
            job_id: &job_id,
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
    db::mark_job_running(&connection, &job_id).unwrap();
    drop(connection);
    job_id
}

async fn wait_until_registered(worker: &BackgroundWorker<NoopExecutor>, job_id: &JobId) {
    for _ in 0..100 {
        if worker.completion_waiters.contains(job_id) {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("waiter did not register");
}

#[tokio::test(flavor = "current_thread")]
async fn completion_wakes_every_waiter_without_the_poll_fallback() {
    // Arrange
    let worker = test_worker();
    let job_id = running_job(&worker);
    let first = {
        let worker = worker.clone();

        tokio::spawn(async move { worker.wait_for_job(&job_id, Duration::from_secs(5)).await })
    };
    let second = {
        let worker = worker.clone();

        tokio::spawn(async move { worker.wait_for_job(&job_id, Duration::from_secs(5)).await })
    };
    wait_until_registered(&worker, &job_id).await;
    tokio::task::yield_now().await;

    // Act
    {
        let connection = worker.db.lock().unwrap();
        db::complete_job(&connection, &job_id, 1).unwrap();
    }
    worker.notify_completion(&job_id);
    let completed = tokio::time::timeout(Duration::from_millis(10), async {
        (first.await.unwrap(), second.await.unwrap())
    })
    .await;

    // Assert
    let (first, second) = completed.expect("all waiters should wake from one completion");
    assert_eq!(first.unwrap().status, "completed");
    assert_eq!(second.unwrap().status, "completed");
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_the_last_waiter_releases_notifier_state() {
    // Arrange
    let worker = test_worker();
    let job_id = running_job(&worker);
    let waiter = {
        let worker = worker.clone();

        tokio::spawn(async move { worker.wait_for_job(&job_id, Duration::from_secs(5)).await })
    };
    wait_until_registered(&worker, &job_id).await;

    // Act
    waiter.abort();
    waiter.await.unwrap_err();
    tokio::task::yield_now().await;

    // Assert
    assert!(!worker.completion_waiters.contains(&job_id));
}
