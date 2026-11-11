use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::Connection;
use tokio_util::sync::CancellationToken;

use super::*;

/// Runs until cancelled, so a submitted job stays active while its submitter
/// waits.
#[derive(Clone)]
struct PendingExecutor;

#[async_trait::async_trait]
impl JobExecutor for PendingExecutor {
    async fn execute(&self, _job: &Job, cancellation: CancellationToken) -> Result<i64, JobError> {
        cancellation.cancelled().await;
        Err(JobError::Permanent("cancelled".to_string()))
    }
}

fn test_worker() -> BackgroundWorker<PendingExecutor> {
    let connection = Connection::open_in_memory().unwrap();
    for migration in sqlx::migrate!("../../../../db/common")
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        connection.execute_batch(migration.sql.as_ref()).unwrap();
    }

    BackgroundWorker::new(
        Arc::new(Mutex::new(connection)),
        PendingExecutor,
        WorkerConfig::new(1, Duration::from_secs(60), 1),
    )
}

fn awaiting(worker: &BackgroundWorker<PendingExecutor>) -> Vec<JobId> {
    worker
        .awaiting_submitters
        .lock()
        .unwrap()
        .keys()
        .copied()
        .collect()
}

/// Submit a deduplicated pull job and check that it is registered for its
/// waiting submitter before the submission returns.
fn submit_registered(
    worker: &BackgroundWorker<PendingExecutor>,
    dedup_key: &str,
) -> Result<JobId, Status> {
    let job_id = worker.submit("pull", "{}", None, Some(dedup_key))?;
    assert_eq!(awaiting(worker), [job_id]);
    Ok(job_id)
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_a_waiting_submitter_removes_its_registration() {
    // Arrange
    let worker = test_worker();
    let call = worker.submit_and_wait(
        async { submit_registered(&worker, "dropped") },
        Duration::from_secs(60),
    );

    // Act
    let outcome = tokio::time::timeout(Duration::from_millis(20), call).await;

    // Assert
    outcome.expect_err("the job is still running, so the wait is dropped");
    assert!(awaiting(&worker).is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn a_submission_failing_after_insert_removes_its_registration() {
    // Arrange
    let worker = test_worker();
    let submission = async {
        submit_registered(&worker, "failed")?;
        Err(Status::internal("failed after the job was inserted"))
    };

    // Act
    let outcome = worker
        .submit_and_wait(submission, Duration::from_secs(60))
        .await;

    // Assert
    assert_eq!(outcome.unwrap_err().code(), tonic::Code::Internal);
    assert!(awaiting(&worker).is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn a_deduplicated_waiter_leaves_the_creating_waiter_registered() {
    // Arrange
    let worker = test_worker();
    let first = {
        let worker = worker.clone();

        tokio::spawn(async move {
            worker
                .submit_and_wait(
                    async { submit_registered(&worker, "shared") },
                    Duration::from_secs(60),
                )
                .await
        })
    };
    for _ in 0..100 {
        if !awaiting(&worker).is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }
    let created = awaiting(&worker);
    assert_eq!(created.len(), 1, "the first waiter registered its job");

    // Act
    let deduplicated = worker
        .submit_and_wait(
            async { worker.submit("pull", "{}", None, Some("shared")) },
            Duration::from_millis(20),
        )
        .await;

    // Assert
    assert_eq!(
        deduplicated.unwrap_err().code(),
        tonic::Code::DeadlineExceeded
    );
    assert_eq!(awaiting(&worker), created);
    first.abort();
}
