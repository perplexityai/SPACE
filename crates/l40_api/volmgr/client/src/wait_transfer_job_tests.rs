use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;

fn completed_job(job_id: &str) -> GetTransferJobResponse {
    GetTransferJobResponse {
        job: Some(proto::TransferJob {
            job_id: job_id.to_string(),
            status: TransferJobStatus::Completed as i32,
            bytes_transferred: 42,
            ..Default::default()
        }),
    }
}

const JOB_ID: &str = "019d84bd-280d-7331-99c6-b63377f3a3a2";

fn submit_counting(
    calls: &Arc<AtomicUsize>,
) -> impl FnMut(
    PullFileRequest,
) -> std::future::Ready<Result<tonic::Response<SubmitTransferResponse>, tonic::Status>> {
    let calls = Arc::clone(calls);
    move |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(tonic::Response::new(SubmitTransferResponse {
            job_id: JOB_ID.to_string(),
        })))
    }
}

#[tokio::test]
async fn transient_wait_failure_retries_without_resubmitting() {
    // Arrange
    let submit_calls = Arc::new(AtomicUsize::new(0));
    let wait_calls = Arc::new(AtomicUsize::new(0));

    // Act
    let output = run_ensure_file_replica_with(
        PullFileRequest::default(),
        EnsureFileReplicaWaitConfig {
            max_total: Duration::from_secs(1),
        },
        submit_counting(&submit_calls),
        {
            let calls = Arc::clone(&wait_calls);
            move |request| {
                assert_eq!(request.job_id, JOB_ID);
                let attempt = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        Err(tonic::Status::unavailable("volume-manager restarting"))
                    } else {
                        Ok(tonic::Response::new(completed_job(JOB_ID)))
                    }
                }
            }
        },
    )
    .await
    .unwrap();

    // Assert
    assert_eq!(output.job_id.to_string(), JOB_ID);
    assert_eq!(output.bytes_present, 42);
    assert_eq!(submit_calls.load(Ordering::SeqCst), 1);
    assert_eq!(wait_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn non_retryable_wait_failure_is_returned() {
    // Arrange
    let submit_calls = Arc::new(AtomicUsize::new(0));
    let wait_calls = Arc::new(AtomicUsize::new(0));

    // Act
    let error = run_ensure_file_replica_with(
        PullFileRequest::default(),
        EnsureFileReplicaWaitConfig {
            max_total: Duration::from_secs(1),
        },
        submit_counting(&submit_calls),
        {
            let calls = Arc::clone(&wait_calls);
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err(tonic::Status::unimplemented("unknown method")) }
            }
        },
    )
    .await
    .unwrap_err();

    // Assert
    assert!(
        matches!(&error, EnsureFileReplicaError::Wait(status) if status.code() == tonic::Code::Unimplemented),
        "unexpected error: {error:?}"
    );
    assert_eq!(submit_calls.load(Ordering::SeqCst), 1);
    assert_eq!(wait_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn wait_request_is_bounded_by_the_total_budget() {
    // Arrange
    let max_total = Duration::from_millis(10);

    // Act
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        wait_transfer_job_with_retry(
            &JOB_ID.parse().unwrap(),
            tokio::time::Instant::now(),
            max_total,
            |_| async { std::future::pending().await },
        ),
    )
    .await;

    // Assert
    let error = result
        .expect("the total wait budget must cancel an in-flight RPC")
        .unwrap_err();
    assert!(matches!(error, WaitTransferJobError::DeadlineExceeded));
}
