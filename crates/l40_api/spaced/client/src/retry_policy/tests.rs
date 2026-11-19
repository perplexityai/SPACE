use super::*;

#[tokio::test]
async fn unrepresentable_timeouts_fail_closed_without_running_the_operation() {
    for context in [
        RetryContext::with_timeout(Duration::MAX),
        RetryContext::none().limited_by_timeout(Duration::MAX),
    ] {
        let error =
            retry_status_with_context("test", "overflow", RetryPolicy::none(), context, || async {
                Ok::<(), tonic::Status>(())
            })
            .await
            .expect_err("unrepresentable timeout");
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    }
}

#[test]
fn retry_delays_respect_delay_and_step_caps() {
    for (steps, expected) in [
        (6, [20, 40, 80, 100, 100, 100]),
        (3, [20, 40, 80, 80, 80, 80]),
    ] {
        let policy = RetryPolicy {
            min_delay: Duration::from_millis(20),
            max_delay: Duration::from_millis(100),
            steps,
            max_retry_count: 6,
        };
        assert_eq!(
            policy.delays(),
            expected.map(Duration::from_millis),
            "steps={steps}"
        );
        for index in 0..6 {
            let envelope = policy.delay(index).unwrap();
            for _ in 0..32 {
                let jittered = policy.jittered_delay(index).unwrap();
                assert!((envelope - envelope / 5..=envelope).contains(&jittered));
            }
        }
        assert_eq!(policy.jittered_delay(6), None);
    }
}

#[tokio::test(start_paused = true)]
async fn retry_context_deadline_stops_retries() {
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts_for_call = std::sync::Arc::clone(&attempts);
    let status = retry_status_with_context(
        "test",
        "deadline",
        RetryPolicy {
            min_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(50),
            steps: 1,
            max_retry_count: 10,
        },
        RetryContext::with_timeout(Duration::from_millis(1)),
        move || {
            let attempts = std::sync::Arc::clone(&attempts_for_call);
            async move {
                attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err::<(), _>(tonic::Status::unavailable("retry"))
            }
        },
    )
    .await
    .expect_err("deadline should stop retry");

    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    assert!(status_is_request_deadline_exceeded(&status));
    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "deadline should prevent the second attempt"
    );
}

#[tokio::test(start_paused = true)]
async fn retry_context_deadline_interrupts_pending_operation() {
    let status = retry_status_with_context(
        "test",
        "pending",
        RetryPolicy {
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            steps: 1,
            max_retry_count: 1,
        },
        RetryContext::with_timeout(Duration::from_millis(10)),
        || async { std::future::pending::<Result<(), tonic::Status>>().await },
    )
    .await
    .expect_err("deadline should interrupt pending operation");

    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    assert!(status_is_request_deadline_exceeded(&status));
}

#[tokio::test]
async fn retry_context_cancellation_stops_retries() {
    let cancellation = CancellationToken::new();
    let cancel_on_retry = cancellation.clone();
    let status = retry_status_with_context_and_retry_hook(
        "test",
        "cancel",
        RetryPolicy {
            min_delay: Duration::from_secs(60),
            max_delay: Duration::from_secs(60),
            steps: 1,
            max_retry_count: 10,
        },
        RetryContext::none().with_cancellation(cancellation),
        || async { Err::<(), _>(tonic::Status::unavailable("retry")) },
        move || {
            let cancel_on_retry = cancel_on_retry.clone();
            async move {
                cancel_on_retry.cancel();
            }
        },
    )
    .await
    .expect_err("cancellation should stop retry");

    assert_eq!(status.code(), tonic::Code::Cancelled);
}

#[tokio::test]
async fn grpc_timeout_metadata_clamps_retry_deadline() {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        "grpc-timeout",
        tonic::metadata::MetadataValue::from_static("1m"),
    );
    let status = retry_status_with_context(
        "test",
        "grpc-timeout",
        RetryPolicy {
            min_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(50),
            steps: 1,
            max_retry_count: 10,
        },
        RetryContext::none().with_grpc_timeout_metadata(&metadata),
        || async { Err::<(), _>(tonic::Status::unavailable("retry")) },
    )
    .await
    .expect_err("grpc-timeout should stop retry");

    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
}
