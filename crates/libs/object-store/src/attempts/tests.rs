use super::*;
use crate::{
    ObjectCredentials, ObjectStore, ObjectStoreError, ObjectTarget, S3ObjectStore, S3Timeouts,
    s3_base_config,
};
use aws_sdk_s3::config::StalledStreamProtectionConfig;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_smithy_runtime_api::client::result::ConnectorError;
use axum::Router;
use axum::body::Bytes as BodyBytes;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct Opaque;

impl fmt::Display for Opaque {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("opaque")
    }
}

impl std::error::Error for Opaque {}

/// Mirrors the HTTP client's connect-timeout error, which names a
/// `TimedOutError` as its source.
#[derive(Debug)]
struct ConnectTimedOut(TimedOutError);

impl fmt::Display for ConnectTimedOut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HTTP connect timeout occurred")
    }
}

impl std::error::Error for ConnectTimedOut {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

#[test]
fn attempt_failures_are_classified_by_cause() {
    let connector = |error: ConnectorError| OrchestratorError::<Opaque>::connector(error);
    let cases = [
        (
            "stall cut: connector timeout with no timer in its chain",
            connector(ConnectorError::timeout(Box::new(Opaque))),
            None,
            None,
            S3AttemptFailureKind::StalledStream,
        ),
        (
            "connect timeout",
            connector(ConnectorError::timeout(Box::new(ConnectTimedOut(
                TimedOutError,
            )))),
            None,
            None,
            S3AttemptFailureKind::Io,
        ),
        (
            "connection reset",
            connector(ConnectorError::io(Box::new(std::io::Error::from(
                std::io::ErrorKind::ConnectionReset,
            )))),
            None,
            None,
            S3AttemptFailureKind::Io,
        ),
        (
            "unknown connector failure",
            connector(ConnectorError::other(Box::new(Opaque), None)),
            None,
            None,
            S3AttemptFailureKind::Other,
        ),
        (
            "attempt timeout",
            OrchestratorError::timeout(Box::new(TimedOutError)),
            None,
            None,
            S3AttemptFailureKind::AttemptTimeout,
        ),
        (
            "server error",
            OrchestratorError::operation(Opaque),
            Some(500),
            Some("InternalError"),
            S3AttemptFailureKind::Http5xx,
        ),
        (
            "503 throttles",
            OrchestratorError::operation(Opaque),
            Some(503),
            Some("ServiceUnavailable"),
            S3AttemptFailureKind::Throttled,
        ),
        (
            "429 throttles",
            OrchestratorError::operation(Opaque),
            Some(429),
            None,
            S3AttemptFailureKind::Throttled,
        ),
        (
            "throttling code on a 400",
            OrchestratorError::operation(Opaque),
            Some(400),
            Some("SlowDown"),
            S3AttemptFailureKind::Throttled,
        ),
        (
            "client error",
            OrchestratorError::operation(Opaque),
            Some(400),
            Some("RequestTimeout"),
            S3AttemptFailureKind::Http4xx,
        ),
        (
            "unreadable success response",
            OrchestratorError::response(Box::new(Opaque)),
            Some(200),
            None,
            S3AttemptFailureKind::Other,
        ),
    ];

    for (case, error, http_status, error_code, expected) in cases {
        assert_eq!(
            classify_attempt_failure(&error, http_status, error_code),
            expected,
            "{case}"
        );
    }
}

#[derive(Debug, Default)]
struct Recorder {
    failures: Mutex<Vec<FailedAttempt>>,
    recoveries: Mutex<Vec<RecoveredOperation>>,
}

impl Recorder {
    fn failures(&self) -> Vec<FailedAttempt> {
        self.failures.lock().unwrap().clone()
    }

    fn recoveries(&self) -> Vec<RecoveredOperation> {
        self.recoveries.lock().unwrap().clone()
    }
}

impl AttemptReporter for Recorder {
    fn attempt_failed(&self, _target: &AttemptTarget, failure: &FailedAttempt) {
        self.failures.lock().unwrap().push(failure.clone());
    }

    fn recovered(&self, _target: &AttemptTarget, recovery: &RecoveredOperation) {
        self.recoveries.lock().unwrap().push(recovery.clone());
    }
}

/// An in-process S3 stand-in that answers every request with `handler`.
async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind S3 stand-in");
    let address = listener.local_addr().expect("S3 stand-in address");
    tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("serve S3 stand-in");
    });
    format!("http://{address}")
}

/// A store wired exactly like production apart from the bounds under test.
/// Retry backoff is shortened so wall time is spent in the attempts
/// themselves; the strategy and its three attempts are the SDK standard.
fn store(timeouts: S3Timeouts, stall_grace: Duration, recorder: &Arc<Recorder>) -> S3ObjectStore {
    let conf = s3_base_config(timeouts, AttemptObserver::new(recorder.clone()))
        .retry_config(RetryConfig::standard().with_initial_backoff(Duration::from_millis(10)))
        .stalled_stream_protection(
            StalledStreamProtectionConfig::enabled()
                .grace_period(stall_grace)
                .build(),
        )
        .build();
    S3ObjectStore::with_base_config(conf)
}

fn credentials() -> ObjectCredentials {
    ObjectCredentials {
        access_key_id: "synthetic-access-key".to_string(),
        secret_access_key: "synthetic-secret-key".to_string(),
        session_token: None,
    }
}

fn target(endpoint: &str) -> ObjectTarget<'_> {
    ObjectTarget {
        bucket: "synthetic-bucket",
        key: "snapshots/synthetic/memory.bin",
        region: "us-east-1",
        endpoint: Some(endpoint),
        path_style: true,
    }
}

fn kinds_and_retries(failures: &[FailedAttempt]) -> Vec<(u32, S3AttemptFailureKind, bool)> {
    failures
        .iter()
        .map(|failure| (failure.attempt, failure.kind, failure.will_retry))
        .collect()
}

/// Reads the whole request, then never answers: the shape the SDK's
/// stalled-stream protection cannot see, because the body is complete.
fn never_answering(requests: Arc<AtomicUsize>) -> Router {
    Router::new().fallback(move |_body: BodyBytes| {
        let requests = requests.clone();
        async move {
            requests.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<Response>().await
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn never_answering_endpoint_is_retried_per_attempt_timeout_then_fails() {
    let requests = Arc::new(AtomicUsize::new(0));
    let endpoint = serve(never_answering(requests.clone())).await;
    let recorder = Arc::new(Recorder::default());
    let operation_timeout = Duration::from_secs(30);
    let store = store(
        S3Timeouts {
            attempt: Duration::from_millis(300),
            operation: operation_timeout,
        },
        Duration::from_secs(5),
        &recorder,
    );

    let started_at = Instant::now();
    let result = store
        .put_bytes(
            &credentials(),
            target(&endpoint),
            bytes::Bytes::from_static(b"payload"),
        )
        .await;

    assert!(
        matches!(result, Err(ObjectStoreError::Put(_))),
        "{result:?}"
    );
    assert!(
        started_at.elapsed() < operation_timeout,
        "attempt timeouts, not the operation timeout, should end the call"
    );
    assert_eq!(requests.load(Ordering::SeqCst), 3);
    assert_eq!(
        kinds_and_retries(&recorder.failures()),
        [
            (1, S3AttemptFailureKind::AttemptTimeout, true),
            (2, S3AttemptFailureKind::AttemptTimeout, true),
            (3, S3AttemptFailureKind::AttemptTimeout, false),
        ]
    );
    assert!(recorder.recoveries().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operation_timeout_bounds_an_attempt_that_never_answers() {
    let requests = Arc::new(AtomicUsize::new(0));
    let endpoint = serve(never_answering(requests.clone())).await;
    let recorder = Arc::new(Recorder::default());
    let attempt_timeout = Duration::from_secs(30);
    let operation_timeout = Duration::from_millis(500);
    let store = store(
        S3Timeouts {
            attempt: attempt_timeout,
            operation: operation_timeout,
        },
        Duration::from_secs(5),
        &recorder,
    );

    let started_at = Instant::now();
    let result = store
        .put_bytes(
            &credentials(),
            target(&endpoint),
            bytes::Bytes::from_static(b"payload"),
        )
        .await;
    let elapsed = started_at.elapsed();

    assert!(
        matches!(result, Err(ObjectStoreError::Put(_))),
        "{result:?}"
    );
    assert!(elapsed >= operation_timeout, "{elapsed:?}");
    assert!(elapsed < attempt_timeout, "{elapsed:?}");
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert_eq!(
        kinds_and_retries(&recorder.failures()),
        [(1, S3AttemptFailureKind::OperationTimeout, false)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_upload_is_cut_and_retried() {
    // Accepts the request but never reads its body, so the upload stops
    // once the socket buffers fill.
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    let endpoint = serve(Router::new().fallback(move || {
        let requests = counted.clone();
        async move {
            requests.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<Response>().await
        }
    }))
    .await;
    let recorder = Arc::new(Recorder::default());
    let store = store(
        S3Timeouts {
            attempt: Duration::from_secs(60),
            operation: Duration::from_secs(180),
        },
        Duration::from_millis(500),
        &recorder,
    );
    // Far larger than loopback socket buffers. Read from a file, as
    // snapshot uploads are: the stall check needs a body that streams in
    // chunks rather than one in-memory frame.
    let src = tempfile::NamedTempFile::new().expect("create upload source");
    src.as_file()
        .set_len(64 * 1024 * 1024)
        .expect("size upload source");

    let result = store
        .put_file(&credentials(), target(&endpoint), src.path())
        .await;

    assert!(
        matches!(result, Err(ObjectStoreError::Put(_))),
        "{result:?}"
    );
    assert_eq!(requests.load(Ordering::SeqCst), 3);
    assert_eq!(
        kinds_and_retries(&recorder.failures()),
        [
            (1, S3AttemptFailureKind::StalledStream, true),
            (2, S3AttemptFailureKind::StalledStream, true),
            (3, S3AttemptFailureKind::StalledStream, false),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn success_after_a_failed_attempt_reports_the_retry() {
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    let endpoint = serve(Router::new().fallback(move |_body: BodyBytes| {
        let requests = counted.clone();
        async move {
            if requests.fetch_add(1, Ordering::SeqCst) == 0 {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [("content-type", "application/xml")],
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <Error><Code>InternalError</Code><Message>synthetic</Message></Error>",
                )
                    .into_response()
            } else {
                (StatusCode::OK, [("etag", "\"synthetic-etag\"")]).into_response()
            }
        }
    }))
    .await;
    let recorder = Arc::new(Recorder::default());
    let store = store(
        S3Timeouts {
            attempt: Duration::from_secs(30),
            operation: Duration::from_secs(90),
        },
        Duration::from_secs(5),
        &recorder,
    );

    store
        .put_bytes(
            &credentials(),
            target(&endpoint),
            bytes::Bytes::from_static(b"payload"),
        )
        .await
        .expect("second attempt succeeds");

    let failures = recorder.failures();
    assert_eq!(
        kinds_and_retries(&failures),
        [(1, S3AttemptFailureKind::Http5xx, true)]
    );
    assert_eq!(failures[0].http_status, Some(500));
    assert_eq!(failures[0].error_code.as_deref(), Some("InternalError"));
    let recoveries = recorder.recoveries();
    assert_eq!(recoveries.len(), 1);
    assert_eq!(recoveries[0].operation, S3Operation::PutObject);
    assert_eq!(recoveries[0].attempts, 2);
}
