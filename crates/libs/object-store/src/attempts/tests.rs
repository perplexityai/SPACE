use super::*;
use crate::test_support::{
    Recorder, credentials, every_class, serve, store, store_with_pool_idle, target,
};
use crate::{ObjectStore, ObjectStoreError};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_types::retry::ErrorKind;
use axum::Router;
use axum::body::Bytes as BodyBytes;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use hyper_util::rt::TokioExecutor;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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

/// The error hyper's legacy client, which the SDK's HTTP client wraps,
/// returns when the server reads a request and closes the connection
/// without answering: `IncompleteMessage`.
async fn closed_before_response() -> BoxError {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind closing server");
    let address = listener.local_addr().expect("closing server address");
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept connection");
        let mut head = Vec::new();
        while !head.windows(4).any(|w| w == b"\r\n\r\n") {
            let mut chunk = [0_u8; 1024];
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(read) => head.extend_from_slice(&chunk[..read]),
            }
        }
    });
    let client =
        hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build_http::<String>();
    let uri = format!("http://{address}/")
        .parse()
        .expect("closing server URI");
    let error = client
        .get(uri)
        .await
        .expect_err("the server closes without answering");
    Box::new(error)
}

#[tokio::test]
async fn closed_connections_are_io_unless_the_request_body_failed() {
    let connector = |error: ConnectorError| OrchestratorError::<Opaque>::connector(error);
    let cases = [
        (
            // aws-smithy-http-client's `to_connector_error` maps hyper's
            // `IncompleteMessage` to this transient *other* error.
            "connection closed before the response head",
            connector(ConnectorError::other(
                closed_before_response().await,
                Some(ErrorKind::TransientError),
            )),
            S3AttemptFailureKind::Io,
        ),
        (
            "request body stream failed with an I/O cause",
            connector(ConnectorError::user(Box::new(std::io::Error::from(
                std::io::ErrorKind::UnexpectedEof,
            )))),
            S3AttemptFailureKind::Other,
        ),
        (
            "request body stream failed because its own source's connection closed",
            connector(ConnectorError::user(closed_before_response().await)),
            S3AttemptFailureKind::Other,
        ),
    ];

    for (case, error, expected) in cases {
        assert_eq!(
            classify_attempt_failure(&error, None, None),
            expected,
            "{case}: {error:?}"
        );
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
async fn operation_timeout_bounds_an_attempt_that_never_answers() {
    let requests = Arc::new(AtomicUsize::new(0));
    let endpoint = serve(never_answering(requests.clone())).await;
    let recorder = Arc::new(Recorder::default());
    let attempt_timeout = Duration::from_secs(30);
    let operation_timeout = Duration::from_millis(500);
    let store = store(
        every_class(attempt_timeout, operation_timeout),
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
        every_class(Duration::from_secs(60), Duration::from_secs(180)),
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
        every_class(Duration::from_secs(30), Duration::from_secs(90)),
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
    assert_eq!(recoveries[0].1.operation, S3Operation::PutObject);
    assert_eq!(recoveries[0].1.attempts, 2);
}

/// An S3 stand-in that answers every request `200 OK` on a keep-alive
/// connection and closes a connection once it has sat idle for
/// `idle_close`, as S3 does. The close takes effect when the client's next
/// request arrives, which is what the client sees when the server's close
/// and its request cross on the wire. A close sent ahead of the request
/// would reach the client's pool over loopback before its next checkout
/// and only show the race that rarely.
async fn closes_idle_connections(idle_close: Duration) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind S3 stand-in");
    let address = listener.local_addr().expect("S3 stand-in address");
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept connection");
            tokio::spawn(serve_until_idle(stream, idle_close));
        }
    });
    format!("http://{address}")
}

async fn serve_until_idle(mut stream: TcpStream, idle_close: Duration) {
    let mut buffered = Vec::new();
    let mut last_response: Option<Instant> = None;
    loop {
        let head_end = loop {
            if let Some(at) = buffered.windows(4).position(|w| w == b"\r\n\r\n") {
                break at + 4;
            }
            let mut chunk = [0_u8; 8192];
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(read) => buffered.extend_from_slice(&chunk[..read]),
            }
        };
        let idle = last_response.map(|at| at.elapsed());
        let head = String::from_utf8_lossy(&buffered[..head_end]).to_ascii_lowercase();
        let body_len = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .map_or(0, |len| {
                len.trim().parse::<usize>().expect("content-length")
            });
        while buffered.len() < head_end + body_len {
            let mut chunk = [0_u8; 8192];
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(read) => buffered.extend_from_slice(&chunk[..read]),
            }
        }
        buffered.drain(..head_end + body_len);
        if idle.is_some_and(|idle| idle >= idle_close) {
            // Closed with the request read, so the client sees a FIN rather
            // than a reset.
            return;
        }
        let response = "HTTP/1.1 200 OK\r\netag: \"synthetic-etag\"\r\ncontent-length: 0\r\n\r\n";
        if stream.write_all(response.as_bytes()).await.is_err() {
            return;
        }
        last_response = Some(Instant::now());
    }
}

/// Two uploads separated by an idle gap longer than the server's idle
/// close, through a store whose connection pool keeps idle connections for
/// `pool_idle_timeout`.
async fn uploads_across_an_idle_close(pool_idle_timeout: Duration) -> Arc<Recorder> {
    let idle_close = Duration::from_millis(600);
    let endpoint = closes_idle_connections(idle_close).await;
    let recorder = Arc::new(Recorder::default());
    let store = store_with_pool_idle(
        every_class(Duration::from_secs(10), Duration::from_secs(30)),
        Duration::from_secs(5),
        pool_idle_timeout,
        &recorder,
    );

    let credentials = credentials();
    let upload = || {
        store.put_bytes(
            &credentials,
            target(&endpoint),
            bytes::Bytes::from_static(b"payload"),
        )
    };
    upload().await.expect("first upload");
    tokio::time::sleep(idle_close * 2).await;
    upload().await.expect("second upload");
    recorder
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reusing_a_connection_the_server_closed_fails_as_io_and_is_retried() {
    let recorder = uploads_across_an_idle_close(Duration::from_secs(90)).await;

    let failures = recorder.failures();
    assert_eq!(
        kinds_and_retries(&failures),
        [(1, S3AttemptFailureKind::Io, true)],
        "{failures:?}"
    );
    assert!(
        failures[0]
            .error
            .contains("connection closed before message completed"),
        "{failures:?}"
    );
    assert_eq!(recorder.recoveries().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_drops_idle_connections_before_the_server_closes_them() {
    let recorder = uploads_across_an_idle_close(Duration::from_millis(200)).await;

    assert_eq!(recorder.failures(), []);
    assert_eq!(recorder.recoveries(), []);
}
