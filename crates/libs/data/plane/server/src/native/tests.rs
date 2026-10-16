use super::*;
use axum::extract::ws::Message;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
async fn process_start_http_errors_preserve_permanent_guest_failures() {
    use http_body_util::BodyExt;

    for (code, guest, expected_status, retryable, websocket_retryable) in [
        (
            tonic::Code::NotFound,
            true,
            StatusCode::NOT_FOUND,
            false,
            false,
        ),
        (
            tonic::Code::InvalidArgument,
            true,
            StatusCode::BAD_REQUEST,
            false,
            false,
        ),
        (
            tonic::Code::PermissionDenied,
            true,
            StatusCode::FORBIDDEN,
            false,
            false,
        ),
        (
            tonic::Code::FailedPrecondition,
            true,
            StatusCode::BAD_REQUEST,
            false,
            false,
        ),
        (
            tonic::Code::NotFound,
            false,
            StatusCode::SERVICE_UNAVAILABLE,
            true,
            false,
        ),
        (
            tonic::Code::PermissionDenied,
            false,
            StatusCode::SERVICE_UNAVAILABLE,
            true,
            false,
        ),
        (
            tonic::Code::InvalidArgument,
            false,
            StatusCode::SERVICE_UNAVAILABLE,
            true,
            false,
        ),
        (
            tonic::Code::Internal,
            false,
            StatusCode::SERVICE_UNAVAILABLE,
            true,
            false,
        ),
        (
            tonic::Code::Unavailable,
            false,
            StatusCode::SERVICE_UNAVAILABLE,
            true,
            true,
        ),
        (
            tonic::Code::DeadlineExceeded,
            false,
            StatusCode::SERVICE_UNAVAILABLE,
            true,
            false,
        ),
    ] {
        let mut status = tonic::Status::new(code, "synthetic process failure");
        if guest {
            status
                .metadata_mut()
                .insert("space-guest-process-error", "terminal".parse().unwrap());
        }
        let error = StreamBridgeError::BackendStatus(status);
        let response = process_stream_start_http_error(&error);
        assert_eq!(
            response.status(),
            expected_status,
            "{code:?}, guest={guest}"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["retryable"], retryable, "{code:?}: {body}");
        let DataPlaneServerFrame::Error {
            retryable: actual_websocket_retryable,
            ..
        } = stream_start_error_frame(&error)
        else {
            panic!("expected error frame")
        };
        assert_eq!(
            actual_websocket_retryable, websocket_retryable,
            "WebSocket opening errors retain the established stream retry contract"
        );
    }
}

struct OpenDropped(Arc<AtomicBool>);
impl Drop for OpenDropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

async fn pending_open(dropped: Arc<AtomicBool>) {
    let _guard = OpenDropped(dropped);
    std::future::pending::<()>().await
}

#[tokio::test]
async fn cdp_discovery_response_is_bounded() {
    let proxy_base = CdpProxyBase {
        http_base: "https://space.example.com/v1/sessions/proto_1/proxy".to_string(),
        ws_base: "wss://space.example.com/v1/sessions/proto_1/proxy".to_string(),
    };
    let response =
        |body: Vec<u8>| hyper::Response::new(http_body_util::Full::new(bytes::Bytes::from(body)));

    let discovery = br#"[{"webSocketDebuggerUrl":"ws://127.0.0.1:9222/devtools/page/1"}]"#;
    let rewritten = rewrite_cdp_discovery_response(response(discovery.to_vec()), &proxy_base)
        .await
        .unwrap();
    let body = http_body_util::BodyExt::collect(rewritten.into_body())
        .await
        .unwrap()
        .to_bytes();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        value[0]["webSocketDebuggerUrl"],
        "wss://space.example.com/v1/sessions/proto_1/proxy/devtools/page/1"
    );

    let oversized = vec![b' '; CDP_DISCOVERY_BODY_LIMIT_BYTES + 1];
    let error = rewrite_cdp_discovery_response(response(oversized), &proxy_base)
        .await
        .unwrap_err();
    assert!(matches!(error, ProxyError::Upstream(_)));
}

#[test]
fn cdp_rewrite_applies_only_to_endpoints_with_debugger_urls() {
    for path in [
        "/json",
        "/json/",
        "/json/list",
        "/json/version",
        "/json/new",
    ] {
        assert!(is_cdp_discovery_path(path), "{path}");
    }
    for path in [
        "/json/protocol",
        "/json/activate/page-1",
        "/json/close/page-1",
        "/jsonx",
        "/devtools/page/1",
    ] {
        assert!(!is_cdp_discovery_path(path), "{path}");
    }
}

#[tokio::test]
async fn client_close_during_open_drops_the_open() {
    let dropped = Arc::new(AtomicBool::new(false));
    let mut source = futures_util::stream::iter(vec![Ok(Message::Close(None))]);
    let mut sink: Vec<Message> = Vec::new();
    let outcome =
        open_while_watching_client(pending_open(dropped.clone()), &mut source, &mut sink).await;
    assert!(matches!(outcome, OpenWatch::ClientClosed));
    assert!(dropped.load(Ordering::Acquire));
}

#[tokio::test]
async fn client_eof_during_open_drops_the_open() {
    let dropped = Arc::new(AtomicBool::new(false));
    let mut source = futures_util::stream::iter(Vec::<Result<Message, axum::Error>>::new());
    let mut sink: Vec<Message> = Vec::new();
    let outcome =
        open_while_watching_client(pending_open(dropped.clone()), &mut source, &mut sink).await;
    assert!(matches!(outcome, OpenWatch::ClientClosed));
    assert!(dropped.load(Ordering::Acquire));
}

#[tokio::test(start_paused = true)]
async fn pings_are_answered_and_other_frames_kept_in_order_during_open() {
    let mut source = futures_util::stream::iter(vec![
        Ok(Message::Ping(bytes::Bytes::from_static(b"p"))),
        Ok(Message::Text("first".into())),
        Ok(Message::Text("second".into())),
    ])
    .chain(futures_util::stream::pending());
    let mut sink: Vec<Message> = Vec::new();
    let open = async {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        "opened"
    };
    let OpenWatch::Opened(result, buffered) =
        open_while_watching_client(open, &mut source, &mut sink).await
    else {
        panic!("open must complete while the client stays connected");
    };
    assert_eq!(result, "opened");
    assert_eq!(sink, vec![Message::Pong(bytes::Bytes::from_static(b"p"))]);
    assert_eq!(
        buffered.into_iter().collect::<Vec<_>>(),
        vec![
            Message::Text("first".into()),
            Message::Text("second".into())
        ]
    );
}

#[tokio::test]
async fn too_many_frames_during_open_overflow() {
    let dropped = Arc::new(AtomicBool::new(false));
    let frames = (0..65).map(|index| Ok(Message::Text(index.to_string().into())));
    let mut source = futures_util::stream::iter(frames.collect::<Vec<_>>());
    let mut sink: Vec<Message> = Vec::new();
    let outcome =
        open_while_watching_client(pending_open(dropped.clone()), &mut source, &mut sink).await;
    assert!(matches!(outcome, OpenWatch::Overflow));
    assert!(dropped.load(Ordering::Acquire));
}

#[tokio::test]
async fn too_many_bytes_during_open_overflow() {
    let dropped = Arc::new(AtomicBool::new(false));
    let large = bytes::Bytes::from(vec![0_u8; 600 * 1024]);
    let mut source = futures_util::stream::iter(vec![
        Ok(Message::Binary(large.clone())),
        Ok(Message::Binary(large)),
    ]);
    let mut sink: Vec<Message> = Vec::new();
    let outcome =
        open_while_watching_client(pending_open(dropped.clone()), &mut source, &mut sink).await;
    assert!(matches!(outcome, OpenWatch::Overflow));
    assert!(dropped.load(Ordering::Acquire));
}

/// A sink whose client never reads: every send stays pending.
struct UnreadSink;

impl futures_util::Sink<Message> for UnreadSink {
    type Error = axum::Error;
    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Pending
    }
    fn start_send(self: std::pin::Pin<&mut Self>, _: Message) -> Result<(), Self::Error> {
        Ok(())
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Pending
    }
    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Pending
    }
}

// A client that pings but does not read the pong must not stall the open:
// it completes, and the session loop takes over.
#[tokio::test(start_paused = true)]
async fn an_unread_pong_does_not_stall_the_open() {
    let mut source =
        futures_util::stream::iter(vec![Ok(Message::Ping(bytes::Bytes::from_static(b"p")))])
            .chain(futures_util::stream::pending());
    let open = async {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        "opened"
    };
    let outcome = open_while_watching_client(open, &mut source, &mut UnreadSink).await;
    assert!(matches!(outcome, OpenWatch::Opened("opened", _)));
}

// While the open is still pending, a pong the client never reads ends the
// watch at its deadline, which drops the open.
#[tokio::test(start_paused = true)]
async fn an_unread_pong_closes_the_client_at_its_deadline() {
    let dropped = Arc::new(AtomicBool::new(false));
    let mut source =
        futures_util::stream::iter(vec![Ok(Message::Ping(bytes::Bytes::from_static(b"p")))])
            .chain(futures_util::stream::pending());
    let started = tokio::time::Instant::now();
    let outcome =
        open_while_watching_client(pending_open(dropped.clone()), &mut source, &mut UnreadSink)
            .await;
    assert!(matches!(outcome, OpenWatch::ClientClosed));
    assert_eq!(started.elapsed(), PONG_SEND_TIMEOUT);
    assert!(dropped.load(Ordering::Acquire));
}

// A refusal the guest made before starting anything is retryable on both
// the SSE and WebSocket openings, with the guest's hint as Retry-After and
// none of its wording in the public body.
// The node's per-sandbox request limiter also refuses before start; its 429
// names that limit, not the guest's own capacity.
#[test]
fn native_refusals_name_the_node_limiter_apart_from_guest_capacity() {
    for (status, expected) in [
        (
            pplx_space_common_grpc::uds_per_sandbox_limit_exceeded_status("busy"),
            TooManyRequestsReason::NodeSandboxLimit,
        ),
        (
            pplx_space_common_grpc::refused_before_start(
                tonic::Status::resource_exhausted("full"),
                std::time::Duration::from_secs(1),
            ),
            TooManyRequestsReason::SandboxAtCapacity,
        ),
    ] {
        let response = process_stream_start_http_error(&StreamBridgeError::BackendStatus(status));
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.extensions().get::<TooManyRequestsReason>(),
            Some(&expected),
        );
    }
}

#[tokio::test]
async fn refusals_before_start_are_retryable_with_retry_after() {
    use http_body_util::BodyExt;

    for (code, expected_status, expected_code) in [
        (
            tonic::Code::ResourceExhausted,
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
        ),
        (
            tonic::Code::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
        ),
    ] {
        let status = pplx_space_common_grpc::refused_before_start(
            tonic::Status::new(code, "synthetic guest wording"),
            std::time::Duration::from_millis(1500),
        );
        let error = StreamBridgeError::BackendStatus(status);
        let response = process_stream_start_http_error(&error);
        assert_eq!(response.status(), expected_status, "{code:?}");
        assert_eq!(
            response
                .extensions()
                .get::<TooManyRequestsReason>()
                .copied(),
            (expected_status == StatusCode::TOO_MANY_REQUESTS)
                .then_some(TooManyRequestsReason::SandboxAtCapacity),
            "{code:?}",
        );
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "2");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["code"], expected_code, "{body}");
        assert_eq!(body["error"]["retryable"], true, "{body}");
        assert!(!body.to_string().contains("synthetic guest wording"));
        let DataPlaneServerFrame::Error { retryable, .. } = stream_start_error_frame(&error) else {
            panic!("expected error frame")
        };
        assert!(retryable, "WebSocket agrees with SSE for {code:?}");
    }
}

// A full sandbox disk is named and not retryable.
#[tokio::test]
async fn full_guest_storage_is_named_and_not_retryable() {
    use http_body_util::BodyExt;

    let mut status = tonic::Status::resource_exhausted("No space left on device (os error 28)");
    status.metadata_mut().insert(
        pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY,
        pplx_space_common_core::spaced::GUEST_STORAGE_FULL
            .parse()
            .unwrap(),
    );
    let response = process_stream_start_http_error(&StreamBridgeError::BackendStatus(status));
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["message"], "sandbox storage is full");
    assert_eq!(body["error"]["retryable"], false);
}
