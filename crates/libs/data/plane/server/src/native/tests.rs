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

/// How a fake cluster API answers `BindDataPlaneSessionProcess`.
#[derive(Clone, Copy)]
enum FakeBind {
    Succeeds,
    NotFound,
}

/// A fake cluster API and worker for one native process session. Each bind
/// waits for a `release` permit before answering, so a test can hold the
/// binding open while it watches what the client receives.
struct BindingFake {
    endpoint: String,
    release: Arc<tokio::sync::Semaphore>,
    bind_started: Arc<tokio::sync::Notify>,
    bound_process_ids: Arc<std::sync::Mutex<Vec<String>>>,
}

async fn spawn_binding_fake(
    session_id: DataPlaneSessionId,
    exits: bool,
    bind: FakeBind,
) -> BindingFake {
    use prost::Message as _;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let mut route = crate::tests::route_response_with_token(session_id, None);
    route.backend.as_mut().unwrap().endpoint = endpoint.clone();
    route.adapter_state_json = r#"{"command":["true"]}"#.to_string();
    let route = route.encode_to_vec();
    let process_id = pplx_space_public_api::process_id_for_session(session_id).to_string();
    let event = |event| {
        let mut framed = vec![0];
        let encoded = ProcessEvent {
            process_id: process_id.clone(),
            pid: Some(42),
            event: Some(event),
        }
        .encode_to_vec();
        framed.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        framed.extend(encoded);
        bytes::Bytes::from(framed)
    };
    let mut events = vec![event(spaced_process_event::Event::Started(
        pplx_space_sbx_mgr_client::operations::ProcessStarted { start_time: None },
    ))];
    if exits {
        events.push(event(spaced_process_event::Event::Exit(
            pplx_space_sbx_mgr_client::proto::SpacedProcessExit {
                outcome: Some(
                    pplx_space_sbx_mgr_client::proto::spaced_process_exit::Outcome::Exited(
                        pplx_space_sbx_mgr_client::proto::SpacedProcessExited { exit_code: 0 },
                    ),
                ),
            },
        )));
    }
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let bind_started = Arc::new(tokio::sync::Notify::new());
    let bound_process_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
    let fake = BindingFake {
        endpoint,
        release: Arc::clone(&release),
        bind_started: Arc::clone(&bind_started),
        bound_process_ids: Arc::clone(&bound_process_ids),
    };
    let service = axum::Router::new().fallback(move |request: Request<Body>| {
        let route = route.clone();
        let events = events.clone();
        let release = Arc::clone(&release);
        let bind_started = Arc::clone(&bind_started);
        let bound_process_ids = Arc::clone(&bound_process_ids);
        async move {
            let path = request.uri().path().to_string();
            if path.ends_with("/Check") {
                return crate::tests::grpc_test_response(vec![8, 1]);
            }
            if path.ends_with("/ResolveDataPlaneSessionRoute") {
                return crate::tests::grpc_test_response(route);
            }
            if path.ends_with("/BindDataPlaneSessionProcess") {
                let body = request.into_body().collect().await.unwrap().to_bytes();
                let bound = BindDataPlaneSessionProcessRequest::decode(&body[5..]).unwrap();
                bound_process_ids.lock().unwrap().push(bound.process_id);
                bind_started.notify_one();
                release.acquire().await.unwrap().forget();
                return match bind {
                    FakeBind::Succeeds => crate::tests::grpc_test_response(Vec::new()),
                    FakeBind::NotFound => {
                        tonic::Status::not_found("process-backed data-plane session not found")
                            .into_http::<Body>()
                    }
                };
            }
            assert!(
                path.ends_with("/SpacedStartProcess"),
                "unexpected call {path}"
            );
            let frames = futures_util::stream::iter(
                events
                    .into_iter()
                    .map(|event| Ok::<_, Infallible>(hyper::body::Frame::data(event))),
            )
            .chain(futures_util::stream::pending());
            Response::builder()
                .header("content-type", "application/grpc")
                .body(Body::new(http_body_util::StreamBody::new(frames)))
                .unwrap()
        }
    });
    tokio::spawn(async move {
        axum::serve(listener, service).await.unwrap();
    });
    fake
}

fn binding_fake_test_process() -> bool {
    pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("test-token")),
        ),
    ])
}

/// Opens the session's event stream and forwards its body text, chunk by
/// chunk, until the body ends.
async fn open_process_events(
    fake: &BindingFake,
    session_id: DataPlaneSessionId,
) -> (DataPlaneState, tokio::sync::mpsc::UnboundedReceiver<String>) {
    let mut state = crate::tests::test_data_plane_state().await;
    state.cluster_api = ClusterApiClients::service_only(
        pplx_space_node_api_client::cluster::connect(fake.endpoint.clone(), "test")
            .await
            .unwrap(),
    );
    state.revocation_check_interval = Duration::from_secs(3600);
    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "c".repeat(64));
    let response = tower::ServiceExt::oneshot(
        router(state.clone()),
        Request::get(format!("/v1/sessions/{session_id}/events"))
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let (chunk_tx, chunks) = tokio::sync::mpsc::unbounded_channel();
    // Dropping the receiver drops the body, as a disconnecting client would.
    tokio::spawn(async move {
        loop {
            let frame = tokio::select! {
                () = chunk_tx.closed() => break,
                frame = body.frame() => frame,
            };
            let Some(Ok(frame)) = frame else { break };
            if let Ok(data) = frame.into_data()
                && chunk_tx
                    .send(String::from_utf8_lossy(&data).into_owned())
                    .is_err()
            {
                break;
            }
        }
    });
    (state, chunks)
}

async fn read_until(
    chunks: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
    seen: &mut String,
    marker: &str,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !seen.contains(marker) {
            let chunk = chunks.recv().await.expect("stream still open");
            seen.push_str(&chunk);
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{marker} within 5s; saw {seen}"));
}

#[tokio::test]
async fn process_start_reaches_the_client_before_its_binding_commits() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_binding_fake(session_id, false, FakeBind::Succeeds).await;
    let (state, mut chunks) = open_process_events(&fake, session_id).await;
    let mut seen = String::new();

    // The bind is still held open, yet the start already reached the client.
    read_until(&mut chunks, &mut seen, "process_started").await;
    tokio::time::timeout(Duration::from_secs(5), fake.bind_started.notified())
        .await
        .expect("the stream bound the process");
    assert!(
        state.native_sessions.is_active(session_id),
        "the start permit stays held while the binding is in flight"
    );

    fake.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while state.native_sessions.is_active(session_id) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the start permit is released once the binding settles");
    assert_eq!(
        *fake.bound_process_ids.lock().unwrap(),
        vec![pplx_space_public_api::process_id_for_session(session_id).to_string()],
        "bound exactly once, to the derived process id"
    );
}

#[tokio::test]
async fn process_exit_is_reported_only_after_its_binding_commits() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_binding_fake(session_id, true, FakeBind::Succeeds).await;
    let (_state, mut chunks) = open_process_events(&fake, session_id).await;
    let mut seen = String::new();

    read_until(&mut chunks, &mut seen, "process_started").await;
    tokio::time::timeout(Duration::from_secs(5), fake.bind_started.notified())
        .await
        .expect("the stream bound the process");
    // The guest already reported the exit; the client must not see it while
    // a reconnect could still miss the binding.
    let early = tokio::time::timeout(Duration::from_millis(300), async {
        while let Some(chunk) = chunks.recv().await {
            seen.push_str(&chunk);
            if seen.contains("process_exited") {
                return;
            }
        }
    })
    .await;
    assert!(early.is_err(), "exit reported before the binding: {seen}");

    fake.release.add_permits(1);
    read_until(&mut chunks, &mut seen, "process_exited").await;
    read_until(&mut chunks, &mut seen, "backend_closed").await;
}

#[tokio::test]
async fn failed_binding_ends_the_process_stream_with_a_retryable_error() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_binding_fake(session_id, false, FakeBind::NotFound).await;
    let (state, mut chunks) = open_process_events(&fake, session_id).await;
    let mut seen = String::new();

    read_until(&mut chunks, &mut seen, "process_started").await;
    fake.release.add_permits(1);
    read_until(&mut chunks, &mut seen, "backend_stream_error").await;
    assert!(seen.contains(r#""retryable":true"#), "{seen}");
    tokio::time::timeout(Duration::from_secs(5), async {
        while chunks.recv().await.is_some() {}
    })
    .await
    .expect("the stream ends after the failed binding");
    assert!(!state.native_sessions.is_active(session_id));
}

#[tokio::test]
async fn binding_survives_a_client_that_disconnects_mid_bind() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_binding_fake(session_id, false, FakeBind::Succeeds).await;
    let (state, mut chunks) = open_process_events(&fake, session_id).await;
    let mut seen = String::new();

    read_until(&mut chunks, &mut seen, "process_started").await;
    tokio::time::timeout(Duration::from_secs(5), fake.bind_started.notified())
        .await
        .expect("the stream bound the process");
    drop(chunks);
    // Let the dropped body tear the stream down before the bind answers.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        state.native_sessions.is_active(session_id),
        "a second attach stays refused until the binding settles"
    );

    fake.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while state.native_sessions.is_active(session_id) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the binding completed after the client left");
    assert_eq!(fake.bound_process_ids.lock().unwrap().len(), 1);
}
