use super::*;
use axum::extract::ws::Message;
use pplx_space_common_svc::space_api_route::{
    RouteIdentityRemediation, set_route_identity_rejection_metadata,
};
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
            message,
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
        assert_eq!(message, STREAM_OPEN_FAILED, "{code:?}");
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
        let DataPlaneServerFrame::Error {
            message, retryable, ..
        } = stream_start_error_frame(&error)
        else {
            panic!("expected error frame")
        };
        assert!(retryable, "WebSocket agrees with SSE for {code:?}");
        assert_eq!(message, STREAM_OPEN_REFUSED, "{code:?}");
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

/// How a fake cluster API answers `BindDataPlaneSessionProcess` once the test
/// adds a `release` permit.
#[derive(Clone, Copy)]
enum FakeBind {
    Succeeds,
    NotFound,
}

/// A process fake whose start reports the process (and its exit, if
/// `exits`) and stays open, and whose binds wait for a `release` permit, so
/// a test can hold the binding open while it watches what the client
/// receives.
async fn spawn_binding_fake(
    session_id: DataPlaneSessionId,
    exits: bool,
    bind: FakeBind,
) -> ProcessFake {
    spawn_process_fake(
        session_id,
        FakeRoute::unbound(),
        Some(bind),
        Arc::new(move |_, open| {
            assert_eq!(open, FakeOpen::Start);
            let mut replies = vec![started()];
            if exits {
                replies.push(exited());
            }
            replies
        }),
    )
    .await
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

/// A data plane whose cluster API is the fake at `endpoint`.
async fn fake_state(endpoint: &str) -> DataPlaneState {
    let mut state = crate::tests::test_data_plane_state().await;
    state.cluster_api = ClusterApiClients::service_only(
        pplx_space_node_api_client::cluster::connect(endpoint.to_string(), "test")
            .await
            .unwrap(),
    );
    state.revocation_check_interval = Duration::from_secs(3600);
    state
}

async fn get_session_events(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
    last_event_id: Option<u64>,
) -> Response {
    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "c".repeat(64));
    let mut request = Request::get(format!("/v1/sessions/{session_id}/events"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"));
    if let Some(last_event_id) = last_event_id {
        request = request.header("last-event-id", last_event_id.to_string());
    }
    tower::ServiceExt::oneshot(router(state.clone()), request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

/// Opens the session's event stream, through the fake cluster API at
/// `endpoint`, and forwards its body text, chunk by chunk, until the body ends.
async fn open_process_events(
    endpoint: &str,
    session_id: DataPlaneSessionId,
    last_event_id: Option<u64>,
) -> (DataPlaneState, tokio::sync::mpsc::UnboundedReceiver<String>) {
    let state = fake_state(endpoint).await;
    let response = get_session_events(&state, session_id, last_event_id).await;
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
    let (state, mut chunks) = open_process_events(&fake.endpoint, session_id, None).await;
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
    let (_state, mut chunks) = open_process_events(&fake.endpoint, session_id, None).await;
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
    let (state, mut chunks) = open_process_events(&fake.endpoint, session_id, None).await;
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
    let (state, mut chunks) = open_process_events(&fake.endpoint, session_id, None).await;
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

const PTY_SESSION: &str = r#"{"command":["sh"],"pty":{"rows":24,"cols":80}}"#;

/// How the fake worker answers a connect to the session's process.
#[derive(Clone, Copy)]
enum FakeProcess {
    Running,
    /// The guest has no such process.
    Missing,
    /// The node has no such sandbox.
    SandboxMissing,
    Unreachable,
}

/// The process session a [`spawn_session_fake`] serves.
struct FakeSession {
    /// The session's stored `CreateProcessRequest`.
    adapter_state: &'static str,
    /// Whether each route lookup, in order, reports the process as bound to
    /// the session; the last answer repeats.
    bound: &'static [bool],
    /// Whether the worker reports the process's exit after its first event.
    exits: bool,
    bind: FakeBind,
    process: FakeProcess,
}

/// A [`ProcessFake`] for one process session whose start reports the process
/// and whose connect answers as `session.process` says, each followed by the
/// exit if `session.exits`. Binds wait for a `release` permit.
async fn spawn_session_fake(session_id: DataPlaneSessionId, session: FakeSession) -> ProcessFake {
    let FakeSession {
        adapter_state,
        bound,
        exits,
        bind,
        process,
    } = session;
    spawn_process_fake(
        session_id,
        FakeRoute {
            adapter_state,
            bound,
        },
        Some(bind),
        Arc::new(move |_, open| {
            let mut replies = match (open, process) {
                (FakeOpen::Start, _) => vec![started()],
                (FakeOpen::Connect(_), FakeProcess::Running) => vec![connected()],
                (FakeOpen::Connect(_), FakeProcess::Missing) => {
                    return vec![FakeReply::Refuse(
                        pplx_space_common_grpc::mark_guest_process_error(tonic::Status::not_found(
                            "process not found",
                        )),
                    )];
                }
                (FakeOpen::Connect(_), FakeProcess::SandboxMissing) => {
                    return vec![FakeReply::Refuse(tonic::Status::not_found(
                        "sandbox not found",
                    ))];
                }
                (FakeOpen::Connect(_), FakeProcess::Unreachable) => {
                    return vec![FakeReply::Refuse(tonic::Status::unavailable(
                        "worker unreachable",
                    ))];
                }
            };
            if exits {
                replies.push(exited());
            }
            replies
        }),
    )
    .await
}

async fn error_body(response: Response) -> serde_json::Value {
    use http_body_util::BodyExt;
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

/// Server-sent events carry no input and cannot close a pty's, so an events
/// attach that would start a pty process is refused once the worker reports
/// that the session has no process. Nothing is started.
#[tokio::test]
async fn events_stream_refuses_to_start_pty_process_sessions() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_session_fake(
        session_id,
        FakeSession {
            adapter_state: PTY_SESSION,
            bound: &[false],
            exits: true,
            bind: FakeBind::Succeeds,
            process: FakeProcess::Missing,
        },
    )
    .await;
    let state = fake_state(&fake.endpoint).await;
    let response = get_session_events(&state, session_id, None).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = error_body(response).await;
    assert_eq!(body["error"]["code"], "invalid_request", "{body}");
    assert_eq!(body["error"]["retryable"], false, "{body}");
    assert!(fake.called("/SpacedConnectProcess"), "the worker was asked");
    assert!(!fake.called("/SpacedStartProcess"), "nothing was started");
}

/// A WebSocket start reports the process before its binding commits. While
/// the route still says unbound, the worker's running process is resumed
/// rather than refused, through the one connect that also asked whether it
/// runs, and this stream commits the binding.
#[tokio::test]
async fn events_stream_resumes_a_running_pty_process_before_its_binding_commits() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_session_fake(
        session_id,
        FakeSession {
            adapter_state: PTY_SESSION,
            bound: &[false],
            exits: true,
            bind: FakeBind::Succeeds,
            process: FakeProcess::Running,
        },
    )
    .await;
    let (_state, mut chunks) = open_process_events(&fake.endpoint, session_id, None).await;
    let mut seen = String::new();

    fake.release.add_permits(1);
    read_until(&mut chunks, &mut seen, "process_exited").await;
    let calls = fake.calls.lock().unwrap();
    let connects = calls
        .iter()
        .filter(|path| path.ends_with("/SpacedConnectProcess"))
        .count();
    assert_eq!(connects, 1, "the stream is the first connect: {calls:?}");
    assert!(
        !calls
            .iter()
            .any(|path| path.ends_with("/SpacedStartProcess")),
        "nothing was started"
    );
    assert_eq!(
        *fake.bound_process_ids.lock().unwrap(),
        vec![pplx_space_public_api::process_id_for_session(session_id).to_string()],
        "the resumed process was bound"
    );
}

/// A `NotFound` from the node, such as for a missing sandbox, is not the
/// guest saying it has no process, so the attach keeps the answer a start
/// gets rather than the pty refusal.
#[tokio::test]
async fn events_stream_does_not_refuse_a_pty_session_whose_sandbox_is_missing() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_session_fake(
        session_id,
        FakeSession {
            adapter_state: PTY_SESSION,
            bound: &[false],
            exits: true,
            bind: FakeBind::Succeeds,
            process: FakeProcess::SandboxMissing,
        },
    )
    .await;
    let state = fake_state(&fake.endpoint).await;
    let response = get_session_events(&state, session_id, None).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = error_body(response).await;
    assert_eq!(body["error"]["code"], "stream_unavailable", "{body}");
    assert_eq!(body["error"]["retryable"], true, "{body}");
    assert!(!fake.called("/SpacedStartProcess"), "nothing was started");
}

/// When the worker cannot say whether the session's process exists, the
/// attach is answered as retryable rather than refused.
#[tokio::test]
async fn events_stream_answers_a_failed_process_connect_as_retryable() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_session_fake(
        session_id,
        FakeSession {
            adapter_state: PTY_SESSION,
            bound: &[false],
            exits: true,
            bind: FakeBind::Succeeds,
            process: FakeProcess::Unreachable,
        },
    )
    .await;
    let state = fake_state(&fake.endpoint).await;
    let response = get_session_events(&state, session_id, None).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = error_body(response).await;
    assert_eq!(body["error"]["retryable"], true, "{body}");
    assert!(!fake.called("/SpacedStartProcess"), "nothing was started");
}

/// A `stdin` process started over server-sent events gets stdin at EOF, so a
/// command that reads it finishes instead of waiting on input nobody sends.
#[tokio::test]
async fn events_stream_starts_stdin_process_sessions_with_stdin_at_eof() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_session_fake(
        session_id,
        FakeSession {
            adapter_state: r#"{"command":["cat"],"stdin":true}"#,
            bound: &[false],
            exits: true,
            bind: FakeBind::Succeeds,
            process: FakeProcess::Running,
        },
    )
    .await;
    let (_state, mut chunks) = open_process_events(&fake.endpoint, session_id, None).await;
    let mut seen = String::new();

    read_until(&mut chunks, &mut seen, "process_started").await;
    fake.release.add_permits(1);
    read_until(&mut chunks, &mut seen, "process_exited").await;
    assert_eq!(
        *fake.started_stdin.lock().unwrap(),
        vec![false],
        "started once, without a stdin pipe"
    );
}

/// A pty process already bound to the session is only read from, so an events
/// reconnect still resumes it.
#[tokio::test]
async fn events_stream_resumes_started_pty_process_sessions() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_session_fake(
        session_id,
        FakeSession {
            adapter_state: PTY_SESSION,
            bound: &[true],
            exits: true,
            bind: FakeBind::Succeeds,
            process: FakeProcess::Running,
        },
    )
    .await;
    let (_state, mut chunks) = open_process_events(&fake.endpoint, session_id, Some(3)).await;
    let mut seen = String::new();

    read_until(&mut chunks, &mut seen, "process_exited").await;
    assert!(seen.contains("resync_required"), "{seen}");
    assert!(fake.called("/SpacedConnectProcess"));
    assert!(!fake.called("/SpacedStartProcess"), "nothing was started");
}

/// The cached route can predate a binding another attach just made. An
/// unbound pty route is read again, and a process bound in the meantime is
/// resumed through its binding, which is not written again.
#[tokio::test]
async fn events_stream_rereads_an_unbound_pty_route_before_refusing() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_session_fake(
        session_id,
        FakeSession {
            adapter_state: PTY_SESSION,
            bound: &[false, true],
            exits: true,
            bind: FakeBind::Succeeds,
            process: FakeProcess::Running,
        },
    )
    .await;
    let (_state, mut chunks) = open_process_events(&fake.endpoint, session_id, None).await;
    let mut seen = String::new();

    fake.release.add_permits(1);
    read_until(&mut chunks, &mut seen, "process_exited").await;
    let calls = fake.calls.lock().unwrap();
    let connects = calls
        .iter()
        .filter(|path| path.ends_with("/SpacedConnectProcess"))
        .count();
    assert_eq!(connects, 1, "resumed from the re-read route: {calls:?}");
    assert!(
        !calls
            .iter()
            .any(|path| path.ends_with("/SpacedStartProcess")),
        "nothing was started"
    );
    assert!(
        fake.bound_process_ids.lock().unwrap().is_empty(),
        "already bound"
    );
}

/// While another attach on this replica holds the session, as a start does
/// until its binding commits, a pty attach gets the retryable conflict rather
/// than the refusal.
#[tokio::test]
async fn events_stream_answers_a_held_pty_session_with_a_retryable_conflict() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_session_fake(
        session_id,
        FakeSession {
            adapter_state: PTY_SESSION,
            bound: &[false],
            exits: true,
            bind: FakeBind::Succeeds,
            process: FakeProcess::Missing,
        },
    )
    .await;
    let state = fake_state(&fake.endpoint).await;
    let _held = state
        .native_sessions
        .try_acquire(session_id, SessionKind::Process)
        .unwrap();
    let response = get_session_events(&state, session_id, None).await;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = error_body(response).await;
    assert_eq!(body["error"]["retryable"], true, "{body}");
}

/// How a fake cluster API answers revocation checks.
#[derive(Clone, Copy)]
enum FakeRevocation {
    Active,
    /// The cluster API is unreachable; its status names an internal address.
    Down,
    /// The cluster API takes the check and never answers.
    Hung,
    /// The org epoch moved on, and the session check then never answers.
    HungAfterOrgEpochChange,
    /// The org read takes most of a check timeout and finds the epoch moved
    /// on, and the session read then never answers.
    SlowOrgThenHungSession,
    /// Every call, route resolves included, answers as `Active` does after
    /// [`FAKE_SLOW_ANSWER`].
    Slow,
    Revoked,
    /// The cluster API refuses the check.
    Rejected,
}

/// How long [`FakeRevocation::Slow`] takes to answer.
const FAKE_SLOW_ANSWER: Duration = Duration::from_millis(300);
/// The check timeout for [`FakeRevocation::SlowOrgThenHungSession`], and how
/// long its org read takes.
const SPLIT_CHECK_TIMEOUT: Duration = Duration::from_millis(2_000);
const SPLIT_CHECK_ORG_READ: Duration = Duration::from_millis(1_200);

/// A fake cluster API and worker for one running process. The test sets how
/// revocation checks are answered and sends stdout chunks down the open
/// process stream.
struct RevocationFake {
    endpoint: String,
    revocation: Arc<std::sync::Mutex<FakeRevocation>>,
    /// Revocation checks answered with an error.
    failed_answers: Arc<std::sync::atomic::AtomicUsize>,
    stdout: tokio::sync::mpsc::UnboundedSender<&'static str>,
}

impl RevocationFake {
    fn answer(&self, revocation: FakeRevocation) {
        *self.revocation.lock().unwrap() = revocation;
    }

    fn print(&self, data: &'static str) {
        self.stdout.send(data).unwrap();
    }

    fn failed_answers(&self) -> usize {
        self.failed_answers.load(Ordering::SeqCst)
    }
}

/// Make the session's org due for a refresh, so its next check reads the
/// cluster API however soon the last read answered.
fn make_org_refresh_due(state: &DataPlaneState, session_id: DataPlaneSessionId) {
    let org_id = state
        .revocation_cache
        .entries
        .get(&session_id.to_string())
        .unwrap()
        .org_id;
    state
        .revocation_cache
        .orgs
        .get_mut(&org_id)
        .unwrap()
        .refresh_after = tokio::time::Instant::now();
}

const FAKE_CLUSTER_API_OUTAGE: &str = "connection refused: 192.0.2.10:9443";

fn grpc_message(message: &impl prost::Message) -> bytes::Bytes {
    let encoded = message.encode_to_vec();
    let mut framed = vec![0];
    framed.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    framed.extend(encoded);
    bytes::Bytes::from(framed)
}

async fn spawn_revocation_fake(session_id: DataPlaneSessionId) -> RevocationFake {
    use pplx_space_node_api_client::proto::{
        GetDataPlaneOrgRevocationEpochResponse, GetDataPlaneSessionRevocationStateResponse,
    };
    use prost::Message as _;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let mut route = crate::tests::route_response_with_token(session_id, None);
    route.backend.as_mut().unwrap().endpoint = endpoint.clone();
    route.adapter_state_json = r#"{"command":["sleep","600"]}"#.to_string();
    let org_id = route.org_id.clone();
    let route = route.encode_to_vec();
    let process_id = pplx_space_public_api::process_id_for_session(session_id).to_string();
    let revocation = Arc::new(std::sync::Mutex::new(FakeRevocation::Active));
    let failed_answers = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (stdout, stdout_rx) = tokio::sync::mpsc::unbounded_channel();
    let stdout_rx = Arc::new(std::sync::Mutex::new(Some(stdout_rx)));
    let fake = RevocationFake {
        endpoint,
        revocation: Arc::clone(&revocation),
        failed_answers: Arc::clone(&failed_answers),
        stdout,
    };
    let service = axum::Router::new().fallback(move |request: Request<Body>| {
        let route = route.clone();
        let org_id = org_id.clone();
        let process_id = process_id.clone();
        let answer = *revocation.lock().unwrap();
        let failed_answers = Arc::clone(&failed_answers);
        let stdout_rx = Arc::clone(&stdout_rx);
        async move {
            let path = request.uri().path().to_string();
            if path.ends_with("/Check") {
                return crate::tests::grpc_test_response(vec![8, 1]);
            }
            if matches!(answer, FakeRevocation::Slow) {
                tokio::time::sleep(FAKE_SLOW_ANSWER).await;
            }
            if path.ends_with("/ResolveDataPlaneSessionRoute") {
                return crate::tests::grpc_test_response(route);
            }
            if path.ends_with("/BindDataPlaneSessionProcess") {
                return crate::tests::grpc_test_response(Vec::new());
            }
            let org_epoch_request = path.ends_with("/GetDataPlaneOrgRevocationEpoch");
            if org_epoch_request || path.ends_with("/GetDataPlaneSessionRevocationState") {
                let failed = || failed_answers.fetch_add(1, Ordering::SeqCst);
                let (epoch, status) = match answer {
                    FakeRevocation::Active | FakeRevocation::Slow => {
                        (1, DataPlaneRevocationStatus::Active)
                    }
                    FakeRevocation::Down => {
                        failed();
                        return tonic::Status::unavailable(FAKE_CLUSTER_API_OUTAGE)
                            .into_http::<Body>();
                    }
                    FakeRevocation::Rejected => {
                        failed();
                        return tonic::Status::permission_denied(FAKE_CLUSTER_API_OUTAGE)
                            .into_http::<Body>();
                    }
                    FakeRevocation::Hung => std::future::pending().await,
                    FakeRevocation::HungAfterOrgEpochChange
                    | FakeRevocation::SlowOrgThenHungSession
                        if !org_epoch_request =>
                    {
                        std::future::pending().await
                    }
                    FakeRevocation::SlowOrgThenHungSession => {
                        tokio::time::sleep(SPLIT_CHECK_ORG_READ).await;
                        (2, DataPlaneRevocationStatus::Active)
                    }
                    FakeRevocation::Revoked => (2, DataPlaneRevocationStatus::Revoked),
                    FakeRevocation::HungAfterOrgEpochChange => {
                        (2, DataPlaneRevocationStatus::Active)
                    }
                };
                if org_epoch_request {
                    return crate::tests::grpc_test_response(
                        GetDataPlaneOrgRevocationEpochResponse {
                            org_id,
                            epoch,
                            updated_at: None,
                        }
                        .encode_to_vec(),
                    );
                }
                return crate::tests::grpc_test_response(
                    GetDataPlaneSessionRevocationStateResponse {
                        session_id: session_id.to_string(),
                        protocol_session_id: None,
                        revocation: Some(DataPlaneRevocationState {
                            epoch,
                            status: status as i32,
                            updated_at: None,
                            reason: None,
                            org_epoch: epoch,
                        }),
                        org_id,
                    }
                    .encode_to_vec(),
                );
            }
            assert!(
                path.ends_with("/SpacedStartProcess"),
                "unexpected call {path}"
            );
            let event = move |event| {
                Ok::<_, Infallible>(hyper::body::Frame::data(grpc_message(&ProcessEvent {
                    process_id: process_id.clone(),
                    pid: Some(42),
                    event: Some(event),
                })))
            };
            let started = event(spaced_process_event::Event::Started(
                pplx_space_sbx_mgr_client::operations::ProcessStarted { start_time: None },
            ));
            let output = tokio_stream::wrappers::UnboundedReceiverStream::new(
                stdout_rx
                    .lock()
                    .unwrap()
                    .take()
                    .expect("one process stream"),
            )
            .map(move |data: &'static str| {
                event(spaced_process_event::Event::Stdout(
                    pplx_space_sbx_mgr_client::proto::SpacedProcessOutput {
                        data: data.as_bytes().to_vec(),
                    },
                ))
            });
            let frames = futures_util::stream::iter([started]).chain(output);
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

/// Revocation timings for the stream tests below. Checks run every 20ms. The
/// staleness bound and check timeout are production's unless a test shortens
/// them; their assertions count checks or wait on outcomes rather than
/// measure time.
struct RevocationTimings {
    max_staleness: Duration,
    check_timeout: Duration,
}

impl Default for RevocationTimings {
    fn default() -> Self {
        Self {
            max_staleness: REVOCATION_MAX_STALENESS,
            check_timeout: REVOCATION_CHECK_TIMEOUT,
        }
    }
}

/// Opens a process session's event stream against the fake and reads it until
/// the process has started.
async fn open_revocation_checked_events(
    fake: &RevocationFake,
    session_id: DataPlaneSessionId,
    timings: RevocationTimings,
) -> (tokio::sync::mpsc::UnboundedReceiver<String>, String) {
    let mut state = crate::tests::test_data_plane_state().await;
    state.cluster_api = ClusterApiClients::service_only(
        pplx_space_node_api_client::cluster::connect(fake.endpoint.clone(), "test")
            .await
            .unwrap(),
    );
    state.revocation_check_interval = Duration::from_millis(20);
    state.revocation_max_staleness = timings.max_staleness;
    state.revocation_check_timeout = timings.check_timeout;
    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "c".repeat(64));
    let response = tower::ServiceExt::oneshot(
        router(state),
        Request::get(format!("/v1/sessions/{session_id}/events"))
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let (chunk_tx, mut chunks) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(Ok(frame)) = body.frame().await {
            if let Ok(data) = frame.into_data()
                && chunk_tx
                    .send(String::from_utf8_lossy(&data).into_owned())
                    .is_err()
            {
                break;
            }
        }
    });
    let mut seen = String::new();
    read_until(&mut chunks, &mut seen, "process_started").await;
    (chunks, seen)
}

async fn assert_stream_ends(chunks: &mut tokio::sync::mpsc::UnboundedReceiver<String>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while chunks.recv().await.is_some() {}
    })
    .await
    .expect("the stream ends");
}

fn stdout_marker(data: &str) -> String {
    BASE64_STANDARD.encode(data)
}

#[tokio::test]
async fn process_stream_survives_a_revocation_outage_within_the_staleness_bound() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_revocation_fake(session_id).await;
    let (mut chunks, mut seen) =
        open_revocation_checked_events(&fake, session_id, RevocationTimings::default()).await;

    // Failed checks well inside the bound: output keeps flowing.
    fake.answer(FakeRevocation::Down);
    tokio::time::timeout(Duration::from_secs(5), async {
        while fake.failed_answers() < 5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("checks failed");
    fake.print("during the outage");
    read_until(&mut chunks, &mut seen, &stdout_marker("during the outage")).await;

    fake.answer(FakeRevocation::Active);
    tokio::time::sleep(Duration::from_millis(100)).await;
    fake.print("after the outage");
    read_until(&mut chunks, &mut seen, &stdout_marker("after the outage")).await;
    assert!(
        !seen.contains("backend_stream_error") && !seen.contains("closed"),
        "{seen}"
    );
}

#[tokio::test]
async fn process_stream_keeps_delivering_while_revocation_checks_hang() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_revocation_fake(session_id).await;
    let timings = RevocationTimings {
        check_timeout: Duration::from_millis(100),
        ..RevocationTimings::default()
    };
    let (mut chunks, mut seen) = open_revocation_checked_events(&fake, session_id, timings).await;

    // Output sent while checks hang arrives as each one times out, long
    // before the cluster API client's own 30-second deadline.
    fake.answer(FakeRevocation::Hung);
    tokio::time::sleep(Duration::from_millis(300)).await;
    fake.print("during the hang");
    read_until(&mut chunks, &mut seen, &stdout_marker("during the hang")).await;

    fake.answer(FakeRevocation::Active);
    tokio::time::sleep(Duration::from_millis(300)).await;
    fake.print("after the hang");
    read_until(&mut chunks, &mut seen, &stdout_marker("after the hang")).await;
    assert!(
        !seen.contains("backend_stream_error") && !seen.contains("closed"),
        "{seen}"
    );
}

#[tokio::test]
async fn hung_check_after_an_org_revocation_fails_closed_once_it_times_out() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_revocation_fake(session_id).await;
    let timings = RevocationTimings {
        check_timeout: Duration::from_millis(100),
        ..RevocationTimings::default()
    };
    let (mut chunks, mut seen) = open_revocation_checked_events(&fake, session_id, timings).await;

    // The org's epoch moved on, so the cached verdict no longer counts, and
    // the session check that would settle it never answers.
    fake.answer(FakeRevocation::HungAfterOrgEpochChange);
    read_until(&mut chunks, &mut seen, "backend_stream_error").await;
    assert_stream_ends(&mut chunks).await;
}

#[tokio::test]
async fn process_stream_fails_closed_once_its_verdict_is_older_than_the_bound() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_revocation_fake(session_id).await;
    let timings = RevocationTimings {
        max_staleness: Duration::from_millis(500),
        check_timeout: Duration::from_millis(50),
    };
    let (mut chunks, mut seen) = open_revocation_checked_events(&fake, session_id, timings).await;

    fake.answer(FakeRevocation::Down);
    read_until(&mut chunks, &mut seen, "backend_stream_error").await;
    // About 20 checks fit in the 430ms the verdict is kept; well over a few
    // of them were ridden out before the stream failed closed.
    assert!(
        fake.failed_answers() >= 5,
        "failed closed after {} failed checks",
        fake.failed_answers()
    );
    assert!(seen.contains(r#""retryable":true"#), "{seen}");
    assert!(
        seen.contains(r#""message":"process event stream failed""#),
        "{seen}"
    );
    assert!(
        !seen.contains("192.0.2.10"),
        "internal detail leaked: {seen}"
    );
    assert_stream_ends(&mut chunks).await;
}

#[tokio::test]
async fn refused_revocation_check_fails_closed_at_once() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_revocation_fake(session_id).await;
    let (mut chunks, mut seen) =
        open_revocation_checked_events(&fake, session_id, RevocationTimings::default()).await;

    // The verdict is fresh, but a refusal is not an outage to ride out: the
    // first one ends the stream.
    fake.answer(FakeRevocation::Rejected);
    read_until(&mut chunks, &mut seen, "backend_stream_error").await;
    assert_eq!(fake.failed_answers(), 1, "{seen}");
    assert_stream_ends(&mut chunks).await;
}

/// Which failed checks keep a fresh verdict: every status the cluster API
/// could not answer with, and nothing else.
#[tokio::test]
async fn only_unanswered_revocation_checks_keep_the_last_verdict() {
    if !binding_fake_test_process() {
        return;
    }
    let state = crate::tests::test_data_plane_state().await;
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    state.revocation_cache.remember_active(
        session_id,
        1,
        OrgId::new(),
        1,
        start_revocation_read(&state),
    );
    for code in (0..=16).map(tonic::Code::from) {
        let keeps = match code {
            tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::ResourceExhausted
            | tonic::Code::Internal
            | tonic::Code::Unknown => true,
            tonic::Code::Ok
            | tonic::Code::InvalidArgument
            | tonic::Code::NotFound
            | tonic::Code::AlreadyExists
            | tonic::Code::PermissionDenied
            | tonic::Code::FailedPrecondition
            | tonic::Code::Aborted
            | tonic::Code::OutOfRange
            | tonic::Code::Unimplemented
            | tonic::Code::DataLoss
            | tonic::Code::Unauthenticated => false,
        };
        let error = StreamBridgeError::RevocationStatus(tonic::Status::new(code, "synthetic"));
        assert_eq!(
            keeps_last_verdict(&state, session_id, &error),
            keeps,
            "{code:?}"
        );
    }
    // Answers that decoded but could not be used.
    for error in [
        StreamBridgeError::MissingRevocationState,
        StreamBridgeError::RouteState("invalid_org_id"),
    ] {
        assert!(!keeps_last_verdict(&state, session_id, &error), "{error}");
    }
}

#[tokio::test]
async fn revoked_session_ends_its_stream_at_once() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_revocation_fake(session_id).await;
    let (mut chunks, mut seen) =
        open_revocation_checked_events(&fake, session_id, RevocationTimings::default()).await;

    // Even mid-outage, a revocation the next check confirms ends the stream
    // without waiting out the minute-long staleness bound.
    fake.answer(FakeRevocation::Down);
    tokio::time::sleep(Duration::from_millis(100)).await;
    fake.answer(FakeRevocation::Revoked);
    read_until(&mut chunks, &mut seen, r#""reason":"revoked""#).await;
    assert_stream_ends(&mut chunks).await;
}

/// At production timings and on a paused clock, for every phase of the stream's
/// checks against its last confirmed read: failed checks keep the verdict
/// while the stream can still end within the bound, and no longer.
#[tokio::test]
async fn failed_checks_keep_a_verdict_only_while_the_stream_ends_within_the_bound() {
    if !binding_fake_test_process() {
        return;
    }
    let mut state = crate::tests::test_data_plane_state().await;
    // Nothing below reaches the network, so the clock moves only as told.
    tokio::time::pause();
    state.revocation_check_interval =
        Duration::from_millis(crate::DEFAULT_REVOCATION_CHECK_INTERVAL_MS);
    let interval = state.revocation_check_interval;
    // The check that fails closed may take the whole check timeout, so it
    // must start no later than this after the read that confirmed the
    // verdict; and since checks are an interval apart, no earlier than one
    // interval before it.
    let latest_start = REVOCATION_MAX_STALENESS - REVOCATION_CHECK_TIMEOUT;
    let unavailable =
        StreamBridgeError::RevocationStatus(tonic::Status::unavailable("synthetic outage"));
    let step = Duration::from_millis(250);
    let phases = (interval.as_millis() / step.as_millis()) as u32;
    for phase in (0..phases).map(|n| step * n) {
        let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
        state.revocation_cache.remember_active(
            session_id,
            1,
            OrgId::new(),
            1,
            start_revocation_read(&state),
        );
        let confirmed = tokio::time::Instant::now();
        // The first check `phase` after the confirmation, then one per
        // interval, each decided as soon as it starts.
        tokio::time::advance(phase).await;
        let failing_check_started = loop {
            if !keeps_last_verdict(&state, session_id, &unavailable) {
                break confirmed.elapsed();
            }
            tokio::time::advance(interval).await;
        };
        assert!(
            failing_check_started <= latest_start,
            "phase {phase:?}: the failing check started {failing_check_started:?} after \
             the confirmed read"
        );
        assert!(
            failing_check_started >= latest_start - interval,
            "phase {phase:?}: the failing check started {failing_check_started:?} after \
             the confirmed read, a whole interval early"
        );
    }
}

/// A stream alone in its org re-reads the org on each of its checks, even
/// when a check starts a little sooner than one interval after the previous
/// read began.
#[tokio::test]
async fn lone_stream_rereads_its_org_on_every_check() {
    if !binding_fake_test_process() {
        return;
    }
    let mut state = crate::tests::test_data_plane_state().await;
    tokio::time::pause();
    state.revocation_check_interval =
        Duration::from_millis(crate::DEFAULT_REVOCATION_CHECK_INTERVAL_MS);
    let interval = state.revocation_check_interval;
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let org_id = OrgId::new();
    state
        .revocation_cache
        .remember_active(session_id, 1, org_id, 1, start_revocation_read(&state));
    for early in [0, 1, 50, 500].map(Duration::from_millis) {
        tokio::time::advance(interval - early).await;
        let RevocationProbe::RefreshOrg(refresh) = state
            .revocation_cache
            .probe(session_id, revocation_tolerance(&state))
        else {
            panic!("a check {early:?} early answered from the cache");
        };
        state
            .revocation_cache
            .remember_org_epoch(org_id, 1, start_revocation_read(&state));
        drop(refresh.guard);
    }
}

/// The org's confirmation is dated from when each read that confirmed it
/// began, whether a route resolve or a revocation check, not from when its
/// slow answer arrived.
#[tokio::test]
async fn revocation_verdicts_are_dated_from_when_their_read_began() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_revocation_fake(session_id).await;
    let mut state = crate::tests::test_data_plane_state().await;
    state.cluster_api = ClusterApiClients::service_only(
        pplx_space_node_api_client::cluster::connect(fake.endpoint.clone(), "test")
            .await
            .unwrap(),
    );
    // Every answer is due for a refresh by the time the next read starts.
    state.revocation_check_interval = Duration::from_millis(1);
    fake.answer(FakeRevocation::Slow);
    let assert_dated_from = |read: &str, began: tokio::time::Instant, state: &DataPlaneState| {
        let org_id = state
            .revocation_cache
            .entries
            .get(&session_id.to_string())
            .unwrap()
            .org_id;
        let confirmed_at = state
            .revocation_cache
            .orgs
            .get(&org_id)
            .unwrap()
            .confirmed_at;
        assert!(
            confirmed_at >= began && confirmed_at < began + FAKE_SLOW_ANSWER,
            "{read}: confirmed {:?} after the read began",
            confirmed_at - began
        );
    };

    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "c".repeat(64));
    let began = tokio::time::Instant::now();
    resolve_session_route(&state, session_id, &token)
        .await
        .unwrap();
    assert_dated_from("route resolve", began, &state);

    let began = tokio::time::Instant::now();
    resolve_session_route_internal(&state, session_id)
        .await
        .unwrap();
    assert_dated_from("internal route resolve", began, &state);

    let began = tokio::time::Instant::now();
    assert!(!session_is_revoked(&state, session_id).await.unwrap());
    assert_dated_from("org check", began, &state);

    state.revocation_cache.forget(session_id);
    let began = tokio::time::Instant::now();
    assert!(!session_is_revoked(&state, session_id).await.unwrap());
    assert_dated_from("session check", began, &state);
}

/// A check that reads the org and then the session gets one timeout for
/// both reads, not one each.
#[tokio::test]
async fn revocation_check_has_one_timeout_across_its_reads() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_revocation_fake(session_id).await;
    let mut state = crate::tests::test_data_plane_state().await;
    state.cluster_api = ClusterApiClients::service_only(
        pplx_space_node_api_client::cluster::connect(fake.endpoint.clone(), "test")
            .await
            .unwrap(),
    );
    state.revocation_check_timeout = SPLIT_CHECK_TIMEOUT;
    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "c".repeat(64));
    resolve_session_route(&state, session_id, &token)
        .await
        .unwrap();

    fake.answer(FakeRevocation::SlowOrgThenHungSession);
    make_org_refresh_due(&state, session_id);
    let started = tokio::time::Instant::now();
    let error = session_is_revoked(&state, session_id)
        .await
        .expect_err("the org moved on, so the hung session read fails closed");
    let took = started.elapsed();
    assert!(
        matches!(&error, StreamBridgeError::RevocationStatus(status)
            if status.code() == tonic::Code::DeadlineExceeded),
        "{error}"
    );
    // Halfway between one timeout and the org read plus a second timeout.
    assert!(
        took < SPLIT_CHECK_TIMEOUT + SPLIT_CHECK_ORG_READ / 2,
        "the check took {took:?}"
    );
}

#[tokio::test]
async fn route_cache_hit_does_not_confirm_the_revocation_verdict() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_revocation_fake(session_id).await;
    let mut state = crate::tests::test_data_plane_state().await;
    state.cluster_api = ClusterApiClients::service_only(
        pplx_space_node_api_client::cluster::connect(fake.endpoint.clone(), "test")
            .await
            .unwrap(),
    );
    let interval = Duration::from_millis(crate::DEFAULT_REVOCATION_CHECK_INTERVAL_MS);
    state.revocation_check_interval = interval;
    state.route_cache_ttl = 10 * REVOCATION_MAX_STALENESS;
    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "c".repeat(64));
    resolve_session_route(&state, session_id, &token)
        .await
        .unwrap();
    let tolerance = Duration::from_secs(1);
    assert!(
        state
            .revocation_cache
            .confirmed_active(session_id, tolerance)
    );

    // Past the tolerance, an attach answered from the route cache asked the
    // cluster API nothing, so the verdict is as old as it was. Nothing below
    // reaches the network, so the paused clock moves only as told.
    tokio::time::pause();
    tokio::time::advance(2 * tolerance).await;
    state.revocation_cache.forget(session_id);
    resolve_session_route(&state, session_id, &token)
        .await
        .unwrap();
    assert!(
        !state
            .revocation_cache
            .confirmed_active(session_id, tolerance)
    );
    assert!(matches!(
        state
            .revocation_cache
            .probe(session_id, revocation_tolerance(&state)),
        RevocationProbe::Fresh(true)
    ));
    // The resolve set the next refresh just under one interval after it
    // began; the cache hit must not have moved it later. This lands a
    // tolerance past that refresh, and a tolerance short of where one set by
    // the cache hit would fall.
    let fresh_for = interval - interval / 10;
    tokio::time::advance(fresh_for - tolerance).await;
    assert!(
        matches!(
            state
                .revocation_cache
                .probe(session_id, revocation_tolerance(&state)),
            RevocationProbe::RefreshOrg(_)
        ),
        "the cache hit pushed the org's next refresh out"
    );
}

/// How the fake backend fails a native WebSocket session.
#[derive(Clone, Copy)]
enum BackendFailure {
    /// The backend stream errors after its first event.
    MidStream,
    /// The stream stays open, and a terminal resize is refused.
    ResizeRefused,
}

/// Backend status text naming an internal endpoint, which must stay out of
/// public frames.
const BACKEND_FAILURE_DETAIL: &str = "dial 192.0.2.10:9443: connection refused";

/// A fake cluster API and worker for one native session that fail it as
/// `failure` says. Returns its endpoint.
async fn spawn_backend_failure_fake(
    session_id: DataPlaneSessionId,
    failure: BackendFailure,
) -> String {
    use prost::Message as _;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let mut route = crate::tests::route_response_with_token(session_id, None);
    route.backend.as_mut().unwrap().endpoint = endpoint.clone();
    route.adapter_state_json = match session_id.kind() {
        SessionKind::FileWatch => r#"{"path":"/tmp"}"#,
        _ => r#"{"command":["sleep","600"]}"#,
    }
    .to_string();
    let route = route.encode_to_vec();
    let process_id = pplx_space_public_api::process_id_for_session(session_id).to_string();
    let service = axum::Router::new().fallback(move |request: Request<Body>| {
        let route = route.clone();
        let process_id = process_id.clone();
        async move {
            let path = request.uri().path().to_string();
            if path.ends_with("/Check") {
                return crate::tests::grpc_test_response(vec![8, 1]);
            }
            if path.ends_with("/ResolveDataPlaneSessionRoute") {
                return crate::tests::grpc_test_response(route);
            }
            if path.ends_with("/BindDataPlaneSessionProcess") {
                return crate::tests::grpc_test_response(Vec::new());
            }
            if path.ends_with("/SpacedResizePty") {
                return tonic::Status::failed_precondition(BACKEND_FAILURE_DETAIL)
                    .into_http::<Body>();
            }
            let first = if path.ends_with("/SpacedWatch") {
                grpc_message(&FileEvent {
                    path: "/tmp/changed".to_string(),
                    kind: SpacedFileEventKind::Modified as i32,
                    ..Default::default()
                })
            } else {
                assert!(
                    path.ends_with("/SpacedStartProcess"),
                    "unexpected call {path}"
                );
                grpc_message(&ProcessEvent {
                    process_id,
                    pid: Some(42),
                    event: Some(spaced_process_event::Event::Started(
                        pplx_space_sbx_mgr_client::operations::ProcessStarted { start_time: None },
                    )),
                })
            };
            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", HeaderValue::from_static("13"));
            trailers.insert(
                "grpc-message",
                HeaderValue::from_static(BACKEND_FAILURE_DETAIL),
            );
            let end = match failure {
                BackendFailure::MidStream => Some(hyper::body::Frame::trailers(trailers)),
                BackendFailure::ResizeRefused => None,
            };
            let frames = futures_util::stream::iter(
                [Some(hyper::body::Frame::data(first)), end]
                    .into_iter()
                    .flatten()
                    .map(Ok::<_, Infallible>),
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
    endpoint
}

/// Serves the data plane against the fake at `endpoint` and attaches a native
/// WebSocket to the session, returning the upgraded connection.
async fn attach_native_websocket(
    endpoint: &str,
    session_id: DataPlaneSessionId,
) -> tokio::net::TcpStream {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut state = crate::tests::test_data_plane_state().await;
    state.cluster_api = ClusterApiClients::service_only(
        pplx_space_node_api_client::cluster::connect(endpoint.to_string(), "test")
            .await
            .unwrap(),
    );
    state.revocation_check_interval = Duration::from_secs(3600);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            router(state).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    tcp.write_all(
        format!(
            "GET /v1/sessions/{session_id}/attach HTTP/1.1\r\n\
             Host: data-plane.test\r\n\
             Authorization: Bearer {DATA_PLANE_TOKEN_PREFIX}{}\r\n\
             Connection: Upgrade\r\n\
             Upgrade: websocket\r\n\
             Sec-WebSocket-Version: 13\r\n\
             Sec-WebSocket-Key: {}\r\n\r\n",
            "c".repeat(64),
            BASE64_STANDARD.encode(b"native-test-nonc"),
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let mut response = Vec::new();
    while !response.ends_with(b"\r\n\r\n") {
        let mut byte = [0_u8; 1];
        tcp.read_exact(&mut byte).await.expect("upgrade response");
        response.push(byte[0]);
    }
    assert!(response.starts_with(b"HTTP/1.1 101"));
    tcp
}

/// Reads server text frames into `seen` until one contains `marker`.
async fn read_websocket_until(tcp: &mut tokio::net::TcpStream, seen: &mut String, marker: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !seen.contains(marker) {
            let (opcode, payload) = crate::tests::read_server_frame(tcp).await;
            assert_eq!(opcode, 0x1, "text frames until {marker}; saw {seen}");
            seen.push_str(&String::from_utf8_lossy(&payload));
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{marker} within 5s; saw {seen}"));
}

/// Sends one short client text frame. Client frames must be masked; an
/// all-zero key leaves the payload as written.
async fn send_websocket_text(tcp: &mut tokio::net::TcpStream, text: &str) {
    use tokio::io::AsyncWriteExt;
    let len = u8::try_from(text.len())
        .ok()
        .filter(|len| *len < 126)
        .unwrap();
    let mut frame = vec![0x81, 0x80 | len, 0, 0, 0, 0];
    frame.extend_from_slice(text.as_bytes());
    tcp.write_all(&frame).await.unwrap();
}

#[tokio::test]
async fn websocket_backend_stream_errors_carry_a_fixed_message() {
    if !binding_fake_test_process() {
        return;
    }
    for (kind, live, message) in [
        (
            SessionKind::Process,
            "process_started",
            PROCESS_STREAM_FAILED,
        ),
        (
            SessionKind::FileWatch,
            "file_event",
            FILE_WATCH_STREAM_FAILED,
        ),
    ] {
        let session_id = DataPlaneSessionId::new_for_kind(kind);
        let endpoint = spawn_backend_failure_fake(session_id, BackendFailure::MidStream).await;
        let mut tcp = attach_native_websocket(&endpoint, session_id).await;
        let mut seen = String::new();
        read_websocket_until(&mut tcp, &mut seen, live).await;
        read_websocket_until(&mut tcp, &mut seen, "backend_stream_error").await;
        assert!(
            seen.contains(&format!(r#""message":"{message}""#)),
            "{kind:?}: {seen}"
        );
        assert!(
            !seen.contains("192.0.2.10"),
            "{kind:?}: internal detail leaked: {seen}"
        );
    }
}

#[tokio::test]
async fn websocket_resize_refusal_carries_a_fixed_message() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let endpoint = spawn_backend_failure_fake(session_id, BackendFailure::ResizeRefused).await;
    let mut tcp = attach_native_websocket(&endpoint, session_id).await;
    let mut seen = String::new();
    read_websocket_until(&mut tcp, &mut seen, "process_started").await;
    send_websocket_text(&mut tcp, r#"{"type":"resize","cols":80,"rows":24}"#).await;
    read_websocket_until(&mut tcp, &mut seen, "resize_not_supported").await;
    assert!(
        seen.contains(r#""message":"terminal resize is not supported by this process""#),
        "{seen}"
    );
    assert!(
        !seen.contains("192.0.2.10"),
        "internal detail leaked: {seen}"
    );
}

/// One reply of a fake guest process stream.
enum FakeReply {
    Event(spaced_process_event::Event),
    /// Ends the stream with this status.
    End(tonic::Status),
    /// Refuses the call outright, before any stream.
    Refuse(tonic::Status),
}

fn end(code: tonic::Code) -> FakeReply {
    FakeReply::End(tonic::Status::new(code, "fake"))
}

fn with_reason(mut status: tonic::Status, reason: &'static str) -> tonic::Status {
    status.metadata_mut().insert(
        pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY,
        tonic::metadata::MetadataValue::from_static(reason),
    );
    status
}

/// Ends the stream as the guest does for a cursor past the output.
fn cursor_ahead() -> FakeReply {
    FakeReply::End(with_reason(
        tonic::Status::out_of_range("fake"),
        pplx_space_common_core::spaced::PROCESS_OUTPUT_CURSOR_AHEAD,
    ))
}

/// Ends the stream as the guest does for a cursor before the oldest retained
/// output, `first_retained`, when it still holds the process.
fn lagged(first_retained: Option<u64>) -> FakeReply {
    let mut status = with_reason(
        tonic::Status::resource_exhausted(
            pplx_space_common_core::spaced::PROCESS_OUTPUT_LAGGED_MESSAGE,
        ),
        pplx_space_common_core::spaced::PROCESS_OUTPUT_LAGGED,
    );
    if let Some(first_retained) = first_retained {
        status.metadata_mut().insert(
            pplx_space_common_core::spaced::PROCESS_OUTPUT_FIRST_SEQUENCE_METADATA_KEY,
            first_retained.into(),
        );
    }
    FakeReply::End(status)
}

/// Ends the stream as a guest older than the lag reason does on eviction.
fn untagged_lagged() -> FakeReply {
    FakeReply::End(tonic::Status::resource_exhausted(
        pplx_space_common_core::spaced::PROCESS_OUTPUT_LAGGED_MESSAGE,
    ))
}

/// Refuses the call as a node does a route token that a refresh replaces,
/// such as an expired one.
fn refreshable_route_rejection() -> FakeReply {
    let mut status = tonic::Status::permission_denied("route token expired");
    set_route_identity_rejection_metadata(
        &mut status,
        RouteIdentityRemediation::RefreshRoute,
        None,
    );
    FakeReply::Refuse(status)
}

/// How the data plane opened a fake guest process stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FakeOpen {
    Start,
    Connect(Option<u64>),
}

fn started() -> FakeReply {
    FakeReply::Event(spaced_process_event::Event::Started(
        pplx_space_sbx_mgr_client::operations::ProcessStarted { start_time: None },
    ))
}

/// A reconnect's first event, from a guest that does not say where the
/// stream starts.
fn connected() -> FakeReply {
    FakeReply::Event(spaced_process_event::Event::Connected(
        pplx_space_sbx_mgr_client::operations::ProcessConnected {
            output_cursor: None,
        },
    ))
}

/// A reconnect's first event, from a guest that started the stream at
/// `start`, its oldest retained output, because the cursor's was evicted.
fn connected_at(start: u64) -> FakeReply {
    FakeReply::Event(spaced_process_event::Event::Connected(
        pplx_space_sbx_mgr_client::operations::ProcessConnected {
            output_cursor: Some(start),
        },
    ))
}

fn stdout(line: &str) -> FakeReply {
    FakeReply::Event(spaced_process_event::Event::Stdout(
        pplx_space_sbx_mgr_client::operations::ProcessOutput {
            data: line.as_bytes().to_vec(),
        },
    ))
}

fn exited() -> FakeReply {
    FakeReply::Event(spaced_process_event::Event::Exit(
        pplx_space_sbx_mgr_client::proto::SpacedProcessExit {
            outcome: Some(
                pplx_space_sbx_mgr_client::proto::spaced_process_exit::Outcome::Exited(
                    pplx_space_sbx_mgr_client::proto::SpacedProcessExited { exit_code: 0 },
                ),
            ),
        },
    ))
}

/// What the guest sends for `open` from a process that wrote `lines` and
/// exited: a start or a cursor replays the output from that position on, a
/// cursor past the output fails, and a connect without a cursor gets only the
/// start and the exit.
fn journal_replay(lines: &[&str], open: FakeOpen) -> Vec<FakeReply> {
    let (mut replies, cursor) = match open {
        FakeOpen::Start => (vec![started()], 0),
        FakeOpen::Connect(None) => return vec![started(), exited()],
        FakeOpen::Connect(Some(cursor)) if cursor as usize > lines.len() => {
            return vec![connected(), cursor_ahead()];
        }
        FakeOpen::Connect(Some(cursor)) => (vec![connected()], cursor),
    };
    replies.extend(lines.iter().skip(cursor as usize).map(|line| stdout(line)));
    replies.push(exited());
    replies
}

type FakeReplies = Arc<dyn Fn(usize, FakeOpen) -> Vec<FakeReply> + Send + Sync>;

/// The route a [`ProcessFake`] resolves for its session.
#[derive(Clone, Copy)]
struct FakeRoute {
    /// The session's stored `CreateProcessRequest`.
    adapter_state: &'static str,
    /// Whether each route lookup, in order, reports the process as bound to
    /// the session; the last answer repeats. Once a bind succeeds, every
    /// lookup reports it bound.
    bound: &'static [bool],
}

impl FakeRoute {
    fn unbound() -> Self {
        Self {
            adapter_state: r#"{"command":["true"]}"#,
            bound: &[false],
        }
    }

    fn bound() -> Self {
        Self {
            bound: &[true],
            ..Self::unbound()
        }
    }
}

/// A fake cluster API and worker for one native process session, whose route
/// is `route`. Every call, and the `stdin` flag of every start, is recorded.
/// Each process stream the data plane opens
/// is answered by `replies`, given how many opens came before it; a stream
/// without a closing status stays open. A bind succeeds at once, or with
/// `gated_bind`, answers as that says once the test adds a `release` permit.
struct ProcessFake {
    endpoint: String,
    bound: Arc<AtomicBool>,
    opens: Arc<std::sync::Mutex<Vec<FakeOpen>>>,
    release: Arc<tokio::sync::Semaphore>,
    bind_started: Arc<tokio::sync::Notify>,
    bound_process_ids: Arc<std::sync::Mutex<Vec<String>>>,
    calls: Arc<std::sync::Mutex<Vec<String>>>,
    started_stdin: Arc<std::sync::Mutex<Vec<bool>>>,
}

impl ProcessFake {
    fn called(&self, method: &str) -> bool {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .any(|path| path.ends_with(method))
    }
}

async fn spawn_journal_fake(
    session_id: DataPlaneSessionId,
    bound: bool,
    replies: FakeReplies,
) -> ProcessFake {
    let route = if bound {
        FakeRoute::bound()
    } else {
        FakeRoute::unbound()
    };
    spawn_process_fake(session_id, route, None, replies).await
}

async fn spawn_process_fake(
    session_id: DataPlaneSessionId,
    fake_route: FakeRoute,
    gated_bind: Option<FakeBind>,
    replies: FakeReplies,
) -> ProcessFake {
    use prost::Message as _;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let process_id = pplx_space_public_api::process_id_for_session(session_id).to_string();
    let mut route = crate::tests::route_response_with_token(session_id, None);
    route.backend.as_mut().unwrap().endpoint = endpoint.clone();
    route.adapter_state_json = fake_route.adapter_state.to_string();
    let lookups = fake_route.bound;
    let route_lookups = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let fake = ProcessFake {
        endpoint,
        bound: Arc::new(AtomicBool::new(lookups[0])),
        opens: Arc::new(std::sync::Mutex::new(Vec::new())),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
        bind_started: Arc::new(tokio::sync::Notify::new()),
        bound_process_ids: Arc::new(std::sync::Mutex::new(Vec::new())),
        calls: Arc::new(std::sync::Mutex::new(Vec::new())),
        started_stdin: Arc::new(std::sync::Mutex::new(Vec::new())),
    };
    let bound = Arc::clone(&fake.bound);
    let calls = Arc::clone(&fake.calls);
    let started_stdin = Arc::clone(&fake.started_stdin);
    let opens = Arc::clone(&fake.opens);
    let release = Arc::clone(&fake.release);
    let bind_started = Arc::clone(&fake.bind_started);
    let bound_process_ids = Arc::clone(&fake.bound_process_ids);
    let service = axum::Router::new().fallback(move |request: Request<Body>| {
        let mut route = route.clone();
        let process_id = process_id.clone();
        let replies = Arc::clone(&replies);
        let opens = Arc::clone(&opens);
        let bound = Arc::clone(&bound);
        let release = Arc::clone(&release);
        let bind_started = Arc::clone(&bind_started);
        let bound_process_ids = Arc::clone(&bound_process_ids);
        let calls = Arc::clone(&calls);
        let started_stdin = Arc::clone(&started_stdin);
        let route_lookups = Arc::clone(&route_lookups);
        async move {
            let path = request.uri().path().to_string();
            calls.lock().unwrap().push(path.clone());
            if path.ends_with("/Check") {
                return crate::tests::grpc_test_response(vec![8, 1]);
            }
            if path.ends_with("/ResolveDataPlaneSessionRoute") {
                let lookup = route_lookups.fetch_add(1, Ordering::SeqCst);
                let bound = bound.load(Ordering::SeqCst) || lookups[lookup.min(lookups.len() - 1)];
                route.backend_process_id = bound.then(|| process_id.clone());
                return crate::tests::grpc_test_response(route.encode_to_vec());
            }
            if path.ends_with("/BindDataPlaneSessionProcess") {
                let body = request.into_body().collect().await.unwrap().to_bytes();
                let request = BindDataPlaneSessionProcessRequest::decode(&body[5..]).unwrap();
                bound_process_ids.lock().unwrap().push(request.process_id);
                if let Some(bind) = gated_bind {
                    bind_started.notify_one();
                    release.acquire().await.unwrap().forget();
                    if let FakeBind::NotFound = bind {
                        return tonic::Status::not_found(
                            "process-backed data-plane session not found",
                        )
                        .into_http::<Body>();
                    }
                }
                bound.store(true, Ordering::SeqCst);
                return crate::tests::grpc_test_response(Vec::new());
            }
            if path.ends_with("/RecordDataPlaneSessionReconnect") {
                return crate::tests::grpc_test_response(Vec::new());
            }
            let open = if path.ends_with("/SpacedStartProcess") {
                let body = request.into_body().collect().await.unwrap().to_bytes();
                let start = SpacedStartProcessRequest::decode(&body[5..]).unwrap();
                started_stdin
                    .lock()
                    .unwrap()
                    .push(start.operation.unwrap().stdin);
                FakeOpen::Start
            } else {
                assert!(
                    path.ends_with("/SpacedConnectProcess"),
                    "unexpected call {path}"
                );
                let body = request.into_body().collect().await.unwrap().to_bytes();
                let operation = SpacedConnectProcessRequest::decode(&body[5..])
                    .unwrap()
                    .operation
                    .unwrap();
                assert!(
                    operation.resume_from_oldest_retained,
                    "a connect asks to start at the oldest retained output"
                );
                FakeOpen::Connect(operation.output_cursor)
            };
            let opened = {
                let mut opens = opens.lock().unwrap();
                opens.push(open);
                opens.len() - 1
            };
            let mut frames = Vec::new();
            let mut trailers = None;
            for reply in replies(opened, open) {
                match reply {
                    FakeReply::Refuse(status) => return status.into_http::<Body>(),
                    FakeReply::Event(event) => {
                        let encoded = ProcessEvent {
                            process_id: process_id.clone(),
                            pid: Some(42),
                            event: Some(event),
                        }
                        .encode_to_vec();
                        let mut framed = vec![0];
                        framed.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
                        framed.extend(encoded);
                        frames.push(hyper::body::Frame::data(bytes::Bytes::from(framed)));
                    }
                    FakeReply::End(status) => {
                        let mut headers = status.into_http::<Body>().into_parts().0.headers;
                        headers.remove(header::CONTENT_TYPE);
                        trailers = Some(headers);
                    }
                }
            }
            let ending = match trailers {
                Some(trailers) => {
                    futures_util::stream::once(
                        async move { hyper::body::Frame::trailers(trailers) },
                    )
                    .boxed()
                }
                None => futures_util::stream::pending().boxed(),
            };
            let frames = futures_util::stream::iter(frames)
                .chain(ending)
                .map(Ok::<_, Infallible>);
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

/// The complete SSE events in `body`, as each one's id and JSON frame.
fn sse_frames(body: &str) -> Vec<(Option<u64>, serde_json::Value)> {
    let mut events: Vec<&str> = body.split("\n\n").collect();
    events.pop();
    events
        .into_iter()
        .filter_map(|event| {
            let mut id = None;
            let mut data = None;
            for line in event.lines() {
                if let Some(value) = line.strip_prefix("id:") {
                    id = Some(value.trim().parse().unwrap());
                } else if let Some(value) = line.strip_prefix("data:") {
                    data = Some(serde_json::from_str(value.trim()).unwrap());
                }
            }
            Some((id, data?))
        })
        .collect()
}

fn frames_of_type<'a>(
    frames: &'a [(Option<u64>, serde_json::Value)],
    frame_type: &'a str,
) -> impl Iterator<Item = &'a (Option<u64>, serde_json::Value)> {
    frames
        .iter()
        .filter(move |(_, frame)| frame["type"] == frame_type)
}

fn stdout_text(frames: &[(Option<u64>, serde_json::Value)]) -> String {
    frames_of_type(frames, "stdout")
        .map(|(_, frame)| {
            let data = BASE64_STANDARD
                .decode(frame["data_base64"].as_str().unwrap())
                .unwrap();
            String::from_utf8(data).unwrap()
        })
        .collect()
}

fn resync_reasons(frames: &[(Option<u64>, serde_json::Value)]) -> Vec<String> {
    frames_of_type(frames, "resync_required")
        .map(|(_, frame)| frame["reason"].as_str().unwrap().to_string())
        .collect()
}

/// Reads one process event stream to its end.
async fn process_events_to_close(
    endpoint: &str,
    session_id: DataPlaneSessionId,
    last_event_id: Option<u64>,
) -> Vec<(Option<u64>, serde_json::Value)> {
    let (_state, mut chunks) = open_process_events(endpoint, session_id, last_event_id).await;
    let mut seen = String::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(chunk) = chunks.recv().await {
            seen.push_str(&chunk);
        }
    })
    .await
    .unwrap_or_else(|_| panic!("stream ends within 30s; saw {seen}"));
    let frames = sse_frames(&seen);
    assert_eq!(frames.last().unwrap().1["type"], "closed", "{seen}");
    let ids: Vec<u64> = frames.iter().filter_map(|(id, _)| *id).collect();
    assert!(ids.is_sorted_by(|a, b| a < b), "ids increase: {ids:?}");
    assert_identified_frames(&frames);
    frames
}

const JOURNAL: [&str; 4] = ["one\n", "two\n", "three\n", "four\n"];

fn assert_ids_above(frames: &[(Option<u64>, serde_json::Value)], last_event_id: u64) {
    let ids: Vec<u64> = frames.iter().filter_map(|(id, _)| *id).collect();
    assert!(
        ids.iter().all(|id| *id > last_event_id),
        "ids {ids:?} after Last-Event-ID {last_event_id}"
    );
}

/// The id a client resumes from: the last one it received.
fn last_id(frames: &[(Option<u64>, serde_json::Value)]) -> u64 {
    frames.iter().rev().find_map(|(id, _)| *id).unwrap()
}

/// Only output and `resync_required` carry an id.
fn assert_identified_frames(frames: &[(Option<u64>, serde_json::Value)]) {
    for (id, frame) in frames {
        let identified = matches!(
            frame["type"].as_str(),
            Some("stdout" | "stderr" | "resync_required")
        );
        assert_eq!(id.is_some(), identified, "{frame}");
    }
}

fn connects(cursors: &[Option<u64>]) -> Vec<FakeOpen> {
    cursors.iter().copied().map(FakeOpen::Connect).collect()
}

/// A client that drops its first stream, and so never sends `Last-Event-ID`,
/// still gets the process's whole retained output when it attaches again.
#[tokio::test]
async fn process_reattach_replays_retained_output_from_the_start() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        true,
        Arc::new(|_, open| journal_replay(&JOURNAL, open)),
    )
    .await;

    let frames = process_events_to_close(&fake.endpoint, session_id, None).await;

    assert_eq!(stdout_text(&frames), JOURNAL.concat());
    assert_eq!(resync_reasons(&frames), Vec::<String>::new());
    assert_eq!(*fake.opens.lock().unwrap(), connects(&[Some(0)]));
}

/// `Last-Event-ID` resumes at the output after the last frame received: the
/// two streams together carry the output once, without a gap or a resync.
#[tokio::test]
async fn process_resume_continues_after_the_last_event_id() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        true,
        Arc::new(|_, open| journal_replay(&JOURNAL, open)),
    )
    .await;
    let first = process_events_to_close(&fake.endpoint, session_id, None).await;
    let received: Vec<_> = first
        .iter()
        .take_while(|(_, frame)| frame["seq"] != 3)
        .cloned()
        .collect();
    let last_event_id = last_id(&received);

    let resumed = process_events_to_close(&fake.endpoint, session_id, Some(last_event_id)).await;

    assert_eq!(stdout_text(&received), "one\ntwo\n");
    assert_eq!(stdout_text(&resumed), "three\nfour\n");
    assert_eq!(resync_reasons(&resumed), Vec::<String>::new());
    assert!(
        resumed
            .iter()
            .filter_map(|(id, _)| *id)
            .all(|id| id > last_event_id)
    );
    assert_eq!(*fake.opens.lock().unwrap(), connects(&[Some(0), Some(2)]));
}

/// Output the guest evicted before the client asked for it is a real gap:
/// the stream reports it once and continues from the oldest output the guest
/// still retains, without reporting the process start again. Its ids keep
/// carrying positions, so a resume from the gap's id is exact.
#[tokio::test]
async fn evicted_process_output_continues_from_the_oldest_retained_output() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        true,
        Arc::new(|_, open| match open {
            FakeOpen::Connect(Some(cursor)) if cursor < 2 => vec![connected(), lagged(Some(2))],
            open => journal_replay(&JOURNAL, open),
        }),
    )
    .await;

    let frames = process_events_to_close(&fake.endpoint, session_id, None).await;
    assert_eq!(resync_reasons(&frames), ["cursor_out_of_window"]);
    assert_eq!(stdout_text(&frames), "three\nfour\n");
    assert_eq!(frames_of_type(&frames, "process_started").count(), 1);
    assert!(
        frames_of_type(&frames, "error").next().is_none(),
        "{frames:?}"
    );

    let gap = frames_of_type(&frames, "resync_required")
        .next()
        .unwrap()
        .0
        .unwrap();
    let resumed = process_events_to_close(&fake.endpoint, session_id, Some(gap)).await;
    assert_eq!(resync_reasons(&resumed), Vec::<String>::new());
    assert_eq!(stdout_text(&resumed), "three\nfour\n");
    assert_ids_above(&resumed, gap);
    assert_eq!(
        *fake.opens.lock().unwrap(),
        connects(&[Some(0), Some(2), Some(2)])
    );
}

/// A guest that starts a lagged connect at its oldest retained output, and
/// says so, needs no reopen: the stream reports the gap once and continues
/// from there, with ids that keep their positions.
#[tokio::test]
async fn evicted_output_continues_where_the_guest_reports_the_stream_starts() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        true,
        Arc::new(|_, open| match open {
            FakeOpen::Connect(Some(cursor)) if cursor < 2 => {
                let mut replies = vec![connected_at(2)];
                replies.extend(
                    journal_replay(&JOURNAL, FakeOpen::Connect(Some(2)))
                        .into_iter()
                        .skip(1),
                );
                replies
            }
            open => journal_replay(&JOURNAL, open),
        }),
    )
    .await;

    let frames = process_events_to_close(&fake.endpoint, session_id, None).await;
    assert_eq!(resync_reasons(&frames), ["cursor_out_of_window"]);
    assert_eq!(stdout_text(&frames), "three\nfour\n");
    assert_eq!(frames_of_type(&frames, "process_started").count(), 1);

    let gap = frames_of_type(&frames, "resync_required")
        .next()
        .unwrap()
        .0
        .unwrap();
    let resumed = process_events_to_close(&fake.endpoint, session_id, Some(gap)).await;
    assert_eq!(resync_reasons(&resumed), Vec::<String>::new());
    assert_eq!(stdout_text(&resumed), "three\nfour\n");
    assert_ids_above(&resumed, gap);
    assert_eq!(*fake.opens.lock().unwrap(), connects(&[Some(0), Some(2)]));
}

/// A guest older than the lag reason does not say where its retained output
/// starts: the stream reports the gap once, follows the live tail without
/// reporting the process start again, and a resume from what it sent next
/// carries no position, so it replays behind `resync_required` rather than
/// silently.
#[tokio::test]
async fn evicted_output_on_an_older_guest_is_reported_before_the_live_tail() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        true,
        Arc::new(|_, open| match open {
            FakeOpen::Connect(Some(cursor)) if cursor < 2 => vec![connected(), untagged_lagged()],
            FakeOpen::Connect(None) => vec![connected(), stdout("live\n"), exited()],
            open => journal_replay(&JOURNAL, open),
        }),
    )
    .await;

    let frames = process_events_to_close(&fake.endpoint, session_id, None).await;
    assert_eq!(resync_reasons(&frames), ["cursor_out_of_window"]);
    assert_eq!(stdout_text(&frames), "live\n");
    assert_eq!(frames_of_type(&frames, "process_started").count(), 1);
    assert!(
        frames_of_type(&frames, "error").next().is_none(),
        "{frames:?}"
    );

    let last_event_id = last_id(&frames);
    let resumed = process_events_to_close(&fake.endpoint, session_id, Some(last_event_id)).await;
    assert_eq!(
        resync_reasons(&resumed),
        ["cursor_replay_unavailable", "cursor_out_of_window"]
    );
    assert_ids_above(&resumed, last_event_id);
    assert_eq!(
        *fake.opens.lock().unwrap(),
        connects(&[Some(0), None, Some(0), None])
    );
}

/// A RESOURCE_EXHAUSTED the guest did not tag as a lag, such as a hop's
/// refusal of an oversized chunk, is not reported as evicted output: the
/// stream ends with a retryable error instead of skipping to the live tail.
#[tokio::test]
async fn untagged_resource_exhausted_is_not_an_output_gap() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        true,
        Arc::new(|_, _| {
            vec![
                connected(),
                stdout(JOURNAL[0]),
                FakeReply::End(tonic::Status::resource_exhausted(
                    "guest process output chunk is too large",
                )),
            ]
        }),
    )
    .await;

    let (_state, mut chunks) = open_process_events(&fake.endpoint, session_id, None).await;
    let mut seen = String::new();
    read_until(&mut chunks, &mut seen, "backend_stream_error").await;
    let frames = sse_frames(&seen);
    assert_eq!(resync_reasons(&frames), Vec::<String>::new());
    assert_eq!(*fake.opens.lock().unwrap(), connects(&[Some(0)]));
}

/// A cursor past the output restarts at the start; when the start has been
/// evicted too, that is the same gap, reported once, whether the guest ends
/// the stream as lagged or starts it at its oldest retained output.
#[tokio::test]
async fn cursor_ahead_then_evicted_start_reports_one_gap() {
    if !binding_fake_test_process() {
        return;
    }
    for reports_start in [false, true] {
        let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
        let fake = spawn_journal_fake(
            session_id,
            true,
            Arc::new(move |_, open| match open {
                FakeOpen::Connect(Some(cursor)) if cursor < 2 && reports_start => {
                    let mut replies = vec![connected_at(2)];
                    replies.extend(
                        journal_replay(&JOURNAL, FakeOpen::Connect(Some(2)))
                            .into_iter()
                            .skip(1),
                    );
                    replies
                }
                FakeOpen::Connect(Some(cursor)) if cursor < 2 => vec![connected(), lagged(Some(2))],
                open => journal_replay(&JOURNAL, open),
            }),
        )
        .await;
        let ahead = ProcessEventId::Position {
            position: 10,
            frame: 0,
        }
        .encode();

        let frames = process_events_to_close(&fake.endpoint, session_id, Some(ahead)).await;

        assert_eq!(
            resync_reasons(&frames),
            ["cursor_out_of_window"],
            "{reports_start}"
        );
        assert_eq!(stdout_text(&frames), "three\nfour\n");
        assert_ids_above(&frames, ahead);
        let opens = if reports_start {
            connects(&[Some(10), Some(0)])
        } else {
            connects(&[Some(10), Some(0), Some(2)])
        };
        assert_eq!(*fake.opens.lock().unwrap(), opens);
    }
}

/// A process that keeps writing can evict its oldest retained output before
/// a stream that moved there reads it, when the guest could not make the
/// stream its primary reader. Chasing the next oldest would lose that race
/// on every reopen, so the stream follows the live tail instead, whether the
/// guest reported the start or ended the stream as lagged.
#[tokio::test]
async fn evicted_output_that_falls_behind_again_follows_the_live_tail() {
    if !binding_fake_test_process() {
        return;
    }
    for reports_start in [false, true] {
        let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
        let fake = spawn_journal_fake(
            session_id,
            true,
            // The oldest retained output moves past every cursor connected at.
            Arc::new(move |_, open| match open {
                FakeOpen::Connect(Some(cursor)) if reports_start => {
                    vec![connected_at(cursor + 1), lagged(Some(cursor + 2))]
                }
                FakeOpen::Connect(Some(cursor)) => vec![connected(), lagged(Some(cursor + 1))],
                FakeOpen::Connect(None) => vec![connected(), stdout("live\n"), exited()],
                FakeOpen::Start => panic!("unexpected start"),
            }),
        )
        .await;

        let frames = process_events_to_close(&fake.endpoint, session_id, None).await;

        assert!(
            frames_of_type(&frames, "error").next().is_none(),
            "{frames:?}"
        );
        assert_eq!(resync_reasons(&frames), ["cursor_out_of_window"]);
        assert_eq!(stdout_text(&frames), "live\n");
        let opens = if reports_start {
            connects(&[Some(0), None])
        } else {
            connects(&[Some(0), Some(1), None])
        };
        assert_eq!(*fake.opens.lock().unwrap(), opens, "{reports_start}");
    }
}

/// A guest that keeps refusing the cursor without ever sending output does
/// not keep the stream reopening: a start that is past the output too, a
/// guest that keeps moving the start past the output, and a live tail that
/// keeps falling behind, end with an error.
#[tokio::test]
async fn window_miss_reopens_without_progress_are_capped() {
    if !binding_fake_test_process() {
        return;
    }
    let ahead = ProcessEventId::Position {
        position: 10,
        frame: 0,
    }
    .encode();
    for (last_event_id, replies, opens) in [
        (
            Some(ahead),
            (|_| vec![connected(), cursor_ahead()]) as fn(FakeOpen) -> Vec<FakeReply>,
            connects(&[Some(10), Some(0)]),
        ),
        (
            Some(ahead),
            |open| match open {
                FakeOpen::Connect(Some(0)) => vec![connected_at(5), cursor_ahead()],
                _ => vec![connected(), cursor_ahead()],
            },
            connects(&[Some(10), Some(0), Some(0), Some(0)]),
        ),
        (
            None,
            |_| vec![connected(), lagged(None)],
            connects(&[Some(0), None, None, None]),
        ),
    ] {
        let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
        let fake =
            spawn_journal_fake(session_id, true, Arc::new(move |_, open| replies(open))).await;

        let (_state, mut chunks) =
            open_process_events(&fake.endpoint, session_id, last_event_id).await;
        let mut seen = String::new();
        read_until(&mut chunks, &mut seen, "backend_stream_error").await;
        let frames = sse_frames(&seen);

        assert_eq!(resync_reasons(&frames), ["cursor_out_of_window"]);
        assert_eq!(*fake.opens.lock().unwrap(), opens);
    }
}

/// Output between window misses is progress: a stream that keeps falling
/// behind but keeps receiving output is not ended by the reopen cap.
#[tokio::test]
async fn window_misses_with_output_between_are_not_capped() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        true,
        Arc::new(|_, open| {
            let FakeOpen::Connect(Some(cursor)) = open else {
                panic!("unexpected open {open:?}");
            };
            // Each reopen gets one line, then falls behind by one more.
            if cursor >= 12 {
                return vec![connected(), exited()];
            }
            vec![connected(), stdout("line\n"), lagged(Some(cursor + 2))]
        }),
    )
    .await;

    let frames = process_events_to_close(&fake.endpoint, session_id, None).await;

    assert!(
        frames_of_type(&frames, "error").next().is_none(),
        "{frames:?}"
    );
    assert_eq!(stdout_text(&frames), "line\n".repeat(6));
    assert_eq!(fake.opens.lock().unwrap().len(), 7);
}

/// The reopen after a window miss uses the stream's route token; when the
/// node refuses it as one a refresh replaces, such as an expired token, the
/// stream refreshes its route and continues instead of failing.
#[tokio::test]
async fn window_miss_reopen_refreshes_a_refused_route() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        true,
        Arc::new(|opened, open| match (opened, open) {
            (0, _) => vec![connected(), lagged(Some(2))],
            (1, _) => vec![refreshable_route_rejection()],
            (_, open) => journal_replay(&JOURNAL, open),
        }),
    )
    .await;

    let frames = process_events_to_close(&fake.endpoint, session_id, None).await;

    assert!(
        frames_of_type(&frames, "error").next().is_none(),
        "{frames:?}"
    );
    assert_eq!(frames_of_type(&frames, "backend_reconnected").count(), 1);
    assert_eq!(resync_reasons(&frames), ["cursor_out_of_window"]);
    assert_eq!(stdout_text(&frames), "three\nfour\n");
    assert_eq!(
        *fake.opens.lock().unwrap(),
        connects(&[Some(0), Some(2), Some(2)])
    );
}

/// Backend reconnects and client resumes before a process's next output.
const IDLE_ROUNDS: usize = 300;

/// The id of the output after the two lines the idle fakes start with.
fn third_line_id() -> u64 {
    ProcessEventId::Position {
        position: 3,
        frame: 0,
    }
    .encode()
}

/// How an idle fake answers `open`, the `opened`th: the lines from its cursor,
/// then, once `rounds` opens have gone by, a third line and the exit.
/// `fails` ends each earlier stream as an unavailable backend.
fn idle_replies(rounds: usize, fails: bool, opened: usize, open: FakeOpen) -> Vec<FakeReply> {
    let FakeOpen::Connect(Some(cursor)) = open else {
        panic!("unexpected open {open:?}");
    };
    let lines = ["one\n", "two\n", "three\n"];
    let written = if opened > rounds { 3 } else { 2 };
    let mut replies = vec![connected()];
    replies.extend(
        lines[..written]
            .iter()
            .skip(cursor as usize)
            .map(|line| stdout(line)),
    );
    if opened > rounds {
        replies.push(exited());
    } else if fails {
        replies.push(end(tonic::Code::Unavailable));
    }
    replies
}

/// The id of the frame that carries the third line.
fn third_line_frame_id(frames: &[(Option<u64>, serde_json::Value)]) -> Option<u64> {
    frames_of_type(frames, "stdout")
        .find(|(_, frame)| frame["seq"] == 3)
        .and_then(|(id, _)| *id)
}

/// Frames that carry no output carry no id either, so resumes against an
/// idle process, however many, spend none of the ids at its position: the
/// next output still carries it.
#[tokio::test]
async fn idle_resumes_keep_positional_ids() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        true,
        Arc::new(|opened, open| idle_replies(IDLE_ROUNDS, false, opened, open)),
    )
    .await;
    let (_state, mut chunks) = open_process_events(&fake.endpoint, session_id, None).await;
    let mut seen = String::new();
    read_until(&mut chunks, &mut seen, &BASE64_STANDARD.encode("two\n")).await;
    read_until(&mut chunks, &mut seen, "\n\n").await;
    drop(chunks);
    // Each resume reports the process and is dropped; like an EventSource,
    // the client resumes from the last id it has received.
    let mut last_event_id = last_id(&sse_frames(&seen));
    for _ in 0..IDLE_ROUNDS {
        let (_state, mut chunks) =
            open_process_events(&fake.endpoint, session_id, Some(last_event_id)).await;
        let mut seen = String::new();
        read_until(&mut chunks, &mut seen, "process_started").await;
        read_until(&mut chunks, &mut seen, "\n\n").await;
        if let Some(id) = sse_frames(&seen).iter().rev().find_map(|(id, _)| *id) {
            last_event_id = id;
        }
    }
    let resumed = process_events_to_close(&fake.endpoint, session_id, Some(last_event_id)).await;
    assert_eq!(stdout_text(&resumed), "three\n");
    assert_eq!(third_line_frame_id(&resumed), Some(third_line_id()));
}

/// Likewise, backend reconnects before the next output spend no ids at the
/// position, however many there are.
#[tokio::test]
async fn idle_backend_reconnects_keep_positional_ids() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        true,
        Arc::new(|opened, open| idle_replies(IDLE_ROUNDS, true, opened, open)),
    )
    .await;

    let frames = process_events_to_close(&fake.endpoint, session_id, None).await;

    assert_eq!(
        frames_of_type(&frames, "backend_reconnected").count(),
        IDLE_ROUNDS + 1
    );
    assert_eq!(resync_reasons(&frames), Vec::<String>::new());
    assert_eq!(stdout_text(&frames), "one\ntwo\nthree\n");
    assert_eq!(third_line_frame_id(&frames), Some(third_line_id()));
}

/// A bound attach whose first backend stream fails before it reports the
/// process still sends `process_started` once, from the reopened stream.
#[tokio::test]
async fn backend_failure_before_the_start_still_reports_it_once() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        true,
        Arc::new(|opened, open| match opened {
            0 => vec![end(tonic::Code::Unavailable)],
            _ => journal_replay(&JOURNAL, open),
        }),
    )
    .await;

    let frames = process_events_to_close(&fake.endpoint, session_id, None).await;

    assert_eq!(frames_of_type(&frames, "backend_reconnected").count(), 1);
    assert_eq!(frames_of_type(&frames, "process_started").count(), 1);
    assert_eq!(stdout_text(&frames), JOURNAL.concat());
    assert_eq!(resync_reasons(&frames), Vec::<String>::new());
    assert_eq!(*fake.opens.lock().unwrap(), connects(&[Some(0), Some(0)]));
}

/// A backend reconnect after a resume reopens the guest stream at the output
/// position the client has reached; that replay is lossless, so the client is
/// not told to resync.
#[tokio::test]
async fn backend_reconnect_after_a_resume_continues_without_a_resync() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        true,
        Arc::new(|opened, open| match (opened, open) {
            // The resume's stream drops after one line.
            (1, FakeOpen::Connect(Some(cursor))) => vec![
                connected(),
                stdout(JOURNAL[cursor as usize]),
                end(tonic::Code::Unavailable),
            ],
            (_, open) => journal_replay(&JOURNAL, open),
        }),
    )
    .await;
    let first = process_events_to_close(&fake.endpoint, session_id, None).await;
    let after_one = first
        .iter()
        .find(|(_, frame)| frame["seq"] == 1)
        .unwrap()
        .0
        .unwrap();

    let resumed = process_events_to_close(&fake.endpoint, session_id, Some(after_one)).await;

    assert_eq!(stdout_text(&resumed), "two\nthree\nfour\n");
    assert_eq!(frames_of_type(&resumed, "backend_reconnected").count(), 1);
    assert_eq!(frames_of_type(&resumed, "process_started").count(), 1);
    assert_eq!(resync_reasons(&resumed), Vec::<String>::new());
    assert_eq!(
        *fake.opens.lock().unwrap(),
        connects(&[Some(0), Some(1), Some(2)])
    );
}

/// A `Last-Event-ID` past the process's output (forged, or another
/// session's) cannot leave the stream waiting for output to reach it: the
/// guest refuses it, and the stream replays the output from the start behind
/// `resync_required`.
#[tokio::test]
async fn resume_past_the_output_replays_it_behind_a_resync() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        true,
        Arc::new(|_, open| journal_replay(&JOURNAL, open)),
    )
    .await;
    let ahead = ProcessEventId::Position {
        position: 10,
        frame: 0,
    }
    .encode();

    let frames = process_events_to_close(&fake.endpoint, session_id, Some(ahead)).await;

    assert_ids_above(&frames, ahead);
    assert_eq!(resync_reasons(&frames), ["cursor_out_of_window"]);
    assert_eq!(stdout_text(&frames), JOURNAL.concat());
    assert_eq!(frames_of_type(&frames, "process_started").count(), 1);
    assert_eq!(*fake.opens.lock().unwrap(), connects(&[Some(10), Some(0)]));
}

/// An id without a position, such as one an earlier data plane issued, is
/// never read as one: the stream replays the retained output behind
/// `resync_required`.
#[tokio::test]
async fn resume_from_an_untagged_id_replays_behind_a_resync() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        true,
        Arc::new(|_, open| journal_replay(&JOURNAL, open)),
    )
    .await;

    let frames = process_events_to_close(&fake.endpoint, session_id, Some(1_500_000)).await;

    assert_eq!(resync_reasons(&frames), ["cursor_replay_unavailable"]);
    assert_eq!(stdout_text(&frames), JOURNAL.concat());
    assert_ids_above(&frames, 1_500_000);
    assert_eq!(*fake.opens.lock().unwrap(), connects(&[Some(0)]));
}

/// The attach that starts a process counts its output from the start, even
/// when the client sent another stream's `Last-Event-ID`: the stream says the
/// id did not apply, numbers its frames above it, and a later resume replays
/// behind `resync_required` rather than at a wrong position.
#[tokio::test]
async fn starting_attach_ignores_a_foreign_last_event_id() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        false,
        Arc::new(|_, open| match open {
            // Still running: the stream stays open after two lines.
            FakeOpen::Start => vec![started(), stdout(JOURNAL[0]), stdout(JOURNAL[1])],
            open => journal_replay(&JOURNAL, open),
        }),
    )
    .await;
    let foreign = ProcessEventId::Position {
        position: 1,
        frame: 0,
    }
    .encode();
    let (_state, mut chunks) = open_process_events(&fake.endpoint, session_id, Some(foreign)).await;
    let mut seen = String::new();
    read_until(&mut chunks, &mut seen, &BASE64_STANDARD.encode(JOURNAL[1])).await;
    read_until(&mut chunks, &mut seen, "\n\n").await;
    drop(chunks);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !fake.bound.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the starting attach binds the process");
    let first = sse_frames(&seen);
    assert_ids_above(&first, foreign);
    assert_eq!(resync_reasons(&first), ["cursor_out_of_window"]);
    assert_eq!(stdout_text(&first), "one\ntwo\n");

    let last_event_id = last_id(&first);
    let resumed = process_events_to_close(&fake.endpoint, session_id, Some(last_event_id)).await;

    assert_ids_above(&resumed, last_event_id);
    assert_eq!(resync_reasons(&resumed), ["cursor_replay_unavailable"]);
    assert_eq!(stdout_text(&resumed), JOURNAL.concat());
    assert_eq!(
        *fake.opens.lock().unwrap(),
        [FakeOpen::Start, FakeOpen::Connect(Some(0))]
    );
}

/// An attach that finds the process already started, its binding never
/// persisted, connects at the client's position: a completed process replays
/// exactly what the client lacks, with no resync.
#[tokio::test]
async fn already_started_process_is_resumed_exactly() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        false,
        Arc::new(|_, open| match open {
            FakeOpen::Start => vec![FakeReply::Refuse(tonic::Status::already_exists("fake"))],
            open => journal_replay(&JOURNAL, open),
        }),
    )
    .await;
    let after_two = ProcessEventId::Position {
        position: 2,
        frame: 0,
    }
    .encode();

    let frames = process_events_to_close(&fake.endpoint, session_id, Some(after_two)).await;

    assert_eq!(stdout_text(&frames), "three\nfour\n");
    assert_eq!(resync_reasons(&frames), Vec::<String>::new());
    assert_ids_above(&frames, after_two);
    assert_eq!(
        *fake.opens.lock().unwrap(),
        [FakeOpen::Start, FakeOpen::Connect(Some(2))]
    );
}

/// A `Last-Event-ID` too close to the end of the id space to number a later
/// frame above is refused before anything is opened.
#[tokio::test]
async fn last_event_id_without_room_after_it_is_rejected() {
    if !binding_fake_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_journal_fake(
        session_id,
        true,
        Arc::new(|_, open| journal_replay(&JOURNAL, open)),
    )
    .await;
    let mut state = crate::tests::test_data_plane_state().await;
    state.cluster_api = ClusterApiClients::service_only(
        pplx_space_node_api_client::cluster::connect(fake.endpoint.clone(), "test")
            .await
            .unwrap(),
    );
    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "c".repeat(64));
    let response = tower::ServiceExt::oneshot(
        router(state),
        Request::get(format!("/v1/sessions/{session_id}/events"))
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header("last-event-id", MAX_LAST_EVENT_ID.to_string())
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(fake.opens.lock().unwrap().is_empty());
}
