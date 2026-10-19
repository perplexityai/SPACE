use super::*;
use crate::tests::test_data_plane_state;
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tower::ServiceExt;

/// A running [`serve_and_drain`] plus the handles a test drives it with.
struct Drain {
    addr: SocketAddr,
    shutdown_token: CancellationToken,
    stream_shutdown_token: CancellationToken,
    force_token: CancellationToken,
    tasks: TaskTracker,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

async fn start_drain(
    app: impl FnOnce(CancellationToken) -> Router,
    shutdown_grace: impl Into<Option<Duration>>,
) -> Drain {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind drain listener");
    let addr = listener.local_addr().expect("drain listener addr");
    let shutdown_token = CancellationToken::new();
    let stream_shutdown_token = CancellationToken::new();
    let force_token = CancellationToken::new();
    let tasks = TaskTracker::new();
    let server = tokio::spawn(serve_and_drain(
        pplx_space_common_svc::http_listener::nodelay(listener),
        app(stream_shutdown_token.clone()),
        DrainLifecycle {
            shutdown_token: shutdown_token.clone(),
            stream_shutdown_token: stream_shutdown_token.clone(),
            tasks: tasks.clone(),
            attached_streams: Arc::new(AttachedStreamLimiter::new(4, 4, 4)),
            shutdown_grace: shutdown_grace.into(),
            force_token: force_token.clone(),
        },
    ));
    Drain {
        addr,
        shutdown_token,
        stream_shutdown_token,
        force_token,
        tasks,
        server,
    }
}

/// A route whose response body yields whatever the test sends, until the
/// test drops the sender.
fn chunk_stream_app() -> (Router, mpsc::Sender<Bytes>) {
    let (tx, rx) = mpsc::channel::<Bytes>(4);
    let rx = Arc::new(std::sync::Mutex::new(Some(rx)));
    let app = Router::new().route(
        "/stream",
        get(move || {
            let rx = rx.lock().expect("stream receiver").take();
            async move {
                let rx = rx.expect("the stream route is opened once");
                Body::from_stream(ReceiverStream::new(rx).map(Ok::<_, Infallible>))
            }
        }),
    );
    (app, tx)
}

/// A route whose response body never yields and ends only when the stream
/// shutdown token is cancelled, the way the data plane's own streams do.
fn token_bound_stream_app(stream_shutdown_token: CancellationToken) -> Router {
    Router::new().route(
        "/stream",
        get(move || {
            let shutdown = stream_shutdown_token.child_token().cancelled_owned();
            async move {
                Body::from_stream(
                    futures_util::stream::pending::<Result<Bytes, Infallible>>()
                        .take_until(shutdown),
                )
            }
        }),
    )
}

async fn open_stream(addr: SocketAddr) -> Body {
    let tcp = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect to drain server");
    let (mut sender, connection) = http1::handshake(TokioIo::new(tcp))
        .await
        .expect("HTTP/1 handshake");
    tokio::spawn(connection);
    let response = sender
        .send_request(
            Request::get("/stream")
                .header(header::HOST, "drain.test")
                .body(Body::empty())
                .expect("stream request"),
        )
        .await
        .expect("stream response");
    assert_eq!(response.status(), StatusCode::OK);
    Body::new(response.into_body())
}

async fn next_chunk(body: &mut Body) -> Option<Bytes> {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), body.frame())
            .await
            .expect("stream frame within timeout")?
            .ok()?;
        if let Ok(data) = frame.into_data() {
            return Some(data);
        }
    }
}

#[tokio::test]
async fn drain_keeps_live_streams_and_exits_when_they_end() {
    let (app, tx) = chunk_stream_app();
    let drain = start_drain(|_| app, Duration::from_secs(60)).await;
    let mut body = open_stream(drain.addr).await;
    tx.send(Bytes::from_static(b"before")).await.unwrap();
    assert_eq!(next_chunk(&mut body).await.as_deref(), Some(&b"before"[..]));

    drain.shutdown_token.cancel();

    // The listener stops accepting new connections...
    let addr = drain.addr;
    tokio::time::timeout(Duration::from_secs(5), async {
        while tokio::net::TcpStream::connect(addr).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("listener closes after SIGTERM");
    // ...while the attached stream keeps flowing, and its token stays live.
    tx.send(Bytes::from_static(b"during")).await.unwrap();
    assert_eq!(next_chunk(&mut body).await.as_deref(), Some(&b"during"[..]));
    assert!(!drain.stream_shutdown_token.is_cancelled());
    assert!(!drain.server.is_finished());

    // The last stream ending is what lets the drain finish.
    drop(tx);
    assert_eq!(next_chunk(&mut body).await, None);
    tokio::time::timeout(Duration::from_secs(5), drain.server)
        .await
        .expect("drain finishes once the stream ends")
        .unwrap()
        .unwrap();
    assert!(drain.stream_shutdown_token.is_cancelled());
}

#[tokio::test]
async fn drain_deadline_cancels_remaining_streams() {
    let shutdown_grace = Duration::from_millis(200);
    let drain = start_drain(token_bound_stream_app, shutdown_grace).await;
    let mut body = open_stream(drain.addr).await;

    let started = tokio::time::Instant::now();
    drain.shutdown_token.cancel();
    assert_eq!(next_chunk(&mut body).await, None);
    assert!(
        started.elapsed() >= shutdown_grace,
        "the stream must outlive SIGTERM until the drain deadline"
    );
    tokio::time::timeout(Duration::from_secs(5), drain.server)
        .await
        .expect("drain finishes after cancelling streams")
        .unwrap()
        .unwrap();
    assert!(drain.stream_shutdown_token.is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn drain_waits_for_tracked_upgraded_connections() {
    let drain = start_drain(|_| Router::new(), Duration::from_secs(60)).await;
    let upgraded = drain.tasks.token();

    drain.shutdown_token.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !drain.server.is_finished(),
        "an upgraded connection is a live stream the drain must wait for"
    );

    drop(upgraded);
    tokio::time::timeout(Duration::from_secs(5), drain.server)
        .await
        .expect("drain finishes once the upgraded connection ends")
        .unwrap()
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn drain_exits_when_a_stream_ignores_cancellation() {
    let drain = start_drain(|_| Router::new(), Duration::from_secs(60)).await;
    let _stuck = drain.tasks.token();

    let started = tokio::time::Instant::now();
    drain.shutdown_token.cancel();
    drain.server.await.unwrap().unwrap();

    assert!(drain.stream_shutdown_token.is_cancelled());
    assert_eq!(
        started.elapsed(),
        Duration::from_secs(60) + STREAM_CANCEL_GRACE,
        "exit at the deadline plus the cancel grace, not when the stream ends"
    );
}

fn insecure_dev_test_process() -> bool {
    pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )])
}

#[tokio::test]
async fn readyz_fails_once_draining_starts() {
    if !insecure_dev_test_process() {
        return;
    }
    let state = test_data_plane_state().await;
    let drain_started = state.drain_started.clone();
    let app = router(state);
    let readyz = || Request::get("/readyz").body(Body::empty()).unwrap();

    let response = app.clone().oneshot(readyz()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    drain_started.cancel();
    let response = app.clone().oneshot(readyz()).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "draining"
    );
    // Liveness is unaffected: the process is healthy, just leaving.
    let response = app
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

fn assert_draining_headers(response: &Response) {
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers().get(header::CONNECTION).unwrap(), "close");
    assert_eq!(response.headers().get(header::RETRY_AFTER).unwrap(), "1");
}

#[tokio::test]
async fn native_event_stream_is_refused_while_draining() {
    if !insecure_dev_test_process() {
        return;
    }
    let state = test_data_plane_state().await;
    state.drain_started.cancel();
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "a".repeat(64));

    let response = router(state)
        .oneshot(
            Request::get(format!("/v1/sessions/{session_id}/events"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Refused before route resolution: the stub cluster API would otherwise
    // fail the request with a different status.
    assert_draining_headers(&response);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({
            "error": {
                "code": "service_unavailable",
                "message": "data plane is draining; retry",
                "retryable": true,
            }
        })
    );
}

#[tokio::test]
async fn ecw_stream_permit_is_refused_while_draining() {
    if !insecure_dev_test_process() {
        return;
    }
    let state = test_data_plane_state().await;
    state.drain_started.cancel();

    let Err(error) = acquire_ecw_stream_permit(&state, ECW_PROCESS_STREAM_KIND) else {
        panic!("no new ECW stream while draining");
    };

    assert_eq!(state.attached_streams.active_total(), 0);
    let response = error.into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers().get(header::RETRY_AFTER).unwrap(), "1");
    assert!(response.extensions().get::<DrainingRefusal>().is_some());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({ "code": 503, "message": "Data plane is draining; retry" })
    );
}

#[tokio::test]
async fn streams_attached_before_draining_keep_their_permits() {
    if !insecure_dev_test_process() {
        return;
    }
    let state = test_data_plane_state().await;
    let permit = acquire_ecw_stream_permit(&state, ECW_PROCESS_STREAM_KIND)
        .expect("stream attaches before the drain");

    state.drain_started.cancel();

    assert_eq!(state.attached_streams.active_total(), 1);
    drop(permit);
    assert_eq!(state.attached_streams.active_total(), 0);
}

#[tokio::test]
async fn drain_refusals_close_http1_connections_only() {
    let app = Router::new()
        .route(
            "/stream",
            get(|| async {
                let mut response = StatusCode::SERVICE_UNAVAILABLE.into_response();
                mark_draining_response(&mut response);
                response
            }),
        )
        .route("/unary", get(|| async { "ok" }))
        .layer(middleware::from_fn(close_draining_http1_connections));
    let request = |path: &str, version| {
        Request::get(path)
            .version(version)
            .body(Body::empty())
            .unwrap()
    };

    let response = app
        .clone()
        .oneshot(request("/stream", axum::http::Version::HTTP_11))
        .await
        .unwrap();
    assert_draining_headers(&response);

    // HTTP/2 forbids connection-specific headers; GOAWAY does that job.
    let response = app
        .clone()
        .oneshot(request("/stream", axum::http::Version::HTTP_2))
        .await
        .unwrap();
    assert_eq!(response.headers().get(header::RETRY_AFTER).unwrap(), "1");
    assert!(response.headers().get(header::CONNECTION).is_none());

    // Responses that are not drain refusals are left alone.
    let response = app
        .oneshot(request("/unary", axum::http::Version::HTTP_11))
        .await
        .unwrap();
    assert!(response.headers().get(header::CONNECTION).is_none());
}

/// Read one server WebSocket frame header from `tcp` and return its opcode and
/// payload. Server frames are never masked, so the header is two bytes plus an
/// optional extended length.
async fn read_server_frame(tcp: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0_u8; 2];
    tcp.read_exact(&mut header).await.expect("frame header");
    let opcode = header[0] & 0x0f;
    let len = match header[1] & 0x7f {
        126 => {
            let mut extended = [0_u8; 2];
            tcp.read_exact(&mut extended)
                .await
                .expect("extended length");
            usize::from(u16::from_be_bytes(extended))
        }
        len => usize::from(len),
    };
    let mut payload = vec![0_u8; len];
    tcp.read_exact(&mut payload).await.expect("frame payload");
    (opcode, payload)
}

#[tokio::test]
async fn unattached_websocket_is_told_to_reconnect_when_the_drain_starts() {
    if !insecure_dev_test_process() {
        return;
    }
    let shutdown_token = CancellationToken::new();
    let mut state = test_data_plane_state().await;
    state.drain_started = shutdown_token.child_token();
    let stream_shutdown_token = state.stream_shutdown_token.clone();
    let tasks = state.upgrade_tasks.clone();
    let tracked = state.upgrade_tasks.clone();
    let attached_streams = Arc::clone(&state.attached_streams);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_and_drain(
        pplx_space_common_svc::http_listener::nodelay(listener),
        router(state),
        DrainLifecycle {
            shutdown_token: shutdown_token.clone(),
            stream_shutdown_token,
            tasks,
            attached_streams,
            shutdown_grace: Some(Duration::from_secs(60)),
            force_token: CancellationToken::new(),
        },
    ));

    // Upgrade without a header token, so the server waits for the first
    // frame: the socket is accepted but not attached.
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    // Any 16 bytes, base64-encoded, make a valid handshake key.
    let key = BASE64_STANDARD.encode(b"drain-test-nonce");
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    tcp.write_all(
        format!(
            "GET /v1/sessions/{session_id}/attach HTTP/1.1\r\n\
             Host: drain.test\r\n\
             Connection: Upgrade\r\n\
             Upgrade: websocket\r\n\
             Sec-WebSocket-Version: 13\r\n\
             Sec-WebSocket-Key: {key}\r\n\r\n"
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
    assert!(
        response.starts_with(b"HTTP/1.1 101"),
        "{}",
        String::from_utf8_lossy(&response)
    );
    // The upgraded socket counts as a task once the HTTP connection is gone,
    // so the drain waits for it.
    assert_eq!(tracked.len(), 1, "the attach holds a tracker token");

    shutdown_token.cancel();

    let (opcode, payload) =
        tokio::time::timeout(Duration::from_secs(5), read_server_frame(&mut tcp))
            .await
            .expect("close frame without waiting for the attach deadline");
    assert_eq!(opcode, 0x8, "close frame");
    assert_eq!(
        payload.get(..2),
        Some(&axum::extract::ws::close_code::RESTART.to_be_bytes()[..]),
        "1012 service restart"
    );
    // Nothing is attached, so the drain finishes long before its 60s budget.
    drop(tcp);
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("drain finishes once the socket is refused")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn native_proxy_upgrade_is_refused_while_draining() {
    if !insecure_dev_test_process() {
        return;
    }
    let state = test_data_plane_state().await;
    state.drain_started.cancel();
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Port);
    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "a".repeat(64));

    let response = router(state)
        .oneshot(
            Request::get(format!("/v1/sessions/{session_id}/proxy"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONNECTION, "upgrade")
                .header(header::UPGRADE, "websocket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Refused before route resolution, which the stub cluster API would fail.
    assert_draining_headers(&response);
}

#[tokio::test]
async fn ecw_service_upgrade_is_refused_while_draining() {
    if !insecure_dev_test_process() {
        return;
    }
    let mut state = test_data_plane_state().await;
    state.ecw_hosts = Arc::new(EcwHostConfig::from_env_value(Some(
        r#"[{"host":"example.test"}]"#,
    )));
    state.drain_started.cancel();
    let sandbox = SandboxId::new();

    let response = router(state)
        .oneshot(
            Request::get("/socket")
                .header(header::HOST, format!("8080-{sandbox}.example.test"))
                .header(header::CONNECTION, "upgrade")
                .header(header::UPGRADE, "websocket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_draining_headers(&response);
}

#[tokio::test]
async fn streamed_process_input_is_refused_while_draining() {
    if !insecure_dev_test_process() {
        return;
    }
    let state = test_data_plane_state().await;
    state.drain_started.cancel();

    let response = router(state)
        .oneshot(
            Request::post("/process.Process/StreamInput")
                .header(header::CONTENT_TYPE, "application/connect+json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Refused before route resolution, which these headers would fail.
    assert_draining_headers(&response);
}

#[tokio::test]
async fn before_stream_deadline_runs_the_open_unless_the_deadline_wins() {
    let token = CancellationToken::new();
    assert_eq!(before_stream_deadline(&token, async { 7 }).await, Some(7));

    token.cancel();
    assert_eq!(
        before_stream_deadline(&token, std::future::pending::<()>()).await,
        None
    );
}

#[tokio::test(start_paused = true)]
async fn drain_without_a_deadline_never_cancels_streams() {
    let drain = start_drain(|_| Router::new(), None).await;
    let upgraded = drain.tasks.token();

    drain.shutdown_token.cancel();
    tokio::time::sleep(Duration::from_secs(3600)).await;
    assert!(!drain.server.is_finished(), "no deadline ends the drain");
    assert!(!drain.stream_shutdown_token.is_cancelled());

    drop(upgraded);
    drain.server.await.unwrap().unwrap();
    assert!(drain.stream_shutdown_token.is_cancelled());
}

/// How the fake worker answers `SpacedStartProcess` and `ServicePortIngress`.
#[derive(Clone, Copy)]
enum FakeStart {
    /// One `Started` event (none for an ingress), then the stream stays open
    /// like a running command.
    StartedThenRunning,
    /// No answer at all: the open is still in flight.
    NeverAnswers,
}

/// A fake that answers as both the cluster API and the worker API, so a
/// native session's route resolves to it and its process stream comes from
/// it. Returns its `http://` endpoint.
/// Returns its endpoint, a notifier that fires each time it opens a backend
/// stream (process, watch or ingress), and one that fires each time it takes
/// a revocation check, which it never answers.
async fn spawn_fake_backend(
    session_id: DataPlaneSessionId,
    start: FakeStart,
) -> (String, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
    let opened = Arc::new(tokio::sync::Notify::new());
    let opened_by_fake = Arc::clone(&opened);
    let revocation_checked = Arc::new(tokio::sync::Notify::new());
    let revocation_checked_by_fake = Arc::clone(&revocation_checked);
    use prost::Message as _;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let mut route = crate::tests::route_response_with_token(session_id, None);
    route.backend.as_mut().unwrap().endpoint = endpoint.clone();
    route.adapter_state_json = match session_id.kind() {
        SessionKind::Process => r#"{"command":["sleep","60"]}"#,
        SessionKind::FileWatch => r#"{"path":"/tmp"}"#,
        _ => "{}",
    }
    .to_string();
    if session_id.kind() == SessionKind::Port {
        // A raw TCP port session, reached through service-port ingress.
        route.target_port = Some(8080);
        route.target_protocol = Some("tcp".to_string());
        route.space_api_route_token = Some(crate::tests::proto_service_port_route_token());
    }
    let route = route.encode_to_vec();
    let process_id = pplx_space_public_api::process_id_for_session(session_id);
    let started = pplx_space_sbx_mgr_client::proto::SpacedProcessEvent {
        process_id: process_id.to_string(),
        pid: Some(42),
        event: Some(
            pplx_space_sbx_mgr_client::proto::spaced_process_event::Event::Started(
                pplx_space_sbx_mgr_client::operations::ProcessStarted { start_time: None },
            ),
        ),
    }
    .encode_to_vec();
    let service = Router::new().fallback(move |request: Request<Body>| {
        let route = route.clone();
        let started = started.clone();
        let opened = Arc::clone(&opened_by_fake);
        let revocation_checked = Arc::clone(&revocation_checked_by_fake);
        async move {
            let path = request.uri().path().to_string();
            if path.ends_with("/Check") {
                return crate::tests::grpc_test_response(vec![8, 1]);
            }
            if path.ends_with("/GetDataPlaneOrgRevocationEpoch")
                || path.ends_with("/GetDataPlaneSessionRevocationState")
            {
                // A cluster API that stalls on the check, at either level.
                revocation_checked.notify_one();
                return std::future::pending().await;
            }
            if path.ends_with("/ResolveDataPlaneSessionRoute") {
                return crate::tests::grpc_test_response(route);
            }
            if path.ends_with("/BindDataPlaneSessionProcess") {
                return crate::tests::grpc_test_response(Vec::new());
            }
            let open_stream = |first: Option<Vec<u8>>| {
                opened.notify_one();
                let first = first.map(|event| {
                    let mut message = vec![0];
                    message.extend_from_slice(&(event.len() as u32).to_be_bytes());
                    message.extend(event);
                    Ok::<_, Infallible>(hyper::body::Frame::data(Bytes::from(message)))
                });
                let frames =
                    futures_util::stream::iter(first).chain(futures_util::stream::pending());
                Response::builder()
                    .header("content-type", "application/grpc")
                    .body(Body::new(http_body_util::StreamBody::new(frames)))
                    .unwrap()
            };
            if path.ends_with("/ServicePortIngress") {
                if let FakeStart::NeverAnswers = start {
                    opened.notify_one();
                    return std::future::pending().await;
                }
                // An ingress whose service hasn't sent anything yet.
                return open_stream(None);
            }
            if path.ends_with("/SpacedWatch") {
                // An open watch that hasn't seen a change yet.
                return open_stream(None);
            }
            assert!(
                path.ends_with("/SpacedStartProcess"),
                "unexpected call {path}"
            );
            match start {
                FakeStart::NeverAnswers => {
                    opened.notify_one();
                    std::future::pending().await
                }
                FakeStart::StartedThenRunning => open_stream(Some(started)),
            }
        }
    });
    tokio::spawn(async move {
        axum::serve(listener, service).await.unwrap();
    });
    (endpoint, opened, revocation_checked)
}

/// A data-plane state whose cluster API is the fake at `endpoint`, with
/// revocation checks pushed out of the test's way.
async fn state_with_fake_backend(endpoint: &str) -> DataPlaneState {
    let mut state = test_data_plane_state().await;
    state.cluster_api = ClusterApiClients::service_only(
        pplx_space_node_api_client::cluster::connect(endpoint.to_string(), "test")
            .await
            .unwrap(),
    );
    state.revocation_check_interval = Duration::from_secs(3600);
    state
}

/// The test process for the fake-backed tests: insecure internal RPC, plus
/// the internal token the data plane attaches to its worker calls.
fn fake_backend_test_process() -> bool {
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

fn native_token() -> String {
    format!("{DATA_PLANE_TOKEN_PREFIX}{}", "b".repeat(64))
}

/// The frame a native stream of this kind sends once it is live: a process
/// reports its start, a file watch only its attach.
fn live_marker(kind: SessionKind) -> &'static str {
    match kind {
        SessionKind::Process | SessionKind::Terminal => "process_started",
        _ => "attached",
    }
}

/// How soon after the deadline a live stream must end. A stalled revocation
/// check gives up after about 5 seconds here, so this tells the deadline
/// ending the stream apart from the check failing.
const AT_ONCE: Duration = Duration::from_secs(1);

/// What a live stream is doing when the drain deadline passes.
#[derive(Clone, Copy, Debug)]
enum AtTheDeadline {
    /// Waiting for backend events or client frames.
    Idle,
    /// Inside a revocation check that the cluster API never answers.
    StalledInRevocationCheck,
}

impl AtTheDeadline {
    fn configure(self, state: &mut DataPlaneState) {
        if let Self::StalledInRevocationCheck = self {
            state.revocation_check_interval = Duration::from_millis(50);
        }
    }

    async fn reached(self, revocation_checked: &tokio::sync::Notify) {
        if let Self::StalledInRevocationCheck = self {
            tokio::time::timeout(Duration::from_secs(5), revocation_checked.notified())
                .await
                .expect("a revocation check reached the cluster API");
        }
    }
}

#[tokio::test]
async fn native_event_streams_end_when_the_drain_deadline_passes() {
    if !fake_backend_test_process() {
        return;
    }
    for (kind, at_the_deadline) in [SessionKind::Process, SessionKind::FileWatch]
        .into_iter()
        .flat_map(|kind| {
            [AtTheDeadline::Idle, AtTheDeadline::StalledInRevocationCheck]
                .map(|at_the_deadline| (kind, at_the_deadline))
        })
    {
        let session_id = DataPlaneSessionId::new_for_kind(kind);
        let (endpoint, _opened, revocation_checked) =
            spawn_fake_backend(session_id, FakeStart::StartedThenRunning).await;
        let mut state = state_with_fake_backend(&endpoint).await;
        at_the_deadline.configure(&mut state);
        let stream_shutdown_token = state.stream_shutdown_token.clone();

        let response = router(state)
            .oneshot(
                Request::get(format!("/v1/sessions/{session_id}/events"))
                    .header(header::AUTHORIZATION, format!("Bearer {}", native_token()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            panic!("{kind:?}: {status}: {}", String::from_utf8_lossy(&body));
        }
        // Read the body throughout: the stream only runs, revocation ticks
        // included, while its body is polled.
        let mut body = response.into_body();
        let (chunk_tx, mut chunks) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(chunk) = next_chunk(&mut body).await {
                if chunk_tx.send(chunk).is_err() {
                    break;
                }
            }
        });
        let mut seen = String::new();
        while !seen.contains(live_marker(kind)) {
            let chunk = chunks.recv().await.expect("event before the deadline");
            seen.push_str(&String::from_utf8_lossy(&chunk));
        }
        at_the_deadline.reached(&revocation_checked).await;

        stream_shutdown_token.cancel();

        // The body ends rather than staying open, so the client reconnects.
        tokio::time::timeout(AT_ONCE, async {
            while let Some(chunk) = chunks.recv().await {
                assert!(
                    !String::from_utf8_lossy(&chunk).contains("event: "),
                    "{kind:?} {at_the_deadline:?}: no further events after the deadline"
                );
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{kind:?} {at_the_deadline:?}: body ends at the deadline"));
    }
}

#[tokio::test]
async fn native_event_stream_still_opening_at_the_deadline_gets_the_draining_503() {
    if !fake_backend_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let (endpoint, opened, _) = spawn_fake_backend(session_id, FakeStart::NeverAnswers).await;
    let state = state_with_fake_backend(&endpoint).await;
    let stream_shutdown_token = state.stream_shutdown_token.clone();
    let response = tokio::spawn(
        router(state).oneshot(
            Request::get(format!("/v1/sessions/{session_id}/events"))
                .header(header::AUTHORIZATION, format!("Bearer {}", native_token()))
                .body(Body::empty())
                .unwrap(),
        ),
    );
    // The deadline passes while the backend stream is still opening.
    tokio::time::timeout(Duration::from_secs(5), opened.notified())
        .await
        .expect("the open reached the backend");
    stream_shutdown_token.cancel();

    let response = tokio::time::timeout(Duration::from_secs(5), response)
        .await
        .expect("answered without waiting for the open")
        .unwrap()
        .unwrap();
    assert_draining_headers(&response);
}

#[tokio::test]
async fn native_websockets_close_with_1012_at_the_drain_deadline() {
    if !fake_backend_test_process() {
        return;
    }
    for kind in [
        SessionKind::Process,
        SessionKind::Terminal,
        SessionKind::FileWatch,
        SessionKind::Port,
    ] {
        for at_the_deadline in [AtTheDeadline::Idle, AtTheDeadline::StalledInRevocationCheck] {
            websocket_closes_with_1012_at_the_drain_deadline(kind, at_the_deadline).await;
        }
    }
}

async fn websocket_closes_with_1012_at_the_drain_deadline(
    kind: SessionKind,
    at_the_deadline: AtTheDeadline,
) {
    let session_id = DataPlaneSessionId::new_for_kind(kind);
    let (endpoint, opened, revocation_checked) =
        spawn_fake_backend(session_id, FakeStart::StartedThenRunning).await;
    let mut state = state_with_fake_backend(&endpoint).await;
    at_the_deadline.configure(&mut state);
    let stream_shutdown_token = state.stream_shutdown_token.clone();
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

    let key = BASE64_STANDARD.encode(b"drain-test-nonce");
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    tcp.write_all(
        format!(
            "GET /v1/sessions/{session_id}/attach HTTP/1.1\r\n\
             Host: drain.test\r\n\
             Authorization: Bearer {}\r\n\
             Connection: Upgrade\r\n\
             Upgrade: websocket\r\n\
             Sec-WebSocket-Version: 13\r\n\
             Sec-WebSocket-Key: {key}\r\n\r\n",
            native_token()
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
    // Wait until the process is attached and running.
    loop {
        let (opcode, payload) =
            tokio::time::timeout(Duration::from_secs(5), read_server_frame(&mut tcp))
                .await
                .expect("frame before the deadline");
        assert_eq!(opcode, 0x1, "{kind:?}: text frames before the deadline");
        if String::from_utf8_lossy(&payload).contains(live_marker(kind)) {
            break;
        }
    }

    // Cancel once the bridge is in its main loop, past opening its backend
    // stream; the opening phase has its own deadline branch.
    tokio::time::timeout(Duration::from_secs(5), opened.notified())
        .await
        .expect("backend stream opened");
    tokio::time::sleep(Duration::from_millis(100)).await;
    at_the_deadline.reached(&revocation_checked).await;

    stream_shutdown_token.cancel();

    let (opcode, payload) = tokio::time::timeout(AT_ONCE, read_server_frame(&mut tcp))
        .await
        .unwrap_or_else(|_| panic!("{kind:?} {at_the_deadline:?}: close frame at the deadline"));
    assert_eq!(opcode, 0x8, "{kind:?} {at_the_deadline:?}: close frame");
    assert_eq!(
        payload.get(..2),
        Some(&axum::extract::ws::close_code::RESTART.to_be_bytes()[..]),
        "{kind:?} {at_the_deadline:?}: 1012 service restart"
    );
}

#[tokio::test]
async fn streamed_process_input_still_open_at_the_deadline_gets_a_409() {
    if !insecure_dev_test_process() {
        return;
    }
    let mut state = test_data_plane_state().await;
    state.ecw_hosts = Arc::new(EcwHostConfig::from_env_value(Some(
        r#"[{"host":"example.test"}]"#,
    )));
    let sandbox = SandboxId::new();
    let route = crate::tests::ecw_resolved_route(
        sandbox,
        1,
        crate::tests::backend_endpoint("http://127.0.0.1:1"),
    );
    state
        .route_cache
        .remember_ecw(route.origin.cache_key(), route, Duration::from_secs(60));
    let stream_shutdown_token = state.stream_shutdown_token.clone();
    // A client that opened the input stream and hasn't sent anything yet.
    let (_input, body) = mpsc::channel::<Result<Bytes, Infallible>>(1);
    let request = Request::post("/process.Process/StreamInput")
        .header(
            header::HOST,
            format!("{ECW_ENVD_PORT}-{sandbox}.example.test"),
        )
        .header(header::CONTENT_TYPE, "application/connect+json")
        .body(Body::from_stream(ReceiverStream::new(body)))
        .unwrap();
    let response = tokio::spawn(router(state).oneshot(request));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !response.is_finished(),
        "the stream is open until the deadline"
    );

    stream_shutdown_token.cancel();

    let response = tokio::time::timeout(Duration::from_secs(5), response)
        .await
        .expect("answered at the deadline")
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(response.headers().get(header::RETRY_AFTER).is_none());
}

#[tokio::test]
async fn native_proxy_still_preparing_at_the_deadline_gets_the_draining_503() {
    if !fake_backend_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Port);
    let (endpoint, opened, _) = spawn_fake_backend(session_id, FakeStart::NeverAnswers).await;
    let state = state_with_fake_backend(&endpoint).await;
    let stream_shutdown_token = state.stream_shutdown_token.clone();
    let response = tokio::spawn(
        router(state).oneshot(
            Request::get(format!("/v1/sessions/{session_id}/proxy"))
                .header(header::AUTHORIZATION, format!("Bearer {}", native_token()))
                .body(Body::empty())
                .unwrap(),
        ),
    );
    // The deadline passes while the service-port ingress is still opening.
    tokio::time::timeout(Duration::from_secs(5), opened.notified())
        .await
        .expect("the ingress open reached the backend");
    stream_shutdown_token.cancel();

    let response = tokio::time::timeout(Duration::from_secs(5), response)
        .await
        .expect("answered without waiting for the ingress")
        .unwrap()
        .unwrap();
    assert_draining_headers(&response);
}

#[tokio::test(start_paused = true)]
async fn second_stop_signal_cancels_streams_without_a_deadline() {
    let drain = start_drain(|_| Router::new(), None).await;
    let _stuck = drain.tasks.token();
    drain.shutdown_token.cancel();
    tokio::time::sleep(Duration::from_secs(3600)).await;
    assert!(!drain.stream_shutdown_token.is_cancelled());

    let forced = tokio::time::Instant::now();
    drain.force_token.cancel();
    drain.server.await.unwrap().unwrap();

    assert!(drain.stream_shutdown_token.is_cancelled());
    assert_eq!(
        forced.elapsed(),
        STREAM_CANCEL_GRACE,
        "the forced drain still gives cancelled streams their grace"
    );
}

#[tokio::test]
async fn native_websockets_still_opening_close_with_1012_at_the_drain_deadline() {
    if !fake_backend_test_process() {
        return;
    }
    for kind in [SessionKind::Process, SessionKind::Port] {
        let session_id = DataPlaneSessionId::new_for_kind(kind);
        let (endpoint, opened, _) = spawn_fake_backend(session_id, FakeStart::NeverAnswers).await;
        let state = state_with_fake_backend(&endpoint).await;
        let stream_shutdown_token = state.stream_shutdown_token.clone();
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
        let key = BASE64_STANDARD.encode(b"drain-test-nonce");
        let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        tcp.write_all(
            format!(
                "GET /v1/sessions/{session_id}/attach HTTP/1.1\r\n\
                 Host: drain.test\r\n\
                 Authorization: Bearer {}\r\n\
                 Connection: Upgrade\r\n\
                 Upgrade: websocket\r\n\
                 Sec-WebSocket-Version: 13\r\n\
                 Sec-WebSocket-Key: {key}\r\n\r\n",
                native_token()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        // The deadline passes while the bridge is still opening its backend
        // stream.
        tokio::time::timeout(Duration::from_secs(5), opened.notified())
            .await
            .expect("the open reached the backend");
        stream_shutdown_token.cancel();

        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\n") {
            let mut byte = [0_u8; 1];
            tcp.read_exact(&mut byte).await.expect("upgrade response");
            response.push(byte[0]);
        }
        assert!(response.starts_with(b"HTTP/1.1 101"), "{kind:?}");
        loop {
            let (opcode, payload) =
                tokio::time::timeout(Duration::from_secs(5), read_server_frame(&mut tcp))
                    .await
                    .expect("close frame at the deadline");
            if opcode == 0x8 {
                assert_eq!(
                    payload.get(..2),
                    Some(&axum::extract::ws::close_code::RESTART.to_be_bytes()[..]),
                    "{kind:?}: 1012 service restart"
                );
                break;
            }
            assert_eq!(opcode, 0x1, "{kind:?}: only the attached frame comes first");
        }
    }
}
