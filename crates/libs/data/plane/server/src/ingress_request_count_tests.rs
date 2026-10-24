//! The stream-load report's count of plain service-port requests, driven
//! through the real ECW ingress handler against a scripted upstream.
//!
//! A plain request takes no attached-stream permit, so before this count a
//! replica serving only long responses (server-sent events, downloads) read as
//! idle and went first on scale-in. These pin when a request starts and stops
//! counting: it counts while its response is open, stops when the response
//! ends or the client goes away, and a declined upgrade counts like a plain
//! request because its permit doesn't outlive the handler. ECW file
//! transfers and streamed process input hold no permit either, and count
//! while they're served.

use super::*;
use crate::tests::{
    backend_endpoint, ecw_resolved_route, test_data_plane_state, test_ecw_route_origin,
};
use http_body_util::BodyExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

/// Read one request head from the handler's side of the ingress.
async fn read_request_head(upstream: &mut DuplexStream) -> String {
    let mut bytes = Vec::new();
    let mut buf = [0; 1024];
    while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
        let count = upstream.read(&mut buf).await.expect("read request");
        assert_ne!(
            count, 0,
            "handler closed the ingress before sending a request"
        );
        bytes.extend_from_slice(&buf[..count]);
    }
    String::from_utf8(bytes).expect("utf-8 request head")
}

const SSE_HEAD: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n";

/// One server-sent event as an HTTP/1.1 chunk.
const SSE_EVENT_CHUNK: &[u8] = b"9\r\ndata: 1\n\n\r\n";

struct Ingress {
    state: DataPlaneState,
    upstream: DuplexStream,
    io: IngressIo,
    progress: Progress,
}

async fn ingress() -> Option<Ingress> {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return None;
    }
    let state = test_data_plane_state().await;
    let (client, upstream) = tokio::io::duplex(64 * 1024);
    let progress = Progress::new();
    let io = IngressIo::new(
        client,
        Completion::new(CancellationToken::new(), progress.clone()),
    );
    Some(Ingress {
        state,
        upstream,
        io,
        progress,
    })
}

async fn proxy(state: &DataPlaneState, io: IngressIo, request: Request<Body>) -> Response {
    let target = EcwSandboxHost {
        sandbox_id: SandboxId::new(),
        port: 8080,
    };
    let route = ecw_resolved_route(target.sandbox_id, 1, backend_endpoint("192.0.2.1:9800"));
    ecw_service_ingress_proxy_with_io(
        state.clone(),
        target,
        route,
        request,
        io,
        state.ingress_requests.track(),
    )
    .await
    .expect("proxied response")
}

fn counted(state: &DataPlaneState) -> usize {
    state.ingress_requests.current()
}

#[tokio::test]
async fn an_open_server_sent_events_response_counts_until_it_ends() {
    let Some(Ingress {
        state,
        mut upstream,
        io,
        ..
    }) = ingress().await
    else {
        return;
    };
    let request = Request::get("/events").body(Body::empty()).unwrap();
    let (handler, head) = tokio::join!(proxy(&state, io, request), async {
        let head = read_request_head(&mut upstream).await;
        upstream.write_all(SSE_HEAD).await.unwrap();
        upstream.write_all(SSE_EVENT_CHUNK).await.unwrap();
        head
    });
    assert!(head.starts_with("GET /events "), "{head}");
    let mut body = handler.into_body();

    let first = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert_eq!(first, "data: 1\n\n");
    assert_eq!(
        counted(&state),
        1,
        "an event stream that is still open is cut by a scale-in like a stream"
    );
    assert_eq!(
        state.attached_streams.active_total(),
        0,
        "it holds no stream permit, which is why it needs its own count"
    );

    upstream.write_all(b"0\r\n\r\n").await.unwrap();
    while let Some(frame) = body.frame().await {
        frame.unwrap();
    }
    drop(body);
    assert_eq!(counted(&state), 0, "a finished response stops counting");
}

#[tokio::test]
async fn a_client_that_goes_away_mid_body_stops_counting() {
    let Some(Ingress {
        state,
        mut upstream,
        io,
        ..
    }) = ingress().await
    else {
        return;
    };
    let request = Request::get("/events").body(Body::empty()).unwrap();
    let (handler, _) = tokio::join!(proxy(&state, io, request), async {
        read_request_head(&mut upstream).await;
        upstream.write_all(SSE_HEAD).await.unwrap();
        upstream.write_all(SSE_EVENT_CHUNK).await.unwrap();
    });
    let mut body = handler.into_body();
    body.frame().await.unwrap().unwrap();
    assert_eq!(counted(&state), 1);

    // The server drops the response body when its client disconnects, while
    // the sandbox is still streaming.
    drop(body);
    assert_eq!(
        counted(&state),
        0,
        "a disconnected client's response must not keep the replica ranked busy"
    );
}

#[tokio::test]
async fn an_upgrade_the_sandbox_declines_counts_as_a_plain_request() {
    let Some(Ingress {
        state,
        mut upstream,
        io,
        ..
    }) = ingress().await
    else {
        return;
    };
    let request = Request::get("/socket")
        .header(header::CONNECTION, "Upgrade")
        .header(header::UPGRADE, "websocket")
        .body(Body::empty())
        .unwrap();
    let (handler, _) = tokio::join!(proxy(&state, io, request), async {
        read_request_head(&mut upstream).await;
        upstream.write_all(SSE_HEAD).await.unwrap();
        upstream.write_all(SSE_EVENT_CHUNK).await.unwrap();
    });
    let mut body = handler.into_body();
    body.frame().await.unwrap().unwrap();

    assert_eq!(
        state.attached_streams.active_total(),
        0,
        "the upgrade's stream permit is released once the sandbox declines it"
    );
    assert_eq!(
        counted(&state),
        1,
        "the plain response that answered the declined upgrade still counts"
    );
    drop(body);
    assert_eq!(counted(&state), 0);
}

#[tokio::test]
async fn a_stalled_response_stops_counting_when_its_progress_times_out() {
    let Some(Ingress {
        state,
        mut upstream,
        io,
        progress,
    }) = ingress().await
    else {
        return;
    };
    let request = Request::get("/download").body(Body::empty()).unwrap();
    let (handler, _) = tokio::join!(proxy(&state, io, request), async {
        read_request_head(&mut upstream).await;
        upstream.write_all(SSE_HEAD).await.unwrap();
        upstream.write_all(SSE_EVENT_CHUNK).await.unwrap();
    });
    let mut body = handler.into_body();
    body.frame().await.unwrap().unwrap();
    assert_eq!(counted(&state), 1);

    // What the ingress pump does once no byte moved within the idle timeout.
    progress.abort.cancel();
    body.frame()
        .await
        .expect("an aborted body reports why")
        .expect_err("a stalled transfer ends with an error");
    assert_eq!(
        counted(&state),
        0,
        "a transfer the ingress gave up on no longer holds the replica"
    );
}

/// A cluster API that completes the client's handshake and then never answers
/// a route lookup, so a request's ingress preparation hangs the way a slow wake
/// or an overloaded control plane leaves it.
async fn spawn_unanswering_cluster_api() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind cluster-api stub");
    let addr = listener.local_addr().expect("stub addr");
    let (_reporter, health) = pplx_space_common_svc::bootstrap::grpc_health_service();
    let routes = tonic::service::Routes::new(health)
        .into_axum_router()
        .fallback(std::future::pending::<axum::response::Response>);
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_routes(tonic::service::Routes::from(routes))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    addr
}

#[tokio::test]
async fn a_request_still_preparing_its_ingress_already_counts() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }
    let addr = spawn_unanswering_cluster_api().await;
    let mut state = test_data_plane_state().await;
    state.cluster_api = ClusterApiClients::service_only(
        pplx_space_node_api_client::cluster::connect(format!("http://{addr}"), "test")
            .await
            .expect("connect to the cluster-api stub"),
    );
    state.ecw_hosts = Arc::new(EcwHostConfig::from_env_value(Some(
        r#"[{"host":"example.test"}]"#,
    )));
    let sandbox_id = SandboxId::new();
    let request = Request::get("/events")
        .header(header::HOST, format!("8080-{sandbox_id}.example.test"))
        .body(Body::empty())
        .unwrap();
    let proxy = tokio::spawn(ecw_service_proxy(State(state.clone()), request));

    tokio::time::timeout(Duration::from_secs(5), async {
        while counted(&state) == 0 {
            assert!(
                !proxy.is_finished(),
                "the route lookup should still be pending"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a request is counted while its route and ingress are still being prepared");
    assert_eq!(counted(&state), 1);

    // The client gives up before the ingress opens.
    proxy.abort();
    let _ = proxy.await;
    assert_eq!(counted(&state), 0, "an abandoned request stops counting");
}

#[tokio::test]
async fn an_accepted_upgrade_counts_as_a_stream_not_twice() {
    let Some(Ingress {
        state,
        mut upstream,
        io,
        ..
    }) = ingress().await
    else {
        return;
    };
    // A real HTTP server in front of the handler: an upgrade can only be taken
    // over from a request the server itself parsed.
    let io = Arc::new(std::sync::Mutex::new(Some(io)));
    let handler_state = state.clone();
    let app = Router::new().route(
        "/socket",
        get(move |request: Request<Body>| {
            let state = handler_state.clone();
            let io = io.lock().unwrap().take().expect("one request");
            async move { proxy(&state, io, request).await }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });

    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
    client
        .write_all(
            b"GET /socket HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
        )
        .await
        .unwrap();
    read_request_head(&mut upstream).await;
    upstream
        .write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
        .await
        .unwrap();
    let mut head = Vec::new();
    let mut buf = [0; 256];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        let count = client.read(&mut buf).await.unwrap();
        assert_ne!(count, 0, "server closed before switching protocols");
        head.extend_from_slice(&buf[..count]);
    }
    assert!(
        head.starts_with(b"HTTP/1.1 101"),
        "{}",
        String::from_utf8_lossy(&head)
    );

    // Both sides are upgraded: bytes now pass straight through.
    client.write_all(b"ping").await.unwrap();
    let mut echoed = [0; 4];
    upstream.read_exact(&mut echoed).await.unwrap();
    assert_eq!(&echoed, b"ping");
    assert_eq!(
        state.attached_streams.active_total(),
        1,
        "the upgrade holds its stream permit"
    );
    assert_eq!(
        counted(&state),
        0,
        "an upgraded connection is counted once, by its permit, not again as a request"
    );
}

fn envd_test_process() -> bool {
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

/// A data plane whose cached envd route for `sandbox_id` points at `worker`.
async fn envd_state(sandbox_id: SandboxId, worker: &str) -> DataPlaneState {
    let mut state = test_data_plane_state().await;
    state.ecw_hosts = Arc::new(EcwHostConfig::from_env_value(Some(
        r#"[{"host":"ecw.test"}]"#,
    )));
    state.route_cache.remember_ecw(
        test_ecw_route_origin(sandbox_id).cache_key(),
        ecw_resolved_route(sandbox_id, 1, backend_endpoint(worker)),
        Duration::from_secs(60),
    );
    state
}

fn envd_request(sandbox_id: SandboxId) -> axum::http::request::Builder {
    Request::builder().header(
        header::HOST,
        format!("{ECW_ENVD_PORT}-{sandbox_id}.ecw.test"),
    )
}

/// A worker whose file read sends one chunk, `hello`, and then neither sends
/// more nor ends, like a large file still being read.
async fn spawn_worker_with_an_open_read() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = Router::new().fallback(|request: Request<Body>| async move {
        let path = request.uri().path().to_string();
        assert!(path.ends_with("/SpacedRead"), "unexpected call {path}");
        // `FileChunk { data: "hello" }`, gRPC-framed.
        let chunk = b"\x0a\x05hello";
        let mut frame = vec![0];
        frame.extend_from_slice(&(chunk.len() as u32).to_be_bytes());
        frame.extend_from_slice(chunk);
        let frames = futures_util::stream::iter([Ok::<_, std::convert::Infallible>(
            hyper::body::Frame::data(bytes::Bytes::from(frame)),
        )])
        .chain(futures_util::stream::pending());
        Response::builder()
            .header(header::CONTENT_TYPE, "application/grpc")
            .body(Body::new(http_body_util::StreamBody::new(frames)))
            .unwrap()
    });
    tokio::spawn(async move { axum::serve(listener, service).await });
    addr
}

#[tokio::test]
async fn an_open_file_download_counts_until_its_client_goes_away() {
    if !envd_test_process() {
        return;
    }
    let worker = spawn_worker_with_an_open_read().await;
    let sandbox_id = SandboxId::new();
    let state = envd_state(sandbox_id, &worker.to_string()).await;
    let request = envd_request(sandbox_id)
        .method("GET")
        .uri("/files?path=/tmp/large")
        .body(Body::empty())
        .unwrap();
    let response = tower::ServiceExt::oneshot(router(state.clone()), request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let first = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert_eq!(first, "hello");
    assert_eq!(
        counted(&state),
        1,
        "a download still streaming holds no stream permit, but a scale-in cuts it"
    );
    assert_eq!(state.attached_streams.active_total(), 0);

    drop(body);
    assert_eq!(
        counted(&state),
        0,
        "a download whose client went away stops counting"
    );
}

#[tokio::test]
async fn a_file_upload_counts_while_its_body_is_read() {
    if !envd_test_process() {
        return;
    }
    let sandbox_id = SandboxId::new();
    let state = envd_state(sandbox_id, "127.0.0.1:1").await;
    let during_read = Arc::new(std::sync::Mutex::new(None));
    let body = {
        let state = state.clone();
        let during_read = during_read.clone();
        Body::from_stream(futures_util::stream::poll_fn(move |_| {
            *during_read.lock().unwrap() = Some(counted(&state));
            std::task::Poll::Ready(None::<Result<bytes::Bytes, std::convert::Infallible>>)
        }))
    };
    let request = envd_request(sandbox_id)
        .method("POST")
        .uri("/files")
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap();
    // The empty JSON body is rejected once read; only the read matters.
    let response = tower::ServiceExt::oneshot(router(state.clone()), request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        *during_read.lock().unwrap(),
        Some(1),
        "an upload being received counts"
    );
    assert_eq!(counted(&state), 0, "a finished upload stops counting");
}

#[tokio::test]
async fn streamed_process_input_counts_while_its_client_keeps_it_open() {
    if !envd_test_process() {
        return;
    }
    let sandbox_id = SandboxId::new();
    let state = envd_state(sandbox_id, "127.0.0.1:1").await;
    // A client that has opened its input stream and not sent anything yet.
    let body = Body::from_stream(futures_util::stream::pending::<
        Result<bytes::Bytes, std::convert::Infallible>,
    >());
    let request = envd_request(sandbox_id)
        .method("POST")
        .uri("/process.Process/StreamInput")
        .header(header::CONTENT_TYPE, "application/connect+json")
        .body(body)
        .unwrap();
    let input = tokio::spawn(tower::ServiceExt::oneshot(router(state.clone()), request));

    tokio::time::timeout(Duration::from_secs(5), async {
        while counted(&state) == 0 {
            assert!(!input.is_finished(), "the input stream should stay open");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("an open input stream counts");
    assert_eq!(state.attached_streams.active_total(), 0);

    // The client goes away.
    input.abort();
    let _ = input.await;
    assert_eq!(
        counted(&state),
        0,
        "an abandoned input stream stops counting"
    );
}
