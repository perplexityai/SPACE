use super::*;
use crate::tests::{grpc_test_response, route_response_with_token, test_data_plane_state};
use std::collections::BTreeMap;
use std::sync::Mutex;

const API_KEY: &str = "Bearer test-api-key";

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

fn attach_token() -> String {
    format!("{DATA_PLANE_TOKEN_PREFIX}{}", "c".repeat(64))
}

/// One request the fake API received.
#[derive(Clone, Debug)]
struct ApiCall {
    path: String,
    headers: HeaderMap,
    body: serde_json::Value,
}

#[derive(Default)]
struct FakeApi {
    calls: Mutex<Vec<ApiCall>>,
    /// Idempotency key to the session it created, as the API replays them.
    sessions: Mutex<BTreeMap<String, DataPlaneSessionId>>,
}

/// A stand-in for the API's `createProcessSession`: it authenticates the API
/// key, requires an idempotency key and replays a key's session.
async fn spawn_fake_api(session_id: DataPlaneSessionId) -> (String, Arc<FakeApi>) {
    let api = Arc::new(FakeApi::default());
    let recorded = Arc::clone(&api);
    let service = Router::new().fallback(move |request: Request<Body>| {
        let api = Arc::clone(&recorded);
        async move {
            let (parts, body) = request.into_parts();
            let body = body.collect().await.unwrap().to_bytes();
            api.calls.lock().unwrap().push(ApiCall {
                path: parts.uri.path().to_string(),
                headers: parts.headers.clone(),
                body: serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
            });
            if parts.headers.get(header::AUTHORIZATION).map(HeaderValue::as_bytes)
                != Some(API_KEY.as_bytes())
            {
                return (
                    StatusCode::UNAUTHORIZED,
                    axum::Json(serde_json::json!({
                        "error": {"code": "unauthorized", "message": "invalid API key"}
                    })),
                );
            }
            let Some(key) = parts
                .headers
                .get("idempotency-key")
                .and_then(|value| value.to_str().ok())
            else {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(serde_json::json!({
                        "error": {"code": "invalid_request", "message": "Idempotency-Key header is required"}
                    })),
                );
            };
            let mut sessions = api.sessions.lock().unwrap();
            let next = if sessions.is_empty() {
                session_id
            } else {
                DataPlaneSessionId::new_for_kind(SessionKind::Process)
            };
            let id = *sessions.entry(key.to_string()).or_insert(next);
            (
                StatusCode::CREATED,
                axum::Json(serde_json::json!({
                    "id": id.to_string(),
                    "kind": "process",
                    "state": "pending",
                    "attach": {
                        "url": format!("/v1/sessions/{id}/attach"),
                        "events_url": null,
                        "token": attach_token(),
                        "expires_at": "2030-01-01T00:00:00Z",
                        "token_transport": ["authorization_header"],
                    },
                })),
            )
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, service).await.unwrap() });
    (origin, api)
}

/// A cluster API and worker in one: the session's route resolves to it, its
/// process starts and keeps running, and every control call is recorded by
/// method name with its request message.
async fn spawn_fake_backend(
    session_id: DataPlaneSessionId,
) -> (String, Arc<Mutex<Vec<(String, Vec<u8>)>>>) {
    use prost::Message as _;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&calls);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let mut route = route_response_with_token(session_id, None);
    route.backend.as_mut().unwrap().endpoint = endpoint.clone();
    route.adapter_state_json = r#"{"command":["sleep","60"]}"#.to_string();
    let route = route.encode_to_vec();
    let started = pplx_space_sbx_mgr_client::proto::SpacedProcessEvent {
        process_id: process_id_for_session(session_id).to_string(),
        pid: Some(42),
        event: Some(spaced_process_event::Event::Started(
            pplx_space_sbx_mgr_client::operations::ProcessStarted { start_time: None },
        )),
    }
    .encode_to_vec();
    let service = Router::new().fallback(move |request: Request<Body>| {
        let route = route.clone();
        let started = started.clone();
        let calls = Arc::clone(&recorded);
        async move {
            let path = request.uri().path().to_string();
            let method = path.rsplit('/').next().unwrap_or_default().to_string();
            let body = request.into_body().collect().await.unwrap().to_bytes();
            calls
                .lock()
                .unwrap()
                .push((method.clone(), body.get(5..).unwrap_or_default().to_vec()));
            match method.as_str() {
                "ResolveDataPlaneSessionRoute" => grpc_test_response(route),
                "SpacedStartProcess" => {
                    let mut message = vec![0];
                    message.extend_from_slice(&(started.len() as u32).to_be_bytes());
                    message.extend(started);
                    let frames = futures_util::stream::iter([Ok::<_, Infallible>(
                        hyper::body::Frame::data(Bytes::from(message)),
                    )])
                    .chain(futures_util::stream::pending());
                    Response::builder()
                        .header("content-type", "application/grpc")
                        .body(Body::new(http_body_util::StreamBody::new(frames)))
                        .unwrap()
                }
                _ => grpc_test_response(Vec::new()),
            }
        }
    });
    tokio::spawn(async move { axum::serve(listener, service).await.unwrap() });
    (endpoint, calls)
}

/// A data plane whose creates go to `api` (none: the feature is off) and
/// whose routes and processes come from `backend`.
async fn serve_data_plane(api: Option<&str>, backend: &str) -> SocketAddr {
    let mut state = test_data_plane_state().await;
    state.cluster_api = ClusterApiClients::service_only(
        pplx_space_node_api_client::cluster::connect(backend.to_string(), "test")
            .await
            .unwrap(),
    );
    state.revocation_check_interval = Duration::from_secs(3600);
    state.session_create = api.map(|origin| ProcessSessionCreator::new(origin).unwrap());
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
    addr
}

/// Open a WebSocket by hand and return it with the handshake response head.
async fn open_socket(
    addr: SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
) -> (tokio::net::TcpStream, String) {
    let key = BASE64_STANDARD.encode(b"create-attach-key");
    let mut request = format!(
        "GET {path} HTTP/1.1\r\nHost: data-plane.test\r\nConnection: Upgrade\r\n\
         Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {key}\r\n"
    );
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    tcp.write_all(request.as_bytes()).await.unwrap();
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let mut byte = [0_u8; 1];
        tcp.read_exact(&mut byte).await.expect("handshake response");
        head.push(byte[0]);
    }
    (tcp, String::from_utf8_lossy(&head).into_owned())
}

/// Send one text frame. Clients must mask; an all-zero key leaves the
/// payload as is.
async fn send_text(tcp: &mut tokio::net::TcpStream, text: &str) {
    let payload = text.as_bytes();
    let mut frame = vec![0x81];
    if payload.len() < 126 {
        frame.push(0x80 | payload.len() as u8);
    } else {
        frame.push(0x80 | 126);
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    }
    frame.extend_from_slice(&[0, 0, 0, 0]);
    frame.extend_from_slice(payload);
    tcp.write_all(&frame).await.unwrap();
}

/// The next text frame as JSON, or `None` once the server closes.
async fn next_frame(tcp: &mut tokio::net::TcpStream) -> Option<serde_json::Value> {
    let read = async {
        let mut header = [0_u8; 2];
        tcp.read_exact(&mut header).await.ok()?;
        let len = match header[1] & 0x7f {
            126 => {
                let mut extended = [0_u8; 2];
                tcp.read_exact(&mut extended).await.ok()?;
                usize::from(u16::from_be_bytes(extended))
            }
            127 => {
                let mut extended = [0_u8; 8];
                tcp.read_exact(&mut extended).await.ok()?;
                u64::from_be_bytes(extended) as usize
            }
            len => usize::from(len),
        };
        let mut payload = vec![0_u8; len];
        tcp.read_exact(&mut payload).await.ok()?;
        (header[0] & 0x0f == 0x1).then(|| serde_json::from_slice(&payload).unwrap())
    };
    tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .expect("server frame")
}

/// Skip frames until one of type `kind`.
async fn frame_of_type(tcp: &mut tokio::net::TcpStream, kind: &str) -> serde_json::Value {
    loop {
        let frame = next_frame(tcp)
            .await
            .unwrap_or_else(|| panic!("socket closed before a {kind} frame"));
        if frame["type"] == kind {
            return frame;
        }
    }
}

fn create_frame(key: Option<&str>) -> String {
    serde_json::json!({
        "type": "create",
        "idempotency_key": key,
        "request": {"command": ["sleep", "60"], "pty": {"cols": 80, "rows": 24}},
    })
    .to_string()
}

const CREATE_PATH: &str = "/v1/sandboxes/sbx_test/process-sessions:attach";

#[tokio::test]
async fn one_socket_creates_attaches_and_carries_control_frames() {
    if !fake_backend_test_process() {
        return;
    }
    use prost::Message as _;
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let (api, api_calls) = spawn_fake_api(session_id).await;
    let (backend, backend_calls) = spawn_fake_backend(session_id).await;
    let addr = serve_data_plane(Some(&api), &backend).await;

    let (mut socket, head) = open_socket(
        addr,
        CREATE_PATH,
        &[
            ("Authorization", API_KEY),
            ("X-Space-Federated-Org-Id", "org-1"),
            ("X-Space-Federated-Service-Grant", "grant"),
            ("X-Request-Id", "req-1"),
            ("Cookie", "not-forwarded"),
        ],
    )
    .await;
    assert!(head.starts_with("HTTP/1.1 101"), "{head}");
    send_text(&mut socket, &create_frame(Some("key-1"))).await;

    let session = next_frame(&mut socket).await.expect("session frame");
    assert_eq!(session["type"], "session");
    assert_eq!(session["session"]["id"], session_id.to_string());
    assert_eq!(session["session"]["attach"]["token"], attach_token());
    let attached = next_frame(&mut socket).await.expect("attached frame");
    assert_eq!(attached["type"], "attached");
    assert_eq!(attached["session_id"], session_id.to_string());
    frame_of_type(&mut socket, "process_started").await;

    // The create is the API's own: the body as sent, the idempotency key as
    // its header, and the caller's credentials and trace, nothing else.
    let calls = api_calls.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].path, "/v1/sandboxes/sbx_test/process-sessions");
    assert_eq!(
        calls[0].body,
        serde_json::json!({"command": ["sleep", "60"], "pty": {"cols": 80, "rows": 24}})
    );
    let forwarded = &calls[0].headers;
    assert_eq!(forwarded["idempotency-key"], "key-1");
    assert_eq!(forwarded["authorization"], API_KEY);
    assert_eq!(forwarded["x-space-federated-org-id"], "org-1");
    assert_eq!(forwarded["x-space-federated-service-grant"], "grant");
    assert_eq!(forwarded["x-request-id"], "req-1");
    assert!(forwarded.get("cookie").is_none());

    send_text(
        &mut socket,
        r#"{"type":"input","seq":1,"data_base64":"aGk="}"#,
    )
    .await;
    assert_eq!(frame_of_type(&mut socket, "input_accepted").await["seq"], 1);
    send_text(&mut socket, r#"{"type":"resize","cols":120,"rows":40}"#).await;
    send_text(&mut socket, r#"{"type":"signal","seq":2,"signal":2}"#).await;
    assert_eq!(
        frame_of_type(&mut socket, "control_accepted").await["seq"],
        2
    );

    let calls = backend_calls.lock().unwrap().clone();
    let request = |method: &str| {
        calls
            .iter()
            .find(|(name, _)| name == method)
            .unwrap_or_else(|| panic!("no {method} call in {calls:?}"))
            .1
            .clone()
    };
    let input = SpacedSendInputRequest::decode(request("SpacedSendInput").as_slice()).unwrap();
    assert_eq!(input.operation.unwrap().data, b"hi");
    let resize = SpacedResizePtyRequest::decode(request("SpacedResizePty").as_slice()).unwrap();
    assert_eq!(
        resize.operation.unwrap().size,
        Some(PtySize {
            rows: 40,
            cols: 120
        })
    );
    let signal = SpacedSendSignalRequest::decode(request("SpacedSendSignal").as_slice()).unwrap();
    assert_eq!(signal.operation.unwrap().signal, 2);
}

#[tokio::test]
async fn returned_token_reattaches_through_the_attach_url() {
    if !fake_backend_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let (api, _api_calls) = spawn_fake_api(session_id).await;
    let (backend, _backend_calls) = spawn_fake_backend(session_id).await;
    let addr = serve_data_plane(Some(&api), &backend).await;

    let (mut socket, _) = open_socket(addr, CREATE_PATH, &[("Authorization", API_KEY)]).await;
    send_text(&mut socket, &create_frame(Some("key-1"))).await;
    let session = frame_of_type(&mut socket, "session").await;
    frame_of_type(&mut socket, "process_started").await;
    drop(socket);

    let token = session["session"]["attach"]["token"].as_str().unwrap();
    let path = session["session"]["attach"]["url"].as_str().unwrap();
    let bearer = format!("Bearer {token}");
    // The first socket's attachment is released once its bridge notices the
    // close, so a reattach can briefly find it still held.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let (mut socket, head) = open_socket(addr, path, &[("Authorization", &bearer)]).await;
        assert!(head.starts_with("HTTP/1.1 101"), "{head}");
        let frame = next_frame(&mut socket).await.expect("attach answer");
        if frame["type"] == "attached" {
            assert_eq!(frame["session_id"], session_id.to_string());
            break;
        }
        assert_eq!(frame["code"], "session_already_attached", "{frame}");
        assert!(tokio::time::Instant::now() < deadline, "never reattached");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_retried_create_replays_the_same_session() {
    if !fake_backend_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let (api, api_calls) = spawn_fake_api(session_id).await;
    let (backend, _backend_calls) = spawn_fake_backend(session_id).await;
    let addr = serve_data_plane(Some(&api), &backend).await;

    let (mut first, _) = open_socket(addr, CREATE_PATH, &[("Authorization", API_KEY)]).await;
    send_text(&mut first, &create_frame(Some("same-key"))).await;
    let first_session = frame_of_type(&mut first, "session").await;
    frame_of_type(&mut first, "process_started").await;

    // A retry while the first socket is still attached gets the same session
    // back, and then the refusal `attachSession` gives a second attachment.
    let (mut retry, _) = open_socket(addr, CREATE_PATH, &[("Authorization", API_KEY)]).await;
    send_text(&mut retry, &create_frame(Some("same-key"))).await;
    let retry_session = next_frame(&mut retry).await.expect("session frame");
    assert_eq!(retry_session["type"], "session");
    assert_eq!(first_session["session"]["id"], session_id.to_string());
    assert_eq!(retry_session["session"], first_session["session"]);
    let refusal = next_frame(&mut retry).await.expect("attach refusal");
    assert_eq!(refusal["code"], "session_already_attached", "{refusal}");
    let keys = api_calls
        .calls
        .lock()
        .unwrap()
        .iter()
        .map(|call| call.headers["idempotency-key"].clone())
        .collect::<Vec<_>>();
    assert_eq!(keys, ["same-key", "same-key"]);
}

#[tokio::test]
async fn api_errors_are_relayed_and_nothing_is_attached() {
    if !fake_backend_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let (api, _api_calls) = spawn_fake_api(session_id).await;
    let (backend, backend_calls) = spawn_fake_backend(session_id).await;
    let addr = serve_data_plane(Some(&api), &backend).await;

    for (authorization, key, status, code) in [
        ("Bearer wrong-key", Some("key-1"), 401, "unauthorized"),
        (API_KEY, None, 400, "invalid_request"),
    ] {
        let (mut socket, _) =
            open_socket(addr, CREATE_PATH, &[("Authorization", authorization)]).await;
        send_text(&mut socket, &create_frame(key)).await;
        let frame = next_frame(&mut socket).await.expect("create_failed frame");
        assert_eq!(frame["type"], "create_failed", "{frame}");
        assert_eq!(frame["status"], status);
        assert_eq!(frame["error"]["error"]["code"], code);
        assert!(next_frame(&mut socket).await.is_none(), "socket closes");
    }
    assert!(
        backend_calls.lock().unwrap().is_empty(),
        "a failed create never resolves a route"
    );
}

#[tokio::test]
async fn an_unreachable_api_is_reported_as_retryable() {
    if !fake_backend_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let (backend, _backend_calls) = spawn_fake_backend(session_id).await;
    // A port nothing listens on.
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let addr = serve_data_plane(Some(&api), &backend).await;

    let (mut socket, _) = open_socket(addr, CREATE_PATH, &[("Authorization", API_KEY)]).await;
    send_text(&mut socket, &create_frame(Some("key-1"))).await;
    let frame = next_frame(&mut socket)
        .await
        .expect("create_unavailable frame");
    assert_eq!(frame["type"], "create_unavailable", "{frame}");
}

#[tokio::test]
async fn an_invalid_first_frame_is_refused_before_any_create() {
    if !fake_backend_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let (api, api_calls) = spawn_fake_api(session_id).await;
    let (backend, _backend_calls) = spawn_fake_backend(session_id).await;
    let addr = serve_data_plane(Some(&api), &backend).await;

    let (mut socket, _) = open_socket(addr, CREATE_PATH, &[("Authorization", API_KEY)]).await;
    send_text(
        &mut socket,
        &format!(r#"{{"type":"attach","token":"{}"}}"#, attach_token()),
    )
    .await;
    let frame = next_frame(&mut socket).await.expect("error frame");
    assert_eq!(frame["type"], "error");
    assert_eq!(frame["code"], "attach_frame_invalid");
    assert!(api_calls.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn without_an_api_url_the_endpoint_is_not_found() {
    if !fake_backend_test_process() {
        return;
    }
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let (backend, _backend_calls) = spawn_fake_backend(session_id).await;
    let addr = serve_data_plane(None, &backend).await;

    let (_socket, head) = open_socket(addr, CREATE_PATH, &[("Authorization", API_KEY)]).await;
    assert!(head.starts_with("HTTP/1.1 404"), "{head}");
}

#[test]
fn api_internal_url_must_be_a_plain_http_origin() {
    for origin in ["http://api:8080", "http://api:8080/", "http://10.0.0.1"] {
        assert!(ProcessSessionCreator::new(origin).is_ok(), "{origin}");
    }
    for origin in [
        "https://api:8080",
        "http://api:8080/prefix",
        "http://api:8080/?x=1",
        "api:8080",
        "",
    ] {
        assert!(ProcessSessionCreator::new(origin).is_err(), "{origin}");
    }
}

#[test]
fn sandbox_ids_cannot_change_the_api_path() {
    assert_eq!(encode_path_segment("sbx_AZ09-.~"), "sbx_AZ09-.~");
    assert_eq!(encode_path_segment("a/../b?c#d"), "a%2F..%2Fb%3Fc%23d");
    assert_eq!(encode_path_segment("é"), "%C3%A9");
}
