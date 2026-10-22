use super::*;
use crate::routes::AppState;
use axum::{
    Router,
    extract::{Path, State},
    http::{HeaderMap, Method},
    routing::get,
};
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDbResult, pg_test_db};
use pplx_space_public_api::{DATA_PLANE_SESSION_EVENTS_PATH_TEMPLATE, PublicSandboxId};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
use uuid::Uuid;

const KEY: &str = "command-stream-test-key";
const DATA_PLANE_EVENTS: &str = "event: attached\ndata: {\"type\":\"attached\"}\n\n\
id: 1\nevent: stdout\ndata: {\"type\":\"stdout\",\"data\":\"hello\\n\"}\n\n\
id: 2\nevent: process_exited\ndata: {\"type\":\"process_exited\",\"exit_code\":0}\n\n";

#[derive(Clone, Copy)]
enum DataPlaneMode {
    Stream,
    Reject,
    Hold,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SeenAttach {
    session_id: String,
    authorization: Option<String>,
    accept: Option<String>,
}

#[derive(Clone)]
struct FakeDataPlane {
    mode: DataPlaneMode,
    seen: Arc<Mutex<Vec<SeenAttach>>>,
}

async fn fake_events(
    State(fake): State<FakeDataPlane>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let header = |name| {
        headers
            .get(name)
            .and_then(|value: &HeaderValue| value.to_str().ok())
            .map(str::to_owned)
    };
    fake.seen.lock().unwrap().push(SeenAttach {
        session_id,
        authorization: header(header::AUTHORIZATION),
        accept: header(header::ACCEPT),
    });
    let sse = |body: Body| {
        Response::builder()
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(body)
            .unwrap()
    };
    match fake.mode {
        DataPlaneMode::Stream => sse(Body::from(DATA_PLANE_EVENTS)),
        DataPlaneMode::Reject => Response::builder()
            .status(StatusCode::CONFLICT)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"code":"conflict"}"#))
            .unwrap(),
        DataPlaneMode::Hold => sse(Body::from_stream(
            futures_util::stream::once(async {
                Ok::<_, std::io::Error>(Bytes::from_static(b"event: attached\ndata: {}\n\n"))
            })
            .chain(futures_util::stream::pending()),
        )),
    }
}

async fn spawn_data_plane(mode: DataPlaneMode) -> (String, Arc<Mutex<Vec<SeenAttach>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route(DATA_PLANE_SESSION_EVENTS_PATH_TEMPLATE, get(fake_events))
        .with_state(FakeDataPlane {
            mode,
            seen: seen.clone(),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (origin, seen)
}

struct Fixture {
    _db: pplx_space_db_test_fixtures::TestDb,
    app: Router,
    commands_path: String,
}

async fn fixture(proxy: Option<CommandStreamProxy>) -> TestDbResult<Fixture> {
    let db = pg_test_db().await?;
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
         VALUES ($1, $2, 'command-stream-test', $3, ARRAY['native'])"
    ))
    .bind(Uuid::now_v7())
    .bind(SEED_ORG_ID)
    .bind(crate::auth::hash_token(KEY))
    .execute(&db.pool)
    .await?;
    let sandbox = PublicSandboxId::from_uuid(Uuid::now_v7());
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, network_policy_override_json,
             network_policy_generation)
         VALUES ($1, $2, $3, 'started', $4, 1)"
    ))
    .bind(sandbox.as_uuid())
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(sqlx::types::Json(
        pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(&db.pool)
    .await?;
    let mut state = AppState::from_pool(db.pool.clone()).await?;
    state.command_stream = proxy;
    let app = Router::new()
        .nest("/v1", crate::routes::public::v1_router(state.clone()))
        .with_state(state);
    Ok(Fixture {
        _db: db,
        app,
        commands_path: format!("/v1/sandboxes/{sandbox}/commands"),
    })
}

struct Answer {
    status: StatusCode,
    content_type: Option<String>,
    body: String,
}

async fn post_command(
    fixture: &Fixture,
    idempotency_key: Option<&str>,
    accept: Option<&str>,
) -> TestDbResult<Answer> {
    post_command_body(
        fixture,
        idempotency_key,
        accept,
        &json!({"command": ["echo", "hello"]}),
    )
    .await
}

async fn post_command_body(
    fixture: &Fixture,
    idempotency_key: Option<&str>,
    accept: Option<&str>,
    body: &Value,
) -> TestDbResult<Answer> {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(&fixture.commands_path)
        .header(header::AUTHORIZATION, format!("Bearer {KEY}"))
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(key) = idempotency_key {
        request = request.header("idempotency-key", key);
    }
    if let Some(accept) = accept {
        request = request.header(header::ACCEPT, accept);
    }
    let body = serde_json::to_vec(body)?;
    let response = fixture
        .app
        .clone()
        .oneshot(request.body(Body::from(body))?)
        .await?;
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
    Ok(Answer {
        status,
        content_type,
        body: String::from_utf8(body.to_vec())?,
    })
}

/// Splits the relayed body into the leading `command` handle and the rest.
fn split_command_event(body: &str) -> (Value, &str) {
    let (first, rest) = body.split_once("\n\n").expect("stream has a first event");
    let data = first
        .strip_prefix(&format!("event: {COMMAND_EVENT}\ndata: "))
        .unwrap_or_else(|| panic!("first event is not the command handle: {first:?}"));
    let frame: Value = serde_json::from_str(data).unwrap();
    assert_eq!(frame["type"], COMMAND_EVENT, "{frame}");
    (frame["command"].clone(), rest)
}

fn proxy(origin: &str) -> CommandStreamProxy {
    CommandStreamProxy::new(origin, CancellationToken::new()).unwrap()
}

#[test]
fn internal_origin_must_be_plain_http_origin() {
    for valid in ["http://data-plane:8090", "http://127.0.0.1:8090/"] {
        let proxy = CommandStreamProxy::new(valid, CancellationToken::new()).unwrap();
        assert!(!proxy.origin.ends_with('/'), "{}", proxy.origin);
    }
    for invalid in [
        "https://data-plane:8090",
        "data-plane:8090",
        "http://data-plane:8090/v1",
        "http://data-plane:8090?x=1",
        "not a url",
    ] {
        assert!(
            CommandStreamProxy::new(invalid, CancellationToken::new()).is_err(),
            "{invalid}"
        );
    }
}

#[tokio::test]
async fn one_request_creates_the_command_and_relays_its_events() -> TestDbResult<()> {
    let (origin, seen) = spawn_data_plane(DataPlaneMode::Stream).await;
    let fixture = fixture(Some(proxy(&origin))).await?;

    let streamed = post_command(&fixture, Some("stream-key"), Some("text/event-stream")).await?;
    assert_eq!(streamed.status, StatusCode::CREATED, "{}", streamed.body);
    assert_eq!(streamed.content_type.as_deref(), Some("text/event-stream"));
    let (command, events) = split_command_event(&streamed.body);
    assert_eq!(events, DATA_PLANE_EVENTS);
    let session_id = command["session_id"].as_str().unwrap();
    let token = command["attach"]["token"].as_str().unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        vec![SeenAttach {
            session_id: session_id.to_owned(),
            authorization: Some(format!("Bearer {token}")),
            accept: Some("text/event-stream".to_owned()),
        }]
    );

    // A retry with the same key replays the same session and attaches to it
    // again, as a two-request client retrying its attach would.
    let replay = post_command(&fixture, Some("stream-key"), Some("text/event-stream")).await?;
    let (replayed, _) = split_command_event(&replay.body);
    assert_eq!(replayed["session_id"], command["session_id"]);
    assert_eq!(seen.lock().unwrap().len(), 2);

    // The mode is not part of the request fingerprint: a JSON retry of the
    // same key gets the same handle instead of an idempotency conflict.
    let json_replay = post_command(&fixture, Some("stream-key"), None).await?;
    assert_eq!(json_replay.status, StatusCode::CREATED);
    let json_replay: Value = serde_json::from_str(&json_replay.body)?;
    assert_eq!(json_replay["session_id"], command["session_id"]);
    assert_eq!(seen.lock().unwrap().len(), 2);
    Ok(())
}

#[tokio::test]
async fn streaming_saves_one_client_round_trip_over_create_then_attach() -> TestDbResult<()> {
    let (origin, _) = spawn_data_plane(DataPlaneMode::Stream).await;
    let fixture = fixture(Some(proxy(&origin))).await?;
    let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .build::<_, Empty<Bytes>>(HttpConnector::new());

    // Two-request flow: create, then attach to the returned events URL.
    let mut two_step_requests = 0;
    let created = post_command(&fixture, Some("two-step"), None).await?;
    two_step_requests += 1;
    let created: Value = serde_json::from_str(&created.body)?;
    let session_id: pplx_space_public_api::DataPlaneSessionId =
        created["session_id"].as_str().unwrap().parse()?;
    let attach = client
        .request(
            Request::get(format!(
                "{origin}{}",
                data_plane_session_events_path(session_id)
            ))
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", created["attach"]["token"].as_str().unwrap()),
            )
            .body(Empty::new())?,
        )
        .await?;
    two_step_requests += 1;
    let two_step_events = attach.into_body().collect().await?.to_bytes();

    let mut one_step_requests = 0;
    let streamed = post_command(&fixture, Some("one-step"), Some("text/event-stream")).await?;
    one_step_requests += 1;
    let (_, one_step_events) = split_command_event(&streamed.body);

    assert_eq!(one_step_events.as_bytes(), two_step_events.as_ref());
    assert_eq!((two_step_requests, one_step_requests), (2, 1));
    Ok(())
}

#[tokio::test]
async fn json_handle_is_returned_when_the_stream_is_not_requested_or_unavailable()
-> TestDbResult<()> {
    let (origin, seen) = spawn_data_plane(DataPlaneMode::Reject).await;
    let api = fixture(Some(proxy(&origin))).await?;

    let plain = post_command(&api, Some("plain"), None).await?;
    assert_eq!(plain.status, StatusCode::CREATED);
    assert_eq!(plain.content_type.as_deref(), Some("application/json"));
    assert!(seen.lock().unwrap().is_empty());

    let rejected = post_command(&api, Some("rejected"), Some("text/event-stream")).await?;
    assert_eq!(rejected.status, StatusCode::CREATED);
    assert_eq!(rejected.content_type.as_deref(), Some("application/json"));
    let command: Value = serde_json::from_str(&rejected.body)?;
    assert!(command["attach"]["events_url"].as_str().is_some());
    assert_eq!(seen.lock().unwrap().len(), 1);

    // Nothing listens on a just-released port.
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let closed_origin = format!("http://{}", closed.local_addr()?);
    drop(closed);
    for proxy in [None, Some(proxy(&closed_origin))] {
        let fixture = fixture(proxy).await?;
        let answer = post_command(&fixture, Some("fallback"), Some("text/event-stream")).await?;
        assert_eq!(answer.status, StatusCode::CREATED, "{}", answer.body);
        assert_eq!(answer.content_type.as_deref(), Some("application/json"));
    }
    Ok(())
}

#[tokio::test]
async fn create_errors_are_unchanged_and_never_open_a_stream() -> TestDbResult<()> {
    let (origin, seen) = spawn_data_plane(DataPlaneMode::Stream).await;
    let fixture = fixture(Some(proxy(&origin))).await?;

    let missing_key = post_command(&fixture, None, Some("text/event-stream")).await?;
    assert_eq!(missing_key.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        missing_key.content_type.as_deref(),
        Some("application/json")
    );
    let envelope: Value = serde_json::from_str(&missing_key.body)?;
    assert_eq!(envelope["code"], "invalid_request", "{envelope}");

    let other_sandbox = Fixture {
        commands_path: format!(
            "/v1/sandboxes/{}/commands",
            PublicSandboxId::from_uuid(Uuid::now_v7())
        ),
        ..fixture
    };
    let not_found =
        post_command(&other_sandbox, Some("missing"), Some("text/event-stream")).await?;
    assert_eq!(not_found.status, StatusCode::NOT_FOUND);
    assert_eq!(not_found.content_type.as_deref(), Some("application/json"));

    assert!(seen.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn streamed_commands_with_a_pty_are_refused_before_the_create() -> TestDbResult<()> {
    let (origin, seen) = spawn_data_plane(DataPlaneMode::Stream).await;
    let fixture = fixture(Some(proxy(&origin))).await?;
    let pty = json!({"rows": 24, "cols": 80});
    for (key, body) in [
        ("pty", json!({"command": ["sh"], "pty": pty})),
        (
            "stdin-and-pty",
            json!({"command": ["sh"], "stdin": true, "pty": pty}),
        ),
    ] {
        let refused =
            post_command_body(&fixture, Some(key), Some("text/event-stream"), &body).await?;
        assert_eq!(refused.status, StatusCode::BAD_REQUEST, "{}", refused.body);
        let envelope: Value = serde_json::from_str(&refused.body)?;
        assert_eq!(envelope["code"], "invalid_request", "{envelope}");
        assert_eq!(envelope["retryable"], false, "{envelope}");
        assert_eq!(
            envelope["details"],
            json!([{"key": "field", "value": "pty"}]),
            "{envelope}"
        );

        // Nothing was stored under the key: a different body reusing it
        // creates a command instead of answering idempotency_conflict.
        let reused = post_command(&fixture, Some(key), None).await?;
        assert_eq!(reused.status, StatusCode::CREATED, "{}", reused.body);
        // A JSON create keeps accepting a pty, for callers that attach over
        // WebSocket.
        let json_create =
            post_command_body(&fixture, Some(&format!("{key}-json")), None, &body).await?;
        assert_eq!(
            json_create.status,
            StatusCode::CREATED,
            "{}",
            json_create.body
        );
    }

    // A zero-weight event stream is a JSON create, which keeps accepting a pty.
    let declined = post_command_body(
        &fixture,
        Some("declined-stream"),
        Some("text/event-stream;q=0, application/json"),
        &json!({"command": ["sh"], "pty": pty}),
    )
    .await?;
    assert_eq!(declined.status, StatusCode::CREATED, "{}", declined.body);
    assert_eq!(declined.content_type.as_deref(), Some("application/json"));
    assert!(seen.lock().unwrap().is_empty());

    // Stdin alone streams: the events stream starts it at EOF.
    for (key, stdin) in [("stdin", true), ("no-stdin", false)] {
        let streamed = post_command_body(
            &fixture,
            Some(key),
            Some("text/event-stream"),
            &json!({"command": ["cat"], "stdin": stdin}),
        )
        .await?;
        assert_eq!(streamed.status, StatusCode::CREATED, "{}", streamed.body);
        assert_eq!(streamed.content_type.as_deref(), Some("text/event-stream"));
    }
    Ok(())
}

#[tokio::test]
async fn shutdown_ends_relayed_streams() -> TestDbResult<()> {
    let (origin, _) = spawn_data_plane(DataPlaneMode::Hold).await;
    let shutdown = CancellationToken::new();
    let fixture = fixture(Some(CommandStreamProxy::new(&origin, shutdown.clone())?)).await?;

    let answer = tokio::spawn(async move {
        post_command(&fixture, Some("held"), Some("text/event-stream"))
            .await
            .map(|answer| answer.body)
            .map_err(|error| error.to_string())
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!answer.is_finished(), "held stream ended on its own");
    shutdown.cancel();
    let body = tokio::time::timeout(Duration::from_secs(5), answer)
        .await
        .expect("stream ends on shutdown")
        .unwrap()
        .unwrap();
    let (_, rest) = split_command_event(&body);
    assert_eq!(rest, "event: attached\ndata: {}\n\n");
    Ok(())
}
