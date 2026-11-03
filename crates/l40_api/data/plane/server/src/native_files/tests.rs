//! The files routes against a scripted lifecycle API and sandbox runtime.
//! Every expected answer is the one the API's own route gives; the rows of
//! [`each_failure_answers_as_the_api_answers_it`] say where it comes from.

mod drain;
mod fakes;
mod mutations;
mod write;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, Request, StatusCode, header},
    response::Response,
};
use fakes::{Answer, FakeApi, FakeWorker, ROUTE_TOKEN, Reply, WorkerRequest};
use http_body_util::BodyExt as _;
use pplx_space_node_api_client::proto::{
    NativeFileOperation, NativeHttpHeader, NativeHttpResponse,
};
use pplx_space_public_api::{PublicErrorEnvelope, PublicSandboxId};
use pplx_space_sbx_mgr_client::operations::{FileChunk, FileInfo, StatResponse};
use serde_json::{Value, json};
use tonic::Status;

use super::{NativeFileAccessClient, NativeFiles};
use crate::DataPlaneState;

const CLIENT_KEY: &str = "Bearer client-key";

/// The fakes listen on plain TCP, and the data plane authenticates its calls
/// with the static internal token.
fn test_process() -> bool {
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

struct Harness {
    state: DataPlaneState,
    api: FakeApi,
    worker: FakeWorker,
}

impl Harness {
    async fn start() -> Self {
        Self::start_with(None).await
    }

    /// With the data plane's Native upload admission set to
    /// `upload_admission`.
    async fn start_with(upload_admission: Option<crate::NativeUploadAdmissionConfig>) -> Self {
        let (worker, worker_addr) = FakeWorker::start().await;
        let (api, api_addr) = FakeApi::start(worker_addr).await;
        let mut state = crate::tests::test_data_plane_state().await;
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{api_addr}"))
            .unwrap()
            .connect_lazy();
        state.native_files = Some(Arc::new(NativeFiles::new(
            NativeFileAccessClient::new(channel),
            upload_admission.as_ref(),
        )));
        Self { state, api, worker }
    }

    fn path(&self, action: &str) -> String {
        format!(
            "/v1/sandboxes/{}/files:{action}",
            PublicSandboxId::from(self.api.sandbox_id)
        )
    }

    fn post(&self, action: &str, body: Value) -> Request<Body> {
        Request::post(self.path(action))
            .header(header::AUTHORIZATION, CLIENT_KEY)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    /// A POST under `Idempotency-Key: key`.
    fn post_keyed(&self, action: &str, key: &str, body: Value) -> Request<Body> {
        self.post_keyed_json(action, key, body.to_string())
    }

    /// A POST under `Idempotency-Key: key` of JSON already serialized.
    fn post_keyed_json(&self, action: &str, key: &str, json: String) -> Request<Body> {
        Request::post(self.path(action))
            .header(header::AUTHORIZATION, CLIENT_KEY)
            .header(header::CONTENT_TYPE, "application/json")
            .header("idempotency-key", key)
            .body(Body::from(json))
            .unwrap()
    }

    async fn send(&self, request: Request<Body>) -> Response {
        tower::ServiceExt::oneshot(crate::router(self.state.clone()), request)
            .await
            .unwrap()
    }
}

async fn read_body(response: Response) -> (StatusCode, HeaderMap, Bytes) {
    let (parts, body) = response.into_parts();
    (
        parts.status,
        parts.headers,
        body.collect().await.unwrap().to_bytes(),
    )
}

fn file_info(path: &str, size_bytes: u64) -> FileInfo {
    FileInfo {
        path: path.to_owned(),
        file_type: pplx_space_sbx_mgr_client::proto::SpacedFileType::File as i32,
        size_bytes,
        mode: 0o644,
        ..FileInfo::default()
    }
}

/// The answer the API gives for `info`: its public shape, as JSON.
fn public_entry(info: FileInfo) -> Value {
    serde_json::to_value(pplx_space_native_files::public_file_info(info)).unwrap()
}

#[tokio::test]
async fn reads_answer_through_the_granted_route_in_the_apis_shapes() {
    if !test_process() {
        return;
    }
    let harness = Harness::start().await;
    let a = file_info("/workspace/a.txt", 5);
    let b = file_info("/workspace/b.txt", 7);
    harness.worker.reply(Reply::Stat(Ok(StatResponse {
        info: Some(a.clone()),
    })));
    harness
        .worker
        .reply(Reply::List(Ok(vec![Ok(a.clone()), Ok(b.clone())])));
    harness.worker.reply(Reply::Read(Ok(vec![
        Ok(FileChunk {
            data: Bytes::from_static(b"hel"),
        }),
        Ok(FileChunk {
            data: Bytes::from_static(b"lo"),
        }),
    ])));

    let (status, headers, body) = read_body(
        harness
            .send(harness.post(
                "stat",
                json!({"path": "/workspace/a.txt", "user": "sandbox"}),
            ))
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({ "entry": public_entry(a.clone()) })
    );
    let stat_request_id = headers
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    let (status, _, body) = read_body(
        harness
            .send(harness.post("list", json!({"path": "/workspace", "depth": 2})))
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({ "entries": [public_entry(a), public_entry(b)] })
    );

    let (status, headers, body) = read_body(
        harness
            .send(harness.post(
                "read",
                json!({"path": "/workspace/a.txt", "offset": 1, "length": 4}),
            ))
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CONTENT_TYPE], "application/octet-stream");
    assert_eq!(body.as_ref(), b"hello");

    // Each call went to the granted runtime, for the sandbox the grant
    // names, with its route token.
    let calls = harness.worker.calls();
    assert_eq!(calls.len(), 3);
    for call in &calls {
        assert_eq!(call.route_token.as_deref(), Some(ROUTE_TOKEN));
    }
    let sandbox_id = harness.api.sandbox_id.to_string();
    match &calls[0].request {
        WorkerRequest::Stat(request) => {
            assert_eq!(request.sandbox_id, sandbox_id);
            let operation = request.operation.as_ref().unwrap();
            assert_eq!(operation.path, "/workspace/a.txt");
            assert_eq!(operation.user.as_deref(), Some("sandbox"));
        }
        other => panic!("expected a stat, got {other:?}"),
    }
    match &calls[1].request {
        WorkerRequest::List(request) => {
            assert_eq!(request.sandbox_id, sandbox_id);
            assert_eq!(request.operation.as_ref().unwrap().depth, Some(2));
        }
        other => panic!("expected a list, got {other:?}"),
    }
    match &calls[2].request {
        WorkerRequest::Read(request) => {
            assert_eq!(request.sandbox_id, sandbox_id);
            let operation = request.operation.as_ref().unwrap();
            assert_eq!((operation.offset, operation.length), (1, Some(4)));
        }
        other => panic!("expected a read, got {other:?}"),
    }

    // The API was asked to authorize, then to prepare, each request: about
    // the operation, the path's sandbox segment and the client's headers,
    // with the request id the client is answered with.
    let access = harness.api.calls();
    let asked: Vec<_> = access
        .iter()
        .map(|call| (call.rpc, call.request.operation))
        .collect();
    let (stat, list, read) = (
        NativeFileOperation::Stat as i32,
        NativeFileOperation::List as i32,
        NativeFileOperation::Read as i32,
    );
    assert_eq!(
        asked,
        [
            ("authorize", stat),
            ("prepare", stat),
            ("authorize", list),
            ("prepare", list),
            ("authorize", read),
            ("prepare", read),
        ]
    );
    for call in &access {
        assert_eq!(
            call.request.sandbox_id,
            PublicSandboxId::from(harness.api.sandbox_id).to_string()
        );
        assert_eq!(call.header("authorization"), Some(CLIENT_KEY.as_bytes()));
        assert_eq!(call.bearer.as_deref(), Some("Bearer test-token"));
        assert_eq!(call.fingerprint, None);
    }
    assert_eq!(access[0].request.request_id, stat_request_id);
    assert_eq!(access[1].request.request_id, stat_request_id);
}

/// A body that records whether anything read it.
fn watched_body(read: &Arc<AtomicBool>) -> Body {
    let read = Arc::clone(read);
    Body::from_stream(futures_util::stream::once(async move {
        read.store(true, Ordering::SeqCst);
        Ok::<_, std::convert::Infallible>(Bytes::from_static(b"{\"path\": \"/a\"}"))
    }))
}

fn rendered(status: u16, headers: &[(&str, &str)], body: &[u8]) -> NativeHttpResponse {
    NativeHttpResponse {
        status: status.into(),
        headers: headers
            .iter()
            .map(|(name, value)| NativeHttpHeader {
                name: (*name).to_owned(),
                value: value.as_bytes().to_vec(),
            })
            .collect(),
        body: body.to_vec(),
    }
}

#[tokio::test]
async fn a_refused_request_is_answered_as_the_api_rendered_it() {
    if !test_process() {
        return;
    }
    let harness = Harness::start().await;
    let unauthorized = br#"{"code":"unauthorized","message":"invalid API key","request_id":"req_1","retryable":false,"details":[]}"#;
    harness.api.authorize(Answer::Refuse(rendered(
        401,
        &[
            ("content-type", "application/json"),
            ("content-length", "9999"),
            ("x-request-id", "req_1"),
            ("www-authenticate", "Bearer"),
        ],
        unauthorized,
    )));
    let read = Arc::new(AtomicBool::new(false));
    let request = Request::post(harness.path("stat"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-request-id", "req_1")
        .body(watched_body(&read))
        .unwrap();
    let (status, headers, body) = read_body(harness.send(request).await).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body.as_ref(), unauthorized);
    assert_eq!(headers[header::WWW_AUTHENTICATE], "Bearer");
    assert_eq!(headers[header::CONTENT_TYPE], "application/json");
    // The client's request id, which the API was asked with and rendered.
    assert_eq!(headers["x-request-id"], "req_1");
    assert_eq!(harness.api.calls()[0].request.request_id, "req_1");
    // Refused from its headers alone: its body was never read, and nothing
    // was prepared or sent to the sandbox.
    assert!(!read.load(Ordering::SeqCst));
    assert_eq!(harness.api.calls().len(), 1);
    assert!(harness.worker.calls().is_empty());

    // The API refuses a request before the route says it takes only POST,
    // so a wrong method without credentials is still a 401.
    harness.api.authorize(Answer::Refuse(rendered(
        401,
        &[("content-type", "application/json")],
        unauthorized,
    )));
    let request = Request::get(harness.path("stat"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        harness.send(request).await.status(),
        StatusCode::UNAUTHORIZED
    );

    // Preparation's refusal, after the body was checked, is answered the
    // same way, and the sandbox is not called.
    let conflict = br#"{"code":"conflict","message":"sandbox is suspended, not running","request_id":"req_2","retryable":false,"details":[]}"#;
    harness.api.prepare(Answer::Refuse(rendered(
        409,
        &[("content-type", "application/json"), ("retry-after", "3")],
        conflict,
    )));
    let (status, headers, body) = read_body(
        harness
            .send(harness.post("stat", json!({"path": "/workspace"})))
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body.as_ref(), conflict);
    assert_eq!(headers[header::RETRY_AFTER], "3");
    assert!(harness.worker.calls().is_empty());
}

#[tokio::test]
async fn the_routes_are_mounted_only_when_served() {
    if !test_process() {
        return;
    }
    let mut harness = Harness::start().await;
    let served = harness
        .send(harness.post("stat", json!({"path": "/workspace"})))
        .await;
    assert!(served.headers().contains_key("x-request-id"));
    assert_eq!(harness.api.calls().len(), 2);

    harness.state.native_files = None;
    let unmounted = harness
        .send(harness.post("stat", json!({"path": "/workspace"})))
        .await;
    // The request reaches the data plane's other routes, which answer it
    // as no Native route.
    assert!(!unmounted.headers().contains_key("x-request-id"));
    assert_eq!(harness.api.calls().len(), 2);
}

/// What the API's route answers for one failure.
struct Expected {
    status: u16,
    code: &'static str,
    message: &'static str,
    retryable: bool,
    retry_after: Option<&'static str>,
    details: Value,
}

const fn invalid(status: u16, message: &'static str) -> Expected {
    Expected {
        status,
        code: "invalid_request",
        message,
        retryable: false,
        retry_after: None,
        details: Value::Null,
    }
}

const fn answered(
    status: u16,
    code: &'static str,
    message: &'static str,
    retryable: bool,
) -> Expected {
    Expected {
        status,
        code,
        message,
        retryable,
        retry_after: None,
        details: Value::Null,
    }
}

/// One failure the API's route answers, and the request that meets it.
struct Row {
    name: &'static str,
    request: fn(&Harness) -> Request<Body>,
    reply: Option<fn() -> Reply>,
    expected: Expected,
    allow: Option<&'static str>,
}

/// The rows' worker failures. Statuses are not `Clone` with their metadata
/// kept, so each is built when its row is sent.
fn failed_stat(status: Status) -> Reply {
    Reply::Stat(Err(status))
}

fn guest_missing() -> Reply {
    failed_stat(pplx_space_common_grpc::mark_guest_path_not_found(
        Status::not_found("no such file or directory"),
    ))
}

fn storage_full() -> Reply {
    let mut status = Status::resource_exhausted("No space left on device (os error 28)");
    status.metadata_mut().insert(
        pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY,
        tonic::metadata::MetadataValue::from_static(
            pplx_space_common_core::spaced::GUEST_STORAGE_FULL,
        ),
    );
    failed_stat(status)
}

fn refused(code: tonic::Code) -> Reply {
    failed_stat(pplx_space_common_grpc::refused_before_start(
        Status::new(code, "too many concurrent filesystem file operations"),
        std::time::Duration::from_millis(1500),
    ))
}

fn stat(harness: &Harness) -> Request<Body> {
    harness.post("stat", json!({"path": "/workspace/a.txt"}))
}

/// Each class of failure the API's files routes answer, with the API's answer
/// for it: the request checks are `files.rs`'s, the router's rejections are
/// normalized by `normalize_public_error_response`, and the sandbox failures
/// are `files/tests.rs`'s mapping (`controller_error` over
/// `ControllerError::from(Status)`).
#[tokio::test]
async fn each_failure_answers_as_the_api_answers_it() {
    if !test_process() {
        return;
    }
    let rows = [
        Row {
            name: "body is not JSON",
            request: |harness| {
                Request::post(harness.path("stat"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{"))
                    .unwrap()
            },
            reply: None,
            expected: invalid(400, "invalid request"),
            allow: None,
        },
        Row {
            name: "body says it is not JSON",
            request: |harness| {
                Request::post(harness.path("stat"))
                    .body(Body::from(r#"{"path": "/a"}"#))
                    .unwrap()
            },
            reply: None,
            expected: invalid(415, "invalid request"),
            allow: None,
        },
        Row {
            name: "body misses a field",
            request: |harness| harness.post("stat", json!({})),
            reply: None,
            expected: invalid(422, "invalid request"),
            allow: None,
        },
        Row {
            name: "body over the 2 MiB default",
            request: |harness| harness.post("stat", json!({ "path": "a".repeat(2 * 1024 * 1024) })),
            reply: None,
            expected: invalid(413, "invalid request"),
            allow: None,
        },
        Row {
            name: "another method",
            request: |harness| {
                Request::get(harness.path("stat"))
                    .body(Body::empty())
                    .unwrap()
            },
            reply: None,
            expected: invalid(405, "invalid request"),
            allow: Some("POST"),
        },
        Row {
            name: "path with a control character",
            request: |harness| harness.post("stat", json!({"path": "/tmp/\nfile"})),
            reply: None,
            expected: invalid(400, "guest path must be bounded and control-free"),
            allow: None,
        },
        Row {
            name: "list depth 0",
            request: |harness| harness.post("list", json!({"path": "/", "depth": 0})),
            reply: None,
            expected: invalid(400, "depth must be at least 1"),
            allow: None,
        },
        Row {
            name: "read of no bytes",
            request: |harness| harness.post("read", json!({"path": "/a", "length": 0})),
            reply: None,
            expected: invalid(400, "file read length must be at least 1 byte"),
            allow: None,
        },
        Row {
            name: "read above 16 MiB",
            request: |harness| {
                harness.post(
                    "read",
                    json!({"path": "/a", "length": 16 * 1024 * 1024 + 1}),
                )
            },
            reply: None,
            expected: invalid(400, "file read length exceeds the control-plane API limit"),
            allow: None,
        },
        Row {
            name: "guest reports the path missing",
            request: stat,
            reply: Some(guest_missing),
            expected: Expected {
                details: json!([{"key": "resource", "value": "path"}]),
                ..answered(404, "not_found", "no such file or directory", false)
            },
            allow: None,
        },
        Row {
            name: "worker not found",
            request: stat,
            reply: Some(|| failed_stat(Status::not_found("sandbox not on this node"))),
            expected: answered(404, "not_found", "sandbox not on this node", false),
            allow: None,
        },
        Row {
            name: "guest storage full",
            request: stat,
            reply: Some(storage_full),
            expected: answered(
                412,
                "failed_precondition",
                "sandbox storage is full: No space left on device (os error 28)",
                false,
            ),
            allow: None,
        },
        Row {
            name: "refused before start, over capacity",
            request: stat,
            reply: Some(|| refused(tonic::Code::ResourceExhausted)),
            expected: Expected {
                retry_after: Some("2"),
                ..answered(
                    429,
                    "rate_limited",
                    "too many concurrent filesystem file operations",
                    true,
                )
            },
            allow: None,
        },
        Row {
            name: "refused before start, unavailable",
            request: stat,
            reply: Some(|| refused(tonic::Code::Unavailable)),
            expected: Expected {
                retry_after: Some("2"),
                ..answered(
                    503,
                    "service_unavailable",
                    "too many concurrent filesystem file operations",
                    true,
                )
            },
            allow: None,
        },
        Row {
            name: "worker unavailable",
            request: stat,
            reply: Some(|| failed_stat(Status::unavailable("worker restarting"))),
            expected: answered(503, "service_unavailable", "worker restarting", true),
            allow: None,
        },
        Row {
            name: "worker deadline",
            request: stat,
            reply: Some(|| failed_stat(Status::deadline_exceeded("request deadline exceeded"))),
            expected: answered(
                503,
                "service_unavailable",
                "sandbox did not answer in time",
                true,
            ),
            allow: None,
        },
        Row {
            name: "worker cancelled",
            request: stat,
            reply: Some(|| failed_stat(Status::cancelled("cancelled"))),
            expected: answered(
                503,
                "service_unavailable",
                "sandbox did not answer in time",
                true,
            ),
            allow: None,
        },
        Row {
            name: "worker internal",
            request: stat,
            reply: Some(|| failed_stat(Status::internal("guest failed"))),
            expected: answered(500, "internal", "internal server error", true),
            allow: None,
        },
        Row {
            name: "worker unimplemented",
            request: stat,
            reply: Some(|| failed_stat(Status::unimplemented("old guest"))),
            expected: answered(500, "internal", "internal server error", true),
            allow: None,
        },
        Row {
            name: "worker unauthenticated",
            request: stat,
            reply: Some(|| failed_stat(Status::unauthenticated("bad token"))),
            expected: answered(500, "internal", "internal server error", true),
            allow: None,
        },
        Row {
            name: "worker permission denied",
            request: stat,
            reply: Some(|| failed_stat(Status::permission_denied("not yours"))),
            expected: answered(403, "forbidden", "not yours", false),
            allow: None,
        },
        Row {
            name: "worker invalid argument",
            request: stat,
            reply: Some(|| failed_stat(Status::invalid_argument("not a directory"))),
            expected: invalid(400, "not a directory"),
            allow: None,
        },
        Row {
            name: "worker out of range",
            request: stat,
            reply: Some(|| failed_stat(Status::out_of_range("past the end"))),
            expected: invalid(400, "past the end"),
            allow: None,
        },
        Row {
            name: "worker failed precondition",
            request: stat,
            reply: Some(|| failed_stat(Status::failed_precondition("is a directory"))),
            expected: answered(412, "failed_precondition", "is a directory", false),
            allow: None,
        },
        Row {
            name: "worker already exists",
            request: stat,
            reply: Some(|| failed_stat(Status::already_exists("exists"))),
            expected: answered(409, "conflict", "exists", false),
            allow: None,
        },
        Row {
            name: "worker aborted",
            request: stat,
            reply: Some(|| failed_stat(Status::aborted("raced"))),
            expected: answered(409, "conflict", "raced", false),
            allow: None,
        },
        Row {
            name: "stat answer without an entry",
            request: stat,
            reply: Some(|| Reply::Stat(Ok(StatResponse { info: None }))),
            expected: answered(500, "internal", "internal server error", true),
            allow: None,
        },
        Row {
            name: "list past 10 000 entries",
            request: |harness| harness.post("list", json!({"path": "/"})),
            reply: Some(|| {
                Reply::List(Ok((0..=10_000)
                    .map(|index| Ok(file_info(&format!("/f{index}"), 0)))
                    .collect()))
            }),
            expected: invalid(
                400,
                "file list returned too many entries; use a smaller depth",
            ),
            allow: None,
        },
        Row {
            name: "mutation without its key",
            request: |harness| harness.post("mkdir", json!({"path": "/d"})),
            reply: None,
            expected: invalid(400, "Idempotency-Key header is required"),
            allow: None,
        },
        Row {
            name: "key checked after the body",
            request: |harness| harness.post("rename", json!({"old_path": "/a"})),
            reply: None,
            expected: invalid(422, "invalid request"),
            allow: None,
        },
        Row {
            name: "key with a byte outside ASCII",
            request: |harness| {
                Request::post(harness.path("remove"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(
                        "idempotency-key",
                        axum::http::HeaderValue::from_bytes(b"k\xff").unwrap(),
                    )
                    .body(Body::from(r#"{"path": "/r"}"#))
                    .unwrap()
            },
            reply: None,
            expected: invalid(
                400,
                "Idempotency-Key must contain only printable ASCII characters, spaces and tabs",
            ),
            allow: None,
        },
        Row {
            name: "tracking enable that disables",
            request: |harness| harness.post_keyed("changes_enable", "k", json!({"enabled": false})),
            reply: None,
            expected: invalid(
                400,
                "file change tracking enable request must set enabled=true",
            ),
            allow: None,
        },
        Row {
            name: "modified with two filters",
            request: |harness| {
                harness.post("modified", json!({"window_seconds": 1, "cursor": "c"}))
            },
            reply: None,
            expected: invalid(
                400,
                "window_seconds, since, and cursor are mutually exclusive",
            ),
            allow: None,
        },
        Row {
            name: "modified since a non-timestamp",
            request: |harness| harness.post("modified", json!({"since": "yesterday"})),
            reply: None,
            expected: invalid(400, "since must be an RFC3339 timestamp"),
            allow: None,
        },
        Row {
            name: "mkdir that may have happened",
            request: |harness| {
                harness.post_keyed("mkdir", "k-mkdir-internal", json!({"path": "/d"}))
            },
            reply: Some(|| Reply::Mkdir(Err(Status::internal("guest failed")))),
            expected: answered(500, "internal", "internal server error", false),
            allow: None,
        },
        Row {
            name: "mkdir refused before start",
            request: |harness| {
                harness.post_keyed("mkdir", "k-mkdir-refused", json!({"path": "/d"}))
            },
            reply: Some(|| {
                Reply::Mkdir(Err(pplx_space_common_grpc::refused_before_start(
                    Status::unavailable("too many concurrent filesystem file operations"),
                    std::time::Duration::from_millis(1500),
                )))
            }),
            expected: Expected {
                retry_after: Some("2"),
                ..answered(
                    503,
                    "service_unavailable",
                    "too many concurrent filesystem file operations",
                    true,
                )
            },
            allow: None,
        },
        Row {
            name: "rename the sandbox replays by key",
            request: |harness| {
                harness.post_keyed(
                    "rename",
                    "k-rename-internal",
                    json!({"old_path": "/a", "new_path": "/b"}),
                )
            },
            reply: Some(|| Reply::Rename(Err(Status::internal("guest failed")))),
            expected: answered(500, "internal", "internal server error", true),
            allow: None,
        },
        Row {
            name: "list entry failure",
            request: |harness| harness.post("list", json!({"path": "/"})),
            reply: Some(|| {
                Reply::List(Ok(vec![
                    Ok(file_info("/a", 0)),
                    Err(Status::permission_denied("unreadable directory")),
                ]))
            }),
            expected: answered(403, "forbidden", "unreadable directory", false),
            allow: None,
        },
    ];

    let harness = Harness::start().await;
    for row in rows {
        if let Some(reply) = row.reply {
            harness.worker.reply(reply());
        }
        let (status, headers, body) = read_body(harness.send((row.request)(&harness)).await).await;
        let name = row.name;
        assert_eq!(status.as_u16(), row.expected.status, "{name}");
        let envelope: PublicErrorEnvelope = serde_json::from_slice(&body)
            .unwrap_or_else(|error| panic!("{name}: {error}: {body:?}"));
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["code"], row.expected.code, "{name}");
        assert_eq!(envelope.message, row.expected.message, "{name}");
        assert_eq!(envelope.retryable, row.expected.retryable, "{name}");
        let details = match row.expected.details {
            Value::Null => json!([]),
            ref details => details.clone(),
        };
        assert_eq!(json["details"], details, "{name}");
        assert_eq!(
            headers
                .get(header::RETRY_AFTER)
                .map(|value| value.to_str().unwrap()),
            row.expected.retry_after,
            "{name}"
        );
        assert_eq!(
            headers
                .get(header::ALLOW)
                .map(|value| value.to_str().unwrap()),
            row.allow,
            "{name}"
        );
        assert_eq!(headers[header::CONTENT_TYPE], "application/json", "{name}");
        // The envelope carries the id the response is labelled with.
        assert_eq!(
            envelope.request_id.as_deref(),
            headers
                .get("x-request-id")
                .map(|value| value.to_str().unwrap()),
            "{name}"
        );
    }
    // Every scripted reply was used, so each row met its own.
    assert_eq!(harness.worker.unanswered(), 0);
}

#[tokio::test]
async fn a_draining_runtime_is_reached_again_through_a_fresh_preparation() {
    if !test_process() {
        return;
    }
    let draining = || Status::unavailable(pplx_space_node_api_client::NODE_API_DRAINING_MESSAGE);
    let harness = Harness::start().await;
    let prepared = |harness: &Harness| {
        harness
            .api
            .calls()
            .iter()
            .filter(|call| call.rpc == "prepare")
            .count()
    };

    // A unary call reconnects once when the node-api drains.
    harness.worker.reply(Reply::Stat(Err(draining())));
    harness.worker.reply(Reply::Stat(Ok(StatResponse {
        info: Some(file_info("/a", 1)),
    })));
    let response = harness.send(stat(&harness)).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(prepared(&harness), 2);
    assert_eq!(harness.worker.calls().len(), 2);

    // Only once: a second drain is the answer.
    harness.worker.reply(Reply::Stat(Err(draining())));
    harness.worker.reply(Reply::Stat(Err(draining())));
    let response = harness.send(stat(&harness)).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(prepared(&harness), 4);
    assert_eq!(harness.worker.calls().len(), 4);

    // A closed connection is no reason to send a unary call again.
    harness.worker.reply(Reply::Stat(Err(Status::unavailable(
        "transport is closing",
    ))));
    let response = harness.send(stat(&harness)).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(prepared(&harness), 5);
    assert_eq!(harness.worker.calls().len(), 5);

    // A stream that never opened is opened again after either.
    for failure in [draining(), Status::unavailable("connection reset")] {
        harness.worker.reply(Reply::Read(Err(failure)));
        harness.worker.reply(Reply::Read(Ok(vec![Ok(FileChunk {
            data: Bytes::from_static(b"x"),
        })])));
    }
    for _ in 0..2 {
        let (status, _, body) = read_body(
            harness
                .send(harness.post("read", json!({"path": "/a", "length": 1})))
                .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.as_ref(), b"x");
    }
    assert_eq!(prepared(&harness), 9);

    // Preparing again is the API's decision, and its refusal is the answer.
    harness.worker.reply(Reply::Stat(Err(draining())));
    let gone = br#"{"code":"not_found","message":"sandbox not found","request_id":null,"retryable":false,"details":[]}"#;
    harness.api.prepare(Answer::Ok(harness.api.grant()));
    harness.api.prepare(Answer::Refuse(rendered(
        404,
        &[("content-type", "application/json")],
        gone,
    )));
    let (status, _, body) = read_body(harness.send(stat(&harness)).await).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body.as_ref(), gone);
}

#[tokio::test]
async fn a_read_that_fails_mid_stream_aborts_its_body() {
    if !test_process() {
        return;
    }
    let harness = Harness::start().await;
    harness.worker.reply(Reply::Read(Ok(vec![
        Ok(FileChunk {
            data: Bytes::from_static(b"partial"),
        }),
        Err(Status::internal("guest read failed")),
    ])));
    let response = harness
        .send(harness.post("read", json!({"path": "/a", "length": 100})))
        .await;
    // The 200 is sent before the sandbox fails; the body ends in an error
    // instead of a clean end.
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await.unwrap_err();
}

#[tokio::test]
async fn an_api_that_cannot_answer_is_a_retryable_503() {
    if !test_process() {
        return;
    }
    let harness = Harness::start().await;
    for failure in [
        Status::unavailable("connection refused"),
        // An API from before these RPCs.
        Status::unimplemented("unknown service"),
        Status::deadline_exceeded("timed out"),
    ] {
        harness.api.authorize(Answer::Fail(failure));
        let (status, headers, body) = read_body(harness.send(stat(&harness)).await).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(headers[header::RETRY_AFTER], "1");
        let envelope: PublicErrorEnvelope = serde_json::from_slice(&body).unwrap();
        assert!(envelope.retryable);
        assert_eq!(
            envelope.message,
            "native file access is unavailable; retry the request"
        );
    }
    // A request the API could not read is this server's fault.
    harness
        .api
        .authorize(Answer::Fail(Status::invalid_argument("bad header")));
    let response = harness.send(stat(&harness)).await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(harness.worker.calls().is_empty());
}
