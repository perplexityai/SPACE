//! `POST /files` through the router with upload admission on: which checks
//! run before admission, what the permit covers, and how the shares and the
//! minimum body rate surface as responses.

use super::*;
use crate::upload_admission::{
    MinBodyRateConfig, UploadAdmission, UploadAdmissionConfig, UploadShareConfig, UploadTenant,
};
use crate::{ECW_ENVD_PORT, EcwHostConfig};
use axum::http::Request;
use pplx_space_common_core::types::{OrgId, SandboxId};
use std::{
    convert::Infallible,
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tower::ServiceExt;

/// A router whose ECW route for `route`'s sandbox is already cached, so
/// `POST /files` reaches admission without ClusterApi, with upload admission
/// under `config` or, given `None`, off.
async fn router_for_route(
    route: EcwResolvedRoute,
    config: Option<UploadAdmissionConfig>,
) -> (axum::Router, Option<Arc<UploadAdmission>>) {
    let (app, uploads, _) = router_for_route_with_tracker(route, config).await;
    (app, uploads)
}

/// [`router_for_route`], also returning the in-flight upload tracker it wires
/// in.
async fn router_for_route_with_tracker(
    route: EcwResolvedRoute,
    config: Option<UploadAdmissionConfig>,
) -> (
    axum::Router,
    Option<Arc<UploadAdmission>>,
    Arc<crate::http_traffic::UploadTracker>,
) {
    router_for_routes_with_tracker(vec![route], config).await
}

/// [`router_for_route_with_tracker`] with every route in `routes` cached.
async fn router_for_routes_with_tracker(
    routes: Vec<EcwResolvedRoute>,
    config: Option<UploadAdmissionConfig>,
) -> (
    axum::Router,
    Option<Arc<UploadAdmission>>,
    Arc<crate::http_traffic::UploadTracker>,
) {
    let mut state = crate::tests::test_data_plane_state().await;
    state.ecw_hosts = Arc::new(EcwHostConfig::from_env_value(Some(
        r#"[{"host":"ecw.test"}]"#,
    )));
    for route in routes {
        state.route_cache.remember_ecw(
            crate::tests::test_ecw_route_origin(route.sandbox_id).cache_key(),
            route,
            std::time::Duration::from_secs(60),
        );
    }
    let uploads = config.map(|config| Arc::new(UploadAdmission::new(&config)));
    state.uploads = uploads.clone();
    let tracker = Arc::clone(&state.upload_tracker);
    (crate::router(state), uploads, tracker)
}

/// `tenant`'s route, to a backend at `backend`.
fn tenant_route(tenant: UploadTenant, backend: &str) -> EcwResolvedRoute {
    let mut route = crate::tests::ecw_resolved_route(
        tenant.sandbox_id,
        1,
        crate::tests::backend_endpoint(backend),
    );
    route.org_id = Some(tenant.org_id);
    route
}

/// `router_for_route` for `tenant`, with admission under `config` and no
/// backend.
async fn upload_router_with(
    tenant: UploadTenant,
    config: UploadAdmissionConfig,
) -> (axum::Router, Arc<UploadAdmission>) {
    let (app, uploads) = router_for_route(tenant_route(tenant, "127.0.0.1:1"), Some(config)).await;
    (app, uploads.unwrap())
}

/// `upload_router_with` a limiter that has one slot and a 50 ms wait.
async fn upload_router(sandbox_id: SandboxId) -> (axum::Router, Arc<UploadAdmission>) {
    let (app, uploads, _) = upload_router_with_tracker(sandbox_id).await;
    (app, uploads)
}

/// [`upload_router`], also returning the in-flight upload tracker it wires in.
async fn upload_router_with_tracker(
    sandbox_id: SandboxId,
) -> (
    axum::Router,
    Arc<UploadAdmission>,
    Arc<crate::http_traffic::UploadTracker>,
) {
    let tenant = UploadTenant {
        org_id: OrgId::new(),
        sandbox_id,
    };
    let (app, uploads, tracker) = router_for_route_with_tracker(
        tenant_route(tenant, "127.0.0.1:1"),
        Some(UploadAdmissionConfig {
            max_concurrent_uploads: NonZeroUsize::MIN,
            wait: std::time::Duration::from_millis(50),
            ..UploadAdmissionConfig::default()
        }),
    )
    .await;
    (app, uploads.unwrap(), tracker)
}

/// Sends `request` through `app`, failing the test rather than hanging it if
/// no response comes within ten seconds.
async fn send(app: axum::Router, request: Request<Body>) -> axum::response::Response {
    tokio::time::timeout(std::time::Duration::from_secs(10), app.oneshot(request))
        .await
        .expect("no response within ten seconds")
        .unwrap()
}

/// Some other org's sandbox, which no route in these routers names.
fn other_tenant() -> UploadTenant {
    UploadTenant {
        org_id: OrgId::new(),
        sandbox_id: SandboxId::new(),
    }
}

fn upload_request(
    sandbox_id: SandboxId,
    path: &str,
    content_type: &str,
    content_length: &str,
    body: Body,
) -> Request<Body> {
    let mut request = Request::post(path)
        .header(
            header::HOST,
            format!("{ECW_ENVD_PORT}-{sandbox_id}.ecw.test"),
        )
        .header(header::CONTENT_TYPE, content_type);
    // An empty length leaves the header off, as a chunked upload would.
    if !content_length.is_empty() {
        request = request.header(header::CONTENT_LENGTH, content_length);
    }
    request.body(body).unwrap()
}

/// An empty body that runs `on_poll` the first time the handler reads it.
fn observed_body(on_poll: impl Fn() + Send + 'static) -> Body {
    Body::from_stream(futures_util::stream::poll_fn(move |_| {
        on_poll();
        std::task::Poll::Ready(None::<Result<bytes::Bytes, std::convert::Infallible>>)
    }))
}

/// Every row hits a saturated limiter. Admittable uploads get 429; uploads
/// the headers or query already condemn get their 400 without queueing for a
/// permit. No row may read the body.
#[tokio::test]
async fn saturated_admission_rejects_post_files_before_reading_the_body() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let sandbox_id = SandboxId::new();
    let (app, uploads) = upload_router(sandbox_id).await;
    let _held = uploads.admit(other_tenant(), 1).await.unwrap();

    let over_limit = (E2B_FILE_CONTENT_LIMIT_BYTES as u64 + 1).to_string();
    let over_json_limit = (E2B_JSON_FILE_WRITE_BODY_LIMIT_BYTES as u64 + 1).to_string();
    let octet = "application/octet-stream";
    let file = "/files?path=/tmp/upload.bin";
    for (name, path, content_type, content_length, expected) in [
        (
            "octet-stream",
            file,
            octet,
            "5",
            StatusCode::TOO_MANY_REQUESTS,
        ),
        (
            "multipart",
            file,
            "multipart/form-data; boundary=b",
            "5",
            StatusCode::TOO_MANY_REQUESTS,
        ),
        (
            "json",
            "/files",
            "application/json",
            "5",
            StatusCode::TOO_MANY_REQUESTS,
        ),
        (
            "octet-stream without path",
            "/files",
            octet,
            "5",
            StatusCode::BAD_REQUEST,
        ),
        (
            "octet-stream with invalid path",
            "/files?path=/tmp/%0Afile",
            octet,
            "5",
            StatusCode::BAD_REQUEST,
        ),
        (
            "octet-stream over the body limit",
            file,
            octet,
            over_limit.as_str(),
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        (
            "invalid username",
            "/files?path=/tmp/upload.bin&username=",
            octet,
            "5",
            StatusCode::BAD_REQUEST,
        ),
        (
            "multipart without boundary",
            "/files",
            "multipart/form-data",
            "5",
            StatusCode::BAD_REQUEST,
        ),
        (
            "unsupported content type",
            "/files",
            "text/plain",
            "5",
            StatusCode::BAD_REQUEST,
        ),
        (
            "json over the json body limit",
            "/files",
            "application/json",
            over_json_limit.as_str(),
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        (
            "multipart over the body limit",
            file,
            "multipart/form-data; boundary=b",
            over_limit.as_str(),
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        (
            "multipart with invalid query path",
            "/files?path=/tmp/%0Afile",
            "multipart/form-data; boundary=b",
            "5",
            StatusCode::BAD_REQUEST,
        ),
        (
            "unparsable content length",
            "/files",
            "application/json",
            "five",
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let body_polled = Arc::new(AtomicBool::new(false));
        let body = {
            let body_polled = body_polled.clone();
            observed_body(move || body_polled.store(true, Ordering::SeqCst))
        };
        let request = upload_request(sandbox_id, path, content_type, content_length, body);
        let response = send(app.clone(), request).await;

        assert_eq!(response.status(), expected, "{name}");
        assert_eq!(
            response.headers().contains_key(header::RETRY_AFTER),
            expected == StatusCode::TOO_MANY_REQUESTS,
            "{name}"
        );
        assert!(
            !body_polled.load(Ordering::SeqCst),
            "{name}: a rejected upload must not read its body"
        );
    }
}

/// The permit is still held while the handler reads the body, not released
/// once admission succeeds, and it reserves the declared length, or the JSON
/// write limit when no length is declared.
#[tokio::test]
async fn an_admitted_upload_holds_its_permit_while_reading_the_body() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let sandbox_id = SandboxId::new();
    let (app, uploads) = upload_router(sandbox_id).await;
    let budget = uploads.available_bytes();
    for (content_length, reserved) in [
        ("1000", 1024),
        ("", E2B_JSON_FILE_WRITE_BODY_LIMIT_BYTES as u64),
    ] {
        let during_read = Arc::new(std::sync::Mutex::new(None));
        let body = {
            let uploads = uploads.clone();
            let during_read = during_read.clone();
            observed_body(move || {
                *during_read.lock().unwrap() =
                    Some((uploads.available_uploads(), uploads.available_bytes()));
            })
        };
        let request = upload_request(
            sandbox_id,
            "/files",
            "application/json",
            content_length,
            body,
        );
        // The empty JSON body is rejected after the read; only the read matters.
        let response = send(app.clone(), request).await;

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "{content_length:?}"
        );
        assert_eq!(
            *during_read.lock().unwrap(),
            Some((0, budget - reserved)),
            "{content_length:?}: slot and reservation held while reading"
        );
        assert_eq!(
            uploads.available_uploads(),
            1,
            "the slot returns afterwards"
        );
        assert_eq!(
            uploads.available_bytes(),
            budget,
            "the bytes return afterwards"
        );
    }
}

/// The route's org and sandbox key the shares: with the pod's budget wide
/// open, an upload still waits on its own sandbox's and org's shares.
#[tokio::test]
async fn post_files_is_admitted_against_the_routes_sandbox_and_org() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let one_slot = UploadShareConfig {
        max_concurrent_uploads: NonZeroUsize::MIN,
        max_in_flight_bytes: NonZeroU64::new(1 << 30).unwrap(),
    };
    let wide_open = UploadShareConfig {
        max_concurrent_uploads: NonZeroUsize::new(64).unwrap(),
        ..one_slot
    };
    let tenant = other_tenant();
    let neighbour = UploadTenant {
        org_id: tenant.org_id,
        sandbox_id: SandboxId::new(),
    };
    // (share held, per-org, per-sandbox, holder)
    for (share, per_org, per_sandbox, held_by) in [
        ("sandbox", wide_open, one_slot, tenant),
        ("org", one_slot, wide_open, neighbour),
    ] {
        let (app, uploads) = upload_router_with(
            tenant,
            UploadAdmissionConfig {
                wait: std::time::Duration::from_millis(50),
                per_org,
                per_sandbox,
                ..UploadAdmissionConfig::default()
            },
        )
        .await;
        let held = uploads.admit(held_by, 1).await.unwrap();
        let _unrelated = uploads.admit(other_tenant(), 1).await.unwrap();

        let request = || {
            upload_request(
                tenant.sandbox_id,
                "/files",
                "application/json",
                "",
                Body::empty(),
            )
        };
        let response = send(app.clone(), request()).await;
        assert_eq!(
            response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "{share} share held"
        );

        drop(held);
        // Admitted now; the empty JSON body is then rejected as invalid.
        let response = send(app.clone(), request()).await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "{share} share free"
        );
    }
}

/// An admitted upload whose client stops sending is answered 408 on every
/// body path that reads before talking to the guest, and gives its permit
/// back.
#[tokio::test(start_paused = true)]
async fn a_stalled_upload_body_gets_408_and_frees_its_permit() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let tenant = other_tenant();
    let (app, uploads) = upload_router_with(
        tenant,
        UploadAdmissionConfig {
            min_body_rate: Some(MinBodyRateConfig {
                bytes_per_second: NonZeroU64::new(1024).unwrap(),
                window: std::time::Duration::from_millis(50),
            }),
            ..UploadAdmissionConfig::default()
        },
    )
    .await;
    let slots = uploads.available_uploads();
    for (name, path, content_type) in [
        ("json", "/files", "application/json"),
        (
            "multipart",
            "/files?path=/tmp/upload.bin",
            "multipart/form-data; boundary=b",
        ),
    ] {
        // A first frame, then nothing: the client is still connected but
        // has stopped sending.
        let stalled = futures_util::stream::once(async {
            Ok::<_, Infallible>(bytes::Bytes::from_static(b"{"))
        })
        .chain(futures_util::stream::pending());
        let request = upload_request(
            tenant.sandbox_id,
            path,
            content_type,
            "",
            Body::from_stream(stalled),
        );
        let response = send(app.clone(), request).await;

        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT, "{name}");
        assert_eq!(uploads.available_uploads(), slots, "{name}");
    }
}

/// A route whose org id didn't parse still serves `POST /files` with
/// admission off; with admission on, the upload has no org to be charged to
/// and is refused rather than admitted unbounded, while `GET /files` on the
/// same route still serves the file.
#[tokio::test]
async fn a_route_without_an_org_refuses_admitted_uploads_only() {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("test-token")),
        ),
    ]) {
        return;
    }

    let sandbox_id = SandboxId::new();
    let worker = spawn_read_worker(vec![b"file".to_vec()]).await;
    for (admission, expected) in [
        (None, StatusCode::BAD_REQUEST),
        (
            Some(UploadAdmissionConfig::default()),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ] {
        let mut route = tenant_route(other_tenant(), &worker);
        route.sandbox_id = sandbox_id;
        route.origin = crate::tests::test_ecw_route_origin(sandbox_id);
        route.org_id = None;
        let admitted = admission.is_some();
        let (app, _) = router_for_route(route, admission).await;
        // An empty JSON body: invalid once read, so reaching the read is a 400.
        let request = upload_request(sandbox_id, "/files", "application/json", "0", Body::empty());
        let response = send(app.clone(), request).await;
        assert_eq!(response.status(), expected, "admission on: {admitted}");

        let download = Request::get("/files?path=/tmp/file.bin")
            .header(
                header::HOST,
                format!("{ECW_ENVD_PORT}-{sandbox_id}.ecw.test"),
            )
            .body(Body::empty())
            .unwrap();
        let response = send(app, download).await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "admission on: {admitted}"
        );
        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(body.as_ref(), b"file", "admission on: {admitted}");
    }
}

/// A worker that answers every `SpacedMkdir` with success, for uploads that
/// create their parent directory before reading the body. Returns its
/// address.
async fn spawn_mkdir_only_worker() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let worker = axum::Router::new().fallback(|request: Request<Body>| async move {
        let path = request.uri().path().to_string();
        assert!(path.ends_with("/SpacedMkdir"), "unexpected call {path}");
        crate::tests::grpc_test_response(Vec::new())
    });
    tokio::spawn(async move {
        axum::serve(listener, worker).await.unwrap();
    });
    address
}

/// The octet-stream path creates the destination's parent before it reads
/// the body; a client that then stalls gets 408 there too, and its slot back.
#[tokio::test]
async fn a_stalled_octet_stream_upload_gets_408_after_its_parent_is_created() {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("test-token")),
        ),
    ]) {
        return;
    }

    let tenant = other_tenant();
    let worker = spawn_mkdir_only_worker().await;
    let (app, uploads) = router_for_route(
        tenant_route(tenant, &worker),
        Some(UploadAdmissionConfig {
            min_body_rate: Some(MinBodyRateConfig {
                bytes_per_second: NonZeroU64::new(1024).unwrap(),
                window: std::time::Duration::from_millis(50),
            }),
            ..UploadAdmissionConfig::default()
        }),
    )
    .await;
    let uploads = uploads.unwrap();
    let slots = uploads.available_uploads();
    let stalled =
        futures_util::stream::once(async { Ok::<_, Infallible>(bytes::Bytes::from_static(b"x")) })
            .chain(futures_util::stream::pending());
    let request = upload_request(
        tenant.sandbox_id,
        "/files?path=/tmp/dir/upload.bin",
        "application/octet-stream",
        "",
        Body::from_stream(stalled),
    );
    let response = send(app, request).await;

    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    assert_eq!(uploads.available_uploads(), slots);
}

/// A worker that answers every `SpacedRead` by streaming `chunks`. Returns
/// its address.
async fn spawn_read_worker(chunks: Vec<Vec<u8>>) -> String {
    spawn_read_worker_by_path(move |_| chunks.clone()).await
}

/// A worker that answers each `SpacedRead` by streaming the chunks `file`
/// gives for the requested path. Returns its address.
async fn spawn_read_worker_by_path(
    file: impl Fn(&str) -> Vec<Vec<u8>> + Clone + Send + Sync + 'static,
) -> String {
    use prost::Message as _;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let worker = axum::Router::new().fallback(move |request: Request<Body>| {
        let file = file.clone();
        async move {
            let path = request.uri().path().to_string();
            assert!(path.ends_with("/SpacedRead"), "unexpected call {path}");
            let body = http_body_util::BodyExt::collect(request.into_body())
                .await
                .unwrap()
                .to_bytes();
            // One gRPC message: a five-byte prefix, then the request.
            let read = SpacedReadRequest::decode(&body[5..]).unwrap();
            let chunks = file(&read.operation.unwrap().path);
            let mut frames: Vec<Result<hyper::body::Frame<bytes::Bytes>, Infallible>> = chunks
                .into_iter()
                .map(|data| {
                    let chunk = spaced_ops::FileChunk { data: data.into() }.encode_to_vec();
                    let mut message = vec![0];
                    message.extend_from_slice(&(chunk.len() as u32).to_be_bytes());
                    message.extend(chunk);
                    Ok(hyper::body::Frame::data(bytes::Bytes::from(message)))
                })
                .collect();
            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", header::HeaderValue::from_static("0"));
            frames.push(Ok(hyper::body::Frame::trailers(trailers)));
            axum::response::Response::builder()
                .header("content-type", "application/grpc")
                .body(Body::new(http_body_util::StreamBody::new(
                    futures_util::stream::iter(frames),
                )))
                .unwrap()
        }
    });
    tokio::spawn(async move {
        axum::serve(listener, worker).await.unwrap();
    });
    address
}

/// `GET /files` streams the guest's file back intact and charges it to the
/// org's transfer rate when admission is on. How the bucket paces a body is
/// the bucket's own tests' business; here it only has to be wired in.
#[tokio::test]
async fn get_files_charges_the_file_to_the_orgs_transfer_rate() {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("test-token")),
        ),
    ]) {
        return;
    }

    const CHUNK: usize = 100_000;
    const BURST: u64 = 1_000_000;
    let chunks: Vec<Vec<u8>> = (0..3u8).map(|byte| vec![byte; CHUNK]).collect();
    let expected = chunks.concat();
    let worker = spawn_read_worker(chunks).await;
    // A burst that covers the whole file and a rate too slow to refill any of
    // it meanwhile, so the bucket ends exactly the file's size lower.
    let charged = UploadAdmissionConfig {
        org_transfer_rate: Some(crate::upload_admission::ByteRateConfig {
            bytes_per_second: NonZeroU64::MIN,
            burst_bytes: NonZeroU64::new(BURST).unwrap(),
        }),
        ..UploadAdmissionConfig::default()
    };
    for admission in [None, Some(charged)] {
        let tenant = other_tenant();
        let admitted = admission.is_some();
        let (app, uploads) = router_for_route(tenant_route(tenant, &worker), admission).await;
        let request = Request::get("/files?path=/tmp/file.bin")
            .header(
                header::HOST,
                format!("{ECW_ENVD_PORT}-{}.ecw.test", tenant.sandbox_id),
            )
            .body(Body::empty())
            .unwrap();
        let response = send(app, request).await;
        assert_eq!(response.status(), StatusCode::OK, "admission: {admitted}");
        let body = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            http_body_util::BodyExt::collect(response.into_body()),
        )
        .await
        .expect("the file didn't arrive within ten seconds")
        .unwrap()
        .to_bytes();
        assert_eq!(body.as_ref(), expected.as_slice(), "admission: {admitted}");
        if let Some(uploads) = uploads {
            let balance = uploads.org_bucket(tenant.org_id).unwrap().balance();
            let expected_balance = (BURST - expected.len() as u64) as f64;
            // At one byte a second, the few seconds the test takes refill
            // only a few bytes.
            assert!(
                (expected_balance..expected_balance + 10.0).contains(&balance),
                "{balance}"
            );
        }
    }
}

/// Through the production router: an admitted upload is in flight with the
/// budget admission actually holds while its body is read and leaves however
/// its write ends, and an upload the saturated limiter turns away gets a 429
/// that names the limit and never enters the in-flight count.
#[tokio::test]
async fn admitted_uploads_are_tracked_with_their_reservation_and_rejections_name_the_limit() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let sandbox_id = SandboxId::new();
    let (app, uploads, tracker) = upload_router_with_tracker(sandbox_id).await;

    let seen = Arc::new(std::sync::Mutex::new(None));
    let body = {
        let tracker = Arc::clone(&tracker);
        let seen = Arc::clone(&seen);
        observed_body(move || *seen.lock().unwrap() = Some(tracker.snapshot()))
    };
    // A JSON write reads its body before calling the guest, so the observer
    // runs mid-upload. The empty body then fails to parse, which ends it.
    let response = app
        .clone()
        .oneshot(upload_request(
            sandbox_id,
            "/files",
            "application/json",
            "5",
            body,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    // Five declared bytes take one whole 1 KiB budget permit.
    assert_eq!(*seen.lock().unwrap(), Some((1, 1024)));
    assert_eq!(tracker.snapshot(), (0, 0));

    let _held = uploads.admit(other_tenant(), 1).await.unwrap();
    let response = app
        .oneshot(upload_request(
            sandbox_id,
            "/files",
            "application/json",
            "5",
            Body::from("hello"),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        response
            .extensions()
            .get::<pplx_space_metrics::data_plane::TooManyRequestsReason>(),
        Some(&pplx_space_metrics::data_plane::TooManyRequestsReason::UploadConcurrency),
    );
    assert_eq!(tracker.snapshot(), (0, 0));
}

/// Through a real hyper server and socket rather than an in-process body, so
/// the minimum body rate sees hyper's own wakes: a client that sends part of a
/// chunked upload and then goes quiet gets 408, and its slot back.
#[tokio::test]
async fn a_client_that_goes_quiet_mid_upload_gets_408_over_a_real_connection() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let tenant = other_tenant();
    let (app, uploads) = router_for_route(
        tenant_route(tenant, "127.0.0.1:1"),
        Some(UploadAdmissionConfig {
            min_body_rate: Some(MinBodyRateConfig {
                bytes_per_second: NonZeroU64::new(1024).unwrap(),
                window: std::time::Duration::from_millis(100),
            }),
            ..UploadAdmissionConfig::default()
        }),
    )
    .await;
    let uploads = uploads.unwrap();
    let slots = uploads.available_uploads();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    let request = format!(
        "POST /files HTTP/1.1\r\nHost: {ECW_ENVD_PORT}-{}.ecw.test\r\n\
         Content-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n\
         1\r\n{{\r\n",
        tenant.sandbox_id
    );
    client.write_all(request.as_bytes()).await.unwrap();
    // The client sends nothing more and keeps the connection open.
    let mut response = vec![0; 1024];
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.read(&mut response),
    )
    .await
    .expect("no response within ten seconds")
    .unwrap();
    let status_line = String::from_utf8_lossy(&response[..read]);
    assert!(
        status_line.starts_with("HTTP/1.1 408"),
        "{}",
        status_line.lines().next().unwrap_or_default()
    );
    assert_eq!(uploads.available_uploads(), slots);
}

/// One HTTP/2 connection to a router whose routes for `tenants` are cached,
/// each to its backend, served on a local port.
async fn http2_connection(
    tenants: &[(UploadTenant, &str)],
    config: UploadAdmissionConfig,
) -> (
    hyper::client::conn::http2::SendRequest<http_body_util::Full<bytes::Bytes>>,
    Arc<UploadAdmission>,
) {
    let (app, uploads, _) = router_for_routes_with_tracker(
        tenants
            .iter()
            .map(|(tenant, backend)| tenant_route(*tenant, backend))
            .collect(),
        Some(config),
    )
    .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let tcp = tokio::net::TcpStream::connect(address).await.unwrap();
    let (sender, connection) = hyper::client::conn::http2::handshake(
        hyper_util::rt::TokioExecutor::new(),
        hyper_util::rt::TokioIo::new(tcp),
    )
    .await
    .unwrap();
    tokio::spawn(connection);
    (sender, uploads.unwrap())
}

/// A backend address nothing listens on.
const NO_BACKEND: &str = "127.0.0.1:1";

fn http2_upload(
    tenant: UploadTenant,
    path: &str,
    content_type: &str,
    body: Vec<u8>,
) -> Request<http_body_util::Full<bytes::Bytes>> {
    let host = format!("{ECW_ENVD_PORT}-{}.ecw.test", tenant.sandbox_id);
    Request::post(format!("http://{host}{path}"))
        .header(header::HOST, host)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, body.len())
        .body(http_body_util::Full::new(bytes::Bytes::from(body)))
        .unwrap()
}

/// A multipart body with one 4 MiB part: more than the server's connection
/// flow-control window, so if its stream went unread it would hold back every
/// other stream on the connection.
fn window_filling_multipart() -> Vec<u8> {
    let mut body = b"--b\r\nContent-Disposition: form-data; name=\"file\"; filename=\"upload.bin\"\r\nContent-Type: application/octet-stream\r\n\r\n".to_vec();
    body.resize(body.len() + (4 << 20), 0);
    body.extend_from_slice(b"\r\n--b--\r\n");
    body
}

/// Sends `first`, gives its body time to reach the server, then sends a
/// one-byte JSON upload for `completing` on the same connection, which must
/// be read to the end, a 400 for malformed JSON, well inside `within`.
/// Returns `first`'s status.
async fn second_upload_completes(
    sender: &mut hyper::client::conn::http2::SendRequest<http_body_util::Full<bytes::Bytes>>,
    first: Request<http_body_util::Full<bytes::Bytes>>,
    completing: UploadTenant,
    within: std::time::Duration,
) -> StatusCode {
    use http_body_util::BodyExt as _;
    let first = tokio::spawn(sender.send_request(first));
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    sender.ready().await.unwrap();
    let completing_upload = sender.send_request(http2_upload(
        completing,
        "/files",
        "application/json",
        b"{".to_vec(),
    ));
    let response = tokio::time::timeout(within, completing_upload)
        .await
        .expect("the other org's upload got no response in time")
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    tokio::time::timeout(within, first)
        .await
        .expect("the first upload got no response in time")
        .unwrap()
        .unwrap()
        .status()
}

/// Two orgs' uploads share one HTTP/2 connection, and one org's sandbox share
/// is full. Its upload is rejected at once rather than left unread through a
/// long admission wait, so the other org's upload behind it on the
/// connection completes.
#[tokio::test]
async fn an_upload_completes_while_another_orgs_is_over_its_share_on_the_same_http2_connection() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }
    let full = other_tenant();
    let completing = other_tenant();
    let wait = std::time::Duration::from_secs(60);
    let (mut sender, uploads) = http2_connection(
        &[(full, NO_BACKEND), (completing, NO_BACKEND)],
        UploadAdmissionConfig {
            wait,
            per_sandbox: UploadShareConfig {
                max_concurrent_uploads: NonZeroUsize::MIN,
                max_in_flight_bytes: NonZeroU64::new(1 << 30).unwrap(),
            },
            org_transfer_rate: None,
            ..UploadAdmissionConfig::default()
        },
    )
    .await;
    let _held = uploads.admit(full, 1).await.unwrap();

    let started = std::time::Instant::now();
    let status = second_upload_completes(
        &mut sender,
        http2_upload(
            full,
            "/files?path=/tmp/upload.bin",
            "multipart/form-data; boundary=b",
            window_filling_multipart(),
        ),
        completing,
        std::time::Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(started.elapsed() < wait);
}

/// Two orgs' uploads share one HTTP/2 connection, and one org's transfer
/// rate is far below its upload's size. Its body is charged to its bucket
/// rather than held while the debt is slept off, so the other org's upload
/// behind it on the connection completes, and the org's next HTTP/2 upload
/// is rejected while it owes the bucket.
#[tokio::test]
async fn an_upload_completes_while_another_orgs_is_over_its_rate_on_the_same_http2_connection() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }
    let throttled = other_tenant();
    let completing = other_tenant();
    let (mut sender, _uploads) = http2_connection(
        &[(throttled, NO_BACKEND), (completing, NO_BACKEND)],
        UploadAdmissionConfig {
            // 4 MiB at 1 KiB/s would take over an hour to sleep off.
            org_transfer_rate: Some(crate::upload_admission::ByteRateConfig {
                bytes_per_second: NonZeroU64::new(1024).unwrap(),
                burst_bytes: NonZeroU64::new(1024).unwrap(),
            }),
            ..UploadAdmissionConfig::default()
        },
    )
    .await;

    let status = second_upload_completes(
        &mut sender,
        http2_upload(
            throttled,
            "/files?path=/tmp/upload.bin",
            "multipart/form-data; boundary=b",
            window_filling_multipart(),
        ),
        completing,
        std::time::Duration::from_secs(10),
    )
    .await;
    // Read to the end; the guest write then fails, as there's no backend.
    assert_ne!(status, StatusCode::TOO_MANY_REQUESTS);
    sender.ready().await.unwrap();
    let next = sender
        .send_request(http2_upload(
            throttled,
            "/files",
            "application/json",
            b"{".to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(next.status(), StatusCode::TOO_MANY_REQUESTS);
}

/// A worker that accepts every call and never answers. Returns its address.
async fn spawn_unanswering_worker() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let worker = axum::Router::new().fallback(std::future::pending::<axum::response::Response>);
    tokio::spawn(async move {
        axum::serve(listener, worker).await.unwrap();
    });
    address
}

/// Two orgs' uploads share one HTTP/2 connection, and one org's guest never
/// answers the octet-stream path's parent-directory call. That upload's body
/// is spooled before the call rather than left unread through it, so the
/// other org's upload behind it on the connection completes.
#[tokio::test]
async fn an_upload_completes_while_another_orgs_guest_is_unresponsive_on_the_same_http2_connection()
{
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("test-token")),
        ),
    ]) {
        return;
    }
    let unresponsive = other_tenant();
    let completing = other_tenant();
    let worker = spawn_unanswering_worker().await;
    let (mut sender, _uploads) = http2_connection(
        &[(unresponsive, &worker), (completing, NO_BACKEND)],
        UploadAdmissionConfig {
            org_transfer_rate: None,
            ..UploadAdmissionConfig::default()
        },
    )
    .await;

    use http_body_util::BodyExt as _;
    let first = tokio::spawn(sender.send_request(http2_upload(
        unresponsive,
        "/files?path=/tmp/dir/upload.bin",
        "application/octet-stream",
        vec![0; 4 << 20],
    )));
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    sender.ready().await.unwrap();
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        sender.send_request(http2_upload(
            completing,
            "/files",
            "application/json",
            b"{".to_vec(),
        )),
    )
    .await
    .expect("the other org's upload got no response in time")
    .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    // The unresponsive guest's upload is still waiting on it.
    assert!(!first.is_finished());
    first.abort();
}

/// One org's downloads are far over its transfer rate while another org's
/// download reads from the same pooled backend connection. The first org's
/// downloads are charged and streamed rather than held, so their unread data
/// never fills the connection's flow-control window, and the other org's
/// download completes promptly.
#[tokio::test]
async fn a_download_completes_while_another_orgs_are_over_their_rate_on_the_same_backend_connection()
 {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("test-token")),
        ),
    ]) {
        return;
    }
    use http_body_util::BodyExt as _;
    // 4 MiB a download: three of them are more than the backend connection's
    // flow-control window, if left unread.
    let worker = spawn_read_worker_by_path(|path| {
        if path.ends_with("big.bin") {
            vec![vec![7; 64 << 10]; 64]
        } else {
            vec![vec![1]]
        }
    })
    .await;
    let heavy = other_tenant();
    let light = other_tenant();
    // The test router pools one backend channel per worker, so both
    // sandboxes' reads share it.
    let (app, _uploads, _) = router_for_routes_with_tracker(
        vec![tenant_route(heavy, &worker), tenant_route(light, &worker)],
        Some(UploadAdmissionConfig {
            // 4 MiB at 1 KiB/s would take over an hour to sleep off.
            org_transfer_rate: Some(crate::upload_admission::ByteRateConfig {
                bytes_per_second: NonZeroU64::new(1024).unwrap(),
                burst_bytes: NonZeroU64::new(1024).unwrap(),
            }),
            ..UploadAdmissionConfig::default()
        }),
    )
    .await;
    let download = |tenant: UploadTenant, path: &str| {
        Request::get(format!("/files?path={path}"))
            .header(
                header::HOST,
                format!("{ECW_ENVD_PORT}-{}.ecw.test", tenant.sandbox_id),
            )
            .body(Body::empty())
            .unwrap()
    };

    // The heavy org's three downloads start together, each read by its
    // client as fast as it arrives.
    let heavy_downloads: Vec<_> = (0..3)
        .map(|_| {
            let response = app.clone().oneshot(download(heavy, "/tmp/big.bin"));
            tokio::spawn(async move {
                // Admitted and read to the end, or refused once the org owes.
                let response = response.await.unwrap();
                response.into_body().collect().await.unwrap();
            })
        })
        .collect();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let response = send(app.clone(), download(light, "/tmp/small.bin")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        response.into_body().collect(),
    )
    .await
    .expect("the other org's download didn't arrive in time")
    .unwrap()
    .to_bytes();
    assert_eq!(body.as_ref(), [1]);
    for heavy_download in heavy_downloads {
        heavy_download.abort();
    }
    // Now in debt, the heavy org's next download is refused before it
    // reaches the guest.
    let refused = send(app, download(heavy, "/tmp/big.bin")).await;
    assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
}
