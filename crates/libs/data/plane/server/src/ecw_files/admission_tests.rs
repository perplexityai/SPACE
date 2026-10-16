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

/// A router whose ECW route for `tenant`'s sandbox is already cached, so
/// `POST /files` reaches admission without ClusterApi, under `config`.
async fn upload_router_with(
    tenant: UploadTenant,
    config: UploadAdmissionConfig,
) -> (axum::Router, Arc<UploadAdmission>) {
    let mut state = crate::tests::test_data_plane_state().await;
    state.ecw_hosts = EcwHostConfig::from_env_value(Some(r#"[{"host":"ecw.test"}]"#));
    let mut route = crate::tests::ecw_resolved_route(
        tenant.sandbox_id,
        1,
        crate::tests::backend_endpoint("127.0.0.1:1"),
    );
    route.org_id = tenant.org_id;
    state.route_cache.remember_ecw(
        crate::tests::test_ecw_route_origin(tenant.sandbox_id).cache_key(),
        route,
        std::time::Duration::from_secs(60),
    );
    let uploads = Arc::new(UploadAdmission::new(&config));
    state.uploads = Some(uploads.clone());
    (crate::router(state), uploads)
}

/// `upload_router_with` a limiter that has one slot and a 50 ms wait.
async fn upload_router(sandbox_id: SandboxId) -> (axum::Router, Arc<UploadAdmission>) {
    upload_router_with(
        UploadTenant {
            org_id: OrgId::new(),
            sandbox_id,
        },
        UploadAdmissionConfig {
            max_concurrent_uploads: NonZeroUsize::MIN,
            wait: std::time::Duration::from_millis(50),
            ..UploadAdmissionConfig::default()
        },
    )
    .await
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
        let response = app.clone().oneshot(request).await.unwrap();

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
        let response = app.clone().oneshot(request).await.unwrap();

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
        let response = app.clone().oneshot(request()).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "{share} share held"
        );

        drop(held);
        // Admitted now; the empty JSON body is then rejected as invalid.
        let response = app.clone().oneshot(request()).await.unwrap();
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
#[tokio::test]
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
        let response = app.clone().oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT, "{name}");
        assert_eq!(uploads.available_uploads(), slots, "{name}");
    }
}
