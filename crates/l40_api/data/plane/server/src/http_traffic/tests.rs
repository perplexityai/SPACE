use super::*;
use crate::ECW_ENVD_PORT;
use axum::Router;
use axum::extract::Extension;
use axum::middleware;
use axum::routing::{get, post};
use http_body::Body as _;
use http_body_util::BodyExt;
use tower::ServiceExt;

const ECW_HOST: &str = "ecw.test";

fn ecw_hosts() -> EcwHostConfig {
    EcwHostConfig::from_env_value(Some(&format!(r#"[{{"host":"{ECW_HOST}"}}]"#)))
}

fn sandbox_host(port: u32) -> String {
    format!("{port}-{}.{ECW_HOST}", SandboxId::new())
}

fn headers_with_host(host: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Some(host) = host {
        headers.insert(axum::http::header::HOST, host.parse().unwrap());
    }
    headers
}

fn observed(router: Router) -> Router {
    router.layer(middleware::from_fn_with_state(
        Arc::new(ecw_hosts()),
        observe_http_traffic,
    ))
}

#[test]
fn routes_classify_by_surface_with_service_ports_decided_by_host() {
    let hosts = ecw_hosts();
    let envd = sandbox_host(ECW_ENVD_PORT);
    let service = sandbox_host(8080);
    for (host, path, expected) in [
        (Some(envd.as_str()), "/files", HttpRouteClass::Files),
        (None, "/files", HttpRouteClass::Files),
        (
            Some(envd.as_str()),
            "/filesystem.Filesystem/ListDir",
            HttpRouteClass::Filesystem,
        ),
        (
            Some(envd.as_str()),
            "/process.Process/Start",
            HttpRouteClass::Process,
        ),
        (
            None,
            "/v1/sessions/abc/events",
            HttpRouteClass::NativeSession,
        ),
        (None, "/healthz", HttpRouteClass::Health),
        (None, "/readyz", HttpRouteClass::Health),
        (Some(envd.as_str()), "/health", HttpRouteClass::Health),
        (Some(envd.as_str()), "/files/compose", HttpRouteClass::Other),
        (Some(envd.as_str()), "/metrics", HttpRouteClass::Other),
        // A service port forwards every path, including ones envd also serves.
        (
            Some(service.as_str()),
            "/files",
            HttpRouteClass::ServicePort,
        ),
        (Some(service.as_str()), "/", HttpRouteClass::ServicePort),
    ] {
        assert_eq!(
            classify_route(&hosts, &headers_with_host(host), path),
            expected,
            "{host:?} {path}",
        );
    }
}

/// The layer is transparent: the handler reads the whole request body, the
/// client gets the whole response with its status and headers, and a `files`
/// handler sees exactly how many body bytes it read.
#[tokio::test]
async fn wrapping_passes_bodies_through_and_reports_bytes_read_to_files_handlers() {
    let router = observed(Router::new().route(
        "/files",
        post(
            |Extension(request_bytes): Extension<RequestBodyBytes>, body: Body| async move {
                let received = axum::body::to_bytes(body, usize::MAX).await.unwrap();
                assert_eq!(received.len(), 1234);
                assert_eq!(request_bytes.get(), 1234);
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    [(axum::http::header::RETRY_AFTER, "3")],
                    "x".repeat(7),
                )
            },
        ),
    ));
    let response = router
        .oneshot(
            Request::post("/files")
                .body(Body::from(vec![0_u8; 1234]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "3");
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.len(), 7);
}

#[tokio::test]
async fn request_body_handle_is_only_offered_to_files_requests() {
    let router = observed(Router::new().route(
        "/process.Process/List",
        post(
            |request_bytes: Option<Extension<RequestBodyBytes>>| async move {
                assert!(request_bytes.is_none());
            },
        ),
    ));
    let response = router
        .oneshot(
            Request::post("/process.Process/List")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn wrapping_keeps_an_exact_response_size_hint() {
    let router = observed(Router::new().route("/healthz", get(|| async { "ok" })));
    let response = router
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    // Losing the exact hint would make hyper send chunked encoding instead of
    // Content-Length.
    assert_eq!(response.body().size_hint().exact(), Some(2));
}

#[test]
fn in_flight_uploads_report_count_and_reserved_bytes() {
    let tracker = Arc::new(UploadTracker::default());
    let first = tracker.begin(100, SandboxId::new(), None, None);
    let second = tracker.begin(
        40,
        SandboxId::new(),
        Some(OrgId::new()),
        Some(RequestBodyBytes::default()),
    );
    assert_eq!(tracker.snapshot(), (2, 140));

    second.finish(StatusCode::OK);
    assert_eq!(tracker.snapshot(), (1, 100));
    // Dropping without `finish` is a cancelled upload; it still releases.
    drop(first);
    assert_eq!(tracker.snapshot(), (0, 0));
}
