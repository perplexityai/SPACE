use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
    routing::on,
};
use http_body_util::BodyExt;
use pplx_space_public_api::routes::{self as native, NATIVE_V1_OPERATIONS};
use std::collections::BTreeSet;
use tower::ServiceExt;

use super::method_filter;

#[tokio::test]
async fn public_get_route_metadata_preserves_implicit_head_support() {
    async fn h() -> &'static str {
        "response body"
    }
    let router = Router::new()
        .route(
            native::LIST_PROCESSES.relative_runtime_path(),
            on(method_filter(native::LIST_PROCESSES.method()), h),
        )
        .route(
            native::GET_PROCESS.relative_runtime_path(),
            on(method_filter(native::GET_PROCESS.method()), h),
        );

    for path in [
        "/sandboxes/sandbox-1/processes",
        "/sandboxes/sandbox-1/processes/process-1",
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri(path)
                    .body(Body::empty())
                    .expect("HEAD request is valid"),
            )
            .await
            .expect("HEAD request completes");

        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert!(
            response
                .into_body()
                .collect()
                .await
                .expect("HEAD response body is readable")
                .to_bytes()
                .is_empty(),
            "{path} returned a body for HEAD"
        );
    }
}

#[test]
fn public_route_patterns_are_valid() {
    async fn h() -> &'static str {
        ""
    }
    let routes = NATIVE_V1_OPERATIONS
        .iter()
        .map(|operation| (operation.relative_runtime_path(), operation.method()))
        .collect::<BTreeSet<_>>();
    let router = routes
        .into_iter()
        .fold(Router::new(), |router, (path, method)| {
            router.route(path, on(method_filter(method), h))
        });
    let _router: Router = Router::new().nest("/v1", router);
}

/// The manifest is only fail-closed if the router serves every control-plane
/// operation the manifest classifies, and classifies every operation the router
/// serves. Without this the public contract can advertise an operation that
/// 404s, or serve one that no M1 status applies to.
#[test]
fn every_control_plane_operation_is_mounted() {
    let mounted = super::mounted_operations()
        .into_iter()
        .map(|(operation, _)| operation)
        .collect::<BTreeSet<_>>();
    let classified = NATIVE_V1_OPERATIONS
        .iter()
        .copied()
        .filter(|operation| operation.surface() == native::NativeSurface::ControlPlane)
        .collect::<BTreeSet<_>>();

    assert_eq!(mounted, classified);
}

#[test]
fn public_network_policy_resource_is_read_only() {
    let network_policy_methods = NATIVE_V1_OPERATIONS
        .iter()
        .filter(|operation| {
            operation.runtime_path() == native::GET_SANDBOX_NETWORK_POLICY.runtime_path()
        })
        .map(|operation| operation.method())
        .collect::<BTreeSet<_>>();

    assert_eq!(
        network_policy_methods,
        BTreeSet::from([native::NativeHttpMethod::Get])
    );
}

/// Every public translator must keep a contention 503 attributed to this
/// service, not re-label it as a fleet condition.
///
/// One table over all five rather than a test per module, because the branch is
/// identical in each and the fallthrough it guards against is not: `sandboxes`
/// answers a bare 503 with `CapacityUnavailable`/`Cluster` ("the fleet is out of
/// capacity") while the other four answer `Node`/`BackendStatus` ("a node is
/// unhealthy"). Both are wrong for a Postgres lock race, and testing only one
/// leaves the other free to lose the branch in a refactor with the suite green.
///
/// `network_policy` was a sixth row until the removal of NetworkPolicyV2 and its
/// `document::controller_error_to_public` with it. What survives on that
/// resource is a read-only `GET` whose `fetch_sandbox` error maps straight to
/// `PublicApiError::internal()`, so it has no translator to table here.
#[test]
fn every_public_translator_keeps_contention_attributed_to_this_service() {
    use pplx_space_metrics::response::{ResponseKind, ResponseOrigin};

    type Translator = fn(crate::controllers::ControllerError) -> super::error::PublicApiError;

    // (name, translator, and what a *non*-contention 503 becomes on that
    // surface — the fallthrough this branch has to avoid inheriting)
    let translators: [(&str, Translator, axum::http::StatusCode, ResponseOrigin); 5] = [
        (
            "sandboxes",
            crate::routes::public::sandboxes::controller_error,
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            ResponseOrigin::Cluster,
        ),
        (
            "ports",
            crate::routes::public::ports::controller_error,
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            ResponseOrigin::Node,
        ),
        (
            "files",
            crate::routes::public::files::controller_error,
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            ResponseOrigin::Node,
        ),
        (
            "processes",
            crate::routes::public::processes::controller_error,
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            ResponseOrigin::Node,
        ),
        (
            "sessions",
            crate::routes::public::sessions::controller_error,
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            ResponseOrigin::Node,
        ),
    ];

    for (name, translate, fleet_fallthrough_status, fleet_origin) in translators {
        let mut contended = crate::controllers::ControllerError::service_unavailable("contended");
        contended.mark_lock_contention_for_test();
        let (status, origin, kind) = translate(contended).classification();

        assert_eq!(
            status,
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "{name}: contention stays retryable"
        );
        assert_eq!(
            origin,
            ResponseOrigin::Api,
            "{name}: attributed to this service"
        );
        assert_eq!(kind, ResponseKind::Internal, "{name}");

        // The same status *without* the contention kind must land somewhere
        // else, so the branch is doing the discriminating rather than the
        // status code. Each surface's fallthrough differs — five keep the 503
        // and re-attribute it, `network_policy` turns it into a 500 — so the
        // expectation is per-row rather than a shared "untouched".
        let fleet = crate::controllers::ControllerError::service_unavailable("node is gone");
        let (fleet_status, origin, _) = translate(fleet).classification();
        assert_eq!(
            (fleet_status, origin),
            (fleet_fallthrough_status, fleet_origin),
            "{name}: a non-contention 503 must take this surface's own fallthrough"
        );
    }
}

/// The 503 has to carry a `Retry-After`, on the wire, on both surfaces.
///
/// The status code alone tells a caller to come back but not when, so a client
/// with no hint retries on its own schedule — which for the tight ones is
/// immediately, back into the same contended lock. Asserting the constant is
/// set on the struct is not enough: `E2bApiError` renders its own header, and
/// rendered it with a bare `as_secs()` that truncates any sub-second hint to
/// `0`. So this drives `into_response()` and reads the header off the response.
#[test]
fn a_contention_503_carries_retry_after_on_the_wire_on_both_surfaces() {
    use axum::http::header;
    use axum::response::IntoResponse;

    let expected = crate::controllers::substrate::LOCK_CONTENTION_RETRY_AFTER;
    assert!(
        !expected.is_zero(),
        "a zero hint would tell the caller to retry immediately"
    );

    for (surface, response) in [
        (
            "public",
            super::error::PublicApiError::lock_contention().into_response(),
        ),
        (
            "e2b",
            crate::e2b::errors::E2bApiError::lock_contention().into_response(),
        ),
    ] {
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "{surface}"
        );
        let value = response
            .headers()
            .get(header::RETRY_AFTER)
            .unwrap_or_else(|| panic!("{surface}: contention 503 without a Retry-After"))
            .to_str()
            .expect("ascii")
            .parse::<u64>()
            .expect("delay-seconds");
        // The property, not the rounding rule: a whole-second header must
        // never advertise *less* time than the hint it stands for, and never
        // zero. `value >= expected.as_secs()` would not say this — a 1500ms
        // hint truncated to `1` passes that and still undershoots.
        assert!(
            value >= 1 && std::time::Duration::from_secs(value) >= expected,
            "{surface}: Retry-After {value}s undershoots the {expected:?} hint"
        );
    }
}
