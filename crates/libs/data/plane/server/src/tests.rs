use super::*;
use crate::{
    native::{ProcessReplayState, first_frame_attach_token_from_recv},
    runtime_route::sweep_expired_ecw_routes,
};
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::Ed25519KeyPair;
use pplx_space_common_core::{
    host::disk_admission::CacheDiskAdmissionReason,
    types::{
        ClusterId, EnvMap, ExecutionGeneration, GuestPort, NodeId, OrgId, PodUid, RouteGeneration,
        SandboxActivationAttemptId, SandboxId, SandboxRouteIdentity, SimpleDuration,
        SpaceApiRouteIdentity,
    },
};
use pplx_space_common_svc::space_api_route::{
    RouteIdentityRemediation, SpaceApiRouteIdentityDimension, SpaceApiRouteProtocol,
    set_route_identity_rejection_metadata,
};
use pplx_space_common_svc::space_api_route_ed25519::{
    SpaceApiRouteEd25519Claims, SpaceApiRouteEd25519Issuer, SpaceApiRouteEd25519KeyId,
    SpaceApiRouteEd25519Signer, SpaceApiRouteEd25519Token,
};
use pplx_space_net_gateway_client::proto::{
    ServicePortIngressFrame, ServicePortIngressStart, service_port_ingress_frame,
};
use pplx_space_node_api_client::proto::{
    DataPlaneBackendEndpoint, ResolveDataPlaneSessionRouteResponse,
    SandboxServiceRouteCredentialKind, SandboxServiceRouteCredentialProof, SandboxServiceRouteKind,
    SandboxServiceRouteTransport, SpaceApiRouteToken as ProtoSpaceApiRouteToken,
    UnstableResolveSandboxServiceRouteRequest, UnstableResolveSandboxServiceRouteResponse,
};
use pplx_space_sbx_mgr_client::proto::{
    SpacedProcessExit, SpacedProcessExited, SpacedProcessOutput, spaced_process_event,
    spaced_process_exit,
};
use std::borrow::Cow;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::wrappers::ReceiverStream;
use tower::ServiceExt;

const TEST_ROUTE_TOKEN: &str = "ed25519.test.cGF5bG9hZA.c2lnbmF0dXJl";

#[tokio::test]
async fn unsupported_envd_surfaces_return_explicit_ecw_errors() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let app = router(test_data_plane_state().await);
    for endpoint in ["/metrics", "/envs", "/files/compose"] {
        let response = app
            .clone()
            .oneshot(Request::get(endpoint).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED, "{endpoint}");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({
                "code": 501,
                "message": "This E2B envd endpoint is not supported by SPACE ECW"
            }),
            "{endpoint}",
        );
    }
}

#[tokio::test]
async fn unsupported_envd_method_returns_explicit_ecw_error() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let response = router(test_data_plane_state().await)
        .oneshot(Request::delete("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({
            "code": 405,
            "message": "This HTTP method is not supported for the requested E2B envd endpoint"
        })
    );
}

/// Both proxy routes must reach the handler through the router. The subpath
/// mount captures `session_id` and `path`; a scalar extractor rejects it with a
/// 500 before authentication, so an unauthenticated request must surface the
/// handler's 401 on every route shape.
#[tokio::test]
async fn proxy_session_routes_extract_path_parameters() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let app = router(test_data_plane_state().await);
    let session_id = DataPlaneSessionId::Port(Default::default());
    for path in [
        format!("/v1/sessions/{session_id}/proxy"),
        format!("/v1/sessions/{session_id}/proxy/api/v1/healthz"),
        format!("/v1/sessions/{session_id}/proxy/assets/app.js?v=1"),
    ] {
        let response = app
            .clone()
            .oneshot(Request::get(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(body["message"], "data-plane token is required", "{path}");
    }
}

pub(crate) fn backend_endpoint(value: &str) -> BackendEndpoint {
    value.parse().expect("valid backend endpoint")
}

fn origin_for_target(
    target: EcwSandboxHost,
    traffic_token_sha256: Option<String>,
) -> EcwRouteOrigin {
    EcwRouteOrigin {
        target,
        transport: runtime_route::EcwRouteTransport::Direct,
        envd_access_token_sha256: None,
        traffic_access_token_sha256: traffic_token_sha256,
        api_key_sha256: None,
    }
}

pub(crate) fn test_ecw_route_origin(sandbox_id: SandboxId) -> EcwRouteOrigin {
    EcwRouteOrigin {
        target: EcwSandboxHost {
            port: ECW_ENVD_PORT,
            sandbox_id,
        },
        transport: runtime_route::EcwRouteTransport::Direct,
        envd_access_token_sha256: None,
        traffic_access_token_sha256: None,
        api_key_sha256: None,
    }
}

pub(crate) fn ecw_resolved_route(
    sandbox_id: SandboxId,
    route_generation: u64,
    backend_endpoint: BackendEndpoint,
) -> EcwResolvedRoute {
    EcwResolvedRoute {
        cache_epoch: route_generation,
        origin: test_ecw_route_origin(sandbox_id),
        sandbox_id,
        org_id: Some(OrgId::new()),
        template_id: Some("seed".to_string()),
        route_generation,
        current_node_id: NodeId::new(),
        backend_endpoint,
        space_api_route_token: None,
        service_port_ingress_token: None,
    }
}

fn test_signer() -> SpaceApiRouteEd25519Signer {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    SpaceApiRouteEd25519Signer::from_pkcs8(
        SpaceApiRouteEd25519KeyId::parse("test-key").unwrap(),
        pkcs8.as_ref(),
    )
    .unwrap()
}

fn route_token_state_for_attempt(
    activation_attempt_id: SandboxActivationAttemptId,
) -> SpaceApiRouteTokenState {
    SpaceApiRouteTokenState {
        token: SpaceApiRouteEd25519Token::parse(TEST_ROUTE_TOKEN).unwrap(),
        expires_at: tokio::time::Instant::now() + Duration::from_secs(120),
        route_identity: SpaceApiRouteIdentity {
            activation_attempt_id: Some(activation_attempt_id),
            ..Default::default()
        },
    }
}

/// A caller rejected for a stale identity must not evict a route that another
/// request already refreshed, or one slow request turns into a resolve storm for
/// everyone sharing the slot.
#[test]
fn stale_caller_cannot_evict_an_already_refreshed_route() {
    let cache = RouteCache::default();
    let sandbox_id = SandboxId::new();
    let key = "ecw:test-slot".to_string();

    let mut stale = ecw_resolved_route(sandbox_id, 1, backend_endpoint("192.0.2.1:9800"));
    let mut refreshed = ecw_resolved_route(sandbox_id, 2, backend_endpoint("192.0.2.2:9800"));
    stale.space_api_route_token = Some(route_token_state_for_attempt(
        SandboxActivationAttemptId::new(),
    ));
    refreshed.space_api_route_token = Some(route_token_state_for_attempt(
        SandboxActivationAttemptId::new(),
    ));

    cache.remember_ecw(key.clone(), refreshed.clone(), Duration::from_secs(60));

    assert!(
        !cache.forget_ecw_if_current(&key, stale.cache_epoch),
        "rejected identity must not evict a different cached identity"
    );
    match cache.lookup_ecw(&key) {
        RouteCacheLookup::Hit(current) => assert_eq!(current.route_generation, 2),
        other => panic!("refreshed route should still be cached, got {other:?}"),
    }

    assert!(
        cache.forget_ecw_if_current(&key, refreshed.cache_epoch),
        "the identity actually cached must be evictable"
    );
    assert!(matches!(cache.lookup_ecw(&key), RouteCacheLookup::Miss));
}

#[test]
fn tokenless_stale_ecw_route_cannot_evict_a_newer_cache_generation() {
    let cache = RouteCache::default();
    let sandbox_id = SandboxId::new();
    let key = "ecw:tokenless-slot".to_string();
    let stale = ecw_resolved_route(sandbox_id, 1, backend_endpoint("192.0.2.1:9800"));
    let refreshed = ecw_resolved_route(sandbox_id, 2, backend_endpoint("192.0.2.2:9800"));

    cache.remember_ecw(key.clone(), refreshed, Duration::from_secs(60));

    assert!(
        !cache.forget_ecw_if_current(&key, stale.cache_epoch),
        "a tokenless stale handle must not evict a newer cached route"
    );
    assert!(matches!(cache.lookup_ecw(&key), RouteCacheLookup::Hit(_)));
}

/// Native sessions update only the authenticated slot that produced the stale
/// handle while retaining an O(1) latest-route view for refresh waiters.
#[test]
fn stale_native_caller_only_replaces_its_own_generation() {
    let cache = RouteCache::default();
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let key = native_route_cache_key(session_id, "token-sha", "connect");

    let mut stale = attached_session(session_id, 1);
    stale.cache_epoch = cache.next_cache_epoch();
    stale.space_api_route_token = Some(route_token_state_for_attempt(
        SandboxActivationAttemptId::new(),
    ));
    let mut refreshed = attached_session(session_id, 2);
    refreshed.cache_epoch = cache.next_cache_epoch();
    refreshed.cache_key = Some(key.clone());
    refreshed.space_api_route_token = Some(route_token_state_for_attempt(
        SandboxActivationAttemptId::new(),
    ));
    cache.remember_native(key.clone(), refreshed.clone(), Duration::from_secs(60));
    let mut replacement = attached_session(session_id, 3);
    replacement.cache_epoch = cache.next_cache_epoch();
    replacement.space_api_route_token = Some(route_token_state_for_attempt(
        SandboxActivationAttemptId::new(),
    ));

    assert!(
        !cache.replace_native_route_if_live(&key, stale.route_identity(), replacement.clone(),),
        "a stale caller must not replace a session with a different identity"
    );
    assert_eq!(
        cache
            .newer_native_session(session_id, stale.route_identity())
            .map(|session| session.route_generation),
        Some(2),
        "a waiter must be able to reuse the newer route instead of resolving again"
    );
    assert!(matches!(
        cache.lookup_native(&key),
        RouteCacheLookup::Hit(_)
    ));

    assert!(cache.replace_native_route_if_live(&key, refreshed.route_identity(), replacement,));
    match cache.lookup_native(&key) {
        RouteCacheLookup::Hit(current) => assert_eq!(current.route_generation, 3),
        other => panic!("replacement route should remain cached, got {other:?}"),
    }
}

fn activation_attempt_rejection() -> tonic::Status {
    let mut status = tonic::Status::permission_denied("stale activation attempt");
    set_route_identity_rejection_metadata(
        &mut status,
        RouteIdentityRemediation::RefreshRoute,
        Some(SpaceApiRouteIdentityDimension::ActivationAttemptId),
    );
    status
}

#[test]
fn native_ingress_preserves_refreshable_backend_rejections() {
    let error =
        crate::native::native_service_port_ingress_open_error(ServicePortIngressOpenError {
            error: EcwProxyError::sandbox_service_unreachable(),
            reason: ServicePortIngressOpenReason::OpenError,
            transport_status: Some(activation_attempt_rejection()),
        });

    assert!(
        matches!(error, StreamBridgeError::BackendStatus(ref status) if runtime_route::is_refreshable_route_identity_rejection(status)),
        "the native route wrapper must see the backend's structured refresh signal"
    );
}

#[tokio::test]
async fn ecw_route_call_retries_once_on_the_newer_cached_identity() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    for (refreshable, dimension, retry_fails) in [
        (
            true,
            Some(SpaceApiRouteIdentityDimension::ActivationAttemptId),
            false,
        ),
        (true, None, false), // Expiry has no mismatched identity dimension.
        (true, None, true),
        (false, None, false),
    ] {
        let state = test_data_plane_state().await;
        let sandbox_id = SandboxId::new();
        let stale = ecw_resolved_route(sandbox_id, 1, backend_endpoint("10.0.0.1:9800"));
        let fresh = ecw_resolved_route(sandbox_id, 2, backend_endpoint("10.0.0.2:9800"));
        state
            .route_cache
            .remember_ecw(stale.origin.cache_key(), fresh, Duration::from_secs(60));
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let result = runtime_route::bounded_ecw_route_call_with_timeout_observer(
            &state,
            stale,
            "test_route_refresh",
            Duration::from_secs(1),
            None,
            |route, _remaining| {
                let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::future::ready(if attempt == 0 || retry_fails {
                    let mut status = tonic::Status::permission_denied("route token rejected");
                    if refreshable {
                        set_route_identity_rejection_metadata(
                            &mut status,
                            RouteIdentityRemediation::RefreshRoute,
                            dimension,
                        );
                    }
                    Err(status)
                } else {
                    Ok(route.route_generation)
                })
            },
            ecw_connect::tonic_status_to_ecw_error,
        )
        .await;
        if refreshable && !retry_fails {
            let (generation, route) =
                result.expect("refreshable rejection retries on the newer route");
            assert_eq!(generation, 2);
            assert_eq!(route.route_generation, 2);
        } else {
            assert_eq!(result.unwrap_err().status, StatusCode::FORBIDDEN);
        }
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            if refreshable { 2 } else { 1 }
        );
    }
}

#[tokio::test]
async fn ecw_route_refresh_reuses_a_newer_install_with_the_same_identity() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let state = test_data_plane_state().await;
    let sandbox_id = SandboxId::new();
    let route_token = route_token_state_for_attempt(SandboxActivationAttemptId::new());
    let mut stale = ecw_resolved_route(sandbox_id, 1, backend_endpoint("10.0.0.1:9800"));
    stale.cache_epoch = state.route_cache.next_cache_epoch();
    stale.space_api_route_token = Some(route_token.clone());
    let mut renewed = ecw_resolved_route(sandbox_id, 1, backend_endpoint("10.0.0.2:9800"));
    renewed.cache_epoch = state.route_cache.next_cache_epoch();
    renewed.space_api_route_token = Some(route_token);
    state.route_cache.remember_ecw(
        stale.origin.cache_key(),
        renewed.clone(),
        Duration::from_secs(60),
    );
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let (endpoint, route) = runtime_route::bounded_ecw_route_call_with_timeout_observer(
        &state,
        stale,
        "test_route_token_renewal",
        Duration::from_secs(1),
        None,
        {
            let attempts = Arc::clone(&attempts);
            move |route, _remaining| {
                let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::future::ready(if attempt == 0 {
                    Err(activation_attempt_rejection())
                } else {
                    Ok(route.backend_endpoint.to_string())
                })
            }
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await
    .expect("a waiter must reuse the already renewed cache install");

    assert_eq!(endpoint, renewed.backend_endpoint.to_string());
    assert_eq!(route.cache_epoch, renewed.cache_epoch);
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn ecw_route_call_refreshes_aging_credentials_before_opening_backend() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    // A process stream retains its route across arbitrary gaps before reattach.
    // The cache may contain a newer install whose credential has also aged.
    for (remaining_secs, cached_remaining_secs, expected) in [
        (120, 120, Some(1)),
        (30, 120, Some(2)),
        (0, 120, Some(2)),
        (0, 30, None),
        (0, 0, None),
    ] {
        for ingress in [false, true] {
            let state = test_data_plane_state().await;
            let mut held =
                ecw_resolved_route(SandboxId::new(), 1, backend_endpoint("10.0.0.1:9800"));
            let mut token = route_token_state_for_attempt(SandboxActivationAttemptId::new());
            token.expires_at = tokio::time::Instant::now() + Duration::from_secs(remaining_secs);
            if ingress {
                held.service_port_ingress_token = Some(token.clone());
            } else {
                held.space_api_route_token = Some(token.clone());
            }
            let mut cached = held.clone();
            cached.cache_epoch = 2;
            token.expires_at =
                tokio::time::Instant::now() + Duration::from_secs(cached_remaining_secs);
            if ingress {
                cached.service_port_ingress_token = Some(token);
            } else {
                cached.space_api_route_token = Some(token);
            }
            state.route_cache.remember_ecw(
                held.origin.cache_key(),
                cached,
                Duration::from_secs(60),
            );
            let calls = std::sync::atomic::AtomicUsize::new(0);
            let result = runtime_route::bounded_ecw_route_call_with_timeout_observer(
                &state,
                held,
                "process_start_open",
                Duration::from_millis(100),
                None,
                |route, _remaining| {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    std::future::ready(Ok::<_, tonic::Status>(route.cache_epoch))
                },
                ecw_connect::tonic_status_to_ecw_error,
            )
            .await;
            match expected {
                Some(epoch) => {
                    let (used_epoch, _) = result.expect("usable credential opens backend");
                    assert_eq!(
                        used_epoch, epoch,
                        "remaining={remaining_secs}, ingress={ingress}"
                    );
                    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
                }
                None => {
                    // The test authority is unavailable: fail closed rather than
                    // open the backend with either aging credential.
                    assert!(result.is_err(), "aging cache install must be re-resolved");
                    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
                }
            }
        }
    }
}

static ECW_TIMEOUT_OBSERVER_CALLS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

fn observe_ecw_timeout(_route: &EcwResolvedRoute, _timeout: Duration) {
    ECW_TIMEOUT_OBSERVER_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
}

#[tokio::test]
async fn ecw_route_timeout_invokes_the_callers_observer() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    ECW_TIMEOUT_OBSERVER_CALLS.store(0, std::sync::atomic::Ordering::SeqCst);
    let state = test_data_plane_state().await;
    let route = ecw_resolved_route(SandboxId::new(), 1, backend_endpoint("10.0.0.1:9800"));

    runtime_route::bounded_ecw_route_call_with_timeout_observer(
        &state,
        route,
        "test_observed_timeout",
        Duration::from_millis(1),
        Some(observe_ecw_timeout),
        |_route, _remaining| async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok::<_, tonic::Status>(())
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await
    .expect_err("the route call should time out");

    assert_eq!(
        ECW_TIMEOUT_OBSERVER_CALLS.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

#[tokio::test]
async fn ecw_route_refresh_lock_wait_obeys_the_operation_deadline() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    for aging_token in [false, true] {
        let state = test_data_plane_state().await;
        let mut stale = ecw_resolved_route(SandboxId::new(), 1, backend_endpoint("10.0.0.1:9800"));
        if aging_token {
            let mut token = route_token_state_for_attempt(SandboxActivationAttemptId::new());
            token.expires_at = tokio::time::Instant::now();
            stale.space_api_route_token = Some(token);
        }
        let cache_key = stale.origin.cache_key();
        let held = state.route_cache.acquire_refresh_slot(&cache_key).await;
        let started = tokio::time::Instant::now();
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let error = runtime_route::bounded_ecw_route_call_with_timeout_observer(
            &state,
            stale,
            "test_route_refresh_deadline",
            Duration::from_millis(5),
            None,
            |_route, _remaining| {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::future::ready(Err::<(), _>(activation_attempt_rejection()))
            },
            ecw_connect::tonic_status_to_ecw_error,
        )
        .await
        .expect_err("refresh lock cannot outlive the operation deadline");
        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            usize::from(!aging_token)
        );
        drop(held);
    }
}

#[tokio::test]
async fn native_route_call_retries_only_refreshable_identity_rejections() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let state = test_data_plane_state().await;
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let key = native_route_cache_key(session_id, "token-sha", "connect");
    let mut stale = attached_session(session_id, 1);
    stale.cache_epoch = state.route_cache.next_cache_epoch();
    stale.cache_key = Some(key.clone());
    stale.space_api_route_token = Some(route_token_state_for_attempt(
        SandboxActivationAttemptId::new(),
    ));
    let mut fresh = attached_session(session_id, 2);
    fresh.cache_epoch = state.route_cache.next_cache_epoch();
    fresh.cache_key = Some(key.clone());
    fresh.space_api_route_token = Some(route_token_state_for_attempt(
        SandboxActivationAttemptId::new(),
    ));
    state
        .route_cache
        .remember_native(key, fresh, Duration::from_secs(60));
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let generation = crate::native::native_route_call(&state, &mut stale, {
        let attempts = std::sync::Arc::clone(&attempts);
        move |route| {
            let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::future::ready(if attempt == 0 {
                Err(StreamBridgeError::BackendStatus(
                    activation_attempt_rejection(),
                ))
            } else {
                Ok(route.route_generation)
            })
        }
    })
    .await
    .expect("native operation retries on the latest session route");

    assert_eq!(generation, 2);
    assert_eq!(stale.route_generation, 2);
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);

    attempts.store(0, std::sync::atomic::Ordering::SeqCst);
    let error = crate::native::native_route_call::<(), _, _>(&state, &mut stale, {
        let attempts = std::sync::Arc::clone(&attempts);
        move |_| {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::future::ready(Err(crate::native::native_service_port_ingress_open_error(
                ServicePortIngressOpenError {
                    error: EcwProxyError::sandbox_service_unreachable(),
                    reason: ServicePortIngressOpenReason::GuestConnectionRefused,
                    transport_status: Some(
                        pplx_space_common_grpc::GuestServiceUnavailable.into_status(),
                    ),
                },
            )))
        }
    })
    .await
    .expect_err("guest refusal must reach the public service error mapping");
    assert!(matches!(error, StreamBridgeError::BackendStatus(status)
        if pplx_space_common_grpc::GuestServiceUnavailable::from_status(&status).is_some()));
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(stale.route_generation, 2);
}

#[tokio::test]
async fn native_route_refresh_does_not_extend_authenticated_slot_lifetime() {
    let cache = RouteCache::default();
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let key = native_route_cache_key(session_id, "token-sha", "connect");
    let activation_attempt_id = SandboxActivationAttemptId::new();
    let mut stale = attached_session(session_id, 1);
    stale.space_api_route_token = Some(route_token_state_for_attempt(activation_attempt_id));
    cache.remember_native(key.clone(), stale.clone(), Duration::from_millis(20));

    tokio::time::sleep(Duration::from_millis(10)).await;
    let mut refreshed = attached_session(session_id, 2);
    refreshed.space_api_route_token = Some(route_token_state_for_attempt(
        SandboxActivationAttemptId::new(),
    ));
    cache.replace_native_route_if_live(&key, stale.route_identity(), refreshed);

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(matches!(
        cache.lookup_native(&key),
        RouteCacheLookup::Expired(_)
    ));
    assert!(matches!(cache.lookup_native(&key), RouteCacheLookup::Miss));
}

#[tokio::test]
async fn native_route_refresh_lock_wait_obeys_the_attach_deadline() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let mut state = test_data_plane_state().await;
    state.attach_deadline = Duration::from_millis(5);
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let stale = attached_session(session_id, 1);
    let slot_key = native_route_cache_prefix(session_id);
    let held = state.route_cache.acquire_refresh_slot(&slot_key).await;

    let result = tokio::time::timeout(
        Duration::from_millis(100),
        crate::native::refresh_session_route(&state, &stale),
    )
    .await;

    assert!(
        matches!(result, Ok(Err(StreamBridgeError::ClusterStatus(status))) if status.code() == tonic::Code::DeadlineExceeded),
        "native refresh should report its own deadline instead of hanging on the slot"
    );
    drop(held);
}

#[tokio::test]
async fn filesystem_transfer_budget_does_not_bound_established_transfer() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let state = test_data_plane_state().await;
    let route = ecw_resolved_route(SandboxId::new(), 1, backend_endpoint("10.0.0.1:9800"));

    let (value, _) = runtime_route::bounded_ecw_route_transfer(
        &state,
        route,
        "test_transfer",
        Duration::from_millis(5),
        |_route, _remaining| async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok::<_, tonic::Status>(42)
        },
        ecw_connect::tonic_status_to_ecw_error,
    )
    .await
    .expect("an established transfer may outlive its route-recovery budget");

    assert_eq!(value, 42);
}

/// Refresh slots are keyed per cache entry, so they must be retired once idle
/// rather than accumulating one per sandbox/credential pair ever refreshed.
#[tokio::test]
async fn refresh_slots_do_not_accumulate_once_idle() {
    let cache = RouteCache::default();
    let key = "ecw:test-slot";

    {
        let _slot = cache.acquire_refresh_slot(key).await;
        assert_eq!(cache.refresh_lock_len(), 1);
        // A second caller shares the slot rather than creating its own.
        assert_eq!(cache.refresh_lock_len(), 1);
    }
    assert_eq!(cache.refresh_lock_len(), 0);
}

/// While one caller holds a refresh slot it must retain it, so a concurrent
/// holder cannot retire the entry out from under it.
#[tokio::test]
async fn refresh_slot_remains_registered_while_held() {
    let cache = RouteCache::default();
    let key = "ecw:test-slot";

    let held = cache.acquire_refresh_slot(key).await;
    assert_eq!(cache.refresh_lock_len(), 1);

    drop(held);
    assert_eq!(cache.refresh_lock_len(), 0);
}

#[tokio::test]
async fn cancelled_refresh_waiter_does_not_strand_the_slot() {
    let cache = RouteCache::default();
    let key = "ecw:cancelled-refresh-slot";
    let held = cache.acquire_refresh_slot(key).await;

    assert!(
        tokio::time::timeout(Duration::from_millis(5), cache.acquire_refresh_slot(key))
            .await
            .is_err()
    );
    drop(held);

    assert_eq!(
        cache.refresh_lock_len(),
        0,
        "cancellation must not leave RouteCache cleanup to the caller"
    );
}

#[tokio::test]
async fn expired_native_route_cleanup_removes_the_latest_index_entry() {
    let cache = RouteCache::default();
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let key = native_route_cache_key(session_id, "token-sha", "connect");
    cache.remember_native(
        key.clone(),
        attached_session(session_id, 1),
        Duration::from_millis(1),
    );
    assert_eq!(cache.native_latest_len(), 1);

    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(matches!(
        cache.lookup_native(&key),
        RouteCacheLookup::Expired(_)
    ));
    assert_eq!(
        cache.native_latest_len(),
        0,
        "native expiration must clean the companion latest-route index"
    );
}

#[tokio::test]
async fn native_route_sweep_bounds_both_cache_indexes() {
    let cache = RouteCache::default();
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    cache.remember_native(
        native_route_cache_key(session_id, "token-sha", "connect"),
        attached_session(session_id, 1),
        Duration::from_millis(1),
    );
    tokio::time::sleep(Duration::from_millis(5)).await;

    assert_eq!(cache.sweep_expired_native(1), (1, 1));
    assert_eq!(cache.native_len(), 0);
    assert_eq!(cache.native_latest_len(), 0);
}

/// A gRPC health listener lets the production `cluster::connect` path complete
/// its handshake. Route RPCs return Unimplemented to exercise resolution
/// failures; cached-route tests need only the handshake.
async fn spawn_cluster_api_stub() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind cluster-api stub");
    let addr = listener.local_addr().expect("stub addr");
    let (_reporter, health) = pplx_space_common_svc::bootstrap::grpc_health_service();
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(health)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    addr
}

#[tokio::test]
async fn accepted_connections_detect_dead_peers() {
    use axum::serve::Listener;

    let mut listener =
        data_plane_listener(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
    let address = listener.local_addr().unwrap();
    let _client = tokio::net::TcpStream::connect(address).await.unwrap();
    let (accepted, _peer) = listener.accept().await;
    assert!(accepted.nodelay().unwrap());
    let socket = socket2::SockRef::from(&accepted);
    assert!(socket.keepalive().unwrap());
    assert_eq!(
        socket.tcp_keepalive_time().unwrap(),
        DATA_PLANE_TCP_KEEPALIVE_TIME
    );
    assert_eq!(
        socket.tcp_keepalive_interval().unwrap(),
        DATA_PLANE_TCP_KEEPALIVE_INTERVAL
    );
    assert_eq!(
        socket.tcp_keepalive_retries().unwrap(),
        DATA_PLANE_TCP_KEEPALIVE_RETRIES
    );
    // A user timeout would cap how long a client may stop reading, ECW's
    // included, and on Linux would decide when keepalive gives up.
    #[cfg(target_os = "linux")]
    assert_eq!(socket.tcp_user_timeout().unwrap(), None);
}

pub(crate) async fn test_data_plane_state() -> DataPlaneState {
    let addr = spawn_cluster_api_stub().await;
    let cluster_api =
        pplx_space_node_api_client::cluster::connect(format!("http://{addr}"), "test")
            .await
            .expect("connect to the cluster-api stub");
    DataPlaneState {
        cluster_api: ClusterApiClients::service_only(cluster_api),
        attach_deadline: Duration::from_secs(1),
        unauthenticated: Arc::new(UnauthenticatedUpgradeLimiter::new(1)),
        attached_streams: Arc::new(AttachedStreamLimiter::new(4, 4, 4)),
        uploads: None,
        upload_tracker: Arc::default(),
        native_sessions: Arc::new(NativeSessionAttachLimiter::default()),
        backend_channels: Arc::new(BackendChannelPool::new(1)),
        revocation_cache: Arc::new(RevocationCache::default()),
        route_cache: Arc::new(RouteCache::default()),
        ecw_route_repairs: Arc::new(DashMap::new()),
        ecw_process_input_locks: Arc::new(EcwProcessInputLocks::default()),
        ecw_hosts: Arc::default(),
        revocation_check_interval: Duration::from_secs(1),
        revocation_max_staleness: REVOCATION_MAX_STALENESS,
        revocation_check_timeout: REVOCATION_CHECK_TIMEOUT,
        route_cache_ttl: Duration::from_secs(60),
        drain_started: CancellationToken::new(),
        stream_shutdown_token: CancellationToken::new(),
        upgrade_tasks: TaskTracker::new(),
    }
}

/// The static gate and the dynamic flag both have to say yes.
///
/// `None` for the assignment is the no-provider deployment, and it is the row
/// that matters most: this flag declares `false`, so a cluster that configures
/// no flag backend must stay on the Service client rather than inherit the
/// balanced one from its environment alone. The static gate is the presence of
/// the EndpointSlice client, which is the deploy-time endpoint-discovery
/// decision already made.
#[test]
fn cluster_api_transport_requires_both_static_and_dynamic_gates() {
    for (name, endpoint_slice_client, assignment, expected) in [
        ("static gate off", None, Some(true), "service"),
        ("static gate off, no provider", None, None, "service"),
        (
            "dynamic gate off",
            Some("endpoint-slice"),
            Some(false),
            "service",
        ),
        (
            "static gate on, no provider",
            Some("endpoint-slice"),
            None,
            "service",
        ),
        (
            "both gates on",
            Some("endpoint-slice"),
            Some(true),
            "endpoint-slice",
        ),
    ] {
        let feature_flags = match assignment {
            Some(value) => FeatureFlagClient::fixed_bools([(
                SPACE_CLUSTER_API_CLIENT_SIDE_BALANCING_FLAG.key(),
                value,
            )]),
            None => FeatureFlagClient::disabled(),
        };
        let clients = ClusterApiClients::new("service", endpoint_slice_client, feature_flags);
        assert_eq!(
            clients.client_for_subject("sandbox:stable-subject"),
            expected,
            "{name}"
        );
    }
}

fn proto_space_api_route_token() -> ProtoSpaceApiRouteToken {
    proto_space_api_route_token_with_claims(space_api_route_claims())
}

pub(crate) fn proto_service_port_route_token() -> ProtoSpaceApiRouteToken {
    let identity = test_route_identity();
    let claims = SpaceApiRouteEd25519Claims::service_port_ingress(
        SpaceApiRouteEd25519Issuer::parse("cluster-api").unwrap(),
        ClusterId::parse("testing").unwrap(),
        identity,
        GuestPort::try_from_u16(8080).unwrap(),
        SpaceApiRouteProtocol::Tcp,
    )
    .unwrap();
    proto_space_api_route_token_with_claims(claims)
}

fn test_route_identity() -> SandboxRouteIdentity {
    SandboxRouteIdentity::try_new(
        SandboxId::new(),
        RouteGeneration::new(7).unwrap(),
        NodeId::new(),
        pplx_space_common_core::types::NodeManagedBy::Kubernetes,
        ExecutionGeneration::new(11).unwrap(),
        SandboxActivationAttemptId::new(),
        Some(PodUid::parse("sandbox-pod-uid").unwrap()),
    )
    .unwrap()
}

fn space_api_route_claims() -> SpaceApiRouteEd25519Claims {
    SpaceApiRouteEd25519Claims::space_api_bridge(
        SpaceApiRouteEd25519Issuer::parse("cluster-api").unwrap(),
        ClusterId::parse("testing").unwrap(),
        test_route_identity(),
    )
    .unwrap()
}

fn proto_space_api_route_token_with_claims(
    claims: SpaceApiRouteEd25519Claims,
) -> ProtoSpaceApiRouteToken {
    ProtoSpaceApiRouteToken {
        token: test_signer().mint(&claims).unwrap().secret().to_string(),
    }
}

fn encoded_proto_hex<M: prost::Message>(message: &M) -> String {
    hex::encode(message.encode_to_vec())
}

#[test]
fn service_port_ingress_proto_wire_shape_is_pinned() {
    let direct_request = UnstableResolveSandboxServiceRouteRequest {
        sandbox_id: "019f0000-0000-7000-8000-000000000001".to_string(),
        port: 8080,
        route_kind: SandboxServiceRouteKind::ServicePort as i32,
        credential_proofs: vec![SandboxServiceRouteCredentialProof {
            kind: SandboxServiceRouteCredentialKind::SandboxTrafficToken as i32,
            sha256: "traffic-token-sha256".to_string(),
        }],
        transport: SandboxServiceRouteTransport::Direct as i32,
    };
    let ingress_request = UnstableResolveSandboxServiceRouteRequest {
        transport: SandboxServiceRouteTransport::ServicePortIngress as i32,
        ..direct_request.clone()
    };
    let ingress_response = UnstableResolveSandboxServiceRouteResponse {
        sandbox_id: direct_request.sandbox_id.clone(),
        org_id: "019f0000-0000-7000-8000-000000000002".to_string(),
        port: direct_request.port,
        lifecycle: "started".to_string(),
        route_generation: 9,
        current_node_id: "019f0000-0000-7000-8000-000000000003".to_string(),
        backend: Some(DataPlaneBackendEndpoint {
            node_id: "019f0000-0000-7000-8000-000000000003".to_string(),
            endpoint: "http://worker:9800".to_string(),
            transport: None,
        }),
        template_id: Some("seed".to_string()),
        space_api_route_token: Some(ProtoSpaceApiRouteToken {
            token: "bridge-token".to_string(),
        }),
        service_port_ingress_token: Some(ProtoSpaceApiRouteToken {
            token: "ingress-token".to_string(),
        }),
    };
    let ingress_start_frame = ServicePortIngressFrame {
        message: Some(service_port_ingress_frame::Message::Start(
            ServicePortIngressStart {
                sandbox_id: direct_request.sandbox_id.clone(),
                port: direct_request.port,
            },
        )),
    };
    let ingress_data_frame = ServicePortIngressFrame {
        message: Some(service_port_ingress_frame::Message::Data(
            b"GET / HTTP/1.1\r\n\r\n".to_vec(),
        )),
    };

    let snapshot = format!(
        "direct_request: {}\ningress_request: {}\ningress_response: {}\ningress_start_frame: {}\ningress_data_frame: {}",
        encoded_proto_hex(&direct_request),
        encoded_proto_hex(&ingress_request),
        encoded_proto_hex(&ingress_response),
        encoded_proto_hex(&ingress_start_frame),
        encoded_proto_hex(&ingress_data_frame),
    );
    insta::assert_snapshot!("service_port_ingress_proto_wire_shape", snapshot);
}

pub(crate) fn route_response_with_token(
    session_id: DataPlaneSessionId,
    space_api_route_token: Option<ProtoSpaceApiRouteToken>,
) -> ResolveDataPlaneSessionRouteResponse {
    let current_node_id = NodeId::new();
    ResolveDataPlaneSessionRouteResponse {
        session_id: session_id.to_string(),
        protocol_session_id: None,
        route_generation: 7,
        current_node_id: current_node_id.to_string(),
        backend: Some(DataPlaneBackendEndpoint {
            node_id: current_node_id.to_string(),
            endpoint: "http://worker:9800".to_string(),
            transport: None,
        }),
        revocation: Some(DataPlaneRevocationState {
            epoch: 1,
            status: DataPlaneRevocationStatus::Active as i32,
            updated_at: None,
            reason: None,
            org_epoch: 1,
        }),
        sandbox_id: SandboxId::new().to_string(),
        kind: session_id.kind().as_str().to_string(),
        target_port: None,
        target_protocol: None,
        target_path: None,
        adapter_state_json: "{}".to_string(),
        backend_process_id: None,
        org_id: OrgId::new().to_string(),
        space_api_route_token,
    }
}

fn parse_native_proto_space_api_route_token(
    token: ProtoSpaceApiRouteToken,
) -> Option<SpaceApiRouteTokenState> {
    parse_proto_space_api_route_token(SpaceApiRouteTokenRouteSource::DataPlaneSession, token)
}

#[test]
fn cluster_api_startup_retry_is_transport_only() {
    let transport_error =
        tonic::transport::Endpoint::from_shared("http://[::1".to_string()).unwrap_err();
    assert!(cluster_api_connect_error_is_retryable(
        &ConnectClusterApiError::Transport(transport_error)
    ));
    assert!(!cluster_api_connect_error_is_retryable(
        &ConnectClusterApiError::MissingEndpoint
    ));
    assert!(!cluster_api_connect_error_is_retryable(
        &ConnectClusterApiError::MissingToken(
            pplx_space_common_svc::bearer_token::BearerTokenError::Missing {
                path_env: "SPACE_CLUSTER_API_TOKEN_PATH",
                default_path: "/var/run/space-auth/cluster-api.token".into(),
                fallback_env: "SPACE_INTERNAL_TOKEN",
            }
        )
    ));
}

#[tokio::test]
async fn backend_channels_accept_bare_direct_node_endpoints() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let channels = build_backend_channels("192.0.2.111:9800", 2)
        .await
        .expect("bare endpoint");
    let dns_channels = build_backend_channels("worker:9800", 1)
        .await
        .expect("bare dns endpoint");

    assert_eq!(channels.len(), 2);
    assert_eq!(dns_channels.len(), 1);
}

#[tokio::test]
async fn backend_channels_advertise_the_configured_connection_window() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }
    const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
    const FRAME_SETTINGS: u8 = 0x4;
    const FRAME_WINDOW_UPDATE: u8 = 0x8;
    const DEFAULT_WINDOW: u32 = 65_535;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let channel = build_backend_channels(&endpoint, 1)
        .await
        .expect("backend channel")
        .remove(0);
    // A lazy channel dials on its first call; the call itself never completes.
    let call = tokio::spawn(
        channel.oneshot(
            tonic::codegen::http::Request::builder()
                .uri(format!("http://{endpoint}/probe"))
                .body(tonic::body::Body::empty())
                .unwrap(),
        ),
    );

    let increment = tokio::time::timeout(Duration::from_secs(5), async {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut preface = [0; PREFACE.len()];
        socket.read_exact(&mut preface).await.unwrap();
        assert_eq!(preface, PREFACE);
        socket
            .write_all(&[0, 0, 0, FRAME_SETTINGS, 0, 0, 0, 0, 0])
            .await
            .unwrap();
        // The client raises the connection window past the protocol default
        // with a WINDOW_UPDATE on stream 0.
        loop {
            let mut header = [0; 9];
            socket.read_exact(&mut header).await.unwrap();
            let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
            let stream = u32::from_be_bytes([header[5], header[6], header[7], header[8]]);
            let mut payload = vec![0; len];
            socket.read_exact(&mut payload).await.unwrap();
            if header[3] == FRAME_WINDOW_UPDATE && stream & 0x7fff_ffff == 0 {
                break u32::from_be_bytes(payload[..4].try_into().unwrap()) & 0x7fff_ffff;
            }
        }
    })
    .await
    .expect("connection WINDOW_UPDATE");
    call.abort();

    assert_eq!(
        DEFAULT_WINDOW + increment,
        pplx_space_node_api_client::MULTIPLEXED_CONNECTION_WINDOW_BYTES
    );
}

#[tokio::test]
async fn backend_channel_pool_normalizes_cache_key() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let pool = BackendChannelPool::new(1);
    let key = Uuid::now_v7();
    let bare_endpoint = backend_endpoint("192.0.2.111:9800");
    let uri_endpoint = backend_endpoint("http://192.0.2.111:9800");

    let _bare = pool
        .channel_for_key(&bare_endpoint, key)
        .await
        .expect("bare endpoint");
    let _uri = pool
        .channel_for_key(&uri_endpoint, key)
        .await
        .expect("uri endpoint");

    assert_eq!(pool.channels.len(), 1);
}

#[tokio::test]
async fn backend_channel_pool_forgets_normalized_endpoint() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let pool = BackendChannelPool::new(1);
    let bare_endpoint = backend_endpoint("192.0.2.111:9800");
    let uri_endpoint = backend_endpoint("http://192.0.2.111:9800");

    let _channel = pool
        .channel_for_key(&bare_endpoint, Uuid::now_v7())
        .await
        .expect("bare endpoint");

    assert!(pool.forget_endpoint(&uri_endpoint));
    assert!(!pool.forget_endpoint(&bare_endpoint));
}

#[tokio::test]
async fn backend_channel_pool_reuses_endpoint_for_ecw_route_generations() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let state = test_data_plane_state().await;
    let endpoint = backend_endpoint("192.0.2.111:9800");
    let sandbox_id = SandboxId::new();
    let gen_one = ecw_resolved_route(sandbox_id, 1, endpoint.clone());
    let gen_two = ecw_resolved_route(sandbox_id, 2, endpoint.clone());

    let _gen_one = ecw_backend_channel(&state, &gen_one)
        .await
        .expect("route generation 1 endpoint");
    let _gen_two = ecw_backend_channel(&state, &gen_two)
        .await
        .expect("route generation 2 endpoint");

    assert_eq!(state.backend_channels.len(), 1);
}

#[tokio::test]
async fn ecw_transport_reset_removes_only_failed_endpoint() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let state = test_data_plane_state().await;
    let endpoint = backend_endpoint("192.0.2.111:9800");
    let other_endpoint = backend_endpoint("192.0.2.112:9800");
    let route = ecw_resolved_route(SandboxId::new(), 1, endpoint);
    let other_route = ecw_resolved_route(SandboxId::new(), 1, other_endpoint);
    let mut transport_status =
        tonic::Status::unknown("gRPC client transport error before status was returned");
    transport_status.set_source(Arc::new(tonic::ConnectError(Box::new(
        std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset"),
    ))));

    let _route = ecw_backend_channel(&state, &route)
        .await
        .expect("route endpoint");
    let _other = ecw_backend_channel(&state, &other_route)
        .await
        .expect("other endpoint");
    assert_eq!(state.backend_channels.len(), 2);

    assert!(reset_ecw_backend_channel_on_transport_status(
        &state,
        &route,
        &transport_status,
        "test"
    ));
    assert_eq!(state.backend_channels.len(), 1);

    let _other = ecw_backend_channel(&state, &other_route)
        .await
        .expect("other endpoint remains cached");
    assert_eq!(state.backend_channels.len(), 1);

    let _route = ecw_backend_channel(&state, &route)
        .await
        .expect("route endpoint is recreated");
    assert_eq!(state.backend_channels.len(), 2);
}

#[tokio::test]
async fn ecw_transport_failure_after_route_refresh_still_resets_channel() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let state = test_data_plane_state().await;
    let route = ecw_resolved_route(SandboxId::new(), 1, backend_endpoint("192.0.2.111:9800"));
    let mut transport_status = tonic::Status::unknown("transport failed after route refresh");
    transport_status.set_source(Arc::new(tonic::ConnectError(Box::new(
        std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset"),
    ))));
    let _channel = ecw_backend_channel(&state, &route)
        .await
        .expect("route endpoint");

    let should_retry = runtime_route::reset_ecw_transport_channel_and_should_retry(
        &state,
        &route,
        &transport_status,
        "test_after_refresh",
        runtime_route::EcwTransportRetry::Allowed,
        true,
    );

    assert!(
        !should_retry,
        "route refresh already consumed the safe replay"
    );
    assert_eq!(state.backend_channels.len(), 0);
}

#[test]
fn refreshable_backend_stream_status_is_retryable_before_process_binding() {
    assert!(crate::native::backend_stream_status_retryable(
        &activation_attempt_rejection()
    ));
    assert!(!crate::native::backend_stream_status_retryable(
        &tonic::Status::permission_denied("ordinary authorization failure")
    ));
}

#[test]
fn ecw_backend_status_round_trip_preserves_grpc_code_and_metadata() {
    let backpressure = pplx_space_common_grpc::uds_per_sandbox_limit_exceeded_status("busy");
    let round_trip = ecw_connect::tonic_status_to_ecw_error(backpressure).into_tonic_status();

    assert_eq!(round_trip.code(), tonic::Code::ResourceExhausted);
    assert!(pplx_space_common_grpc::UdsPerSandboxLimitExceeded::from_status(&round_trip).is_some());

    let not_found = ecw_connect::tonic_status_to_ecw_error(tonic::Status::not_found("gone"))
        .into_tonic_status();
    assert_eq!(not_found.code(), tonic::Code::NotFound);
}

/// Every 429 a producer builds names its limit, so
/// `data_plane.http.too_many_requests` never has to fall back to `unclassified`
/// for a data-plane rejection.
#[test]
fn data_plane_429s_carry_their_limit_as_a_response_extension() {
    let reason = |response: Response| {
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        response
            .extensions()
            .get::<TooManyRequestsReason>()
            .copied()
    };
    for (status, expected) in [
        (
            pplx_space_common_grpc::uds_per_sandbox_limit_exceeded_status("busy"),
            TooManyRequestsReason::NodeSandboxLimit,
        ),
        (
            pplx_space_common_grpc::refused_before_start(
                tonic::Status::resource_exhausted("full"),
                Duration::from_secs(2),
            ),
            TooManyRequestsReason::SandboxAtCapacity,
        ),
        (
            tonic::Status::resource_exhausted("quota"),
            TooManyRequestsReason::BackendResourceExhausted,
        ),
    ] {
        assert_eq!(
            reason(ecw_connect::tonic_status_to_ecw_error(status).into_response()),
            Some(expected),
        );
    }
    for (limit, expected) in [
        (
            AttachedStreamLimit::Total,
            TooManyRequestsReason::AttachedStreams,
        ),
        (
            AttachedStreamLimit::Org,
            TooManyRequestsReason::AttachedStreamsPerOrg,
        ),
        (
            AttachedStreamLimit::Sandbox,
            TooManyRequestsReason::AttachedStreamsPerSandbox,
        ),
    ] {
        assert_eq!(
            reason(native::too_many_attached_streams_response(limit)),
            Some(expected)
        );
    }
}

#[tokio::test]
async fn backend_channel_pool_acquisition_runs_throttled_idle_sweep() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let pool = BackendChannelPool::new(1);
    let stale_endpoint = backend_endpoint("192.0.2.111:9800");
    let recent_endpoint = backend_endpoint("192.0.2.112:9800");
    let key = Uuid::now_v7();

    let _stale = pool
        .channel_for_key(&stale_endpoint, key)
        .await
        .expect("stale endpoint");
    pool.mark_endpoint_idle_for(&stale_endpoint, Duration::from_secs(31 * 60));

    let _recent = pool
        .channel_for_key(&recent_endpoint, key)
        .await
        .expect("recent endpoint before sweep is due");
    assert_eq!(pool.channels.len(), 2);

    pool.mark_idle_sweep_due(Duration::from_secs(60));
    let _recent = pool
        .channel_for_key(&recent_endpoint, key)
        .await
        .expect("recent endpoint triggers idle sweep");

    assert_eq!(pool.channels.len(), 1);
    assert!(!pool.forget_endpoint(&stale_endpoint));
    assert!(pool.forget_endpoint(&recent_endpoint));
}

#[tokio::test]
async fn expired_ecw_route_sweep_does_not_reclaim_backend_channel() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let state = test_data_plane_state().await;
    let target = EcwSandboxHost {
        sandbox_id: SandboxId::new(),
        port: 8080,
    };
    let endpoint = backend_endpoint("192.0.2.111:9800");
    let route = ecw_resolved_route(target.sandbox_id, 9, endpoint.clone());
    let key = ecw_route_cache_key(
        &target,
        EcwRouteTransport::ServicePortIngress,
        Some("envd-token-sha"),
        Some("traffic-token-sha"),
        Some("api-key-sha"),
    );

    let _channel = ecw_backend_channel(&state, &route)
        .await
        .expect("ECW backend channel");
    assert_eq!(state.backend_channels.len(), 1);

    state.route_cache.remember_ecw(key, route, Duration::ZERO);
    state
        .route_cache
        .mark_ecw_sweep_due(Duration::from_secs(60));
    sweep_expired_ecw_routes(&state);

    assert_eq!(state.route_cache.ecw_len(), 0);
    assert_eq!(state.backend_channels.len(), 1);
}

#[test]
fn backend_transport_reset_reason_is_transport_only() {
    let mut transport_status =
        tonic::Status::unknown("gRPC client transport error before status was returned");
    transport_status.set_source(Arc::new(tonic::ConnectError(Box::new(
        std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset"),
    ))));

    assert_eq!(
        backend_transport_reset_reason(&transport_status),
        Some(ReconnectReason::WorkerUnavailable)
    );
    assert_eq!(
        backend_transport_reset_reason(&tonic::Status::unavailable(
            "spaced-host unavailable: socket missing"
        )),
        None
    );
    assert_eq!(
        backend_transport_reset_reason(&tonic::Status::cancelled("node-api draining")),
        None
    );
    assert_eq!(
        backend_transport_reset_reason(&tonic::Status::failed_precondition(
            "sandbox service route is not ready"
        )),
        None
    );
    assert_eq!(
        backend_transport_reset_reason(&tonic::Status::unknown("guest connection closed")),
        None
    );
}

#[test]
fn ecw_generated_errors_carry_response_origin() {
    let backend_error =
        ecw_connect::tonic_status_to_ecw_error(tonic::Status::internal("guest failed"));
    assert_eq!(backend_error.origin, ResponseOrigin::Spaced);
    assert_eq!(backend_error.kind, ResponseKind::BackendStatus);

    let timeout_error = EcwProxyError::sandbox_timeout()
        .with_classification(ResponseOrigin::Node, ResponseKind::Timeout);
    assert_eq!(timeout_error.origin, ResponseOrigin::Node);
    assert_eq!(timeout_error.kind, ResponseKind::Timeout);
}

#[test]
fn ecw_sandbox_service_transport_errors_are_sandbox_traffic() {
    let error = EcwProxyError::sandbox_service_unreachable();

    assert_eq!(error.status, StatusCode::BAD_GATEWAY);
    assert_eq!(error.message, "Sandbox service is not reachable");
    assert_eq!(error.origin, ResponseOrigin::SandboxTraffic);
    assert_eq!(error.kind, ResponseKind::BackendTransport);
    assert!(!ecw_error_can_use_runtime_access_decision(&error));
}

#[test]
fn ecw_sandbox_service_route_repair_cooldown_coalesces_and_expires() {
    let route_repairs = DashMap::new();
    let repair_key = "sandbox:8080:7".to_string();
    let now = tokio::time::Instant::now();
    let cooldown = Duration::from_secs(30);

    assert_eq!(
        claim_ecw_sandbox_service_route_repair(&route_repairs, repair_key.clone(), now, cooldown),
        EcwRouteRepairSchedule::Enqueue
    );

    assert_eq!(
        claim_ecw_sandbox_service_route_repair(
            &route_repairs,
            repair_key.clone(),
            now + Duration::from_secs(5),
            cooldown
        ),
        EcwRouteRepairSchedule::Coalesce {
            cooldown_remaining: Duration::from_secs(25)
        }
    );

    assert_eq!(
        claim_ecw_sandbox_service_route_repair(
            &route_repairs,
            repair_key,
            now + Duration::from_secs(31),
            cooldown
        ),
        EcwRouteRepairSchedule::Enqueue
    );
}

#[tokio::test]
async fn native_route_resolution_error_response_shape_is_stable() {
    let cases = [
        (
            RouteResolutionError::Unauthenticated,
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "invalid or expired data-plane token",
            false,
        ),
        (
            RouteResolutionError::Forbidden,
            StatusCode::FORBIDDEN,
            "forbidden",
            "data-plane token does not allow this session",
            false,
        ),
        (
            RouteResolutionError::SessionNotFound,
            StatusCode::NOT_FOUND,
            "not_found",
            "session not found",
            false,
        ),
        (
            RouteResolutionError::SessionNotReady,
            StatusCode::SERVICE_UNAVAILABLE,
            "not_ready",
            "session route is not ready",
            true,
        ),
        (
            RouteResolutionError::ResolutionFailed,
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            "session route could not be resolved",
            true,
        ),
        (
            RouteResolutionError::RouteStateInvalid,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "session route is invalid",
            false,
        ),
    ];

    for (error, status, code, message, retryable) in cases {
        let response = ProxyError::Route(error).into_response();
        assert_eq!(response.status(), status);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "code": code,
                "message": message,
                "retryable": retryable,
            })
        );
    }
}

#[test]
fn attached_stream_limiter_caps_and_releases() {
    let limiter = Arc::new(AttachedStreamLimiter::new(2, 2, 1));
    let terminal_id = DataPlaneSessionId::new_for_kind(SessionKind::Terminal);
    let process_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let org_1 = OrgId::new();
    let org_2 = OrgId::new();
    let sandbox_1 = SandboxId::new();
    let sandbox_2 = SandboxId::new();
    let sandbox_3 = SandboxId::new();
    let mut terminal_session = attached_session(terminal_id, 1);
    terminal_session.org_id = org_1;
    terminal_session.sandbox_id = sandbox_1;
    let mut process_session = attached_session(process_id, 1);
    process_session.org_id = org_1;
    process_session.sandbox_id = sandbox_2;
    let mut same_sandbox_session =
        attached_session(DataPlaneSessionId::new_for_kind(SessionKind::FileWatch), 1);
    same_sandbox_session.org_id = org_1;
    same_sandbox_session.sandbox_id = sandbox_1;
    let mut third_org_session =
        attached_session(DataPlaneSessionId::new_for_kind(SessionKind::FileWatch), 1);
    third_org_session.org_id = org_2;
    third_org_session.sandbox_id = sandbox_3;

    let terminal = limiter
        .try_acquire_attached(&terminal_session)
        .expect("first stream");
    assert!(matches!(
        limiter.try_acquire_attached(&same_sandbox_session),
        Err(AttachedStreamLimit::Sandbox)
    ));
    let process = limiter
        .try_acquire_attached(&process_session)
        .expect("second stream");

    assert!(matches!(
        limiter.try_acquire_attached(&third_org_session),
        Err(AttachedStreamLimit::Total)
    ));
    assert_eq!(limiter.active_total(), 2);
    assert_eq!(limiter.active_kind(SessionKind::Terminal), 1);
    assert_eq!(limiter.active_kind(SessionKind::Process), 1);
    assert_eq!(limiter.active_org(org_1), 2);
    assert_eq!(limiter.active_sandbox(sandbox_1), 1);

    drop(terminal);

    assert_eq!(limiter.active_total(), 1);
    assert_eq!(limiter.active_kind(SessionKind::Terminal), 0);
    assert_eq!(limiter.active_org(org_1), 1);
    assert_eq!(limiter.active_sandbox(sandbox_1), 0);
    let ecw_service = limiter
        .try_acquire_label("ecw_service")
        .expect("released slot");
    assert_eq!(limiter.active_total(), 2);
    assert_eq!(limiter.active_label("ecw_service"), 1);

    drop(process);
    drop(ecw_service);

    assert_eq!(limiter.active_total(), 0);
    assert_eq!(limiter.active_kind(SessionKind::Process), 0);
    assert_eq!(limiter.active_label("ecw_service"), 0);
    assert_eq!(limiter.tracked_orgs(), 0);
    assert_eq!(limiter.tracked_sandboxes(), 0);

    let org_limiter = Arc::new(AttachedStreamLimiter::new(3, 1, 3));
    let org_cap = OrgId::new();
    let mut first_org_session =
        attached_session(DataPlaneSessionId::new_for_kind(SessionKind::Terminal), 1);
    first_org_session.org_id = org_cap;
    first_org_session.sandbox_id = SandboxId::new();
    let mut second_org_session =
        attached_session(DataPlaneSessionId::new_for_kind(SessionKind::FileWatch), 1);
    second_org_session.org_id = org_cap;
    second_org_session.sandbox_id = SandboxId::new();
    let _permit = org_limiter
        .try_acquire_attached(&first_org_session)
        .expect("first org stream");
    assert!(matches!(
        org_limiter.try_acquire_attached(&second_org_session),
        Err(AttachedStreamLimit::Org)
    ));
}

#[tokio::test]
async fn proxy_streams_preserves_asymmetric_half_close() {
    let (mut downstream_client, downstream_proxy) = tokio::io::duplex(128);
    let (upstream_proxy, mut upstream_server) = tokio::io::duplex(128);

    let proxy = tokio::spawn(proxy_streams(
        "test",
        downstream_proxy,
        upstream_proxy,
        CancellationToken::new(),
    ));

    downstream_client.write_all(b"request").await.unwrap();
    downstream_client.shutdown().await.unwrap();

    let mut request = [0_u8; 7];
    upstream_server.read_exact(&mut request).await.unwrap();
    assert_eq!(&request, b"request");

    tokio::task::yield_now().await;

    upstream_server
        .write_all(b"response-after-half-close")
        .await
        .unwrap();
    upstream_server.shutdown().await.unwrap();

    let mut response = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(1),
        downstream_client.read_to_end(&mut response),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(&response, b"response-after-half-close");

    let close = tokio::time::timeout(Duration::from_secs(1), proxy)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(close, UpgradedProxyClose::Completed);
}

#[tokio::test]
async fn proxy_streams_exits_when_shutdown_is_already_cancelled() {
    let (_downstream_client, downstream_proxy) = tokio::io::duplex(128);
    let (upstream_proxy, _upstream_server) = tokio::io::duplex(128);
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    let close = tokio::time::timeout(
        Duration::from_secs(1),
        proxy_streams("test", downstream_proxy, upstream_proxy, shutdown),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(close, UpgradedProxyClose::Shutdown);
}

#[test]
fn attached_stream_limiter_removes_zero_count_scoped_entries() {
    let limiter = Arc::new(AttachedStreamLimiter::new(10, 10, 10));

    for _ in 0..128 {
        let mut session =
            attached_session(DataPlaneSessionId::new_for_kind(SessionKind::Terminal), 1);
        session.org_id = OrgId::new();
        session.sandbox_id = SandboxId::new();
        let permit = limiter
            .try_acquire_attached(&session)
            .expect("stream permit");
        drop(permit);
    }

    assert_eq!(limiter.active_total(), 0);
    assert_eq!(limiter.tracked_orgs(), 0);
    assert_eq!(limiter.tracked_sandboxes(), 0);
}
#[test]
fn native_session_limiter_allows_one_terminal_or_process_attach() {
    let limiter = Arc::new(NativeSessionAttachLimiter::default());
    let terminal_id = DataPlaneSessionId::Terminal(Default::default());
    let first = limiter
        .try_acquire(terminal_id, SessionKind::Terminal)
        .expect("first terminal attach")
        .expect("terminal attach is tracked");

    assert!(limiter.is_active(terminal_id));
    assert!(
        limiter
            .try_acquire(terminal_id, SessionKind::Terminal)
            .is_err()
    );

    drop(first);

    assert!(!limiter.is_active(terminal_id));
    assert!(
        limiter
            .try_acquire(terminal_id, SessionKind::Terminal)
            .expect("terminal attach after release")
            .is_some()
    );

    let watch_id = DataPlaneSessionId::FileWatch(Default::default());
    assert!(
        limiter
            .try_acquire(watch_id, SessionKind::FileWatch)
            .expect("file watch is not single-writer")
            .is_none()
    );
    assert!(
        limiter
            .try_acquire(watch_id, SessionKind::FileWatch)
            .expect("second file watch is allowed")
            .is_none()
    );
}

/// The tolerance production derives from its default check interval.
fn production_revocation_tolerance() -> Duration {
    crate::native::revocation_tolerance_within(
        REVOCATION_MAX_STALENESS,
        Duration::from_millis(DEFAULT_REVOCATION_CHECK_INTERVAL_MS),
        REVOCATION_CHECK_TIMEOUT,
    )
}

#[tokio::test]
async fn run_refuses_a_revocation_check_interval_above_the_maximum() {
    let maximum = Duration::from_millis(MAX_REVOCATION_CHECK_INTERVAL_MS);
    let config = |revocation_check_interval| DataPlaneConfig {
        revocation_check_interval,
        ..DataPlaneConfig::default()
    };
    config(maximum).validate().unwrap();
    let above = config(maximum + Duration::from_millis(1));
    assert!(matches!(
        above.validate(),
        Err(RunDataPlaneError::RevocationCheckIntervalTooLong { .. })
    ));
    // Refused before it connects to anything.
    let stop = pplx_space_common_svc::StopTokens {
        shutdown: CancellationToken::new(),
        forced: CancellationToken::new(),
    };
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), run(above, stop)).await,
        Ok(Err(
            RunDataPlaneError::RevocationCheckIntervalTooLong { .. }
        ))
    ));
}

#[test]
fn revocation_cache_uses_epoch_and_coalesces_refresh() {
    let cache = Arc::new(RevocationCache::default());
    let session_id = DataPlaneSessionId::Process(Default::default());
    let org_id = OrgId::new();

    assert!(matches!(
        cache.probe(session_id, production_revocation_tolerance()),
        RevocationProbe::RefreshSession
    ));

    cache.remember_active(
        session_id,
        10,
        org_id,
        20,
        RevocationRead::start(Duration::from_secs(60)),
    );
    assert_eq!(cache.cached_epoch(session_id), Some(10));
    assert_eq!(cache.cached_org_epoch(org_id), Some(20));
    assert!(matches!(
        cache.probe(session_id, production_revocation_tolerance()),
        RevocationProbe::Fresh(true)
    ));

    cache.remember_state(
        session_id,
        9,
        false,
        org_id,
        19,
        RevocationRead::start(Duration::from_secs(60)),
    );
    assert_eq!(cache.cached_epoch(session_id), Some(10));
    assert_eq!(cache.cached_org_epoch(org_id), Some(20));
    assert!(matches!(
        cache.probe(session_id, production_revocation_tolerance()),
        RevocationProbe::Fresh(true)
    ));

    cache.remember_state(
        session_id,
        11,
        false,
        org_id,
        21,
        RevocationRead::start(Duration::from_secs(60)),
    );
    assert_eq!(cache.cached_epoch(session_id), Some(11));
    assert_eq!(cache.cached_org_epoch(org_id), Some(21));
    assert!(matches!(
        cache.probe(session_id, production_revocation_tolerance()),
        RevocationProbe::Fresh(false)
    ));

    cache.remember_active(
        session_id,
        12,
        org_id,
        22,
        RevocationRead::start(Duration::from_millis(0)),
    );
    let guard = match cache.probe(session_id, production_revocation_tolerance()) {
        RevocationProbe::RefreshOrg(refresh) => refresh.guard,
        other => panic!("expected refresh probe for expired entry, got {other:?}"),
    };
    assert!(matches!(
        cache.probe(session_id, production_revocation_tolerance()),
        RevocationProbe::RefreshInFlight(true)
    ));
    drop(guard);

    cache.forget(session_id);
    assert!(cache.cached_epoch(session_id).is_none());
}

#[test]
fn revocation_cache_refreshes_session_behind_org_epoch() {
    let cache = Arc::new(RevocationCache::default());
    let session_id = DataPlaneSessionId::Process(Default::default());
    let org_id = OrgId::new();

    cache.remember_active(
        session_id,
        10,
        org_id,
        20,
        RevocationRead::start(Duration::from_secs(60)),
    );
    assert!(matches!(
        cache.probe(session_id, production_revocation_tolerance()),
        RevocationProbe::Fresh(true)
    ));

    // A revocation in the org advanced its epoch, and a sibling session's
    // refresh recorded the new epoch before this session's tick. The org
    // entry's TTL is fresh, but this session's own row was last read under
    // the old epoch, so it must be re-fetched rather than reported fresh.
    cache.remember_org_epoch(org_id, 21, RevocationRead::start(Duration::from_secs(60)));
    assert!(matches!(
        cache.probe(session_id, production_revocation_tolerance()),
        RevocationProbe::RefreshSession
    ));

    // Once the session row has been re-read under the current epoch the
    // cache answers from memory again.
    cache.remember_active(
        session_id,
        11,
        org_id,
        21,
        RevocationRead::start(Duration::from_secs(60)),
    );
    assert!(matches!(
        cache.probe(session_id, production_revocation_tolerance()),
        RevocationProbe::Fresh(true)
    ));
}

#[tokio::test]
async fn revocation_cache_coalesces_concurrent_org_refreshes() {
    let cache = Arc::new(RevocationCache::default());
    let session_id = DataPlaneSessionId::Process(Default::default());
    let org_id = OrgId::new();
    cache.remember_active(
        session_id,
        10,
        org_id,
        20,
        RevocationRead::start(Duration::ZERO),
    );

    let guard = match cache.probe(session_id, production_revocation_tolerance()) {
        RevocationProbe::RefreshOrg(refresh) => refresh.guard,
        other => panic!("expected first probe to own refresh, got {other:?}"),
    };
    let handles = (0..16)
        .map(|_| {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move {
                matches!(
                    cache.probe(session_id, production_revocation_tolerance()),
                    RevocationProbe::RefreshInFlight(true)
                )
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        assert!(handle.await.unwrap());
    }

    drop(guard);
}

#[test]
fn revocation_cache_keeps_a_confirmed_verdict_only_within_the_staleness_bound() {
    let cache = Arc::new(RevocationCache::default());
    let session_id = DataPlaneSessionId::Process(Default::default());
    let org_id = OrgId::new();
    assert!(!cache.confirmed_active(session_id, production_revocation_tolerance()));

    // Confirmed just now, and already due for a refresh.
    cache.remember_active(
        session_id,
        10,
        org_id,
        20,
        RevocationRead::start(Duration::ZERO),
    );
    assert!(cache.confirmed_active(session_id, production_revocation_tolerance()));
    assert!(!cache.confirmed_active(session_id, Duration::ZERO));

    // While another stream's refresh is in flight the cached verdict answers
    // within the bound; past it, this session is re-read.
    let _guard = match cache.probe(session_id, production_revocation_tolerance()) {
        RevocationProbe::RefreshOrg(refresh) => refresh.guard,
        other => panic!("expected the first probe to own the refresh, got {other:?}"),
    };
    assert!(matches!(
        cache.probe(session_id, production_revocation_tolerance()),
        RevocationProbe::RefreshInFlight(true)
    ));
    assert!(matches!(
        cache.probe(session_id, Duration::ZERO),
        RevocationProbe::RefreshSession
    ));

    // A verdict read under an older org epoch no longer counts, nor does a
    // revoked one.
    cache.remember_org_epoch(org_id, 21, RevocationRead::start(Duration::from_secs(60)));
    assert!(!cache.confirmed_active(session_id, production_revocation_tolerance()));
    cache.remember_state(
        session_id,
        11,
        false,
        org_id,
        21,
        RevocationRead::start(Duration::from_secs(60)),
    );
    assert!(!cache.confirmed_active(session_id, production_revocation_tolerance()));
}

#[test]
fn revocation_cache_org_confirmation_only_moves_forward() {
    let cache = RevocationCache::default();
    let org_id = OrgId::new();
    let fresh_for = Duration::from_secs(60);
    let earlier = RevocationRead::start(fresh_for);
    let later = RevocationRead {
        started_at: earlier.started_at + Duration::from_secs(5),
        fresh_for,
    };
    let org = |cache: &RevocationCache| {
        let entry = cache.orgs.get(&org_id).unwrap();
        (entry.epoch, entry.confirmed_at, entry.refresh_after)
    };
    cache.remember_org_epoch(org_id, 20, later);
    let confirmed = (20, later.started_at, later.started_at + fresh_for);
    assert_eq!(org(&cache), confirmed);

    // A slower read that began first and answers last moves nothing back.
    cache.remember_org_epoch(org_id, 20, earlier);
    assert_eq!(org(&cache), confirmed);
    // Nor does an older epoch, however recent its read.
    let latest = RevocationRead {
        started_at: later.started_at + Duration::from_secs(5),
        fresh_for,
    };
    cache.remember_org_epoch(org_id, 19, latest);
    assert_eq!(org(&cache), confirmed);
    // A newer epoch replaces it, dated from its own read.
    cache.remember_org_epoch(org_id, 21, earlier);
    assert_eq!(
        org(&cache),
        (21, earlier.started_at, earlier.started_at + fresh_for)
    );
}

#[test]
fn route_cache_expires_and_forgets_native_routes() {
    let cache = RouteCache::default();
    let session_id = DataPlaneSessionId::Port(Default::default());
    let key = native_route_cache_key(session_id, "token-hash", "port:connect");
    assert!(matches!(cache.lookup_native(&key), RouteCacheLookup::Miss));
    assert_eq!(cache.lookup_native(&key).result_label(), "miss");

    cache.remember_native(
        key.clone(),
        attached_session(session_id, 7),
        Duration::from_secs(60),
    );
    let hit = cache.lookup_native(&key);
    let RouteCacheLookup::Hit(route) = hit else {
        panic!("expected route cache hit");
    };
    assert_eq!(route.route_generation, 7);

    cache.forget_native_session(session_id);
    assert!(matches!(cache.lookup_native(&key), RouteCacheLookup::Miss));

    cache.remember_native(key.clone(), attached_session(session_id, 8), Duration::ZERO);
    let expired = cache.lookup_native(&key);
    assert!(matches!(expired, RouteCacheLookup::Expired(_)));
    assert!(matches!(cache.lookup_native(&key), RouteCacheLookup::Miss));

    cache.remember_native(key.clone(), attached_session(session_id, 9), Duration::ZERO);
    let expired = cache.lookup_native(&key);
    assert!(matches!(expired, RouteCacheLookup::Expired(_)));
    assert_eq!(expired.result_label(), "expired");
    assert_eq!(cache.lookup_native(&key).result_label(), "miss");
}

#[test]
fn route_cache_keys_and_forgets_ecw_routes_by_target() {
    let cache = RouteCache::default();
    let target = EcwSandboxHost {
        sandbox_id: SandboxId::new(),
        port: 8080,
    };
    let key = ecw_route_cache_key(
        &target,
        EcwRouteTransport::Direct,
        Some("envd"),
        None,
        Some("api"),
    );
    let ingress_key = ecw_route_cache_key(
        &target,
        EcwRouteTransport::ServicePortIngress,
        Some("envd"),
        None,
        Some("api"),
    );
    assert_ne!(key, ingress_key);

    cache.remember_ecw(
        key.clone(),
        ecw_resolved_route(target.sandbox_id, 3, backend_endpoint("http://worker:9800")),
        Duration::from_secs(60),
    );
    cache.remember_ecw(
        ingress_key.clone(),
        ecw_resolved_route(target.sandbox_id, 4, backend_endpoint("http://worker:9800")),
        Duration::from_secs(60),
    );
    let hit = cache.lookup_ecw(&key);
    let RouteCacheLookup::Hit(route) = hit else {
        panic!("expected ECW route cache hit");
    };
    assert_eq!(route.route_generation, 3);
    let ingress_hit = cache.lookup_ecw(&ingress_key);
    let RouteCacheLookup::Hit(ingress_route) = ingress_hit else {
        panic!("expected ECW ingress route cache hit");
    };
    assert_eq!(ingress_route.route_generation, 4);

    assert_eq!(cache.forget_ecw_target(target.sandbox_id, target.port), 2);
    assert!(matches!(cache.lookup_ecw(&key), RouteCacheLookup::Miss));
    assert!(matches!(
        cache.lookup_ecw(&ingress_key),
        RouteCacheLookup::Miss
    ));
}

#[test]
fn route_cache_replaces_and_expires_ecw_routes() {
    let cache = RouteCache::default();
    let target = EcwSandboxHost {
        sandbox_id: SandboxId::new(),
        port: 8080,
    };
    let key = ecw_route_cache_key(
        &target,
        EcwRouteTransport::ServicePortIngress,
        None,
        None,
        None,
    );
    cache.remember_ecw(
        key.clone(),
        ecw_resolved_route(
            target.sandbox_id,
            1,
            backend_endpoint("http://worker-one:9800"),
        ),
        Duration::from_secs(60),
    );
    cache.remember_ecw(
        key.clone(),
        ecw_resolved_route(
            target.sandbox_id,
            2,
            backend_endpoint("http://worker-two:9800"),
        ),
        Duration::ZERO,
    );

    let expired = cache.lookup_ecw(&key);
    let RouteCacheLookup::Expired(expired) = expired else {
        panic!("expected expired ECW route");
    };
    assert_eq!(expired.route_generation, 2);
    assert!(matches!(cache.lookup_ecw(&key), RouteCacheLookup::Miss));
}

#[test]
fn present_space_api_route_token_uses_signed_payload_for_refresh_deadline() {
    let parsed = parse_native_proto_space_api_route_token(proto_space_api_route_token())
        .expect("token-only proto should parse");

    assert!(!parsed.needs_refresh(
        tokio::time::Instant::now(),
        SPACE_API_ROUTE_TOKEN_REFRESH_WINDOW
    ));
}

#[test]
fn present_space_api_route_token_drops_expired_token_for_refresh() {
    let mut expired = space_api_route_claims();
    expired.expires_at_unix_secs = 1;

    assert!(
        parse_native_proto_space_api_route_token(proto_space_api_route_token_with_claims(expired))
            .is_none()
    );
}

#[test]
fn route_token_purpose_must_match_route_source() {
    assert!(
        parse_proto_space_api_route_token(
            SpaceApiRouteTokenRouteSource::DataPlaneSession,
            proto_space_api_route_token(),
        )
        .is_some()
    );
    assert!(
        parse_proto_space_api_route_token(
            SpaceApiRouteTokenRouteSource::SandboxServiceRouteBridge,
            proto_space_api_route_token(),
        )
        .is_some()
    );
    assert!(
        parse_proto_space_api_route_token(
            SpaceApiRouteTokenRouteSource::SandboxServiceRoute,
            proto_space_api_route_token(),
        )
        .is_none()
    );
    assert!(
        parse_proto_space_api_route_token(
            SpaceApiRouteTokenRouteSource::DataPlaneSession,
            proto_service_port_route_token(),
        )
        .is_some()
    );
    assert!(
        parse_proto_space_api_route_token(
            SpaceApiRouteTokenRouteSource::SandboxServiceRoute,
            proto_service_port_route_token(),
        )
        .is_some()
    );
}

fn ecw_service_proxy_request() -> Request<Body> {
    Request::builder()
        .uri("/service")
        .body(Body::empty())
        .expect("request builds")
}

fn remember_ecw_ingress_route(
    state: &DataPlaneState,
    target: &EcwSandboxHost,
    route: EcwResolvedRoute,
) -> String {
    let cache_key = ecw_route_cache_key(
        target,
        EcwRouteTransport::ServicePortIngress,
        None,
        None,
        None,
    );

    state
        .route_cache
        .remember_ecw(cache_key.clone(), route.clone(), Duration::from_secs(60));
    cache_key
}

#[test]
fn service_port_ingress_open_reason_preserves_unimplemented() {
    assert_eq!(
        service_port_ingress_open_reason(&tonic::Status::unimplemented("old runtime")),
        ServicePortIngressOpenReason::Unsupported
    );
    assert_eq!(
        service_port_ingress_open_reason(&tonic::Status::unavailable("connect failed")),
        ServicePortIngressOpenReason::OpenError
    );
}

#[tokio::test]
async fn service_port_ingress_proxy_fails_closed_for_every_open_error() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let target = EcwSandboxHost {
        sandbox_id: SandboxId::new(),
        port: 8080,
    };
    let route = ecw_resolved_route(
        target.sandbox_id,
        7,
        backend_endpoint("http://worker-one:9800"),
    );

    for open_error in [
        service_port_ingress_missing_token_error(),
        ServicePortIngressOpenError {
            error: EcwProxyError::sandbox_service_unreachable(),
            reason: ServicePortIngressOpenReason::GuestConnectionRefused,
            transport_status: Some(pplx_space_common_grpc::GuestServiceUnavailable.into_status()),
        },
        ServicePortIngressOpenError {
            error: EcwProxyError::sandbox_service_unreachable(),
            reason: ServicePortIngressOpenReason::Unsupported,
            transport_status: None,
        },
        ServicePortIngressOpenError {
            error: EcwProxyError::sandbox_service_unreachable(),
            reason: ServicePortIngressOpenReason::OpenError,
            transport_status: None,
        },
    ] {
        let state = test_data_plane_state().await;
        let cache_key = remember_ecw_ingress_route(&state, &target, route.clone());
        let _channel = ecw_backend_channel(&state, &route)
            .await
            .expect("backend channel");
        let error = ecw_service_proxy_via_service_port_ingress(
            state.clone(),
            target.clone(),
            route.clone(),
            ecw_service_proxy_request(),
            Err(open_error),
        )
        .await
        .expect_err("ingress open errors fail closed");
        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
        assert!(
            matches!(
                state.route_cache.lookup_ecw(&cache_key),
                RouteCacheLookup::Hit(_)
            ),
            "fail-closed ingress errors must not route to Direct"
        );
        assert_eq!(state.backend_channels.len(), 1);
        state
            .route_cache
            .forget_ecw_target(target.sandbox_id, target.port);
    }
}

#[tokio::test]
async fn service_port_ingress_transport_open_error_resets_cached_route_and_channel() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }

    let state = test_data_plane_state().await;
    let target = EcwSandboxHost {
        sandbox_id: SandboxId::new(),
        port: 8080,
    };
    let route = ecw_resolved_route(
        target.sandbox_id,
        7,
        backend_endpoint("http://worker-one:9800"),
    );
    let cache_key = remember_ecw_ingress_route(&state, &target, route.clone());
    let _channel = ecw_backend_channel(&state, &route)
        .await
        .expect("backend channel");
    let mut transport_status =
        tonic::Status::unknown("gRPC client transport error before status was returned");
    transport_status.set_source(Arc::new(tonic::ConnectError(Box::new(
        std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset"),
    ))));

    let error = ecw_service_proxy_via_service_port_ingress(
        state.clone(),
        target,
        route,
        ecw_service_proxy_request(),
        Err(ServicePortIngressOpenError {
            error: EcwProxyError::sandbox_service_unreachable(),
            reason: ServicePortIngressOpenReason::OpenError,
            transport_status: Some(transport_status),
        }),
    )
    .await
    .expect_err("transport failure still fails closed");

    assert_eq!(error.status, StatusCode::BAD_GATEWAY);
    assert!(matches!(
        state.route_cache.lookup_ecw(&cache_key),
        RouteCacheLookup::Miss
    ));
    assert_eq!(state.backend_channels.len(), 0);
}

#[tokio::test]
async fn service_port_ingress_pump_preserves_half_close_until_downstream_completion() {
    let (ingress_io, mut client_io) = tokio::io::duplex(64);
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let (stream_tx, stream_rx) = tokio::sync::mpsc::channel(4);
    let done = CancellationToken::new();
    let pump = tokio::spawn(pump_service_port_ingress_io_with_stream(
        ingress_io,
        tx,
        ReceiverStream::new(stream_rx),
        CancellationToken::new(),
        done.clone(),
        Progress::new(),
        true,
    ));
    for message in [
        service_port_ingress_frame::Message::Data(b"tail".to_vec()),
        service_port_ingress_frame::Message::Eof(()),
    ] {
        stream_tx
            .send(Ok(ServicePortIngressFrame {
                message: Some(message),
            }))
            .await
            .unwrap();
    }
    let mut body = Vec::new();
    client_io.read_to_end(&mut body).await.unwrap();
    assert_eq!(body, b"tail");
    assert!(!pump.is_finished(), "EOF must not complete the RPC");
    client_io.write_all(b"upload").await.unwrap();
    client_io.shutdown().await.unwrap();
    assert!(matches!(
        rx.recv().await.unwrap().message,
        Some(service_port_ingress_frame::Message::Data(_))
    ));
    assert!(matches!(
        rx.recv().await.unwrap().message,
        Some(service_port_ingress_frame::Message::Eof(_))
    ));
    done.cancel();
    assert!(matches!(
        rx.recv().await.unwrap().message,
        Some(service_port_ingress_frame::Message::Complete(_))
    ));
    pump.await.unwrap().unwrap();
}

#[test]
fn space_api_route_token_drop_reasons_use_bounded_metric_labels() {
    let cases = [
        (
            ProtoSpaceApiRouteTokenError::InvalidToken,
            SpaceApiRouteTokenDropReason::InvalidToken,
            "invalid_token",
        ),
        (
            ProtoSpaceApiRouteTokenError::InvalidExpiry,
            SpaceApiRouteTokenDropReason::InvalidExpiry,
            "invalid_expiry",
        ),
        (
            ProtoSpaceApiRouteTokenError::Expired,
            SpaceApiRouteTokenDropReason::Expired,
            "expired",
        ),
        (
            ProtoSpaceApiRouteTokenError::InvalidPurpose,
            SpaceApiRouteTokenDropReason::InvalidPurpose,
            "invalid_purpose",
        ),
        (
            ProtoSpaceApiRouteTokenError::InvalidSandboxId,
            SpaceApiRouteTokenDropReason::InvalidSandboxId,
            "invalid_sandbox_id",
        ),
    ];

    for (error, expected_reason, expected_label) in cases {
        let reason = error.metric_reason();
        assert_eq!(reason, expected_reason);
        assert_eq!(reason.to_string(), expected_label);
    }
}

#[test]
fn space_api_route_token_state_refreshes_before_expiry() {
    let now = tokio::time::Instant::now();
    let token = SpaceApiRouteEd25519Token::parse(TEST_ROUTE_TOKEN).unwrap();
    let state = SpaceApiRouteTokenState {
        token,
        expires_at: now + SPACE_API_ROUTE_TOKEN_REFRESH_WINDOW / 2,
        route_identity: SpaceApiRouteIdentity::default(),
    };

    assert!(state.needs_refresh(now, SPACE_API_ROUTE_TOKEN_REFRESH_WINDOW));
    assert!(!state.needs_refresh(now, Duration::from_millis(1)));
}

#[test]
fn native_route_drops_present_space_api_route_token_with_invalid_payload() {
    let session_id = DataPlaneSessionId::Terminal(Default::default());
    let mut token = proto_space_api_route_token();
    token.token = "invalid".to_string();

    let attached = attached_session_from_route_response(
        route_response_with_token(session_id, Some(token)),
        session_id,
        1,
    )
    .expect("route token forwarding should degrade gracefully");

    assert!(attached.space_api_route_token.is_none());
}

#[test]
fn native_service_sessions_require_ingress_transport() {
    assert_eq!(
        native_requested_transport(SessionKind::Port),
        SandboxServiceRouteTransport::ServicePortIngress
    );
    assert_eq!(
        native_requested_transport(SessionKind::Terminal),
        SandboxServiceRouteTransport::Direct
    );
}

#[test]
fn ecw_process_input_locks_are_scoped_to_sandbox_and_process() {
    let locks = EcwProcessInputLocks::default();
    let sandbox_id = SandboxId::new();
    let same = locks.lock_for(sandbox_id, "proc-1".parse().unwrap());
    let same_again = locks.lock_for(sandbox_id, "proc-1".parse().unwrap());
    let other_process = locks.lock_for(sandbox_id, "proc-2".parse().unwrap());
    let other_sandbox = locks.lock_for(SandboxId::new(), "proc-1".parse().unwrap());

    assert!(Arc::ptr_eq(&same, &same_again));
    assert!(!Arc::ptr_eq(&same, &other_process));
    assert!(!Arc::ptr_eq(&same, &other_sandbox));
}

#[test]
fn ecw_process_input_locks_prune_idle_entries() {
    let locks = EcwProcessInputLocks::default();
    let sandbox_id = SandboxId::new();
    let stale = locks.lock_for(sandbox_id, "proc-1".parse().unwrap());
    assert_eq!(locks.len(), 1);

    drop(stale);
    let _fresh = locks.lock_for(sandbox_id, "proc-2".parse().unwrap());

    assert_eq!(locks.len(), 1);
}

#[test]
fn query_tokens_are_rejected() {
    reject_query_tokens(Some("token=dpt_secret")).unwrap_err();
    reject_query_tokens(Some("a=1&access_token=dpt_secret")).unwrap_err();
    reject_query_tokens(Some("cursor=abc")).unwrap();
}

#[tokio::test]
async fn attach_error_uses_structured_data_plane_error() {
    let response = AttachError::BadRequest("bad attach").into_response();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "invalid_request");
    assert_eq!(body["error"]["message"], "bad attach");
    assert_eq!(body["error"]["retryable"], serde_json::json!(false));
}

#[test]
fn header_token_accepts_authorization_and_subprotocol() {
    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "a".repeat(64));
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        format!("Bearer {token}").parse().unwrap(),
    );
    assert_eq!(token_from_headers(&headers).unwrap(), Some(token.clone()));

    let mut headers = HeaderMap::new();
    headers.insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        format!("space.data-plane, {token}").parse().unwrap(),
    );
    assert_eq!(token_from_headers(&headers).unwrap(), Some(token));
}

#[test]
fn ecw_host_config_parses_wildcard_sandbox_hosts() {
    let sandbox_id = SandboxId::new();
    let config =
        EcwHostConfig::from_env_value(Some(r#"[{"apiHost":"api.ecw.test","host":"ecw.test."}]"#));
    let mut headers = HeaderMap::new();
    headers.insert(
        header::HOST,
        format!("8080-{sandbox_id}.ECW.TEST:443").parse().unwrap(),
    );

    let target = config.parse_sandbox_host(&headers).unwrap();
    assert_eq!(target.port, 8080);
    assert_eq!(target.sandbox_id, sandbox_id);
}

#[test]
fn ecw_sandbox_service_port_requests_use_service_proxy_path() {
    let sandbox_id = SandboxId::new();
    let config = EcwHostConfig::from_env_value(Some(r#"[{"host":"ecw.test"}]"#));
    let mut headers = HeaderMap::new();
    headers.insert(
        header::HOST,
        format!("8124-{sandbox_id}.ecw.test").parse().unwrap(),
    );

    assert!(should_proxy_ecw_service_port_request(&config, &headers));

    headers.insert(
        header::HOST,
        format!("{ECW_ENVD_PORT}-{sandbox_id}.ecw.test")
            .parse()
            .unwrap(),
    );
    assert!(!should_proxy_ecw_service_port_request(&config, &headers));

    headers.insert(header::HOST, "space-data-plane.ecw.test".parse().unwrap());
    assert!(!should_proxy_ecw_service_port_request(&config, &headers));
}

#[test]
fn data_plane_http_span_metadata_redacts_ecw_proxy_paths() {
    let sandbox_id = SandboxId::new();
    let config = EcwHostConfig::from_env_value(Some(r#"[{"host":"ecw.test"}]"#));
    let metadata = DataPlaneHttpSpanMetadata::new(Arc::new(config));
    let mut headers = HeaderMap::new();
    headers.insert(
        header::HOST,
        format!("8080-{sandbox_id}.ecw.test").parse().unwrap(),
    );

    assert_eq!(
        metadata.span_path_and_route(
            &axum::http::Method::GET,
            &headers,
            "/customer/private/path",
            None
        ),
        (
            Cow::Borrowed(pplx_space_common_svc::REDACTED_HTTP_PATH_VALUE),
            Cow::Borrowed(pplx_space_common_svc::REDACTED_HTTP_PATH_VALUE),
        ),
    );
    assert_eq!(
        metadata.span_path_and_route(&axum::http::Method::GET, &headers, "/files", Some("/files")),
        (Cow::Borrowed("/files"), Cow::Borrowed("/files")),
    );
}

#[test]
fn data_plane_http_span_metadata_extracts_native_session_id() {
    let metadata = DataPlaneHttpSpanMetadata::new(Arc::default());

    assert_eq!(
        metadata.session_id_from_path("/v1/sessions/proc_0193a8f0000070008000000000000000/attach"),
        Some("proc_0193a8f0000070008000000000000000"),
    );
    assert_eq!(metadata.session_id_from_path("/files"), None);
}

#[test]
fn ecw_host_config_rejects_unknown_host_and_invalid_port() {
    let sandbox_id = SandboxId::new();
    let config = EcwHostConfig::from_env_value(Some(r#"[{"host":"ecw.test"}]"#));
    let mut headers = HeaderMap::new();
    headers.insert(
        header::HOST,
        format!("8080-{sandbox_id}.other.test").parse().unwrap(),
    );
    assert!(config.parse_sandbox_host(&headers).is_none());

    headers.insert(
        header::HOST,
        format!("0-{sandbox_id}.ecw.test").parse().unwrap(),
    );
    assert!(config.parse_sandbox_host(&headers).is_none());
}

#[test]
fn ecw_sandbox_header_validation_rejects_mismatches() {
    let sandbox_id = SandboxId::new();
    let target = EcwSandboxHost {
        port: 8080,
        sandbox_id,
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        ECW_SANDBOX_ID_HEADER,
        Uuid::now_v7().to_string().parse().unwrap(),
    );
    assert_eq!(
        validate_ecw_sandbox_headers(&headers, &target)
            .unwrap_err()
            .status,
        StatusCode::BAD_REQUEST,
    );

    headers.clear();
    headers.insert(ECW_SANDBOX_PORT_HEADER, "8081".parse().unwrap());
    assert_eq!(
        validate_ecw_sandbox_headers(&headers, &target)
            .unwrap_err()
            .status,
        StatusCode::BAD_REQUEST,
    );
}

#[test]
fn ecw_service_ingress_rewrite_uses_origin_form_and_preserves_customer_auth() {
    let mut request = Request::builder()
        .uri("http://ignored.space.example/api/events?token=customer-owned")
        .header(ECW_TRAFFIC_ACCESS_TOKEN_HEADER, "space-traffic")
        .header(ECW_API_KEY_HEADER, "space-api-key")
        .header(ECW_ENVD_ACCESS_TOKEN_HEADER, "space-envd")
        .header(FEDERATED_ORG_ID_HEADER, "org")
        .header(FEDERATED_USER_ID_HEADER, "user")
        .header(header::AUTHORIZATION, "Bearer customer")
        .body(Body::empty())
        .unwrap();

    rewrite_ecw_service_ingress_request(&mut request).unwrap();

    assert_eq!(
        request.uri().to_string(),
        "/api/events?token=customer-owned"
    );
    for header in ECW_SERVICE_PROXY_STRIPPED_SPACE_HEADERS {
        assert!(!request.headers().contains_key(*header));
    }
    assert_eq!(
        request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer customer"),
    );
}

#[tokio::test]
async fn ecw_service_route_repair_times_out_cached_bridge_close_and_still_resets() {
    #[derive(Debug, PartialEq, Eq)]
    enum RepairCall {
        Close {
            sandbox_id: String,
            port: u32,
        },
        Reset {
            sandbox_id: String,
            port: u32,
            kind: i32,
            route_generation: u64,
            diagnostic_message: Option<String>,
        },
    }

    let sandbox_id = SandboxId::new();
    let target = EcwSandboxHost {
        sandbox_id,
        port: 8123,
    };
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let close_calls = calls.clone();
    let reset_calls = calls.clone();

    apply_ecw_sandbox_service_route_repair(
        ecw_sandbox_service_route_repair(&target, 42, "connection refused"),
        std::time::Duration::from_millis(1),
        move |request| {
            let close_calls = close_calls.clone();
            async move {
                let operation = request.operation.expect("close operation");
                close_calls.lock().unwrap().push(RepairCall::Close {
                    sandbox_id: request.sandbox_id,
                    port: operation.port,
                });
                std::future::pending::<()>().await;
            }
        },
        move |request| {
            let reset_calls = reset_calls.clone();
            async move {
                let route = request.route.expect("reset route");
                reset_calls.lock().unwrap().push(RepairCall::Reset {
                    sandbox_id: route.sandbox_id,
                    port: route.port,
                    kind: route.kind,
                    route_generation: request.route_generation,
                    diagnostic_message: request.diagnostic_message,
                });
            }
        },
    )
    .await;

    let sandbox_id = sandbox_id.to_string();
    assert_eq!(
        &*calls.lock().unwrap(),
        &[
            RepairCall::Close {
                sandbox_id: sandbox_id.clone(),
                port: 8123,
            },
            RepairCall::Reset {
                sandbox_id,
                port: 8123,
                kind: SandboxServiceRouteKind::ServicePort as i32,
                route_generation: 42,
                diagnostic_message: Some("connection refused".to_string()),
            },
        ]
    );
}

#[test]
fn ecw_route_permission_denied_keeps_traffic_token_error_shape() {
    let sandbox_id = SandboxId::new();
    let target = EcwSandboxHost {
        port: 8080,
        sandbox_id,
    };
    let origin = origin_for_target(target.clone(), None);
    let error =
        runtime_route::ecw_route_error(tonic::Status::permission_denied("missing"), &origin);
    assert_eq!(error.status, StatusCode::FORBIDDEN);
    assert_eq!(error.sandbox_id, Some(sandbox_id));
    assert!(error.message.contains("missing"));

    let origin = origin_for_target(target, Some("bad-token-sha256".to_string()));
    let error =
        runtime_route::ecw_route_error(tonic::Status::permission_denied("invalid"), &origin);
    assert_eq!(error.status, StatusCode::FORBIDDEN);
    assert_eq!(error.sandbox_id, Some(sandbox_id));
    assert!(error.message.contains("invalid"));
}

#[test]
fn ecw_service_route_not_ready_gets_non_terminal_error_shape() {
    let target = EcwSandboxHost {
        port: 8080,
        sandbox_id: SandboxId::new(),
    };
    let error = runtime_route::ecw_route_error(
        tonic::Status::failed_precondition("sandbox service route port exposure is not applied"),
        &origin_for_target(target, None),
    );

    assert_eq!(error.status, StatusCode::BAD_GATEWAY);
    assert_eq!(error.message, "Sandbox service route is not ready");
    assert_eq!(error.retry_after, Some(Duration::from_secs(1)));
    let response = error.into_response();
    assert_eq!(response.headers()[header::RETRY_AFTER], "1");
}

#[test]
fn ecw_service_route_deadline_keeps_runtime_access_classifier() {
    let target = EcwSandboxHost {
        port: 8080,
        sandbox_id: SandboxId::new(),
    };
    for status in [
        tonic::Status::deadline_exceeded("route deadline"),
        tonic::Status::cancelled("route cancelled"),
    ] {
        let error =
            runtime_route::ecw_route_error(status, &origin_for_target(target.clone(), None));

        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
        assert_eq!(error.message, "Sandbox is not running");
        assert!(ecw_error_can_use_runtime_access_decision(&error));
    }
}

#[tokio::test]
async fn ecw_service_route_resolution_timeout_gets_retryable_shape_without_classifier() {
    let target = EcwSandboxHost {
        port: 8080,
        sandbox_id: SandboxId::new(),
    };
    let pending_route = std::future::pending::<Result<(), tonic::Status>>();

    let timeout =
        runtime_route::bounded_ecw_route_resolve(&target, Duration::from_millis(1), pending_route)
            .await
            .expect_err("pending route resolve should time out");
    let runtime_route::EcwRouteResolveError::Timeout = timeout else {
        panic!("expected route resolve timeout");
    };

    let error = runtime_route::route_resolve_timeout_error();
    assert_eq!(error.status, StatusCode::BAD_GATEWAY);
    assert_eq!(error.message, "Sandbox service route is not ready");
    assert_eq!(error.retry_after, Some(Duration::from_secs(1)));
    assert!(!ecw_error_can_use_runtime_access_decision(&error));
}

#[test]
fn ecw_runtime_access_convergence_gets_retryable_shape() {
    let error = ecw_error_for_runtime_access(
        SandboxRuntimeAccess {
            decision: SandboxRuntimeAccessDecision::RetryableConverging as i32,
            retry_after_ms: 12_000,
        },
        EcwProxyError::upstream("Sandbox service is not reachable"),
    );

    assert_eq!(error.status, StatusCode::BAD_GATEWAY);
    assert_eq!(error.message, "Sandbox is waking; retry");
    assert_eq!(error.retry_after, Some(Duration::from_secs(12)));
    let response = error.into_response();
    assert_eq!(response.headers()[header::RETRY_AFTER], "12");
}

#[test]
fn ecw_runtime_access_convergence_defaults_retry_after_when_missing() {
    let error = ecw_error_for_runtime_access(
        SandboxRuntimeAccess {
            decision: SandboxRuntimeAccessDecision::RetryableConverging as i32,
            retry_after_ms: 0,
        },
        EcwProxyError::upstream("Sandbox service is not reachable"),
    );

    assert_eq!(error.status, StatusCode::BAD_GATEWAY);
    assert_eq!(error.message, "Sandbox is waking; retry");
    assert_eq!(error.retry_after, Some(Duration::from_secs(1)));
    let response = error.into_response();
    assert_eq!(response.headers()[header::RETRY_AFTER], "1");
}

#[test]
fn ecw_runtime_access_terminal_gets_sdk_timeout_shape() {
    let error = ecw_error_for_runtime_access(
        SandboxRuntimeAccess {
            decision: SandboxRuntimeAccessDecision::TerminalUnavailable as i32,
            retry_after_ms: 0,
        },
        EcwProxyError::upstream("Sandbox service is not reachable"),
    );

    assert_eq!(error.status, StatusCode::BAD_GATEWAY);
    assert_eq!(error.message, "Sandbox is not running");
    assert_eq!(error.retry_after, None);
}

#[test]
fn ecw_runtime_access_ready_preserves_original_proxy_error() {
    let error = ecw_error_for_runtime_access(
        SandboxRuntimeAccess {
            decision: SandboxRuntimeAccessDecision::Ready as i32,
            retry_after_ms: 0,
        },
        EcwProxyError::upstream("Sandbox service is not reachable"),
    );

    assert_eq!(error.status, StatusCode::BAD_GATEWAY);
    assert_eq!(error.message, "Sandbox service is not reachable");
}

#[test]
fn ecw_connect_backend_timeout_statuses_keep_sdk_timeout_shape() {
    for status in [
        tonic::Status::deadline_exceeded("spaced call timed out"),
        tonic::Status::cancelled("spaced call cancelled"),
        tonic::Status::failed_precondition("sandbox is not running"),
        tonic::Status::failed_precondition("cannot wake for access from stopped"),
        tonic::Status::failed_precondition("sandbox is deleting, cannot wake"),
    ] {
        let error = ecw_connect::tonic_status_to_ecw_error(status);
        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
        assert_eq!(error.message, "Sandbox is not running");
    }
}

#[test]
fn ecw_connect_guest_capability_unsupported_gets_not_implemented_shape() {
    let error = ecw_connect::tonic_status_to_ecw_error(
        pplx_space_common_grpc::guest_capability_unsupported_status(
            pplx_space_common_grpc::GuestCapability::InteractiveProcessControl,
            "guest spaced-svc version 0.1.1 does not support SendInput; rebuild or restore the sandbox with spaced guest version 0.1.2 or newer",
        ),
    );

    assert_eq!(error.status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(error.origin, ResponseOrigin::Spaced);
    assert_eq!(error.kind, ResponseKind::Validation);
    assert!(error.message.contains("0.1.2 or newer"));
}

#[test]
fn ecw_connect_backend_disk_full_status_gets_storage_shape() {
    let error = ecw_connect::tonic_status_to_ecw_error(tonic::Status::resource_exhausted(
        "No space left on device",
    ));
    assert_eq!(error.status, StatusCode::INSUFFICIENT_STORAGE);
    assert_eq!(error.message, "Sandbox storage is full");

    for reason in [
        CacheDiskAdmissionReason::DiskUnknownStale,
        CacheDiskAdmissionReason::DiskAvailableBelowThreshold,
        CacheDiskAdmissionReason::BtrfsAllocationPressure,
    ] {
        let error = ecw_connect::tonic_status_to_ecw_error(tonic::Status::resource_exhausted(
            format!("StartSandbox rejected by node disk admission: {reason}"),
        ));
        assert_eq!(error.status, StatusCode::INSUFFICIENT_STORAGE);
        assert_eq!(error.message, "Sandbox storage is full");
    }

    let error = ecw_connect::tonic_status_to_ecw_error(tonic::Status::resource_exhausted(
        "too many open streams",
    ));
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(error.retry_after, None);
}

#[test]
fn ecw_connect_uds_limiter_gets_retryable_backpressure_shape() {
    let error = ecw_connect::tonic_status_to_ecw_error(
        pplx_space_common_grpc::uds_per_sandbox_limit_exceeded_status(
            "SpacedStartProcess UDS per-sandbox limiter exceeded: scope=sandbox, reason=saturated, in_flight=64, permits=64",
        ),
    );

    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(error.message, "Sandbox is busy; retry");
    assert_eq!(error.retry_after, Some(Duration::from_secs(1)));
    let response = error.into_response();
    assert_eq!(response.headers()[header::RETRY_AFTER], "1");
}

#[test]
fn ecw_connect_uds_limiter_legacy_message_gets_generic_backpressure_shape() {
    let legacy_message = "SpacedStartProcess UDS per-sandbox limiter exceeded: scope=sandbox, reason=saturated, in_flight=64, permits=64";
    let error =
        ecw_connect::tonic_status_to_ecw_error(tonic::Status::resource_exhausted(legacy_message));

    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(error.message, legacy_message);
    assert_eq!(error.retry_after, None);
}

#[test]
fn proxy_path_rewrite_preserves_tail_and_query() {
    let session_id = DataPlaneSessionId::Port(Default::default());
    let uri = format!("/v1/sessions/{session_id}/proxy/api/events?cursor=abc")
        .parse::<Uri>()
        .unwrap();
    assert_eq!(
        proxy_path_and_query(session_id, &uri).unwrap(),
        "/api/events?cursor=abc"
    );

    let uri = format!("/v1/sessions/{session_id}/proxy")
        .parse::<Uri>()
        .unwrap();
    assert_eq!(proxy_path_and_query(session_id, &uri).unwrap(), "/");

    let session_id = DataPlaneSessionId::Protocol(Default::default());
    let uri = format!("/v1/sessions/{session_id}/proxy/json/version")
        .parse::<Uri>()
        .unwrap();
    assert_eq!(
        proxy_path_and_query(session_id, &uri).unwrap(),
        "/json/version"
    );
}

#[test]
fn cdp_proxy_base_uses_forwarded_public_origin() {
    let session_id = DataPlaneSessionId::Protocol(Default::default());
    let mut headers = HeaderMap::new();
    headers.insert(header::HOST, "internal.local".parse().unwrap());
    headers.insert("x-forwarded-host", "space.example.com".parse().unwrap());
    headers.insert("x-forwarded-proto", "https".parse().unwrap());

    let base = CdpProxyBase::from_headers(&headers, session_id);
    assert_eq!(
        base.http_base,
        format!("https://space.example.com/v1/sessions/{session_id}/proxy")
    );
    assert_eq!(
        base.ws_base,
        format!("wss://space.example.com/v1/sessions/{session_id}/proxy")
    );
}

#[test]
fn cdp_discovery_rewrite_rewrites_debugger_urls() {
    let proxy_base = CdpProxyBase {
        http_base: "https://space.example.com/v1/sessions/proto_1/proxy".to_string(),
        ws_base: "wss://space.example.com/v1/sessions/proto_1/proxy".to_string(),
    };
    let mut value = serde_json::json!([
        {
            "id": "page-1",
            "webSocketDebuggerUrl": "ws://127.0.0.1:9222/devtools/page/page-1",
            "devtoolsFrontendUrl": "/devtools/inspector.html?ws=127.0.0.1:9222/devtools/page/page-1"
        },
        {
            "id": "page-2",
            "webSocketDebuggerUrl": "/devtools/page/page-2"
        }
    ]);

    rewrite_cdp_discovery_value(&mut value, &proxy_base);

    assert_eq!(
        value[0]["webSocketDebuggerUrl"],
        "wss://space.example.com/v1/sessions/proto_1/proxy/devtools/page/page-1"
    );
    assert_eq!(
        value[0]["devtoolsFrontendUrl"],
        "https://space.example.com/v1/sessions/proto_1/proxy/devtools/inspector.html?ws=space.example.com/v1/sessions/proto_1/proxy/devtools/page/page-1"
    );
    assert_eq!(
        value[1]["webSocketDebuggerUrl"],
        "wss://space.example.com/v1/sessions/proto_1/proxy/devtools/page/page-2"
    );
}

#[test]
fn proxy_strips_only_consumed_data_plane_token() {
    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "a".repeat(64));
    let mut headers = HeaderMap::new();
    headers.insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        format!("chat, {token}, superchat").parse().unwrap(),
    );
    strip_data_plane_auth_headers(&mut headers, HeaderTokenSource::WebSocketProtocol).unwrap();
    assert_eq!(
        headers
            .get(header::SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok()),
        Some("chat, superchat")
    );

    headers.insert(
        header::AUTHORIZATION,
        format!("Bearer {token}").parse().unwrap(),
    );
    strip_data_plane_auth_headers(&mut headers, HeaderTokenSource::Authorization).unwrap();
    assert!(!headers.contains_key(header::AUTHORIZATION));
}

#[test]
fn upgraded_proxy_close_labels_are_bounded() {
    assert_eq!(UpgradedProxyClose::Completed.result_label(), "ok");
    assert_eq!(UpgradedProxyClose::Revoked.result_label(), "revoked");
}

#[test]
fn unauthenticated_limiter_caps_per_source_ip() {
    let limiter = Arc::new(UnauthenticatedUpgradeLimiter::new(1));
    let ip = "127.0.0.1".parse::<IpAddr>().unwrap();
    let guard = limiter.try_register(ip).unwrap();
    assert!(limiter.try_register(ip).is_err());
    drop(guard);
    assert!(limiter.try_register(ip).is_ok());
}

#[test]
fn terminal_session_defaults_to_shell_command() {
    let session = AttachedSession {
        cache_epoch: 1,
        cache_key: None,
        session_id: DataPlaneSessionId::Terminal(Default::default()),
        sandbox_id: SandboxId::new(),
        org_id: OrgId::new(),
        kind: SessionKind::Terminal,
        route_generation: 1,
        revocation_epoch: 0,
        org_revocation_epoch: 0,
        current_node_id: NodeId::new(),
        backend_endpoint: backend_endpoint("http://127.0.0.1:9800"),
        target_port: None,
        target_protocol: None,
        backend_process_id: None,
        space_api_route_token: None,
        adapter_state_json: serde_json::to_string(&CreateTerminalSessionRequest {
            command: Vec::new(),
            cwd: Some("/workspace".to_string()),
            env: EnvMap::try_from(std::collections::BTreeMap::from([(
                "A".to_string(),
                "B".to_string(),
            )]))
            .unwrap(),
            size: Some(TerminalSize { rows: 24, cols: 80 }),
        })
        .unwrap(),
    };

    let request = process_start_request(&session, session.session_id).unwrap();
    assert_eq!(request.command, vec!["/bin/sh".to_string()]);
    assert_eq!(request.cwd.as_deref(), Some("/workspace"));
    assert_eq!(request.env.get("A").map(String::as_str), Some("B"));
    assert!(!request.background);
    assert!(request.stdin);
    let size = request.pty.expect("terminal pty").size.expect("pty size");
    assert_eq!(size.rows, 24);
    assert_eq!(size.cols, 80);
}

#[test]
fn process_session_maps_interactive_stdin_request() {
    let session = AttachedSession {
        cache_epoch: 1,
        cache_key: None,
        session_id: DataPlaneSessionId::Process(Default::default()),
        sandbox_id: SandboxId::new(),
        org_id: OrgId::new(),
        kind: SessionKind::Process,
        route_generation: 1,
        revocation_epoch: 0,
        org_revocation_epoch: 0,
        current_node_id: NodeId::new(),
        backend_endpoint: backend_endpoint("http://127.0.0.1:9800"),
        target_port: None,
        target_protocol: None,
        backend_process_id: None,
        space_api_route_token: None,
        adapter_state_json: serde_json::to_string(&CreateProcessRequest {
            command: vec!["cat".to_string()],
            cwd: None,
            env: Default::default(),
            pty: None,
            stdin: Some(true),
            user: None,
            timeout: None,
        })
        .unwrap(),
    };

    let request = process_start_request(&session, session.session_id).unwrap();
    assert_eq!(request.command, vec!["cat".to_string()]);
    assert!(request.stdin);
    assert!(request.pty.is_none());
}

#[test]
fn process_session_maps_timeout_duration() {
    let session = AttachedSession {
        cache_epoch: 1,
        cache_key: None,
        session_id: DataPlaneSessionId::Process(Default::default()),
        sandbox_id: SandboxId::new(),
        org_id: OrgId::new(),
        kind: SessionKind::Process,
        route_generation: 1,
        revocation_epoch: 0,
        org_revocation_epoch: 0,
        current_node_id: NodeId::new(),
        backend_endpoint: backend_endpoint("http://127.0.0.1:9800"),
        target_port: None,
        target_protocol: None,
        backend_process_id: None,
        space_api_route_token: None,
        adapter_state_json: serde_json::to_string(&CreateProcessRequest {
            command: vec!["sleep".to_string(), "10".to_string()],
            cwd: None,
            env: Default::default(),
            pty: None,
            stdin: Some(false),
            user: None,
            timeout: Some("1.25s".parse::<SimpleDuration>().unwrap()),
        })
        .unwrap(),
    };

    let request = process_start_request(&session, session.session_id).unwrap();
    let timeout = request.timeout.expect("process timeout");
    assert_eq!(timeout.seconds, 1);
    assert_eq!(timeout.nanos, 250_000_000);
}

#[test]
fn process_session_maps_requested_pty() {
    let session = AttachedSession {
        cache_epoch: 1,
        cache_key: None,
        session_id: DataPlaneSessionId::Process(Default::default()),
        sandbox_id: SandboxId::new(),
        org_id: OrgId::new(),
        kind: SessionKind::Process,
        route_generation: 1,
        revocation_epoch: 0,
        org_revocation_epoch: 0,
        current_node_id: NodeId::new(),
        backend_endpoint: backend_endpoint("http://127.0.0.1:9800"),
        target_port: None,
        target_protocol: None,
        backend_process_id: None,
        space_api_route_token: None,
        adapter_state_json: serde_json::to_string(&CreateProcessRequest {
            command: vec!["/bin/sh".to_string()],
            cwd: None,
            env: Default::default(),
            pty: Some(TerminalSize { rows: 31, cols: 99 }),
            stdin: Some(true),
            user: None,
            timeout: None,
        })
        .unwrap(),
    };

    let request = process_start_request(&session, session.session_id).unwrap();
    let size = request.pty.expect("process pty").size.expect("pty size");
    assert_eq!(size.rows, 31);
    assert_eq!(size.cols, 99);
}

#[test]
fn process_session_carries_the_requested_user_to_the_guest() {
    // The guest resolves `user`, falling back to its configured default. The
    // bridge used to drop it, so every command ran as that default however
    // it was requested.
    let session = AttachedSession {
        cache_epoch: 1,
        cache_key: None,
        session_id: DataPlaneSessionId::Process(Default::default()),
        sandbox_id: SandboxId::new(),
        org_id: OrgId::new(),
        kind: SessionKind::Process,
        route_generation: 1,
        revocation_epoch: 0,
        org_revocation_epoch: 0,
        current_node_id: NodeId::new(),
        backend_endpoint: backend_endpoint("http://127.0.0.1:9800"),
        target_port: None,
        target_protocol: None,
        backend_process_id: None,
        space_api_route_token: None,
        adapter_state_json: serde_json::to_string(&CreateProcessRequest {
            command: vec!["/bin/sh".to_string()],
            cwd: None,
            env: Default::default(),
            pty: None,
            stdin: Some(false),
            user: Some("root".to_string()),
            timeout: None,
        })
        .unwrap(),
    };

    let request = process_start_request(&session, session.session_id).unwrap();

    assert_eq!(request.user.as_deref(), Some("root"));
}

#[test]
fn process_session_without_a_user_defers_to_the_guest_default() {
    let session = AttachedSession {
        cache_epoch: 1,
        cache_key: None,
        session_id: DataPlaneSessionId::Process(Default::default()),
        sandbox_id: SandboxId::new(),
        org_id: OrgId::new(),
        kind: SessionKind::Process,
        route_generation: 1,
        revocation_epoch: 0,
        org_revocation_epoch: 0,
        current_node_id: NodeId::new(),
        backend_endpoint: backend_endpoint("http://127.0.0.1:9800"),
        target_port: None,
        target_protocol: None,
        backend_process_id: None,
        space_api_route_token: None,
        adapter_state_json: serde_json::to_string(&CreateProcessRequest {
            command: vec!["/bin/sh".to_string()],
            cwd: None,
            env: Default::default(),
            pty: None,
            stdin: Some(false),
            user: None,
            timeout: None,
        })
        .unwrap(),
    };

    let request = process_start_request(&session, session.session_id).unwrap();

    assert_eq!(request.user, None);
}

#[test]
fn file_watch_session_maps_to_spaced_watch_request() {
    let session = AttachedSession {
        cache_epoch: 1,
        cache_key: None,
        session_id: DataPlaneSessionId::FileWatch(Default::default()),
        sandbox_id: SandboxId::new(),
        org_id: OrgId::new(),
        kind: SessionKind::FileWatch,
        route_generation: 1,
        revocation_epoch: 0,
        org_revocation_epoch: 0,
        current_node_id: NodeId::new(),
        backend_endpoint: backend_endpoint("http://127.0.0.1:9800"),
        target_port: None,
        target_protocol: None,
        backend_process_id: None,
        space_api_route_token: None,
        adapter_state_json: serde_json::to_string(&CreateFileWatchSessionRequest {
            path: "/workspace/src".to_string(),
            recursive: true,
        })
        .unwrap(),
    };

    let request = file_watch_request(&session).unwrap();
    assert_eq!(request.path, "/workspace/src");
    assert!(request.recursive);
    assert!(request.include_metadata_events);
    assert!(request.include_globs.is_empty());
    assert!(request.exclude_globs.is_empty());
}

#[tokio::test]
async fn file_watch_stale_route_token_refreshes_attached_session_before_watch_reconnect() {
    let session_id = DataPlaneSessionId::FileWatch(Default::default());
    let now = tokio::time::Instant::now();
    let token = SpaceApiRouteEd25519Token::parse(TEST_ROUTE_TOKEN).unwrap();
    let fresh_token = SpaceApiRouteTokenState {
        token: token.clone(),
        expires_at: now + SPACE_API_ROUTE_TOKEN_REFRESH_WINDOW + Duration::from_secs(60),
        route_identity: SpaceApiRouteIdentity::default(),
    };
    let stale_token = SpaceApiRouteTokenState {
        token,
        expires_at: now + SPACE_API_ROUTE_TOKEN_REFRESH_WINDOW / 2,
        route_identity: SpaceApiRouteIdentity::default(),
    };

    let calls = AtomicUsize::new(0);
    let no_token = attached_session(session_id, 1);
    let no_token_result = refresh_file_watch_attached_if_route_token_stale_with(no_token, || {
        calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(attached_session(session_id, 99)))
    })
    .await
    .expect("no-token session should not refresh");
    assert_eq!(no_token_result.route_generation, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let calls = AtomicUsize::new(0);
    let mut fresh = attached_session(session_id, 2);
    fresh.space_api_route_token = Some(fresh_token);
    let fresh_result = refresh_file_watch_attached_if_route_token_stale_with(fresh, || {
        calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(attached_session(session_id, 99)))
    })
    .await
    .expect("fresh-token session should not refresh");
    assert_eq!(fresh_result.route_generation, 2);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let calls = AtomicUsize::new(0);
    let mut stale = attached_session(session_id, 3);
    stale.space_api_route_token = Some(stale_token);
    let stale_result = refresh_file_watch_attached_if_route_token_stale_with(stale, || {
        calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(attached_session(session_id, 99)))
    })
    .await
    .expect("stale-token session should refresh");
    assert_eq!(stale_result.route_generation, 99);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn process_events_map_to_native_data_plane_frames() {
    let mut seq = 0;
    let stdout = process_event_frames(
        ProcessEvent {
            process_id: "proc-1".to_string(),
            pid: Some(7),
            event: Some(spaced_process_event::Event::Stdout(SpacedProcessOutput {
                data: b"hello".to_vec(),
            })),
        },
        &mut seq,
    );
    assert_eq!(stdout.output_bytes, 5);
    assert_eq!(
        stdout.frames,
        vec![DataPlaneServerFrame::Stdout {
            seq: 1,
            data_base64: BASE64_STANDARD.encode("hello"),
        }]
    );

    let exited = process_event_frames(
        ProcessEvent {
            process_id: "proc-1".to_string(),
            pid: Some(7),
            event: Some(spaced_process_event::Event::Exit(SpacedProcessExit {
                outcome: Some(spaced_process_exit::Outcome::Exited(SpacedProcessExited {
                    exit_code: 13,
                })),
            })),
        },
        &mut seq,
    );
    assert_eq!(exited.output_bytes, 0);
    assert_eq!(
        exited.frames,
        vec![DataPlaneServerFrame::ProcessExited {
            process_id: "proc-1".to_string(),
            exit_code: 13,
            status: DataPlaneProcessExitStatus::Exited,
        }]
    );
}

#[test]
fn input_accepted_frame_uses_public_data_plane_contract() {
    let frame = server_frame_json(&DataPlaneServerFrame::InputAccepted { seq: 7 });
    let decoded: DataPlaneServerFrame = serde_json::from_str(&frame).unwrap();
    assert_eq!(decoded, DataPlaneServerFrame::InputAccepted { seq: 7 });
}

#[test]
fn file_events_map_to_native_data_plane_frames() {
    let frame = file_event_frame(
        FileEvent {
            path: "/workspace/src/lib.rs".to_string(),
            kind: SpacedFileEventKind::Modified as i32,
            observed_at: None,
        },
        7,
    );

    assert_eq!(
        frame,
        DataPlaneServerFrame::FileEvent {
            seq: 7,
            path: "/workspace/src/lib.rs".to_string(),
            kind: FileEventKind::Modified,
        }
    );
    assert_eq!(
        file_event_kind(SpacedFileEventKind::Unspecified as i32),
        FileEventKind::Unknown
    );
}

#[test]
fn resync_required_frame_uses_public_data_plane_contract() {
    let frame = DataPlaneServerFrame::ResyncRequired {
        reason: DataPlaneResyncReason::FileWatchRestarted,
    };
    let encoded = server_frame_json(&frame);
    let decoded: DataPlaneServerFrame = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, frame);
    assert_eq!(server_frame_event_name(&frame), "resync_required");
}

#[test]
fn process_output_frames_are_the_only_backend_cursor_frames() {
    assert!(process_frame_advances_backend_cursor(
        &DataPlaneServerFrame::Stdout {
            seq: 1,
            data_base64: "aGVsbG8=".to_string(),
        }
    ));
    assert!(process_frame_advances_backend_cursor(
        &DataPlaneServerFrame::Stderr {
            seq: 2,
            data_base64: "ZXJy".to_string(),
        }
    ));
    assert!(!process_frame_advances_backend_cursor(
        &DataPlaneServerFrame::ProcessExited {
            process_id: "proc-1".to_string(),
            exit_code: 0,
            status: DataPlaneProcessExitStatus::Exited,
        }
    ));
    assert!(!process_frame_advances_backend_cursor(
        &DataPlaneServerFrame::Closed {
            reason: StreamCloseReason::BackendClosed,
        }
    ));
}

#[test]
fn last_event_id_header_parses_unsigned_cursor() {
    let mut headers = HeaderMap::new();
    assert_eq!(last_event_id(&headers).unwrap(), None);

    headers.insert("last-event-id", HeaderValue::from_static("42"));
    assert_eq!(last_event_id(&headers).unwrap(), Some(42));

    headers.insert("last-event-id", HeaderValue::from_static("not-a-number"));
    last_event_id(&headers).unwrap_err();

    // The OpenAPI `int64` maximum is the last resumable cursor; anything above
    // it is a 400 rather than a stream whose `last + 1` would saturate onto an
    // id the client already acknowledged.
    headers.insert(
        "last-event-id",
        HeaderValue::from_static("9223372036854775807"),
    );
    assert_eq!(last_event_id(&headers).unwrap(), Some(MAX_LAST_EVENT_ID));
    headers.insert(
        "last-event-id",
        HeaderValue::from_static("9223372036854775808"),
    );
    last_event_id(&headers).unwrap_err();
    headers.insert(
        "last-event-id",
        HeaderValue::from_static("18446744073709551615"),
    );
    last_event_id(&headers).unwrap_err();
}

#[tokio::test]
async fn first_frame_attach_token_times_out_at_deadline() {
    let result = first_frame_attach_token_from_recv(
        std::future::pending::<Option<Result<Message, ()>>>(),
        Duration::from_millis(1),
    )
    .await;

    assert_eq!(result, Err(DataPlaneErrorCode::AttachDeadlineExceeded));
}

#[test]
fn process_sse_resume_emits_resync_at_next_event_id() {
    let session_id = DataPlaneSessionId::Process(Default::default());
    let replay = ProcessReplayState::load(session_id, Some(41));

    assert_eq!(replay.event_seq, 42);
    assert_eq!(replay.backend_output_cursor, None);
    assert_eq!(replay.pending.len(), 1);

    let fresh = ProcessReplayState::load(session_id, None);
    assert_eq!(fresh.event_seq, 0);
    assert!(fresh.pending.is_empty());
}

/// The public reconnection contract, shared by process and file-watch
/// sessions: `Last-Event-ID` resumes the live stream at `last + 1` behind a
/// `resync_required(cursor_replay_unavailable)` frame; no cursor, no frame.
/// Missed output is never replayed, so the backend cursor stays unset.
#[test]
fn last_event_id_resume_is_a_cursor_replay_unavailable_resync() {
    for kind in ["process", "file_watch"] {
        assert_eq!(resume_resync_frame(kind, None), None, "{kind}");
        assert_eq!(
            resume_resync_frame(kind, Some(41)),
            Some((
                42,
                DataPlaneServerFrame::ResyncRequired {
                    reason: DataPlaneResyncReason::CursorReplayUnavailable,
                },
            )),
            "{kind}"
        );
        // The largest cursor the request boundary admits still gets a fresh id.
        assert_eq!(
            resume_resync_frame(kind, Some(MAX_LAST_EVENT_ID)).map(|(seq, _)| seq),
            Some(MAX_LAST_EVENT_ID + 1),
            "{kind}"
        );
    }
    assert_eq!(
        resume_resync_frame("process", Some(0)).map(|(seq, _)| seq),
        Some(1)
    );
}

#[test]
fn attached_frame_uses_public_data_plane_contract() {
    let session_id = DataPlaneSessionId::Terminal(Default::default());
    let current_node_id = NodeId::new();
    let frame = attached_json(&AttachedSession {
        cache_epoch: 1,
        cache_key: None,
        session_id,
        sandbox_id: SandboxId::new(),
        org_id: OrgId::new(),
        kind: SessionKind::Terminal,
        route_generation: 3,
        revocation_epoch: 0,
        org_revocation_epoch: 0,
        current_node_id,
        backend_endpoint: backend_endpoint("http://127.0.0.1:9800"),
        target_port: None,
        target_protocol: None,
        backend_process_id: None,
        space_api_route_token: None,
        adapter_state_json: "{}".to_string(),
    });
    let decoded: DataPlaneServerFrame = serde_json::from_str(&frame).unwrap();
    assert_eq!(
        decoded,
        DataPlaneServerFrame::Attached {
            session_id,
            route_generation: 3,
            current_node_id: current_node_id.to_string(),
        }
    );
}

#[test]
fn revocation_state_is_active_only_for_active_status() {
    let mut revocation = DataPlaneRevocationState {
        epoch: 1,
        status: DataPlaneRevocationStatus::Active as i32,
        updated_at: None,
        reason: None,
        org_epoch: 1,
    };
    assert!(revocation_state_is_active(&revocation));

    revocation.status = DataPlaneRevocationStatus::Revoking as i32;
    assert!(!revocation_state_is_active(&revocation));

    revocation.status = DataPlaneRevocationStatus::Revoked as i32;
    assert!(!revocation_state_is_active(&revocation));

    revocation.status = DataPlaneRevocationStatus::Unspecified as i32;
    assert!(!revocation_state_is_active(&revocation));
}

#[test]
fn stream_bridge_error_labels_are_bounded() {
    assert_eq!(
        stream_bridge_error_label(&StreamBridgeError::InvalidClientFrame),
        "invalid_client_frame"
    );
    assert_eq!(
        stream_bridge_error_label(&StreamBridgeError::RouteState("invalid")),
        "route_state"
    );
    assert_eq!(
        stream_bridge_error_label(&StreamBridgeError::ClientClosed),
        "client_closed"
    );
}

#[test]
fn reconnect_reason_mapping_tracks_route_generation_changes() {
    assert_eq!(
        reconnect_reason_for_route_change(3, 3),
        ReconnectReason::WorkerUnavailable
    );
    assert_eq!(
        reconnect_reason_for_route_change(3, 4),
        ReconnectReason::RouteGenerationChanged
    );
    assert_eq!(
        proto_reconnect_reason(&ReconnectReason::RouteGenerationChanged),
        DataPlaneReconnectReason::RouteGenerationChanged
    );
}

#[test]
fn decrement_counter_saturates_at_zero() {
    let counter = AtomicUsize::new(1);
    assert_eq!(decrement_counter(&counter), 0);
    assert_eq!(counter.load(Ordering::Acquire), 0);
    assert_eq!(decrement_counter(&counter), 0);
    assert_eq!(counter.load(Ordering::Acquire), 0);
}

fn attached_session(session_id: DataPlaneSessionId, route_generation: u64) -> AttachedSession {
    AttachedSession {
        cache_epoch: 1,
        cache_key: None,
        session_id,
        sandbox_id: SandboxId::new(),
        org_id: OrgId::new(),
        kind: session_id.kind(),
        route_generation,
        revocation_epoch: 1,
        org_revocation_epoch: 1,
        current_node_id: NodeId::new(),
        backend_endpoint: backend_endpoint("http://worker:9800"),
        target_port: None,
        target_protocol: None,
        adapter_state_json: "{}".to_string(),
        backend_process_id: None,
        space_api_route_token: None,
    }
}

#[tokio::test]
async fn close_delimited_http_retains_attachment_until_downstream_body_drop() {
    let (client, ingress) = tokio::io::duplex(1024);
    let (tx, mut requests) = tokio::sync::mpsc::channel(8);
    let (response_tx, response_rx) = tokio::sync::mpsc::channel(8);
    let done = CancellationToken::new();
    let progress = Progress::new();
    let owner = Completion::new(done.clone(), progress.clone());
    let pump = tokio::spawn(pump_service_port_ingress_io_with_stream(
        ingress,
        tx,
        ReceiverStream::new(response_rx),
        CancellationToken::new(),
        done.clone(),
        progress,
        true,
    ));
    let (mut sender, connection) = http1::handshake(TokioIo::new(client)).await.unwrap();
    let driver = tokio::spawn(connection);
    let reply = tokio::spawn(async move {
        let request = requests.recv().await.unwrap();
        assert!(matches!(
            request.message,
            Some(service_port_ingress_frame::Message::Data(_))
        ));
        for message in [
            service_port_ingress_frame::Message::Data(
                b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nslow-tail".to_vec(),
            ),
            service_port_ingress_frame::Message::Eof(()),
        ] {
            response_tx
                .send(Ok(ServicePortIngressFrame {
                    message: Some(message),
                }))
                .await
                .unwrap();
        }
        while let Some(frame) = requests.recv().await {
            if matches!(
                frame.message,
                Some(service_port_ingress_frame::Message::Complete(_))
            ) {
                return;
            }
        }
        panic!("attachment closed without downstream completion");
    });
    let response = sender
        .send_request(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = owner.body(Body::new(response.into_body()));
    drop(owner);
    assert!(
        !done.is_cancelled(),
        "HTTP connection EOF is earlier than downstream body completion"
    );
    let data = body.collect().await.unwrap().to_bytes();
    assert_eq!(data, "slow-tail");
    driver.await.unwrap().unwrap();
    done.cancelled().await;
    pump.await.unwrap().unwrap();
    reply.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn ingress_completion_timeout_survives_guest_eof() {
    let (io, mut client) = tokio::io::duplex(64);
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let (upstream, frames) = tokio::sync::mpsc::channel(4);
    let progress = Progress::new();
    let abort = progress.abort.clone();
    let pump = tokio::spawn(pump_service_port_ingress_io_with_stream(
        io,
        tx,
        ReceiverStream::new(frames),
        CancellationToken::new(),
        CancellationToken::new(),
        progress,
        true,
    ));
    upstream
        .send(Ok(ServicePortIngressFrame {
            message: Some(service_port_ingress_frame::Message::Eof(())),
        }))
        .await
        .unwrap();
    let mut body = Vec::new();
    client.read_to_end(&mut body).await.unwrap();
    assert!(!pump.is_finished());
    tokio::time::advance(
        pplx_space_net_gateway_client::service_port_ingress_idle_timeout_from_env(),
    )
    .await;
    pump.await.unwrap().unwrap_err();
    assert!(abort.is_cancelled());
    assert!(rx.recv().await.is_none());
}

#[tokio::test]
async fn ingress_cancellation_releases_a_backpressured_response() {
    let (io, _client) = tokio::io::duplex(1);
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let (upstream, frames) = tokio::sync::mpsc::channel(1);
    let shutdown = CancellationToken::new();
    let progress = Progress::new();
    let abort = progress.abort.clone();
    let pump = tokio::spawn(pump_service_port_ingress_io_with_stream(
        io,
        tx,
        ReceiverStream::new(frames),
        shutdown.clone(),
        CancellationToken::new(),
        progress,
        true,
    ));
    upstream
        .send(Ok(ServicePortIngressFrame {
            message: Some(service_port_ingress_frame::Message::Data(vec![1; 64])),
        }))
        .await
        .unwrap();
    tokio::task::yield_now().await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), pump)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(abort.is_cancelled());
    assert!(rx.recv().await.is_none());
}

#[test]
fn ecw_ingress_revalidation_compares_the_complete_execution_identity() {
    let mut route = ecw_resolved_route(SandboxId::new(), 1, backend_endpoint("192.0.2.1:9800"));
    let mut token = route_token_state_for_attempt(SandboxActivationAttemptId::new());
    token.route_identity = test_route_identity().into();
    route.service_port_ingress_token = Some(token);
    assert!(route.same_ingress_execution(&route));
    for field in ["execution", "attempt", "pod", "missing"] {
        let mut fresh = route.clone();
        let identity = &mut fresh
            .service_port_ingress_token
            .as_mut()
            .unwrap()
            .route_identity;
        match field {
            "execution" => {
                identity.execution_generation = Some(ExecutionGeneration::new(12).unwrap())
            }
            "attempt" => identity.activation_attempt_id = Some(SandboxActivationAttemptId::new()),
            "pod" => identity.pod_uid = Some(PodUid::parse("replacement-pod").unwrap()),
            "missing" => fresh.service_port_ingress_token = None,
            _ => unreachable!(),
        }
        assert!(!route.same_ingress_execution(&fresh), "{field}");
    }
}

#[tokio::test]
async fn ecw_ingress_does_not_replay_after_upstream_accepts_request_bytes() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }
    let state = test_data_plane_state().await;
    let target = EcwSandboxHost {
        sandbox_id: SandboxId::new(),
        port: 8080,
    };
    let route = ecw_resolved_route(target.sandbox_id, 1, backend_endpoint("192.0.2.1:9800"));
    let (client, mut upstream) = tokio::io::duplex(4096);
    let done = CancellationToken::new();
    let io = IngressIo::new(client, Completion::new(done.clone(), Progress::new()));
    let accepted = tokio::spawn(async move {
        let mut bytes = Vec::new();
        let mut buf = [0; 1024];
        while !bytes.ends_with(b"unique-write") {
            let count = upstream.read(&mut buf).await.unwrap();
            assert_ne!(count, 0);
            bytes.extend_from_slice(&buf[..count]);
        }
        assert_eq!(bytes.windows(5).filter(|part| *part == b"POST ").count(), 1);
        // The operation may have executed; close before sending any response.
        drop(upstream);
    });
    let request = Request::builder()
        .method("POST")
        .uri("/mutate")
        .body(Body::from("unique-write"))
        .unwrap();
    let result = ecw_service_ingress_proxy_with_io(state.clone(), target, route, request, io).await;
    result.unwrap_err();
    accepted.await.unwrap();
    state.upgrade_tasks.close();
    state.upgrade_tasks.wait().await;
    assert!(done.is_cancelled());
}

/// A 429 from the sandbox's own service is relayed, not a data-plane limit, so
/// it must not land in `data_plane.http.too_many_requests` as `unclassified`.
#[tokio::test]
async fn ecw_ingress_marks_relayed_sandbox_429_as_sandbox_service() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }
    let state = test_data_plane_state().await;
    let target = EcwSandboxHost {
        sandbox_id: SandboxId::new(),
        port: 8080,
    };
    let route = ecw_resolved_route(target.sandbox_id, 1, backend_endpoint("192.0.2.1:9800"));
    let (client, mut upstream) = tokio::io::duplex(4096);
    let io = IngressIo::new(
        client,
        Completion::new(CancellationToken::new(), Progress::new()),
    );
    let sandbox = tokio::spawn(async move {
        let mut bytes = Vec::new();
        let mut buf = [0; 1024];
        while !bytes.ends_with(b"\r\n\r\n") {
            let count = upstream.read(&mut buf).await.unwrap();
            assert_ne!(count, 0);
            bytes.extend_from_slice(&buf[..count]);
        }
        upstream
            .write_all(b"HTTP/1.1 429 Too Many Requests\r\ncontent-length: 0\r\n\r\n")
            .await
            .unwrap();
        upstream
    });
    let request = Request::get("/").body(Body::empty()).unwrap();
    let response = ecw_service_ingress_proxy_with_io(state.clone(), target, route, request, io)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        response.extensions().get::<TooManyRequestsReason>(),
        Some(&TooManyRequestsReason::SandboxService),
    );
    drop(response);
    drop(sandbox.await.unwrap());
    state.upgrade_tasks.close();
    state.upgrade_tasks.wait().await;
}

#[tokio::test]
async fn legacy_ingress_eof_delivers_buffered_body_without_completion_frames() {
    let (io, mut client) = tokio::io::duplex(8);
    let (tx, mut requests) = tokio::sync::mpsc::channel(4);
    let done = CancellationToken::new();
    let progress = Progress::new();
    let abort = progress.abort.clone();
    let frames = tokio_stream::iter([Ok(ServicePortIngressFrame {
        message: Some(service_port_ingress_frame::Message::Data(vec![b'x'; 1024])),
    })]);
    let pump = tokio::spawn(pump_service_port_ingress_io_with_stream(
        io,
        tx,
        frames,
        CancellationToken::new(),
        done.clone(),
        progress,
        false,
    ));
    client.shutdown().await.unwrap();
    assert!(
        requests.recv().await.is_none(),
        "legacy upload closes the RPC direction without an EOF frame"
    );
    let mut body = Vec::new();
    client.read_to_end(&mut body).await.unwrap();
    assert_eq!(body, vec![b'x'; 1024]);
    pump.await.unwrap().unwrap();
    assert!(
        !abort.is_cancelled(),
        "normal legacy EOF must not cancel buffered delivery"
    );
}

/// Read one server WebSocket frame header from `tcp` and return its opcode and
/// payload. Server frames are never masked, so the header is two bytes plus an
/// optional extended length.
pub(crate) async fn read_server_frame(tcp: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
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

pub(crate) fn grpc_test_response(bytes: Vec<u8>) -> Response {
    let mut message = vec![0];
    message.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    message.extend(bytes);
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));
    let frames = tokio_stream::iter([
        Ok::<_, Infallible>(hyper::body::Frame::data(bytes::Bytes::from(message))),
        Ok(hyper::body::Frame::trailers(trailers)),
    ]);
    Response::builder()
        .header("content-type", "application/grpc")
        .body(Body::new(http_body_util::StreamBody::new(frames)))
        .unwrap()
}

fn grpc_ingress_reply_after_request(request: Request<Body>) -> Response {
    use prost::Message as _;
    let output = futures_util::stream::once(async move {
        let mut body = request.into_body();
        let mut buffered = Vec::new();
        loop {
            let frame = body.frame().await.expect("application request").unwrap();
            if let Some(bytes) = frame.data_ref() {
                buffered.extend_from_slice(bytes);
            }
            while buffered.len() >= 5 {
                let len = u32::from_be_bytes(buffered[1..5].try_into().unwrap()) as usize;
                if buffered.len() < len + 5 {
                    break;
                }
                let frame = ServicePortIngressFrame::decode(&buffered[5..5 + len]).unwrap();
                buffered.drain(..5 + len);
                if matches!(
                    frame.message,
                    Some(service_port_ingress_frame::Message::Data(_))
                ) {
                    let reply = ServicePortIngressFrame {
                        message: Some(service_port_ingress_frame::Message::Data(
                            b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nreplacement".to_vec(),
                        )),
                    }
                    .encode_to_vec();
                    let mut bytes = vec![0];
                    bytes.extend_from_slice(&(reply.len() as u32).to_be_bytes());
                    bytes.extend(reply);
                    return Ok::<_, Infallible>(hyper::body::Frame::data(bytes::Bytes::from(
                        bytes,
                    )));
                }
            }
        }
    });
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));
    let frames = output.chain(tokio_stream::iter([Ok(hyper::body::Frame::trailers(
        trailers,
    ))]));
    Response::builder()
        .header("content-type", "application/grpc")
        .body(Body::new(http_body_util::StreamBody::new(frames)))
        .unwrap()
}

#[tokio::test]
async fn cached_ingress_refreshes_replaced_execution_but_not_denied_access() {
    if !pplx_space_test_env::enter_test_process(&[
        ("SPACE_INTERNAL_TOKEN", Some(std::ffi::OsStr::new("test"))),
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
    ]) {
        return;
    }
    use prost::Message as _;
    for (native, refreshable) in [(false, true), (true, true), (false, false), (true, false)] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let sandbox = SandboxId::new();
        let target = EcwSandboxHost {
            sandbox_id: sandbox,
            port: 8080,
        };
        let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Port);
        let client_token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "a".repeat(64));
        let fresh_token = proto_service_port_route_token();
        let mut stale = ecw_resolved_route(sandbox, 1, backend_endpoint(&endpoint));
        stale.origin.target = target.clone();
        stale.origin.transport = EcwRouteTransport::ServicePortIngress;
        stale.service_port_ingress_token = parse_proto_space_api_route_token(
            SpaceApiRouteTokenRouteSource::SandboxServiceRoute,
            proto_service_port_route_token(),
        );
        let fresh = UnstableResolveSandboxServiceRouteResponse {
            sandbox_id: sandbox.to_string(),
            org_id: OrgId::new().to_string(),
            port: 8080,
            lifecycle: "started".into(),
            route_generation: 2,
            current_node_id: stale.current_node_id.to_string(),
            backend: Some(DataPlaneBackendEndpoint {
                node_id: stale.current_node_id.to_string(),
                endpoint: endpoint.clone(),
                transport: None,
            }),
            template_id: Some("seed".into()),
            space_api_route_token: None,
            service_port_ingress_token: Some(fresh_token.clone()),
        }
        .encode_to_vec();
        let mut native_fresh = route_response_with_token(session_id, Some(fresh_token));
        native_fresh.sandbox_id = sandbox.to_string();
        native_fresh.target_port = Some(8080);
        native_fresh.backend.as_mut().unwrap().endpoint = endpoint.clone();
        let mut native_stale = native_fresh.clone();
        native_stale.route_generation = 1;
        native_stale.space_api_route_token = Some(proto_service_port_route_token());
        let fresh = if native {
            native_fresh.encode_to_vec()
        } else {
            fresh
        };
        let opens = Arc::new(AtomicUsize::new(0));
        let resolves = Arc::new(AtomicUsize::new(0));
        let service = Router::new().fallback({
            let opens = opens.clone();
            let resolves = resolves.clone();
            move |request: Request<Body>| {
                let opens = opens.clone();
                let resolves = resolves.clone();
                let fresh = fresh.clone();
                async move {
                    if request.uri().path().ends_with("/Check") {
                        return grpc_test_response(vec![8, 1]);
                    }
                    if request
                        .uri()
                        .path()
                        .ends_with("/UnstableResolveSandboxServiceRoute")
                        || request
                            .uri()
                            .path()
                            .ends_with("/ResolveDataPlaneSessionRoute")
                    {
                        resolves.fetch_add(1, Ordering::SeqCst);
                        return grpc_test_response(fresh);
                    }
                    assert!(request.uri().path().ends_with("/ServicePortIngress"));
                    if opens.fetch_add(1, Ordering::SeqCst) == 0 {
                        let status = if refreshable {
                            activation_attempt_rejection()
                        } else {
                            tonic::Status::permission_denied("revoked authorization")
                        };
                        return status.into_http::<Body>();
                    }
                    grpc_ingress_reply_after_request(request)
                }
            }
        });
        let server = tokio::spawn(async move {
            axum::serve(listener, service).await.unwrap();
        });
        let mut state = test_data_plane_state().await;
        state.cluster_api = ClusterApiClients::service_only(
            pplx_space_node_api_client::cluster::connect(endpoint, "test")
                .await
                .unwrap(),
        );
        state.ecw_hosts = Arc::new(EcwHostConfig::from_env_value(Some(
            r#"[{"host":"example.test"}]"#,
        )));
        let result = if native {
            let key = native_route_cache_key(
                session_id,
                &token_sha256(&client_token),
                session_id.kind().connect_scope().as_str(),
            );
            let mut attached =
                crate::native::attached_session_from_route_response(native_stale, session_id, 1)
                    .unwrap();
            attached.cache_key = Some(key.clone());
            state
                .route_cache
                .remember_native(key, attached, Duration::from_secs(60));
            let request = Request::builder()
                .uri(format!("/v1/sessions/{session_id}/proxy/service"))
                .header("authorization", format!("Bearer {client_token}"))
                .body(Body::empty())
                .unwrap();
            crate::native::proxy_session(
                State(state.clone()),
                Path(crate::native::ProxySessionPath {
                    session_id: session_id.to_string(),
                }),
                request,
            )
            .await
            .map_err(|e| format!("{e:?}"))
        } else {
            remember_ecw_ingress_route(&state, &target, stale);
            let request = Request::builder()
                .uri("/service")
                .header("host", format!("8080-{sandbox}.example.test"))
                .body(Body::empty())
                .unwrap();
            ecw_service_proxy(State(state.clone()), request)
                .await
                .map_err(|e| format!("{e:?}"))
        };
        if refreshable {
            let response = result.unwrap_or_else(|error| {
                panic!(
                    "cached recovery: {error:?}, opens={}, resolves={}",
                    opens.load(Ordering::SeqCst),
                    resolves.load(Ordering::SeqCst)
                )
            });
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                "replacement"
            );
            assert_eq!(opens.load(Ordering::SeqCst), 2);
            assert_eq!(
                resolves.load(Ordering::SeqCst),
                2,
                "recover, then reauthorize before bytes"
            );
        } else {
            result.expect_err("terminal authorization must not retry");
            assert_eq!(opens.load(Ordering::SeqCst), 1);
            assert_eq!(resolves.load(Ordering::SeqCst), 0);
        }
        state.stream_shutdown_token.cancel();
        server.abort();
    }
}

// ECW keeps its status shapes, but says when to retry a refusal the guest
// made before starting anything, and finds a full disk or quota by the
// guest's reason rather than its wording.
#[test]
fn ecw_answers_refusals_with_retry_after_and_full_disks_by_reason() {
    let refused = pplx_space_common_grpc::refused_before_start(
        tonic::Status::resource_exhausted("too many concurrent filesystem file operations"),
        std::time::Duration::from_millis(2500),
    );
    let error = ecw_connect::tonic_status_to_ecw_error(refused);
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        error.retry_after,
        Some(std::time::Duration::from_millis(2500))
    );
    assert_eq!(
        error.message,
        "too many concurrent filesystem file operations"
    );

    let mut quota = tonic::Status::resource_exhausted("Disk quota exceeded (os error 122)");
    quota.metadata_mut().insert(
        pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY,
        pplx_space_common_core::spaced::GUEST_STORAGE_FULL
            .parse()
            .unwrap(),
    );
    assert_eq!(
        ecw_connect::tonic_status_to_ecw_error(quota).status,
        StatusCode::INSUFFICIENT_STORAGE
    );

    // Any other refusal (a guest shutting down) keeps its status, 502 for
    // E2B clients, and gains the hint; an unmarked one stays as it was.
    let shutting_down =
        ecw_connect::tonic_status_to_ecw_error(pplx_space_common_grpc::refused_before_start(
            tonic::Status::unavailable("guest shutting down"),
            std::time::Duration::from_millis(2500),
        ));
    assert_eq!(shutting_down.status, StatusCode::BAD_GATEWAY);
    assert_eq!(
        shutting_down.retry_after,
        Some(std::time::Duration::from_millis(2500))
    );
    let unmarked =
        ecw_connect::tonic_status_to_ecw_error(tonic::Status::unavailable("transport closed"));
    assert_eq!(unmarked.status, StatusCode::BAD_GATEWAY);
    assert_eq!(unmarked.retry_after, None);

    let plain =
        ecw_connect::tonic_status_to_ecw_error(tonic::Status::resource_exhausted("unmarked limit"));
    assert_eq!(plain.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(plain.retry_after, None);
}
