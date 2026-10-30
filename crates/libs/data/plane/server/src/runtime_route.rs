use axum::http::HeaderMap;
use std::fmt;
use std::future::Future;
use std::time::{Duration as StdDuration, Instant};

use pplx_space_common_grpc::SandboxServiceRouteNotReady;
use pplx_space_common_svc::space_api_route::{
    RouteIdentityRemediation, route_identity_dimension_from_status,
    route_identity_remediation_from_status,
};
use pplx_space_metrics::data_plane::{RouteIdentityRefreshOutcome, RouteIdentityRefreshProtocol};
use pplx_space_node_api_client::proto::{
    SandboxServiceRouteCredentialKind, SandboxServiceRouteCredentialProof, SandboxServiceRouteKind,
    SandboxServiceRouteTransport, UnstableResolveSandboxServiceRouteRequest,
};
use tonic::{Request, transport::Channel};

use super::*;

const ECW_ROUTE_RESOLVE_OPERATION: &str = "unstable_resolve_sandbox_service_route";
const ECW_ROUTE_CACHE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const ECW_ROUTE_CACHE_SWEEP_MAX_ENTRIES: usize = 16 * 1024;

pub(super) async fn resolve_ecw_envd_route(
    state: &DataPlaneState,
    headers: &HeaderMap,
) -> Result<EcwResolvedRoute, EcwProxyError> {
    let target = state
        .ecw_hosts
        .parse_sandbox_host(headers)
        .ok_or_else(|| EcwProxyError::not_found("Endpoint"))?;
    validate_ecw_sandbox_headers(headers, &target)?;
    if target.port != ECW_ENVD_PORT {
        return Err(EcwProxyError::not_found("Endpoint"));
    }
    resolve_ecw_route(state, &target, headers, EcwRouteTransport::Direct).await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EcwRouteTransport {
    Direct,
    ServicePortIngress,
}

impl EcwRouteTransport {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::ServicePortIngress => "service_port_ingress",
        }
    }

    fn proto(self) -> SandboxServiceRouteTransport {
        match self {
            Self::Direct => SandboxServiceRouteTransport::Direct,
            Self::ServicePortIngress => SandboxServiceRouteTransport::ServicePortIngress,
        }
    }
}

impl fmt::Display for EcwRouteTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub(super) async fn resolve_ecw_route(
    state: &DataPlaneState,
    target: &EcwSandboxHost,
    headers: &HeaderMap,
    transport: EcwRouteTransport,
) -> Result<EcwResolvedRoute, EcwProxyError> {
    sweep_expired_ecw_routes(state);
    let origin = EcwRouteOrigin {
        target: target.clone(),
        transport,
        envd_access_token_sha256: header_sha256(headers, ECW_ENVD_ACCESS_TOKEN_HEADER),
        traffic_access_token_sha256: header_sha256(headers, ECW_TRAFFIC_ACCESS_TOKEN_HEADER),
        api_key_sha256: header_sha256(headers, ECW_API_KEY_HEADER),
    };
    let cache_key = origin.cache_key();
    let cache_lookup = state.route_cache.lookup_ecw(&cache_key);
    pplx_space_metrics::data_plane::route_cache_lookup(
        "ecw",
        "ecw_service",
        cache_lookup.result_label(),
    );
    if let RouteCacheLookup::Hit(route) = cache_lookup
        && !ecw_route_tokens_need_refresh(&route)
    {
        return Ok(route);
    }

    resolve_ecw_route_uncached(
        state,
        &origin,
        &cache_key,
        ECW_SERVICE_ROUTE_RESOLVE_TIMEOUT,
    )
    .await
}

/// Ask ClusterApi for the authoritative route and install it, skipping the
/// cache read.
///
/// Separate from [`resolve_ecw_route`] because a stale-identity refresh must not
/// consult the cache it is replacing, and must reach the same slot and target as
/// the original resolve.
pub(super) async fn resolve_ecw_route_uncached(
    state: &DataPlaneState,
    origin: &EcwRouteOrigin,
    cache_key: &str,
    timeout: StdDuration,
) -> Result<EcwResolvedRoute, EcwProxyError> {
    let target = &origin.target;
    let mut request = Request::new(UnstableResolveSandboxServiceRouteRequest {
        sandbox_id: target.sandbox_id.to_string(),
        port: target.port,
        route_kind: ecw_sandbox_service_route_kind(target) as i32,
        transport: origin.transport.proto() as i32,
        credential_proofs: ecw_route_credential_proofs(
            origin.envd_access_token_sha256.clone(),
            origin.traffic_access_token_sha256.clone(),
            origin.api_key_sha256.clone(),
        ),
    });
    request.set_timeout(timeout);

    let mut client = state.cluster_api.for_sandbox(&target.sandbox_id);
    let route = bounded_ecw_route_resolve(
        target,
        timeout,
        client.unstable_resolve_sandbox_service_route(request),
    )
    .await
    .map_err(|error| match error {
        EcwRouteResolveError::Status(status) => ecw_route_error(status, origin),
        EcwRouteResolveError::Timeout => route_resolve_timeout_error(),
    })?
    .into_inner();
    let backend = route
        .backend
        .ok_or_else(EcwProxyError::sandbox_data_plane_unavailable)?;
    let sandbox_id = route
        .sandbox_id
        .parse::<SandboxId>()
        .map_err(|_| EcwProxyError::internal("Invalid sandbox route"))?;
    // Only upload admission, the transfer rate and upload attribution need the
    // org, so a route whose org doesn't parse still serves everything else.
    let org_id = route.org_id.parse::<OrgId>().ok();
    // Only worth a warning when admission, which needs the org, is on.
    if org_id.is_none() && state.uploads.is_some() {
        tracing::warn!(%sandbox_id, "ECW sandbox route has no valid org id");
    }

    let resolved = EcwResolvedRoute {
        cache_epoch: state.route_cache.next_cache_epoch(),
        origin: origin.clone(),
        sandbox_id,
        org_id,
        template_id: route.template_id,
        route_generation: route.route_generation,
        current_node_id: route
            .current_node_id
            .parse()
            .map_err(|_| EcwProxyError::internal("Invalid sandbox route"))?,
        backend_endpoint: backend
            .endpoint
            .parse()
            .map_err(|_| EcwProxyError::internal("Invalid sandbox route"))?,
        space_api_route_token: route.space_api_route_token.and_then(|token| {
            parse_proto_space_api_route_token(
                SpaceApiRouteTokenRouteSource::SandboxServiceRouteBridge,
                token,
            )
        }),
        service_port_ingress_token: route.service_port_ingress_token.and_then(|token| {
            parse_proto_space_api_route_token(
                SpaceApiRouteTokenRouteSource::SandboxServiceRoute,
                token,
            )
        }),
    };
    state.route_cache.remember_ecw(
        cache_key.to_string(),
        resolved.clone(),
        state.route_cache_ttl,
    );
    Ok(resolved)
}

/// Whether the route cache would still hand this exact route to a new request
/// without asking ClusterApi.
///
/// An ingress open that finishes while this holds sends its first byte under
/// authorization at most about one route-cache TTL old, as a cache hit does, so
/// the proxy skips the post-open resolve, which would also repeat ClusterApi's
/// node-port setup. The proxy renews before the first byte when the open
/// outlasted what remained of the cache entry (such as a slow implicit wake),
/// the entry was replaced or evicted, or a token is inside its refresh window.
pub(super) fn ecw_route_still_cached(state: &DataPlaneState, route: &EcwResolvedRoute) -> bool {
    matches!(
        state.route_cache.lookup_ecw(&route.origin.cache_key()),
        RouteCacheLookup::Hit(current) if current.cache_epoch == route.cache_epoch
    ) && !ecw_route_tokens_need_refresh(route)
}

/// Whether an endpoint rejected this call because the route token names a
/// superseded incarnation or has expired, which re-resolving can fix.
///
/// The verdict comes from the rejecting endpoint, not from inspecting the status
/// message: a mismatch on the sandbox ID looks identical in prose but must never
/// be retried.
pub(super) fn is_refreshable_route_identity_rejection(status: &tonic::Status) -> bool {
    route_identity_remediation_from_status(status) == Some(RouteIdentityRemediation::RefreshRoute)
}

/// Re-resolve a route whose token is aging or whose endpoint rejected it as
/// refreshable.
///
/// Concurrent callers holding the same stale route collapse onto one
/// authoritative resolve: the first to take the slot evicts and resolves, and
/// the rest observe the route it installed. Eviction is conditional on the
/// rejected identity still being the cached one, so a slow caller cannot
/// discard a route that a faster one already refreshed.
pub(super) async fn refresh_ecw_route(
    state: &DataPlaneState,
    stale: &EcwResolvedRoute,
    deadline: Instant,
) -> Result<EcwResolvedRoute, EcwProxyError> {
    let cache_key = stale.origin.cache_key();
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(route_resolve_timeout_error());
    }
    let _slot = tokio::time::timeout(
        remaining,
        state.route_cache.acquire_refresh_slot(&cache_key),
    )
    .await
    .map_err(|_| route_resolve_timeout_error())?;
    {
        match state.route_cache.lookup_ecw(&cache_key) {
            RouteCacheLookup::Hit(current)
                if current.cache_epoch != stale.cache_epoch
                    && !ecw_route_tokens_need_refresh(&current) =>
            {
                Ok(current)
            }
            _ => {
                state
                    .route_cache
                    .forget_ecw_if_current(&cache_key, stale.cache_epoch);
                let remaining = deadline
                    .saturating_duration_since(Instant::now())
                    .min(ECW_SERVICE_ROUTE_RESOLVE_TIMEOUT);
                if remaining.is_zero() {
                    Err(route_resolve_timeout_error())
                } else {
                    resolve_ecw_route_uncached(state, &stale.origin, &cache_key, remaining).await
                }
            }
        }
    }
}

/// Run a route-bound operation with fresh credentials that can be rebuilt after
/// a structured route-token rejection. End-to-end policies include the first attempt, refresh-lock wait,
/// authoritative route resolution, and retry in one deadline; transfer policies
/// bound route recovery while allowing established data movement to complete.
async fn bounded_ecw_route_call_inner<T, Call, Fut, E, AttemptError>(
    state: &DataPlaneState,
    route: EcwResolvedRoute,
    policy: EcwRouteCallPolicy,
    call: Call,
    map_status: E,
) -> Result<(T, EcwResolvedRoute), EcwProxyError>
where
    Call: Fn(EcwResolvedRoute, StdDuration) -> Fut,
    Fut: Future<Output = Result<T, AttemptError>>,
    AttemptError: Into<EcwRouteAttemptError>,
    E: Fn(tonic::Status) -> EcwProxyError,
{
    let started_at = Instant::now();
    let deadline = started_at + policy.timeout;
    let mut route = route;
    // Stream reattachment can hold a route long after its cache entry and
    // credential expire. Refresh before opening the backend, within the same
    // recovery budget, including when the peer cannot signal token expiry.
    if ecw_route_tokens_need_refresh(&route) {
        route = refresh_ecw_route(state, &route, deadline).await?;
    }
    let mut refresh_dimension = None;
    let mut transport_retried = false;

    loop {
        let remaining = match policy.timeout_scope {
            EcwCallTimeoutScope::EndToEnd => deadline.saturating_duration_since(Instant::now()),
            EcwCallTimeoutScope::RouteRecovery => policy.timeout,
        };
        if policy.timeout_scope == EcwCallTimeoutScope::EndToEnd && remaining.is_zero() {
            if let Some(dimension) = refresh_dimension {
                pplx_space_metrics::data_plane::route_identity_refresh(
                    RouteIdentityRefreshProtocol::Ecw,
                    dimension,
                    RouteIdentityRefreshOutcome::DeadlineExceeded,
                );
            }
            return Err(ecw_backend_timeout(
                state,
                &route,
                policy.operation,
                policy.timeout,
                policy.timeout_observer,
                started_at,
            )
            .await);
        }

        let attempt = match policy.timeout_scope {
            EcwCallTimeoutScope::EndToEnd => {
                tokio::time::timeout(remaining, async {
                    call(route.clone(), remaining).await.map_err(Into::into)
                })
                .await
            }
            EcwCallTimeoutScope::RouteRecovery => {
                Ok(call(route.clone(), remaining).await.map_err(Into::into))
            }
        };
        match attempt {
            Ok(Ok(value)) => {
                if let Some(dimension) = refresh_dimension {
                    pplx_space_metrics::data_plane::route_identity_refresh(
                        RouteIdentityRefreshProtocol::Ecw,
                        dimension,
                        RouteIdentityRefreshOutcome::Succeeded,
                    );
                }
                record_ecw_backend_rpc_latency(policy.operation, BackendRpcResult::Ok, started_at);
                return Ok((value, route));
            }
            Ok(Err(EcwRouteAttemptError::Status(status)))
                if refresh_dimension.is_none()
                    && is_refreshable_route_identity_rejection(&status) =>
            {
                let dimension = refresh_dimension_label(&status);
                let refresh_deadline = match policy.timeout_scope {
                    EcwCallTimeoutScope::EndToEnd => deadline,
                    EcwCallTimeoutScope::RouteRecovery => Instant::now() + policy.timeout,
                };
                match refresh_ecw_route(state, &route, refresh_deadline).await {
                    Ok(refreshed) => {
                        tracing::debug!(
                            operation = policy.operation,
                            sandbox_id = %route.sandbox_id,
                            dimension = %dimension,
                            stale_route_generation = route.route_generation,
                            refreshed_route_generation = refreshed.route_generation,
                            "re-resolved sandbox route after refreshable route token rejection",
                        );
                        route = refreshed;
                        refresh_dimension = Some(dimension);
                        // The refreshed-route attempt is the one safe replay
                        // authorized by the structured rejection. Do not add a
                        // second replay if that attempt then loses transport.
                        transport_retried = true;
                    }
                    Err(error) => {
                        let outcome = if refresh_deadline
                            .saturating_duration_since(Instant::now())
                            .is_zero()
                        {
                            RouteIdentityRefreshOutcome::DeadlineExceeded
                        } else {
                            RouteIdentityRefreshOutcome::ResolveFailed
                        };
                        pplx_space_metrics::data_plane::route_identity_refresh(
                            RouteIdentityRefreshProtocol::Ecw,
                            dimension,
                            outcome,
                        );
                        record_ecw_backend_rpc_latency(
                            policy.operation,
                            BackendRpcResult::Status,
                            started_at,
                        );
                        tracing::warn!(
                            operation = policy.operation,
                            sandbox_id = %route.sandbox_id,
                            dimension = %dimension,
                            "could not re-resolve sandbox route after refreshable route token rejection",
                        );
                        return Err(error);
                    }
                }
            }
            Ok(Err(EcwRouteAttemptError::Status(status))) => {
                let should_retry = reset_ecw_transport_channel_and_should_retry(
                    state,
                    &route,
                    &status,
                    policy.operation,
                    policy.transport_retry,
                    transport_retried,
                );
                if should_retry {
                    tracing::debug!(
                        operation = policy.operation,
                        sandbox_id = %route.sandbox_id,
                        route_generation = route.route_generation,
                        current_node_id = %route.current_node_id,
                        backend_endpoint = %route.backend_endpoint,
                        grpc_code = ?status.code(),
                        grpc_message = %status.message(),
                        "retrying ECW backend call after transport failure",
                    );
                    transport_retried = true;
                    continue;
                }
                if let Some(dimension) = refresh_dimension {
                    pplx_space_metrics::data_plane::route_identity_refresh(
                        RouteIdentityRefreshProtocol::Ecw,
                        dimension,
                        RouteIdentityRefreshOutcome::RetryFailed,
                    );
                }
                record_ecw_backend_rpc_latency(
                    policy.operation,
                    BackendRpcResult::Status,
                    started_at,
                );
                let error = map_status(status);
                return Err(crate::ecw_runtime_access_error(state, route.sandbox_id, error).await);
            }
            Ok(Err(EcwRouteAttemptError::Proxy(error))) => {
                record_ecw_backend_rpc_latency(
                    policy.operation,
                    BackendRpcResult::Status,
                    started_at,
                );
                return Err(crate::ecw_runtime_access_error(state, route.sandbox_id, error).await);
            }
            Err(_) => {
                if let Some(dimension) = refresh_dimension {
                    pplx_space_metrics::data_plane::route_identity_refresh(
                        RouteIdentityRefreshProtocol::Ecw,
                        dimension,
                        RouteIdentityRefreshOutcome::DeadlineExceeded,
                    );
                }
                return Err(ecw_backend_timeout(
                    state,
                    &route,
                    policy.operation,
                    policy.timeout,
                    policy.timeout_observer,
                    started_at,
                )
                .await);
            }
        }
    }
}

pub(super) fn reset_ecw_transport_channel_and_should_retry(
    state: &DataPlaneState,
    route: &EcwResolvedRoute,
    status: &tonic::Status,
    operation: &'static str,
    transport_retry: EcwTransportRetry,
    transport_retried: bool,
) -> bool {
    let channel_reset =
        reset_ecw_backend_channel_on_transport_status(state, route, status, operation);
    transport_retry == EcwTransportRetry::Allowed && !transport_retried && channel_reset
}

pub(super) async fn bounded_ecw_route_call_with_timeout_observer<T, Call, Fut, E, AttemptError>(
    state: &DataPlaneState,
    route: EcwResolvedRoute,
    operation: &'static str,
    timeout: StdDuration,
    timeout_observer: Option<EcwTimeoutObserver>,
    call: Call,
    map_status: E,
) -> Result<(T, EcwResolvedRoute), EcwProxyError>
where
    Call: Fn(EcwResolvedRoute, StdDuration) -> Fut,
    Fut: Future<Output = Result<T, AttemptError>>,
    AttemptError: Into<EcwRouteAttemptError>,
    E: Fn(tonic::Status) -> EcwProxyError,
{
    bounded_ecw_route_call_inner(
        state,
        route,
        EcwRouteCallPolicy {
            operation,
            timeout,
            transport_retry: EcwTransportRetry::Forbidden,
            timeout_observer,
            timeout_scope: EcwCallTimeoutScope::EndToEnd,
        },
        call,
        map_status,
    )
    .await
}

/// Run a replayable transfer whose established data movement may outlive the
/// route-recovery budget. Structured identity rejection still receives one
/// bounded refresh and one rebuilt attempt.
pub(super) async fn bounded_ecw_route_transfer<T, Call, Fut, E, AttemptError>(
    state: &DataPlaneState,
    route: EcwResolvedRoute,
    operation: &'static str,
    route_recovery_timeout: StdDuration,
    call: Call,
    map_status: E,
) -> Result<(T, EcwResolvedRoute), EcwProxyError>
where
    Call: Fn(EcwResolvedRoute, StdDuration) -> Fut,
    Fut: Future<Output = Result<T, AttemptError>>,
    AttemptError: Into<EcwRouteAttemptError>,
    E: Fn(tonic::Status) -> EcwProxyError,
{
    bounded_ecw_route_call_inner(
        state,
        route,
        EcwRouteCallPolicy {
            operation,
            timeout: route_recovery_timeout,
            transport_retry: EcwTransportRetry::Forbidden,
            timeout_observer: None,
            timeout_scope: EcwCallTimeoutScope::RouteRecovery,
        },
        call,
        map_status,
    )
    .await
}

pub(super) fn sweep_expired_ecw_routes(state: &DataPlaneState) {
    if state
        .route_cache
        .sweep_expired_ecw_throttled(
            ECW_ROUTE_CACHE_SWEEP_INTERVAL,
            ECW_ROUTE_CACHE_SWEEP_MAX_ENTRIES,
        )
        .is_some()
    {
        pplx_space_metrics::data_plane::route_cache_entries(
            pplx_space_metrics::data_plane::RouteCacheKind::Ecw,
            state.route_cache.ecw_len(),
        );
    }
}

#[derive(Debug)]
pub(super) enum EcwRouteResolveError {
    Status(tonic::Status),
    Timeout,
}

pub(super) async fn bounded_ecw_route_resolve<T, F>(
    target: &EcwSandboxHost,
    timeout: Duration,
    future: F,
) -> Result<T, EcwRouteResolveError>
where
    F: std::future::Future<Output = Result<T, tonic::Status>>,
{
    let started_at = std::time::Instant::now();
    match tokio::time::timeout(timeout, future).await {
        Ok(Ok(response)) => {
            record_ecw_backend_rpc_latency(
                ECW_ROUTE_RESOLVE_OPERATION,
                BackendRpcResult::Ok,
                started_at,
            );
            Ok(response)
        }
        Ok(Err(status)) => {
            record_ecw_backend_rpc_latency(
                ECW_ROUTE_RESOLVE_OPERATION,
                BackendRpcResult::Status,
                started_at,
            );
            Err(EcwRouteResolveError::Status(status))
        }
        Err(_) => {
            record_ecw_backend_rpc_latency(
                ECW_ROUTE_RESOLVE_OPERATION,
                BackendRpcResult::Timeout,
                started_at,
            );
            tracing::warn!(
                sandbox_id = %target.sandbox_id,
                port = target.port,
                timeout_ms = timeout.as_millis(),
                "ECW sandbox service route resolution timed out",
            );
            Err(EcwRouteResolveError::Timeout)
        }
    }
}

pub(super) fn route_resolve_timeout_error() -> EcwProxyError {
    EcwProxyError::sandbox_service_route_not_ready().without_runtime_access_classification()
}

fn ecw_sandbox_service_route_kind(target: &EcwSandboxHost) -> SandboxServiceRouteKind {
    if target.port == ECW_ENVD_PORT {
        SandboxServiceRouteKind::Agent
    } else {
        SandboxServiceRouteKind::ServicePort
    }
}

fn ecw_route_credential_proofs(
    envd_access_token_sha256: Option<String>,
    traffic_access_token_sha256: Option<String>,
    api_key_sha256: Option<String>,
) -> Vec<SandboxServiceRouteCredentialProof> {
    let mut proofs = Vec::new();
    push_ecw_route_credential_proof(
        &mut proofs,
        SandboxServiceRouteCredentialKind::SandboxAgentToken,
        envd_access_token_sha256,
    );
    push_ecw_route_credential_proof(
        &mut proofs,
        SandboxServiceRouteCredentialKind::SandboxTrafficToken,
        traffic_access_token_sha256,
    );
    push_ecw_route_credential_proof(
        &mut proofs,
        SandboxServiceRouteCredentialKind::OrgApiKey,
        api_key_sha256,
    );
    proofs
}

fn push_ecw_route_credential_proof(
    proofs: &mut Vec<SandboxServiceRouteCredentialProof>,
    kind: SandboxServiceRouteCredentialKind,
    sha256: Option<String>,
) {
    let Some(sha256) = sha256 else {
        return;
    };
    proofs.push(SandboxServiceRouteCredentialProof {
        kind: kind as i32,
        sha256,
    });
}

pub(super) async fn ecw_backend_channel(
    state: &DataPlaneState,
    route: &EcwResolvedRoute,
) -> Result<Channel, EcwProxyError> {
    state
        .backend_channels
        .channel_for_key(&route.backend_endpoint, route.sandbox_id.as_uuid())
        .await
        .map_err(|error| {
            tracing::warn!(
                sandbox_id = %route.sandbox_id,
                current_node_id = %route.current_node_id,
                %error,
                "failed to build ECW backend channel",
            );
            EcwProxyError::upstream("Sandbox runtime is not reachable")
                .with_classification(ResponseOrigin::Node, ResponseKind::BackendTransport)
        })
}

/// Refuse a new ECW stream while this replica drains. Handlers call it on
/// entry so a refused request does no backend work; the permit path calls it
/// again so no stream can attach without passing it.
pub(super) fn refuse_new_ecw_stream_while_draining(
    state: &DataPlaneState,
    kind: &'static str,
) -> Result<(), EcwProxyError> {
    if state.drain_started.is_cancelled() {
        pplx_space_metrics::data_plane::attach(kind, "draining");
        return Err(EcwProxyError::draining());
    }
    Ok(())
}

pub(super) fn acquire_ecw_stream_permit(
    state: &DataPlaneState,
    kind: &'static str,
) -> Result<AttachedStreamPermit, EcwProxyError> {
    refuse_new_ecw_stream_while_draining(state, kind)?;
    match state.attached_streams.try_acquire_label(kind) {
        Ok(permit) => {
            pplx_space_metrics::data_plane::attach(kind, "ok");
            Ok(permit)
        }
        Err(()) => {
            pplx_space_metrics::data_plane::attach(kind, "too_many_attached_streams");
            Err(EcwProxyError::too_many_attached_streams(
                TooManyRequestsReason::AttachedStreams,
            ))
        }
    }
}

pub(super) fn validate_ecw_sandbox_headers(
    headers: &HeaderMap,
    target: &EcwSandboxHost,
) -> Result<(), EcwProxyError> {
    if let Some(header_sandbox_id) = optional_header(headers, ECW_SANDBOX_ID_HEADER)
        && header_sandbox_id != target.sandbox_id.to_string()
    {
        return Err(EcwProxyError::bad_request(
            "E2b-Sandbox-Id does not match sandbox host",
        ));
    }

    if let Some(header_port) = optional_header(headers, ECW_SANDBOX_PORT_HEADER) {
        let header_port = header_port
            .parse::<u32>()
            .map_err(|_| EcwProxyError::bad_request("Invalid E2b-Sandbox-Port"))?;
        if header_port != target.port {
            return Err(EcwProxyError::bad_request(
                "E2b-Sandbox-Port does not match sandbox host",
            ));
        }
    }

    Ok(())
}

pub(super) fn ecw_route_error(status: tonic::Status, origin: &EcwRouteOrigin) -> EcwProxyError {
    let target = &origin.target;
    match status.code() {
        tonic::Code::Unauthenticated => EcwProxyError::unauthorized(),
        tonic::Code::PermissionDenied if target.port != ECW_ENVD_PORT => {
            // Presence of the traffic-token hash records whether the caller
            // supplied the header, so this classification survives a refresh
            // that no longer has the original request in hand.
            if origin.traffic_access_token_sha256.is_some() {
                EcwProxyError::traffic_token_invalid(target.sandbox_id)
            } else {
                EcwProxyError::traffic_token_missing(target.sandbox_id)
            }
        }
        tonic::Code::PermissionDenied => EcwProxyError::unauthorized(),
        tonic::Code::NotFound => EcwProxyError::not_found("Sandbox"),
        tonic::Code::InvalidArgument => EcwProxyError::bad_request(status.message().to_string()),
        tonic::Code::FailedPrecondition
        | tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
            if SandboxServiceRouteNotReady::from_status(&status).is_some() =>
        {
            EcwProxyError::sandbox_service_route_not_ready()
                .with_classification(ResponseOrigin::Cluster, ResponseKind::RouteNotReady)
        }
        tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => EcwProxyError::sandbox_timeout()
            .with_classification(ResponseOrigin::Cluster, ResponseKind::Timeout),
        tonic::Code::FailedPrecondition | tonic::Code::Unavailable => {
            EcwProxyError::sandbox_data_plane_unavailable()
                .with_classification(ResponseOrigin::Cluster, ResponseKind::RouteUnavailable)
        }
        _ => {
            tracing::warn!(
                sandbox_id = %target.sandbox_id,
                port = target.port,
                code = ?status.code(),
                message = status.message(),
                "failed to resolve ECW data-plane route",
            );
            EcwProxyError::internal("ECW route resolution failed")
                .with_classification(ResponseOrigin::Cluster, ResponseKind::RouteResolution)
        }
    }
}

fn header_sha256(headers: &HeaderMap, name: &str) -> Option<String> {
    optional_header(headers, name).map(token_sha256)
}

// ---------------------------------------------------------------------------
// Route execution
//
// Running an operation against a sandbox route belongs with resolving and
// refreshing that route: recovering from a superseded route identity requires
// all three, and splitting them is what leaves each caller reimplementing the
// recovery policy.
// ---------------------------------------------------------------------------

pub(super) fn ecw_node_api_request_with_timeout<T>(
    route: &EcwResolvedRoute,
    message: T,
    timeout: StdDuration,
) -> Result<tonic::Request<T>, EcwProxyError> {
    node_api_request_with_timeout_and_route_token_state(
        message,
        timeout,
        route.space_api_route_token.as_ref(),
    )
    .map_err(ecw_connect::tonic_status_to_ecw_error)
}

/// Whether an operation may be re-sent after an ambiguous transport failure.
///
/// A transport reset can drop the response to a request the endpoint already
/// executed, so only operations whose replay is harmless opt in. Route-identity
/// refresh is deliberately *not* gated on this: the endpoint rejected the token
/// during authorization, before running anything, so re-sending after a refresh
/// cannot duplicate work even for a mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EcwTransportRetry {
    Forbidden,
    Allowed,
}

pub(super) enum EcwRouteAttemptError {
    Status(tonic::Status),
    Proxy(EcwProxyError),
}

impl From<tonic::Status> for EcwRouteAttemptError {
    fn from(status: tonic::Status) -> Self {
        Self::Status(status)
    }
}

impl From<EcwProxyError> for EcwRouteAttemptError {
    fn from(error: EcwProxyError) -> Self {
        Self::Proxy(error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EcwCallTimeoutScope {
    EndToEnd,
    RouteRecovery,
}

/// What one node-api unary call is and how it is allowed to recover.
#[derive(Clone, Copy, Debug)]
struct EcwRouteCallPolicy {
    operation: &'static str,
    /// Bounds either the whole call or only route recovery, according to
    /// `timeout_scope`.
    timeout: StdDuration,
    transport_retry: EcwTransportRetry,
    timeout_observer: Option<EcwTimeoutObserver>,
    timeout_scope: EcwCallTimeoutScope,
}

pub(super) type EcwTimeoutObserver = fn(&EcwResolvedRoute, StdDuration);

pub(super) fn refresh_dimension_label(status: &tonic::Status) -> &'static str {
    route_identity_dimension_from_status(status)
        .map_or("unspecified", |dimension| dimension.as_str())
}

/// Run one node-api unary call against a sandbox's route, recovering once from a
/// superseded route identity.
///
/// This owns token binding rather than accepting a pre-built request, which is
/// what lets it re-mint the request against a refreshed route. Callers supply
/// the message and the RPC to invoke; they never see tokens, cache entries, or
/// authorization failures. The returned route is the one that served the call,
/// so a caller chaining a follow-up operation uses the refreshed route rather
/// than the stale one it started with.
///
/// `timeout` bounds the whole sequence — first attempt, re-resolution, and
/// retry — so recovery cannot extend a request past its budget.
async fn ecw_node_api_unary_inner<Msg, Resp, Call, Fut, E>(
    state: &DataPlaneState,
    route: EcwResolvedRoute,
    policy: EcwRouteCallPolicy,
    message: Msg,
    call: Call,
    map_status: E,
) -> Result<(Resp, EcwResolvedRoute), EcwProxyError>
where
    Msg: Clone,
    Call: Fn(Channel, tonic::Request<Msg>) -> Fut + Clone,
    Fut: Future<Output = Result<tonic::Response<Resp>, tonic::Status>>,
    E: Fn(tonic::Status) -> EcwProxyError,
{
    bounded_ecw_route_call_inner(
        state,
        route,
        policy,
        move |route, remaining| {
            let message = message.clone();
            let call = call.clone();
            async move {
                let channel = ecw_backend_channel(state, &route)
                    .await
                    .map_err(EcwRouteAttemptError::Proxy)?;
                let request = ecw_node_api_request_with_timeout(&route, message, remaining)
                    .map_err(EcwRouteAttemptError::Proxy)?;
                call(channel, request)
                    .await
                    .map(|response| response.into_inner())
                    .map_err(EcwRouteAttemptError::Status)
            }
        },
        map_status,
    )
    .await
}
async fn ecw_backend_timeout(
    state: &DataPlaneState,
    route: &EcwResolvedRoute,
    operation: &'static str,
    timeout: StdDuration,
    timeout_observer: Option<EcwTimeoutObserver>,
    started_at: Instant,
) -> EcwProxyError {
    record_ecw_backend_rpc_latency(operation, BackendRpcResult::Timeout, started_at);
    tracing::warn!(
        operation,
        sandbox_id = %route.sandbox_id,
        route_generation = route.route_generation,
        current_node_id = %route.current_node_id,
        backend_endpoint = %route.backend_endpoint,
        timeout_ms = timeout.as_millis(),
        "sandbox runtime backend RPC timed out",
    );
    if let Some(timeout_observer) = timeout_observer {
        timeout_observer(route, timeout);
    }
    let error = EcwProxyError::sandbox_timeout()
        .with_classification(ResponseOrigin::Node, ResponseKind::Timeout);
    crate::ecw_runtime_access_error(state, route.sandbox_id, error).await
}

/// Call a node-api unary that must not be replayed after a transport failure.
pub(super) async fn bounded_node_api_unary<Msg, Resp, Call, Fut, E>(
    state: &DataPlaneState,
    route: EcwResolvedRoute,
    operation: &'static str,
    timeout: StdDuration,
    message: Msg,
    call: Call,
    map_status: E,
) -> Result<(Resp, EcwResolvedRoute), EcwProxyError>
where
    Msg: Clone,
    Call: Fn(Channel, tonic::Request<Msg>) -> Fut + Clone,
    Fut: Future<Output = Result<tonic::Response<Resp>, tonic::Status>>,
    E: Fn(tonic::Status) -> EcwProxyError,
{
    ecw_node_api_unary_inner(
        state,
        route,
        EcwRouteCallPolicy {
            operation,
            timeout,
            transport_retry: EcwTransportRetry::Forbidden,
            timeout_observer: None,
            timeout_scope: EcwCallTimeoutScope::EndToEnd,
        },
        message,
        call,
        map_status,
    )
    .await
}

/// Call a node-api unary that is safe to replay after a transport failure.
pub(super) async fn retryable_bounded_node_api_unary<Msg, Resp, Call, Fut, E>(
    state: &DataPlaneState,
    route: EcwResolvedRoute,
    operation: &'static str,
    timeout: StdDuration,
    message: Msg,
    call: Call,
    map_status: E,
) -> Result<(Resp, EcwResolvedRoute), EcwProxyError>
where
    Msg: Clone,
    Call: Fn(Channel, tonic::Request<Msg>) -> Fut + Clone,
    Fut: Future<Output = Result<tonic::Response<Resp>, tonic::Status>>,
    E: Fn(tonic::Status) -> EcwProxyError,
{
    ecw_node_api_unary_inner(
        state,
        route,
        EcwRouteCallPolicy {
            operation,
            timeout,
            transport_retry: EcwTransportRetry::Allowed,
            timeout_observer: None,
            timeout_scope: EcwCallTimeoutScope::EndToEnd,
        },
        message,
        call,
        map_status,
    )
    .await
}

/// Only pre-forwarding attachment failures may consume the ingress recovery attempt.
pub(super) fn retryable_ingress_attachment(status: &tonic::Status) -> bool {
    is_refreshable_route_identity_rejection(status)
        || matches!(
            status.code(),
            tonic::Code::Aborted
                | tonic::Code::FailedPrecondition
                | tonic::Code::Unavailable
                | tonic::Code::NotFound
        )
}
