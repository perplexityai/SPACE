use super::*;
use crate::runtime_route::{is_refreshable_route_identity_rejection, refresh_dimension_label};
use pplx_space_metrics::data_plane::{
    RouteIdentityRefreshOutcome, RouteIdentityRefreshProtocol, ServiceRouteSurface,
    ServiceRouteTransport,
};

mod auth;
mod protocol;
mod proxy;
mod sse;
mod streams;
mod websocket;

const NATIVE_ROUTE_CACHE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const NATIVE_ROUTE_CACHE_SWEEP_MAX_ENTRIES: usize = 16 * 1024;

pub(super) use auth::*;
pub(super) use protocol::*;
pub(super) use proxy::*;
pub(super) use sse::*;
pub(super) use streams::*;
pub(super) use websocket::*;

pub(super) async fn resolve_session_route(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
    token: &str,
) -> Result<AttachedSession, RouteResolutionError> {
    resolve_session_route_with_cache(state, session_id, token, true).await
}

async fn resolve_session_route_with_cache(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
    token: &str,
    use_cache: bool,
) -> Result<AttachedSession, RouteResolutionError> {
    sweep_expired_native_routes(state);
    let token_sha256 = token_sha256(token);
    let required_scope = session_id.kind().connect_scope().as_str().to_string();
    let cache_key = native_route_cache_key(session_id, &token_sha256, &required_scope);
    let kind = session_id.kind().as_str();
    let cache_lookup = state.route_cache.lookup_native(&cache_key);
    pplx_space_metrics::data_plane::route_cache_lookup("native", kind, cache_lookup.result_label());
    if let RouteCacheLookup::Hit(attached) = cache_lookup
        && use_cache
        && !space_api_route_token_needs_refresh(&attached.space_api_route_token)
    {
        state.revocation_cache.remember_active(
            session_id,
            attached.revocation_epoch,
            attached.org_id,
            attached.org_revocation_epoch,
            state.revocation_check_interval,
        );
        return Ok(attached);
    }

    let mut client = state.cluster_api.for_session(&session_id);
    let response = client
        .resolve_data_plane_session_route(ResolveDataPlaneSessionRouteRequest {
            session_id: session_id.to_string(),
            protocol_session_id: protocol_session_id(session_id),
            token_sha256: Some(token_sha256),
            required_scope: Some(required_scope),
            requested_transport: Some(native_requested_transport(session_id.kind()) as i32),
        })
        .await
        .map_err(|status| match status.code() {
            tonic::Code::Unauthenticated => RouteResolutionError::Unauthenticated,
            tonic::Code::PermissionDenied => RouteResolutionError::Forbidden,
            tonic::Code::NotFound => RouteResolutionError::SessionNotFound,
            tonic::Code::FailedPrecondition => RouteResolutionError::SessionNotReady,
            _ => RouteResolutionError::ResolutionFailed,
        })?
        .into_inner();

    let mut attached = attached_session_from_route_response(
        response,
        session_id,
        state.route_cache.next_cache_epoch(),
    )
    .map_err(|_| RouteResolutionError::RouteStateInvalid)?;
    state.revocation_cache.remember_active(
        session_id,
        attached.revocation_epoch,
        attached.org_id,
        attached.org_revocation_epoch,
        state.revocation_check_interval,
    );
    attached.cache_key = Some(cache_key.clone());
    state
        .route_cache
        .remember_native(cache_key, attached.clone(), state.route_cache_ttl);
    Ok(attached)
}

/// Resolve the authoritative session route after a rejection or a worker going
/// away, replacing a still-live stale cache entry without renewing its auth.
///
/// Without the conditional replacement, the cache keeps handing later attaches
/// the same route token the endpoint just refused until the authenticated slot
/// expires.
///
/// Concurrent callers serialize on one refresh slot and reuse whatever
/// different identity the winner installed. A refreshed route replaces only a
/// still-live authenticated cache slot and preserves that slot's original
/// expiry, because the internal resolve does not revalidate the client token.
pub(super) async fn refresh_session_route(
    state: &DataPlaneState,
    stale: &AttachedSession,
) -> Result<AttachedSession, StreamBridgeError> {
    refresh_session_route_until(
        state,
        stale,
        tokio::time::Instant::now() + state.attach_deadline,
    )
    .await
}

async fn refresh_session_route_until(
    state: &DataPlaneState,
    stale: &AttachedSession,
    deadline: tokio::time::Instant,
) -> Result<AttachedSession, StreamBridgeError> {
    let session_id = stale.session_id;
    let cache_key = stale.cache_key.clone();
    let slot_key = native_route_cache_prefix(session_id);
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    let _slot = tokio::time::timeout(remaining, state.route_cache.acquire_refresh_slot(&slot_key))
        .await
        .map_err(|_| native_route_refresh_timeout())?;
    {
        // Someone may have refreshed this session while we waited for the slot.
        if let Some(current) = state
            .route_cache
            .newer_native_session(session_id, stale.route_identity())
        {
            let mut current = current;
            state
                .route_cache
                .remember_native_latest_route(current.clone(), state.route_cache_ttl);
            if let Some(cache_key) = cache_key.clone() {
                current.cache_key = Some(cache_key.clone());
                state.route_cache.replace_native_route_if_live(
                    &cache_key,
                    stale.route_identity(),
                    current.clone(),
                );
            }
            Ok(current)
        } else {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, resolve_session_route_internal(state, session_id))
                .await
                .map_err(|_| native_route_refresh_timeout())?
            {
                Ok(mut refreshed) => {
                    state
                        .route_cache
                        .remember_native_latest_route(refreshed.clone(), state.route_cache_ttl);
                    if let Some(cache_key) = cache_key {
                        refreshed.cache_key = Some(cache_key.clone());
                        state.route_cache.replace_native_route_if_live(
                            &cache_key,
                            stale.route_identity(),
                            refreshed.clone(),
                        );
                    }
                    Ok(refreshed)
                }
                Err(error) => Err(error),
            }
        }
    }
}

fn sweep_expired_native_routes(state: &DataPlaneState) {
    if state
        .route_cache
        .sweep_expired_native_throttled(
            NATIVE_ROUTE_CACHE_SWEEP_INTERVAL,
            NATIVE_ROUTE_CACHE_SWEEP_MAX_ENTRIES,
        )
        .is_some()
    {
        pplx_space_metrics::data_plane::route_cache_entries(
            pplx_space_metrics::data_plane::RouteCacheKind::Native,
            state.route_cache.native_len(),
        );
        pplx_space_metrics::data_plane::route_cache_entries(
            pplx_space_metrics::data_plane::RouteCacheKind::NativeLatest,
            state.route_cache.native_latest_len(),
        );
    }
}

pub(super) fn native_service_port_ingress_open_error(
    error: ServicePortIngressOpenError,
) -> StreamBridgeError {
    record_service_port_ingress_open(ServicePortIngressOpenResult::Error, error.reason);
    if let Some(status) = error.transport_status {
        StreamBridgeError::BackendStatus(status)
    } else {
        tracing::debug!(error = ?error.error, "failed to open native service ingress");
        StreamBridgeError::RouteState("service_ingress_unavailable")
    }
}

fn native_route_refresh_timeout() -> StreamBridgeError {
    StreamBridgeError::ClusterStatus(tonic::Status::deadline_exceeded(
        "native route refresh deadline exceeded",
    ))
}

pub(super) async fn native_route_call<T, Call, Fut>(
    state: &DataPlaneState,
    attached: &mut AttachedSession,
    call: Call,
) -> Result<T, StreamBridgeError>
where
    Call: Fn(AttachedSession) -> Fut,
    Fut: Future<Output = Result<T, StreamBridgeError>>,
{
    let mut refresh_dimension = None;
    let deadline = tokio::time::Instant::now() + state.attach_deadline;
    loop {
        match call(attached.clone()).await {
            Ok(value) => {
                if let Some(dimension) = refresh_dimension {
                    pplx_space_metrics::data_plane::route_identity_refresh(
                        RouteIdentityRefreshProtocol::Native,
                        dimension,
                        RouteIdentityRefreshOutcome::Succeeded,
                    );
                }
                return Ok(value);
            }
            Err(StreamBridgeError::BackendStatus(status))
                if refresh_dimension.is_none()
                    && is_refreshable_route_identity_rejection(&status) =>
            {
                let dimension = refresh_dimension_label(&status);
                match refresh_session_route_until(state, attached, deadline).await {
                    Ok(refreshed) => {
                        *attached = refreshed;
                        refresh_dimension = Some(dimension);
                    }
                    Err(error) => {
                        pplx_space_metrics::data_plane::route_identity_refresh(
                            RouteIdentityRefreshProtocol::Native,
                            dimension,
                            RouteIdentityRefreshOutcome::ResolveFailed,
                        );
                        return Err(error);
                    }
                }
            }
            Err(error) => {
                if let Some(dimension) = refresh_dimension {
                    pplx_space_metrics::data_plane::route_identity_refresh(
                        RouteIdentityRefreshProtocol::Native,
                        dimension,
                        RouteIdentityRefreshOutcome::RetryFailed,
                    );
                }
                return Err(error);
            }
        }
    }
}

async fn refresh_native_stream_route(
    state: &DataPlaneState,
    attached: &AttachedSession,
    status: &tonic::Status,
) -> Result<(AttachedSession, Option<&'static str>), StreamBridgeError> {
    let dimension =
        is_refreshable_route_identity_rejection(status).then(|| refresh_dimension_label(status));
    match refresh_session_route(state, attached).await {
        Ok(refreshed) => Ok((refreshed, dimension)),
        Err(error) => {
            if let Some(dimension) = dimension {
                pplx_space_metrics::data_plane::route_identity_refresh(
                    RouteIdentityRefreshProtocol::Native,
                    dimension,
                    RouteIdentityRefreshOutcome::ResolveFailed,
                );
            }
            Err(error)
        }
    }
}

fn finish_native_stream_refresh<T>(
    dimension: Option<&'static str>,
    result: Result<T, StreamBridgeError>,
) -> Result<T, StreamBridgeError> {
    match result {
        Ok(value) => {
            if let Some(dimension) = dimension {
                pplx_space_metrics::data_plane::route_identity_refresh(
                    RouteIdentityRefreshProtocol::Native,
                    dimension,
                    RouteIdentityRefreshOutcome::Succeeded,
                );
            }
            Ok(value)
        }
        Err(error) => Err(native_stream_refresh_failed(dimension, error)),
    }
}

fn native_stream_refresh_failed(
    dimension: Option<&'static str>,
    error: StreamBridgeError,
) -> StreamBridgeError {
    if let Some(dimension) = dimension {
        pplx_space_metrics::data_plane::route_identity_refresh(
            RouteIdentityRefreshProtocol::Native,
            dimension,
            RouteIdentityRefreshOutcome::RetryFailed,
        );
    }
    error
}

async fn native_node_api_unary<Msg, Resp, Call, Fut>(
    state: &DataPlaneState,
    attached: &mut AttachedSession,
    message: Msg,
    call: Call,
) -> Result<Resp, StreamBridgeError>
where
    Msg: Clone,
    Call: Fn(WorkerApiClient<GrpcClientTrace<Channel>>, tonic::Request<Msg>) -> Fut + Clone,
    Fut: Future<Output = Result<tonic::Response<Resp>, tonic::Status>>,
{
    let call_state = state.clone();
    let refresh_state = state.clone();
    native_route_call(&refresh_state, attached, move |route| {
        let message = message.clone();
        let state = call_state.clone();
        let call = call.clone();
        async move {
            let channel = state
                .backend_channels
                .channel(&route.backend_endpoint, route.session_id)
                .await?;
            let client = WorkerApiClient::new(wrap_grpc_client_channel(channel));
            let request = node_api_request_with_route_token_state(
                message,
                route.space_api_route_token.as_ref(),
            )?;
            call(client, request)
                .await
                .map(|response| response.into_inner())
                .map_err(StreamBridgeError::BackendStatus)
        }
    })
    .await
}

pub(super) async fn resolve_session_route_internal(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
) -> Result<AttachedSession, StreamBridgeError> {
    let mut client = state.cluster_api.for_session(&session_id);
    let response = client
        .resolve_data_plane_session_route(ResolveDataPlaneSessionRouteRequest {
            session_id: session_id.to_string(),
            protocol_session_id: protocol_session_id(session_id),
            token_sha256: None,
            required_scope: None,
            requested_transport: Some(native_requested_transport(session_id.kind()) as i32),
        })
        .await
        .map_err(StreamBridgeError::ClusterStatus)?
        .into_inner();

    let attached = attached_session_from_route_response(
        response,
        session_id,
        state.route_cache.next_cache_epoch(),
    )?;
    state.revocation_cache.remember_active(
        session_id,
        attached.revocation_epoch,
        attached.org_id,
        attached.org_revocation_epoch,
        state.revocation_check_interval,
    );
    Ok(attached)
}

pub(super) fn attached_session_from_route_response(
    response: ResolveDataPlaneSessionRouteResponse,
    session_id: DataPlaneSessionId,
    cache_epoch: u64,
) -> Result<AttachedSession, StreamBridgeError> {
    let kind = response
        .kind
        .parse::<SessionKind>()
        .map_err(|_| StreamBridgeError::RouteState("invalid_session_kind"))?;
    if kind != session_id.kind() {
        return Err(StreamBridgeError::RouteState("session_kind_mismatch"));
    }
    let backend = response
        .backend
        .ok_or(StreamBridgeError::RouteState("route_backend_missing"))?;
    let revocation = response
        .revocation
        .as_ref()
        .ok_or(StreamBridgeError::RouteState("revocation_missing"))?;
    if !revocation_state_is_active(revocation) {
        return Err(StreamBridgeError::RouteState("session_revoked"));
    }
    Ok(AttachedSession {
        cache_epoch,
        cache_key: None,
        session_id,
        sandbox_id: response
            .sandbox_id
            .parse()
            .map_err(|_| StreamBridgeError::RouteState("invalid_sandbox_id"))?,
        org_id: response
            .org_id
            .parse()
            .map_err(|_| StreamBridgeError::RouteState("invalid_org_id"))?,
        kind,
        route_generation: response.route_generation,
        current_node_id: response
            .current_node_id
            .parse()
            .map_err(|_| StreamBridgeError::RouteState("invalid_current_node_id"))?,
        backend_endpoint: backend
            .endpoint
            .parse()
            .map_err(|_| StreamBridgeError::RouteState("invalid_backend_endpoint"))?,
        target_port: response.target_port,
        target_protocol: response
            .target_protocol
            .map(|protocol| protocol.parse())
            .transpose()
            .map_err(|_| StreamBridgeError::RouteState("invalid_target_protocol"))?,
        adapter_state_json: response.adapter_state_json,
        backend_process_id: response
            .backend_process_id
            .map(|process_id| process_id.parse())
            .transpose()
            .map_err(|_| StreamBridgeError::RouteState("invalid_backend_process_id"))?,
        space_api_route_token: response.space_api_route_token.and_then(|token| {
            parse_proto_space_api_route_token(
                SpaceApiRouteTokenRouteSource::DataPlaneSession,
                token,
            )
        }),
        revocation_epoch: revocation.epoch,
        org_revocation_epoch: revocation.org_epoch,
    })
}

pub(super) fn native_requested_transport(kind: SessionKind) -> SandboxServiceRouteTransport {
    if matches!(kind, SessionKind::Port | SessionKind::Protocol) {
        SandboxServiceRouteTransport::ServicePortIngress
    } else {
        SandboxServiceRouteTransport::Direct
    }
}

pub(super) fn protocol_session_id(session_id: DataPlaneSessionId) -> Option<String> {
    match session_id {
        DataPlaneSessionId::Protocol(id) => Some(id.to_string()),
        _ => None,
    }
}

pub(super) fn revocation_state_is_active(revocation: &DataPlaneRevocationState) -> bool {
    revocation.status == DataPlaneRevocationStatus::Active as i32
}

/// Path parameters shared by the bare proxy route and its `{*path}` subpath
/// mount.
///
/// The subpath mount captures two parameters, so a scalar `Path<String>`
/// extractor rejects every subpath request with axum's 500
/// `WrongNumberOfParameters` before the handler runs. Naming the field lets
/// both routes deserialize; the proxied tail is read from the request URI.
#[derive(Debug, Deserialize)]
pub(super) struct ProxySessionPath {
    pub(super) session_id: String,
}

pub(super) async fn proxy_session(
    State(state): State<DataPlaneState>,
    Path(ProxySessionPath { session_id }): Path<ProxySessionPath>,
    mut request: Request<Body>,
) -> Result<Response, ProxyError> {
    reject_query_tokens(request.uri().query())?;
    let session_id = session_id
        .parse::<DataPlaneSessionId>()
        .map_err(|_| ProxyError::BadRequest("invalid session id"))?;
    let (token, token_source) = token_from_headers_with_source(request.headers())
        .map_err(ProxyError::BadRequest)?
        .ok_or(ProxyError::Unauthorized("data-plane token is required"))?;
    if is_upgrade_request(request.headers()) && state.drain_started.is_cancelled() {
        return Ok(native_draining_response(session_id.kind().as_str()));
    }
    let mut preparation = pplx_space_metrics::data_plane::IngressPreparationObservation::default();
    let mut waiter =
        pplx_space_common_svc::ingress_admission::IngressPreparationPermit::acquire(None)
            .map_err(|_| ProxyError::Upstream("ingress preparation capacity exhausted"))?;
    let deadline = tokio::time::Instant::now()
        + pplx_space_common_svc::ingress_admission::preparation_timeout();
    let mut attached =
        tokio::time::timeout_at(deadline, resolve_session_route(&state, session_id, &token))
            .await
            .map_err(|_| ProxyError::Upstream("ingress preparation timed out"))?
            .map_err(ProxyError::Route)?;
    waiter
        .bind_sandbox(attached.sandbox_id)
        .map_err(|_| ProxyError::Upstream("sandbox ingress capacity exhausted"))?;
    match attached.kind {
        SessionKind::Port | SessionKind::Protocol => {}
        SessionKind::Terminal | SessionKind::Process | SessionKind::FileWatch => {
            return Err(ProxyError::BadRequest(
                "session kind does not support proxy",
            ));
        }
    }

    let cdp_proxy_base = CdpProxyBase::from_headers(request.headers(), session_id);
    rewrite_proxy_request_for_ingress(&mut request, session_id, token_source)?;
    let kind = attached.kind.as_str();
    let upgrade_stream_permit = if is_upgrade_request(request.headers()) {
        // Again after the route lookup: SIGTERM may have landed while it ran.
        if state.drain_started.is_cancelled() {
            return Ok(native_draining_response(kind));
        }
        match state.attached_streams.try_acquire_attached(&attached) {
            Ok(permit) => Some(permit),
            Err(limit) => {
                pplx_space_metrics::data_plane::attach(kind, limit.metric_label());
                return Ok(data_plane_http_error(
                    StatusCode::TOO_MANY_REQUESTS,
                    DataPlaneHttpErrorCode::RateLimited,
                    "too many attached streams",
                    true,
                    ResponseOrigin::DataPlane,
                    ResponseKind::Admission,
                ));
            }
        }
    } else {
        None
    };
    let proxied_path = request.uri().path().to_string();
    let downstream_upgrade = if upgrade_stream_permit.is_some() {
        Some(hyper::upgrade::on(&mut request))
    } else {
        None
    };

    pplx_space_metrics::data_plane::service_route_transport(
        ServiceRouteSurface::Native,
        ServiceRouteTransport::ServicePortIngress,
    );
    let prepared = before_stream_deadline(
        &state.stream_shutdown_token,
        tokio::time::timeout_at(deadline, async {
            for attempt in 0..2 {
                match native_service_port_ingress_io(&state, &attached).await {
                    Ok(io) => {
                        let fresh =
                            resolve_session_route_with_cache(&state, session_id, &token, false)
                                .await
                                .map_err(ProxyError::Route)?;
                        if fresh.route_identity() == attached.route_identity() {
                            return Ok(io);
                        }
                        attached = fresh;
                    }
                    Err(error) => {
                        let retryable = error
                            .transport_status
                            .as_ref()
                            .is_some_and(crate::runtime_route::retryable_ingress_attachment);
                        if attempt == 1 || !retryable {
                            return Err(ProxyError::Upstream("session service is not reachable"));
                        }
                        attached =
                            resolve_session_route_with_cache(&state, session_id, &token, false)
                                .await
                                .map_err(ProxyError::Route)?;
                    }
                }
            }
            Err(ProxyError::Upstream(
                "session route changed during attachment",
            ))
        }),
    )
    .await;
    let Some(prepared) = prepared else {
        // Only an upgrade is a stream; a plain request is answered as
        // draining without counting as a refused attach.
        return Ok(if upgrade_stream_permit.is_some() {
            native_draining_response(kind)
        } else {
            native_draining_error()
        });
    };
    let mut ingress_io =
        prepared.map_err(|_| ProxyError::Upstream("ingress preparation timed out"))??;
    drop(waiter);
    preparation.ready();
    drop(preparation);
    let completion = ingress_io.take_completion();
    if upgrade_stream_permit.is_some() {
        ingress_io.track_transport_progress();
    }
    let request = request.map(|body| completion.body(body));
    let (mut sender, connection) =
        http1::handshake(TokioIo::new(ingress_io))
            .await
            .map_err(|error| {
                state.route_cache.forget_native_session(session_id);
                tracing::debug!(%error, "failed to open native service ingress HTTP connection");
                ProxyError::Upstream("session service is not reachable")
            })?;
    let connection_tasks = state.upgrade_tasks.clone();
    connection_tasks.spawn(async move {
        if let Err(error) = connection.with_upgrades().await {
            tracing::debug!(%error, "native service ingress HTTP connection ended with error");
        }
    });
    let mut response = sender.send_request(request).await.map_err(|error| {
        state.route_cache.forget_native_session(session_id);
        tracing::debug!(%error, "failed to proxy public port session request");
        ProxyError::Upstream("session service is not reachable")
    })?;
    let upstream_upgrade =
        if downstream_upgrade.is_some() && response.status() == StatusCode::SWITCHING_PROTOCOLS {
            Some(hyper::upgrade::on(&mut response))
        } else {
            None
        };

    if attached.kind == SessionKind::Protocol
        && upstream_upgrade.is_none()
        && is_cdp_discovery_path(&proxied_path)
        && response.status().is_success()
    {
        let response = rewrite_cdp_discovery_response(response, &cdp_proxy_base).await?;
        return Ok(response.map(|body| completion.body(body)));
    }

    record_native_proxied_response(response.status());

    let response = response.map(|body| completion.body(Body::new(body)));
    if let (Some(downstream_upgrade), Some(upstream_upgrade), Some(stream_permit)) =
        (downstream_upgrade, upstream_upgrade, upgrade_stream_permit)
    {
        let shutdown_token = state.stream_shutdown_token.child_token();
        let upgrade_tasks = state.upgrade_tasks.clone();
        upgrade_tasks.spawn(async move {
            let _stream_permit = stream_permit;
            let _completion = completion;
            match proxy_upgraded_session_connection(
                state,
                session_id,
                kind,
                downstream_upgrade,
                upstream_upgrade,
                shutdown_token,
            )
            .await
            {
                Ok(close) => {
                    pplx_space_metrics::data_plane::stream_closed(kind, close.result_label())
                }
                Err(error) => {
                    pplx_space_metrics::data_plane::stream_closed(kind, "proxy_error");
                    tracing::debug!(%error, "public port session upgrade proxy ended with error");
                }
            }
        });
    }

    Ok(response.into_response())
}

pub(super) fn record_native_proxied_response(status: StatusCode) {
    pplx_space_metrics::data_plane::response(
        ResponseSurface::Native,
        ResponseOrigin::SandboxTraffic,
        ResponseKind::ProxiedStatus,
        status.as_u16(),
    );
    tracing::Span::current().record(
        "space.response_origin",
        ResponseOrigin::SandboxTraffic.as_str(),
    );
    tracing::Span::current().record("space.response_kind", ResponseKind::ProxiedStatus.as_str());
}

pub(super) fn attached_json(attached: &AttachedSession) -> String {
    server_frame_json(&attached_frame(attached))
}

pub(super) fn attached_frame(attached: &AttachedSession) -> DataPlaneServerFrame {
    DataPlaneServerFrame::Attached {
        session_id: attached.session_id,
        route_generation: attached.route_generation,
        current_node_id: attached.current_node_id.to_string(),
    }
}

pub(super) fn error_json(code: DataPlaneErrorCode) -> String {
    server_frame_json(&DataPlaneServerFrame::Error {
        message: code.as_str().to_string(),
        code,
        retryable: false,
    })
}

pub(super) fn server_frame_json(frame: &DataPlaneServerFrame) -> String {
    serde_json::to_string(frame)
        .unwrap_or_else(|_| "{\"type\":\"error\",\"code\":\"internal\"}".to_string())
}

pub(super) fn sse_event_from_frame(frame: &DataPlaneServerFrame, id: Option<u64>) -> Event {
    let mut event = Event::default()
        .event(server_frame_event_name(frame))
        .data(server_frame_json(frame));
    if let Some(id) = id {
        event = event.id(id.to_string());
    }
    event
}

pub(super) fn server_frame_event_name(frame: &DataPlaneServerFrame) -> &'static str {
    match frame {
        DataPlaneServerFrame::Attached { .. } => "attached",
        DataPlaneServerFrame::ProcessStarted { .. } => "process_started",
        DataPlaneServerFrame::Stdout { .. } => "stdout",
        DataPlaneServerFrame::Stderr { .. } => "stderr",
        DataPlaneServerFrame::InputAccepted { .. } => "input_accepted",
        DataPlaneServerFrame::ControlAccepted { .. } => "control_accepted",
        DataPlaneServerFrame::ProcessExited { .. } => "process_exited",
        DataPlaneServerFrame::FileEvent { .. } => "file_event",
        DataPlaneServerFrame::BackendReconnecting { .. } => "backend_reconnecting",
        DataPlaneServerFrame::BackendReconnected { .. } => "backend_reconnected",
        DataPlaneServerFrame::ResyncRequired { .. } => "resync_required",
        DataPlaneServerFrame::Error { .. } => "error",
        DataPlaneServerFrame::Closed { .. } => "closed",
        DataPlaneServerFrame::Unknown => "unknown",
    }
}

pub(super) fn resync_required_frame(
    kind: &str,
    reason: DataPlaneResyncReason,
) -> DataPlaneServerFrame {
    pplx_space_metrics::data_plane::replay_miss(kind, reason.as_str());
    DataPlaneServerFrame::ResyncRequired { reason }
}

pub(super) async fn send_server_frame<S>(
    sink: &mut S,
    frame: &DataPlaneServerFrame,
) -> Result<(), ()>
where
    S: SinkExt<Message> + Unpin,
{
    sink.send(Message::Text(server_frame_json(frame).into()))
        .await
        .map_err(|_| ())
}

pub(super) fn revocation_check_interval(state: &DataPlaneState) -> tokio::time::Interval {
    let mut interval = tokio::time::interval_at(
        tokio::time::Instant::now() + state.revocation_check_interval,
        state.revocation_check_interval,
    );
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

pub(super) async fn session_is_revoked(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
) -> Result<bool, StreamBridgeError> {
    let kind = session_id.kind().as_str();
    match state.revocation_cache.probe(session_id) {
        RevocationProbe::Fresh(active) => {
            pplx_space_metrics::data_plane::revocation_cache_probe(kind, "fresh");
            Ok(!active)
        }
        RevocationProbe::RefreshInFlight(active) => {
            pplx_space_metrics::data_plane::revocation_cache_probe(kind, "refresh_in_flight");
            Ok(!active)
        }
        RevocationProbe::RefreshSession => {
            pplx_space_metrics::data_plane::revocation_cache_probe(kind, "refresh");
            let Some((org_id, revocation)) = fetch_session_revocation_state(state, session_id)
                .await
                .inspect_err(|error| {
                    pplx_space_metrics::data_plane::revocation_check(
                        kind,
                        revocation_check_error_label(error),
                    );
                })?
            else {
                pplx_space_metrics::data_plane::revocation_check(kind, "not_found");
                return Ok(true);
            };
            let active = revocation_state_is_active(&revocation);
            let result = if active { "active" } else { "revoked" };
            pplx_space_metrics::data_plane::revocation_check(kind, result);
            state.revocation_cache.remember(
                session_id,
                org_id,
                &revocation,
                state.revocation_check_interval,
            );
            Ok(!active)
        }
        RevocationProbe::RefreshOrg(refresh) => {
            pplx_space_metrics::data_plane::revocation_cache_probe(kind, "refresh");
            let org_epoch = fetch_org_revocation_epoch(state, refresh.org_id)
                .await
                .inspect_err(|error| {
                    pplx_space_metrics::data_plane::revocation_check(
                        kind,
                        revocation_check_error_label(error),
                    );
                })?;
            state.revocation_cache.remember_org_epoch(
                refresh.org_id,
                org_epoch,
                state.revocation_check_interval,
            );
            if org_epoch <= refresh.cached_org_epoch {
                pplx_space_metrics::data_plane::revocation_check(kind, "org_epoch_unchanged");
                drop(refresh.guard);
                return Ok(!refresh.active);
            }
            pplx_space_metrics::data_plane::revocation_check(kind, "org_epoch_changed");
            drop(refresh.guard);
            let Some((org_id, revocation)) = fetch_session_revocation_state(state, session_id)
                .await
                .inspect_err(|error| {
                    pplx_space_metrics::data_plane::revocation_check(
                        kind,
                        revocation_check_error_label(error),
                    );
                })?
            else {
                pplx_space_metrics::data_plane::revocation_check(kind, "not_found");
                return Ok(true);
            };
            let active = revocation_state_is_active(&revocation);
            let result = if active { "active" } else { "revoked" };
            pplx_space_metrics::data_plane::revocation_check(kind, result);
            state.revocation_cache.remember(
                session_id,
                org_id,
                &revocation,
                state.revocation_check_interval,
            );
            Ok(!active)
        }
    }
}

pub(super) async fn fetch_session_revocation_state(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
) -> Result<Option<(OrgId, DataPlaneRevocationState)>, StreamBridgeError> {
    let mut client = state.cluster_api.for_session(&session_id);
    let response = match client
        .get_data_plane_session_revocation_state(GetDataPlaneSessionRevocationStateRequest {
            session_id: session_id.to_string(),
            protocol_session_id: protocol_session_id(session_id),
        })
        .await
    {
        Ok(response) => response.into_inner(),
        Err(status) if status.code() == tonic::Code::NotFound => return Ok(None),
        Err(status) => return Err(StreamBridgeError::RevocationStatus(status)),
    };
    let Some(revocation) = response.revocation else {
        return Err(StreamBridgeError::MissingRevocationState);
    };
    let org_id = response
        .org_id
        .parse()
        .map_err(|_| StreamBridgeError::RouteState("invalid_org_id"))?;
    Ok(Some((org_id, revocation)))
}

pub(super) async fn fetch_org_revocation_epoch(
    state: &DataPlaneState,
    org_id: OrgId,
) -> Result<u64, StreamBridgeError> {
    let mut client = state.cluster_api.for_org(&org_id);
    let response = client
        .get_data_plane_org_revocation_epoch(GetDataPlaneOrgRevocationEpochRequest {
            org_id: org_id.to_string(),
        })
        .await
        .map_err(StreamBridgeError::RevocationStatus)?
        .into_inner();
    Ok(response.epoch)
}

pub(super) fn revocation_check_error_label(error: &StreamBridgeError) -> &'static str {
    match error {
        StreamBridgeError::MissingRevocationState => "missing_state",
        StreamBridgeError::RevocationStatus(_) => "status_error",
        _ => "internal_error",
    }
}

pub(super) async fn bind_session_process(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
    process_id: &ProcessId,
) -> Result<(), StreamBridgeError> {
    let mut client = state.cluster_api.for_session(&session_id);
    client
        .bind_data_plane_session_process(BindDataPlaneSessionProcessRequest {
            session_id: session_id.to_string(),
            protocol_session_id: protocol_session_id(session_id),
            process_id: process_id.to_string(),
        })
        .await
        .map_err(StreamBridgeError::ClusterStatus)?;
    // The route cached at attach time predates the binding and still says
    // `backend_process_id: None`. A client that reconnects inside the cache
    // TTL would otherwise take the start path and run the command a second
    // time before its bind is refused. Drop the entry so the next attach
    // re-resolves the bound route and connects to the existing process.
    state.route_cache.forget_native_session(session_id);
    Ok(())
}

#[cfg(test)]
mod tests;
