use super::*;
use crate::runtime_route::{is_refreshable_route_identity_rejection, refresh_dimension_label};
use pplx_space_metrics::data_plane::{
    RouteIdentityRefreshOutcome, RouteIdentityRefreshProtocol, ServiceRouteSurface,
    ServiceRouteTransport,
};

mod auth;
mod protocol;
mod proxy;

const NATIVE_ROUTE_CACHE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const NATIVE_ROUTE_CACHE_SWEEP_MAX_ENTRIES: usize = 16 * 1024;

pub(super) use auth::*;
pub(super) use protocol::*;
pub(super) use proxy::*;

pub(super) async fn attach_session(
    State(state): State<DataPlaneState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
    uri: axum::http::Uri,
) -> Result<Response, AttachError> {
    reject_query_tokens(uri.query())?;
    let session_id = session_id
        .parse::<DataPlaneSessionId>()
        .map_err(|_| AttachError::BadRequest("invalid session id"))?;
    let header_token = token_from_headers(&headers)?;
    Ok(upgrade.on_upgrade(move |socket| async move {
        handle_socket(state, peer.ip(), session_id, header_token, socket).await;
    }))
}

pub(super) async fn handle_socket(
    state: DataPlaneState,
    source_ip: IpAddr,
    session_id: DataPlaneSessionId,
    header_token: Option<String>,
    mut socket: WebSocket,
) {
    let Ok(_guard) = state.unauthenticated.try_register(source_ip) else {
        pplx_space_metrics::data_plane::attach(
            session_id.kind().as_str(),
            "too_many_unauthenticated_sockets",
        );
        let _ = socket
            .send(Message::Text(
                error_json(DataPlaneErrorCode::TooManyUnauthenticatedSockets).into(),
            ))
            .await;
        let _ = socket.close().await;
        return;
    };

    let token = match header_token {
        Some(token) => token,
        None => match first_frame_attach_token(&mut socket, state.attach_deadline).await {
            Ok(token) => token,
            Err(error) => {
                pplx_space_metrics::data_plane::attach(session_id.kind().as_str(), error.as_str());
                let _ = socket.send(Message::Text(error_json(error).into())).await;
                let _ = socket.close().await;
                return;
            }
        },
    };

    let attached = match resolve_session_route(&state, session_id, &token).await {
        Ok(attached) => attached,
        Err(error) => {
            pplx_space_metrics::data_plane::attach(
                session_id.kind().as_str(),
                error.metric_label(),
            );
            let _ = socket
                .send(Message::Text(error_json(error.frame_code()).into()))
                .await;
            let _ = socket.close().await;
            return;
        }
    };
    let native_session_permit = match state.native_sessions.try_acquire(session_id, attached.kind) {
        Ok(permit) => permit,
        Err(()) => {
            pplx_space_metrics::data_plane::attach(
                session_id.kind().as_str(),
                "session_already_attached",
            );
            let _ = socket
                .send(Message::Text(
                    error_json(DataPlaneErrorCode::SessionAlreadyAttached).into(),
                ))
                .await;
            let _ = socket.close().await;
            return;
        }
    };
    let stream_permit = match state.attached_streams.try_acquire_attached(&attached) {
        Ok(permit) => permit,
        Err(limit) => {
            pplx_space_metrics::data_plane::attach(
                session_id.kind().as_str(),
                limit.metric_label(),
            );
            let _ = socket
                .send(Message::Text(
                    error_json(DataPlaneErrorCode::TooManyAttachedStreams).into(),
                ))
                .await;
            let _ = socket.close().await;
            return;
        }
    };

    drop(_guard);
    let _ = socket
        .send(Message::Text(attached_json(&attached).into()))
        .await;
    pplx_space_metrics::data_plane::attach(session_id.kind().as_str(), "ok");
    let _stream_permit = stream_permit;
    let _native_session_permit = native_session_permit;
    let revocation_cache = Arc::clone(&state.revocation_cache);
    bridge_attached_session(state, session_id, attached, socket).await;
    revocation_cache.forget(session_id);
}

pub(super) async fn session_events(
    State(state): State<DataPlaneState>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if let Err(error) = reject_query_tokens(uri.query()) {
        return error.into_response();
    }
    let session_id = match session_id.parse::<DataPlaneSessionId>() {
        Ok(session_id) => session_id,
        Err(_) => {
            return data_plane_http_error(
                StatusCode::BAD_REQUEST,
                DataPlaneHttpErrorCode::InvalidRequest,
                "invalid session id",
                false,
                ResponseOrigin::Client,
                ResponseKind::Validation,
            );
        }
    };
    if !matches!(
        session_id.kind(),
        SessionKind::Process | SessionKind::FileWatch
    ) {
        return data_plane_http_error(
            StatusCode::BAD_REQUEST,
            DataPlaneHttpErrorCode::InvalidRequest,
            "events endpoint only supports process and file-watch sessions",
            false,
            ResponseOrigin::Client,
            ResponseKind::Validation,
        );
    }
    let token = match token_from_headers(&headers) {
        Ok(Some(token)) => token,
        Ok(None) => {
            return data_plane_http_error(
                StatusCode::UNAUTHORIZED,
                DataPlaneHttpErrorCode::Unauthorized,
                "data-plane token required",
                false,
                ResponseOrigin::Client,
                ResponseKind::Auth,
            );
        }
        Err(error) => return error.into_response(),
    };
    let last_event_id = match last_event_id(&headers) {
        Ok(last_event_id) => last_event_id,
        Err(message) => {
            return data_plane_http_error(
                StatusCode::BAD_REQUEST,
                DataPlaneHttpErrorCode::InvalidRequest,
                message,
                false,
                ResponseOrigin::Client,
                ResponseKind::Validation,
            );
        }
    };

    let attached = match resolve_session_route(&state, session_id, &token).await {
        Ok(attached) => attached,
        Err(error) => {
            pplx_space_metrics::data_plane::attach(
                session_id.kind().as_str(),
                error.metric_label(),
            );
            return route_resolution_http_error(error);
        }
    };
    let stream_permit = match state.attached_streams.try_acquire_attached(&attached) {
        Ok(permit) => permit,
        Err(limit) => {
            pplx_space_metrics::data_plane::attach(
                session_id.kind().as_str(),
                limit.metric_label(),
            );
            return data_plane_http_error(
                StatusCode::TOO_MANY_REQUESTS,
                DataPlaneHttpErrorCode::RateLimited,
                "too many attached streams",
                true,
                ResponseOrigin::DataPlane,
                ResponseKind::Admission,
            );
        }
    };
    let stream_state = match attached.kind {
        SessionKind::Process => {
            let start_permit = match state.native_sessions.try_acquire(session_id, attached.kind) {
                Ok(permit) => permit,
                Err(()) => {
                    pplx_space_metrics::data_plane::attach(
                        session_id.kind().as_str(),
                        "session_already_attached",
                    );
                    return data_plane_http_error(
                        StatusCode::CONFLICT,
                        DataPlaneHttpErrorCode::Conflict,
                        "process session is already attached",
                        true,
                        ResponseOrigin::DataPlane,
                        ResponseKind::Admission,
                    );
                }
            };
            match ProcessSseState::new(
                state,
                session_id,
                attached,
                last_event_id,
                stream_permit,
                start_permit,
            )
            .await
            {
                Ok(stream_state) => SessionSseState::Process(stream_state),
                Err(error) => {
                    let result = stream_bridge_error_label(&error);
                    pplx_space_metrics::data_plane::attach(session_id.kind().as_str(), result);
                    tracing::warn!(%error, "failed to start process event stream");
                    return data_plane_http_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        DataPlaneHttpErrorCode::StreamUnavailable,
                        "process event stream is not available",
                        true,
                        ResponseOrigin::Node,
                        ResponseKind::BackendTransport,
                    );
                }
            }
        }
        SessionKind::FileWatch => {
            match FileWatchSseState::new(state, session_id, attached, last_event_id, stream_permit)
                .await
            {
                Ok(stream_state) => SessionSseState::FileWatch(stream_state),
                Err(error) => {
                    let result = stream_bridge_error_label(&error);
                    pplx_space_metrics::data_plane::attach(session_id.kind().as_str(), result);
                    tracing::warn!(%error, "failed to start file-watch event stream");
                    return data_plane_http_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        DataPlaneHttpErrorCode::StreamUnavailable,
                        "file-watch stream is not available",
                        true,
                        ResponseOrigin::Node,
                        ResponseKind::BackendTransport,
                    );
                }
            }
        }
        SessionKind::Terminal | SessionKind::Port | SessionKind::Protocol => {
            return data_plane_http_error(
                StatusCode::BAD_REQUEST,
                DataPlaneHttpErrorCode::InvalidRequest,
                "session kind does not support event streaming",
                false,
                ResponseOrigin::Client,
                ResponseKind::Validation,
            );
        }
    };

    pplx_space_metrics::data_plane::attach(session_id.kind().as_str(), "ok");
    let stream = futures_util::stream::unfold(stream_state, next_session_sse_event);
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

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

pub(super) async fn proxy_session(
    State(state): State<DataPlaneState>,
    Path(session_id): Path<String>,
    mut request: Request<Body>,
) -> Result<Response, ProxyError> {
    reject_query_tokens(request.uri().query())?;
    let session_id = session_id
        .parse::<DataPlaneSessionId>()
        .map_err(|_| ProxyError::BadRequest("invalid session id"))?;
    let (token, token_source) = token_from_headers_with_source(request.headers())
        .map_err(ProxyError::BadRequest)?
        .ok_or(ProxyError::Unauthorized("data-plane token is required"))?;
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
    let mut ingress_io = tokio::time::timeout_at(deadline, async {
        for attempt in 0..2 {
            match native_service_port_ingress_io(&state, &attached).await {
                Ok(io) => {
                    let fresh = resolve_session_route_with_cache(&state, session_id, &token, false)
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
                    attached = resolve_session_route_with_cache(&state, session_id, &token, false)
                        .await
                        .map_err(ProxyError::Route)?;
                }
            }
        }
        Err(ProxyError::Upstream(
            "session route changed during attachment",
        ))
    })
    .await
    .map_err(|_| ProxyError::Upstream("ingress preparation timed out"))??;
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
        let shutdown_token = state.shutdown_token.child_token();
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

pub(super) async fn bridge_attached_session(
    state: DataPlaneState,
    session_id: DataPlaneSessionId,
    attached: AttachedSession,
    socket: WebSocket,
) {
    match attached.kind {
        SessionKind::Terminal | SessionKind::Process => {
            let kind = attached.kind.as_str();
            match bridge_process_session(state, session_id, attached, socket).await {
                Ok(()) => pplx_space_metrics::data_plane::stream_closed(kind, "ok"),
                Err(error) => {
                    let result = stream_bridge_error_label(&error);
                    pplx_space_metrics::data_plane::stream_closed(kind, result);
                    tracing::warn!(%error, "data-plane process bridge failed");
                }
            }
        }
        SessionKind::FileWatch => {
            let kind = attached.kind.as_str();
            match bridge_file_watch_session(state, session_id, attached, socket).await {
                Ok(()) => pplx_space_metrics::data_plane::stream_closed(kind, "ok"),
                Err(error) => {
                    let result = stream_bridge_error_label(&error);
                    pplx_space_metrics::data_plane::stream_closed(kind, result);
                    tracing::warn!(%error, "data-plane file watch bridge failed");
                }
            }
        }
        SessionKind::Port if attached.target_protocol == Some(SessionTargetProtocol::Tcp) => {
            let kind = attached.kind.as_str();
            match bridge_raw_service_session(state, attached, socket).await {
                Ok(()) => pplx_space_metrics::data_plane::stream_closed(kind, "ok"),
                Err(error) => {
                    let result = stream_bridge_error_label(&error);
                    pplx_space_metrics::data_plane::stream_closed(kind, result);
                    tracing::warn!(%error, "data-plane raw service bridge failed");
                }
            }
        }
        SessionKind::Port | SessionKind::Protocol => {
            pplx_space_metrics::data_plane::stream_closed(
                attached.kind.as_str(),
                "unsupported_session_kind",
            );
            close_with_error(socket, DataPlaneErrorCode::UnsupportedSessionKind, false).await;
        }
    }
}

async fn bridge_raw_service_session(
    state: DataPlaneState,
    mut attached: AttachedSession,
    socket: WebSocket,
) -> Result<(), StreamBridgeError> {
    let session_id = attached.session_id;
    let ingress_io = native_service_io(&state, &mut attached).await?;
    let (mut ingress_read, mut ingress_write) = tokio::io::split(ingress_io);
    let (mut sink, mut source) = socket.split();
    let mut revocation_tick = revocation_check_interval(&state);
    let shutdown_token = state.shutdown_token.child_token();
    let mut buffer = vec![0_u8; 16 * 1024];

    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                let _ = sink.close().await;
                return Ok(());
            }
            _ = revocation_tick.tick() => {
                if session_is_revoked(&state, session_id).await? {
                    let _ = sink.close().await;
                    return Ok(());
                }
            }
            message = source.next() => {
                match message {
                    Some(Ok(Message::Binary(data))) => {
                        ingress_write.write_all(&data).await.map_err(|_| {
                            StreamBridgeError::RouteState("service_ingress_write_failed")
                        })?;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        let _ = ingress_write.shutdown().await;
                        return Ok(());
                    }
                    Some(Ok(Message::Ping(data))) => {
                        sink.send(Message::Pong(data))
                            .await
                            .map_err(|_| StreamBridgeError::ClientClosed)?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Text(_))) => {
                        return Err(StreamBridgeError::RouteState(
                            "raw_service_requires_binary_frames",
                        ));
                    }
                    Some(Err(_)) => return Err(StreamBridgeError::ClientClosed),
                }
            }
            read = ingress_read.read(&mut buffer) => {
                let read = read.map_err(|_| {
                    StreamBridgeError::RouteState("service_ingress_read_failed")
                })?;
                if read == 0 {
                    let _ = sink.close().await;
                    return Ok(());
                }
                let data = buffer
                    .get(..read)
                    .ok_or(StreamBridgeError::RouteState("service_ingress_read_failed"))?;
                sink.send(Message::Binary(data.to_vec().into()))
                    .await
                    .map_err(|_| StreamBridgeError::ClientClosed)?;
            }
        }
    }
}

async fn native_service_io(
    state: &DataPlaneState,
    attached: &mut AttachedSession,
) -> Result<Box<dyn AsyncReadWrite>, StreamBridgeError> {
    pplx_space_metrics::data_plane::service_route_transport(
        ServiceRouteSurface::Native,
        ServiceRouteTransport::ServicePortIngress,
    );
    let session_id = attached.session_id;
    let _waiter = pplx_space_common_svc::ingress_admission::IngressPreparationPermit::acquire(
        Some(attached.sandbox_id),
    )
    .map_err(StreamBridgeError::ClusterStatus)?;
    let deadline = tokio::time::Instant::now()
        + pplx_space_common_svc::ingress_admission::preparation_timeout();
    let io = tokio::time::timeout_at(deadline, async {
        for attempt in 0..2 {
            match native_service_port_ingress_io(state, attached).await {
                Ok(io) => {
                    let fresh = resolve_session_route_internal(state, session_id).await?;
                    if fresh.route_identity() == attached.route_identity() {
                        return Ok(io);
                    }
                    *attached = fresh;
                }
                Err(error) => {
                    let retryable = error
                        .transport_status
                        .as_ref()
                        .is_some_and(crate::runtime_route::retryable_ingress_attachment);
                    if attempt == 1 || !retryable {
                        return Err(native_service_port_ingress_open_error(error));
                    }
                    *attached = resolve_session_route_internal(state, session_id).await?;
                }
            }
        }
        Err(StreamBridgeError::RouteState("ingress_route_changed"))
    })
    .await
    .map_err(|_| native_route_refresh_timeout())??;
    Ok(Box::new(io))
}

trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

pub(super) async fn close_with_error(
    mut socket: WebSocket,
    code: DataPlaneErrorCode,
    retryable: bool,
) {
    let _ = socket
        .send(Message::Text(
            server_frame_json(&DataPlaneServerFrame::Error {
                message: code.as_str().to_string(),
                code,
                retryable,
            })
            .into(),
        ))
        .await;
    let _ = socket.close().await;
}

const MAX_CLIENT_FRAMES_DURING_OPEN: usize = 64;
const MAX_CLIENT_BYTES_DURING_OPEN: usize = 1024 * 1024;
/// How long a pong may wait for a client that stops reading before the client
/// is treated as gone.
const PONG_SEND_TIMEOUT: Duration = Duration::from_secs(5);

enum OpenWatch<T> {
    Opened(T, VecDeque<Message>),
    ClientClosed,
    Overflow,
}

/// Drive the initial backend open while reading the client socket, so a client
/// that disconnects mid-open drops the open. Pings are answered as the session
/// loop does; other frames are kept, in order and within a bound, for that loop.
///
/// A pong is sent alongside the open and the client read, never instead of
/// them, so a client that stops reading cannot stall either. Only the latest
/// unanswered ping is kept, which bounds pings as the limits bound other frames.
async fn open_while_watching_client<T, S, K>(
    open: impl Future<Output = T>,
    source: &mut S,
    sink: &mut K,
) -> OpenWatch<T>
where
    S: futures_util::Stream<Item = Result<Message, axum::Error>> + Unpin,
    K: futures_util::Sink<Message> + Unpin,
{
    tokio::pin!(open);
    let mut buffered = VecDeque::new();
    let mut buffered_bytes = 0_usize;
    // The payload of the ping to answer, and when its pong must be out by.
    let mut pong: Option<(bytes::Bytes, tokio::time::Instant)> = None;
    loop {
        tokio::select! {
            biased;
            result = &mut open => return OpenWatch::Opened(result, buffered),
            // A send interrupted by another branch is retried; a repeated pong
            // is harmless.
            sent = send_pong(sink, pong.clone()), if pong.is_some() => match sent {
                Ok(()) => pong = None,
                Err(()) => return OpenWatch::ClientClosed,
            },
            message = source.next() => match message {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return OpenWatch::ClientClosed,
                Some(Ok(Message::Ping(payload))) => {
                    let deadline = pong.as_ref().map_or_else(
                        || tokio::time::Instant::now() + PONG_SEND_TIMEOUT,
                        |(_, deadline)| *deadline,
                    );
                    pong = Some((payload, deadline));
                }
                Some(Ok(message)) => {
                    buffered_bytes = buffered_bytes.saturating_add(client_message_len(&message));
                    buffered.push_back(message);
                    if buffered.len() > MAX_CLIENT_FRAMES_DURING_OPEN
                        || buffered_bytes > MAX_CLIENT_BYTES_DURING_OPEN
                    {
                        return OpenWatch::Overflow;
                    }
                }
            },
        }
    }
}

/// Send a pending pong by its deadline; with none pending, never completes.
async fn send_pong<K>(
    sink: &mut K,
    pong: Option<(bytes::Bytes, tokio::time::Instant)>,
) -> Result<(), ()>
where
    K: futures_util::Sink<Message> + Unpin,
{
    let Some((payload, deadline)) = pong else {
        return std::future::pending().await;
    };
    match tokio::time::timeout_at(deadline, sink.send(Message::Pong(payload))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) | Err(_) => Err(()),
    }
}

fn client_message_len(message: &Message) -> usize {
    match message {
        Message::Text(text) => text.as_str().len(),
        Message::Binary(data) | Message::Ping(data) | Message::Pong(data) => data.len(),
        Message::Close(_) => 0,
    }
}

pub(super) async fn bridge_process_session(
    state: DataPlaneState,
    session_id: DataPlaneSessionId,
    mut attached: AttachedSession,
    socket: WebSocket,
) -> Result<(), StreamBridgeError> {
    let (mut sink, mut source) = socket.split();
    let open = open_native_process_stream(&state, session_id, &mut attached, None);
    let (initial, buffered) = match open_while_watching_client(open, &mut source, &mut sink).await {
        OpenWatch::Opened(initial, buffered) => (initial, buffered),
        // Dropping the open resets the backend stream, which cancels a start
        // that is still queued downstream.
        OpenWatch::ClientClosed => return Ok(()),
        // The open may already have passed the guest's start, so a fresh
        // session could run the command twice; only re-attaching this one is
        // safe, which is not what `retryable` tells a client.
        OpenWatch::Overflow => {
            let _ = send_server_frame(
                &mut sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::SessionNotReady,
                    message: "too many client frames before the session was ready".to_string(),
                    retryable: false,
                },
            )
            .await;
            let _ = sink.close().await;
            return Ok(());
        }
    };
    let mut source = futures_util::stream::iter(buffered.into_iter().map(Ok)).chain(source);
    let mut stream = match initial {
        Ok((_client, initial)) => initial,
        Err(error) => {
            let _ = send_server_frame(&mut sink, &stream_start_error_frame(&error)).await;
            let _ = sink.close().await;
            return Err(error);
        }
    };
    let mut output_seq = 0_u64;
    let mut process_id = attached.backend_process_id.clone();
    let mut process_bound = attached.backend_process_id.is_some();
    let mut revocation_tick = revocation_check_interval(&state);

    loop {
        tokio::select! {
            _ = revocation_tick.tick() => {
                if session_is_revoked(&state, session_id).await? {
                    send_server_frame(
                        &mut sink,
                        &DataPlaneServerFrame::Closed {
                            reason: StreamCloseReason::Revoked,
                        },
                    )
                    .await
                    .map_err(|_| StreamBridgeError::ClientClosed)?;
                    let _ = sink.close().await;
                    return Ok(());
                }
            }
            event = stream.next() => {
                let Some(event) = event else {
                    let _ = send_server_frame(
                        &mut sink,
                        &DataPlaneServerFrame::Closed {
                            reason: StreamCloseReason::BackendClosed,
                        },
                    )
                    .await;
                    let _ = sink.close().await;
                    return Ok(());
                };
                match event {
                    Ok(event) => {
                        if matches!(
                            event.event,
                            Some(spaced_process_event::Event::Started(_))
                                | Some(spaced_process_event::Event::Connected(_))
                        ) {
                            let event_process_id: ProcessId = event.process_id.parse()
                                .map_err(|_| StreamBridgeError::RouteState("invalid_backend_process_id"))?;
                            process_id = Some(event_process_id.clone());
                            if !process_bound {
                                bind_session_process(&state, session_id, &event_process_id).await?;
                                process_bound = true;
                            }
                        }
                        let batch = process_event_frames(event, &mut output_seq);
                        record_process_output_bytes(attached.kind.as_str(), &batch);
                        for frame in batch.frames {
                            if send_server_frame(&mut sink, &frame).await.is_err() {
                                return Ok(());
                            }
                            if matches!(frame, DataPlaneServerFrame::ProcessExited { .. }) {
                                let _ = send_server_frame(
                                    &mut sink,
                                    &DataPlaneServerFrame::Closed {
                                        reason: StreamCloseReason::BackendClosed,
                                    },
                                )
                                .await;
                                let _ = sink.close().await;
                                return Ok(());
                            }
                        }
                    }
                    Err(status) => {
                        if (status.code() == tonic::Code::Unavailable
                            || is_refreshable_route_identity_rejection(&status))
                            && let Some(current_process_id) = process_id.clone()
                        {
                            let (refreshed, refresh_dimension) =
                                refresh_native_stream_route(&state, &attached, &status).await?;
                            let reason = reconnect_reason_for_route_change(
                                attached.route_generation,
                                refreshed.route_generation,
                            );
                            record_backend_channel_reset(attached.kind.as_str(), &reason);
                            send_server_frame(
                                &mut sink,
                                &DataPlaneServerFrame::BackendReconnecting {
                                    reason: reason.clone(),
                                    route_generation: attached.route_generation,
                                },
                            )
                            .await
                            .map_err(|_| StreamBridgeError::ClientClosed)?;
                            record_session_reconnect(
                                &state,
                                session_id,
                                reason,
                                attached.route_generation,
                            )
                            .await?;
                            let reopened = async {
                                let channel = state
                                    .backend_channels
                                    .channel(&refreshed.backend_endpoint, session_id).await?;
                                let mut reconnect_client =
                                    WorkerApiClient::new(wrap_grpc_client_channel(channel));
                                connect_process_stream(
                                    &mut reconnect_client,
                                    &refreshed.sandbox_id,
                                    &current_process_id,
                                    None,
                                    refreshed.space_api_route_token.as_ref(),
                                )
                                .await
                            }
                            .await;
                            stream = finish_native_stream_refresh(refresh_dimension, reopened)?;
                            send_server_frame(
                                &mut sink,
                                &DataPlaneServerFrame::BackendReconnected {
                                    route_generation: refreshed.route_generation,
                                    current_node_id: refreshed.current_node_id.to_string(),
                                },
                            )
                            .await
                            .map_err(|_| StreamBridgeError::ClientClosed)?;
                            send_server_frame(
                                &mut sink,
                                &resync_required_frame(
                                    attached.kind.as_str(),
                                    DataPlaneResyncReason::ProcessStreamReconnected,
                                ),
                            )
                            .await
                            .map_err(|_| StreamBridgeError::ClientClosed)?;
                            attached = refreshed;
                            continue;
                        }
                        let _ = send_server_frame(
                            &mut sink,
                            &DataPlaneServerFrame::Error {
                                code: DataPlaneErrorCode::BackendStreamError,
                                message: status.message().to_string(),
                                retryable: backend_stream_status_retryable(&status),
                            },
                        )
                        .await;
                        let _ = sink.close().await;
                        return Err(StreamBridgeError::BackendStatus(status));
                    }
                }
            }
            message = source.next() => {
                let Some(message) = message else {
                    return Ok(());
                };
                match message {
                    Ok(Message::Close(_)) | Err(_) => return Ok(()),
                    Ok(Message::Ping(payload)) => {
                        sink.send(Message::Pong(payload)).await.map_err(|_| StreamBridgeError::ClientClosed)?;
                    }
                    Ok(Message::Text(text)) => {
                        if space_api_route_token_needs_refresh(&attached.space_api_route_token) {
                            let refreshed = refresh_session_route(&state, &attached).await?;
                            if attached_process_route_changed(&attached, &refreshed) {
                                let Some(current_process_id) = process_id.clone() else {
                                    return Err(StreamBridgeError::RouteState(
                                        "route_token_refresh_process_unbound",
                                    ));
                                };
                                let reason = reconnect_reason_for_route_change(
                                    attached.route_generation,
                                    refreshed.route_generation,
                                );
                                record_backend_channel_reset(attached.kind.as_str(), &reason);
                                send_server_frame(
                                    &mut sink,
                                    &DataPlaneServerFrame::BackendReconnecting {
                                        reason: reason.clone(),
                                        route_generation: attached.route_generation,
                                    },
                                )
                                .await
                                .map_err(|_| StreamBridgeError::ClientClosed)?;
                                record_session_reconnect(
                                    &state,
                                    session_id,
                                    reason,
                                    attached.route_generation,
                                )
                                .await?;
                                let channel = state
                                    .backend_channels
                                    .channel(&refreshed.backend_endpoint, session_id).await?;
                                let mut reconnect_client =
                                    WorkerApiClient::new(wrap_grpc_client_channel(channel));
                                stream = connect_process_stream(
                                    &mut reconnect_client,
                                    &refreshed.sandbox_id,
                                    &current_process_id,
                                    None,
                                    refreshed.space_api_route_token.as_ref(),
                                )
                                .await?;
                                send_server_frame(
                                    &mut sink,
                                    &DataPlaneServerFrame::BackendReconnected {
                                        route_generation: refreshed.route_generation,
                                        current_node_id: refreshed.current_node_id.to_string(),
                                    },
                                )
                                .await
                                .map_err(|_| StreamBridgeError::ClientClosed)?;
                                send_server_frame(
                                    &mut sink,
                                    &resync_required_frame(
                                        attached.kind.as_str(),
                                        DataPlaneResyncReason::ProcessStreamReconnected,
                                    ),
                                )
                                .await
                                .map_err(|_| StreamBridgeError::ClientClosed)?;
                            }
                            attached = refreshed;
                        }
                        if handle_process_client_control_frame(
                            &mut sink,
                            &state,
                            &mut attached,
                            process_id.as_ref(),
                            &text,
                        )
                        .await?
                            == ClientFrameAction::Close
                        {
                            let _ = sink.close().await;
                            return Ok(());
                        }
                    }
                    Ok(Message::Binary(_)) => {
                        send_server_frame(
                            &mut sink,
                            &DataPlaneServerFrame::Error {
                                code: DataPlaneErrorCode::BinaryClientFramesNotSupported,
                                message: "binary client frames are not supported".to_string(),
                                retryable: false,
                            },
                        )
                        .await
                        .map_err(|_| StreamBridgeError::ClientClosed)?;
                    }
                    Ok(Message::Pong(_)) => {}
                }
            }
        }
    }
}

async fn initial_process_stream(
    client: &mut WorkerApiClient<GrpcClientTrace<Channel>>,
    attached: &AttachedSession,
    session_id: DataPlaneSessionId,
    output_cursor: Option<u64>,
) -> Result<tonic::Streaming<ProcessEvent>, StreamBridgeError> {
    if let Some(process_id) = attached.backend_process_id.as_ref() {
        return connect_process_stream(
            client,
            &attached.sandbox_id,
            process_id,
            output_cursor,
            attached.space_api_route_token.as_ref(),
        )
        .await;
    }

    let start = process_start_request(attached, session_id)?;
    let started = start_process_stream(
        client,
        &attached.sandbox_id,
        start,
        attached.space_api_route_token.as_ref(),
    )
    .await;

    match started {
        // The guest already holds the process this session assigned, so the
        // start succeeded earlier and its binding was never persisted. The
        // process id is derived rather than discovered, so recovery does not
        // depend on that binding: connect to the existing process instead of
        // running the command a second time.
        Err(StreamBridgeError::BackendStatus(status))
            if status.code() == tonic::Code::AlreadyExists =>
        {
            connect_process_stream(
                client,
                &attached.sandbox_id,
                &process_id_for_session(session_id),
                output_cursor,
                attached.space_api_route_token.as_ref(),
            )
            .await
        }
        result => result,
    }
}

async fn open_native_process_stream(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
    attached: &mut AttachedSession,
    output_cursor: Option<u64>,
) -> Result<
    (
        WorkerApiClient<GrpcClientTrace<Channel>>,
        tonic::Streaming<ProcessEvent>,
    ),
    StreamBridgeError,
> {
    native_route_call(state, attached, move |route| async move {
        let channel = state
            .backend_channels
            .channel(&route.backend_endpoint, session_id)
            .await?;
        let mut client = WorkerApiClient::new(wrap_grpc_client_channel(channel));
        let stream = initial_process_stream(&mut client, &route, session_id, output_cursor).await?;
        Ok((client, stream))
    })
    .await
}

fn stream_start_error_frame(error: &StreamBridgeError) -> DataPlaneServerFrame {
    let retryable = matches!(
        error,
        StreamBridgeError::BackendStatus(status)
            if backend_stream_status_retryable(status)
    );
    DataPlaneServerFrame::Error {
        code: DataPlaneErrorCode::BackendStreamError,
        message: error.to_string(),
        retryable,
    }
}

pub(super) fn backend_stream_status_retryable(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Unavailable || is_refreshable_route_identity_rejection(status)
}

pub(super) async fn start_process_stream(
    client: &mut WorkerApiClient<GrpcClientTrace<Channel>>,
    sandbox_id: &SandboxId,
    start: StartRequest,
    route_token: Option<&SpaceApiRouteTokenState>,
) -> Result<tonic::Streaming<ProcessEvent>, StreamBridgeError> {
    let request = node_api_request_with_route_token_state(
        SpacedStartProcessRequest {
            sandbox_id: sandbox_id.to_string(),
            operation: Some(start),
        },
        route_token,
    )?;
    Ok(client
        .spaced_start_process(request)
        .await
        .map_err(StreamBridgeError::BackendStatus)?
        .into_inner())
}

pub(super) async fn connect_process_stream(
    client: &mut WorkerApiClient<GrpcClientTrace<Channel>>,
    sandbox_id: &SandboxId,
    process_id: &ProcessId,
    output_cursor: Option<u64>,
    route_token: Option<&SpaceApiRouteTokenState>,
) -> Result<tonic::Streaming<ProcessEvent>, StreamBridgeError> {
    let request = node_api_request_with_route_token_state(
        SpacedConnectProcessRequest {
            sandbox_id: sandbox_id.to_string(),
            operation: Some(ConnectProcessRequest {
                process_id: process_id.to_string(),
                timeout: None,
                output_cursor,
            }),
        },
        route_token,
    )?;
    Ok(client
        .spaced_connect_process(request)
        .await
        .map_err(StreamBridgeError::BackendStatus)?
        .into_inner())
}

pub(super) async fn record_session_reconnect(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
    reason: ReconnectReason,
    observed_route_generation: u64,
) -> Result<u64, StreamBridgeError> {
    let mut client = state.cluster_api.for_session(&session_id);
    let response = client
        .record_data_plane_session_reconnect(RecordDataPlaneSessionReconnectRequest {
            session_id: session_id.to_string(),
            protocol_session_id: protocol_session_id(session_id),
            reason: proto_reconnect_reason(&reason) as i32,
            observed_route_generation,
        })
        .await
        .map_err(StreamBridgeError::ClusterStatus)?
        .into_inner();
    pplx_space_metrics::data_plane::session_reconnect(session_id.kind().as_str(), reason.as_str());
    Ok(response.reconnect_count)
}

pub(super) fn reconnect_reason_for_route_change(previous: u64, current: u64) -> ReconnectReason {
    if current != previous {
        ReconnectReason::RouteGenerationChanged
    } else {
        ReconnectReason::WorkerUnavailable
    }
}

pub(super) fn attached_process_route_changed(
    current: &AttachedSession,
    refreshed: &AttachedSession,
) -> bool {
    current.sandbox_id != refreshed.sandbox_id
        || current.route_generation != refreshed.route_generation
        || current.current_node_id != refreshed.current_node_id
        || current.backend_endpoint != refreshed.backend_endpoint
}

pub(super) fn record_backend_channel_reset(kind: &str, reason: &ReconnectReason) {
    pplx_space_metrics::data_plane::backend_channel_reset(kind, reason.as_str());
    pplx_space_metrics::data_plane::backend_affected_streams(kind, 1);
}

pub(super) fn proto_reconnect_reason(reason: &ReconnectReason) -> DataPlaneReconnectReason {
    match reason {
        ReconnectReason::BackendRestart => DataPlaneReconnectReason::BackendRestart,
        ReconnectReason::SandboxMigration => DataPlaneReconnectReason::SandboxMigration,
        ReconnectReason::RouteGenerationChanged => DataPlaneReconnectReason::RouteGenerationChanged,
        ReconnectReason::WorkerUnavailable => DataPlaneReconnectReason::WorkerUnavailable,
        ReconnectReason::DataPlaneRestart => DataPlaneReconnectReason::DataPlaneRestart,
        // Unknown reasons come from a newer data-plane build; the cluster API
        // keeps the count and the public frame keeps the original string.
        ReconnectReason::Other(_) => DataPlaneReconnectReason::Unspecified,
    }
}

pub(super) async fn bridge_file_watch_session(
    state: DataPlaneState,
    session_id: DataPlaneSessionId,
    mut attached: AttachedSession,
    socket: WebSocket,
) -> Result<(), StreamBridgeError> {
    let watch = file_watch_request(&attached)?;
    let (_client, mut stream) =
        open_native_watch_stream(&state, session_id, &mut attached, watch.clone()).await?;

    let (mut sink, mut source) = socket.split();
    let mut event_seq = 0_u64;
    let mut revocation_tick = revocation_check_interval(&state);

    loop {
        tokio::select! {
            _ = revocation_tick.tick() => {
                if session_is_revoked(&state, session_id).await? {
                    send_server_frame(
                        &mut sink,
                        &DataPlaneServerFrame::Closed {
                            reason: StreamCloseReason::Revoked,
                        },
                    )
                    .await
                    .map_err(|_| StreamBridgeError::ClientClosed)?;
                    let _ = sink.close().await;
                    return Ok(());
                }
            }
            event = stream.next() => {
                let Some(event) = event else {
                    let _ = send_server_frame(
                        &mut sink,
                        &DataPlaneServerFrame::Closed {
                            reason: StreamCloseReason::BackendClosed,
                        },
                    )
                    .await;
                    let _ = sink.close().await;
                    return Ok(());
                };
                match event {
                    Ok(event) => {
                        event_seq = event_seq.saturating_add(1);
                        let frame = file_event_frame(event, event_seq);
                        if send_server_frame(&mut sink, &frame).await.is_err() {
                            return Ok(());
                        }
                    }
                    Err(status) => {
                        if status.code() == tonic::Code::Unavailable
                            || is_refreshable_route_identity_rejection(&status)
                        {
                            let (refreshed, refresh_dimension) =
                                refresh_native_stream_route(&state, &attached, &status).await?;
                            let reason = reconnect_reason_for_route_change(
                                attached.route_generation,
                                refreshed.route_generation,
                            );
                            record_backend_channel_reset(attached.kind.as_str(), &reason);
                            send_server_frame(
                                &mut sink,
                                &DataPlaneServerFrame::BackendReconnecting {
                                    reason: reason.clone(),
                                    route_generation: attached.route_generation,
                                },
                            )
                            .await
                            .map_err(|_| StreamBridgeError::ClientClosed)?;
                            record_session_reconnect(
                                &state,
                                session_id,
                                reason,
                                attached.route_generation,
                            )
                            .await?;
                            let reopened = async {
                                let channel = state
                                    .backend_channels
                                    .channel(&refreshed.backend_endpoint, session_id).await?;
                                let mut reconnect_client =
                                    WorkerApiClient::new(wrap_grpc_client_channel(channel));
                                watch_stream(
                                    &mut reconnect_client,
                                    &refreshed.sandbox_id,
                                    watch.clone(),
                                    refreshed.space_api_route_token.as_ref(),
                                )
                                .await
                            }
                            .await;
                            stream = finish_native_stream_refresh(refresh_dimension, reopened)?;
                            send_server_frame(
                                &mut sink,
                                &DataPlaneServerFrame::BackendReconnected {
                                    route_generation: refreshed.route_generation,
                                    current_node_id: refreshed.current_node_id.to_string(),
                                },
                            )
                            .await
                            .map_err(|_| StreamBridgeError::ClientClosed)?;
                            send_server_frame(
                                &mut sink,
                                &resync_required_frame(
                                    attached.kind.as_str(),
                                    DataPlaneResyncReason::FileWatchRestarted,
                                ),
                            )
                            .await
                            .map_err(|_| StreamBridgeError::ClientClosed)?;
                            attached = refreshed;
                            continue;
                        }
                        let _ = send_server_frame(
                            &mut sink,
                            &DataPlaneServerFrame::Error {
                                code: DataPlaneErrorCode::BackendStreamError,
                                message: status.message().to_string(),
                                retryable: backend_stream_status_retryable(&status),
                            },
                        )
                        .await;
                        let _ = sink.close().await;
                        return Err(StreamBridgeError::BackendStatus(status));
                    }
                }
            }
            message = source.next() => {
                let Some(message) = message else {
                    return Ok(());
                };
                match message {
                    Ok(Message::Close(_)) | Err(_) => return Ok(()),
                    Ok(Message::Ping(payload)) => {
                        sink.send(Message::Pong(payload)).await.map_err(|_| StreamBridgeError::ClientClosed)?;
                    }
                    Ok(Message::Text(text)) => {
                        if handle_client_control_frame(&mut sink, &text).await?
                            == ClientFrameAction::Close
                        {
                            let _ = sink.close().await;
                            return Ok(());
                        }
                    }
                    Ok(Message::Binary(_)) => {
                        send_server_frame(
                            &mut sink,
                            &DataPlaneServerFrame::Error {
                                code: DataPlaneErrorCode::BinaryClientFramesNotSupported,
                                message: "binary client frames are not supported".to_string(),
                                retryable: false,
                            },
                        )
                        .await
                        .map_err(|_| StreamBridgeError::ClientClosed)?;
                    }
                    Ok(Message::Pong(_)) => {}
                }
            }
        }
    }
}

pub(super) enum SessionSseState {
    Process(ProcessSseState),
    FileWatch(FileWatchSseState),
}

pub(super) async fn next_session_sse_event(
    state: SessionSseState,
) -> Option<(Result<Event, Infallible>, SessionSseState)> {
    match state {
        SessionSseState::Process(state) => next_process_sse_event(state)
            .await
            .map(|(event, state)| (event, SessionSseState::Process(state))),
        SessionSseState::FileWatch(state) => next_file_watch_sse_event(state)
            .await
            .map(|(event, state)| (event, SessionSseState::FileWatch(state))),
    }
}

pub(super) struct ProcessSseState {
    state: DataPlaneState,
    session_id: DataPlaneSessionId,
    attached: AttachedSession,
    client: WorkerApiClient<GrpcClientTrace<Channel>>,
    stream: tonic::Streaming<ProcessEvent>,
    event_seq: u64,
    backend_output_cursor: Option<u64>,
    process_id: Option<ProcessId>,
    process_bound: bool,
    revocation_tick: tokio::time::Interval,
    pending: VecDeque<Event>,
    finished: bool,
    start_permit: Option<NativeSessionAttachPermit>,
    _stream_permit: AttachedStreamPermit,
}

pub(super) struct ProcessReplayState {
    pub(super) event_seq: u64,
    pub(super) backend_output_cursor: Option<u64>,
    pub(super) pending: VecDeque<Event>,
}

/// What a `Last-Event-ID` resume produces before any live event: the next SSE
/// id and the `resync_required(cursor_replay_unavailable)` frame that carries
/// it. `None` when the client did not send a cursor.
///
/// This is the public reconnection contract for both process and file-watch
/// sessions: the data plane resumes the *live* stream at `last + 1` and does
/// not replay what was missed, because it does not map SSE ids onto the guest
/// output journal. Keep this the single place that decides so the docs, the
/// conformance smoke, and both session kinds cannot drift.
///
/// `last + 1` cannot overflow: the request boundary rejects cursors above
/// [`auth::MAX_LAST_EVENT_ID`] (`i64::MAX`) with a 400, so a resumed stream
/// never has to saturate onto an id the client already acknowledged.
pub(super) fn resume_resync_frame(
    kind: &str,
    last_event_id: Option<u64>,
) -> Option<(u64, DataPlaneServerFrame)> {
    let after_sequence = last_event_id?;
    debug_assert!(after_sequence <= auth::MAX_LAST_EVENT_ID);
    Some((
        after_sequence.saturating_add(1),
        resync_required_frame(kind, DataPlaneResyncReason::CursorReplayUnavailable),
    ))
}

impl ProcessReplayState {
    pub(super) fn load(session_id: DataPlaneSessionId, last_event_id: Option<u64>) -> Self {
        let Some((event_seq, frame)) =
            resume_resync_frame(session_id.kind().as_str(), last_event_id)
        else {
            return Self {
                event_seq: 0,
                backend_output_cursor: None,
                pending: VecDeque::new(),
            };
        };

        let mut pending = VecDeque::new();
        pending.push_back(sse_event_from_frame(&frame, Some(event_seq)));

        Self {
            event_seq,
            backend_output_cursor: None,
            pending,
        }
    }
}

impl ProcessSseState {
    async fn new(
        state: DataPlaneState,
        session_id: DataPlaneSessionId,
        mut attached: AttachedSession,
        last_event_id: Option<u64>,
        stream_permit: AttachedStreamPermit,
        start_permit: Option<NativeSessionAttachPermit>,
    ) -> Result<Self, StreamBridgeError> {
        let replay = ProcessReplayState::load(session_id, last_event_id);
        let (client, stream) = open_native_process_stream(
            &state,
            session_id,
            &mut attached,
            replay.backend_output_cursor,
        )
        .await?;
        let mut pending = VecDeque::new();
        pending.push_back(sse_event_from_frame(&attached_frame(&attached), None));
        pending.extend(replay.pending);
        let process_id = attached.backend_process_id.clone();
        let process_bound = process_id.is_some();
        let revocation_tick = revocation_check_interval(&state);
        let backend_output_cursor = replay
            .backend_output_cursor
            .or_else(|| attached.backend_process_id.is_none().then_some(0));

        Ok(Self {
            state,
            session_id,
            attached,
            client,
            stream,
            event_seq: replay.event_seq,
            backend_output_cursor,
            process_id,
            process_bound,
            revocation_tick,
            pending,
            finished: false,
            start_permit,
            _stream_permit: stream_permit,
        })
    }

    fn push_frame(&mut self, frame: DataPlaneServerFrame) {
        self.event_seq = self.event_seq.saturating_add(1);
        self.pending
            .push_back(sse_event_from_frame(&frame, Some(self.event_seq)));
    }

    fn finish(&mut self, result: &'static str) {
        pplx_space_metrics::data_plane::stream_closed(self.session_id.kind().as_str(), result);
        self.finished = true;
    }

    fn finish_with_frame(&mut self, frame: DataPlaneServerFrame, result: &'static str) {
        pplx_space_metrics::data_plane::stream_closed(self.session_id.kind().as_str(), result);
        self.finished = true;
        self.push_frame(frame);
    }

    fn finish_with_error(&mut self, error: StreamBridgeError) {
        let result = stream_bridge_error_label(&error);
        pplx_space_metrics::data_plane::stream_closed(self.session_id.kind().as_str(), result);
        tracing::warn!(%error, "data-plane process event stream failed");
        self.finished = true;
        self.push_frame(DataPlaneServerFrame::Error {
            code: DataPlaneErrorCode::BackendStreamError,
            message: error.to_string(),
            retryable: true,
        });
    }
}

impl Drop for ProcessSseState {
    fn drop(&mut self) {
        self.state.revocation_cache.forget(self.session_id);
    }
}

pub(super) async fn next_process_sse_event(
    mut state: ProcessSseState,
) -> Option<(Result<Event, Infallible>, ProcessSseState)> {
    if let Some(event) = state.pending.pop_front() {
        return Some((Ok(event), state));
    }
    if state.finished {
        return None;
    }

    loop {
        tokio::select! {
            _ = state.revocation_tick.tick() => {
                match session_is_revoked(&state.state, state.session_id).await {
                    Ok(true) => {
                        state.finish_with_frame(
                            DataPlaneServerFrame::Closed {
                                reason: StreamCloseReason::Revoked,
                            },
                            "revoked",
                        );
                        let event = state.pending.pop_front()?;
                        return Some((Ok(event), state));
                    }
                    Ok(false) => {}
                    Err(error) => {
                        state.finish_with_error(error);
                        let event = state.pending.pop_front()?;
                        return Some((Ok(event), state));
                    }
                }
            }
            event = state.stream.next() => {
                let Some(event) = event else {
                    state.finish_with_frame(
                        DataPlaneServerFrame::Closed {
                            reason: StreamCloseReason::BackendClosed,
                        },
                        "backend_closed",
                    );
                    let event = state.pending.pop_front()?;
                    return Some((Ok(event), state));
                };
                match event {
                    Ok(event) => {
                        if matches!(
                            event.event,
                            Some(spaced_process_event::Event::Started(_))
                                | Some(spaced_process_event::Event::Connected(_))
                        ) {
                            let event_process_id: ProcessId = match event.process_id.parse() {
                                Ok(process_id) => process_id,
                                Err(_) => {
                                    state.finish_with_error(StreamBridgeError::RouteState(
                                        "invalid_backend_process_id",
                                    ));
                                    let event = state.pending.pop_front()?;
                                    return Some((Ok(event), state));
                                }
                            };
                            state.process_id = Some(event_process_id.clone());
                            if !state.process_bound {
                                match bind_session_process(
                                    &state.state,
                                    state.session_id,
                                    &event_process_id,
                                )
                                .await
                                {
                                    Ok(()) => {
                                        state.process_bound = true;
                                        state.start_permit = None;
                                    }
                                    Err(error) => {
                                        state.finish_with_error(error);
                                        let event = state.pending.pop_front()?;
                                        return Some((Ok(event), state));
                                    }
                                }
                            }
                        }
                        let mut output_seq = state.backend_output_cursor.unwrap_or_default();
                        let batch = process_event_frames(event, &mut output_seq);
                        record_process_output_bytes(state.session_id.kind().as_str(), &batch);
                        for frame in batch.frames {
                            let exited =
                                matches!(frame, DataPlaneServerFrame::ProcessExited { .. });
                            let advances_backend_cursor =
                                process_frame_advances_backend_cursor(&frame);
                            state.push_frame(frame);
                            if advances_backend_cursor {
                                state.backend_output_cursor = Some(output_seq);
                            }
                            if exited {
                                state.push_frame(DataPlaneServerFrame::Closed {
                                    reason: StreamCloseReason::BackendClosed,
                                });
                                state.finish("backend_closed");
                            }
                        }
                        if let Some(event) = state.pending.pop_front() {
                            return Some((Ok(event), state));
                        }
                    }
                    Err(status)
                        if status.code() == tonic::Code::Unavailable
                            || is_refreshable_route_identity_rejection(&status) =>
                    {
                        let Some(current_process_id) = state.process_id.clone() else {
                            state.finish_with_error(StreamBridgeError::BackendStatus(status));
                            let event = state.pending.pop_front()?;
                            return Some((Ok(event), state));
                        };
                        let current_generation = state.attached.route_generation;
                        let (refreshed, refresh_dimension) =
                            match refresh_native_stream_route(
                                &state.state,
                                &state.attached,
                                &status,
                            )
                            .await
                            {
                            Ok(refreshed) => refreshed,
                            Err(error) => {
                                state.finish_with_error(error);
                                let event = state.pending.pop_front()?;
                                return Some((Ok(event), state));
                            }
                        };
                        let reason = reconnect_reason_for_route_change(
                            current_generation,
                            refreshed.route_generation,
                        );
                        record_backend_channel_reset(state.session_id.kind().as_str(), &reason);
                        state.push_frame(DataPlaneServerFrame::BackendReconnecting {
                            reason: reason.clone(),
                            route_generation: current_generation,
                        });
                        if let Err(error) = record_session_reconnect(
                            &state.state,
                            state.session_id,
                            reason,
                            current_generation,
                        )
                        .await
                        {
                            state.finish_with_error(error);
                            let event = state.pending.pop_front()?;
                            return Some((Ok(event), state));
                        }
                        let reopened = async {
                            let channel = state
                                .state
                                .backend_channels
                                .channel(&refreshed.backend_endpoint, state.session_id).await?;
                            let mut client =
                                WorkerApiClient::new(wrap_grpc_client_channel(channel));
                            let stream = connect_process_stream(
                                &mut client,
                                &refreshed.sandbox_id,
                                &current_process_id,
                                state.backend_output_cursor,
                                refreshed.space_api_route_token.as_ref(),
                            )
                            .await?;
                            Ok((client, stream))
                        }
                        .await;
                        match finish_native_stream_refresh(refresh_dimension, reopened) {
                            Ok((client, stream)) => {
                                state.client = client;
                                state.stream = stream;
                                state.push_frame(DataPlaneServerFrame::BackendReconnected {
                                    route_generation: refreshed.route_generation,
                                    current_node_id: refreshed.current_node_id.to_string(),
                                });
                                state.push_frame(resync_required_frame(
                                    state.session_id.kind().as_str(),
                                    DataPlaneResyncReason::ProcessStreamReconnected,
                                ));
                                state.attached = refreshed;
                            }
                            Err(error) => state.finish_with_error(error),
                        }
                        let event = state.pending.pop_front()?;
                        return Some((Ok(event), state));
                    }
                    Err(status) => {
                        state.finish_with_error(StreamBridgeError::BackendStatus(status));
                        let event = state.pending.pop_front()?;
                        return Some((Ok(event), state));
                    }
                }
            }
        }
    }
}

pub(super) struct FileWatchSseState {
    state: DataPlaneState,
    session_id: DataPlaneSessionId,
    attached: AttachedSession,
    watch: WatchOperation,
    client: WorkerApiClient<GrpcClientTrace<Channel>>,
    stream: tonic::Streaming<FileEvent>,
    event_seq: u64,
    revocation_tick: tokio::time::Interval,
    pending: VecDeque<Event>,
    finished: bool,
    _stream_permit: AttachedStreamPermit,
}

impl FileWatchSseState {
    async fn new(
        state: DataPlaneState,
        session_id: DataPlaneSessionId,
        attached: AttachedSession,
        last_event_id: Option<u64>,
        stream_permit: AttachedStreamPermit,
    ) -> Result<Self, StreamBridgeError> {
        let mut attached =
            refresh_file_watch_attached_if_route_token_stale(&state, attached).await?;
        let watch = file_watch_request(&attached)?;
        let (client, stream) =
            open_native_watch_stream(&state, session_id, &mut attached, watch.clone()).await?;
        let mut pending = VecDeque::new();
        pending.push_back(sse_event_from_frame(&attached_frame(&attached), None));
        let mut event_seq = last_event_id.unwrap_or_default();
        if let Some((resumed_seq, frame)) =
            resume_resync_frame(session_id.kind().as_str(), last_event_id)
        {
            event_seq = resumed_seq;
            pending.push_back(sse_event_from_frame(&frame, Some(event_seq)));
        }
        let revocation_tick = revocation_check_interval(&state);

        Ok(Self {
            state,
            session_id,
            attached,
            watch,
            client,
            stream,
            event_seq,
            revocation_tick,
            pending,
            finished: false,
            _stream_permit: stream_permit,
        })
    }

    fn push_frame(&mut self, frame: DataPlaneServerFrame) {
        self.event_seq = self.event_seq.saturating_add(1);
        self.pending
            .push_back(sse_event_from_frame(&frame, Some(self.event_seq)));
    }

    fn finish_with_frame(&mut self, frame: DataPlaneServerFrame, result: &'static str) {
        pplx_space_metrics::data_plane::stream_closed(self.session_id.kind().as_str(), result);
        self.finished = true;
        self.push_frame(frame);
    }

    fn finish_with_error(&mut self, error: StreamBridgeError) {
        let result = stream_bridge_error_label(&error);
        pplx_space_metrics::data_plane::stream_closed(self.session_id.kind().as_str(), result);
        tracing::warn!(%error, "data-plane file watch event stream failed");
        self.finished = true;
        self.push_frame(DataPlaneServerFrame::Error {
            code: DataPlaneErrorCode::BackendStreamError,
            message: error.to_string(),
            retryable: true,
        });
    }
}

impl Drop for FileWatchSseState {
    fn drop(&mut self) {
        self.state.revocation_cache.forget(self.session_id);
    }
}

pub(super) async fn next_file_watch_sse_event(
    mut state: FileWatchSseState,
) -> Option<(Result<Event, Infallible>, FileWatchSseState)> {
    if let Some(event) = state.pending.pop_front() {
        return Some((Ok(event), state));
    }
    if state.finished {
        return None;
    }

    loop {
        tokio::select! {
            _ = state.revocation_tick.tick() => {
                match session_is_revoked(&state.state, state.session_id).await {
                    Ok(true) => {
                        state.finish_with_frame(
                            DataPlaneServerFrame::Closed {
                                reason: StreamCloseReason::Revoked,
                            },
                            "revoked",
                        );
                        let event = state.pending.pop_front()?;
                        return Some((Ok(event), state));
                    }
                    Ok(false) => {}
                    Err(error) => {
                        state.finish_with_error(error);
                        let event = state.pending.pop_front()?;
                        return Some((Ok(event), state));
                    }
                }
            }
            event = state.stream.next() => {
                let Some(event) = event else {
                    state.finish_with_frame(
                        DataPlaneServerFrame::Closed {
                            reason: StreamCloseReason::BackendClosed,
                        },
                        "backend_closed",
                    );
                    let event = state.pending.pop_front()?;
                    return Some((Ok(event), state));
                };
                match event {
                    Ok(event) => {
                        state.push_frame(file_event_frame(event, state.event_seq.saturating_add(1)));
                        let event = state.pending.pop_front()?;
                        return Some((Ok(event), state));
                    }
                    Err(status)
                        if status.code() == tonic::Code::Unavailable
                            || is_refreshable_route_identity_rejection(&status) =>
                    {
                        let current_generation = state.attached.route_generation;
                        let (refreshed, refresh_dimension) =
                            match refresh_native_stream_route(
                                &state.state,
                                &state.attached,
                                &status,
                            )
                            .await
                            {
                            Ok(refreshed) => refreshed,
                            Err(error) => {
                                state.finish_with_error(error);
                                let event = state.pending.pop_front()?;
                                return Some((Ok(event), state));
                            }
                        };
                        let refreshed = match refresh_file_watch_attached_if_route_token_stale(
                            &state.state,
                            refreshed,
                        )
                        .await
                        {
                            Ok(refreshed) => refreshed,
                            Err(error) => {
                                state.finish_with_error(native_stream_refresh_failed(
                                    refresh_dimension,
                                    error,
                                ));
                                let event = state.pending.pop_front()?;
                                return Some((Ok(event), state));
                            }
                        };
                        let reason = reconnect_reason_for_route_change(
                            current_generation,
                            refreshed.route_generation,
                        );
                        record_backend_channel_reset(state.session_id.kind().as_str(), &reason);
                        state.push_frame(DataPlaneServerFrame::BackendReconnecting {
                            reason: reason.clone(),
                            route_generation: current_generation,
                        });
                        if let Err(error) = record_session_reconnect(
                            &state.state,
                            state.session_id,
                            reason,
                            current_generation,
                        )
                        .await
                        {
                            state.finish_with_error(error);
                            let event = state.pending.pop_front()?;
                            return Some((Ok(event), state));
                        }
                        let reopened = async {
                            let channel = state
                                .state
                                .backend_channels
                                .channel(&refreshed.backend_endpoint, state.session_id).await?;
                            let mut client =
                                WorkerApiClient::new(wrap_grpc_client_channel(channel));
                            let stream = watch_stream(
                                &mut client,
                                &refreshed.sandbox_id,
                                state.watch.clone(),
                                refreshed.space_api_route_token.as_ref(),
                            )
                            .await?;
                            Ok((client, stream))
                        }
                        .await;
                        match finish_native_stream_refresh(refresh_dimension, reopened) {
                            Ok((client, stream)) => {
                                state.client = client;
                                state.stream = stream;
                                state.push_frame(DataPlaneServerFrame::BackendReconnected {
                                    route_generation: refreshed.route_generation,
                                    current_node_id: refreshed.current_node_id.to_string(),
                                });
                                state.push_frame(resync_required_frame(
                                    state.session_id.kind().as_str(),
                                    DataPlaneResyncReason::FileWatchRestarted,
                                ));
                                state.attached = refreshed;
                            }
                            Err(error) => state.finish_with_error(error),
                        }
                        let event = state.pending.pop_front()?;
                        return Some((Ok(event), state));
                    }
                    Err(status) => {
                        state.finish_with_error(StreamBridgeError::BackendStatus(status));
                        let event = state.pending.pop_front()?;
                        return Some((Ok(event), state));
                    }
                }
            }
        }
    }
}

async fn refresh_file_watch_attached_if_route_token_stale(
    state: &DataPlaneState,
    attached: AttachedSession,
) -> Result<AttachedSession, StreamBridgeError> {
    let stale = attached.clone();
    refresh_file_watch_attached_if_route_token_stale_with(attached, || {
        refresh_session_route(state, &stale)
    })
    .await
}

pub(super) async fn refresh_file_watch_attached_if_route_token_stale_with<F, Fut>(
    attached: AttachedSession,
    resolve: F,
) -> Result<AttachedSession, StreamBridgeError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<AttachedSession, StreamBridgeError>>,
{
    if file_watch_attached_needs_route_token_refresh(&attached) {
        resolve().await
    } else {
        Ok(attached)
    }
}

pub(super) fn file_watch_attached_needs_route_token_refresh(attached: &AttachedSession) -> bool {
    space_api_route_token_needs_refresh(&attached.space_api_route_token)
}

pub(super) async fn watch_stream(
    client: &mut WorkerApiClient<GrpcClientTrace<Channel>>,
    sandbox_id: &SandboxId,
    watch: WatchOperation,
    route_token: Option<&SpaceApiRouteTokenState>,
) -> Result<tonic::Streaming<FileEvent>, StreamBridgeError> {
    let request = node_api_request_with_route_token_state(
        SpacedWatchRequest {
            sandbox_id: sandbox_id.to_string(),
            operation: Some(watch),
        },
        route_token,
    )?;
    Ok(client
        .spaced_watch(request)
        .await
        .map_err(StreamBridgeError::BackendStatus)?
        .into_inner())
}

async fn open_native_watch_stream(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
    attached: &mut AttachedSession,
    watch: WatchOperation,
) -> Result<
    (
        WorkerApiClient<GrpcClientTrace<Channel>>,
        tonic::Streaming<FileEvent>,
    ),
    StreamBridgeError,
> {
    native_route_call(state, attached, move |route| {
        let watch = watch.clone();
        async move {
            let channel = state
                .backend_channels
                .channel(&route.backend_endpoint, session_id)
                .await?;
            let mut client = WorkerApiClient::new(wrap_grpc_client_channel(channel));
            let stream = watch_stream(
                &mut client,
                &route.sandbox_id,
                watch,
                route.space_api_route_token.as_ref(),
            )
            .await?;
            Ok((client, stream))
        }
    })
    .await
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClientFrameAction {
    Continue,
    Close,
}

pub(super) async fn handle_process_client_control_frame<S>(
    sink: &mut S,
    state: &DataPlaneState,
    attached: &mut AttachedSession,
    process_id: Option<&ProcessId>,
    text: &str,
) -> Result<ClientFrameAction, StreamBridgeError>
where
    S: SinkExt<Message> + Unpin,
{
    let frame = serde_json::from_str::<DataPlaneClientFrame>(text)
        .map_err(|_| StreamBridgeError::InvalidClientFrame)?;
    match frame {
        DataPlaneClientFrame::Attach { .. } => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::AlreadyAttached,
                    message: "session is already attached".to_string(),
                    retryable: false,
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Input { seq, data_base64 } => {
            let Some(process_id) = process_id else {
                send_server_frame(
                    sink,
                    &DataPlaneServerFrame::Error {
                        code: DataPlaneErrorCode::ProcessNotStarted,
                        message: "process has not started yet".to_string(),
                        retryable: true,
                    },
                )
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
                return Ok(ClientFrameAction::Continue);
            };
            let data = BASE64_STANDARD
                .decode(data_base64)
                .map_err(|_| StreamBridgeError::InvalidClientFrame)?;
            let bytes = data.len() as u64;
            native_node_api_unary(
                state,
                attached,
                SpacedSendInputRequest {
                    sandbox_id: attached.sandbox_id.to_string(),
                    operation: Some(SendInputRequest {
                        process_id: process_id.to_string(),
                        data,
                    }),
                },
                |mut client, request| async move { client.spaced_send_input(request).await },
            )
            .await?;
            pplx_space_metrics::data_plane::stream_bytes_in(attached.kind.as_str(), bytes);
            send_server_frame(sink, &DataPlaneServerFrame::InputAccepted { seq })
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Resize { cols, rows } => {
            let Some(process_id) = process_id else {
                send_server_frame(
                    sink,
                    &DataPlaneServerFrame::Error {
                        code: DataPlaneErrorCode::ProcessNotStarted,
                        message: "process has not started yet".to_string(),
                        retryable: true,
                    },
                )
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
                return Ok(ClientFrameAction::Continue);
            };
            if cols == 0 || rows == 0 {
                send_server_frame(
                    sink,
                    &DataPlaneServerFrame::Error {
                        code: DataPlaneErrorCode::InvalidResize,
                        message: "terminal resize rows and cols must be non-zero".to_string(),
                        retryable: false,
                    },
                )
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
                return Ok(ClientFrameAction::Continue);
            }
            let resize = native_node_api_unary(
                state,
                attached,
                SpacedResizePtyRequest {
                    sandbox_id: attached.sandbox_id.to_string(),
                    operation: Some(ResizePtyRequest {
                        process_id: process_id.to_string(),
                        size: Some(PtySize {
                            rows: u32::from(rows),
                            cols: u32::from(cols),
                        }),
                    }),
                },
                |mut client, request| async move { client.spaced_resize_pty(request).await },
            )
            .await;
            match resize {
                Ok(_) => {}
                Err(StreamBridgeError::BackendStatus(status))
                    if status.code() == tonic::Code::FailedPrecondition =>
                {
                    send_server_frame(
                        sink,
                        &DataPlaneServerFrame::Error {
                            code: DataPlaneErrorCode::ResizeNotSupported,
                            message: status.message().to_string(),
                            retryable: false,
                        },
                    )
                    .await
                    .map_err(|_| StreamBridgeError::ClientClosed)?;
                }
                Err(error) => return Err(error),
            }
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Signal { seq, signal } => {
            let Some(process_id) = process_id else {
                send_server_frame(
                    sink,
                    &DataPlaneServerFrame::Error {
                        code: DataPlaneErrorCode::ProcessNotStarted,
                        message: "process has not started yet".to_string(),
                        retryable: true,
                    },
                )
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
                return Ok(ClientFrameAction::Continue);
            };
            native_node_api_unary(
                state,
                attached,
                SpacedSendSignalRequest {
                    sandbox_id: attached.sandbox_id.to_string(),
                    operation: Some(SendSignalRequest {
                        process_id: process_id.to_string(),
                        signal,
                    }),
                },
                |mut client, request| async move { client.spaced_send_signal(request).await },
            )
            .await?;
            send_server_frame(sink, &DataPlaneServerFrame::ControlAccepted { seq })
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::CloseStdin { seq } => {
            let Some(process_id) = process_id else {
                send_server_frame(
                    sink,
                    &DataPlaneServerFrame::Error {
                        code: DataPlaneErrorCode::ProcessNotStarted,
                        message: "process has not started yet".to_string(),
                        retryable: true,
                    },
                )
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
                return Ok(ClientFrameAction::Continue);
            };
            native_node_api_unary(
                state,
                attached,
                SpacedCloseStdinRequest {
                    sandbox_id: attached.sandbox_id.to_string(),
                    operation: Some(CloseStdinRequest {
                        process_id: process_id.to_string(),
                    }),
                },
                |mut client, request| async move { client.spaced_close_stdin(request).await },
            )
            .await?;
            send_server_frame(sink, &DataPlaneServerFrame::ControlAccepted { seq })
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Heartbeat { seq } => {
            send_server_frame(sink, &DataPlaneServerFrame::ControlAccepted { seq })
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Close => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Closed {
                    reason: StreamCloseReason::ClientClosed,
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Close)
        }
    }
}

pub(super) async fn handle_client_control_frame<S>(
    sink: &mut S,
    text: &str,
) -> Result<ClientFrameAction, StreamBridgeError>
where
    S: SinkExt<Message> + Unpin,
{
    let frame = serde_json::from_str::<DataPlaneClientFrame>(text)
        .map_err(|_| StreamBridgeError::InvalidClientFrame)?;
    match frame {
        DataPlaneClientFrame::Attach { .. } => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::AlreadyAttached,
                    message: "session is already attached".to_string(),
                    retryable: false,
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Input { .. } => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::InputNotSupported,
                    message: "interactive stdin is not supported by this backend stream"
                        .to_string(),
                    retryable: false,
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Signal { .. } | DataPlaneClientFrame::CloseStdin { .. } => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::InputNotSupported,
                    message: "process control is not supported by this backend stream".to_string(),
                    retryable: false,
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Heartbeat { seq } => {
            send_server_frame(sink, &DataPlaneServerFrame::ControlAccepted { seq })
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Resize { .. } => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::ResizeNotSupported,
                    message: "terminal resize is not supported by this backend stream".to_string(),
                    retryable: false,
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Close => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Closed {
                    reason: StreamCloseReason::ClientClosed,
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Close)
        }
    }
}

#[cfg(test)]
mod tests;
