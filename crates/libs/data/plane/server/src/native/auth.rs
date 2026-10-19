use super::*;
use std::future::Future;

pub(crate) async fn first_frame_attach_token(
    socket: &mut WebSocket,
    deadline: Duration,
) -> Result<String, DataPlaneErrorCode> {
    first_frame_attach_token_from_recv(socket.recv(), deadline).await
}

pub(crate) async fn first_frame_attach_token_from_recv<F, E>(
    recv: F,
    deadline: Duration,
) -> Result<String, DataPlaneErrorCode>
where
    F: Future<Output = Option<Result<Message, E>>>,
{
    let message = tokio::time::timeout(deadline, recv)
        .await
        .map_err(|_| DataPlaneErrorCode::AttachDeadlineExceeded)?
        .ok_or(DataPlaneErrorCode::AttachRequired)?
        .map_err(|_| DataPlaneErrorCode::AttachFrameInvalid)?;
    let Message::Text(text) = message else {
        return Err(DataPlaneErrorCode::AttachFrameInvalid);
    };
    match serde_json::from_str::<DataPlaneClientFrame>(&text)
        .map_err(|_| DataPlaneErrorCode::AttachFrameInvalid)?
    {
        DataPlaneClientFrame::Attach { token } => {
            validate_token_shape(&token).map_err(|_| DataPlaneErrorCode::InvalidToken)?;
            Ok(token)
        }
        DataPlaneClientFrame::Input { .. }
        | DataPlaneClientFrame::Resize { .. }
        | DataPlaneClientFrame::Signal { .. }
        | DataPlaneClientFrame::CloseStdin { .. }
        | DataPlaneClientFrame::Heartbeat { .. }
        | DataPlaneClientFrame::Close => Err(DataPlaneErrorCode::AttachFrameInvalid),
    }
}

pub(crate) fn token_from_headers(headers: &HeaderMap) -> Result<Option<String>, AttachError> {
    token_from_headers_with_source(headers)
        .map(|token| token.map(|(token, _source)| token))
        .map_err(AttachError::BadRequest)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HeaderTokenSource {
    Authorization,
    WebSocketProtocol,
}

pub(crate) fn token_from_headers_with_source(
    headers: &HeaderMap,
) -> Result<Option<(String, HeaderTokenSource)>, &'static str> {
    if let Some(token) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().strip_prefix("Bearer "))
    {
        validate_token_shape(token)?;
        return Ok(Some((token.to_string(), HeaderTokenSource::Authorization)));
    }

    if let Some(protocols) = headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        && let Some(token) = protocols
            .split(',')
            .map(str::trim)
            .find(|value| value.starts_with(DATA_PLANE_TOKEN_PREFIX))
    {
        validate_token_shape(token)?;
        return Ok(Some((
            token.to_string(),
            HeaderTokenSource::WebSocketProtocol,
        )));
    }

    Ok(None)
}

pub(crate) fn reject_query_tokens(query: Option<&str>) -> Result<(), AttachError> {
    let Some(query) = query else {
        return Ok(());
    };
    for (key, _) in form_urlencoded::parse(query.as_bytes()) {
        if matches!(key.as_ref(), "token" | "access_token" | "data_plane_token") {
            return Err(AttachError::BadRequest(
                "data-plane tokens must not be sent in query strings",
            ));
        }
    }
    Ok(())
}

/// Largest `Last-Event-ID` a client may resume from. It is the OpenAPI
/// parameter's `int64` maximum, and it leaves room for the `last + 1` id the
/// resume contract promises, so the stream can never saturate onto an id the
/// client already acknowledged.
pub(crate) const MAX_LAST_EVENT_ID: u64 = i64::MAX as u64;

pub(crate) fn last_event_id(headers: &HeaderMap) -> Result<Option<u64>, &'static str> {
    let Some(value) = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    match value.parse::<u64>() {
        Ok(cursor) if cursor <= MAX_LAST_EVENT_ID => Ok(Some(cursor)),
        Ok(_) => Err("last-event-id must not exceed 9223372036854775807"),
        Err(_) => Err("last-event-id must be an unsigned integer"),
    }
}

pub(crate) fn validate_token_shape(token: &str) -> Result<(), &'static str> {
    let Some(hex) = token.strip_prefix(DATA_PLANE_TOKEN_PREFIX) else {
        return Err("invalid_token");
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("invalid_token");
    }
    Ok(())
}

pub(crate) fn token_sha256(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum RouteResolutionError {
    Unauthenticated,
    Forbidden,
    SessionNotFound,
    SessionNotReady,
    ResolutionFailed,
    RouteStateInvalid,
}

impl RouteResolutionError {
    pub(crate) const fn metric_label(self) -> &'static str {
        match self {
            Self::Unauthenticated => "unauthenticated",
            Self::Forbidden => "forbidden",
            Self::SessionNotFound => "session_not_found",
            Self::SessionNotReady => "session_not_ready",
            Self::ResolutionFailed => "route_resolution_failed",
            Self::RouteStateInvalid => "route_state_invalid",
        }
    }

    pub(crate) const fn frame_code(self) -> DataPlaneErrorCode {
        match self {
            Self::Unauthenticated => DataPlaneErrorCode::Unauthenticated,
            Self::Forbidden => DataPlaneErrorCode::Forbidden,
            Self::SessionNotFound => DataPlaneErrorCode::SessionNotFound,
            Self::SessionNotReady => DataPlaneErrorCode::SessionNotReady,
            Self::ResolutionFailed => DataPlaneErrorCode::RouteResolutionFailed,
            Self::RouteStateInvalid => DataPlaneErrorCode::RouteStateInvalid,
        }
    }

    pub(crate) const fn classification(self) -> (ResponseOrigin, ResponseKind) {
        match self {
            Self::Unauthenticated | Self::Forbidden => (ResponseOrigin::Client, ResponseKind::Auth),
            Self::SessionNotFound => (ResponseOrigin::Client, ResponseKind::Validation),
            Self::SessionNotReady => (ResponseOrigin::Cluster, ResponseKind::RouteNotReady),
            Self::ResolutionFailed => (ResponseOrigin::Cluster, ResponseKind::RouteResolution),
            Self::RouteStateInvalid => (ResponseOrigin::DataPlane, ResponseKind::Internal),
        }
    }
}

pub(crate) fn route_resolution_http_error(error: RouteResolutionError) -> Response {
    let (origin, kind) = error.classification();
    match error {
        RouteResolutionError::Unauthenticated => data_plane_http_error(
            StatusCode::UNAUTHORIZED,
            DataPlaneHttpErrorCode::Unauthorized,
            "invalid or expired data-plane token",
            false,
            origin,
            kind,
        ),
        RouteResolutionError::Forbidden => data_plane_http_error(
            StatusCode::FORBIDDEN,
            DataPlaneHttpErrorCode::Forbidden,
            "data-plane token does not allow this session",
            false,
            origin,
            kind,
        ),
        RouteResolutionError::SessionNotFound => data_plane_http_error(
            StatusCode::NOT_FOUND,
            DataPlaneHttpErrorCode::NotFound,
            "session not found",
            false,
            origin,
            kind,
        ),
        RouteResolutionError::SessionNotReady => data_plane_http_error(
            StatusCode::SERVICE_UNAVAILABLE,
            DataPlaneHttpErrorCode::NotReady,
            "session route is not ready",
            true,
            origin,
            kind,
        ),
        RouteResolutionError::ResolutionFailed => data_plane_http_error(
            StatusCode::SERVICE_UNAVAILABLE,
            DataPlaneHttpErrorCode::ServiceUnavailable,
            "session route could not be resolved",
            true,
            origin,
            kind,
        ),
        RouteResolutionError::RouteStateInvalid => data_plane_http_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            DataPlaneHttpErrorCode::Internal,
            "session route is invalid",
            false,
            origin,
            kind,
        ),
    }
}

pub(crate) fn data_plane_http_error(
    status: StatusCode,
    code: DataPlaneHttpErrorCode,
    message: &'static str,
    retryable: bool,
    origin: ResponseOrigin,
    kind: ResponseKind,
) -> Response {
    observe_data_plane_http_error(status, origin, kind);

    (
        status,
        Json(DataPlaneHttpErrorEnvelope {
            error: DataPlaneHttpError {
                code,
                message: message.to_string(),
                retryable,
            },
        }),
    )
        .into_response()
}

/// `Retry-After` from a backend retry hint, in whole seconds between one and
/// a minute; none when the backend gave no hint.
pub(crate) fn insert_retry_after(response: &mut Response, try_again: Option<std::time::Duration>) {
    let Some(try_again) = try_again else {
        return;
    };
    let seconds = try_again.as_millis().div_ceil(1000).clamp(1, 60);
    if let Ok(value) = axum::http::HeaderValue::from_str(&seconds.to_string()) {
        response
            .headers_mut()
            .insert(axum::http::header::RETRY_AFTER, value);
    }
}

pub(crate) fn too_many_attached_streams_response(limit: AttachedStreamLimit) -> Response {
    let mut response = data_plane_http_error(
        StatusCode::TOO_MANY_REQUESTS,
        DataPlaneHttpErrorCode::RateLimited,
        "too many attached streams",
        true,
        ResponseOrigin::DataPlane,
        ResponseKind::Admission,
    );
    response
        .extensions_mut()
        .insert(limit.too_many_requests_reason());
    response
}

pub(crate) fn observe_data_plane_http_error(
    status: StatusCode,
    origin: ResponseOrigin,
    kind: ResponseKind,
) {
    pplx_space_metrics::data_plane::response(
        ResponseSurface::Native,
        origin,
        kind,
        status.as_u16(),
    );
    tracing::Span::current().record("space.response_origin", origin.as_str());
    tracing::Span::current().record("space.response_kind", kind.as_str());
}
