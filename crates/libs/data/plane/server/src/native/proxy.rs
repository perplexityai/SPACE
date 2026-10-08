use super::*;

pub(crate) fn rewrite_proxy_request_for_ingress(
    request: &mut Request<Body>,
    session_id: DataPlaneSessionId,
    token_source: HeaderTokenSource,
) -> Result<(), ProxyError> {
    *request.uri_mut() = proxy_path_and_query(session_id, request.uri())?
        .parse::<Uri>()
        .map_err(|_| ProxyError::Internal("invalid proxy path"))?;
    strip_data_plane_auth_headers(request.headers_mut(), token_source)?;
    Ok(())
}

pub(crate) fn proxy_path_and_query(
    session_id: DataPlaneSessionId,
    uri: &Uri,
) -> Result<String, ProxyError> {
    let prefix = format!("/v1/sessions/{session_id}/proxy");
    let path = uri
        .path()
        .strip_prefix(&prefix)
        .ok_or(ProxyError::BadRequest("invalid proxy path"))?;
    let path = if path.is_empty() { "/" } else { path };
    let query = uri
        .query()
        .map(|query| format!("?{query}"))
        .unwrap_or_default();
    Ok(format!("{path}{query}"))
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct CdpProxyBase {
    pub(crate) http_base: String,
    pub(crate) ws_base: String,
}

impl CdpProxyBase {
    pub(crate) fn from_headers(headers: &HeaderMap, session_id: DataPlaneSessionId) -> Self {
        let host = forwarded_header_value(headers, "x-forwarded-host")
            .or_else(|| {
                headers
                    .get(header::HOST)
                    .and_then(|value| value.to_str().ok())
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned)
            })
            .unwrap_or_else(|| "localhost".to_string());
        let http_scheme = forwarded_header_value(headers, "x-forwarded-proto")
            .map(|value| value.to_ascii_lowercase())
            .filter(|value| matches!(value.as_str(), "http" | "https"))
            .unwrap_or_else(|| "http".to_string());
        let ws_scheme = if http_scheme == "https" { "wss" } else { "ws" };
        let proxy_prefix = format!("/v1/sessions/{session_id}/proxy");
        let http_base = format!("{http_scheme}://{host}{proxy_prefix}");
        let ws_base = format!("{ws_scheme}://{host}{proxy_prefix}");
        Self { http_base, ws_base }
    }
}

pub(crate) fn forwarded_header_value(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

/// DevTools endpoints whose responses carry debugger URLs to rewrite. Others,
/// such as the full `/json/protocol` schema, stream unchanged.
pub(crate) fn is_cdp_discovery_path(path: &str) -> bool {
    matches!(
        path.trim_end_matches('/'),
        "/json" | "/json/list" | "/json/version" | "/json/new"
    )
}

/// DevTools discovery documents are a few KiB; matches the ECW JSON body limit.
pub(crate) const CDP_DISCOVERY_BODY_LIMIT_BYTES: usize = 1024 * 1024;

pub(crate) async fn rewrite_cdp_discovery_response<B>(
    response: hyper::Response<B>,
    proxy_base: &CdpProxyBase,
) -> Result<Response, ProxyError>
where
    B: hyper::body::Body<Data = bytes::Bytes>,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let (mut parts, body) = response.into_parts();
    let bytes = http_body_util::Limited::new(body, CDP_DISCOVERY_BODY_LIMIT_BYTES)
        .collect()
        .await
        .map_err(|error| {
            tracing::debug!(%error, "failed to read CDP discovery response body");
            ProxyError::Upstream("failed to read protocol discovery response")
        })?
        .to_bytes();
    let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        record_native_proxied_response(parts.status);
        return Ok(Response::from_parts(parts, Body::from(bytes)).into_response());
    };
    rewrite_cdp_discovery_value(&mut value, proxy_base);
    let body = serde_json::to_vec(&value).map_err(|error| {
        tracing::debug!(%error, "failed to serialize rewritten CDP discovery response");
        ProxyError::Internal("failed to rewrite protocol discovery response")
    })?;
    parts.headers.remove(header::CONTENT_LENGTH);
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    record_native_proxied_response(parts.status);
    Ok(Response::from_parts(parts, Body::from(body)).into_response())
}

pub(crate) fn rewrite_cdp_discovery_value(
    value: &mut serde_json::Value,
    proxy_base: &CdpProxyBase,
) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                rewrite_cdp_discovery_value(value, proxy_base);
            }
        }
        serde_json::Value::Object(object) => {
            if let Some(raw_url) = object
                .get("webSocketDebuggerUrl")
                .and_then(serde_json::Value::as_str)
                && let Some(path) = debugger_url_path(raw_url)
            {
                object.insert(
                    "webSocketDebuggerUrl".to_string(),
                    serde_json::Value::String(format!("{}{}", proxy_base.ws_base, path)),
                );
            }
            if let Some(raw_url) = object
                .get("devtoolsFrontendUrl")
                .and_then(serde_json::Value::as_str)
                && let Some(url) = rewrite_devtools_frontend_url(raw_url, proxy_base)
            {
                object.insert(
                    "devtoolsFrontendUrl".to_string(),
                    serde_json::Value::String(url),
                );
            }
        }
        _ => {}
    }
}

pub(crate) fn debugger_url_path(raw_url: &str) -> Option<String> {
    if raw_url.starts_with('/') {
        return Some(raw_url.to_string());
    }
    let (_scheme, rest) = raw_url.split_once("://")?;
    let slash = rest.find('/')?;
    rest.get(slash..).map(ToOwned::to_owned)
}

pub(crate) fn rewrite_devtools_frontend_url(
    raw_url: &str,
    proxy_base: &CdpProxyBase,
) -> Option<String> {
    let path = debugger_url_path(raw_url)?;
    let Some((frontend_path, query)) = path.split_once('?') else {
        return Some(format!("{}{}", proxy_base.http_base, path));
    };
    let rewritten_query = query
        .split('&')
        .map(|part| {
            part.strip_prefix("ws=")
                .and_then(|target| devtools_ws_query_value(target, proxy_base))
                .map(|target| format!("ws={target}"))
                .unwrap_or_else(|| part.to_string())
        })
        .collect::<Vec<_>>()
        .join("&");
    Some(format!(
        "{}{}?{}",
        proxy_base.http_base, frontend_path, rewritten_query
    ))
}

pub(crate) fn devtools_ws_query_value(
    raw_target: &str,
    proxy_base: &CdpProxyBase,
) -> Option<String> {
    let target_path = if raw_target.starts_with("ws://") || raw_target.starts_with("wss://") {
        debugger_url_path(raw_target)?
    } else if raw_target.starts_with('/') {
        raw_target.to_string()
    } else {
        let slash = raw_target.find('/')?;
        raw_target.get(slash..)?.to_string()
    };
    proxy_base
        .ws_base
        .split_once("://")
        .map(|(_, base)| format!("{base}{target_path}"))
}

pub(crate) fn strip_data_plane_auth_headers(
    headers: &mut HeaderMap,
    token_source: HeaderTokenSource,
) -> Result<(), ProxyError> {
    match token_source {
        HeaderTokenSource::Authorization => {
            headers.remove(header::AUTHORIZATION);
        }
        HeaderTokenSource::WebSocketProtocol => {
            if let Some(protocols) = headers
                .get(header::SEC_WEBSOCKET_PROTOCOL)
                .and_then(|value| value.to_str().ok())
            {
                let forwarded = protocols
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.starts_with(DATA_PLANE_TOKEN_PREFIX))
                    .filter(|value| !value.is_empty())
                    .collect::<Vec<_>>()
                    .join(", ");
                if forwarded.is_empty() {
                    headers.remove(header::SEC_WEBSOCKET_PROTOCOL);
                } else {
                    let value = HeaderValue::from_str(&forwarded)
                        .map_err(|_| ProxyError::BadRequest("invalid websocket protocols"))?;
                    headers.insert(header::SEC_WEBSOCKET_PROTOCOL, value);
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn is_upgrade_request(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONNECTION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
        })
        && headers.contains_key(header::UPGRADE)
}

pub(crate) async fn proxy_upgraded_connection(
    kind: &'static str,
    downstream: hyper::upgrade::OnUpgrade,
    upstream: hyper::upgrade::OnUpgrade,
    shutdown_token: CancellationToken,
) -> Result<UpgradedProxyClose, Box<dyn std::error::Error + Send + Sync>> {
    let (downstream, upstream) = tokio::try_join!(downstream, upstream)?;
    proxy_streams(
        kind,
        TokioIo::new(downstream),
        TokioIo::new(upstream),
        shutdown_token,
    )
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpgradedProxyClose {
    Completed,
    Revoked,
    Shutdown,
}

impl UpgradedProxyClose {
    pub(crate) const fn result_label(self) -> &'static str {
        match self {
            Self::Completed => "ok",
            Self::Revoked => "revoked",
            Self::Shutdown => "shutdown",
        }
    }
}

pub(crate) async fn proxy_upgraded_session_connection(
    state: DataPlaneState,
    session_id: DataPlaneSessionId,
    kind: &'static str,
    downstream: hyper::upgrade::OnUpgrade,
    upstream: hyper::upgrade::OnUpgrade,
    shutdown_token: CancellationToken,
) -> Result<UpgradedProxyClose, Box<dyn std::error::Error + Send + Sync>> {
    let (downstream, upstream) = tokio::try_join!(downstream, upstream)?;
    let copy = proxy_streams(
        kind,
        TokioIo::new(downstream),
        TokioIo::new(upstream),
        shutdown_token,
    );
    tokio::pin!(copy);
    let mut revocation_tick = revocation_check_interval(&state);

    loop {
        tokio::select! {
            result = &mut copy => {
                return result;
            }
            _ = revocation_tick.tick() => {
                if session_is_revoked(&state, session_id).await? {
                    return Ok(UpgradedProxyClose::Revoked);
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ProxyDirection {
    In,
    Out,
}

pub(crate) async fn proxy_streams<A, B>(
    kind: &'static str,
    downstream: A,
    upstream: B,
    shutdown_token: CancellationToken,
) -> Result<UpgradedProxyClose, Box<dyn std::error::Error + Send + Sync>>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    let (mut downstream_read, mut downstream_write) = io::split(downstream);
    let (mut upstream_read, mut upstream_write) = io::split(upstream);
    let downstream_to_upstream = metered_copy(
        kind,
        ProxyDirection::In,
        &mut downstream_read,
        &mut upstream_write,
    );
    let upstream_to_downstream = metered_copy(
        kind,
        ProxyDirection::Out,
        &mut upstream_read,
        &mut downstream_write,
    );

    tokio::select! {
        result = async {
            tokio::try_join!(downstream_to_upstream, upstream_to_downstream)
        } => {
            result?;
            Ok(UpgradedProxyClose::Completed)
        }
        _ = shutdown_token.cancelled() => Ok(UpgradedProxyClose::Shutdown),
    }
}

pub(crate) async fn metered_copy<R, W>(
    kind: &'static str,
    direction: ProxyDirection,
    reader: &mut R,
    writer: &mut W,
) -> io::Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut total = 0_u64;
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            writer.shutdown().await?;
            return Ok(total);
        }
        let chunk = buffer.get(..read).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "read exceeded buffer length")
        })?;
        writer.write_all(chunk).await?;
        let bytes = u64::try_from(read)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "read length exceeds u64"))?;
        total = total.saturating_add(bytes);
        match direction {
            ProxyDirection::In => pplx_space_metrics::data_plane::stream_bytes_in(kind, bytes),
            ProxyDirection::Out => pplx_space_metrics::data_plane::stream_bytes_out(kind, bytes),
        }
    }
}
