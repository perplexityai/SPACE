use pplx_space_common_core::types::SandboxId;

use std::net::{IpAddr, SocketAddr};

use axum::{
    Extension, Router,
    extract::{ConnectInfo, OriginalUri, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};

use pplx_space_metrics::sandbox::api::client_limit::ClientScope;

use crate::middleware::auth::EcwCallerIdentity;

use super::template_build::{
    check_alias, create_template_v3, file_upload_capability_org, get_build_status, get_file_upload,
    put_uploaded_file, start_build,
};
use super::{
    AppState, ECW_SANDBOX_API_LOG_EVENT, EcwHostConfig, EcwRequestLogContext, connect_sandbox,
    connect_sandbox_v2, create_sandbox, delete_sandbox, fork_sandbox, get_sandbox, list_sandboxes,
    pause_sandbox_e2b, record_ecw_sandbox_span_tag, refresh_sandbox, request_client_id,
    require_ecw_api_host, resume_sandbox_e2b, timeout_sandbox,
};

pub(crate) fn dedicated_host_router(state: AppState) -> Router<AppState> {
    dedicated_host_router_with_ecw_hosts(state, EcwHostConfig::from_env())
}

pub(crate) fn path_root_router(state: AppState) -> Router<AppState> {
    path_root_router_with_ecw_hosts(state, EcwHostConfig::from_env())
}

pub(super) fn dedicated_host_router_with_ecw_hosts(
    state: AppState,
    ecw_hosts: EcwHostConfig,
) -> Router<AppState> {
    let api_routes = protected_e2b_api_routes(state)
        .layer(Extension(ecw_hosts.clone()))
        .layer(middleware::from_fn_with_state(
            ecw_hosts.clone(),
            require_ecw_api_host,
        ));

    api_routes.layer(middleware::from_fn(log_ecw_request))
}

pub(super) fn path_root_router_with_ecw_hosts(
    state: AppState,
    ecw_hosts: EcwHostConfig,
) -> Router<AppState> {
    e2b_api_router(state, ecw_hosts)
}

fn e2b_api_router(state: AppState, ecw_hosts: EcwHostConfig) -> Router<AppState> {
    protected_e2b_api_routes(state)
        .layer(Extension(ecw_hosts.clone()))
        .layer(middleware::from_fn(log_ecw_request))
}

fn protected_e2b_api_routes(state: AppState) -> Router<AppState> {
    authenticated_e2b_api_routes()
        .layer(middleware::from_fn_with_state(
            state.clone(),
            crate::middleware::auth::require_e2b_api_key,
        ))
        .merge(file_upload_routes().layer(middleware::from_fn_with_state(
            state,
            require_file_upload_capability_or_api_key,
        )))
}

async fn require_file_upload_capability_or_api_key(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let capability_uri = request
        .extensions()
        .get::<OriginalUri>()
        .map_or_else(|| request.uri(), |original_uri| &original_uri.0);
    if file_upload_capability_org(capability_uri).is_some() {
        return next.run(request).await;
    }

    crate::middleware::auth::require_e2b_api_key(State(state), request, next).await
}

pub(super) async fn log_ecw_request(request: Request, next: Next) -> Response {
    let logged_path = ecw_logged_path(request.uri().path());
    let request_sandbox_id = ecw_api_sandbox_id_from_path(request.uri().path());
    let log_raw_caller_metadata = !is_sandbox_fork_path(request.uri().path());
    let client_address = log_raw_caller_metadata
        .then(|| {
            client_source_ip(
                request.headers(),
                request
                    .extensions()
                    .get::<ConnectInfo<SocketAddr>>()
                    .map(|connect_info| connect_info.0.ip()),
            )
            .map(|ip| ip.to_string())
        })
        .flatten();
    let user_agent = log_raw_caller_metadata
        .then(|| {
            request
                .headers()
                .get(header::USER_AGENT)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        })
        .flatten();
    // Informational X-Client-ID (e.g. `example-client`, `cleanup`) the caller sends.
    // The limiter's metrics carry it only when it's allowlisted (else
    // `client_id:unknown`), so the top-N 429-sources-by-triplet view reads an
    // unlisted ID from this log. A missing header logs the same `unspecified`
    // string as the limiter's `client_scope` bucket.
    let client_id = log_raw_caller_metadata.then(|| {
        request_client_id(request.headers())
            .unwrap_or_else(|| ClientScope::Unspecified.as_str().to_string())
    });

    let response = next.run(request).await;
    let response_sandbox_id = response
        .extensions()
        .get::<EcwRequestLogContext>()
        .and_then(|context| context.sandbox_id);
    let status = response.status();
    let sandbox_id = response_sandbox_id.or(request_sandbox_id);
    if let Some(sandbox_id) = sandbox_id {
        record_ecw_sandbox_span_tag(sandbox_id);
    }
    if !should_log_ecw_request(status) {
        return response;
    }

    let sandbox_id = sandbox_id.map(|id| id.to_string()).unwrap_or_default();
    // Auth runs inside this middleware, so caller identity arrives via the
    // response extension the e2b auth layer inserts on success; absent on auth
    // rejections.
    let caller = response.extensions().get::<EcwCallerIdentity>().copied();
    let api_key_id = caller.map(|identity| identity.api_key_id.to_string());
    let org_id = caller.map(|identity| identity.org_id.to_string());

    if status.is_server_error() {
        tracing::warn!(
            event_type = ECW_SANDBOX_API_LOG_EVENT,
            sandbox_id = %sandbox_id,
            http.response.status_code = status.as_u16(),
            url.path = logged_path.as_str(),
            client.address = client_address.as_deref(),
            client.id = client_id.as_deref(),
            user_agent.original = user_agent.as_deref(),
            api_key_id = api_key_id.as_deref(),
            org_id = org_id.as_deref(),
            "ECW sandbox API request complete",
        );
    } else {
        tracing::info!(
            event_type = ECW_SANDBOX_API_LOG_EVENT,
            sandbox_id = %sandbox_id,
            http.response.status_code = status.as_u16(),
            url.path = logged_path.as_str(),
            client.address = client_address.as_deref(),
            client.id = client_id.as_deref(),
            user_agent.original = user_agent.as_deref(),
            api_key_id = api_key_id.as_deref(),
            org_id = org_id.as_deref(),
            "ECW sandbox API request complete",
        );
    }
    response
}

fn is_sandbox_fork_path(path: &str) -> bool {
    let mut segments = path.trim_matches('/').split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next()
        ),
        (Some("sandboxes"), Some(_), Some("fork"), None)
    )
}

fn ecw_logged_path(path: &str) -> String {
    if is_sandbox_fork_path(path) {
        "/sandboxes/{sandboxID}/fork".to_string()
    } else {
        path.to_string()
    }
}

#[cfg(test)]
mod tests;

pub(super) fn should_log_ecw_request(status: StatusCode) -> bool {
    status.is_client_error() || status.is_server_error()
}

/// Client source IP for the ECW request log. The first `X-Forwarded-For`
/// hop wins over the TCP peer address: behind the edge LB the peer is the
/// LB itself and the first hop is the original client. The first hop is
/// attacker-controlled for direct callers, so this is attribution
/// telemetry, not an authorization signal.
pub(super) fn client_source_ip(headers: &HeaderMap, peer_ip: Option<IpAddr>) -> Option<IpAddr> {
    forwarded_client_ip(headers).or(peer_ip)
}

fn forwarded_client_ip(headers: &HeaderMap) -> Option<IpAddr> {
    let first_hop = headers
        .get("x-forwarded-for")?
        .to_str()
        .ok()?
        .split(',')
        .next()?
        .trim();
    // Hops are usually bare IPs, but some proxies append `ip:port`; a hop
    // that parses as neither falls through to the peer address.
    first_hop
        .parse::<IpAddr>()
        .ok()
        .or_else(|| first_hop.parse::<SocketAddr>().ok().map(|addr| addr.ip()))
}

fn ecw_api_sandbox_id_from_path(path: &str) -> Option<SandboxId> {
    let mut segments = path.trim_start_matches('/').split('/');
    match (segments.next(), segments.next()) {
        (Some("sandboxes"), Some(sandbox_id)) => sandbox_id.parse::<SandboxId>().ok(),
        _ => None,
    }
}

#[cfg(test)]
pub(super) fn e2b_api_routes() -> Router<AppState> {
    authenticated_e2b_api_routes().merge(file_upload_routes())
}

fn authenticated_e2b_api_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/sandboxes",
            axum::routing::get(list_sandboxes).post(create_sandbox),
        )
        .route(
            "/v2/sandboxes",
            axum::routing::get(list_sandboxes).post(create_sandbox),
        )
        // Declare the unsupported static collection endpoint before the
        // `{sandboxID}` route so Axum does not interpret "metrics" as an ID
        // and return the misleading "invalid sandbox ID" error.
        .route(
            "/sandboxes/metrics",
            axum::routing::get(unsupported_e2b_endpoint),
        )
        .route(
            "/sandboxes/{sandboxID}",
            axum::routing::get(get_sandbox).delete(delete_sandbox),
        )
        .route(
            "/sandboxes/{sandboxID}/pause",
            axum::routing::post(pause_sandbox_e2b),
        )
        .route(
            "/sandboxes/{sandboxID}/fork",
            axum::routing::post(fork_sandbox),
        )
        .route(
            "/sandboxes/{sandboxID}/resume",
            axum::routing::post(resume_sandbox_e2b),
        )
        .route(
            "/sandboxes/{sandboxID}/connect",
            axum::routing::post(connect_sandbox),
        )
        .route(
            "/v2/sandboxes/{sandboxID}/connect",
            axum::routing::post(connect_sandbox_v2),
        )
        .route(
            "/sandboxes/{sandboxID}/timeout",
            axum::routing::post(timeout_sandbox),
        )
        .route(
            "/sandboxes/{sandboxID}/refreshes",
            axum::routing::post(refresh_sandbox),
        )
        // ── Template build (e2b SDK compatible) ─────────────────────────────
        //
        // Mirror the URLs the e2b Python/JS SDK actually calls so existing
        // template builders work unmodified. Translation
        // and snapshot caveats live in routes/e2b/template_build/mod.rs.
        .route("/v3/templates", axum::routing::post(create_template_v3))
        .route(
            "/templates/aliases/{alias}",
            axum::routing::get(check_alias),
        )
        .route(
            "/templates/{templateID}/files/{filesHash}",
            axum::routing::get(get_file_upload),
        )
        .route(
            "/v2/templates/{templateID}/builds/{buildID}",
            axum::routing::post(start_build),
        )
        .route(
            "/templates/{templateID}/builds/{buildID}/status",
            axum::routing::get(get_build_status),
        )
        .method_not_allowed_fallback(unsupported_e2b_method)
        .fallback(unsupported_e2b_endpoint)
}

async fn unsupported_e2b_method() -> Response {
    super::E2bApiError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "This HTTP method is not supported for the requested E2B API endpoint",
    )
    .into_response()
}

async fn unsupported_e2b_endpoint() -> Response {
    super::E2bApiError::not_implemented("This E2B API endpoint is not supported by SPACE ECW")
        .into_response()
}

fn file_upload_routes() -> Router<AppState> {
    Router::new().route(
        "/v1/internal/e2b-files/{templateID}/{filesHash}",
        axum::routing::put(put_uploaded_file),
    )
}
