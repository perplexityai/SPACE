//! Opening a session's stream on the route the API handed over, instead of
//! resolving it again, when the API itself relays the stream of a session it
//! just created.
//!
//! A handover replaces route resolution only when all of these hold:
//! - the request arrived on the internal listener, which public ingress never
//!   reaches;
//! - the handover verifies against the route verification keys, is unexpired,
//!   and names this cluster;
//! - it names this session and its kind, and was minted for the SHA-256 of the
//!   attach token on this request;
//! - its node route token, verified with the same keys, names the handover's
//!   sandbox, node, and execution generation.
//!
//! Any other request resolves the route as before. Revocation is unchanged:
//! the stream remembers the epochs the handover carries and checks them
//! periodically. A route that went stale between the handover and the open
//! (a wake, restore, or migration) is refused by the node, and the stream
//! refreshes it as it would any rejected route.

use super::*;
use pplx_space_common_svc::space_api_route_ed25519::{
    DATA_PLANE_ROUTE_HANDOVER_HEADER, DataPlaneRouteHandoverRoute, SpaceApiRouteEd25519Token,
};
use pplx_space_node_api_client::proto::SpaceApiRouteToken as ProtoSpaceApiRouteToken;
use subtle::ConstantTimeEq as _;

/// Marks requests that arrived on the internal listener.
#[derive(Clone, Copy, Debug)]
pub(crate) struct InternalListener;

/// The handover a request carries, and whether the listener it arrived on may
/// honor one.
pub(crate) struct OfferedRouteHandover {
    handover: String,
    internal_listener: bool,
}

impl OfferedRouteHandover {
    pub(crate) fn from_request(
        headers: &HeaderMap,
        internal_listener: Option<InternalListener>,
    ) -> Option<Self> {
        let handover = headers.get(DATA_PLANE_ROUTE_HANDOVER_HEADER)?;
        Some(Self {
            handover: handover.to_str().unwrap_or_default().to_string(),
            internal_listener: internal_listener.is_some(),
        })
    }
}

/// Resolves the session's route, or takes it from a valid handover.
pub(super) async fn resolve_session_route_with_handover(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
    token: &str,
    handover: Option<OfferedRouteHandover>,
) -> Result<AttachedSession, RouteResolutionError> {
    if let Some(handover) = handover {
        let kind = session_id.kind().as_str();
        match accept_route_handover(state, session_id, token, &handover) {
            Ok(attached) => {
                pplx_space_metrics::data_plane::route_handover(kind, "accepted");
                state.revocation_cache.remember_active(
                    session_id,
                    attached.revocation_epoch,
                    attached.org_id,
                    attached.org_revocation_epoch,
                    start_revocation_read(state),
                );
                return Ok(attached);
            }
            Err(reason) => {
                pplx_space_metrics::data_plane::route_handover(kind, reason);
                tracing::debug!(
                    session_id = %session_id,
                    reason,
                    "route handover refused; resolving the session route"
                );
            }
        }
    }
    resolve_session_route(state, session_id, token).await
}

pub(super) fn accept_route_handover(
    state: &DataPlaneState,
    session_id: DataPlaneSessionId,
    token: &str,
    offered: &OfferedRouteHandover,
) -> Result<AttachedSession, &'static str> {
    if !offered.internal_listener {
        return Err("public_listener");
    }
    let verifier = state.route_handover.as_deref().ok_or("not_configured")?;
    if !matches!(
        session_id.kind(),
        SessionKind::Terminal | SessionKind::Process | SessionKind::FileWatch
    ) {
        return Err("unsupported_kind");
    }
    let route = verifier
        .verify_data_plane_route_handover(&offered.handover)
        .map_err(|error| error.label())?;
    let DataPlaneRouteHandoverRoute {
        session_id: handed_session_id,
        token_sha256: handed_token_sha256,
        org_id,
        sandbox_id,
        kind,
        route_generation,
        current_node_id,
        backend_endpoint,
        execution_generation,
        revocation_epoch,
        org_revocation_epoch,
        adapter_state_json,
        space_api_route_token,
    } = route;
    if handed_session_id != session_id.to_string() {
        return Err("wrong_session");
    }
    if handed_token_sha256
        .as_bytes()
        .ct_eq(token_sha256(token).as_bytes())
        .unwrap_u8()
        != 1
    {
        return Err("wrong_token");
    }
    if kind.parse::<SessionKind>().ok() != Some(session_id.kind()) {
        return Err("wrong_kind");
    }

    let route_token = SpaceApiRouteEd25519Token::parse(space_api_route_token.clone())
        .map_err(|_| "invalid_route_token")?;
    let route_claims = verifier
        .verify(&route_token)
        .map_err(|_| "invalid_route_token")?;
    let identity = &route_claims.route_identity;
    if identity.sandbox_id() != sandbox_id
        || identity.current_node_id() != current_node_id
        || identity.execution_generation() != execution_generation
    {
        return Err("route_token_mismatch");
    }
    let space_api_route_token = parse_proto_space_api_route_token(
        SpaceApiRouteTokenRouteSource::DataPlaneSession,
        ProtoSpaceApiRouteToken {
            token: space_api_route_token,
        },
    )
    .ok_or("invalid_route_token")?;

    Ok(AttachedSession {
        cache_epoch: state.route_cache.next_cache_epoch(),
        cache_key: None,
        session_id,
        sandbox_id,
        org_id,
        kind: session_id.kind(),
        route_generation,
        revocation_epoch,
        org_revocation_epoch,
        current_node_id,
        backend_endpoint: backend_endpoint
            .parse()
            .map_err(|_| "invalid_backend_endpoint")?,
        target_port: None,
        target_protocol: None,
        adapter_state_json,
        // A handover is minted only for a session that has never bound a
        // process; any later attach resolves the bound one.
        backend_process_id: None,
        space_api_route_token: Some(space_api_route_token),
    })
}

#[cfg(test)]
mod tests;
