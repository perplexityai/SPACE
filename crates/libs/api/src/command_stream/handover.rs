//! The route handover the API attaches to its in-cluster request for a
//! session it just created, so the data plane can open that session's stream
//! without resolving the route again.
//!
//! The handover carries what route resolution would return for this attach,
//! and is minted only after the same checks route resolution makes: the
//! presented token matches the stored digest, is unexpired, and grants the
//! kind's connect scope; the session is active and has never bound a process;
//! and the sandbox is placed on a node with a node-API endpoint. Anything
//! else sends no handover, and the data plane resolves the route itself.

use pplx_space_common_core::types::{
    ExecutionGeneration, NodeId, NodeManagedBy, OrgId, PodUid, RouteGeneration,
    SandboxActivationAttemptId, SandboxId, SandboxRouteIdentity,
};
use pplx_space_common_svc::space_api_route_ed25519::{
    DataPlaneRouteHandoverRoute, SpaceApiRouteEd25519Authority,
};
use pplx_space_common_svc::sql;
use pplx_space_public_api::SessionKind;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::sync::Arc;
use subtle::ConstantTimeEq as _;

/// Turns route handovers on for the in-cluster command stream. Enable it only
/// where the data plane's internal URL reaches its internal listener;
/// elsewhere the data plane ignores handovers and the route read is wasted.
pub const DATA_PLANE_ROUTE_HANDOVER_ENV: &str = "SPACE_DATA_PLANE_ROUTE_HANDOVER";
const BACKEND_TRANSPORT_GRPC: &str = "grpc";

#[derive(Debug, thiserror::Error)]
pub(crate) enum HandoverError {
    #[error("read the session route: {0}")]
    Read(#[from] sqlx::Error),
    #[error("session not found")]
    NotFound,
    #[error("session cannot take a handover: {0}")]
    Ineligible(&'static str),
    #[error("route identity is incomplete: {0}")]
    Identity(String),
    #[error("mint the route token: {0}")]
    RouteToken(#[from] pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Error),
    #[error("mint the handover: {0}")]
    Handover(#[from] pplx_space_common_svc::space_api_route_ed25519::DataPlaneRouteHandoverError),
}

#[derive(Clone)]
pub(crate) struct RouteHandoverIssuer {
    pool: PgPool,
    authority: Arc<SpaceApiRouteEd25519Authority>,
}

#[derive(sqlx::FromRow)]
struct HandoverRouteRow {
    org_id: OrgId,
    sandbox_id: SandboxId,
    kind: String,
    adapter_state_json: String,
    route_generation: i64,
    current_node_id: Option<NodeId>,
    backend_endpoint: Option<String>,
    backend_transport: Option<String>,
    revocation_epoch: i64,
    revocation_status: String,
    org_revocation_epoch: i64,
    scopes: Vec<String>,
    token_hash: Option<String>,
    token_unexpired: bool,
    backend_process_id: Option<String>,
    sandbox_active_node_id: Option<NodeId>,
    node_address: Option<String>,
    node_managed_by: Option<NodeManagedBy>,
    direct_node_api_endpoint: Option<String>,
    sandbox_pod_uid: Option<PodUid>,
    execution_generation: Option<i64>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
}

impl RouteHandoverIssuer {
    pub(crate) fn new(pool: PgPool, authority: Arc<SpaceApiRouteEd25519Authority>) -> Self {
        Self { pool, authority }
    }

    /// Mints the handover for attaching `session_id` with `token`.
    pub(crate) async fn mint(
        &self,
        session_id: &str,
        token: &str,
    ) -> Result<String, HandoverError> {
        // The same joins route resolution reads, so both see one placement.
        let row = sqlx::query_as::<_, HandoverRouteRow>(sql!(
            "SELECT d.org_id,
                    d.sandbox_id,
                    d.kind,
                    d.adapter_state::TEXT AS adapter_state_json,
                    d.route_generation,
                    d.current_node_id,
                    d.backend_endpoint,
                    d.backend_transport,
                    d.revocation_epoch,
                    d.revocation_status,
                    COALESCE(o.epoch, 0) AS org_revocation_epoch,
                    d.scopes,
                    d.token_hash,
                    COALESCE(d.token_expires_at > NOW(), FALSE) AS token_unexpired,
                    d.backend_process_id,
                    COALESCE(direct_route.current_node_id, s.active_node_id)
                        AS sandbox_active_node_id,
                    HOST(n.address) AS node_address,
                    COALESCE(direct_route.substrate_kind, n.managed_by) AS node_managed_by,
                    nd.node_api_endpoint AS direct_node_api_endpoint,
                    COALESCE(NULLIF(BTRIM(e.pod_uid), ''), direct_route.pod_uid)
                        AS sandbox_pod_uid,
                    COALESCE(e.generation, direct_route.execution_generation)
                        AS execution_generation,
                    COALESCE(e.route_activation_id, direct_route.activation_attempt_id)
                        AS activation_attempt_id
             FROM data_plane_sessions d
             JOIN sandboxes s ON s.id = d.sandbox_id
             LEFT JOIN data_plane_org_revocation_epochs o ON o.org_id = d.org_id
             LEFT JOIN sandbox_direct_route_identities direct_route
               ON direct_route.sandbox_id = s.id
             LEFT JOIN nodes n
               ON n.id = COALESCE(direct_route.current_node_id, s.active_node_id)
             LEFT JOIN node_direct nd ON nd.node_id = n.id AND nd.deleted_at IS NULL
             LEFT JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
             WHERE d.session_id = $1
               AND (d.expires_at IS NULL OR d.expires_at > NOW())"
        ))
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(HandoverError::NotFound)?;
        let route = handover_route(session_id, token, row)?;
        let route_token = self.authority.mint_space_api_bridge(route.identity)?;
        Ok(self
            .authority
            .mint_data_plane_route_handover(DataPlaneRouteHandoverRoute {
                space_api_route_token: route_token.secret().to_string(),
                ..route.handover
            })?)
    }
}

struct HandoverRoute {
    handover: DataPlaneRouteHandoverRoute,
    identity: SandboxRouteIdentity,
}

fn handover_route(
    session_id: &str,
    token: &str,
    row: HandoverRouteRow,
) -> Result<HandoverRoute, HandoverError> {
    let kind = row
        .kind
        .parse::<SessionKind>()
        .map_err(|_| HandoverError::Ineligible("unknown_kind"))?;
    if !matches!(
        kind,
        SessionKind::Terminal | SessionKind::Process | SessionKind::FileWatch
    ) {
        return Err(HandoverError::Ineligible("unsupported_kind"));
    }
    let token_sha256 = hex::encode(Sha256::digest(token.as_bytes()));
    let stored = row.token_hash.as_deref().map(str::trim).unwrap_or_default();
    if stored.is_empty() || stored.as_bytes().ct_eq(token_sha256.as_bytes()).unwrap_u8() != 1 {
        return Err(HandoverError::Ineligible("token_mismatch"));
    }
    if !row.token_unexpired {
        return Err(HandoverError::Ineligible("token_expired"));
    }
    if !row
        .scopes
        .iter()
        .any(|scope| scope == kind.connect_scope().as_str())
    {
        return Err(HandoverError::Ineligible("scope_missing"));
    }
    if row.revocation_status != "active" {
        return Err(HandoverError::Ineligible("revoked"));
    }
    if row.backend_process_id.is_some() {
        return Err(HandoverError::Ineligible("process_bound"));
    }
    let current_node_id = row
        .sandbox_active_node_id
        .ok_or(HandoverError::Ineligible("unplaced"))?;
    let backend_endpoint = pplx_space_common_grpc::internal_transport::node_api_endpoint(
        row.direct_node_api_endpoint.as_deref(),
        row.node_address.as_deref(),
    )
    .ok_or(HandoverError::Ineligible("no_endpoint"))?;
    let (Some(substrate_kind), Some(execution_generation), Some(activation_attempt_id)) = (
        row.node_managed_by,
        row.execution_generation,
        row.activation_attempt_id,
    ) else {
        return Err(HandoverError::Identity(
            "route identity is incomplete".to_string(),
        ));
    };
    let execution_generation = ExecutionGeneration::new(execution_generation)
        .map_err(|error| HandoverError::Identity(error.to_string()))?;
    let identity = SandboxRouteIdentity::try_new(
        row.sandbox_id,
        u64::try_from(execution_generation.get())
            .ok()
            .and_then(|value| RouteGeneration::new(value).ok())
            .ok_or_else(|| HandoverError::Identity("execution generation".to_string()))?,
        current_node_id,
        substrate_kind,
        execution_generation,
        activation_attempt_id,
        row.sandbox_pod_uid,
    )
    .map_err(|error| HandoverError::Identity(error.to_string()))?;

    // Route resolution stores this route on the session and bumps its
    // generation when it differs from the stored one. The data plane compares
    // generations when it later refreshes the route, so hand over the one
    // that resolution will store.
    let stored_generation =
        u64::try_from(row.route_generation).map_err(|_| HandoverError::Ineligible("generation"))?;
    let backend_transport = row
        .backend_transport
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(BACKEND_TRANSPORT_GRPC);
    let unchanged = row.current_node_id == Some(current_node_id)
        && row.backend_endpoint.as_deref() == Some(backend_endpoint.as_str())
        && row.backend_transport.as_deref() == Some(backend_transport);
    let route_generation = if unchanged {
        stored_generation
    } else {
        stored_generation + 1
    };

    Ok(HandoverRoute {
        handover: DataPlaneRouteHandoverRoute {
            session_id: session_id.to_string(),
            token_sha256,
            org_id: row.org_id,
            sandbox_id: row.sandbox_id,
            kind: kind.as_str().to_string(),
            route_generation,
            current_node_id,
            backend_endpoint,
            execution_generation,
            revocation_epoch: u64::try_from(row.revocation_epoch)
                .map_err(|_| HandoverError::Ineligible("revocation_epoch"))?,
            org_revocation_epoch: u64::try_from(row.org_revocation_epoch)
                .map_err(|_| HandoverError::Ineligible("org_revocation_epoch"))?,
            adapter_state_json: row.adapter_state_json,
            space_api_route_token: String::new(),
        },
        identity,
    })
}

#[cfg(test)]
mod tests;
