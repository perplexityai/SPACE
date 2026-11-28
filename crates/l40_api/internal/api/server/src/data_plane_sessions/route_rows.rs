//! Reads and writes of the stored route rows, and the backend endpoint a route resolves to.

use super::*;
use pplx_space_data_plane_session_store as data_plane_session_store;

pub(super) const DEFAULT_NODE_API_PORT: u16 = 9800;

// Each read here places the sandbox on
// `COALESCE(direct_route.current_node_id, s.active_node_id)`, and joins the
// Direct route identity only while `s.active_node_id` is set. A worker's own
// suspend (an idle suspend) clears `active_node_id` but leaves the identity
// naming that worker, where the sandbox no longer runs; routed by it, a
// reattach would never ask for the wake an unplaced row gets. The only live
// identity without `active_node_id` is a create's receipt before the create
// records its node, and that `creating` row is unplaced on Kubernetes too.

pub(super) async fn fetch_route_row(
    pool: &sqlx::PgPool,
    session_id: &str,
    protocol_session_id: Option<&str>,
) -> Result<SessionRoute, Status> {
    data_plane_session_store::direct::live_session_route(pool, session_id, protocol_session_id)
        .await
        .map_err(|error| Status::internal(format!("resolve data-plane session route: {error}")))?
        .ok_or_else(|| Status::not_found("data-plane session not found"))
}

pub(super) async fn update_route_row(
    pool: &sqlx::PgPool,
    session_id: &str,
    current_node_id: NodeId,
    backend_endpoint: &str,
    backend_transport: &str,
) -> Result<SessionRoute, Status> {
    // The update re-reads the sandbox, and is rolled back if it refuses
    // what it read, so a refused refresh changes no route.
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Status::internal(format!("update data-plane session route: {error}")))?;
    let row = data_plane_session_store::direct::repoint_session_route(
        &mut *tx,
        session_id,
        current_node_id,
        backend_endpoint,
        backend_transport,
    )
    .await
    .map_err(|error| Status::internal(format!("update data-plane session route: {error}")))?
    .ok_or_else(|| Status::not_found("data-plane session not found"))?;
    // A suspend and a restore may have committed since the caller's first
    // read, and the refreshed identity is then the restoring replacement's.
    // Dropping the transaction rolls the route change back.
    refuse_restoring_route(row.sandbox_lifecycle)?;
    tx.commit()
        .await
        .map_err(|error| Status::internal(format!("update data-plane session route: {error}")))?;
    Ok(row)
}

/// Explicit registered endpoints win; database INET host values need IPv6 brackets.
pub(super) fn node_api_backend_endpoint(
    direct: Option<&str>,
    address: Option<&str>,
) -> Option<String> {
    direct
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            let ip = address?.trim().parse::<std::net::IpAddr>().ok()?;
            let address = std::net::SocketAddr::new(ip, DEFAULT_NODE_API_PORT);
            Some(format!(
                "{}{address}",
                pplx_space_common_grpc::internal_transport::advertised_endpoint_prefix()
            ))
        })
}

pub(super) fn resolve_backend_endpoint(row: &SessionRoute) -> Option<String> {
    node_api_backend_endpoint(
        row.direct_node_api_endpoint.as_deref(),
        row.node_address.as_deref(),
    )
}

pub(super) fn missing_backend_endpoint_message(
    context: &str,
    active_node_id: Option<NodeId>,
    node_managed_by: Option<&str>,
    k8s_node_name: Option<&str>,
) -> String {
    let node_id = active_node_id
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string());
    let managed_by = node_managed_by
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("-");
    let k8s_node_name = k8s_node_name
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("-");
    format!(
        "{context} has no backend endpoint (active_node_id={node_id}, managed_by={managed_by}, k8s_node_name={k8s_node_name}; expected node_direct.node_api_endpoint or nodes.address)",
    )
}

pub(super) async fn fetch_sandbox_service_route_row(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<SandboxServiceRouteRow, Status> {
    sandbox_store::direct::route_reads::service_route_row(pool, sandbox_id)
        .await
        .map_err(|error| Status::internal(format!("resolve sandbox service route: {error}")))?
        .ok_or_else(|| Status::not_found("sandbox not found"))
}

pub(super) fn resolve_sandbox_service_backend_endpoint(
    row: &SandboxServiceRouteRow,
) -> Option<String> {
    node_api_backend_endpoint(
        row.direct_node_api_endpoint.as_deref(),
        row.node_address.as_deref(),
    )
}
