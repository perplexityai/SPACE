//! Reads and writes of the stored route rows, and the backend endpoint a route resolves to.

use super::*;

pub(super) const DEFAULT_NODE_API_PORT: u16 = 9800;

pub(super) async fn fetch_route_row(
    pool: &sqlx::PgPool,
    session_id: &str,
    protocol_session_id: Option<&str>,
) -> Result<SessionRouteRow, Status> {
    sqlx::query_as::<_, SessionRouteRow>(sql!(
        "SELECT d.session_id,
                d.protocol_session_id,
                d.org_id,
                d.sandbox_id,
                d.kind,
                d.target_port,
                d.target_protocol,
                d.target_path,
                d.adapter_state::TEXT AS adapter_state_json,
                d.route_generation,
                d.current_node_id,
                d.backend_endpoint,
                d.backend_transport,
                d.revocation_epoch,
                d.revocation_status,
                d.revocation_reason,
                d.revocation_updated_at,
                COALESCE(o.epoch, 0) AS org_revocation_epoch,
                d.scopes,
                d.token_hash,
                d.token_expires_at,
                s.lifecycle AS sandbox_lifecycle,
                COALESCE(direct_route.current_node_id, s.active_node_id)
                    AS sandbox_active_node_id,
                HOST(n.address) AS node_address,
                COALESCE(direct_route.substrate_kind, n.managed_by) AS node_managed_by,
                nk.k8s_node_name AS k8s_node_name,
                nd.node_api_endpoint AS direct_node_api_endpoint,
                COALESCE(NULLIF(BTRIM(e.pod_uid), ''), direct_route.pod_uid)
                    AS sandbox_pod_uid,
                COALESCE(e.generation, direct_route.execution_generation)
                    AS execution_generation,
                COALESCE(e.route_activation_id, direct_route.activation_attempt_id)
                    AS activation_attempt_id,
                d.backend_process_id
         FROM data_plane_sessions d
         JOIN sandboxes s ON s.id = d.sandbox_id
         LEFT JOIN data_plane_org_revocation_epochs o ON o.org_id = d.org_id
         LEFT JOIN sandbox_direct_route_identities direct_route
           ON direct_route.sandbox_id = s.id
         LEFT JOIN nodes n
           ON n.id = COALESCE(direct_route.current_node_id, s.active_node_id)
         LEFT JOIN node_k8s nk ON nk.node_id = n.id
         LEFT JOIN node_direct nd ON nd.node_id = n.id AND nd.deleted_at IS NULL
         LEFT JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
         WHERE d.session_id = $1
           AND ($2::TEXT IS NULL OR d.protocol_session_id = $2)
           AND (d.expires_at IS NULL OR d.expires_at > NOW())"
    ))
    .bind(session_id)
    .bind(protocol_session_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("resolve data-plane session route: {error}")))?
    .ok_or_else(|| Status::not_found("data-plane session not found"))
}

/// Inputs that let one statement persist the session's current route while it
/// reads it, saving the separate [`update_route_row`] round trip that every
/// first attach otherwise pays.
pub(super) struct RoutePersistence<'a> {
    /// Only an attach that presents the session's live token, with the scope
    /// it asks for, may write the route. Anything else just reads.
    pub(super) token_sha256: &'a str,
    pub(super) required_scope: &'a str,
}

/// Like [`fetch_route_row`], but also stores the route it resolves when it
/// differs from the stored one, in the same statement.
///
/// The SQL derives the backend endpoint the way [`node_api_backend_endpoint`]
/// does, but that function stays authoritative: the caller compares the row
/// with its own resolution as before, and any difference (or a session this
/// statement did not write) takes the existing [`update_route_row`] path. So a
/// formatting difference costs the old round trip, never a wrong route.
///
/// Only terminal, process and file-watch sessions take this path; port and
/// protocol sessions must pass their ingress checks before anything is
/// written, so they keep reading first.
pub(super) async fn fetch_and_persist_route_row(
    pool: &sqlx::PgPool,
    session_id: &str,
    protocol_session_id: Option<&str>,
    persistence: RoutePersistence<'_>,
) -> Result<SessionRouteRow, Status> {
    let kinds = [
        SessionKind::Terminal,
        SessionKind::Process,
        SessionKind::FileWatch,
    ]
    .map(|kind| kind.as_str().to_string());
    sqlx::query_as::<_, SessionRouteRow>(sql!(
        "WITH current_route AS (
             SELECT d.session_id,
                    d.protocol_session_id,
                    d.org_id,
                    d.sandbox_id,
                    d.kind,
                    d.target_port,
                    d.target_protocol,
                    d.target_path,
                    d.adapter_state::TEXT AS adapter_state_json,
                    d.route_generation,
                    d.current_node_id,
                    d.backend_endpoint,
                    d.backend_transport,
                    d.revocation_epoch,
                    d.revocation_status,
                    d.revocation_reason,
                    d.revocation_updated_at,
                    COALESCE(o.epoch, 0) AS org_revocation_epoch,
                    d.scopes,
                    d.token_hash,
                    d.token_expires_at,
                    s.lifecycle AS sandbox_lifecycle,
                    COALESCE(direct_route.current_node_id, s.active_node_id)
                        AS sandbox_active_node_id,
                    HOST(n.address) AS node_address,
                    COALESCE(direct_route.substrate_kind, n.managed_by) AS node_managed_by,
                    nk.k8s_node_name AS k8s_node_name,
                    nd.node_api_endpoint AS direct_node_api_endpoint,
                    COALESCE(NULLIF(BTRIM(e.pod_uid), ''), direct_route.pod_uid)
                        AS sandbox_pod_uid,
                    COALESCE(e.generation, direct_route.execution_generation)
                        AS execution_generation,
                    COALESCE(e.route_activation_id, direct_route.activation_attempt_id)
                        AS activation_attempt_id,
                    d.backend_process_id,
                    CASE
                        WHEN BTRIM(nd.node_api_endpoint) <> '' THEN nd.node_api_endpoint
                        WHEN n.address IS NOT NULL THEN
                            $5::TEXT
                            || CASE
                                   WHEN family(n.address) = 6
                                   THEN '[' || HOST(n.address) || ']'
                                   ELSE HOST(n.address)
                               END
                            || ':' || $6::INT::TEXT
                    END AS resolved_backend_endpoint,
                    CASE
                        WHEN BTRIM(d.backend_transport) <> '' THEN d.backend_transport
                        ELSE $7::TEXT
                    END AS resolved_backend_transport
             FROM data_plane_sessions d
             JOIN sandboxes s ON s.id = d.sandbox_id
             LEFT JOIN data_plane_org_revocation_epochs o ON o.org_id = d.org_id
             LEFT JOIN sandbox_direct_route_identities direct_route
               ON direct_route.sandbox_id = s.id
             LEFT JOIN nodes n
               ON n.id = COALESCE(direct_route.current_node_id, s.active_node_id)
             LEFT JOIN node_k8s nk ON nk.node_id = n.id
             LEFT JOIN node_direct nd ON nd.node_id = n.id AND nd.deleted_at IS NULL
             LEFT JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
             WHERE d.session_id = $1
               AND ($2::TEXT IS NULL OR d.protocol_session_id = $2)
               AND (d.expires_at IS NULL OR d.expires_at > NOW())
         ),
         persisted AS (
             UPDATE data_plane_sessions d
             SET current_node_id = c.sandbox_active_node_id,
                 backend_endpoint = c.resolved_backend_endpoint,
                 backend_transport = c.resolved_backend_transport,
                 route_generation = d.route_generation + 1,
                 updated_at = NOW()
             FROM current_route c
             WHERE d.session_id = c.session_id
               AND c.kind = ANY($8::TEXT[])
               AND c.token_hash = $3::TEXT
               AND c.token_expires_at > NOW()
               AND $4::TEXT = ANY(c.scopes)
               AND c.sandbox_active_node_id IS NOT NULL
               AND c.resolved_backend_endpoint IS NOT NULL
               AND (d.current_node_id IS DISTINCT FROM c.sandbox_active_node_id
                    OR d.backend_endpoint IS DISTINCT FROM c.resolved_backend_endpoint
                    OR d.backend_transport IS DISTINCT FROM c.resolved_backend_transport)
             RETURNING d.route_generation,
                       d.current_node_id,
                       d.backend_endpoint,
                       d.backend_transport
         )
         SELECT c.session_id,
                c.protocol_session_id,
                c.org_id,
                c.sandbox_id,
                c.kind,
                c.target_port,
                c.target_protocol,
                c.target_path,
                c.adapter_state_json,
                COALESCE(p.route_generation, c.route_generation) AS route_generation,
                COALESCE(p.current_node_id, c.current_node_id) AS current_node_id,
                COALESCE(p.backend_endpoint, c.backend_endpoint) AS backend_endpoint,
                COALESCE(p.backend_transport, c.backend_transport) AS backend_transport,
                c.revocation_epoch,
                c.revocation_status,
                c.revocation_reason,
                c.revocation_updated_at,
                c.org_revocation_epoch,
                c.scopes,
                c.token_hash,
                c.token_expires_at,
                c.sandbox_lifecycle,
                c.sandbox_active_node_id,
                c.node_address,
                c.node_managed_by,
                c.k8s_node_name,
                c.direct_node_api_endpoint,
                c.sandbox_pod_uid,
                c.execution_generation,
                c.activation_attempt_id,
                c.backend_process_id
         FROM current_route c
         LEFT JOIN persisted p ON TRUE"
    ))
    .bind(session_id)
    .bind(protocol_session_id)
    .bind(persistence.token_sha256)
    .bind(persistence.required_scope)
    .bind(pplx_space_common_grpc::internal_transport::advertised_endpoint_prefix())
    .bind(i32::from(DEFAULT_NODE_API_PORT))
    .bind(BACKEND_TRANSPORT_GRPC)
    .bind(&kinds[..])
    .fetch_optional(pool)
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
) -> Result<SessionRouteRow, Status> {
    sqlx::query_as::<_, SessionRouteRow>(sql!(
        "UPDATE data_plane_sessions d
         SET current_node_id = $2,
             backend_endpoint = $3,
             backend_transport = $4,
             route_generation = CASE
                 WHEN d.current_node_id IS DISTINCT FROM $2
                   OR d.backend_endpoint IS DISTINCT FROM $3
                   OR d.backend_transport IS DISTINCT FROM $4
                 THEN d.route_generation + 1
                 ELSE d.route_generation
             END,
             updated_at = NOW()
         FROM sandboxes s
         LEFT JOIN data_plane_org_revocation_epochs o ON o.org_id = s.org_id
         LEFT JOIN sandbox_direct_route_identities direct_route
           ON direct_route.sandbox_id = s.id
         LEFT JOIN nodes n
           ON n.id = COALESCE(direct_route.current_node_id, s.active_node_id)
         LEFT JOIN node_k8s nk ON nk.node_id = n.id
         LEFT JOIN node_direct nd ON nd.node_id = n.id AND nd.deleted_at IS NULL
         LEFT JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
         WHERE d.session_id = $1
           AND s.id = d.sandbox_id
         RETURNING d.session_id,
                   d.protocol_session_id,
                   d.org_id,
                   d.sandbox_id,
                   d.kind,
                   d.target_port,
                   d.target_protocol,
                   d.target_path,
                   d.adapter_state::TEXT AS adapter_state_json,
                   d.route_generation,
                   d.current_node_id,
                   d.backend_endpoint,
                   d.backend_transport,
                   d.revocation_epoch,
                   d.revocation_status,
                   d.revocation_reason,
                   d.revocation_updated_at,
                   COALESCE(o.epoch, 0) AS org_revocation_epoch,
                   d.scopes,
                   d.token_hash,
                   d.token_expires_at,
                   s.lifecycle AS sandbox_lifecycle,
                   COALESCE(direct_route.current_node_id, s.active_node_id)
                       AS sandbox_active_node_id,
                   HOST(n.address) AS node_address,
                   COALESCE(direct_route.substrate_kind, n.managed_by) AS node_managed_by,
                   nk.k8s_node_name AS k8s_node_name,
                   nd.node_api_endpoint AS direct_node_api_endpoint,
                   COALESCE(NULLIF(BTRIM(e.pod_uid), ''), direct_route.pod_uid)
                       AS sandbox_pod_uid,
                   COALESCE(e.generation, direct_route.execution_generation)
                       AS execution_generation,
                   COALESCE(e.route_activation_id, direct_route.activation_attempt_id)
                       AS activation_attempt_id,
                   d.backend_process_id"
    ))
    .bind(session_id)
    .bind(current_node_id)
    .bind(backend_endpoint)
    .bind(backend_transport)
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("update data-plane session route: {error}")))?
    .ok_or_else(|| Status::not_found("data-plane session not found"))
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

pub(super) fn resolve_backend_endpoint(row: &SessionRouteRow) -> Option<String> {
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
    sqlx::query_as::<_, SandboxServiceRouteRow>(sql!(
        "SELECT s.id AS sandbox_id,
                s.org_id,
                s.template_id,
                s.lifecycle,
                COALESCE(direct_route.current_node_id, s.active_node_id) AS active_node_id,
                HOST(n.address) AS node_address,
                COALESCE(direct_route.substrate_kind, n.managed_by) AS node_managed_by,
                nk.k8s_node_name AS k8s_node_name,
                nd.node_api_endpoint AS direct_node_api_endpoint,
                COALESCE(NULLIF(BTRIM(e.pod_uid), ''), direct_route.pod_uid)
                    AS sandbox_pod_uid,
                COALESCE(e.generation, direct_route.execution_generation)
                    AS execution_generation,
                COALESCE(e.route_activation_id, direct_route.activation_attempt_id)
                    AS activation_attempt_id,
                COALESCE(ecw_credential.epoch, 1) AS ecw_credential_epoch,
                s.command_ready_pending
         FROM sandboxes s
         LEFT JOIN sandbox_direct_route_identities direct_route
           ON direct_route.sandbox_id = s.id
         LEFT JOIN nodes n
           ON n.id = COALESCE(direct_route.current_node_id, s.active_node_id)
         LEFT JOIN node_k8s nk ON nk.node_id = n.id
         LEFT JOIN node_direct nd ON nd.node_id = n.id AND nd.deleted_at IS NULL
         LEFT JOIN sandbox_k8s_executions e
           ON e.sandbox_id = s.id
          AND e.active
          AND e.terminated_at IS NULL
         LEFT JOIN sandbox_ecw_credential_epochs ecw_credential
           ON ecw_credential.sandbox_id = s.id
         WHERE s.id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_optional(pool)
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
