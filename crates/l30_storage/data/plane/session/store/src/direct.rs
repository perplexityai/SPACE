//! The data-plane session route: a session's stored route joined with where
//! its sandbox runs now, as the internal API resolves and refreshes it.
//!
//! Both statements place the sandbox on
//! `COALESCE(direct_route.current_node_id, s.active_node_id)` and join the
//! Direct route identity (`sandbox_direct_route_identities`) only while
//! `s.active_node_id` is set: a worker's own suspend clears `active_node_id`
//! but leaves the identity naming that worker, where the sandbox no longer
//! runs. They read the Direct node's registered endpoint from `node_direct`.
//! They join tables other stores own. The Direct ones (`node_direct`,
//! `sandbox_direct_route_identities`) are why they live in this `direct`
//! module: `pattern_ratchet.direct_tables` allows those names in a store's
//! `direct` module. The session row is the only one they write.

use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_k8s_ids::PodUid;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{
    ExecutionGeneration, RouteGeneration, SandboxActivationAttemptId, SandboxId, SandboxLifecycle,
};
use pplx_space_sql_locator::sql;
use sqlx::FromRow;
use time::OffsetDateTime;

#[cfg(test)]
mod tests;

/// A session's route, from `data_plane_sessions` joined with its sandbox, the
/// node it runs on, that node's Kubernetes and Direct registrations, the
/// sandbox's active Kubernetes execution or Direct route identity, and its
/// org's revocation epoch.
///
/// The vocabulary columns other than `sandbox_lifecycle` stay text so a value
/// written by a newer build still reads; the caller parses them.
/// `sandbox_lifecycle` decodes as [`SandboxLifecycle`], so a lifecycle this
/// build does not know makes [`live_session_route`] and
/// [`repoint_session_route`] return a decode error.
#[derive(Debug, Clone, FromRow)]
pub struct SessionRoute {
    pub session_id: String,
    pub protocol_session_id: Option<String>,
    pub org_id: OrgId,
    pub sandbox_id: SandboxId,
    pub kind: String,
    pub target_port: Option<i32>,
    pub target_protocol: Option<String>,
    pub target_path: Option<String>,
    pub adapter_state_json: String,
    pub route_generation: RouteGeneration,
    pub current_node_id: Option<NodeId>,
    pub backend_endpoint: Option<String>,
    pub backend_transport: Option<String>,
    pub revocation_epoch: i64,
    pub revocation_status: String,
    pub revocation_reason: Option<String>,
    pub revocation_updated_at: OffsetDateTime,
    pub org_revocation_epoch: i64,
    pub scopes: Vec<String>,
    pub token_hash: Option<String>,
    pub token_expires_at: Option<OffsetDateTime>,
    pub sandbox_active_node_id: Option<NodeId>,
    pub sandbox_lifecycle: SandboxLifecycle,
    pub node_address: Option<String>,
    pub node_managed_by: Option<String>,
    pub k8s_node_name: Option<String>,
    pub direct_node_api_endpoint: Option<String>,
    pub sandbox_pod_uid: Option<PodUid>,
    pub execution_generation: Option<ExecutionGeneration>,
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub backend_process_id: Option<String>,
}

/// The route of the session `session_id`; `None` when it is gone, has
/// expired by the database clock, or carries another protocol session id
/// than `protocol_session_id` (when one is given).
pub async fn live_session_route(
    executor: impl Executor<'_>,
    session_id: &str,
    protocol_session_id: Option<&str>,
) -> Result<Option<SessionRoute>, DbError> {
    Ok(sqlx::query_as::<_, SessionRoute>(sql!(
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
          AND s.active_node_id IS NOT NULL
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
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Point the session `session_id`'s route at `current_node_id`,
/// `backend_endpoint` and `backend_transport`, and return the route as the
/// update left it, its sandbox re-read in the same statement; `None` when the
/// session is gone.
///
/// The route generation advances only when one of the three changes. The
/// caller decides what to do with the sandbox it re-read, and rolls its
/// transaction back to refuse it.
pub async fn repoint_session_route(
    executor: impl Executor<'_>,
    session_id: &str,
    current_node_id: NodeId,
    backend_endpoint: &str,
    backend_transport: &str,
) -> Result<Option<SessionRoute>, DbError> {
    Ok(sqlx::query_as::<_, SessionRoute>(sql!(
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
          AND s.active_node_id IS NOT NULL
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
    .fetch_optional(executor.into_sqlx())
    .await?)
}
