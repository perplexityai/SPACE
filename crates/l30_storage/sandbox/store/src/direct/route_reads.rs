//! Reads of where a sandbox routes: the authoritative route identity a
//! runtime grant is signed for, and the row the ECW service route resolves
//! from. Each prefers the Direct route identity
//! (`sandbox_direct_route_identities`) over the row's own placement and
//! active execution where it has one.

use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_k8s_ids::PodUid;
use pplx_space_node_model::NodeManagedBy;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{
    EcwCredentialEpoch, ExecutionGeneration, SandboxActivationAttemptId, SandboxId,
};
use pplx_space_sql_locator::sql;

#[cfg(test)]
mod tests;

/// The route identity [`authoritative_route_identity`] reads.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AuthoritativeRouteIdentityRow {
    pub active_node_id: Option<NodeId>,
    pub active_node_managed_by: Option<NodeManagedBy>,
    pub active_execution_generation: Option<i64>,
    pub active_pod_uid: Option<PodUid>,
    pub active_activation_attempt_id: Option<SandboxActivationAttemptId>,
}

/// The sandbox's current route identity: the Direct route identity's node
/// and substrate, else the row's active node, and the active Kubernetes
/// execution's generation, pod and activation, else the Direct route
/// identity's. `None` when the row is gone.
pub async fn authoritative_route_identity(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<AuthoritativeRouteIdentityRow>, DbError> {
    Ok(sqlx::query_as::<_, AuthoritativeRouteIdentityRow>(sql!(
        "SELECT COALESCE(direct_route.current_node_id, s.active_node_id) AS active_node_id,
                COALESCE(direct_route.substrate_kind, active_node.managed_by)
                    AS active_node_managed_by,
                COALESCE(active_execution.generation, direct_route.execution_generation)
                    AS active_execution_generation,
                COALESCE(active_execution.pod_uid, direct_route.pod_uid) AS active_pod_uid,
                COALESCE(
                    active_execution.route_activation_id,
                    direct_route.activation_attempt_id
                ) AS active_activation_attempt_id
         FROM sandboxes s
         LEFT JOIN sandbox_direct_route_identities direct_route
             ON direct_route.sandbox_id = s.id
         LEFT JOIN nodes active_node
             ON active_node.id = COALESCE(direct_route.current_node_id, s.active_node_id)
         LEFT JOIN LATERAL (
             SELECT e.generation,
                    NULLIF(BTRIM(e.pod_uid), '') AS pod_uid,
                    e.route_activation_id
             FROM sandbox_k8s_executions e
             WHERE e.sandbox_id = s.id
               AND e.active
             ORDER BY e.generation DESC
             LIMIT 1
         ) active_execution ON TRUE
         WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The row [`service_route_row`] reads.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SandboxServiceRouteRow {
    pub sandbox_id: SandboxId,
    pub org_id: OrgId,
    pub template_id: Option<String>,
    pub lifecycle: String,
    pub active_node_id: Option<NodeId>,
    pub node_address: Option<String>,
    pub node_managed_by: Option<NodeManagedBy>,
    pub k8s_node_name: Option<String>,
    pub direct_node_api_endpoint: Option<String>,
    pub sandbox_pod_uid: Option<PodUid>,
    pub execution_generation: Option<ExecutionGeneration>,
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub ecw_credential_epoch: EcwCredentialEpoch,
    pub command_ready_pending: bool,
}

/// What the ECW service route resolves a sandbox from: its placement, the
/// node's address and registered Direct endpoint, the live execution's
/// identity and the ECW credential epoch. `None` when the row is gone.
///
/// The Direct route identity is joined only while `s.active_node_id` is set,
/// as internal-api-server's other route reads join it: a worker's own
/// suspend clears `active_node_id` but leaves the identity naming that
/// worker, where the sandbox no longer runs.
pub async fn service_route_row(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<SandboxServiceRouteRow>, DbError> {
    Ok(sqlx::query_as::<_, SandboxServiceRouteRow>(sql!(
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
          AND s.active_node_id IS NOT NULL
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
    .fetch_optional(executor.into_sqlx())
    .await?)
}
