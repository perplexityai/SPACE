//! The registered Direct worker a sandbox row names, with its node API
//! endpoint (`node_direct`). Each verb answers for one of the row's node
//! columns. "Registered" means a Direct worker with a live `node_direct`
//! row, whatever its `nodes.state`; any other node answers `None`.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;

#[cfg(test)]
mod tests;

/// A registered Direct worker and its node API endpoint.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct DirectNodeEndpoint {
    pub id: NodeId,
    pub node_api_endpoint: String,
}

/// The worker named by the row's `active_node_id`.
pub async fn active_node<'e>(
    executor: impl PgExecutor<'e>,
    sandbox_id: SandboxId,
) -> Result<Option<DirectNodeEndpoint>, sqlx::Error> {
    sqlx::query_as::<_, DirectNodeEndpoint>(sql!(
        "SELECT n.id, nd.node_api_endpoint
             FROM sandboxes s
             JOIN nodes n ON n.id = s.active_node_id
             JOIN node_direct nd ON nd.node_id = n.id
             WHERE s.id = $1
               AND n.managed_by = $2
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
             LIMIT 1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(SubstrateKind::Space)
    .fetch_optional(executor)
    .await
}

/// The worker holding the sandbox's node-local state: its `active_node_id`,
/// else its `target_node_id`.
pub async fn local_state_node<'e>(
    executor: impl PgExecutor<'e>,
    sandbox_id: SandboxId,
) -> Result<Option<DirectNodeEndpoint>, sqlx::Error> {
    sqlx::query_as::<_, DirectNodeEndpoint>(sql!(
        "SELECT n.id, nd.node_api_endpoint
             FROM sandboxes s
             JOIN nodes n ON n.id = COALESCE(s.active_node_id, s.target_node_id)
             JOIN node_direct nd ON nd.node_id = n.id
             WHERE s.id = $1
               AND n.managed_by = $2
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
             LIMIT 1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(SubstrateKind::Space)
    .fetch_optional(executor)
    .await
}

/// The worker holding the sandbox's artifact: its `target_node_id`, else its
/// `active_node_id`.
pub async fn provenance_source_node<'e>(
    executor: impl PgExecutor<'e>,
    sandbox_id: SandboxId,
) -> Result<Option<DirectNodeEndpoint>, sqlx::Error> {
    sqlx::query_as::<_, DirectNodeEndpoint>(sql!(
        "SELECT n.id, nd.node_api_endpoint
             FROM sandboxes s
             JOIN nodes n ON n.id = COALESCE(s.target_node_id, s.active_node_id)
             JOIN node_direct nd ON nd.node_id = n.id
             WHERE s.id = $1
               AND n.managed_by = $2
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
             LIMIT 1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(SubstrateKind::Space)
    .fetch_optional(executor)
    .await
}
