//! The desired state a node reads back for the sandboxes it holds local state
//! for.

use pplx_space_accounts::NodeId;
use pplx_space_auto_lifecycle::AutoLifecyclePolicy;
use pplx_space_postgres_pool::Executor;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::types::Json;

use crate::SandboxStoreError;

/// One row per requested id that exists. The desired-state payload
/// columns are conditionally projected: populated only when the row is
/// assigned to the querying node, NULL otherwise — rows that exist but
/// live elsewhere are fetched solely for the exists/deleted partition,
/// and shipping their jsonb policy fleet-wide would multiply the result
/// bytes ~30x for data the handler discards.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SandboxDesiredStateRow {
    pub sandbox_id: SandboxId,
    pub assigned_here: bool,
    pub snapshots_enabled_desired: Option<bool>,
    pub auto_lifecycle_json: Option<Json<AutoLifecyclePolicy>>,
}

/// Every one of `sandbox_ids` that has a `sandboxes` row, whichever node it is
/// on, with its desired state when it is active on `node_id`. Deliberately
/// unfiltered by node, so one consistent read partitions the requested ids
/// into assigned-here, exists-elsewhere or unplaced, and absent (no row at
/// all).
pub async fn partition(
    executor: impl Executor<'_>,
    sandbox_ids: &[SandboxId],
    node_id: NodeId,
) -> Result<Vec<SandboxDesiredStateRow>, SandboxStoreError> {
    Ok(sqlx::query_as::<_, SandboxDesiredStateRow>(sql!(
        "SELECT id AS sandbox_id,
                    (active_node_id = $2) IS TRUE AS assigned_here,
                    CASE WHEN active_node_id = $2
                         THEN snapshots_enabled_desired END AS snapshots_enabled_desired,
                    CASE WHEN active_node_id = $2
                         THEN auto_lifecycle_json END AS auto_lifecycle_json
             FROM sandboxes
             WHERE id = ANY($1)"
    ))
    .bind(sandbox_ids)
    .bind(node_id)
    .fetch_all(executor.into_sqlx())
    .await?)
}
