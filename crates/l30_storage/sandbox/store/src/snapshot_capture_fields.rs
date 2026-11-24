//! The capture-time fields a `snapshots` row inherits from its source
//! sandbox.

use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;

use crate::SandboxStoreError;

/// Capture-time fields the `snapshots` row inherits from the source sandbox.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct SnapshotSandboxRow {
    pub org_id: OrgId,
    pub active_node_id: Option<NodeId>,
    pub target_node_id: Option<NodeId>,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub template_id: Option<String>,
    pub content_hash: Option<String>,
}

/// The capture-time fields of `sandbox_id`; `None` when no row exists.
pub async fn get(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<SnapshotSandboxRow>, SandboxStoreError> {
    Ok(sqlx::query_as::<_, SnapshotSandboxRow>(sql!(
        // A primary-key fetch is sufficient: `snapshot_execution_metadata`
        // requires caller-supplied execution metadata, so no template or
        // runtime-guest lookup is needed here.
        "SELECT org_id, active_node_id, target_node_id, vcpu, memory_mib,
                    template_id, content_hash
             FROM sandboxes
             WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await?)
}
