//! Every catalog row of one sandbox as stored, for views that show what the
//! catalog holds even when a value would not parse.

use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;
use sqlx::FromRow;
use time::OffsetDateTime;

/// The columns of one `snapshots` row that debug views show, with text columns
/// such as `type` and `state` left unparsed.
#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct StoredSnapshot {
    pub id: SnapshotId,
    pub sandbox_id: SandboxId,
    pub r#type: String,
    pub state: String,
    pub timestamp: OffsetDateTime,
    pub source_node_id: Option<NodeId>,
    pub label: Option<String>,
    pub retention_tier: Option<String>,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub size_bytes: Option<i64>,
    pub template_id: Option<String>,
    pub content_hash: Option<String>,
    pub org_id: Option<OrgId>,
    pub failure_reason: Option<String>,
}

/// Every catalog row of `sandbox_id`, newest first.
pub async fn stored_for_sandbox(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Vec<StoredSnapshot>, DbError> {
    Ok(sqlx::query_as::<_, StoredSnapshot>(sql!(
        "SELECT id,
                sandbox_id,
                type,
                state,
                timestamp,
                source_node_id,
                label,
                retention_tier,
                vcpu,
                memory_mib,
                size_bytes,
                template_id,
                content_hash,
                org_id,
                failure_reason
           FROM snapshots
          WHERE sandbox_id = $1
          ORDER BY timestamp DESC, id DESC"
    ))
    .bind(sandbox_id)
    .fetch_all(executor.into_sqlx())
    .await?)
}
