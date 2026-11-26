//! The reads behind a v2 drain's record of one evacuation a node reported:
//! the row's lifecycle, placement and pin ([`v2_evacuation_row`]), and, for a
//! row that pins no artifact, whether its restore has a source
//! ([`fresh_ready_full_snapshot`]). Both are plain reads; every decision is
//! the caller's.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::fresh_ready_snapshot_exists;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use uuid::Uuid;

/// A sandbox row as the v2 evacuation record reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V2EvacuationRow {
    pub lifecycle: String,
    pub active_node_id: Option<NodeId>,
    pub target_node_id: Option<NodeId>,
    /// `current_local_artifact_id`, the artifact the row pins.
    pub current_local_artifact_id: Option<Uuid>,
    /// The sandbox has an active execution whose `k8s_node_name` `IS
    /// DISTINCT FROM` the reporting node's registered name: a name missing on
    /// one side only counts as elsewhere; missing on both (no `node_k8s` row
    /// for the node, and an execution with no name) does not.
    pub executes_elsewhere: bool,
}

/// `sandbox_id`'s row as the record of an evacuation reported by `node_id`
/// reads it. A plain read; `node_id` only supplies the name
/// `executes_elsewhere` compares with. `None` when the row is gone.
pub async fn v2_evacuation_row(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> Result<Option<V2EvacuationRow>, sqlx::Error> {
    type Row = (String, Option<NodeId>, Option<NodeId>, Option<Uuid>, bool);
    let row: Option<Row> = sqlx::query_as(sql!(
        "SELECT s.lifecycle, s.active_node_id, s.target_node_id, s.current_local_artifact_id,
                EXISTS (
                  SELECT 1 FROM sandbox_k8s_executions e
                   WHERE e.sandbox_id = s.id
                     AND e.active
                     AND e.k8s_node_name IS DISTINCT FROM
                         (SELECT k8s_node_name FROM node_k8s WHERE node_id = $2)
                )
           FROM sandboxes s
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .fetch_optional(executor)
    .await?;
    Ok(row.map(
        |(
            lifecycle,
            active_node_id,
            target_node_id,
            current_local_artifact_id,
            executes_elsewhere,
        )| {
            V2EvacuationRow {
                lifecycle,
                active_node_id,
                target_node_id,
                current_local_artifact_id,
                executes_elsewhere,
            }
        },
    ))
}

/// `$1` = sandbox id.
static FRESH_READY_FULL_SNAPSHOT: LazyLock<String> = LazyLock::new(|| {
    sql!(format!(
        "SELECT {} FROM sandboxes s WHERE s.id = $1",
        fresh_ready_snapshot_exists("s")
    ))
});

/// Whether the restore selector's [`fresh_ready_snapshot_exists`] finds a
/// source for `sandbox_id`, as the row reads when this runs. For a row that
/// pins an artifact, that is a ready, stored full snapshot of that artifact.
/// For one that pins none, the case the v2 evacuation record asks, it is a
/// ready, stored full snapshot taken at or after `placement_changed_at`, or
/// any such snapshot while that is NULL. Fails with `RowNotFound` when the row
/// is gone.
pub async fn fresh_ready_full_snapshot(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(FRESH_READY_FULL_SNAPSHOT.as_str()))
        .bind(sandbox_id)
        .fetch_one(executor)
        .await
}
