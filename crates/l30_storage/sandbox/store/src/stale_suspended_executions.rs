//! Which `suspended` Kubernetes sandboxes still carry an active execution
//! that the suspended-execution reaper may retire: the reaper's candidate
//! scan. The retire itself is the reaper's transaction, ending in
//! [`crate::repair::unplace_suspended_after_execution_retired`].

use pplx_space_accounts::NodeId;
use pplx_space_durations::PositiveDuration;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::LocalArtifactId;

use std::sync::LazyLock;

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_portability::newest_suspend_snapshot_current;
use pplx_space_sql_locator::sql;

/// A `suspended` sandbox with its active execution, from [`list`].
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct StaleSuspendedExecution {
    #[sqlx(rename = "id")]
    pub sandbox_id: SandboxId,
    pub generation: i64,
    pub pod_name: String,
    pub pod_uid: Option<String>,
    /// The execution's stored state text.
    #[sqlx(rename = "state")]
    pub execution_state: String,
    pub active_node_id: Option<NodeId>,
    pub current_local_artifact_id: Option<LocalArtifactId>,
}

/// One keyset page, ordered by id, of the `suspended` Kubernetes sandboxes
/// that still carry an active execution and whose newest suspend snapshot is
/// the durable full one markerless restore would select
/// ([`newest_suspend_snapshot_current`]): at most `batch_size` rows strictly
/// after `after_sandbox_id` (`None` starts a sweep) and no later than
/// `upper_bound`. Both the suspend (`lifecycle_changed_at`) and the execution
/// row (`created_at`) must be older than `min_age`.
///
/// A plain read; the ages compare with `NOW()`, so it reads the clock of the
/// executor's transaction.
pub async fn list(
    executor: impl Executor<'_>,
    batch_size: u32,
    after_sandbox_id: Option<SandboxId>,
    upper_bound: SandboxId,
    min_age: PositiveDuration,
) -> Result<Vec<StaleSuspendedExecution>, DbError> {
    /// Built once: every input is a literal or a bind placeholder.
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        let markerless_snapshot_current = newest_suspend_snapshot_current("s");
        sql!(format!(
            "SELECT s.id, e.generation, e.pod_name, e.pod_uid, e.state, s.active_node_id,
                    s.current_local_artifact_id
               FROM sandboxes s
               JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
              WHERE s.managed_by = $5
                AND s.lifecycle = 'suspended'
                AND s.lifecycle_changed_at <= NOW() - make_interval(secs => $1::BIGINT)
                AND e.created_at <= NOW() - make_interval(secs => $1::BIGINT)
                AND ({markerless_snapshot_current})
                AND ($2::uuid IS NULL OR s.id > $2)
                AND s.id <= $3
              ORDER BY s.id
              LIMIT $4"
        ))
    });
    Ok(sqlx::query_as(sqlx::AssertSqlSafe(QUERY.as_str()))
        .bind(crate::repair::duration_secs_i64(min_age))
        .bind(after_sandbox_id)
        .bind(upper_bound)
        .bind(i64::from(batch_size))
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(executor.into_sqlx())
        .await?)
}
