//! Node drain's give-up for a resident with no pod: the row-state guards it
//! reads before the pod reads and again under the row locks
//! ([`read_guards`]). The revert itself is
//! [`crate::drain::revert_pod_less_resident_to_suspended`], and the
//! give-up's execution lock and retire are in
//! [`crate::k8s_executions::pod_less_giveup`]; the decision is the caller's.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_node_model::{NodeDrainMode, NodeDrainPhase, SubstrateKind};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{ExecutionGeneration, SandboxId, SandboxLifecycle};
use pplx_space_sandbox_portability::{
    failed_repair_revert_eligibility, newest_full_snapshot_unambiguous,
    pod_less_drain_revert_loses_no_newer_bytes,
};
use pplx_space_sql_locator::sql;

/// The row-state guards of one sandbox, as [`read_guards`] reads them.
#[derive(Debug, Clone, Copy, sqlx::FromRow)]
pub struct PodLessGuards {
    /// Still the resident the suspend failed on: the lifecycle the claim
    /// reverted to, on the node, no evacuation marker, the given substrate,
    /// and the node still in the given drain phase and mode under a v1
    /// drain. The mode is checked because the give-up is for a
    /// force-draining node: a gentle drain doesn't touch residents.
    pub row_matches: bool,
    /// A node publishes the sandbox as anything but a `failed`, pid-less
    /// record.
    pub vm_observed: bool,
    /// An active execution carries a pod UID, or any execution a pod IP.
    pub pod_bound: bool,
    /// An execution advanced `updated_at` within the staleness window.
    pub recent_progress: bool,
    /// A durable suspend snapshot passes
    /// [`failed_repair_revert_eligibility`].
    pub restorable: bool,
    /// Reverting now loses nothing newer and restores the snapshot the
    /// eligibility proved current:
    /// [`pod_less_drain_revert_loses_no_newer_bytes`] and
    /// [`newest_full_snapshot_unambiguous`] hold.
    pub revert_safe: bool,
    /// The sandbox's newest execution generation; `None` without an
    /// execution row.
    pub max_generation: Option<ExecutionGeneration>,
}

/// What [`read_guards`] reads about.
#[derive(Debug, Clone, Copy)]
pub struct PodLessGuardsQuery {
    pub sandbox_id: SandboxId,
    /// The draining node the sandbox is resident on.
    pub source_node_id: NodeId,
    /// The lifecycle the suspend's claim reverted to.
    pub pre_lifecycle: SandboxLifecycle,
    /// The staleness window of `recent_progress`, in seconds.
    pub stale_secs: i64,
    pub managed_by: SubstrateKind,
    pub drain_phase: NodeDrainPhase,
    pub drain_mode: NodeDrainMode,
}

/// `$1` = sandbox id, `$2` = source node id, `$3` = pre-claim lifecycle,
/// `$4` = staleness window in seconds, `$5` = substrate, `$6` = drain phase,
/// `$7` = drain mode.
static READ_GUARDS: LazyLock<String> = LazyLock::new(|| {
    sql!(format!(
        "SELECT COALESCE((
                    s.lifecycle = $3
                    AND s.active_node_id = $2
                    AND s.evacuation_restore_target IS NULL
                    AND s.managed_by = $5
                    AND EXISTS (
                        SELECT 1
                          FROM nodes n
                         WHERE n.id = $2
                           AND n.drain_phase = $6
                           AND n.drain_mode = $7
                           AND n.drain_engine IS NULL
                    )
                ), FALSE) AS row_matches,
                EXISTS (
                    SELECT 1
                      FROM sandbox_observed_states o
                     WHERE o.sandbox_id = s.id
                       AND (o.status IS DISTINCT FROM 'failed' OR o.pid IS NOT NULL)
                ) AS vm_observed,
                EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND (
                           e.pod_ip IS NOT NULL
                           OR (e.active AND NULLIF(BTRIM(e.pod_uid), '') IS NOT NULL)
                       )
                ) AS pod_bound,
                EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.updated_at > NOW() - make_interval(secs => $4::BIGINT)
                ) AS recent_progress,
                COALESCE({eligible}, FALSE) AS restorable,
                COALESCE({loses_no_newer_bytes} AND {unambiguous}, FALSE) AS revert_safe,
                (
                    SELECT MAX(e.generation)
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                ) AS max_generation
           FROM sandboxes s
          WHERE s.id = $1",
        eligible = failed_repair_revert_eligibility("s").eligible,
        loses_no_newer_bytes = pod_less_drain_revert_loses_no_newer_bytes("s"),
        unambiguous = newest_full_snapshot_unambiguous("s"),
    ))
});

/// Every guard of the give-up for `query.sandbox_id`, evaluated once;
/// `None` when the row is gone. A plain read: the caller runs it lock-free
/// before the pod reads, and again on its transaction under its locks on the
/// node, sandbox and execution rows. Those locks don't hold the snapshot rows
/// behind `restorable` and `revert_safe`, which the revert re-checks in its
/// own statement, nor the observed states behind `vm_observed`.
/// `recent_progress` compares with
/// the database clock of the executor it runs on.
pub async fn read_guards(
    executor: impl Executor<'_>,
    query: PodLessGuardsQuery,
) -> Result<Option<PodLessGuards>, DbError> {
    Ok(sqlx::query_as(sqlx::AssertSqlSafe(READ_GUARDS.as_str()))
        .bind(query.sandbox_id)
        .bind(query.source_node_id)
        .bind(query.pre_lifecycle.as_str())
        .bind(query.stale_secs)
        .bind(query.managed_by)
        .bind(query.drain_phase.as_str())
        .bind(query.drain_mode)
        .fetch_optional(executor.into_sqlx())
        .await?)
}
