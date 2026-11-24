//! The cluster controller's orphaned-sandbox plan read: what a missing pod's
//! row would resolve to, read under the same classification guard the
//! resolution writes in [`super`] re-check.
//!
//! The guard is spelled out in each statement, one statement per guard and
//! lock shape, and reads the same as the classification guard fragments the
//! resolution writes are built from, which its tests compare them with. The
//! restart eligibility comes whole from `pplx_space_sandbox_portability` and
//! the node-report check from `pplx_space_sandbox_residency`.
//!
//! The reconciler keeps its transaction, the per-sandbox observation lock it
//! takes first in its own statement ([`super::lock_orphan_observation`]), and
//! the disposition it decides from the plan.

use std::sync::LazyLock;

use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use pplx_space_sandbox_portability::orphan_recovery_revert_eligibility;
use pplx_space_sandbox_residency::node_observes_sandbox;
use pplx_space_sql_locator::sql;
use sqlx::{Postgres, Transaction};

use super::OrphanGuard;

/// What the plan read found for a row that still matches its guard.
#[derive(Debug, sqlx::FromRow)]
pub struct OrphanPlanRow {
    pub from_lifecycle: SandboxLifecycle,
    /// The row may restart from its durable artifacts
    /// (`orphan_recovery_revert_eligibility`).
    pub recoverable: bool,
    /// One of the sandbox's snapshots is still `creating`.
    pub creating_snapshot: bool,
    /// The sandbox's active node reports an observed state for it. The
    /// internal API writes a node's observed state only for the rows it is
    /// the active node of, so that is the node whose report counts.
    pub node_reports_copy: bool,
}

/// [`OrphanGuard::PodName`]'s plan, locking the row and its matched
/// execution.
static LOCK_POD_NAME_PLAN: LazyLock<String> = LazyLock::new(|| {
    let recoverable = orphan_recovery_revert_eligibility("s").eligible;
    let node_observes = node_observes_sandbox("s", "s.active_node_id");
    sql!(
        format!(
            "SELECT s.lifecycle AS from_lifecycle,
                    ({recoverable}) AS recoverable,
                    EXISTS (
                        SELECT 1
                          FROM snapshots creating_snapshot
                         WHERE creating_snapshot.sandbox_id = s.id
                           AND creating_snapshot.state = 'creating'
                    ) AS creating_snapshot,
                    -- The revert restores a snapshot, which loses whatever the
                    -- sandbox wrote since. The activity clocks can't prove it
                    -- wrote nothing, so while the node holding its copy reports
                    -- it, that node decides. The candidate listing skips such a
                    -- row whatever the census's age, so this sees a report the
                    -- node published after the listing, and holds it the same
                    -- way. Any record
                    -- counts, unlike the drain's `vm_observed`: a node that
                    -- reports the sandbox `failed` gets the row projected
                    -- `failed` itself.
                    {node_observes} AS node_reports_copy
               FROM sandboxes s
               JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND e.active
                AND e.pod_name = $3
                AND NULLIF(BTRIM(e.pod_uid), '') IS NOT DISTINCT FROM $4
              WHERE s.id = $1
                AND s.lifecycle <> ALL($2)
                AND (
                    (
                        s.lifecycle = ANY($5)
                        AND (
                            e.state NOT IN ('creating', 'recovering')
                            OR e.updated_at <= NOW() - ($7 * INTERVAL '1 second')
                        )
                    )
                    OR (
                        s.lifecycle = ANY($6)
                        AND e.updated_at <= NOW() - ($7 * INTERVAL '1 second')
                    )
                )
              FOR UPDATE OF s, e"
        )
        .as_str()
    )
});

/// [`OrphanGuard::Forgotten`]'s plan, locking the row.
static LOCK_FORGOTTEN_PLAN: LazyLock<String> = LazyLock::new(|| {
    let recoverable = orphan_recovery_revert_eligibility("s").eligible;
    let node_observes = node_observes_sandbox("s", "s.active_node_id");
    sql!(
        format!(
            "SELECT s.lifecycle AS from_lifecycle,
                    ({recoverable}) AS recoverable,
                    EXISTS (
                        SELECT 1
                          FROM snapshots creating_snapshot
                         WHERE creating_snapshot.sandbox_id = s.id
                           AND creating_snapshot.state = 'creating'
                    ) AS creating_snapshot,
                    -- The revert restores a snapshot, which loses whatever the
                    -- sandbox wrote since. The activity clocks can't prove it
                    -- wrote nothing, so while the node holding its copy reports
                    -- it, that node decides. The candidate listing skips such a
                    -- row whatever the census's age, so this sees a report the
                    -- node published after the listing, and holds it the same
                    -- way. Any record
                    -- counts, unlike the drain's `vm_observed`: a node that
                    -- reports the sandbox `failed` gets the row projected
                    -- `failed` itself.
                    {node_observes} AS node_reports_copy
               FROM sandboxes s
              WHERE s.id = $1
                AND s.lifecycle <> ALL($2)
                AND s.lifecycle = ANY($3)
                AND EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions f
                     WHERE f.sandbox_id = s.id
                       AND f.state = 'forgotten'
                       AND f.generation = (
                           SELECT MAX(generation)
                             FROM sandbox_k8s_executions
                            WHERE sandbox_id = s.id
                       )
                )
              FOR UPDATE OF s"
        )
        .as_str()
    )
});

/// [`OrphanGuard::LegacyDeleting`]'s plan, locking the row.
static LOCK_LEGACY_DELETING_PLAN: LazyLock<String> = LazyLock::new(|| {
    let recoverable = orphan_recovery_revert_eligibility("s").eligible;
    let node_observes = node_observes_sandbox("s", "s.active_node_id");
    sql!(
        format!(
            "SELECT s.lifecycle AS from_lifecycle,
                    ({recoverable}) AS recoverable,
                    EXISTS (
                        SELECT 1
                          FROM snapshots creating_snapshot
                         WHERE creating_snapshot.sandbox_id = s.id
                           AND creating_snapshot.state = 'creating'
                    ) AS creating_snapshot,
                    -- The revert restores a snapshot, which loses whatever the
                    -- sandbox wrote since. The activity clocks can't prove it
                    -- wrote nothing, so while the node holding its copy reports
                    -- it, that node decides. The candidate listing skips such a
                    -- row whatever the census's age, so this sees a report the
                    -- node published after the listing, and holds it the same
                    -- way. Any record
                    -- counts, unlike the drain's `vm_observed`: a node that
                    -- reports the sandbox `failed` gets the row projected
                    -- `failed` itself.
                    {node_observes} AS node_reports_copy
               FROM sandboxes s
              WHERE s.id = $1
                AND s.lifecycle <> ALL($2)
                AND s.lifecycle = 'deleting'
              FOR UPDATE OF s"
        )
        .as_str()
    )
});

/// [`OrphanGuard::PodName`]'s plan, unlocked.
static POD_NAME_PLAN: LazyLock<String> = LazyLock::new(|| {
    let recoverable = orphan_recovery_revert_eligibility("s").eligible;
    let node_observes = node_observes_sandbox("s", "s.active_node_id");
    sql!(
        format!(
            "SELECT s.lifecycle AS from_lifecycle,
                    ({recoverable}) AS recoverable,
                    EXISTS (
                        SELECT 1
                          FROM snapshots creating_snapshot
                         WHERE creating_snapshot.sandbox_id = s.id
                           AND creating_snapshot.state = 'creating'
                    ) AS creating_snapshot,
                    -- The revert restores a snapshot, which loses whatever the
                    -- sandbox wrote since. The activity clocks can't prove it
                    -- wrote nothing, so while the node holding its copy reports
                    -- it, that node decides. The candidate listing skips such a
                    -- row whatever the census's age, so this sees a report the
                    -- node published after the listing, and holds it the same
                    -- way. Any record
                    -- counts, unlike the drain's `vm_observed`: a node that
                    -- reports the sandbox `failed` gets the row projected
                    -- `failed` itself.
                    {node_observes} AS node_reports_copy
               FROM sandboxes s
               JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND e.active
                AND e.pod_name = $3
                AND NULLIF(BTRIM(e.pod_uid), '') IS NOT DISTINCT FROM $4
              WHERE s.id = $1
                AND s.lifecycle <> ALL($2)
                AND (
                    (
                        s.lifecycle = ANY($5)
                        AND (
                            e.state NOT IN ('creating', 'recovering')
                            OR e.updated_at <= NOW() - ($7 * INTERVAL '1 second')
                        )
                    )
                    OR (
                        s.lifecycle = ANY($6)
                        AND e.updated_at <= NOW() - ($7 * INTERVAL '1 second')
                    )
                )"
        )
        .as_str()
    )
});

/// [`OrphanGuard::Forgotten`]'s plan, unlocked.
static FORGOTTEN_PLAN: LazyLock<String> = LazyLock::new(|| {
    let recoverable = orphan_recovery_revert_eligibility("s").eligible;
    let node_observes = node_observes_sandbox("s", "s.active_node_id");
    sql!(
        format!(
            "SELECT s.lifecycle AS from_lifecycle,
                    ({recoverable}) AS recoverable,
                    EXISTS (
                        SELECT 1
                          FROM snapshots creating_snapshot
                         WHERE creating_snapshot.sandbox_id = s.id
                           AND creating_snapshot.state = 'creating'
                    ) AS creating_snapshot,
                    -- The revert restores a snapshot, which loses whatever the
                    -- sandbox wrote since. The activity clocks can't prove it
                    -- wrote nothing, so while the node holding its copy reports
                    -- it, that node decides. The candidate listing skips such a
                    -- row whatever the census's age, so this sees a report the
                    -- node published after the listing, and holds it the same
                    -- way. Any record
                    -- counts, unlike the drain's `vm_observed`: a node that
                    -- reports the sandbox `failed` gets the row projected
                    -- `failed` itself.
                    {node_observes} AS node_reports_copy
               FROM sandboxes s
              WHERE s.id = $1
                AND s.lifecycle <> ALL($2)
                AND s.lifecycle = ANY($3)
                AND EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions f
                     WHERE f.sandbox_id = s.id
                       AND f.state = 'forgotten'
                       AND f.generation = (
                           SELECT MAX(generation)
                             FROM sandbox_k8s_executions
                            WHERE sandbox_id = s.id
                       )
                )"
        )
        .as_str()
    )
});

/// [`OrphanGuard::LegacyDeleting`]'s plan, unlocked.
static LEGACY_DELETING_PLAN: LazyLock<String> = LazyLock::new(|| {
    let recoverable = orphan_recovery_revert_eligibility("s").eligible;
    let node_observes = node_observes_sandbox("s", "s.active_node_id");
    sql!(
        format!(
            "SELECT s.lifecycle AS from_lifecycle,
                    ({recoverable}) AS recoverable,
                    EXISTS (
                        SELECT 1
                          FROM snapshots creating_snapshot
                         WHERE creating_snapshot.sandbox_id = s.id
                           AND creating_snapshot.state = 'creating'
                    ) AS creating_snapshot,
                    -- The revert restores a snapshot, which loses whatever the
                    -- sandbox wrote since. The activity clocks can't prove it
                    -- wrote nothing, so while the node holding its copy reports
                    -- it, that node decides. The candidate listing skips such a
                    -- row whatever the census's age, so this sees a report the
                    -- node published after the listing, and holds it the same
                    -- way. Any record
                    -- counts, unlike the drain's `vm_observed`: a node that
                    -- reports the sandbox `failed` gets the row projected
                    -- `failed` itself.
                    {node_observes} AS node_reports_copy
               FROM sandboxes s
              WHERE s.id = $1
                AND s.lifecycle <> ALL($2)
                AND s.lifecycle = 'deleting'"
        )
        .as_str()
    )
});

/// Lock the rows the guard reads and read the row's plan; `None` when the
/// row is in one of `stop_lifecycles` or no longer matches the guard. For [`OrphanGuard::PodName`] it locks the sandbox
/// row and the matched active execution (`FOR UPDATE OF s, e`), for the other
/// guards the sandbox row only (`FOR UPDATE OF s`). The lock lasts until the
/// caller's transaction ends, where the resolution write that follows
/// re-checks the same guard. Run [`super::lock_orphan_observation`]
/// first, in its own statement. Bind order as on [`OrphanGuard`].
pub async fn lock_plan(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    stop_lifecycles: &[&str],
    guard: &OrphanGuard<'_>,
) -> Result<Option<OrphanPlanRow>, sqlx::Error> {
    let query = match guard {
        OrphanGuard::PodName { .. } => &*LOCK_POD_NAME_PLAN,
        OrphanGuard::Forgotten { .. } => &*LOCK_FORGOTTEN_PLAN,
        OrphanGuard::LegacyDeleting => &*LOCK_LEGACY_DELETING_PLAN,
    };
    fetch_plan(&mut **tx, query, sandbox_id, stop_lifecycles, guard).await
}

/// Read the row's plan without locking it; `None` as for [`lock_plan`]. The
/// guard compares execution ages with the database clock, so run it on the
/// executor whose snapshot the caller acts on.
pub async fn read_plan(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    stop_lifecycles: &[&str],
    guard: &OrphanGuard<'_>,
) -> Result<Option<OrphanPlanRow>, sqlx::Error> {
    let query = match guard {
        OrphanGuard::PodName { .. } => &*POD_NAME_PLAN,
        OrphanGuard::Forgotten { .. } => &*FORGOTTEN_PLAN,
        OrphanGuard::LegacyDeleting => &*LEGACY_DELETING_PLAN,
    };
    fetch_plan(executor, query, sandbox_id, stop_lifecycles, guard).await
}

async fn fetch_plan(
    executor: impl sqlx::PgExecutor<'_>,
    query: &str,
    sandbox_id: SandboxId,
    stop_lifecycles: &[&str],
    guard: &OrphanGuard<'_>,
) -> Result<Option<OrphanPlanRow>, sqlx::Error> {
    let query = sqlx::query_as::<_, OrphanPlanRow>(sqlx::AssertSqlSafe(query))
        .bind(sandbox_id)
        .bind(stop_lifecycles);
    match *guard {
        OrphanGuard::PodName {
            name,
            uid,
            live_pod_required_lifecycles,
            pending_pod_lifecycles,
            create_age_threshold_secs,
        } => {
            query
                .bind(name)
                .bind(uid)
                .bind(live_pod_required_lifecycles)
                .bind(pending_pod_lifecycles)
                .bind(create_age_threshold_secs)
                .fetch_optional(executor)
                .await
        }
        OrphanGuard::Forgotten {
            active_execution_required_lifecycles,
        } => {
            query
                .bind(active_execution_required_lifecycles)
                .fetch_optional(executor)
                .await
        }
        OrphanGuard::LegacyDeleting => query.fetch_optional(executor).await,
    }
}

#[cfg(test)]
mod tests;
