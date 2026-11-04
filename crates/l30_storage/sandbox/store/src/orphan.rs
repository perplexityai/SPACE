//! Guarded writes the cluster controller's orphaned-sandbox reconciler makes
//! to `sandboxes` once a row's pod is gone: projecting a recoverable row back
//! to `suspended`, or holding an ambiguous one in `quarantined`.
//!
//! The reconciler keeps everything around them: its candidate list, its plan
//! read (`SELECT ... FOR UPDATE` on the same guard), the transaction, the
//! reconcile-state cleanup, and the quarantine state and history rows. Each
//! write here is one statement whose CTEs also retire the active execution
//! and reset open port exposures; splitting it would change what it does.
//!
//! The classification guard fragments ([`classify_guard_sql`],
//! [`classify_guard_from_sql`] and [`classify_guard_lock_sql`]) are also
//! interpolated by the reconciler's plan read. They are public so that each
//! guard keeps one definition while that read lives elsewhere.

use pplx_space_common_core::types::{NodeId, PodUid, SandboxId, SandboxLifecycle};

use pplx_space_common_core::sql;
use pplx_space_sandbox_portability::started_repair_revert_eligibility;
use sqlx::{Postgres, Transaction};

/// The classification guard a statement here re-checks, with the values it
/// binds: the reconciler's own guard, converted at its call site, and the
/// same lifecycle sets and create age it binds into its plan read.
///
/// Every statement that interpolates the guard fragments binds `$1` = sandbox
/// id and `$2` = the reconciler's stop lifecycles, then for
/// [`OrphanGuard::PodName`] `$3` = pod name, `$4` = pod UID, `$5` = the
/// live-pod lifecycles, `$6` = the pending-pod lifecycles and `$7` = the
/// create age in seconds, and for [`OrphanGuard::Forgotten`] `$3` = the
/// active-execution lifecycles.
#[derive(Debug, Clone, Copy)]
pub enum OrphanGuard<'a> {
    /// The active execution's pod was observed missing under this name and
    /// UID (`None` when the stored UID was NULL).
    PodName {
        name: &'a str,
        uid: Option<&'a PodUid>,
        live_pod_required_lifecycles: &'a [&'a str],
        pending_pod_lifecycles: &'a [&'a str],
        create_age_threshold_secs: i64,
    },
    /// The latest execution was forgotten while the lifecycle still needs an
    /// active one.
    Forgotten {
        active_execution_required_lifecycles: &'a [&'a str],
    },
    /// The row already sits in `deleting`.
    LegacyDeleting,
}

/// What a resolution wrote: the lifecycle it left, the node it retired, and
/// whether it cleared the local-artifact marker.
#[derive(Debug, sqlx::FromRow)]
pub struct OrphanResolution {
    pub from_lifecycle: SandboxLifecycle,
    pub active_node_id: Option<NodeId>,
    clear_marker: Option<bool>,
}

impl OrphanResolution {
    pub fn clear_marker(&self) -> bool {
        self.clear_marker.unwrap_or(false)
    }
}

/// Project an orphaned row whose bytes are recoverable back to `suspended`,
/// retiring its node assignment, its active execution and its open port
/// exposures in the same statement. Returns what it wrote, or `None` when the
/// row no longer matches.
///
/// Writes an **inference**: the reconciler concluding, from a missing pod and
/// durable artifact evidence, that the sandbox is at rest. The statement
/// re-checks the guard and the restart eligibility itself. Its first CTE locks
/// the sandbox row and, for [`OrphanGuard::PodName`], the matched execution
/// (`FOR UPDATE OF`). The other rows its predicates read, such as `snapshots`
/// and `snapshot_storage_locations`, are not locked; the statement reads them
/// again when it runs. The caller runs it in the transaction whose plan read
/// already locked the same rows and decided the same predicates, so a re-check
/// can change the outcome only for a caller without that read, or when one of
/// those unlocked rows changed between the two statements.
pub async fn recover_orphan_to_suspended(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    stop_lifecycles: &[&str],
    guard: &OrphanGuard<'_>,
) -> Result<Option<OrphanResolution>, sqlx::Error> {
    let eligibility = started_repair_revert_eligibility("s");
    let clear_marker = eligibility.clear_marker;
    let eligible = eligibility.eligible;
    let guard_sql = classify_guard_sql(guard);
    let guard_from_sql = classify_guard_from_sql(guard);
    let guard_lock_sql = classify_guard_lock_sql(guard);
    let query = format!(
        "WITH pre AS (
             SELECT s.id,
                    s.lifecycle AS from_lifecycle,
                    s.active_node_id,
                    {clear_marker} AS clear_marker
               FROM {guard_from_sql}
              WHERE s.id = $1
                AND s.lifecycle <> ALL($2)
                AND {guard_sql}
                AND {eligible}
              FOR UPDATE OF {guard_lock_sql}
         ),
         updated_sandbox AS (
             UPDATE sandboxes s
                SET lifecycle = 'suspended',
                    placement = 'unplaced',
                    active_node_id = NULL,
                    command_ready_pending = FALSE,
                    target_node_id = NULL,
                    placement_changed_at = NULL,
                    current_local_artifact_id = CASE WHEN pre.clear_marker THEN NULL ELSE s.current_local_artifact_id END,\n\
                    current_local_artifact_kind = CASE WHEN pre.clear_marker THEN NULL ELSE s.current_local_artifact_kind END,\n\
                    current_local_artifact_origin = CASE WHEN pre.clear_marker THEN NULL ELSE s.current_local_artifact_origin END
               FROM pre
              WHERE s.id = pre.id
              RETURNING s.id,
                        pre.from_lifecycle,
                        pre.active_node_id,
                        pre.clear_marker
         ),
         retired_execution AS (\n\
             UPDATE sandbox_k8s_executions e\n\
                SET active = FALSE,\n\
                    state = 'suspended',\n\
                    pod_ip = NULL,\n\
                    terminated_at = COALESCE(e.terminated_at, NOW()),\n\
                    updated_at = NOW()\n\
               FROM updated_sandbox\n\
              WHERE e.sandbox_id = updated_sandbox.id\n\
                AND e.active\n\
              RETURNING e.sandbox_id\n\
         ),
         reset_port_exposures AS (\n\
             UPDATE sandbox_port_exposures spe\n\
                SET apply_state = 'pending',\n\
                    last_applied_node_id = NULL,\n\
                    last_error = NULL,\n\
                    last_bridge_status = NULL,\n\
                    last_applied_bridge_generation = NULL,\n\
                    last_observed_at = NULL,\n\
                    updated_at = NOW()\n\
               FROM updated_sandbox\n\
              WHERE spe.sandbox_id = updated_sandbox.id\n\
                AND spe.desired_state = 'open'\n\
              RETURNING spe.sandbox_id\n\
         )
         SELECT from_lifecycle, active_node_id, clear_marker
           FROM updated_sandbox"
    );
    fetch_guarded_resolution(tx, query, sandbox_id, stop_lifecycles, guard).await
}

/// Move an orphaned row whose state is ambiguous to `quarantined`, retiring
/// its node assignment, its active execution and its open port exposures in
/// the same statement. Returns what it wrote, or `None` when the row no longer
/// matches.
///
/// Writes an **inference**: the reconciler concluding that nothing can prove
/// the sandbox recoverable. The statement re-checks the guard and that the
/// row is not eligible for recovery. Its first CTE locks the sandbox row and,
/// for [`OrphanGuard::PodName`], the matched execution (`FOR UPDATE OF`), but
/// not the snapshot rows the eligibility reads. The caller runs it in the
/// transaction whose plan read already locked the same rows, and writes the
/// quarantine rows after it. For [`OrphanGuard::LegacyDeleting`] the
/// eligibility check is `TRUE`, which the reconciler never reaches today: its
/// plan finalizes such a row's delete, or defers it while a snapshot is being
/// created.
pub async fn quarantine_orphan(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    stop_lifecycles: &[&str],
    guard: &OrphanGuard<'_>,
) -> Result<Option<OrphanResolution>, sqlx::Error> {
    let eligibility = started_repair_revert_eligibility("s");
    let eligible = eligibility.eligible;
    let artifact_predicate = if matches!(guard, OrphanGuard::LegacyDeleting) {
        "TRUE".to_string()
    } else {
        format!("NOT ({eligible})")
    };
    // Retire the old node assignment along with the lifecycle flip: the pod
    // is authoritatively gone, so leaving `active_node_id` in place keeps the
    // old node eligible for the same sandbox desired state (including open
    // port exposures against a torn-down CNI netns) forever. Open port
    // exposure rows keep their `desired_state='open'` user intent and only
    // have the application observation reset, so the intent re-applies if an
    // operator later recovers the sandbox.
    let guard_sql = classify_guard_sql(guard);
    let guard_from_sql = classify_guard_from_sql(guard);
    let guard_lock_sql = classify_guard_lock_sql(guard);
    let query = format!(
        "WITH pre AS (
             SELECT s.id,
                    s.lifecycle AS from_lifecycle,
                    s.active_node_id
               FROM {guard_from_sql}
              WHERE s.id = $1
                AND s.lifecycle <> ALL($2)
                AND {guard_sql}
                AND {artifact_predicate}
              FOR UPDATE OF {guard_lock_sql}
         ),
         updated_sandbox AS (
             UPDATE sandboxes s
                SET lifecycle = 'quarantined',
                    placement = 'unplaced',
                    active_node_id = NULL,
                    command_ready_pending = FALSE,
                    target_node_id = NULL,
                    placement_changed_at = NULL
               FROM pre
              WHERE s.id = pre.id
              RETURNING s.id,
                        pre.from_lifecycle,
                        pre.active_node_id,
                        FALSE AS clear_marker
         ),
         retired_execution AS (\n\
             UPDATE sandbox_k8s_executions e\n\
                SET active = FALSE,\n\
                    state = 'quarantined',\n\
                    pod_ip = NULL,\n\
                    terminated_at = COALESCE(e.terminated_at, NOW()),\n\
                    updated_at = NOW()\n\
               FROM updated_sandbox\n\
              WHERE e.sandbox_id = updated_sandbox.id\n\
                AND e.active\n\
              RETURNING e.sandbox_id\n\
         ),
         reset_port_exposures AS (\n\
             UPDATE sandbox_port_exposures spe\n\
                SET apply_state = 'pending',\n\
                    last_applied_node_id = NULL,\n\
                    last_error = NULL,\n\
                    last_bridge_status = NULL,\n\
                    last_applied_bridge_generation = NULL,\n\
                    last_observed_at = NULL,\n\
                    updated_at = NOW()\n\
               FROM updated_sandbox\n\
              WHERE spe.sandbox_id = updated_sandbox.id\n\
                AND spe.desired_state = 'open'\n\
              RETURNING spe.sandbox_id\n\
         )
         SELECT from_lifecycle, active_node_id, clear_marker
           FROM updated_sandbox"
    );
    fetch_guarded_resolution(tx, query, sandbox_id, stop_lifecycles, guard).await
}

/// The guard's lifecycle and execution predicate, on the row aliased `s` and,
/// for [`OrphanGuard::PodName`], the execution aliased `e` (see
/// [`classify_guard_from_sql`]). Bind order as on [`OrphanGuard`].
pub fn classify_guard_sql(guard: &OrphanGuard<'_>) -> &'static str {
    match guard {
        OrphanGuard::PodName { .. } => {
            "(
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
        }
        OrphanGuard::Forgotten { .. } => {
            "s.lifecycle = ANY($3)
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
        }
        OrphanGuard::LegacyDeleting => "s.lifecycle = 'deleting'",
    }
}

/// The `FROM` items the guard reads: `sandboxes s`, joined for
/// [`OrphanGuard::PodName`] to the active execution `e` with the observed pod
/// name and UID.
pub fn classify_guard_from_sql(guard: &OrphanGuard<'_>) -> &'static str {
    match guard {
        OrphanGuard::PodName { .. } => {
            "sandboxes s
             JOIN sandbox_k8s_executions e
              ON e.sandbox_id = s.id
              AND e.active
              AND e.pod_name = $3
              AND NULLIF(BTRIM(e.pod_uid), '') IS NOT DISTINCT FROM $4"
        }
        OrphanGuard::Forgotten { .. } | OrphanGuard::LegacyDeleting => "sandboxes s",
    }
}

/// The `FOR UPDATE OF` targets that lock what the guard read.
pub fn classify_guard_lock_sql(guard: &OrphanGuard<'_>) -> &'static str {
    match guard {
        OrphanGuard::PodName { .. } => "s, e",
        OrphanGuard::Forgotten { .. } | OrphanGuard::LegacyDeleting => "s",
    }
}

async fn fetch_guarded_resolution(
    tx: &mut Transaction<'_, Postgres>,
    query: String,
    sandbox_id: SandboxId,
    stop_lifecycles: &[&str],
    guard: &OrphanGuard<'_>,
) -> Result<Option<OrphanResolution>, sqlx::Error> {
    let query = sqlx::query_as::<_, OrphanResolution>(sqlx::AssertSqlSafe(sql!(query.as_str())))
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
                .fetch_optional(&mut **tx)
                .await
        }
        OrphanGuard::Forgotten {
            active_execution_required_lifecycles,
        } => {
            query
                .bind(active_execution_required_lifecycles)
                .fetch_optional(&mut **tx)
                .await
        }
        OrphanGuard::LegacyDeleting => query.fetch_optional(&mut **tx).await,
    }
}
