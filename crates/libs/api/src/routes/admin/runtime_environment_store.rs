//! Observed-state reads behind the runtime environment admin routes: which
//! environment each sandbox and template release is bound to. What each node
//! reports enforcing comes from the admin node listing in `node_store`. The
//! policy itself is read and written only through
//! `pplx_space_runtime_environment_policy_store`.
//!
//! Every read here is observational. Nothing takes a lock, and a count can be
//! stale by the time the caller acts on it.

use std::sync::LazyLock;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::SubstrateKind;
use pplx_space_sandbox_portability::{
    failed_repair_revert_eligibility, no_newer_full_snapshot,
    restore_retry_source_snapshot_is_current, snapshot_is_current_local_artifact,
};
use sqlx::FromRow;

/// Sandboxes grouped by their environment binding and lifecycle.
#[derive(Debug, FromRow)]
pub(super) struct SandboxPopulationRow {
    /// The `environment` field of the binding record, unparsed. `None` when
    /// the record carries no binding, or when there's no record.
    pub(super) environment_ref: Option<String>,
    /// Whether a binding record exists: the node's observation for a sandbox
    /// that isn't suspended or restoring, the snapshot a restore would pick for
    /// one that is.
    pub(super) recorded: bool,
    /// Raw `sandboxes.lifecycle`, so a value this build doesn't know is still
    /// counted rather than failing the read.
    pub(super) lifecycle: String,
    /// Whether these sandboxes can run again, by the repo's live-work rule:
    /// not `cleanup_required` or `deleting`, and not `failed` unless it looks live (an
    /// observation or Kubernetes execution the repair loop reads as live) or
    /// the repair loop can revert it to `suspended`
    /// (`failed_repair_revert_eligibility`).
    pub(super) runnable: bool,
    /// The binding record exists but isn't execution metadata this build can
    /// read: not an object, or an `environment` that's neither a string nor
    /// null (which, like an absent one, means unbound).
    pub(super) malformed: bool,
    pub(super) sandboxes: i64,
}

/// The population statement, built once: it interpolates the shared
/// definitions of a sandbox's current local artifact, of a restore retry's
/// source and of an evacuation's newest snapshot rather than re-spelling
/// them. It approximates the restore selectors' modes from row state: a
/// Kubernetes evacuation is read off `evacuation_restore_target`, and a
/// storage location already recorded on a pod isn't visible here. `$1` and
/// `$2` are the Direct and Kubernetes `managed_by` values.
static SANDBOX_POPULATION_QUERY: LazyLock<String> = LazyLock::new(|| {
    let marked = snapshot_is_current_local_artifact("sb", "sn");
    let retry_source = restore_retry_source_snapshot_is_current("sb", "sn");
    let newest_full = no_newer_full_snapshot("sb", "sn");
    let repair = failed_repair_revert_eligibility("sb");
    let (repairable, stale_marker) = (repair.eligible, repair.clear_marker);
    sql!(format!(
        "SELECT binding.metadata ->> 'environment' AS environment_ref,
                binding.recorded,
                sb.lifecycle,
                CASE
                    WHEN sb.lifecycle IN ('cleanup_required', 'deleting') THEN FALSE
                    WHEN sb.lifecycle = 'failed'
                        THEN sos.sandbox_id IS NOT NULL
                             OR (sb.managed_by = $2 AND (live.execution OR {repairable}))
                    ELSE TRUE
                END AS runnable,
                (binding.metadata IS NOT NULL
                 AND (jsonb_typeof(binding.metadata) <> 'object'
                      OR (binding.metadata ? 'environment'
                          AND jsonb_typeof(binding.metadata -> 'environment') NOT IN ('string', 'null'))))
                    AS malformed,
                COUNT(*) AS sandboxes
         FROM sandboxes sb
         LEFT JOIN sandbox_observed_states sos
                -- A failed row's observation is evidence only while it looks
                -- live, as the repair loop reads it: a status other than
                -- failed, a pid, or a sandbox still assigned to a node.
                -- Otherwise it predates the failure.
                ON sos.sandbox_id = sb.id
                AND (sb.lifecycle NOT IN ('suspended', 'restoring', 'failed')
                     OR (sb.lifecycle = 'failed'
                         AND (sos.status IS DISTINCT FROM 'failed'
                              OR sos.pid IS NOT NULL
                              OR sb.active_node_id IS NOT NULL)))
         -- The repair loop's evidence that a failed sandbox's VM may still run
         -- (`failed_restorable_repair::store::row_state_guards`): an execution
         -- the Kubernetes substrate holds active or with a pod IP.
         CROSS JOIN LATERAL (
             SELECT EXISTS (
                 SELECT 1 FROM sandbox_k8s_executions e
                  WHERE e.sandbox_id = sb.id AND (e.active OR e.pod_ip IS NOT NULL)
             ) AS execution
         ) live
         LEFT JOIN LATERAL (
             SELECT sn.id, sn.execution_metadata
             FROM snapshots sn
             WHERE sn.sandbox_id = sb.id
               AND sn.type = 'full'
               AND (
                   -- A sandbox a restore would bring back: its substrate's rule.
                   (sb.lifecycle IN ('suspended', 'restoring')
                    AND (
                        (sb.managed_by = $1 AND (
                            sb.lifecycle = 'suspended' OR {retry_source}
                        ))
                        OR (sb.managed_by = $2
                            AND sn.state = 'ready'
                            AND EXISTS (
                                SELECT 1
                                FROM snapshot_storage_locations ssl
                                WHERE ssl.snapshot_id = sn.id AND ssl.orphaned_at IS NULL
                            )
                            AND (
                                (sb.evacuation_restore_target IS NOT NULL AND (
                                    {marked}
                                    OR (sb.current_local_artifact_id IS NULL AND {newest_full})
                                ))
                                OR (sb.evacuation_restore_target IS NULL
                                    AND sb.lifecycle = 'suspended'
                                    AND (
                                        {marked}
                                        OR (
                                            sb.current_local_artifact_id IS NULL
                                            AND (sb.placement_changed_at IS NULL
                                                 OR sn.timestamp >= sb.placement_changed_at)
                                        )
                                    ))
                                OR (sb.evacuation_restore_target IS NULL
                                    AND sb.lifecycle = 'restoring'
                                    AND {retry_source})
                            ))
                    ))
                   -- A failed sandbox the repair loop can revert to suspended,
                   -- then restored from its durable suspend snapshot.
                   -- Only Kubernetes runs the repair loop, and it leaves a row
                   -- alone while its node still reports the VM live.
                   OR (sb.lifecycle = 'failed'
                       AND sb.managed_by = $2
                       AND sos.sandbox_id IS NULL
                       AND NOT live.execution
                       AND {repairable}
                       AND sn.state = 'ready'
                       AND EXISTS (
                           SELECT 1
                           FROM snapshot_storage_locations ssl
                           WHERE ssl.snapshot_id = sn.id AND ssl.orphaned_at IS NULL
                       )
                       -- Repair clears a marker no current backup holds, then
                       -- restores as a markerless sandbox would.
                       AND ({marked} OR sb.current_local_artifact_id IS NULL OR {stale_marker}))
               )
             ORDER BY sn.timestamp DESC, sn.id DESC
             LIMIT 1
         ) current_snapshot ON TRUE
         CROSS JOIN LATERAL (
             -- The chosen snapshot decides, even with no metadata; the
             -- observation only stands in when no snapshot was chosen.
             SELECT CASE
                        WHEN current_snapshot.id IS NOT NULL
                            THEN current_snapshot.execution_metadata
                        ELSE sos.execution_metadata
                    END AS metadata,
                    (current_snapshot.id IS NOT NULL OR sos.sandbox_id IS NOT NULL)
                        AS recorded
         ) binding
         WHERE sb.lifecycle <> 'deleted'
         GROUP BY 1, 2, 3, 4, 5"
    ))
});

/// Count every sandbox that isn't a deletion tombstone by environment binding
/// and lifecycle.
///
/// A suspended or restoring sandbox runs on whatever its snapshot was
/// captured with, so its binding comes from the snapshot its substrate's
/// restore would pick:
///
/// * **Direct** (`direct_restore_candidates_for_mode`) restores from the
///   artifact its source node holds, so it takes the newest full snapshot in
///   any upload state, or, for a restoring sandbox, the newest from before the
///   restore began, by the shared retry-source rule.
/// * **Kubernetes** (`selected_suspend_restore_storage_uri`) restores from a
///   durable copy, so it takes the newest `ready` full snapshot with a live
///   storage location. While a node drain evacuates the sandbox, that's the
///   one holding its current local artifact, or, with no artifact marker,
///   the newest full snapshot. Otherwise a suspended sandbox takes the one
///   holding its current local artifact, or, with no marker, one taken since
///   its last placement, and a restoring sandbox takes the retry source.
///
/// A failed sandbox whose observation looks live, as the repair loop reads
/// it (a status other than `failed`, a pid, or the sandbox still assigned to
/// a node), is bound by that observation, and can run. One with a live
/// Kubernetes execution but no such observation can run too, and has no
/// record. Otherwise, a failed
/// Kubernetes sandbox the repair loop can revert to `suspended`
/// (`failed_repair_revert_eligibility`) is bound by the snapshot it would then
/// restore from: its newest ready full snapshot with a live storage location,
/// the marked one if a current backup holds its marker. Placement freshness
/// doesn't apply: a failed transition leaves its placement stamps unreliable.
/// Any other failed sandbox can't run again and has no record. The repair
/// loop's age and restore-lease guards aren't modelled, so this can count as
/// runnable a row the loop would still skip, which only asks for `strand`
/// more often.
///
/// A suspended or restoring sandbox with no such snapshot has no restore
/// source and counts as unrecorded.
/// Every other sandbox's binding comes from the execution metadata its node
/// last published. Unbound records (legacy work) aren't attributed to an
/// environment here: that needs the legacy-equivalence rule, which only a
/// node's compiled catalog can apply.
pub(super) async fn sandbox_population(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<SandboxPopulationRow>, sqlx::Error> {
    sqlx::query_as(sqlx::AssertSqlSafe(SANDBOX_POPULATION_QUERY.as_str()))
        .bind(SubstrateKind::Space)
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(executor)
        .await
}

/// Template releases grouped by environment binding and whether anything
/// still creates sandboxes from them.
#[derive(Debug, FromRow)]
pub(super) struct TemplateReleasePopulationRow {
    /// Unparsed, like [`SandboxPopulationRow::environment_ref`]. `None` for
    /// a release built before environments were recorded.
    pub(super) environment_ref: Option<String>,
    /// Whether a tag points to these releases, which creates by tag resolve
    /// to, or an enabled pool config pins them, which keeps creating warm
    /// slots. Any other release is reachable only by pinning its exact hash.
    pub(super) in_use: bool,
    /// As [`SandboxPopulationRow::malformed`].
    pub(super) malformed: bool,
    pub(super) template_releases: i64,
}

/// Count every template release by the environment it was built for and
/// whether it's in use.
pub(super) async fn template_release_population(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<TemplateReleasePopulationRow>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT tr.execution_metadata ->> 'environment' AS environment_ref,
                (EXISTS (
                     SELECT 1 FROM template_release_tags tag
                     WHERE tag.template_release_id = tr.id
                 )
                 OR EXISTS (
                     SELECT 1 FROM kubernetes_pool_configs pool
                     WHERE pool.template_release_id = tr.id AND pool.enabled
                 )) AS in_use,
                (tr.execution_metadata IS NOT NULL
                 AND (jsonb_typeof(tr.execution_metadata) <> 'object'
                      OR (tr.execution_metadata ? 'environment'
                          AND jsonb_typeof(tr.execution_metadata -> 'environment') NOT IN ('string', 'null'))))
                    AS malformed,
                COUNT(*) AS template_releases
         FROM template_releases tr
         GROUP BY 1, 2, 3"
    ))
    .fetch_all(executor)
    .await
}
