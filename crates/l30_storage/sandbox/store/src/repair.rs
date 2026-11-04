//! Writes by the cluster controller's repair loops: the control plane
//! concluding, from its own rows and, depending on the loop, Kubernetes pod
//! lookups and the worker node's absence report, that a sandbox row no longer
//! describes a running sandbox, and leaving it `suspended` and unplaced (the
//! suspended-execution reaper's row is already `suspended`; that verb only
//! unplaces it).
//!
//! Each verb is one `UPDATE sandboxes` statement, and all but
//! [`recover_quarantined_publication_to_suspended`] re-check part of the
//! repair's preconditions in their `WHERE`. The caller owns everything
//! around it: the transaction, any `SELECT ... FOR UPDATE` it takes first,
//! and the writes to the other tables the same repair touches (executions,
//! port exposures, observed states, quarantine state).

use pplx_space_common_core::types::{LocalArtifactId, PositiveDuration, SandboxId, SubstrateKind};

use pplx_space_common_core::sql;
use pplx_space_sandbox_portability::{
    SuspendedTargetNodeAssignment, clear_current_local_artifact_marker_assignments,
    failed_repair_revert_eligibility, newest_suspend_snapshot_current,
    started_repair_revert_eligibility, suspended_k8s_finalization_sql,
};
use sqlx::{Postgres, Transaction};

/// `make_interval(secs => $n::BIGINT)` needs an i64 bind. Saturate rather
/// than wrap: any value above i64::MAX means "never act" in human terms.
fn duration_secs_i64(duration: PositiveDuration) -> i64 {
    i64::try_from(duration.get().as_secs()).unwrap_or(i64::MAX)
}

/// The row-state guards shared by the started-repair scan and
/// [`revert_started_to_suspended`]: an aged `started` Kubernetes row with no
/// active execution and no execution that still carries a `pod_ip` (a row
/// that could describe a live route). The snapshot eligibility is composed in
/// separately. `managed_by_param` is bound to [`SubstrateKind::Kubernetes`].
///
/// Public only because the caller's candidate scan composes the same guards;
/// it stays one definition so the scan and the apply cannot drift.
pub fn started_repair_row_state_guards(managed_by_param: &str, min_age_param: &str) -> String {
    format!(
        "s.managed_by = {managed_by_param}
         AND s.lifecycle = 'started'
         AND s.lifecycle_changed_at <= NOW() - make_interval(secs => {min_age_param}::BIGINT)
         AND NOT EXISTS (
             SELECT 1
               FROM sandbox_k8s_executions e
              WHERE e.sandbox_id = s.id
                AND e.active
         )
         AND NOT EXISTS (
             SELECT 1
               FROM sandbox_k8s_executions e
              WHERE e.sandbox_id = s.id
                AND e.pod_ip IS NOT NULL
         )"
    )
}

/// **Inference.** Revert an aged `started` Kubernetes sandbox with no active
/// execution and no execution carrying a `pod_ip` to `suspended`, clearing the
/// current local artifact marker when it is stale. Returns `Some(cleared_marker)` when the row flipped and `None`
/// when a guard refused it.
///
/// The `WHERE` re-verifies every scan condition
/// ([`started_repair_row_state_guards`] and
/// [`started_repair_revert_eligibility`]) and asserts the newest execution
/// generation is still `newest_generation` (0 for none): a raced start
/// allocates a higher generation, so the CAS matches zero rows. The `pre` CTE
/// re-derives the marker-clear decision at apply time.
///
/// The generation CAS is sound only under the caller's sandbox row lock
/// (`SELECT ... FOR UPDATE`, in its own earlier statement of the same
/// transaction): the subqueries here evaluate on this statement's snapshot,
/// and a lock conflict on the sandbox row re-checks only the row's own
/// quals, so without the pre-lock a newly committed generation could go
/// unseen.
pub async fn revert_started_to_suspended(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    min_age: PositiveDuration,
    newest_generation: i64,
) -> Result<Option<bool>, sqlx::Error> {
    let eligibility = started_repair_revert_eligibility("s");
    let artifact_marker_assignments =
        clear_current_local_artifact_marker_assignments("s", "pre.will_clear");
    let finalization = suspended_k8s_finalization_sql(
        "s",
        "pre",
        &artifact_marker_assignments,
        SuspendedTargetNodeAssignment::Clear,
    );
    let query = format!(
        "WITH pre AS (
                 SELECT id,
                        {clear_marker} AS will_clear
                   FROM sandboxes s
                  WHERE s.id = $1
             )
             UPDATE sandboxes s
                SET {sandbox_assignments}
               FROM pre
              WHERE s.id = pre.id
                AND {guards}
                AND (SELECT COALESCE(MAX(e.generation), 0)
                       FROM sandbox_k8s_executions e
                      WHERE e.sandbox_id = s.id) = $3
                AND {eligible}
            RETURNING pre.will_clear",
        clear_marker = eligibility.clear_marker,
        sandbox_assignments = finalization.sandbox_assignments(),
        guards = started_repair_row_state_guards("$4", "$2"),
        eligible = eligibility.eligible,
    );
    sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id)
        .bind(duration_secs_i64(min_age))
        .bind(newest_generation)
        .bind(SubstrateKind::Kubernetes)
        .fetch_optional(&mut **tx)
        .await
}

/// The row-state guards shared by the failed-restorable scan and
/// [`revert_failed_to_suspended`]. The snapshot eligibility is composed in
/// separately (`failed_repair_revert_eligibility`); everything here is the
/// "provably inactive and safe to touch" half of the predicate.
/// The authoritative execution guards below remain shared by scan and apply.
///
/// Deferring on an observed row is the live-VM guard: a cleanup failure can
/// retire the execution row and drop pod tracking while the pod's VM
/// survives, and the only durable evidence is the node still publishing the
/// sandbox. Flipping such a row to `suspended` would let a later wake restore
/// a second VM beside the live one.
///
/// One shape is carved out: a `failed`, pid-less record on an **unassigned**
/// sandbox. Observed-state write authority is placement-scoped — the publish
/// path accepts a node's report only while `active_node_id` points at it — so
/// such a record is a fossil written in the brief window the failing restore
/// held the assignment; after the release no publisher can update or delete
/// it, and it would defer the repair forever while the node's re-publishes
/// bounce off the fence. The caller of [`revert_failed_to_suspended`] deletes
/// it in the same transaction, after the flip; that statement does not.
/// Anything live-looking (a non-`failed` status, a pid, or a sandbox still
/// assigned to a node whose next publish could race the purge) keeps
/// deferring.
///
/// The lease clause makes the age/lease relationship structural: the apply
/// clears the restore lease, which is only safe once the failed attempt's
/// lease has expired — previously implied by the repair minimum age exceeding
/// the lease TTL.
///
/// `managed_by_param` is bound to [`SubstrateKind::Kubernetes`]. Public only
/// because the caller's candidate scan composes the same guards.
pub fn failed_repair_row_state_guards(managed_by_param: &str, min_age_param: &str) -> String {
    format!(
        "s.managed_by = {managed_by_param}
         AND s.lifecycle = 'failed'
         AND s.lifecycle_changed_at <= NOW() - make_interval(secs => {min_age_param}::BIGINT)
         AND (s.restore_lease_expires_at IS NULL OR s.restore_lease_expires_at < NOW())
         AND NOT EXISTS (
             SELECT 1 FROM sandbox_k8s_executions e
              WHERE e.sandbox_id = s.id AND (e.active OR e.pod_ip IS NOT NULL)
         )
         AND NOT EXISTS (
             SELECT 1 FROM sandbox_observed_states o
              WHERE o.sandbox_id = s.id
                AND (o.status IS DISTINCT FROM 'failed'
                     OR o.pid IS NOT NULL
                     OR s.active_node_id IS NOT NULL)
         )"
    )
}

/// **Inference.** Revert a `failed` Kubernetes sandbox with a restorable
/// suspend snapshot to `suspended`, clearing a stale current local artifact
/// marker and the expired restore lease. Returns `Some(cleared_marker)` when
/// the row flipped and `None` when a guard refused it.
///
/// One atomic statement: every scan guard
/// ([`failed_repair_row_state_guards`] and
/// [`failed_repair_revert_eligibility`]) is re-verified in the `WHERE`, so a
/// row that changed since the scan matches zero rows. The `pre` CTE
/// re-derives the marker-clear decision under the same snapshot. Clearing
/// the restore lease is safe because the guards require any lease from the
/// failed restore attempt to have expired; leaving it would only delay the
/// first wake after repair.
///
/// The guards admit a row that still has a `failed`, pid-less
/// `sandbox_observed_states` record (see [`failed_repair_row_state_guards`]).
/// This statement leaves it in place: the caller deletes it in the same
/// transaction, after the flip, or the API keeps reporting the sandbox as
/// failed.
pub async fn revert_failed_to_suspended(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    min_age: PositiveDuration,
) -> Result<Option<bool>, sqlx::Error> {
    let eligibility = failed_repair_revert_eligibility("s");
    let marker_assignments = clear_current_local_artifact_marker_assignments("s", "pre.will_clear");
    let finalization = suspended_k8s_finalization_sql(
        "s",
        "pre",
        &marker_assignments,
        SuspendedTargetNodeAssignment::Clear,
    );
    let query = format!(
        "WITH pre AS (
                 SELECT id, {clear} AS will_clear
                   FROM sandboxes s
                  WHERE s.id = $1
             )
             UPDATE sandboxes s
                SET {sandbox_assignments},
                    restore_lease_expires_at = NULL,
                    restore_lease_token = NULL
               FROM pre
              WHERE s.id = pre.id
                AND {guards}
                AND {eligible}
            RETURNING pre.will_clear",
        clear = eligibility.clear_marker,
        sandbox_assignments = finalization.sandbox_assignments(),
        guards = failed_repair_row_state_guards("$3", "$2"),
        eligible = eligibility.eligible,
    );
    sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id)
        .bind(duration_secs_i64(min_age))
        .bind(SubstrateKind::Kubernetes)
        .fetch_optional(&mut **tx)
        .await
}

/// **Inference.** Project a quarantined sandbox whose failed suspend
/// publication was abandoned back to `suspended` and unplaced, with
/// `artifact_id` (the ready suspend snapshot's artifact) as its current
/// local artifact marker.
///
/// No guard of its own: the `WHERE` matches on `id` alone. It relies on the
/// caller having locked the sandbox, quarantine, execution and snapshot rows
/// and re-run the candidate query under those locks in the same transaction.
/// It also assigns `updated_at` itself, although a trigger maintains it.
pub async fn recover_quarantined_publication_to_suspended(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    artifact_id: LocalArtifactId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspended', placement = 'unplaced',
             active_node_id = NULL, target_node_id = NULL,
             lifecycle_changed_at = NOW(), placement_changed_at = NULL, updated_at = NOW(),
             current_local_artifact_id = $2, current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend', command_ready_pending = FALSE,
             restore_lease_expires_at = NULL, restore_lease_token = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// **Inference.** Unplace a `suspended` sandbox whose stale active execution
/// the caller just retired: `target_node_id` is backfilled from
/// `active_node_id` when unset (so the wake-affinity pin is unchanged),
/// `placement_changed_at` is deliberately not bumped, and the current local
/// artifact marker is cleared. Returns whether the row was projected.
///
/// Guarded only by [`newest_suspend_snapshot_current`], the proof that
/// markerless restore would select a ready durable full suspend snapshot for
/// this lifecycle cycle. There is no lifecycle predicate of its own: it
/// relies on the caller's sandbox row lock and its `lifecycle = 'suspended'`
/// re-check under that lock, in the same transaction.
pub async fn unplace_suspended_after_execution_retired(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    let markerless_snapshot_current = newest_suspend_snapshot_current("s");
    let query = format!(
        "UPDATE sandboxes s
                SET placement = 'unplaced',
                    target_node_id = COALESCE(target_node_id, active_node_id),
                    active_node_id = NULL,
                    current_local_artifact_id = NULL,
                    current_local_artifact_kind = NULL,
                    current_local_artifact_origin = NULL
              WHERE s.id = $1
                AND ({markerless_snapshot_current})"
    );
    let projected = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id)
        .execute(&mut **tx)
        .await?
        .rows_affected()
        > 0;
    Ok(projected)
}
