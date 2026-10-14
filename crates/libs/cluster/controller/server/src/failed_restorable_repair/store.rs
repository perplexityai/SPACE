//! Postgres store for the failed-restorable repair loop.

use pplx_space_common_core::types::LocalArtifactId;
use pplx_space_common_core::types::SandboxId;
use pplx_space_common_core::types::SubstrateKind;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::PositiveDuration;
use pplx_space_sandbox_portability::{
    SuspendedTargetNodeAssignment, clear_current_local_artifact_marker_assignments,
    failed_repair_revert_eligibility, suspended_k8s_finalization_sql,
};
use sqlx::PgPool;

use super::{ApplyOutcome, RepairCandidate};

pub struct RepairDbStore {
    pool: PgPool,
}

/// The row-state guards shared by scan and apply. The snapshot eligibility is
/// composed in separately (`failed_repair_revert_eligibility`); everything
/// here is the "provably inactive and safe to touch" half of the predicate.
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
/// bounce off the fence. The apply purges it with the flip. Anything
/// live-looking (a non-`failed` status, a pid, or a sandbox still assigned to
/// a node whose next publish could race the purge) keeps deferring.
///
/// The lease clause makes the age/lease relationship structural: the apply
/// clears the restore lease, which is only safe once the failed attempt's
/// lease has expired — previously implied by the repair minimum age exceeding
/// the lease TTL.
///
/// `managed_by_param` is bound to [`SubstrateKind::Kubernetes`].
fn row_state_guards(managed_by_param: &str, min_age_param: &str) -> String {
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

impl RepairDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Keyset page ordered by `s.id`: candidates strictly after
    /// `after_sandbox_id` (None starts a sweep). Advancing the cursor
    /// regardless of per-row outcomes is what lets dry-run enumerate the
    /// whole cohort and keeps a batch of persistent racers/errors from
    /// starving the rows behind it.
    pub async fn list_candidates(
        &self,
        batch_size: u32,
        after_sandbox_id: Option<SandboxId>,
        min_age: PositiveDuration,
    ) -> Result<Vec<RepairCandidate>, sqlx::Error> {
        let eligibility = failed_repair_revert_eligibility("s");
        let guards = row_state_guards("$4", "$1");
        let query = format!(
            "SELECT s.id, s.current_local_artifact_id, {clear} AS clear_marker
               FROM sandboxes s
              WHERE {guards}
                AND {eligible}
                AND ($2::uuid IS NULL OR s.id > $2)
              ORDER BY s.id
              LIMIT $3",
            clear = eligibility.clear_marker,
            eligible = eligibility.eligible,
        );
        let rows: Vec<(SandboxId, Option<LocalArtifactId>, bool)> =
            sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(duration_secs_i64(min_age))
                .bind(after_sandbox_id)
                .bind(i64::from(batch_size))
                .bind(SubstrateKind::Kubernetes)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows
            .into_iter()
            .map(
                |(sandbox_id, current_local_artifact_id, clear_marker)| RepairCandidate {
                    sandbox_id,
                    current_local_artifact_id,
                    clear_marker,
                },
            )
            .collect())
    }

    pub async fn revert_failed_to_suspended(
        &self,
        candidate: &RepairCandidate,
        min_age: PositiveDuration,
    ) -> Result<ApplyOutcome, sqlx::Error> {
        // Single atomic statement; every guard is re-verified inside the
        // UPDATE, so a row that changed since scan time simply matches zero
        // rows (CAS lost). The `pre` CTE re-derives the marker-clear decision
        // under the same snapshot the UPDATE sees, and `RETURNING` reports
        // what actually happened. Clearing the restore lease is safe here:
        // the guards require any lease from the failed restore attempt to
        // have expired; leaving it would only delay the first wake after
        // repair.
        let eligibility = failed_repair_revert_eligibility("s");
        let marker_assignments =
            clear_current_local_artifact_marker_assignments("s", "pre.will_clear");
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
            guards = row_state_guards("$3", "$2"),
            eligible = eligibility.eligible,
        );
        let mut tx = self.pool.begin().await?;
        let cleared_marker: Option<bool> =
            sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(candidate.sandbox_id)
                .bind(duration_secs_i64(min_age))
                .bind(SubstrateKind::Kubernetes)
                .fetch_optional(&mut *tx)
                .await?;
        let Some(cleared_marker) = cleared_marker else {
            tx.rollback().await?;
            return Ok(ApplyOutcome::SkippedRaced);
        };

        // Purge the tolerated fossil projection with the flip (see
        // `row_state_guards`): the API overlays observed status over the DB
        // lifecycle, so leaving it would keep the sandbox looking failed
        // after the repair, and no publisher can correct or remove it —
        // unassigned sandboxes are unpublishable. The shape is re-pinned so
        // a projection that turned live since the guard ran survives (the
        // CAS would have refused the flip in that case anyway).
        sqlx::query(sql!(
            "DELETE FROM sandbox_observed_states
              WHERE sandbox_id = $1 AND status = 'failed' AND pid IS NULL"
        ))
        .bind(candidate.sandbox_id)
        .execute(&mut *tx)
        .await?;

        let query = format!(
            "UPDATE sandbox_port_exposures
                SET {assignments}
              WHERE sandbox_id = $1 AND desired_state = 'open'",
            assignments = finalization.open_port_exposure_assignments(),
        );
        sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(candidate.sandbox_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(if cleared_marker {
            ApplyOutcome::RevertedToSuspendedMarkerCleared
        } else {
            ApplyOutcome::RevertedToSuspended
        })
    }
}

fn duration_secs_i64(duration: PositiveDuration) -> i64 {
    i64::try_from(duration.get().as_secs()).unwrap_or(i64::MAX)
}
