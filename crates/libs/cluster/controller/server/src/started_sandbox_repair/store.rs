//! Production [`RepairSink`] backed by Postgres.

use pplx_space_common_core::types::LocalArtifactId;
use pplx_space_common_core::types::SandboxId;
use pplx_space_common_core::types::SubstrateKind;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::PositiveDuration;
use pplx_space_sandbox_portability::{
    SuspendedK8sFinalizationSql, SuspendedTargetNodeAssignment,
    clear_current_local_artifact_marker_assignments, started_repair_revert_eligibility,
    suspended_k8s_finalization_sql,
};
use sqlx::PgPool;

use super::{ApplyOutcome, NewestExecutionRow, RepairCandidate, RepairSink};

type ExecutionCandidateRow = (
    SandboxId,
    Option<String>,
    Option<i64>,
    Option<LocalArtifactId>,
    bool,
);

pub struct RepairDbStore {
    pool: PgPool,
}

impl RepairDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn duration_secs_i64(duration: PositiveDuration) -> i64 {
    i64::try_from(duration.get().as_secs()).unwrap_or(i64::MAX)
}

/// The row-state guards shared by scan and apply: an aged
/// `started` Kubernetes row with no active execution and no execution that
/// still carries a `pod_ip` (a row that could describe a live route). The
/// snapshot eligibility is composed in separately. `managed_by_param` is bound
/// to [`SubstrateKind::Kubernetes`].
fn row_state_guards(managed_by_param: &str, min_age_param: &str) -> String {
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

impl RepairDbStore {
    /// The newest `sandbox_k8s_executions` row supplies the pod identity to probe.
    ///
    /// The `MATERIALIZED` CTE is an optimizer fence, not style: it forces the
    /// cheap row-state guards (lifecycle, age, the two execution `NOT EXISTS`)
    /// to fully reduce the cohort before the snapshot-eligibility subqueries
    /// run. Written as one flat WHERE, the planner satisfies `ORDER BY s.id
    /// LIMIT` by walking the `sandboxes` primary key across the whole table
    /// and evaluates the eligibility subplans on every aged `started` row
    /// before the execution anti-join that eliminates most of them. The fence
    /// avoids that repeated work and holds under
    /// generic (prepared-statement) plans as well.
    ///
    /// The CTE must expose every `sandboxes` column the eligibility fragments
    /// reference; it is aliased `s` so the shared fragments compose unchanged.
    async fn list_execution_candidates(
        &self,
        batch_size: u32,
        after_sandbox_id: Option<SandboxId>,
        upper_bound: SandboxId,
        min_age: PositiveDuration,
    ) -> Result<Vec<RepairCandidate>, sqlx::Error> {
        let eligibility = started_repair_revert_eligibility("s");
        let query = format!(
            r"
            WITH cohort AS MATERIALIZED (
                SELECT s.id,
                       s.current_local_artifact_id,
                       s.current_local_artifact_kind,
                       s.current_local_artifact_origin,
                       s.last_command_at,
                       s.last_access_at,
                       s.lifecycle_changed_at
                  FROM sandboxes s
                 WHERE {guards}
                   AND ($2::uuid IS NULL OR s.id > $2)
                   AND s.id <= $3
            )
            SELECT s.id,
                   newest.pod_name,
                   newest.generation,
                   s.current_local_artifact_id,
                   {clear_marker} AS clear_marker
              FROM cohort s
              LEFT JOIN LATERAL (
                  SELECT e.pod_name, e.generation
                    FROM sandbox_k8s_executions e
                   WHERE e.sandbox_id = s.id
                   ORDER BY e.generation DESC
                   LIMIT 1
              ) newest ON TRUE
             WHERE {eligible}
             ORDER BY s.id
             LIMIT $4
            ",
            clear_marker = eligibility.clear_marker,
            guards = row_state_guards("$5", "$1"),
            eligible = eligibility.eligible,
        );
        let rows: Vec<ExecutionCandidateRow> =
            sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(duration_secs_i64(min_age))
                .bind(after_sandbox_id)
                .bind(upper_bound)
                .bind(i64::from(batch_size))
                .bind(SubstrateKind::Kubernetes)
                .fetch_all(&self.pool)
                .await?;

        Ok(rows
            .into_iter()
            .map(
                |(sandbox_id, pod_name, generation, current_local_artifact_id, clear_marker)| {
                    RepairCandidate {
                        sandbox_id,
                        newest_execution: pod_name.zip(generation).map(|(pod_name, generation)| {
                            NewestExecutionRow {
                                pod_name,
                                generation,
                            }
                        }),
                        current_local_artifact_id,
                        clear_marker,
                    }
                },
            )
            .collect())
    }

    /// A guarded UPDATE re-verifies every scan
    /// condition and additionally asserts the newest execution generation is
    /// unchanged — a raced start allocates a higher generation, so the CAS
    /// simply matches zero rows. The `pre` CTE re-derives the marker-clear
    /// decision at apply time; `RETURNING pre.will_clear` reports what
    /// actually happened.
    ///
    /// Concurrency: the sandbox row is locked (`SELECT ... FOR UPDATE`) in
    /// its own statement before the guarded UPDATE. Execution writers
    /// (wake/restore) hold this row lock while allocating their new
    /// generation, so the pre-lock parks this apply until the writer's
    /// transaction finishes; the UPDATE then evaluates on a fresh snapshot
    /// that sees the committed generation and the CAS refuses the flip.
    /// Without the pre-lock, the UPDATE's subqueries would evaluate on a
    /// statement snapshot taken before the writer committed — a lock
    /// conflict on the sandbox row re-checks only the row's own quals, not
    /// the subqueries — and the CAS could pass over a newly revived sandbox.
    async fn revert_with_execution_identity(
        &self,
        candidate: &RepairCandidate,
        newest_generation: i64,
        min_age: PositiveDuration,
    ) -> Result<ApplyOutcome, sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        let lifecycle: Option<String> = sqlx::query_scalar(sql!(
            "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
        ))
        .bind(candidate.sandbox_id)
        .fetch_optional(&mut *tx)
        .await?;
        if lifecycle.as_deref() != Some("started") {
            tx.rollback().await?;
            return Ok(ApplyOutcome::SkippedRaced);
        }

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
            guards = row_state_guards("$4", "$2"),
            eligible = eligibility.eligible,
        );
        let cleared_marker: Option<bool> =
            sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(candidate.sandbox_id)
                .bind(duration_secs_i64(min_age))
                .bind(newest_generation)
                .bind(SubstrateKind::Kubernetes)
                .fetch_optional(&mut *tx)
                .await?;
        let Some(cleared_marker) = cleared_marker else {
            tx.rollback().await?;
            return Ok(ApplyOutcome::SkippedRaced);
        };

        self.reset_open_port_exposures(&mut tx, candidate.sandbox_id, &finalization)
            .await?;

        tx.commit().await?;
        Ok(revert_outcome(cleared_marker))
    }

    async fn reset_open_port_exposures(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        sandbox_id: SandboxId,
        finalization: &SuspendedK8sFinalizationSql,
    ) -> Result<(), sqlx::Error> {
        let query = format!(
            "UPDATE sandbox_port_exposures
                SET {assignments}
              WHERE sandbox_id = $1
                AND desired_state = 'open'",
            assignments = finalization.open_port_exposure_assignments(),
        );
        sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }
}

fn revert_outcome(cleared_marker: bool) -> ApplyOutcome {
    if cleared_marker {
        ApplyOutcome::RevertedToSuspendedMarkerCleared
    } else {
        ApplyOutcome::RevertedToSuspended
    }
}

impl RepairSink for RepairDbStore {
    async fn list_candidates(
        &self,
        batch_size: u32,
        after_sandbox_id: Option<SandboxId>,
        upper_bound: SandboxId,
        min_age: PositiveDuration,
    ) -> Result<Vec<RepairCandidate>, sqlx::Error> {
        self.list_execution_candidates(batch_size, after_sandbox_id, upper_bound, min_age)
            .await
    }

    async fn current_max_sandbox_id(&self) -> Result<Option<SandboxId>, sqlx::Error> {
        let row: Option<(SandboxId,)> = sqlx::query_as(sql!(
            "SELECT id FROM sandboxes WHERE managed_by = $1 ORDER BY id DESC LIMIT 1"
        ))
        .bind(SubstrateKind::Kubernetes)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(id,)| id))
    }

    async fn revert_started_to_suspended(
        &self,
        candidate: &RepairCandidate,
        min_age: PositiveDuration,
    ) -> Result<ApplyOutcome, sqlx::Error> {
        // No rows at scan time encodes as 0, matching the apply's
        // COALESCE(MAX(generation), 0): the table must still be empty.
        let newest_generation = candidate
            .newest_execution
            .as_ref()
            .map(|row| row.generation)
            .unwrap_or(0);
        self.revert_with_execution_identity(candidate, newest_generation, min_age)
            .await
    }
}
