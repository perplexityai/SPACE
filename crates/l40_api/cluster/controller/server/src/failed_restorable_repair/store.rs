//! Postgres store for the failed-restorable repair loop.

use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::LocalArtifactId;

use pplx_space_durations::PositiveDuration;
use pplx_space_sandbox_portability::failed_repair_revert_eligibility;
use pplx_space_sandbox_store::repair;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;

use super::{ApplyOutcome, RepairCandidate};

pub struct RepairDbStore {
    pool: PgPool,
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
        let guards = repair::failed_repair_row_state_guards("$4", "$1");
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
        // rows (CAS lost). See [`repair::revert_failed_to_suspended`].
        let mut tx = self.pool.begin().await?;
        let Some(cleared_marker) =
            repair::revert_failed_to_suspended(&mut tx, candidate.sandbox_id, min_age).await?
        else {
            tx.rollback().await?;
            return Ok(ApplyOutcome::SkippedRaced);
        };

        // Purge the tolerated fossil projection with the flip (see
        // `failed_repair_row_state_guards`): the API overlays observed status
        // over the DB lifecycle, so leaving it would keep the sandbox looking
        // failed after the repair, and no publisher can correct or remove it —
        // unassigned sandboxes are unpublishable. The shape is re-pinned so
        // a projection that turned live since the guard ran survives (the
        // CAS would have refused the flip in that case anyway).
        pplx_space_sandbox_store::observed_states::clear_failed_observation(
            &mut tx,
            candidate.sandbox_id,
        )
        .await?;

        pplx_space_sandbox_store::port_exposures::invalidate_open_observations(
            &mut *tx,
            candidate.sandbox_id,
        )
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
