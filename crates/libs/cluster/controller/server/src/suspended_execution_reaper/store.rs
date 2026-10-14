//! Production [`RetireSink`] backed by Postgres.

use pplx_space_common_core::types::SandboxId;
use pplx_space_common_core::types::{LocalArtifactId, NodeId, SubstrateKind};

use pplx_space_common_core::sql;
use pplx_space_common_core::types::PositiveDuration;
use pplx_space_sandbox_portability::newest_suspend_snapshot_current;
use sqlx::PgPool;

use super::{RetireOutcome, RetireSink, StaleExecutionCandidate};

/// Row shape of the candidate scan: `(sandbox_id, generation, pod_name,
/// pod_uid, execution_state, active_node_id, current_local_artifact_id)`.
type CandidateRow = (
    SandboxId,
    i64,
    String,
    Option<String>,
    String,
    Option<NodeId>,
    Option<LocalArtifactId>,
);

pub struct ExecutionDbStore {
    pool: PgPool,
}

impl ExecutionDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// `make_interval(secs => $n::BIGINT)` needs an i64 bind. Saturate rather
/// than wrap: any value above i64::MAX means "never act" in human terms.
fn min_age_secs_i64(min_age: PositiveDuration) -> i64 {
    i64::try_from(min_age.get().as_secs()).unwrap_or(i64::MAX)
}

impl RetireSink for ExecutionDbStore {
    /// Candidate scan: suspended sandboxes that still carry an active
    /// execution row and have a durable full suspend snapshot that the
    /// markerless restore selector would choose after repair. Both the
    /// suspend itself (`lifecycle_changed_at`) and the execution row
    /// (`created_at`) must be older than `min_age`: the former keeps the
    /// scan clear of the suspend-publish convergence window, the latter
    /// fences a freshly inserted execution claim whose pod creation is still
    /// in flight (the claim is inserted before the pod is POSTed, so a young
    /// row with a missing pod can be healthy).
    async fn list_candidates(
        &self,
        batch_size: u32,
        after_sandbox_id: Option<SandboxId>,
        upper_bound: SandboxId,
        min_age: PositiveDuration,
    ) -> Result<Vec<StaleExecutionCandidate>, sqlx::Error> {
        let markerless_snapshot_current = newest_suspend_snapshot_current("s");
        let query = format!(
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
        );
        let rows: Vec<CandidateRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(min_age_secs_i64(min_age))
            .bind(after_sandbox_id)
            .bind(upper_bound)
            .bind(i64::from(batch_size))
            .bind(SubstrateKind::Kubernetes)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    sandbox_id,
                    generation,
                    pod_name,
                    pod_uid,
                    execution_state,
                    active_node_id,
                    current_local_artifact_id,
                )| {
                    StaleExecutionCandidate {
                        sandbox_id,
                        generation,
                        pod_name,
                        pod_uid,
                        execution_state,
                        active_node_id,
                        current_local_artifact_id,
                    }
                },
            )
            .collect())
    }

    async fn current_max_sandbox_id(&self) -> Result<Option<SandboxId>, sqlx::Error> {
        // Pre-PG-15 has no `max(uuid)` aggregate, so ORDER BY DESC LIMIT 1
        // over the primary key index instead.
        let row: Option<(SandboxId,)> = sqlx::query_as(sql!(
            "SELECT id FROM sandboxes WHERE managed_by = $1 ORDER BY id DESC LIMIT 1"
        ))
        .bind(SubstrateKind::Kubernetes)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(id,)| id))
    }

    /// One transaction, mirroring the API-driven suspend's
    /// `finish_suspended_execution` bookkeeping but only after re-proving a
    /// durable full suspend artifact that markerless restore will select:
    ///
    /// 1. Lock the sandbox row `FOR UPDATE` — the same lock execution
    ///    claims take — and re-check `lifecycle = 'suspended'` under it,
    ///    so the whole transaction is serialized against a concurrent
    ///    wake's `suspended -> restoring` CAS and against claim inserts.
    /// 2. Retire the execution row, guarded on the exact
    ///    `(generation, pod_name, pod_uid)` the scan observed and the pod
    ///    check verified missing.
    /// 3. Invalidate open port-exposure observations (the node state they
    ///    described died with the pod).
    /// 4. Clear the compat pod IP.
    /// 5. Mark the sandbox unplaced, preserving the wake-affinity pin:
    ///    `target_node_id` is backfilled from `active_node_id` when unset
    ///    so `COALESCE(target_node_id, active_node_id)` is unchanged.
    ///    `placement_changed_at` is deliberately NOT bumped (the legacy
    ///    snapshot-portability branch compares snapshot timestamps against
    ///    it). Artifact identity columns are cleared: preserving a marker
    ///    would require node-local evidence that the marker names this exact
    ///    suspend cycle's artifact, and this reaper only has Postgres/S3
    ///    evidence. Before clearing the marker, the transaction re-proves
    ///    that markerless restore would select a ready durable full suspend
    ///    snapshot for this lifecycle cycle.
    async fn retire_stale_execution(
        &self,
        candidate: &StaleExecutionCandidate,
    ) -> Result<RetireOutcome, sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        let lifecycle: Option<String> = sqlx::query_scalar(sql!(
            "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
        ))
        .bind(candidate.sandbox_id)
        .fetch_optional(&mut *tx)
        .await?;
        if lifecycle.as_deref() != Some("suspended") {
            tx.rollback().await?;
            return Ok(RetireOutcome::SkippedRaced);
        }

        let retired = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET active = FALSE,
                    state = 'suspended',
                    pod_ip = NULL,
                    terminated_at = COALESCE(terminated_at, NOW()),
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND active
                AND generation = $2
                AND pod_name = $3
                AND pod_uid IS NOT DISTINCT FROM $4"
        ))
        .bind(candidate.sandbox_id)
        .bind(candidate.generation)
        .bind(&candidate.pod_name)
        .bind(&candidate.pod_uid)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            > 0;
        if !retired {
            tx.rollback().await?;
            return Ok(RetireOutcome::SkippedRaced);
        }

        sqlx::query(sql!(
            "UPDATE sandbox_port_exposures
                SET apply_state = 'pending',
                    last_applied_node_id = NULL,
                    last_error = NULL,
                    last_bridge_status = NULL,
                    last_applied_bridge_generation = NULL,
                    last_observed_at = NULL,
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND desired_state = 'open'"
        ))
        .bind(candidate.sandbox_id)
        .execute(&mut *tx)
        .await?;

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
            .bind(candidate.sandbox_id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            > 0;
        if !projected {
            tx.rollback().await?;
            return Ok(RetireOutcome::SkippedSnapshotNotCurrent);
        }

        tx.commit().await?;
        Ok(RetireOutcome::Retired)
    }
}
