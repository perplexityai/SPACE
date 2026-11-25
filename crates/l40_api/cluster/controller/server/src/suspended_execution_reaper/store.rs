//! Production [`RetireSink`] backed by Postgres.

use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;

use pplx_space_durations::PositiveDuration;
use pplx_space_sandbox_store::k8s_executions::suspended_reaper;
use pplx_space_sandbox_store::repair::unplace_suspended_after_execution_retired;
use pplx_space_sandbox_store::row_locks;
use sqlx::PgPool;

use super::{RetireOutcome, RetireSink, StaleExecutionCandidate};

pub struct ExecutionDbStore {
    pool: PgPool,
}

impl ExecutionDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
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
        let rows = pplx_space_sandbox_store::stale_suspended_executions::list(
            &self.pool,
            batch_size,
            after_sandbox_id,
            upper_bound,
            min_age,
        )
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| StaleExecutionCandidate {
                sandbox_id: row.sandbox_id,
                generation: row.generation,
                pod_name: row.pod_name,
                pod_uid: row.pod_uid,
                execution_state: row.execution_state,
                active_node_id: row.active_node_id,
                current_local_artifact_id: row.current_local_artifact_id,
            })
            .collect())
    }

    async fn current_max_sandbox_id(&self) -> Result<Option<SandboxId>, sqlx::Error> {
        Ok(
            pplx_space_sandbox_store::sandbox_id_bounds::newest_managed_by(
                &self.pool,
                SubstrateKind::Kubernetes,
            )
            .await?,
        )
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

        let lifecycle = row_locks::lock_lifecycle(&mut tx, candidate.sandbox_id).await?;
        if lifecycle.as_deref() != Some("suspended") {
            tx.rollback().await?;
            return Ok(RetireOutcome::SkippedRaced);
        }

        let retired = suspended_reaper::retire_stale_execution(
            &mut *tx,
            candidate.sandbox_id,
            candidate.generation,
            &candidate.pod_name,
            candidate.pod_uid.as_deref(),
        )
        .await?;
        if !retired {
            tx.rollback().await?;
            return Ok(RetireOutcome::SkippedRaced);
        }

        pplx_space_sandbox_store::port_exposures::invalidate_open_observations(
            &mut *tx,
            candidate.sandbox_id,
        )
        .await?;

        let projected =
            unplace_suspended_after_execution_retired(&mut tx, candidate.sandbox_id).await?;
        if !projected {
            tx.rollback().await?;
            return Ok(RetireOutcome::SkippedSnapshotNotCurrent);
        }

        tx.commit().await?;
        Ok(RetireOutcome::Retired)
    }
}
