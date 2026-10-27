//! Production [`LostStartedSink`] backed by Postgres.
//!
//! Scan and apply state the same conditions; the apply re-checks them under
//! the sandbox row lock, taken first as the API's execution mutations take it,
//! so a concurrent recovery claim either lands before the check or waits for
//! the flip. The two portability fragments are interpolated whole; everything
//! else is spelled at its query site.

use std::sync::LazyLock;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{SandboxId, SubstrateKind};
use pplx_space_sandbox_portability::{
    failed_repair_revert_eligibility, inflight_suspend_upload_exists,
};
use sqlx::PgPool;

use super::{LostStartedCandidate, LostStartedSink};

/// The cohort CTE is an optimizer fence: the few `recovery_failed` executions
/// bound the scan before the snapshot subqueries run.
static LIST_CANDIDATES_SQL: LazyLock<String> = LazyLock::new(|| {
    let restorable = failed_repair_revert_eligibility("s").eligible;
    let inflight = inflight_suspend_upload_exists("s");
    format!(
        "WITH cohort AS MATERIALIZED (
             SELECT s.id,
                    s.current_local_artifact_id,
                    s.current_local_artifact_kind,
                    s.current_local_artifact_origin,
                    s.last_command_at,
                    s.last_access_at,
                    s.lifecycle_changed_at,
                    s.placement_changed_at,
                    e.generation AS recovery_generation,
                    lost.k8s_node_name AS lost_node_name
               FROM sandbox_k8s_executions e
               JOIN sandboxes s ON s.id = e.sandbox_id
               JOIN LATERAL (
                    SELECT x.k8s_node_name, x.terminated_at
                      FROM sandbox_k8s_executions x
                     WHERE x.sandbox_id = e.sandbox_id
                       AND x.generation < e.generation
                       AND x.k8s_node_name IS NOT NULL
                     ORDER BY x.generation DESC
                     LIMIT 1
               ) lost ON TRUE
              WHERE e.active
                AND e.state = 'recovery_failed'
                AND e.pod_uid IS NULL
                AND e.pod_ip IS NULL
                AND e.k8s_node_name IS NULL
                AND s.managed_by = $1
                AND s.lifecycle = 'started'
                AND s.placement = 'unplaced'
                AND s.active_node_id IS NULL
                AND s.target_node_id IS NULL
                AND lost.terminated_at <= NOW() - make_interval(secs => $2::BIGINT)
                AND ($3::uuid IS NULL OR s.id > $3)
         )
         SELECT s.id, s.recovery_generation, s.lost_node_name
           FROM cohort s
          WHERE NOT {restorable}
            AND NOT {inflight}
          ORDER BY s.id
          LIMIT $4"
    )
});

static MARK_LOST_SQL: LazyLock<String> = LazyLock::new(|| {
    let restorable = failed_repair_revert_eligibility("s").eligible;
    let inflight = inflight_suspend_upload_exists("s");
    format!(
        "UPDATE sandboxes s
            SET lifecycle = 'failed'
          WHERE s.id = $1
            AND s.managed_by = $4
            AND s.lifecycle = 'started'
            AND s.placement = 'unplaced'
            AND s.active_node_id IS NULL
            AND s.target_node_id IS NULL
            AND EXISTS (
                SELECT 1
                  FROM sandbox_k8s_executions e
                 WHERE e.sandbox_id = s.id
                   AND e.generation = $2
                   AND e.active
                   AND e.state = 'recovery_failed'
                   AND e.pod_uid IS NULL
                   AND e.pod_ip IS NULL
                   AND e.k8s_node_name IS NULL
            )
            AND (
                SELECT x.k8s_node_name
                  FROM sandbox_k8s_executions x
                 WHERE x.sandbox_id = s.id
                   AND x.generation < $2
                   AND x.k8s_node_name IS NOT NULL
                 ORDER BY x.generation DESC
                 LIMIT 1
            ) = $3
            AND NOT {restorable}
            AND NOT {inflight}
          RETURNING s.id"
    )
});

pub struct LostStartedDbStore {
    pool: PgPool,
}

impl LostStartedDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl LostStartedSink for LostStartedDbStore {
    async fn list_candidates(
        &self,
        limit: i64,
        after: Option<SandboxId>,
        min_age: Duration,
    ) -> Result<Vec<LostStartedCandidate>, sqlx::Error> {
        let rows: Vec<(SandboxId, i64, String)> =
            sqlx::query_as(sqlx::AssertSqlSafe(sql!(LIST_CANDIDATES_SQL.as_str())))
                .bind(SubstrateKind::Kubernetes)
                .bind(i64::try_from(min_age.as_secs()).unwrap_or(i64::MAX))
                .bind(after)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows
            .into_iter()
            .map(
                |(sandbox_id, recovery_generation, lost_node_name)| LostStartedCandidate {
                    sandbox_id,
                    recovery_generation,
                    lost_node_name,
                },
            )
            .collect())
    }

    async fn mark_lost(&self, candidate: &LostStartedCandidate) -> Result<bool, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let locked: Option<SandboxId> =
            sqlx::query_scalar(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
                .bind(candidate.sandbox_id)
                .fetch_optional(&mut *tx)
                .await?;
        if locked.is_none() {
            return Ok(false);
        }
        let failed: Option<SandboxId> =
            sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(MARK_LOST_SQL.as_str())))
                .bind(candidate.sandbox_id)
                .bind(candidate.recovery_generation)
                .bind(&candidate.lost_node_name)
                .bind(SubstrateKind::Kubernetes)
                .fetch_optional(&mut *tx)
                .await?;
        if failed.is_none() {
            return Ok(false);
        }
        // Retire the refused recovery in the same transaction: a `failed` row
        // that keeps an active execution is the failed-sandbox cleanup's
        // teardown work, and this one has no runtime left to tear down.
        let retired = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET active = FALSE,
                    state = 'forgotten',
                    terminated_at = COALESCE(terminated_at, NOW()),
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND generation = $2
                AND active
                AND state = 'recovery_failed'"
        ))
        .bind(candidate.sandbox_id)
        .bind(candidate.recovery_generation)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if retired != 1 {
            return Ok(false);
        }
        // The lost node's last report still says the sandbox is running, and
        // ECW reads prefer a node's observation over the lifecycle. The node is
        // confirmed gone, so the report cannot be current.
        sqlx::query(sql!(
            "DELETE FROM sandbox_observed_states o
              USING node_k8s nk
              WHERE o.sandbox_id = $1
                AND o.node_id = nk.node_id
                AND nk.k8s_node_name = $2"
        ))
        .bind(candidate.sandbox_id)
        .bind(&candidate.lost_node_name)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }
}
