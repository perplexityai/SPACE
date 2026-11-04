//! Production [`ReaperSink`] backed by Postgres.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::PositiveDuration;
use pplx_space_common_core::types::{NodeId, SandboxId, SnapshotId};
use sqlx::PgPool;

use super::{REAP_FAILURE_REASON, ReapedSnapshot, ReaperSink};

pub struct SnapshotDbStore {
    pool: PgPool,
}

impl SnapshotDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl ReaperSink for SnapshotDbStore {
    /// Atomic CAS reap: flip every `type = 'full'` suspend-snapshot row in
    /// `state = 'creating'` whose `timestamp < NOW() - $min_age` to
    /// `state = 'failed'`, stamping `failure_reason` only when it is still
    /// NULL (so a reason the worker recorded on an earlier best-effort
    /// attempt is preserved). The `RETURNING` clause makes the match and
    /// UPDATE a single statement, so a row the on-node worker finalized
    /// under us — to `ready` on a late-but-successful upload, or to `failed`
    /// on its own give-up — drops out of the `state = 'creating'` predicate
    /// and is left untouched.
    ///
    /// Scope is `type = 'full'`, which **no longer matches the set the node-drain
    /// in-flight gates key on.** It was chosen to mirror
    /// `inflight_suspend_upload_exists`, and that is still the suspend side's
    /// gate — but the stopped-resident evacuation added a second one, and it
    /// keys on `creating` **disk** rows (`node_drain::store`'s
    /// `StoppedEvacuationState::upload_in_flight`, consumed at step 1b of
    /// `evacuate_stopped_sandbox`).
    ///
    /// So a stale `creating` disk row has no reaper: this statement will not
    /// touch it and the retention sweeper filters `type = 'full'` too. The
    /// consequence is documented where it bites — that gate parks an evacuation
    /// instead of retrying it, fails stuck rather than open, and shows up on
    /// `node_drain.stopped_evacuation_stall_secs`. **Do not read the exclusion
    /// below as still being sufficient**; whether this scope should widen is an
    /// open question.
    ///
    /// Ages on `COALESCE(last_attempt_at, timestamp)` — last observed worker
    /// activity, falling back to the immutable creation stamp. `last_attempt_at`
    /// is bumped by the on-node upload worker at the start of each retry
    /// attempt, so a node outage that pauses the worker's retry clock also
    /// pauses the reaper's clock: an upload that recovers and resumes after a
    /// long downtime keeps a recent heartbeat and is never reaped mid-flight.
    /// Until the heartbeat writer exists the column is NULL and this collapses
    /// to the creation-time anchor (see the module docs).
    async fn reap_stuck_creating(
        &self,
        min_age: PositiveDuration,
    ) -> Result<Vec<ReapedSnapshot>, sqlx::Error> {
        // `make_interval(secs => $1::BIGINT)` needs an i64 bind. Saturate
        // rather than wrap: any value above i64::MAX (~2.9e11 years) means
        // "never reap" in human terms, which matches what an operator typing
        // a huge minimum age was expressing.
        let secs = min_age.get().as_secs();
        debug_assert!(
            secs <= i64::MAX as u64,
            "min_age {secs}s exceeds i64::MAX; will saturate to i64::MAX (effectively disabling the reaper)"
        );
        let min_age_secs_i64 = i64::try_from(secs).unwrap_or(i64::MAX);
        let rows: Vec<(SnapshotId, SandboxId, Option<NodeId>)> = sqlx::query_as(sql!(
            "UPDATE snapshots
                SET state = 'failed',
                    failure_reason = COALESCE(failure_reason, $2)
              WHERE state = 'creating'
                AND type = 'full'
                AND COALESCE(last_attempt_at, timestamp) < NOW() - make_interval(secs => $1::BIGINT)
              RETURNING id, sandbox_id, source_node_id"
        ))
        .bind(min_age_secs_i64)
        .bind(REAP_FAILURE_REASON)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(snapshot_id, sandbox_id, source_node_id)| ReapedSnapshot {
                snapshot_id,
                sandbox_id,
                source_node_id,
            })
            .collect())
    }
}
