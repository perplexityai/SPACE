//! Snapshot uploads still in flight, and their guarded transitions.

use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::{LocalArtifactId, SnapshotId, SnapshotState};
use pplx_space_sql_locator::sql;
use sqlx::FromRow;

/// How long the stalest `creating` snapshot of `sandbox_id` has gone without
/// upload activity, by the database clock, or `None` when none is in flight.
/// Fork snapshots don't count; suspend, stop, and named-snapshot backups all
/// do.
///
/// Activity is `COALESCE(last_attempt_at, "timestamp")`, the anchor the
/// stuck-snapshot reaper ages on. The upload worker reuses one row across
/// its retries and heartbeats `last_attempt_at` at the start of each attempt,
/// so a slow but live upload reads as recent. Nothing refreshes it during an
/// attempt, so one long attempt reads as idle for as long as it runs.
///
/// A plain read: a snapshot inserted after it isn't counted. To make it a
/// race-safe delete gate, take `FOR UPDATE` on the sandbox row first, in the
/// same transaction. That conflicts with the `FOR KEY SHARE` lock a snapshot
/// insert's foreign-key check takes on the row, so no snapshot can be
/// registered between the read and the delete.
pub async fn in_flight_idle_for(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<Duration>, sqlx::Error> {
    let idle_secs: Option<f64> = sqlx::query_scalar(sql!(
        "SELECT EXTRACT(EPOCH FROM NOW() - MIN(COALESCE(last_attempt_at, \"timestamp\")))
                    ::DOUBLE PRECISION
         FROM snapshots
         WHERE sandbox_id = $1
           AND state = 'creating'
           AND artifact_origin IS DISTINCT FROM 'fork'"
    ))
    .bind(sandbox_id)
    .fetch_one(executor)
    .await?;
    Ok(idle_secs.map(|secs| Duration::from_secs_f64(secs.max(0.0))))
}

/// The identity of an upload [`finalize_attempt`] just closed, as its
/// `RETURNING` produces it. `artifact_kind` is unparsed, so the caller decides
/// how an unknown value fails.
#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct FinalizedAttempt {
    pub sandbox_id: SandboxId,
    pub source_node_id: Option<NodeId>,
    pub local_artifact_id: Option<LocalArtifactId>,
    pub artifact_kind: Option<String>,
}

/// Close upload attempt `snapshot_id` in `target_state`, from `creating` or
/// from `target_state` itself, so re-finalizing into the same state succeeds
/// again. `size_bytes` replaces the stored size when set. The failure reason
/// becomes `failure_reason`, or keeps its stored value when that is `None`, in
/// `failed`, and is cleared in any other state. Leaves `timestamp` alone.
///
/// `None` when no row matches: there's no such snapshot, or it's in another
/// state.
pub async fn finalize_attempt(
    executor: impl sqlx::PgExecutor<'_>,
    snapshot_id: SnapshotId,
    target_state: SnapshotState,
    size_bytes: Option<i64>,
    failure_reason: Option<&str>,
) -> Result<Option<FinalizedAttempt>, sqlx::Error> {
    sqlx::query_as(sql!(
        "UPDATE snapshots
                SET state = $2,
                    size_bytes = COALESCE($3, size_bytes),
                    failure_reason = CASE
                        WHEN $2 = 'failed' THEN COALESCE($4, failure_reason)
                        ELSE NULL
                    END
              WHERE id = $1
                AND state IN ('creating', $2)
            RETURNING sandbox_id, source_node_id, local_artifact_id, artifact_kind"
    ))
    .bind(snapshot_id)
    .bind(target_state.as_str())
    .bind(size_bytes)
    .bind(failure_reason)
    .fetch_optional(executor)
    .await
}

/// Stamp upload attempt `snapshot_id`'s `last_attempt_at` with the database
/// clock, while it is still `creating`. `false` when there's no such row or
/// it is no longer `creating`.
pub async fn touch_attempt(
    executor: impl sqlx::PgExecutor<'_>,
    snapshot_id: SnapshotId,
) -> Result<bool, sqlx::Error> {
    let updated = sqlx::query(sql!(
        "UPDATE snapshots
                SET last_attempt_at = NOW()
              WHERE id = $1
                AND state = 'creating'"
    ))
    .bind(snapshot_id)
    .execute(executor)
    .await?
    .rows_affected();
    Ok(updated == 1)
}

/// A full upload [`fail_stuck_full_uploads`] gave up on.
#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct StuckFullUpload {
    #[sqlx(rename = "id")]
    pub snapshot_id: SnapshotId,
    pub sandbox_id: SandboxId,
    pub source_node_id: Option<NodeId>,
}

/// Fail every `creating` full upload with no activity for longer than
/// `min_age` by the database clock, and return them. Activity is
/// `COALESCE(last_attempt_at, timestamp)`, as for [`in_flight_idle_for`]. The
/// failure reason becomes `failure_reason` only where none is stored yet.
///
/// A `min_age` past `i64::MAX` seconds is bound as `i64::MAX`. A row that left
/// `creating` before the statement ran is not matched.
pub async fn fail_stuck_full_uploads(
    executor: impl sqlx::PgExecutor<'_>,
    min_age: Duration,
    failure_reason: &str,
) -> Result<Vec<StuckFullUpload>, sqlx::Error> {
    let min_age_secs = i64::try_from(min_age.as_secs()).unwrap_or(i64::MAX);
    sqlx::query_as(sql!(
        "UPDATE snapshots
                SET state = 'failed',
                    failure_reason = COALESCE(failure_reason, $2)
              WHERE state = 'creating'
                AND type = 'full'
                AND COALESCE(last_attempt_at, timestamp) < NOW() - make_interval(secs => $1::BIGINT)
              RETURNING id, sandbox_id, source_node_id"
    ))
    .bind(min_age_secs)
    .bind(failure_reason)
    .fetch_all(executor)
    .await
}

#[cfg(test)]
mod tests;
