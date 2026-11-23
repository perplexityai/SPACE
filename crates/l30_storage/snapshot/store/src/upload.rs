//! Snapshot uploads still in flight.

use std::time::Duration;

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;

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

#[cfg(test)]
mod tests;
