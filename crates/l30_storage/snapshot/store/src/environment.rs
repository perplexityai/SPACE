//! The execution environment a catalog snapshot recorded it was produced
//! under, unparsed, for the environment placement gate.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::{LocalArtifactId, SnapshotId};
use pplx_space_sql_locator::sql;

/// The environment `snapshot_id` was produced under; `None` when it records
/// none or there's no such snapshot.
pub async fn of_snapshot(
    executor: impl Executor<'_>,
    snapshot_id: SnapshotId,
) -> Result<Option<String>, DbError> {
    Ok(sqlx::query_scalar::<_, Option<String>>(sql!(
        "SELECT execution_metadata->>'environment' FROM snapshots WHERE id = $1"
    ))
    .bind(snapshot_id.as_uuid())
    .fetch_optional(executor.into_sqlx())
    .await?
    .flatten())
}

/// The environment the newest ready disk snapshot of `sandbox_id`'s local
/// artifact `local_artifact_id` was produced under; `None` when it records
/// none or there's no such snapshot.
pub async fn of_ready_disk_artifact(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
) -> Result<Option<String>, DbError> {
    Ok(sqlx::query_scalar::<_, Option<String>>(sql!(
        "SELECT execution_metadata->>'environment'
               FROM snapshots
              WHERE sandbox_id = $1
                AND local_artifact_id = $2
                AND type = 'disk'
                AND state = 'ready'
              ORDER BY timestamp DESC
              LIMIT 1"
    ))
    .bind(sandbox_id)
    .bind(local_artifact_id.as_uuid())
    .fetch_optional(executor.into_sqlx())
    .await?
    .flatten())
}

/// The environment the newest ready disk snapshot of `sandbox_id`'s current
/// local artifact was produced under; `None` when it records none or there's
/// no such snapshot.
pub async fn of_current_ready_disk_artifact(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<String>, DbError> {
    Ok(sqlx::query_scalar::<_, Option<String>>(sql!(
        "SELECT sn.execution_metadata->>'environment'
               FROM sandboxes s
               JOIN snapshots sn
                 ON sn.sandbox_id = s.id
                AND sn.local_artifact_id = s.current_local_artifact_id
              WHERE s.id = $1
                AND sn.type = 'disk'
                AND sn.state = 'ready'
              ORDER BY sn.timestamp DESC
              LIMIT 1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor.into_sqlx())
    .await?
    .flatten())
}
