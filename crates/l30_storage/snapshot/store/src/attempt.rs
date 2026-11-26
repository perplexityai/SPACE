//! One snapshot attempt's stored state, as its catalog row records it.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;
use sqlx::FromRow;

/// The stored state and failure reason of one snapshot attempt. `state` is
/// unparsed, so the caller decides how an unknown value fails.
#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct AttemptStatus {
    pub state: String,
    pub failure_reason: Option<String>,
}

/// The status of snapshot attempt `snapshot_id`; `None` when there's no such
/// row.
pub async fn status(
    executor: impl Executor<'_>,
    snapshot_id: SnapshotId,
) -> Result<Option<AttemptStatus>, DbError> {
    Ok(sqlx::query_as::<_, AttemptStatus>(sql!(
        "SELECT state, failure_reason FROM snapshots WHERE id = $1"
    ))
    .bind(snapshot_id.as_uuid())
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The stored state of snapshot `snapshot_id`, unparsed; `None` when there's
/// no such row.
pub async fn state(
    executor: impl Executor<'_>,
    snapshot_id: SnapshotId,
) -> Result<Option<String>, DbError> {
    Ok(
        sqlx::query_scalar(sql!("SELECT state FROM snapshots WHERE id = $1"))
            .bind(snapshot_id)
            .fetch_optional(executor.into_sqlx())
            .await?,
    )
}
