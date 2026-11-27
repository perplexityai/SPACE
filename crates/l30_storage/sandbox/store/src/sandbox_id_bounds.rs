//! The bounds of a substrate's sandbox ids, for sweeps that page by id.

use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::Executor;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;

use crate::SandboxStoreError;

/// The newest id among `managed_by`'s sandbox rows, every lifecycle included;
/// `None` when it has none.
pub async fn newest_managed_by(
    executor: impl Executor<'_>,
    managed_by: SubstrateKind,
) -> Result<Option<SandboxId>, SandboxStoreError> {
    // Pre-PG-15 has no `max(uuid)` aggregate, so ORDER BY DESC LIMIT 1
    // instead; `None` when `managed_by` has no row.
    let row: Option<(SandboxId,)> = sqlx::query_as(sql!(
        "SELECT id FROM sandboxes WHERE managed_by = $1 ORDER BY id DESC LIMIT 1"
    ))
    .bind(managed_by)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(row.map(|(id,)| id))
}
