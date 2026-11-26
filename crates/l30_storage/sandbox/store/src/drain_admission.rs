//! What the start of a v2 drain reads of the sandbox rows.
//!
//! A v2 drain never moves a stopped sandbox and never terminates a node that
//! holds one, so the admin API's `:drain {engine: v2}` refuses a node whose
//! own preview lists a sandbox pending that Postgres holds stopped
//! ([`stopped_among`]). The node's list decides which sandboxes are its: a
//! stopped row whose node id a replacement node reused, and which that node
//! doesn't hold, refuses nothing.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;

/// Of `sandbox_ids`, the ones whose row is `stopped`, by id. A read: it
/// takes no lock and writes nothing.
pub async fn stopped_among(
    executor: impl Executor<'_>,
    sandbox_ids: &[SandboxId],
) -> Result<Vec<SandboxId>, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT id FROM sandboxes
          WHERE id = ANY($1::UUID[])
            AND lifecycle = 'stopped'
          ORDER BY id"
    ))
    .bind(sandbox_ids)
    .fetch_all(executor.into_sqlx())
    .await?)
}

#[cfg(test)]
mod tests;
