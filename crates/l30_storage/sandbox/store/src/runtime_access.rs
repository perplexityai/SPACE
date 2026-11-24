//! What the data plane reads off the `sandboxes` row to decide whether a
//! sandbox's runtime may be reached now, and what api re-reads to decide
//! whether a failed upstream connect happened while the sandbox was
//! plausibly mid-wake.

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;

/// The row's lifecycle and whether it changed within the caller's grace
/// window, by the database clock.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct RuntimeAccessRecord {
    /// Raw `sandboxes.lifecycle`; the caller parses it.
    pub lifecycle: String,
    pub lifecycle_changed_recently: bool,
}

/// The sandbox's [`RuntimeAccessRecord`], with "recently" meaning less than
/// `grace_secs` ago. `None` when no such sandbox exists.
pub async fn runtime_access(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    grace_secs: f64,
) -> Result<Option<RuntimeAccessRecord>, sqlx::Error> {
    sqlx::query_as::<_, RuntimeAccessRecord>(sql!(
        "SELECT lifecycle,
                lifecycle_changed_at > NOW() - make_interval(secs => $2)
                    AS lifecycle_changed_recently
         FROM sandboxes
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(grace_secs)
    .fetch_optional(executor)
    .await
}
