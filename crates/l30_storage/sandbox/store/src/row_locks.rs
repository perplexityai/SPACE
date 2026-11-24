//! Plain by-id locks of the `sandboxes` row that several paths take on their
//! own transaction before they re-check or write the row.
//!
//! Each verb runs on the caller's open transaction, and the lock it takes
//! lasts until that transaction ends. The caller keeps the transaction, any
//! `lock_timeout` it sets, and the re-checks it runs under the lock. A lock
//! that carries a guard, or that only one path takes, lives in a module named
//! for that path.

use pplx_space_sandbox_model::SandboxId;

use pplx_space_sql_locator::sql;
use sqlx::PgConnection;

/// Lock the sandbox's row `FOR UPDATE` and return its stored lifecycle,
/// unparsed; `None` when the row is absent. The lock lasts until the caller's
/// transaction ends.
pub async fn lock_lifecycle(
    conn: &mut PgConnection,
    sandbox_id: SandboxId,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
    ))
    .bind(sandbox_id)
    .fetch_optional(conn)
    .await
}

/// Lock the sandbox's row `FOR UPDATE`; `None` when the row is absent. The
/// lock lasts until the caller's transaction ends.
pub async fn lock_id(
    conn: &mut PgConnection,
    sandbox_id: SandboxId,
) -> Result<Option<SandboxId>, sqlx::Error> {
    sqlx::query_scalar(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .fetch_optional(conn)
        .await
}

/// Lock the sandbox's row `FOR UPDATE`; `false` when the row is absent. The
/// lock lasts until the caller's transaction ends.
pub async fn lock_exists(
    conn: &mut PgConnection,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    Ok(
        sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
            .bind(sandbox_id)
            .fetch_optional(conn)
            .await?
            .is_some(),
    )
}
