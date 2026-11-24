//! Plain by-id locks of the `sandboxes` row that several paths take on their
//! own transaction before they re-check or write the row.
//!
//! Each verb runs on the caller's open transaction, and the lock it takes
//! lasts until that transaction ends. The verbs take a `&mut PgConnection`,
//! never a pool, on which the lock would end with its own statement. The
//! caller keeps the transaction, any `lock_timeout` it sets, and the
//! re-checks it runs under the lock. A lock that carries a guard, or that
//! only one path takes, lives in a module named for that path.
//!
//! Each verb's doc names its lock mode. `FOR NO KEY UPDATE` still blocks
//! other transactions' updates and deletes of the row, but not the
//! `FOR KEY SHARE` lock that a child-row insert's foreign-key check takes,
//! so a node publication's insert of a row that references the sandbox does
//! not wait on a path that holds it.

use pplx_space_accounts::NodeId;
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

/// Lock the sandbox's row `FOR NO KEY UPDATE`; `None` when the row is absent.
/// The lock lasts until the caller's transaction ends. Unlike `FOR UPDATE`,
/// it lets a child-row insert's foreign-key check (`FOR KEY SHARE`) on the
/// row through.
pub async fn lock_id_no_key_update(
    conn: &mut PgConnection,
    sandbox_id: SandboxId,
) -> Result<Option<SandboxId>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT id FROM sandboxes WHERE id = $1 FOR NO KEY UPDATE"
    ))
    .bind(sandbox_id)
    .fetch_optional(conn)
    .await
}

/// Lock the sandbox's row `FOR NO KEY UPDATE` and return its active node:
/// `None` when the row is absent, `Some(None)` when it has no active node.
/// The lock lasts until the caller's transaction ends. Unlike `FOR UPDATE`,
/// it lets a child-row insert's foreign-key check (`FOR KEY SHARE`) on the
/// row through.
pub async fn lock_active_node_id_no_key_update(
    conn: &mut PgConnection,
    sandbox_id: SandboxId,
) -> Result<Option<Option<NodeId>>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT active_node_id FROM sandboxes WHERE id = $1 FOR NO KEY UPDATE"
    ))
    .bind(sandbox_id)
    .fetch_optional(conn)
    .await
}
