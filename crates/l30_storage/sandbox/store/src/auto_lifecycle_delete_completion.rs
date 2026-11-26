//! The internal API's lock of the `sandboxes` row when it consumes a node's
//! auto-lifecycle delete completion receipt.
//!
//! The caller keeps its per-receipt savepoint, the classification of a
//! contended lock (`NOWAIT` fails with `lock_not_available`), and every
//! decision it makes from the row.

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use sqlx::types::{Json, JsonValue};

/// The receipt's authority columns of the locked row.
#[derive(Debug, sqlx::FromRow)]
pub struct CompletionAuthorityRow {
    pub lifecycle: String,
    pub auto_lifecycle_json: Json<JsonValue>,
    pub active_node_id: Option<NodeId>,
    /// Whether the row's placement (or, when never placed, its creation) is
    /// at least `ownership_grace_secs` old on the database clock.
    pub ownership_evidence_expired: bool,
}

/// Lock the sandbox's row `FOR UPDATE OF s NOWAIT` and read the receipt's
/// authority columns; `None` when the row is absent. Fails at once with
/// `lock_not_available` when another transaction holds the row. The lock
/// lasts until the caller's transaction ends. The caller must pass its open
/// transaction.
pub async fn lock_completion_authority<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
    ownership_grace_secs: f64,
) -> Result<Option<CompletionAuthorityRow>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_as::<_, CompletionAuthorityRow>(sql!(
        "SELECT s.lifecycle,
                s.auto_lifecycle_json,
                s.active_node_id,
                COALESCE(s.placement_changed_at, s.created_at)
                    <= NOW() - make_interval(secs => $2) AS ownership_evidence_expired
           FROM sandboxes s
          WHERE s.id = $1
          FOR UPDATE OF s NOWAIT"
    ))
    .bind(sandbox_id)
    .bind(ownership_grace_secs)
    .fetch_optional(tx.into_sqlx())
    .await?)
}
