//! Locks of a sandbox's open quarantine episode, taken on the caller's
//! transaction. Each lock lasts until that transaction ends. The caller keeps
//! the transaction, its lock settings, the classification of a lock error,
//! and the re-checks it runs under the lock.

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{SandboxId, SandboxQuarantineReasonCode, SandboxQuarantineSource};

use pplx_space_sql_locator::sql;

/// Lock the sandbox's open episode `FOR UPDATE`; `None` when it has none.
/// The lock lasts until the caller's transaction ends; the caller must pass
/// its open transaction.
pub async fn lock_quarantine_state<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
) -> Result<Option<SandboxId>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar(sql!(
        "SELECT sandbox_id FROM sandbox_quarantine_state WHERE sandbox_id = $1 FOR UPDATE"
    ))
    .bind(sandbox_id)
    .fetch_optional(tx.into_sqlx())
    .await?)
}

/// Lock the sandbox's open episode `FOR UPDATE NOWAIT` and answer whether it
/// was last observed on `node_id` (`last_observed_node_id`), opened by
/// `source`, for one of `reason_codes`; `None` when it has no episode. Fails
/// at once with `lock_not_available` when another transaction holds the
/// episode. The lock lasts until the caller's transaction ends; the caller
/// must pass its open transaction.
pub async fn lock_orphan_quarantine_match<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
    node_id: NodeId,
    source: SandboxQuarantineSource,
    reason_codes: &[SandboxQuarantineReasonCode],
) -> Result<Option<bool>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT COALESCE(last_observed_node_id = $2, FALSE)
                AND source = $3
                AND reason_code = ANY($4::TEXT[])
           FROM sandbox_quarantine_state
          WHERE sandbox_id = $1
          FOR UPDATE NOWAIT"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(source)
    .bind(reason_codes)
    .fetch_optional(tx.into_sqlx())
    .await?)
}
