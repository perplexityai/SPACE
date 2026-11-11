//! Transaction lock for operations that change or depend on a fork source
//! sandbox's lifecycle and durable snapshot set.

use pplx_space_common_core::types::SandboxId;

use pplx_space_common_core::sql;
use sqlx::{Postgres, Transaction};

/// Stable namespace for the per-source advisory transaction lock.
pub const FORK_SOURCE_LIFECYCLE_LOCK_SEED: i64 = 734_927_411;

/// Serialize fork admission, source deletion, and snapshot-retention claims for
/// one sandbox. Every participant must acquire this lock before the sandbox
/// row lock so their ordering cannot deadlock.
pub async fn lock_fork_source_lifecycle(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock(hashtextextended($1::text, $2))"
    ))
    .bind(sandbox_id)
    .bind(FORK_SOURCE_LIFECYCLE_LOCK_SEED)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Return whether a queued or running fork still depends on this source.
///
/// A fork names its source in `target_id` by the bare UUID or, from
/// producers that record the public form, `sbx_<uuid>`; both match, so the
/// guard holds across the change and for rows written before it.
///
/// Callers that use this as a lifecycle or snapshot-deletion guard must first
/// acquire [`lock_fork_source_lifecycle`] in the same transaction.
pub async fn active_fork_source_operation_exists(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
               FROM public_operations
              WHERE kind = 'fork_sandbox'
                AND target_id IN ($1, 'sbx_' || $1)
                AND state IN ('queued', 'running')
         )"
    ))
    .bind(sandbox_id.to_string())
    .fetch_one(&mut **tx)
    .await
}
