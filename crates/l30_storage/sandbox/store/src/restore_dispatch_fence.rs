//! The restore dispatch fence on `sandbox_k8s_executions`: a Kubernetes user
//! restore's durable proof that it has not sent its Wake, and the takeover of
//! a restore whose owner stopped renewing that proof.
//!
//! A user restore's owner writes the fence on the active execution it claims,
//! bound to its `sandboxes.restore_lease_token`, and renews its expiry while
//! its process is alive. Immediately before it sends the Wake it clears the
//! fence, holding the sandbox row lock and having checked that the row still
//! carries its token. A re-drive may take over a markerless `restoring` row
//! whose active execution still carries a fence bound to the row's current
//! token after the fence has expired: that owner never sent a Wake and has
//! stopped renewing, so no node work can be in flight for it. The takeover
//! clears the fence and rotates the token under the same row lock, so of the
//! owner's Wake and the takeover at most one goes ahead.
//!
//! A row with no fence, or a fence bound to a token the row no longer
//! carries, keeps the restore lease and execution-age guards of
//! [`crate::restore_lease`]: an owner that predates the fence, or a takeover
//! by one, writes none.
//!
//! The owner's sandbox row lock before it sends the Wake, which reads whether
//! the row is still `restoring` under its token, is
//! [`lock_dispatch_ownership`]; the takeover locks the row with
//! [`crate::row_locks::lock_id`]. The Kubernetes store keeps the
//! transactions those locks last for and the decision of when to fence;
//! these functions are the statements.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use uuid::Uuid;

use crate::restore_lease::RestoreClaimRow;

/// Fence the active execution for the user restore `restore_lease_token`
/// owns, until `fence_ttl_secs` from now. Writes an **operation step** (the
/// restore's ownership). Returns whether an active execution was fenced.
///
/// No guard of its own: the caller holds the sandbox row lock and has just
/// renewed its restore lease in the same transaction. A fence another owner
/// wrote is replaced; it is bound to a token the row no longer carries. The
/// caller must pass its open transaction.
pub async fn fence_unsent_user_restore<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
    restore_lease_token: Uuid,
    fence_ttl_secs: i64,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let fenced = sqlx::query_scalar::<_, bool>(sql!(
        "UPDATE sandbox_k8s_executions
            SET restore_dispatch_fence_token = $2,
                restore_dispatch_fence_expires_at = NOW() + make_interval(secs => $3)
          WHERE sandbox_id = $1
            AND active
         RETURNING TRUE"
    ))
    .bind(sandbox_id)
    .bind(restore_lease_token)
    .bind(fence_ttl_secs as f64)
    .fetch_optional(tx.into_sqlx())
    .await?;
    Ok(fenced.is_some())
}

/// Extend the fence `restore_lease_token` holds on the sandbox's active
/// execution to `fence_ttl_secs` from now. Writes an **operation step** (the
/// owner's liveness). Returns `false` when the execution carries no fence
/// under that token: the owner cleared it to send its Wake, or a takeover
/// cleared it.
pub async fn renew_unsent_user_restore_fence(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    restore_lease_token: Uuid,
    fence_ttl_secs: i64,
) -> Result<bool, DbError> {
    let renewed = sqlx::query_scalar::<_, bool>(sql!(
        "UPDATE sandbox_k8s_executions
            SET restore_dispatch_fence_expires_at = NOW() + make_interval(secs => $3)
          WHERE sandbox_id = $1
            AND active
            AND restore_dispatch_fence_token = $2
         RETURNING TRUE"
    ))
    .bind(sandbox_id)
    .bind(restore_lease_token)
    .bind(fence_ttl_secs as f64)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(renewed.is_some())
}

/// Whether the sandbox's active execution of `generation` carries the fence
/// `restore_lease_token` holds. That restore owns the execution's pod
/// create: either it created the execution, or it took the execution over
/// from an owner that died before sending its Wake, maybe before creating the
/// pod.
pub async fn holds_fence(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    restore_lease_token: Uuid,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
             SELECT 1
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND active
                AND generation = $2
                AND restore_dispatch_fence_token = $3
         )"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(restore_lease_token)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Lock the sandbox row `FOR UPDATE` until the caller's transaction ends and
/// read whether it is still `restoring` under `restore_lease_token`. `None`
/// when no row exists. The row is locked whenever it exists, whatever the
/// answer.
///
/// The owner takes it before [`clear_for_dispatch`]; the takeover holds the
/// same row lock, so the two serialize. The caller must pass its open
/// transaction.
pub async fn lock_dispatch_ownership<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
    restore_lease_token: Uuid,
) -> Result<Option<bool>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar(sql!(
        "SELECT LOWER(TRIM(lifecycle)) = 'restoring'
                    AND restore_lease_token IS NOT DISTINCT FROM $2
               FROM sandboxes
              WHERE id = $1
              FOR UPDATE"
    ))
    .bind(sandbox_id)
    .bind(restore_lease_token)
    .fetch_optional(tx.into_sqlx())
    .await?)
}

/// Clear the fence on the sandbox's active execution as its owner sends the
/// Wake, and stamp the execution's `updated_at`, which starts the
/// execution-age guard's window over the Wake. Writes an **operation step**
/// (the Wake's dispatch).
///
/// No guard of its own: the caller holds the sandbox row lock and has checked,
/// under it, that the row is still `restoring` with its restore lease token.
/// The caller must pass its open transaction.
pub async fn clear_for_dispatch<C>(tx: &mut C, sandbox_id: SandboxId) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET restore_dispatch_fence_token = NULL,
                restore_dispatch_fence_expires_at = NULL,
                updated_at = NOW()
          WHERE sandbox_id = $1
            AND active"
    ))
    .bind(sandbox_id)
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}

/// Take over the user restore of a markerless `restoring` row whose active
/// execution carries an expired fence bound to the row's current restore
/// lease token: clear that fence, expire the dead owner's activation attempt
/// so the new owner takes its own, and write a new restore lease under
/// `restore_lease_token` for `lease_ttl_secs`. Writes an **operation step**
/// (the claim). Returns the claimed row, or `None` when there is no such
/// fence.
///
/// The caller holds the sandbox row lock in `tx`, which the owner's
/// [`clear_for_dispatch`] path also takes, so an owner that clears its fence
/// first leaves nothing to take over, and one that comes second finds the
/// token changed. The caller must pass its open transaction.
pub async fn take_over_unsent_user_restore<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
    lease_ttl_secs: i64,
    restore_lease_token: Uuid,
) -> Result<Option<RestoreClaimRow>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let fenced = sqlx::query_scalar::<_, i64>(sql!(
        "UPDATE sandbox_k8s_executions e
            SET restore_dispatch_fence_token = NULL,
                restore_dispatch_fence_expires_at = NULL,
                activation_attempt_expires_at = CASE
                    WHEN e.activation_attempt_expires_at IS NULL THEN NULL
                    ELSE LEAST(e.activation_attempt_expires_at, NOW())
                END
           FROM sandboxes s
          WHERE e.sandbox_id = $1
            AND e.active
            AND s.id = e.sandbox_id
            AND LOWER(TRIM(s.lifecycle)) = 'restoring'
            AND s.evacuation_restore_target IS NULL
            AND e.restore_dispatch_fence_token = s.restore_lease_token
            AND e.restore_dispatch_fence_expires_at <= NOW()
         RETURNING e.generation"
    ))
    .bind(sandbox_id)
    .fetch_optional((&mut *tx).into_sqlx())
    .await?;
    if fenced.is_none() {
        return Ok(None);
    }
    Ok(sqlx::query_as::<_, RestoreClaimRow>(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() + ($2::bigint * INTERVAL '1 second'),
                restore_lease_token = $3
          WHERE id = $1
            AND LOWER(TRIM(lifecycle)) = 'restoring'
         RETURNING lifecycle, $3::uuid AS restore_lease_token"
    ))
    .bind(sandbox_id)
    .bind(lease_ttl_secs)
    .bind(restore_lease_token)
    .fetch_optional(tx.into_sqlx())
    .await?)
}
