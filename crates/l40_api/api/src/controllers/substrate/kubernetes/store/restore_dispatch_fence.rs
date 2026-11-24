//! The restore dispatch fence around a user restore's Wake: fencing the
//! claimed execution, renewing the fence while the owner lives, confirming
//! ownership as the Wake is sent, and taking over an expired fence.

use super::*;
use pplx_space_sandbox_store::restore_dispatch_fence;

use super::super::restore_dispatch_fence::{FENCE_RENEW_INTERVAL, FENCE_TTL};

fn fence_ttl_secs() -> i64 {
    i64::try_from(FENCE_TTL.as_secs()).expect("fence TTL seconds fit i64")
}

impl K8sStore {
    /// Fence the active execution for the user restore `restore_lease_token`
    /// owns, in the execution claim's transaction, when the switch is on.
    pub(super) async fn fence_unsent_user_restore_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        restore_lease_token: Uuid,
    ) -> Result<(), SubstrateError> {
        if !self.restore_dispatch_fence.is_enabled() {
            return Ok(());
        }
        restore_dispatch_fence::fence_unsent_user_restore(
            &mut **tx,
            sandbox_id,
            restore_lease_token,
            fence_ttl_secs(),
        )
        .await
        .db_context("failed to fence the restore's unsent Wake")?;
        Ok(())
    }

    /// Renew the fence `restore_lease_token` holds every
    /// [`FENCE_RENEW_INTERVAL`] until the returned guard drops, while the
    /// switch is on. Renewal stops once a fence it has renewed is gone (the
    /// owner cleared it to send its Wake, or a takeover did); until the
    /// execution claim writes the fence there is nothing to renew.
    pub(in crate::controllers::substrate::kubernetes) fn renew_unsent_user_restore_fence_until_dropped(
        &self,
        sandbox_id: SandboxId,
        restore_lease_token: Uuid,
    ) -> Option<tokio_util::sync::DropGuard> {
        if !self.restore_dispatch_fence.is_enabled() {
            return None;
        }
        let stop = tokio_util::sync::CancellationToken::new();
        let pool = self.pool.clone();
        let stopped = stop.clone();
        crate::task_scope::spawn(async move {
            let mut renewed_once = false;
            loop {
                tokio::select! {
                    () = stopped.cancelled() => return,
                    () = tokio::time::sleep(FENCE_RENEW_INTERVAL) => {}
                }
                match restore_dispatch_fence::renew_unsent_user_restore_fence(
                    &pool,
                    sandbox_id,
                    restore_lease_token,
                    fence_ttl_secs(),
                )
                .await
                {
                    Ok(true) => renewed_once = true,
                    Ok(false) if renewed_once => return,
                    Ok(false) => {}
                    Err(error) => tracing::warn!(
                        %sandbox_id,
                        %error,
                        "failed to renew the restore's dispatch fence"
                    ),
                }
            }
        });
        Some(stop.drop_guard())
    }

    /// Whether the user restore `restore_lease_token` owns holds the fence on
    /// the sandbox's active execution of `generation`, and so owns its pod
    /// create, when the switch is on.
    pub(in crate::controllers::substrate::kubernetes) async fn holds_restore_dispatch_fence(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        restore_lease_token: Uuid,
    ) -> Result<bool, SubstrateError> {
        if !self.restore_dispatch_fence.is_enabled() {
            return Ok(false);
        }
        restore_dispatch_fence::holds_fence(&self.pool, sandbox_id, generation, restore_lease_token)
            .await
            .db_context("failed to read the restore's dispatch fence")
    }

    /// Confirm, as the owner sends its Wake, that the sandbox is still
    /// `restoring` under `restore_lease_token`, and clear its fence. Returns
    /// `false` when another restore took the row over or it left `restoring`:
    /// the Wake must not be sent.
    ///
    /// Holds the sandbox row lock the takeover also takes, so the two
    /// serialize: a takeover that commits first changes the token this checks,
    /// and one that comes second finds no fence to take.
    pub(in crate::controllers::substrate::kubernetes) async fn confirm_user_restore_dispatch(
        &self,
        sandbox_id: SandboxId,
        restore_lease_token: Uuid,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start restore dispatch transaction")?;
        let owned: Option<bool> = sqlx::query_scalar(sql!(
            "SELECT LOWER(TRIM(lifecycle)) = 'restoring'
                    AND restore_lease_token IS NOT DISTINCT FROM $2
               FROM sandboxes
              WHERE id = $1
              FOR UPDATE"
        ))
        .bind(sandbox_id)
        .bind(restore_lease_token)
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to lock sandbox for restore dispatch")?;
        if owned != Some(true) {
            return Ok(false);
        }
        restore_dispatch_fence::clear_for_dispatch(&mut *tx, sandbox_id)
            .await
            .db_context("failed to clear the restore's dispatch fence")?;
        tx.commit()
            .await
            .db_context("failed to commit restore dispatch")?;
        Ok(true)
    }

    /// Take over the user restore of a markerless `restoring` row whose owner
    /// let its fence expire without sending its Wake, when the switch is on.
    pub(super) async fn take_over_unsent_user_restore(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<pplx_space_sandbox_store::restore_lease::RestoreClaimRow>, SubstrateError>
    {
        if !self.restore_dispatch_fence.is_enabled() {
            return Ok(None);
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start restore takeover transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(None);
        }
        let claim = restore_dispatch_fence::take_over_unsent_user_restore(
            &mut tx,
            sandbox_id,
            recovery_lease_ttl_secs(),
            Uuid::now_v7(),
        )
        .await
        .db_context("failed to take over an unsent restore")?;
        tx.commit()
            .await
            .db_context("failed to commit restore takeover")?;
        if claim.is_some() {
            tracing::info!(
                %sandbox_id,
                "took over a restore whose owner stopped renewing its unsent Wake's fence"
            );
        }
        Ok(claim)
    }
}
