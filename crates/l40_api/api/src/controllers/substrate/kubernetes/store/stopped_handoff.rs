//! A start's statements for the v2 drain's stopped handoff: its read of the
//! row's handoff state, the handoff itself, and the claim on a handed-off
//! row, with its release after a failure.

use super::*;
use pplx_space_sandbox_store::drain_restore;

/// What a start's claim on a handed-off stopped row came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::controllers::substrate::kubernetes) enum StoppedHandoffClaim {
    /// This start owns the re-placement under its lease `token`;
    /// `resumed` when it took over a claim a failed or dead start left.
    Claimed { token: Uuid, resumed: bool },
    /// The row is no longer a handed-off one: v1's sweep or another start
    /// landed it, or it moved on. The ordinary start takes it as it is.
    Landed,
}

impl K8sStore {
    /// The row's handoff state, in one statement; `None` when it is gone.
    pub(in crate::controllers::substrate::kubernetes) async fn stopped_handoff_state(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<drain_restore::StoppedHandoffState>, SubstrateError> {
        drain_restore::stopped_handoff_state(&self.pool, sandbox_id)
            .await
            .db_context("failed to read a stopped sandbox's handoff state")
    }

    /// Hand stopped sandbox `sandbox_id` off from its home node `node_id`
    /// (`drain_restore::hand_off_stopped`), in a transaction of its own.
    pub(in crate::controllers::substrate::kubernetes) async fn hand_off_stopped(
        &self,
        sandbox_id: SandboxId,
        node_id: NodeId,
    ) -> Result<drain_restore::StoppedHandoff, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to begin a stopped sandbox's handoff")?;
        let done = drain_restore::hand_off_stopped(&mut tx, sandbox_id, node_id)
            .await
            .db_context("failed to hand off a stopped sandbox")?;
        tx.commit()
            .await
            .db_context("failed to commit a stopped sandbox's handoff")?;
        Ok(done)
    }

    /// Claim handed-off stopped row `sandbox_id` for this start under a new
    /// restore lease (`drain_restore::claim_handed_off_stopped`). A claim
    /// that loses is classified from the row: one still `restoring` with
    /// its marker is someone else's re-placement in flight, v1's sweep's
    /// or another start's, and answers the retryable
    /// [`SubstrateError::RestoreInFlight`] with the lease's remaining time;
    /// any other row is [`StoppedHandoffClaim::Landed`].
    pub(in crate::controllers::substrate::kubernetes) async fn claim_handed_off_stopped(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<StoppedHandoffClaim, SubstrateError> {
        let token = Uuid::now_v7();
        let claimed = drain_restore::claim_handed_off_stopped(
            &self.pool,
            sandbox_id,
            stopped_handoff_lease_ttl_secs(),
            token,
        )
        .await
        .db_context("failed to claim a handed-off stopped sandbox")?;
        if let Some(lifecycle) = claimed {
            return Ok(StoppedHandoffClaim::Claimed {
                token,
                resumed: lifecycle.trim().eq_ignore_ascii_case("restoring"),
            });
        }
        let (lifecycle, marker, ownership_remaining_secs) =
            self.fetch_restore_ownership_snapshot(sandbox_id).await?;
        if lifecycle == SandboxLifecycle::Restoring && marker.as_deref() == Some("stopped") {
            return Err(SubstrateError::restore_in_flight_with_lease(
                format!(
                    "stopped sandbox {sandbox_id} is being re-placed off its drained node; \
                     retry the start once its re-placement lands"
                ),
                Duration::from_secs_f64(ownership_remaining_secs.max(1.0)),
            ));
        }
        Ok(StoppedHandoffClaim::Landed)
    }

    /// Give back this start's claim `token` on handed-off row `sandbox_id`
    /// after a failure, lapsing its lease so the next start resumes it
    /// (`drain_restore::lapse_handed_off_stopped_claim`).
    pub(in crate::controllers::substrate::kubernetes) async fn lapse_handed_off_stopped_claim(
        &self,
        sandbox_id: SandboxId,
        token: Uuid,
    ) -> Result<bool, SubstrateError> {
        drain_restore::lapse_handed_off_stopped_claim(&self.pool, sandbox_id, token)
            .await
            .db_context("failed to give back a handed-off stopped sandbox's claim")
    }
}

/// How long a start's claim on a handed-off stopped row lasts: the node RPC
/// timeout, as the user restore lease's. The re-placement it drives runs
/// tokenless, as v1's does, and its wait is held below that timeout
/// (`NODE_RPC_BLOCKING_BUDGET`), so the lease outlasts the drive; a start
/// that returns gives it back at once, and one that died lets it lapse.
pub(in crate::controllers::substrate::kubernetes) fn stopped_handoff_lease_ttl_secs() -> i64 {
    recovery_lease_ttl_secs()
}
