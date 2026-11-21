//! The start of a stopped sandbox a v2 drain drained: the stopped handoff,
//! then v1's stopped re-placement, before the ordinary start boots it.

use super::*;
use crate::controllers::substrate::kubernetes::store::StoppedHandoffClaim;
use pplx_space_sandbox_store::drain_restore::StoppedHandoff;

impl KubernetesSubstrate {
    /// Move a stopped sandbox off the node a v2 drain drained before its
    /// start boots it, and return the row the ordinary start boots, with
    /// what is left of the caller's readiness window. Any other row, and
    /// every row while [`StoppedHandoffAtStart`] is off, is returned as it
    /// was at no extra statement, for main's start.
    ///
    /// For a `stopped` or `restoring` row it reads the row's handoff state
    /// (`pplx_space_sandbox_store::drain_restore::stopped_handoff_state`,
    /// one statement):
    ///
    /// * a stopped row at home on a drained or terminating v2 node, its disk
    ///   durable since its stop, is handed off first
    ///   ([`K8sStore::hand_off_stopped`]): pinned, marked for v1's stopped
    ///   re-placement, unplaced, its execution retired;
    /// * a handed-off row whose re-placement hasn't landed is resumed: an
    ///   earlier start handed it off, or claimed it and failed or died, or
    ///   v1's drain released it.
    ///
    /// Either way the start then claims the row under a restore lease
    /// ([`K8sStore::claim_handed_off_stopped`]), which v1's restore sweep
    /// honours, and drives v1's stopped re-placement
    /// ([`Self::drive_claimed_restore`] toward `stopped`, on any node but the
    /// ones it ran on, from the disk's exact URI): the node stages the disk
    /// without booting it. Then it lands the row `stopped` on that node and
    /// releases the placeholder pod, best effort, as v1's drain does. A
    /// re-placement the sweep or another start owns answers the retryable
    /// [`SubstrateError::RestoreInFlight`]. A failure after the claim gives
    /// the claim back, lapsing its lease, and answers the same retryable
    /// 503, so the next start resumes it.
    pub(in crate::controllers::substrate::kubernetes) async fn hand_off_stopped_at_start(
        &self,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox: Sandbox,
        options: ActivationOptions,
    ) -> Result<(Sandbox, ActivationOptions), SubstrateError> {
        if !self.k8s.stopped_handoff_at_start.is_enabled()
            || !matches!(
                normalize_lifecycle(&sandbox.lifecycle).as_str(),
                "stopped" | "restoring"
            )
        {
            return Ok((sandbox, options));
        }
        let sandbox_id = sandbox.id;
        let Some(state) = self.store.stopped_handoff_state(sandbox_id).await? else {
            return Ok((sandbox, options));
        };
        if let Some(home) = state.hand_off_from {
            match self.store.hand_off_stopped(sandbox_id, home).await? {
                StoppedHandoff::HandedOff(_) => {}
                StoppedHandoff::MovedOn => {
                    return Ok((self.store.require_sandbox(sandbox_id).await?, options));
                }
                StoppedHandoff::Refused(reason) => {
                    return Err(SubstrateError::RestoreSourcePending {
                        message: format!(
                            "stopped sandbox {sandbox_id} can't move off its drained node yet: \
                             {reason}; retry the start"
                        ),
                    });
                }
            }
        } else if !state.pending {
            return Ok((sandbox, options));
        }
        let started = Instant::now();
        let StoppedHandoffClaim::Claimed { token, resumed } =
            self.store.claim_handed_off_stopped(sandbox_id).await?
        else {
            return Ok((self.store.require_sandbox(sandbox_id).await?, options));
        };
        if let Err(error) = self
            .re_place_handed_off_stopped(metric_identity, sandbox_id, options, resumed)
            .await
        {
            if let Err(lapse) = self
                .store
                .lapse_handed_off_stopped_claim(sandbox_id, token)
                .await
            {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %lapse,
                    "failed to give back a handed-off stopped sandbox's claim; the next start \
                     resumes it once its lease lapses"
                );
            }
            return Err(SubstrateError::restore_in_flight_with_lease(
                format!(
                    "stopped sandbox {sandbox_id} could not be re-placed off its drained node: \
                     {error}; retry the start to resume it"
                ),
                Duration::ZERO,
            ));
        }
        let options = ActivationOptions {
            ready_timeout: options.ready_timeout.saturating_sub(started.elapsed()),
            ..options
        };
        Ok((self.store.require_sandbox(sandbox_id).await?, options))
    }

    /// Drive the re-placement of handed-off stopped row `sandbox_id` this
    /// start claimed (`resumed` when it took over an earlier claim) and land
    /// it `stopped`, then release the placeholder pod, best effort.
    async fn re_place_handed_off_stopped(
        &self,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        options: ActivationOptions,
        resumed: bool,
    ) -> Result<(), SubstrateError> {
        use crate::controllers::substrate::DbContext as _;
        let (_, join_deadline) = ready_wait_bounds(options.ready_timeout);
        // Tokenless, as v1's evacuation claim is: the start's lease fences
        // the sweep off the row, and the drive is v1's stopped re-placement.
        let claim = if resumed {
            RestoreClaim::RedroveRestoring {
                restore_lease_token: None,
            }
        } else {
            RestoreClaim::FreshFromStopped
        };
        self.drive_claimed_restore(
            metric_identity,
            sandbox_id,
            join_deadline,
            RestoreIntent::drain(None, SandboxLifecycle::Stopped),
            claim,
        )
        .await?;
        if !pplx_space_sandbox_store::drain::finalize_evacuation_restore(
            self.store.pool(),
            sandbox_id,
            SandboxLifecycle::Stopped,
        )
        .await
        .db_context("failed to land a re-placed stopped sandbox")?
        {
            return Err(SubstrateError::Unavailable {
                message: format!("stopped sandbox {sandbox_id} moved while its disk was re-placed"),
            });
        }
        // Best effort: a failure costs the node the pod's slot until the
        // start replaces it, and nothing about the sandbox's data.
        if let Err(error) = self.release_stopped_materialization_pod(sandbox_id).await {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                %error,
                "failed to delete a re-placed stopped sandbox's placeholder pod"
            );
        }
        Ok(())
    }
}
