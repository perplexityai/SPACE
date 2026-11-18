//! The start of a stopped sandbox a v2 drain drained: the stopped handoff,
//! then v1's stopped re-placement, before the ordinary start boots it.

use super::*;
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
    /// Either way the start then claims the row with v1's own fenced claim
    /// ([`K8sStore::begin_evacuation_restore_lifecycle`] toward `stopped`),
    /// which writes no lease: with the switch on, v1 and v2 drains never run
    /// together, so every marked row is a v2 handoff and only a start drives
    /// it. It drives v1's stopped re-placement
    /// ([`Self::drive_claimed_restore`] toward `stopped`, on any node but the
    /// ones it ran on, from the disk's exact URI): the node stages the disk
    /// without booting it. Then it lands the row `stopped` on that node,
    /// fenced to the execution its claim drove, and releases the placeholder
    /// pod, best effort, as v1's drain does ([`Self::land_re_placed_stopped`]).
    /// A row the claim no longer takes has landed or moved on, and goes to
    /// main's start as it is. A failure after the claim leaves the row claimed
    /// (`restoring`, marked) and answers a retryable 503
    /// ([`SubstrateError::RestoreInFlight`]); the claim takes a `restoring`
    /// marked row again, so the next start resumes it.
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
        let claim = match self
            .store
            .begin_evacuation_restore_lifecycle(sandbox_id, SandboxLifecycle::Stopped)
            .await
        {
            Ok(claim) => claim,
            // The claim lost: another start landed the row, or it moved on.
            // Main's start takes it as it is.
            Err(SubstrateError::EvacuationClaimLost { .. } | SubstrateError::Conflict { .. }) => {
                return Ok((self.store.require_sandbox(sandbox_id).await?, options));
            }
            Err(error) => return Err(error),
        };
        if let Err(error) = self
            .re_place_handed_off_stopped(metric_identity, sandbox_id, options, claim)
            .await
        {
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

    /// Drive the re-placement of handed-off stopped row `sandbox_id` under
    /// this start's claim `claim` (`FreshFromStopped`, or `RedroveRestoring`
    /// when it resumed an earlier start's) and land it `stopped`
    /// ([`Self::land_re_placed_stopped`]).
    async fn re_place_handed_off_stopped(
        &self,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        options: ActivationOptions,
        claim: RestoreClaim,
    ) -> Result<(), SubstrateError> {
        let (_, join_deadline) = ready_wait_bounds(options.ready_timeout);
        let activation = self
            .drive_claimed_restore(
                metric_identity,
                sandbox_id,
                join_deadline,
                RestoreIntent::drain(None, SandboxLifecycle::Stopped),
                claim,
            )
            .await?;
        self.land_re_placed_stopped(sandbox_id, activation.binding.execution_generation)
            .await
    }

    /// Land handed-off stopped row `sandbox_id`, whose re-placement this
    /// start's claim drove as execution `execution_generation`, then release
    /// the placeholder pod, best effort.
    ///
    /// The claim writes no lease and takes a claimed row again, so a later
    /// start may have claimed the row while this one drove it. The landing is
    /// fenced to this claim's execution
    /// ([`K8sStore::land_handed_off_stopped`]): a row whose active execution
    /// is no longer this one, or that moved, is left as it is, and this
    /// start answers a retryable 503 ([`SubstrateError::RestoreInFlight`])
    /// while the later claim finishes. v1's finalize
    /// (`pplx_space_sandbox_store::drain::finalize_evacuation_restore`) stays
    /// the controller's alone.
    pub(in crate::controllers::substrate::kubernetes) async fn land_re_placed_stopped(
        &self,
        sandbox_id: SandboxId,
        execution_generation: u64,
    ) -> Result<(), SubstrateError> {
        let generation =
            i64::try_from(execution_generation).map_err(|_| SubstrateError::Failed {
                message: format!(
                    "stopped sandbox {sandbox_id}'s execution generation \
                     {execution_generation} is out of range"
                ),
            })?;
        if !self
            .store
            .land_handed_off_stopped(sandbox_id, generation)
            .await?
        {
            return Err(SubstrateError::restore_in_flight_with_lease(
                format!(
                    "stopped sandbox {sandbox_id} moved, or a later start's claim took it, \
                     while its disk was re-placed; retry the start"
                ),
                Duration::ZERO,
            ));
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
