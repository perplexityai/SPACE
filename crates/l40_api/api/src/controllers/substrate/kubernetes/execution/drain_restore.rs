use super::*;

impl KubernetesSubstrate {
    /// Place an unplaced stopped sandbox whose pinned disk is durable on a
    /// node before its start boots it: what a v2 drain's release leaves. It
    /// marks the row a pending stopped re-placement
    /// ([`pplx_space_sandbox_store::drain_restore::mark_unplaced_stopped_for_materialization`])
    /// and runs v1's stopped re-placement ([`Self::restore_evacuated_execution`]
    /// toward `stopped`, on any node, from the disk's exact URI): the node
    /// stages the disk without booting it. Then it lands the row `stopped`
    /// on that node and releases the placeholder pod, best effort, as v1's
    /// drain does, and returns the row the ordinary start boots. Any other
    /// row is returned as it was. A re-placement that fails before its claim
    /// takes the marker back, so the next start tries again.
    pub(in crate::controllers::substrate::kubernetes) async fn materialize_unplaced_stopped(
        &self,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox: Sandbox,
        options: ActivationOptions,
    ) -> Result<Sandbox, SubstrateError> {
        use crate::controllers::substrate::DbContext as _;
        let sandbox_id = sandbox.id;
        if !pplx_space_sandbox_store::drain_restore::mark_unplaced_stopped_for_materialization(
            self.store.pool(),
            sandbox_id,
        )
        .await
        .db_context("failed to mark an unplaced stopped sandbox for materialization")?
        {
            return Ok(sandbox);
        }
        if let Err(error) = self
            .restore_evacuated_execution(
                metric_identity,
                sandbox_id,
                options,
                None,
                SandboxLifecycle::Stopped,
            )
            .await
        {
            if let Err(unmark) = pplx_space_sandbox_store::drain_restore::unmark_unplaced_stopped(
                self.store.pool(),
                sandbox_id,
            )
            .await
            {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %unmark,
                    "failed to take back an unplaced stopped sandbox's materialization marker"
                );
            }
            return Err(error);
        }
        if !pplx_space_sandbox_store::drain::finalize_evacuation_restore(
            self.store.pool(),
            sandbox_id,
            SandboxLifecycle::Stopped,
        )
        .await
        .db_context("failed to land a materialized stopped sandbox")?
        {
            return Err(SubstrateError::Unavailable {
                message: format!(
                    "stopped sandbox {sandbox_id} moved while its disk was materialized; retry"
                ),
            });
        }
        // Best effort: a failure costs the node the pod's slot until the
        // start below replaces it, and nothing about the sandbox's data.
        if let Err(error) = self.release_stopped_materialization_pod(sandbox_id).await {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                %error,
                "failed to delete a materialized stopped sandbox's placeholder pod"
            );
        }
        self.store.require_sandbox(sandbox_id).await
    }

    /// One attempt of the drain restore operation's restore of a sandbox a
    /// forced drain suspended, back to `started`, placed as the drain's
    /// evacuation is ([`Self::restore_evacuated_execution`]). It claims the
    /// row first, as a user's restore does ([`K8sStore::claim_drain_restore`]);
    /// a row it can't claim answers [`DrainRestoreAttempt::Refused`] with why,
    /// and nothing was written. Once the node brought the VM up it lands the
    /// row `started` under the claim's lease
    /// ([`K8sStore::complete_drain_restore`]), or reads the row the node's
    /// activation receipt already landed on this restore's execution as
    /// landed, and releases the activation attempt the restore ran under.
    ///
    /// A failed activation is reverted to `suspended` as a failed user
    /// restore's is, so the operation's next attempt claims afresh, but only
    /// while no Wake can have landed ([`drain_restore_timeout_reverts`] and
    /// [`restore_failure_should_revert_to_suspended`]). One that may have
    /// leaves the row `restoring` under the lease and answers the error: the
    /// next attempt finds the row held and waits, until the node's receipt
    /// lands it `started` or the lease lapses and the transient-lifecycle
    /// sweeper settles it.
    pub async fn restore_drained_execution(
        &self,
        metric_identity: crate::metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        options: ActivationOptions,
        placement: Option<&EvacuationPlacement>,
        restore: DrainRestore,
    ) -> Result<DrainRestoreAttempt, SubstrateError> {
        use pplx_space_sandbox_store::drain_restore::DrainRestoreClaim;
        match self.store.claim_drain_restore(sandbox_id, restore).await? {
            DrainRestoreClaim::Claimed => {}
            DrainRestoreClaim::Refused(refusal) => {
                return Ok(DrainRestoreAttempt::Refused(refusal));
            }
        }
        let activation = match self
            .restore_execution_activation(
                metric_identity,
                sandbox_id,
                options,
                RestoreIntent::drain_operation(placement.cloned(), restore),
            )
            .await?
        {
            RestoreActivation::Owned(activation) => activation,
            // A drain restore's claim neither joins an in-flight restore nor
            // a completed one.
            RestoreActivation::Joined => {
                return Err(SubstrateError::Failed {
                    message: format!("drain restore of sandbox {sandbox_id} joined a restore"),
                });
            }
        };
        let generation = i64::try_from(activation.binding.execution_generation).map_err(|_| {
            SubstrateError::Failed {
                message: format!(
                    "drain restore of sandbox {sandbox_id} ran on execution generation {}, \
                     above i64::MAX",
                    activation.binding.execution_generation
                ),
            }
        })?;
        let attempt = activation
            .lease_completion
            .filter(|completion| completion.generation == generation)
            .map(|completion| completion.attempt_id);
        Ok(
            if self
                .store
                .complete_drain_restore(sandbox_id, restore, generation, attempt)
                .await?
                .landed()
            {
                DrainRestoreAttempt::Landed
            } else {
                DrainRestoreAttempt::Lost
            },
        )
    }
}
