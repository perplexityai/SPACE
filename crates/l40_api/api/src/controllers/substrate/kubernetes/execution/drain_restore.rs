use super::*;

impl KubernetesSubstrate {
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
