//! The start of a stopped sandbox a v2 drain released from its node: its
//! disk is materialized on any node first.

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
}
