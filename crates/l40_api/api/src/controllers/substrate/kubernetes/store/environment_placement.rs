//! What a sandbox pod's work is bound to, for the environment placement
//! gate: the template build's binding, or a fork's source snapshot's.

use super::*;
use crate::controllers::substrate::environment_placement::EnvironmentPlacementGate;

impl K8sStore {
    /// The environment work that boots `template` at `content_hash` runs
    /// under `gate`, unparsed: with the gate on, the binding of the release a
    /// create resolves
    /// ([`crate::controllers::substrate::environment_placement::resolved_work_environment`]),
    /// else the named release's.
    pub(in crate::controllers::substrate::kubernetes) async fn work_environment(
        &self,
        gate: EnvironmentPlacementGate,
        template: &str,
        content_hash: &str,
    ) -> Result<Option<String>, SubstrateError> {
        // A pod may land on any node, so no candidate's own inventory row can
        // gate it: the build's holders have to agree.
        let scope =
            crate::controllers::substrate::environment_placement::BindingScope::ReleaseOrHolders;
        // A pod requires its work's environment only with the gate on, so
        // only then does it boot the twin a create resolves to; otherwise it
        // boots the named release's build, whose binding it renders.
        if gate.refuses() {
            crate::controllers::substrate::environment_placement::resolved_work_environment(
                gate,
                &self.pool,
                template,
                Some(content_hash),
                scope,
            )
            .await
        } else {
            crate::controllers::substrate::environment_placement::work_environment(
                gate,
                &self.pool,
                template,
                Some(content_hash),
                scope,
            )
            .await
        }
        .db_context("failed to read the template build's environment")
    }

    /// The environment snapshot `snapshot_id` was produced under, unparsed;
    /// `None` when it records none or there's no such snapshot. A fork boots
    /// its source's disk artifact, so this is the fork's binding.
    pub(in crate::controllers::substrate::kubernetes) async fn snapshot_environment(
        &self,
        snapshot_id: &str,
    ) -> Result<Option<String>, SubstrateError> {
        let snapshot_id: Uuid = snapshot_id
            .parse()
            .map_err(|error| SubstrateError::Failed {
                message: format!("fork source snapshot id {snapshot_id:?} isn't a UUID: {error}"),
            })?;
        pplx_space_snapshot_store::environment::of_snapshot(
            &self.pool,
            pplx_space_snapshot_model::SnapshotId::from_uuid(snapshot_id),
        )
        .await
        .db_context("failed to read the fork source snapshot's environment")
    }

    /// The environment the ready disk snapshot of `sandbox_id`'s local
    /// artifact `artifact_id` was produced under, unparsed; `None` when it
    /// records none or there's no such snapshot.
    pub(in crate::controllers::substrate::kubernetes) async fn disk_artifact_environment(
        &self,
        sandbox_id: SandboxId,
        artifact_id: LocalArtifactId,
    ) -> Result<Option<String>, SubstrateError> {
        pplx_space_snapshot_store::environment::of_ready_disk_artifact(
            &self.pool,
            sandbox_id,
            artifact_id,
        )
        .await
        .db_context("failed to read the pinned disk artifact's environment")
    }

    /// [`Self::disk_artifact_environment`] for `sandbox_id`'s current local
    /// artifact.
    pub(in crate::controllers::substrate::kubernetes) async fn current_disk_artifact_environment(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<String>, SubstrateError> {
        pplx_space_snapshot_store::environment::of_current_ready_disk_artifact(
            &self.pool, sandbox_id,
        )
        .await
        .db_context("failed to read the stopped disk artifact's environment")
    }
}
