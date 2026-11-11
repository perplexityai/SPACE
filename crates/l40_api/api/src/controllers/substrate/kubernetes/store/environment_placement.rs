//! What a sandbox pod's work is bound to, for the environment placement
//! gate: the template build's binding, or a fork's source snapshot's.

use super::*;
use crate::controllers::substrate::environment_placement::EnvironmentPlacementGate;

impl K8sStore {
    /// The environment work that boots `template` at `content_hash` runs
    /// under `gate`, unparsed
    /// ([`crate::controllers::substrate::environment_placement::work_environment`]).
    pub(in crate::controllers::substrate::kubernetes) async fn work_environment(
        &self,
        gate: EnvironmentPlacementGate,
        template: &str,
        content_hash: &str,
    ) -> Result<Option<String>, SubstrateError> {
        crate::controllers::substrate::environment_placement::work_environment(
            gate,
            &self.pool,
            template,
            Some(content_hash),
            // A pod may land on any node, so no candidate's own inventory row
            // can gate it: the build's holders have to agree.
            crate::controllers::substrate::environment_placement::BindingScope::ReleaseOrHolders,
        )
        .await
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
        Ok(sqlx::query_scalar::<_, Option<String>>(sql!(
            "SELECT execution_metadata->>'environment' FROM snapshots WHERE id = $1"
        ))
        .bind(snapshot_id)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to read the fork source snapshot's environment")?
        .flatten())
    }

    /// The environment the ready disk snapshot of `sandbox_id`'s local
    /// artifact `artifact_id` was produced under, unparsed; `None` when it
    /// records none or there's no such snapshot.
    pub(in crate::controllers::substrate::kubernetes) async fn disk_artifact_environment(
        &self,
        sandbox_id: SandboxId,
        artifact_id: LocalArtifactId,
    ) -> Result<Option<String>, SubstrateError> {
        Ok(sqlx::query_scalar::<_, Option<String>>(sql!(
            "SELECT execution_metadata->>'environment'
               FROM snapshots
              WHERE sandbox_id = $1
                AND local_artifact_id = $2
                AND type = 'disk'
                AND state = 'ready'
              ORDER BY timestamp DESC
              LIMIT 1"
        ))
        .bind(sandbox_id)
        .bind(artifact_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to read the pinned disk artifact's environment")?
        .flatten())
    }

    /// [`Self::disk_artifact_environment`] for `sandbox_id`'s current local
    /// artifact.
    pub(in crate::controllers::substrate::kubernetes) async fn current_disk_artifact_environment(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<String>, SubstrateError> {
        Ok(sqlx::query_scalar::<_, Option<String>>(sql!(
            "SELECT sn.execution_metadata->>'environment'
               FROM sandboxes s
               JOIN snapshots sn
                 ON sn.sandbox_id = s.id
                AND sn.local_artifact_id = s.current_local_artifact_id
              WHERE s.id = $1
                AND sn.type = 'disk'
                AND sn.state = 'ready'
              ORDER BY sn.timestamp DESC
              LIMIT 1"
        ))
        .bind(sandbox_id)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to read the stopped disk artifact's environment")?
        .flatten())
    }
}
