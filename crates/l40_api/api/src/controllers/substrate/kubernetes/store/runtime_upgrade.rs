//! The evidence a resume- or start-time runtime upgrade is decided from: the
//! execution a sandbox last ran or checkpointed under, and the environment
//! catalog's certified edges.

use super::*;
use crate::controllers::substrate::runtime_upgrade::{CatalogEnvironment, RecordedExecution};
use pplx_space_snapshot_model::LocalArtifactId;

/// An execution the control plane recorded, with the identity of what it
/// records: the checkpoint's snapshot for a restore, the published execution
/// for a start.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::controllers::substrate::kubernetes) struct RecordedActivation {
    pub(in crate::controllers::substrate::kubernetes) subject: String,
    /// The checkpoint's local artifact, for a restore whose snapshot row
    /// records one.
    pub(in crate::controllers::substrate::kubernetes) local_artifact_id: Option<LocalArtifactId>,
    pub(in crate::controllers::substrate::kubernetes) execution: RecordedExecution,
}

#[derive(sqlx::FromRow)]
struct RecordedExecutionRow {
    subject: String,
    local_artifact_id: Option<LocalArtifactId>,
    execution_metadata: Option<sqlx::types::Json<serde_json::Value>>,
    runtime: Option<String>,
    runtime_version: Option<String>,
    state_format_name: Option<String>,
    state_format_version: Option<String>,
}

impl From<pplx_space_sandbox_store::observed_states::PublishedExecution> for RecordedExecutionRow {
    fn from(row: pplx_space_sandbox_store::observed_states::PublishedExecution) -> Self {
        Self {
            subject: row.subject,
            local_artifact_id: row.local_artifact_id,
            execution_metadata: row.execution_metadata,
            runtime: row.runtime,
            runtime_version: row.runtime_version,
            state_format_name: row.state_format_name,
            state_format_version: row.state_format_version,
        }
    }
}

impl From<RecordedExecutionRow> for RecordedActivation {
    fn from(row: RecordedExecutionRow) -> Self {
        let metadata = row.execution_metadata.map(|metadata| metadata.0);
        Self {
            subject: row.subject,
            local_artifact_id: row.local_artifact_id,
            execution: RecordedExecution::from_record(
                metadata.as_ref(),
                row.runtime,
                row.runtime_version,
                row.state_format_name,
                row.state_format_version,
            ),
        }
    }
}

impl K8sStore {
    /// The producer evidence of the checkpoint a restore of `sandbox_id`
    /// loads: the full snapshot stored at the restore's selected
    /// `storage_uri`, or, for a restore with none, the snapshot of the
    /// sandbox's current local artifact. `None` when no snapshot row records
    /// it. It doesn't select a checkpoint of its own: the restore plan did.
    pub(in crate::controllers::substrate::kubernetes) async fn restore_checkpoint_execution(
        &self,
        sandbox_id: SandboxId,
        storage_uri: Option<&str>,
    ) -> Result<Option<RecordedActivation>, SubstrateError> {
        let row = pplx_space_snapshot_store::checkpoint::restore_execution(
            &self.pool,
            sandbox_id,
            storage_uri,
        )
        .await
        .db_context("failed to read the restore checkpoint's execution")?;
        Ok(row.map(|row| {
            RecordedExecutionRow {
                subject: row.subject,
                local_artifact_id: row.local_artifact_id,
                execution_metadata: row.execution_metadata,
                runtime: row.runtime,
                runtime_version: row.runtime_version,
                state_format_name: row.state_format_name,
                state_format_version: row.state_format_version,
            }
            .into()
        }))
    }

    /// The execution `sandbox_id` last published, which a start of the
    /// stopped sandbox boots under. The subject digests what it records and
    /// when the sandbox entered its lifecycle, so retries of one stopped
    /// period name the same upgrade, and a later stop names a new one even
    /// with the same execution. `None` when nothing is published, or the
    /// publish carried no execution.
    pub(in crate::controllers::substrate::kubernetes) async fn last_published_execution(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RecordedActivation>, SubstrateError> {
        let row = pplx_space_sandbox_store::observed_states::last_published_execution(
            &self.pool, sandbox_id,
        )
        .await
        .db_context("failed to read the sandbox's published execution")?;
        Ok(row.map(|row| RecordedExecutionRow::from(row).into()))
    }

    /// Every environment in the catalog that isn't retired, with its edges.
    pub(in crate::controllers::substrate::kubernetes) async fn unretired_runtime_environments(
        &self,
    ) -> Result<Vec<CatalogEnvironment>, SubstrateError> {
        let rows = pplx_space_runtime_environment_catalog_store::unretired_environments(&self.pool)
            .await
            .db_context("failed to read the runtime environment catalog")?;
        rows.into_iter()
            .map(|row| {
                CatalogEnvironment::from_row(row).map_err(|error| SubstrateError::Failed {
                    message: error.to_string(),
                })
            })
            .collect()
    }

    /// The cluster's runtime environment policy.
    pub(in crate::controllers::substrate::kubernetes) async fn runtime_environment_policy(
        &self,
    ) -> Result<pplx_space_runtime_environment_policy_store::RuntimeEnvironmentPolicy, SubstrateError>
    {
        use pplx_space_runtime_environment_policy_store::GetPolicyError;
        pplx_space_runtime_environment_policy_store::get(&self.pool)
            .await
            .map_err(|error| match error {
                GetPolicyError::Database(error) => {
                    crate::controllers::substrate::substrate_db_error(
                        "failed to read the runtime environment policy",
                        &error,
                    )
                }
                error => SubstrateError::Failed {
                    message: format!("failed to read the runtime environment policy: {error}"),
                },
            })
    }
}
