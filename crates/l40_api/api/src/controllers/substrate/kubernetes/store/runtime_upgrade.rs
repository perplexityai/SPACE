//! The evidence a resume- or start-time runtime upgrade is decided from: the
//! execution a sandbox last ran or checkpointed under, and the environment
//! catalog's certified edges.

use super::*;
use crate::controllers::substrate::runtime_upgrade::{CatalogEnvironment, RecordedExecution};
use pplx_space_execution_spec::{ArtifactComponentKind, EnvironmentReference};
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
        let metadata = metadata.as_ref();
        // Read leniently: a record is evidence, and a field it lacks or holds
        // in another shape matches anything rather than failing the
        // activation.
        let environment = metadata
            .and_then(|metadata| metadata.get("environment"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        let kernel_version = metadata
            .and_then(|metadata| metadata.get("components"))
            .and_then(serde_json::Value::as_array)
            .and_then(|components| {
                components.iter().find(|component| {
                    component.get("kind").and_then(serde_json::Value::as_str)
                        == Some(ArtifactComponentKind::Kernel.as_str())
                })
            })
            .and_then(|kernel| kernel.get("version"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        Self {
            subject: row.subject,
            local_artifact_id: row.local_artifact_id,
            execution: RecordedExecution {
                environment,
                runtime: row.runtime,
                runtime_version: row.runtime_version,
                kernel_version,
                state_format_name: row.state_format_name,
                state_format_version: row.state_format_version,
            },
        }
    }
}

fn catalog_reference(value: &str) -> Result<EnvironmentReference, SubstrateError> {
    value.parse().map_err(|error| SubstrateError::Failed {
        message: format!("runtime_environments holds an invalid reference {value:?}: {error}"),
    })
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
        let row: Option<RecordedExecutionRow> = sqlx::query_as(sql!(
            "SELECT sn.id::text AS subject, sn.local_artifact_id, sn.execution_metadata, sn.runtime,
                    sn.runtime_version, sn.state_format_name, sn.state_format_version
               FROM snapshots sn
               JOIN sandboxes s ON s.id = sn.sandbox_id
              WHERE sn.sandbox_id = $1
                AND sn.type = 'full'
                AND CASE WHEN $2::text IS NULL
                         THEN sn.local_artifact_id = s.current_local_artifact_id
                         ELSE EXISTS (
                             SELECT 1
                               FROM snapshot_storage_locations ssl
                              WHERE ssl.snapshot_id = sn.id
                                AND ssl.storage_connection_string = $2)
                    END
              ORDER BY sn.timestamp DESC
              LIMIT 1"
        ))
        .bind(sandbox_id)
        .bind(storage_uri)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to read the restore checkpoint's execution")?;
        Ok(row.map(Into::into))
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
                Ok(CatalogEnvironment {
                    reference: catalog_reference(&row.environment_ref)?,
                    runtime: row.runtime,
                    runtime_version: row.runtime_version,
                    kernel_version: row.kernel_version,
                    state_format_name: row.state_format_name,
                    state_format_version: row.state_format_version,
                    upgrade_edges: row
                        .upgrade_edges
                        .iter()
                        .map(|edge| catalog_reference(edge))
                        .collect::<Result<_, _>>()?,
                    memory_edges: row
                        .memory_edges
                        .iter()
                        .map(|edge| catalog_reference(edge))
                        .collect::<Result<_, _>>()?,
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
