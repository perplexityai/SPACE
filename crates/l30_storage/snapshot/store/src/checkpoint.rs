//! The execution a restore's checkpoint recorded it was produced under, for
//! the runtime upgrade decision.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::LocalArtifactId;
use pplx_space_sql_locator::sql;
use sqlx::FromRow;
use sqlx::types::Json;

/// A checkpoint's producer evidence, with its snapshot id as text in
/// `subject`. The execution columns are left unparsed: a record is evidence,
/// and the caller reads it leniently.
#[derive(Clone, Debug)]
pub struct CheckpointExecution {
    pub subject: String,
    pub local_artifact_id: Option<LocalArtifactId>,
    pub execution_metadata: Option<serde_json::Value>,
    pub runtime: Option<String>,
    pub runtime_version: Option<String>,
    pub state_format_name: Option<String>,
    pub state_format_version: Option<String>,
}

#[derive(FromRow)]
struct CheckpointExecutionRow {
    subject: String,
    local_artifact_id: Option<LocalArtifactId>,
    execution_metadata: Option<Json<serde_json::Value>>,
    runtime: Option<String>,
    runtime_version: Option<String>,
    state_format_name: Option<String>,
    state_format_version: Option<String>,
}

impl From<CheckpointExecutionRow> for CheckpointExecution {
    fn from(row: CheckpointExecutionRow) -> Self {
        Self {
            subject: row.subject,
            local_artifact_id: row.local_artifact_id,
            execution_metadata: row.execution_metadata.map(|json| json.0),
            runtime: row.runtime,
            runtime_version: row.runtime_version,
            state_format_name: row.state_format_name,
            state_format_version: row.state_format_version,
        }
    }
}

/// The newest full snapshot of `sandbox_id` with a storage location at
/// `storage_uri`, or, when `storage_uri` is `None`, the newest full snapshot
/// of the sandbox's current local artifact. `None` when no snapshot matches:
/// a `storage_uri` that no location records never falls back to the current
/// artifact.
pub async fn restore_execution(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    storage_uri: Option<&str>,
) -> Result<Option<CheckpointExecution>, DbError> {
    let row = sqlx::query_as::<_, CheckpointExecutionRow>(sql!(
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
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(row.map(CheckpointExecution::from))
}
