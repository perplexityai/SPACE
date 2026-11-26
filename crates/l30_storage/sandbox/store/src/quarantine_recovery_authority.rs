//! What a quarantine recovery plan is authorized by: the sandbox's active
//! quarantine out of `restoring`, the node it is active on, its active
//! execution and its local-artifact marker.

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::Executor;
use pplx_space_sandbox_model::{SandboxId, SandboxQuarantineSource};
use pplx_space_snapshot_model::LocalArtifactId;
use pplx_space_sql_locator::sql;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::SandboxStoreError;

/// One quarantined sandbox's recovery authority.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct QuarantineRecoveryAuthority {
    pub quarantine_id: Uuid,
    pub reason_code: String,
    pub entered_at: OffsetDateTime,
    pub active_node_id: Option<NodeId>,
    /// The active execution's generation; `None` without one.
    pub execution_generation: Option<i64>,
    pub current_local_artifact_id: Option<LocalArtifactId>,
    pub current_local_artifact_kind: Option<String>,
    pub current_local_artifact_origin: Option<String>,
}

/// The recovery authority of `sandbox_id` when it is `quarantined` by a
/// quarantine `source` entered from `restoring`; `None` otherwise. Not scoped
/// to a node: the caller tells a missing quarantine, a missing active node
/// and another node's sandbox apart.
pub async fn get(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    source: SandboxQuarantineSource,
) -> Result<Option<QuarantineRecoveryAuthority>, SandboxStoreError> {
    Ok(sqlx::query_as::<_, QuarantineRecoveryAuthority>(sql!(
        "SELECT quarantine.quarantine_id,
                quarantine.reason_code,
                quarantine.entered_at,
                sandbox.active_node_id,
                execution.generation AS execution_generation,
                sandbox.current_local_artifact_id,
                sandbox.current_local_artifact_kind,
                sandbox.current_local_artifact_origin
           FROM sandboxes sandbox
           JOIN sandbox_quarantine_state quarantine
             ON quarantine.sandbox_id = sandbox.id
           LEFT JOIN sandbox_k8s_executions execution
             ON execution.sandbox_id = sandbox.id AND execution.active
          WHERE sandbox.id = $1
            AND sandbox.lifecycle = 'quarantined'
            AND quarantine.source = $2
            AND quarantine.entered_from_lifecycle = 'restoring'"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(source.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?)
}
