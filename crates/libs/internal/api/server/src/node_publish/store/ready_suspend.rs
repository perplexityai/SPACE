use pplx_space_common_core::{
    sql,
    types::{NodeId, SandboxId, SnapshotId},
};
use pplx_space_sandbox_portability::{
    started_suspend_revert_eligibility, suspended_projection_lifecycle_eligible,
};
use sqlx::PgConnection;

use super::suspend_finalization::{self, FinalizeSuspensionError, SuspendedFinalizationCandidate};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProjectReadySuspendError {
    #[error("select ready suspend projection: {0}")]
    Select(#[source] sqlx::Error),
    #[error(transparent)]
    Finalize(#[from] FinalizeSuspensionError),
}

/// Project only a ready full suspend snapshot with storage, an owning source
/// node and eligible lifecycle evidence. Lock the selected sandbox before
/// guarded finalization on the same caller connection. No eligible candidate
/// or an unmatched finalization guard returns `None`, not an error.
pub(crate) async fn project(
    conn: &mut PgConnection,
    snapshot_id: SnapshotId,
) -> Result<Option<(SandboxId, NodeId, String)>, ProjectReadySuspendError> {
    let eligibility = started_suspend_revert_eligibility("s");
    let clear_marker = eligibility.clear_marker;
    let eligible = eligibility.eligible;
    let committed_suspend_evidence = "
        s.current_local_artifact_id = sn.local_artifact_id
        AND s.current_local_artifact_kind = sn.artifact_kind
        AND s.current_local_artifact_origin = 'suspend'
    ";
    let lifecycle_eligible =
        suspended_projection_lifecycle_eligible("s", committed_suspend_evidence);
    let candidate_query = format!(
        "SELECT s.id,
                {clear_marker} AS clear_marker,
                sn.source_node_id,
                s.lifecycle
           FROM snapshots sn
           JOIN sandboxes s ON s.id = sn.sandbox_id
          WHERE sn.id = $1
            AND sn.state = 'ready'
            AND sn.type = 'full'
            AND (sn.artifact_origin IS NULL OR sn.artifact_origin = 'suspend')
            AND sn.source_node_id IS NOT NULL
            AND s.active_node_id = sn.source_node_id
            AND {lifecycle_eligible}
            AND EXISTS (
                SELECT 1
                  FROM snapshot_storage_locations ssl
                 WHERE ssl.snapshot_id = sn.id
            )
            AND {eligible}
          FOR UPDATE OF s",
    );
    let candidates: Vec<(SandboxId, bool, NodeId, String)> =
        sqlx::query_as(sqlx::AssertSqlSafe(sql!(candidate_query.as_str())))
            .bind(snapshot_id)
            .fetch_all(&mut *conn)
            .await
            .map_err(ProjectReadySuspendError::Select)?;
    let Some((sandbox_id, clear_marker, source_node_id, from_lifecycle)) =
        candidates.first().cloned()
    else {
        return Ok(None);
    };

    let projected = suspend_finalization::apply(
        conn,
        &[SuspendedFinalizationCandidate {
            sandbox_id,
            clear_marker,
            published_artifact: None,
            published_execution_identity: None,
        }],
    )
    .await?;
    if projected
        .iter()
        .any(|(projected_id, _)| *projected_id == sandbox_id)
    {
        Ok(Some((sandbox_id, source_node_id, from_lifecycle)))
    } else {
        Ok(None)
    }
}
