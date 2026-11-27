//! The locking read that picks the sandbox a ready suspend backup
//! (`FinalizeArtifactBackup`) projects to `suspended`. The projection itself
//! is [`crate::ingest::finalize_suspended`], run by the caller in the same
//! transaction under this lock.

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::{LocalArtifactId, SnapshotId};

use std::sync::LazyLock;

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_portability::{
    started_suspend_finalize_eligibility, suspend_snapshot_is_current_capture,
    suspended_projection_lifecycle_eligible,
};
use pplx_space_sql_locator::sql;

/// The sandbox a ready suspend snapshot may project, from
/// [`lock_candidates`].
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct ReadySuspendProjectionCandidate {
    #[sqlx(rename = "id")]
    pub sandbox_id: SandboxId,
    /// Whether the projection clears the current local artifact marker.
    pub clear_marker: bool,
    /// The snapshot's source node, which the sandbox is still active on.
    pub source_node_id: NodeId,
    /// The row's stored lifecycle, the one the projection would leave.
    pub lifecycle: String,
    /// The snapshot's artifact when it's this suspend's capture: nothing is
    /// newer ([`suspend_snapshot_is_current_capture`]), so the projection
    /// pins the row to it and the restore loads exactly it.
    pub captured_artifact: Option<LocalArtifactId>,
}

/// Lock the sandbox that the ready full suspend snapshot `snapshot_id`
/// may project to `suspended`, and return it: the snapshot has a storage
/// location, its sandbox is still active on its source node, the sandbox's
/// lifecycle admits the projection ([`suspended_projection_lifecycle_eligible`],
/// with the row's suspend-origin marker on this snapshot's artifact as
/// committed evidence), and [`started_suspend_finalize_eligibility`] holds:
/// finalizing records the node's suspend and chooses no restore source.
/// Empty when nothing qualifies.
///
/// `FOR UPDATE OF s`: the sandbox row lock lasts until the caller's
/// transaction ends, so the caller's projection in the same transaction
/// writes the row this read decided on. The caller must pass its open
/// transaction.
pub async fn lock_candidates<C>(
    tx: &mut C,
    snapshot_id: SnapshotId,
) -> Result<Vec<ReadySuspendProjectionCandidate>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    /// Built once: every input is a literal or a bind placeholder.
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        let eligibility = started_suspend_finalize_eligibility("s");
        let current_capture = suspend_snapshot_is_current_capture("s", "sn");
        let lifecycle_eligible = suspended_projection_lifecycle_eligible(
            "s",
            "s.current_local_artifact_id = sn.local_artifact_id
             AND s.current_local_artifact_kind = sn.artifact_kind
             AND s.current_local_artifact_origin = 'suspend'",
        );
        sql!(format!(
            "SELECT s.id,
                    {clear_marker} AS clear_marker,
                    sn.source_node_id,
                    s.lifecycle,
                    CASE WHEN {current_capture} THEN sn.local_artifact_id END
                        AS captured_artifact
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
            clear_marker = eligibility.clear_marker,
            eligible = eligibility.eligible,
        ))
    });
    Ok(sqlx::query_as(sqlx::AssertSqlSafe(QUERY.as_str()))
        .bind(snapshot_id)
        .fetch_all(tx.into_sqlx())
        .await?)
}
