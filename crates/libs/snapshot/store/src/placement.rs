//! Where each catalog snapshot's bytes are: the object store, the node that
//! holds the sandbox's current local artifact, both, or neither.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId, SnapshotId};
use sqlx::FromRow;

/// Where one catalog snapshot's bytes are, as the catalog records it.
#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct SnapshotStoragePlacement {
    pub snapshot_id: SnapshotId,
    /// A `ready` snapshot with a live (not orphaned) storage location: its
    /// bytes are in the object store.
    pub in_object_store: bool,
    /// The node holding the bytes locally: the sandbox's node, when this
    /// snapshot is the current local artifact that node published for it.
    /// `None` for an older artifact, or a sandbox placed on no node.
    pub node_local_node_id: Option<NodeId>,
}

/// One statement, built once, so the current-artifact test is the shared
/// portability fragment rather than a second spelling of it. "The sandbox's
/// node" is its target, else its active node, as restore picks its source.
static STORAGE_PLACEMENTS_SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let current_artifact =
        pplx_space_sandbox_portability::snapshot_is_current_local_artifact("sb", "s");
    format!(
        "SELECT s.id AS snapshot_id,
                (s.state = 'ready' AND EXISTS (
                    SELECT 1
                      FROM snapshot_storage_locations ssl
                     WHERE ssl.snapshot_id = s.id
                       AND ssl.orphaned_at IS NULL
                )) AS in_object_store,
                CASE WHEN {current_artifact}
                     THEN COALESCE(sb.target_node_id, sb.active_node_id)
                END AS node_local_node_id
           FROM snapshots s
           JOIN sandboxes sb ON sb.id = s.sandbox_id
          WHERE s.sandbox_id = $1
          ORDER BY s.timestamp DESC, s.id DESC"
    )
});

/// The storage placement of every catalog snapshot of `sandbox_id`, newest
/// first. A snapshot can be in both places, or in neither: still uploading,
/// failed, or an older artifact whose stored copy has been orphaned.
pub async fn storage_placements(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Vec<SnapshotStoragePlacement>, sqlx::Error> {
    sqlx::query_as::<_, SnapshotStoragePlacement>(sqlx::AssertSqlSafe(sql!(
        STORAGE_PLACEMENTS_SQL.as_str()
    )))
    .bind(sandbox_id)
    .fetch_all(executor)
    .await
}
