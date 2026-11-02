//! Where each catalog snapshot's bytes are: the object store, the node that
//! holds the sandbox's current local artifact, both, or neither.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId, SandboxLifecycle, SnapshotId};
use sqlx::FromRow;

/// Where one catalog snapshot's bytes are, as the catalog records it.
#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct SnapshotStoragePlacement {
    pub snapshot_id: SnapshotId,
    /// A `ready` snapshot with a live (not orphaned) storage location: its
    /// bytes are in the object store.
    pub in_object_store: bool,
    /// The node holding the bytes locally: the sandbox's node, when this
    /// snapshot is the current local artifact that node published for it and
    /// the sandbox is settled in a lifecycle that keeps it there
    /// ([`NODE_LOCAL_LIFECYCLES`]). `None` for an older artifact, a sandbox
    /// placed on no node, and a sandbox in any other lifecycle.
    pub node_local_node_id: Option<NodeId>,
}

/// The lifecycles in which the sandbox's placement columns name the node
/// holding its current local artifact. In the others the row may already
/// record a different node: a restore onto another node writes the
/// destination into both `active_node_id` and `target_node_id` once its pod
/// is scheduled, before that node holds any bytes, and the artifact marker
/// still names the source's copy. The row does not record which node
/// published the marker, so a move in flight reports no node-local copy
/// rather than a wrong one.
pub const NODE_LOCAL_LIFECYCLES: [SandboxLifecycle; 2] =
    [SandboxLifecycle::Suspended, SandboxLifecycle::Stopped];

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
                CASE WHEN {current_artifact} AND sb.lifecycle = ANY($2)
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
    .bind(NODE_LOCAL_LIFECYCLES.map(SandboxLifecycle::as_str).to_vec())
    .fetch_all(executor)
    .await
}
