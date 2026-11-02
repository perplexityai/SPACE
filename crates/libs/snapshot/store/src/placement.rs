//! Where each catalog snapshot's bytes are: the object store, the node that
//! holds the sandbox's current local artifact, both, or neither.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId, SandboxLifecycle, SnapshotId};
use sqlx::FromRow;

/// Where one catalog snapshot's bytes are, as the catalog records it.
#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct SnapshotStoragePlacement {
    pub snapshot_id: SnapshotId,
    /// The snapshot has a durable copy, as
    /// `pplx_space_sandbox_portability::snapshot_has_durable_copy` defines it
    /// (`ready`, with a storage location that is not orphaned): its bytes are
    /// in the object store.
    pub in_object_store: bool,
    /// The node holding the bytes locally, when all of these hold:
    ///
    /// - the snapshot is the sandbox's current local artifact;
    /// - the sandbox is settled in [`NODE_LOCAL_LIFECYCLES`];
    /// - its node (target, else active) is the node the snapshot uploaded
    ///   from (`snapshots.source_node_id`).
    ///
    /// Otherwise `None`, including when `source_node_id` is NULL: the catalog
    /// cannot place the copy, and no guess is reported.
    pub node_local_node_id: Option<NodeId>,
}

/// The lifecycles in which the sandbox's placement columns can name the node
/// holding its current local artifact. In the others a move may be in flight:
/// a restore onto another node writes the destination into both
/// `active_node_id` and `target_node_id` once its pod is scheduled, before that
/// node holds any bytes, while the artifact marker still names the source's
/// copy.
///
/// Settling is not enough on its own: a failed restore onto another node
/// returns the row to `suspended` with the placement columns still naming the
/// destination, as do the sweeper's revert and unplace paths. So the
/// placement node must also be the node the snapshot uploaded from.
pub const NODE_LOCAL_LIFECYCLES: [SandboxLifecycle; 2] =
    [SandboxLifecycle::Suspended, SandboxLifecycle::Stopped];

/// One statement, built once, so the durable-copy and current-artifact tests
/// are the shared portability fragments rather than second spellings of them.
/// "The sandbox's node" is its target, else its active node, as restore picks
/// its source.
static STORAGE_PLACEMENTS_SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let durable_copy = pplx_space_sandbox_portability::snapshot_has_durable_copy("s");
    let current_artifact =
        pplx_space_sandbox_portability::snapshot_is_current_local_artifact("sb", "s");
    format!(
        "SELECT s.id AS snapshot_id,
                {durable_copy} AS in_object_store,
                CASE WHEN {current_artifact}
                          AND sb.lifecycle = ANY($2)
                          AND s.source_node_id = COALESCE(sb.target_node_id, sb.active_node_id)
                     THEN s.source_node_id
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
