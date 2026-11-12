//! Reads of node drain's stopped evacuations: where one claim stands, and
//! which claimed evacuation has waited longest for its bytes.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::{
    current_disk_artifact_backup_ready, snapshot_is_current_local_artifact,
};
use pplx_space_snapshot_model::LocalArtifactId;
use pplx_space_sql_locator::sql;

/// Where one sandbox's stopped evacuation stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub struct StoppedEvacuationRow {
    /// The row is still a claimed stopped evacuation on the source node, and
    /// that node is still force-draining.
    pub claim_holds: bool,
    /// [`current_disk_artifact_backup_ready`] for the row.
    pub bytes_durable: bool,
    pub current_local_artifact_id: Option<LocalArtifactId>,
    /// The row carries the stopped evacuation's marker.
    pub marked: bool,
    /// A `creating` snapshot of the pinned artifact exists.
    pub upload_in_flight: bool,
    /// The row is still a `stopped` resident of the source node, and that
    /// node is still force-draining: `claim_holds` without the marker
    /// conjunct, for a step that acts on a row before any claim re-checks the
    /// drain.
    pub resident: bool,
}

/// `$1` = sandbox id, `$2` = source node id.
///
/// `COALESCE(..., FALSE)` because two conjuncts are nullable columns: an
/// unmarked row and a torn-down one have to read as "no claim", not decode as
/// a NULL bool. The force-draining term matches `drain_phase = 'draining'` and
/// `drain_mode = 'forced'` literally: a NULL or unrecognized mode reads as
/// gentle. Its alias is `n_fd` so it can't shadow a joined `nodes n`.
static STATE: LazyLock<String> = LazyLock::new(|| {
    let ready = current_disk_artifact_backup_ready("s");
    let pinned = snapshot_is_current_local_artifact("s", "sn");
    sql!(format!(
        "SELECT COALESCE((
                 s.lifecycle = 'stopped'
                 AND s.evacuation_restore_target = 'stopped'
                 AND s.active_node_id = $2
                 AND EXISTS (
                SELECT 1
                  FROM nodes n_fd
                 WHERE n_fd.id = $2
                   AND n_fd.drain_phase = 'draining'
                   AND n_fd.drain_mode = 'forced'
            )
             ), FALSE) AS claim_holds,
             {ready} AS bytes_durable,
             s.current_local_artifact_id,
             COALESCE(s.evacuation_restore_target = 'stopped', FALSE) AS marked,
             EXISTS (
                 SELECT 1
                   FROM snapshots sn
                  WHERE sn.sandbox_id = s.id
                    AND {pinned}
                    AND sn.state = 'creating'
             ) AS upload_in_flight,
             COALESCE((
                 s.lifecycle = 'stopped'
                 AND s.active_node_id = $2
                 AND EXISTS (
                SELECT 1
                  FROM nodes n_fd
                 WHERE n_fd.id = $2
                   AND n_fd.drain_phase = 'draining'
                   AND n_fd.drain_mode = 'forced'
            )
             ), FALSE) AS resident
             FROM sandboxes s
             WHERE s.id = $1"
    ))
});

/// Where `sandbox_id`'s stopped evacuation off `source_node_id` stands, or
/// `None` when the row is gone. A read; it takes no lock and has no freshness
/// term (the claim guarantees what it can see was minted by this evacuation).
pub async fn state(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
) -> Result<Option<StoppedEvacuationRow>, DbError> {
    Ok(sqlx::query_as(sqlx::AssertSqlSafe(STATE.as_str()))
        .bind(sandbox_id)
        .bind(source_node_id)
        .fetch_optional(executor.into_sqlx())
        .await?)
}

/// The claimed stopped evacuation that has waited longest for its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub struct LongestStall {
    /// Seconds by the database clock since the pinned artifact's earliest
    /// catalog registration, else the drain's start, else the stop.
    pub stall_secs: i64,
    pub sandbox_id: SandboxId,
    pub node_id: NodeId,
}

/// No binds. Three clocks in order, so the age never resolves to NULL; `MIN`
/// because re-registering an artifact adds a catalog row. Forced drains only,
/// with the force-draining term spelled as in [`STATE`] over the joined node.
/// `s.id DESC` breaks ties between evacuations that started in the same
/// second, so the row reported stays the same for an unchanged incident.
static LONGEST_STALL: LazyLock<String> = LazyLock::new(|| {
    let pinned = snapshot_is_current_local_artifact("s", "sn");
    sql!(format!(
        "SELECT EXTRACT(EPOCH FROM (NOW() - COALESCE(
                    (SELECT MIN(sn.timestamp)
                       FROM snapshots sn
                      WHERE sn.sandbox_id = s.id
                        AND {pinned}),
                    n.drain_started_at,
                    s.lifecycle_changed_at
                )))::BIGINT AS stall_secs,
                s.id AS sandbox_id,
                n.id AS node_id
           FROM sandboxes s
           JOIN nodes n ON n.id = s.active_node_id
          WHERE s.lifecycle = 'stopped'
            AND EXISTS (
                SELECT 1
                  FROM nodes n_fd
                 WHERE n_fd.id = n.id
                   AND n_fd.drain_phase = 'draining'
                   AND n_fd.drain_mode = 'forced'
            )
            AND s.evacuation_restore_target = 'stopped'
          ORDER BY stall_secs DESC, s.id DESC
          LIMIT 1"
    ))
});

/// The claimed stopped evacuation on a force-draining node that has waited
/// longest for its bytes, or `None` when none is waiting. A read; it takes no
/// lock, and its age is measured against the executor's `NOW()`.
pub async fn longest_stall(executor: impl Executor<'_>) -> Result<Option<LongestStall>, DbError> {
    Ok(sqlx::query_as(sqlx::AssertSqlSafe(LONGEST_STALL.as_str()))
        .fetch_optional(executor.into_sqlx())
        .await?)
}
