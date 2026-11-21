//! The operator's way out of a refused restore: list the captures a sandbox
//! could restore from, and pin one.
//!
//! An unpinned sandbox whose history is unordered
//! ([`pplx_space_sandbox_portability::restore_order_unordered`]) refuses to
//! restore off its node, since capture order can't tell which of its
//! captures is the latest. The restore-pin repair resolves it while the node
//! still holds the sandbox. When it doesn't, an operator who knows which
//! capture holds the sandbox's latest state pins it here, and the restore
//! loads exactly that artifact.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::{
    capture_rank, capture_registered_at, full_snapshot_outranked_by_live_capture,
    restore_order_unordered,
};
use pplx_space_snapshot_model::LocalArtifactId;
use pplx_space_sql_locator::sql;

/// A sandbox's restore state, as an operator choosing a pin sees it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestorePinState {
    pub lifecycle: String,
    /// The token [`pin_restore_source`] requires unchanged.
    pub lifecycle_changed_at: Option<time::OffsetDateTime>,
    pub pinned_artifact: Option<LocalArtifactId>,
    /// The node a restore would load the local copy from, resolved as the
    /// Kubernetes restore resolves its source node: the placement's
    /// (`target_node_id`, else `active_node_id`) mapped node, else the node the
    /// newest execution named. A retire that keeps the sandbox recoverable
    /// clears the placement but keeps that execution, and a node holding a
    /// local copy restores it in place of any pinned backup. Falls back to the
    /// placement for a node with no Kubernetes mapping.
    pub restore_node_id: Option<NodeId>,
    /// The unpinned restore refuses: its captures can't be ordered.
    pub unordered: bool,
    /// The captures [`pin_restore_source`] accepts, latest first registration
    /// first.
    pub captures: Vec<PinnableCapture>,
}

/// One capture an operator may pin: a full suspend artifact with a ready
/// backup and a live location, that no capture outranks and no later
/// capture of the same rank follows.
#[derive(Clone, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct PinnableCapture {
    pub local_artifact_id: LocalArtifactId,
    /// The capture generation its backups record, if any: the one any backup
    /// of the artifact records, as [`capture_rank`] reads it.
    pub generation: Option<i64>,
    /// When its first backup registered.
    pub first_registered_at: time::OffsetDateTime,
    /// The node its newest ready backup was uploaded from.
    pub source_node_id: Option<NodeId>,
}

#[derive(sqlx::FromRow)]
struct StateRow {
    lifecycle: String,
    lifecycle_changed_at: Option<time::OffsetDateTime>,
    current_local_artifact_id: Option<LocalArtifactId>,
    restore_node_id: Option<NodeId>,
    unordered: bool,
}

/// Returns a boolean SQL fragment: the snapshot aliased as `snapshot_alias`
/// is a ready full backup, with a live location, of a full suspend artifact
/// whose capture is one the selector could take: no capture that's uploading
/// or ready outranks it by capture generation (a failed one, which won't
/// land, leaves the operator the choice), and no full suspend capture of
/// another artifact with the same rank ([`capture_rank`]), uploading or
/// ready, first registered at or after it. In an unordered history that's the
/// latest capture of each side.
fn pinnable_capture(snapshot_alias: &str) -> String {
    let outranked = full_snapshot_outranked_by_live_capture("s", snapshot_alias);
    let rank = capture_rank(snapshot_alias);
    let captured = capture_registered_at(snapshot_alias);
    let later_rank = capture_rank("later");
    let later_captured = capture_registered_at("later");
    format!(
        "({snapshot_alias}.sandbox_id = s.id
          AND {snapshot_alias}.type = 'full'
          AND {snapshot_alias}.state = 'ready'
          AND {snapshot_alias}.local_artifact_id IS NOT NULL
          AND {snapshot_alias}.artifact_kind = 'full'
          AND {snapshot_alias}.artifact_origin = 'suspend'
          AND EXISTS (
              SELECT 1
                FROM snapshot_storage_locations pinnable_ssl
               WHERE pinnable_ssl.snapshot_id = {snapshot_alias}.id
                 AND pinnable_ssl.orphaned_at IS NULL
          )
          AND NOT {outranked}
          AND NOT EXISTS (
              SELECT 1
                FROM snapshots later
               WHERE later.sandbox_id = s.id
                 AND later.type = 'full'
                 AND (later.artifact_origin IS NULL OR later.artifact_origin = 'suspend')
                 AND later.local_artifact_id IS DISTINCT FROM {snapshot_alias}.local_artifact_id
                 AND {later_rank} IS NOT DISTINCT FROM {rank}
                 AND later.state <> 'failed'
                 AND {later_captured} >= {captured}
          ))"
    )
}

/// Read `sandbox_id`'s restore state and the captures an operator may pin.
/// `None` for a missing sandbox.
pub async fn restore_pin_state(
    executor: impl sqlx::PgExecutor<'_> + Copy,
    sandbox_id: SandboxId,
) -> Result<Option<RestorePinState>, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static STATE: LazyLock<String> = LazyLock::new(|| {
        let unordered = restore_order_unordered("s");
        sql!(format!(
            "SELECT s.lifecycle, s.lifecycle_changed_at, s.current_local_artifact_id,
                    COALESCE(
                        (SELECT nk.node_id
                           FROM node_k8s nk
                          WHERE nk.node_id = COALESCE(s.target_node_id, s.active_node_id)),
                        (SELECT nk.node_id
                           FROM node_k8s nk
                           JOIN nodes n ON n.id = nk.node_id
                          WHERE nk.k8s_node_name = (
                                SELECT ske.k8s_node_name
                                  FROM sandbox_k8s_executions ske
                                 WHERE ske.sandbox_id = s.id
                                   AND ske.k8s_node_name IS NOT NULL
                                 ORDER BY ske.generation DESC
                                 LIMIT 1)
                          ORDER BY n.state = 'ready' DESC
                          LIMIT 1),
                        s.target_node_id,
                        s.active_node_id
                    ) AS restore_node_id,
                    {unordered} AS unordered
               FROM sandboxes s
              WHERE s.id = $1"
        ))
    });
    /// Built once: every input is a literal or a bind placeholder.
    static CAPTURES: LazyLock<String> = LazyLock::new(|| {
        let pinnable = pinnable_capture("sn");
        let captured = capture_registered_at("sn");
        sql!(format!(
            "SELECT DISTINCT ON (sn.local_artifact_id)
                    sn.local_artifact_id,
                    COALESCE(sn.generation, (
                        SELECT MIN(same_artifact.generation)
                          FROM snapshots same_artifact
                         WHERE same_artifact.sandbox_id = s.id
                           AND same_artifact.type = sn.type
                           AND same_artifact.local_artifact_id = sn.local_artifact_id
                           AND same_artifact.artifact_kind IS NOT DISTINCT FROM sn.artifact_kind
                           AND same_artifact.generation IS NOT NULL
                    )) AS generation,
                    {captured} AS first_registered_at,
                    sn.source_node_id
               FROM sandboxes s
               JOIN snapshots sn ON sn.sandbox_id = s.id
              WHERE s.id = $1
                AND {pinnable}
              ORDER BY sn.local_artifact_id, sn.timestamp DESC, sn.id DESC"
        ))
    });
    let Some(state) = sqlx::query_as::<_, StateRow>(sqlx::AssertSqlSafe(STATE.as_str()))
        .bind(sandbox_id)
        .fetch_optional(executor)
        .await?
    else {
        return Ok(None);
    };
    let mut captures: Vec<PinnableCapture> = sqlx::query_as(sqlx::AssertSqlSafe(CAPTURES.as_str()))
        .bind(sandbox_id)
        .fetch_all(executor)
        .await?;
    captures.sort_by_key(|capture| std::cmp::Reverse(capture.first_registered_at));
    Ok(Some(RestorePinState {
        lifecycle: state.lifecycle,
        lifecycle_changed_at: state.lifecycle_changed_at,
        pinned_artifact: state.current_local_artifact_id,
        restore_node_id: state.restore_node_id,
        unordered: state.unordered,
        captures,
    }))
}

/// **Guarded write.** Pin `sandbox_id` to `artifact`, a capture an operator
/// chose, and return whether it did.
///
/// It pins only a `suspended`, unpinned row whose `lifecycle_changed_at` is
/// still `expected_lifecycle_changed_at`, the value the operator read with
/// the captures (a sandbox that ran since may hold a later state), and only
/// to a capture [`restore_pin_state`] lists: one the selector could take, so
/// a pin can't restore a capture another one provably follows. Otherwise it
/// writes nothing.
///
/// Writes an **assertion**: the operator's word that `artifact` holds the
/// sandbox's latest state. Of the captures listed, only they can tell.
///
/// A node that still holds the sandbox suspended restores its own local copy
/// whatever the pin names, so the caller asks that node first, and pins only
/// its copy, or when it holds none.
pub async fn pin_restore_source(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    artifact: LocalArtifactId,
    expected_lifecycle_changed_at: time::OffsetDateTime,
) -> Result<bool, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static PIN: LazyLock<String> = LazyLock::new(|| {
        let pinnable = pinnable_capture("sn");
        sql!(format!(
            "UPDATE sandboxes s
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE s.id = $1
                AND s.lifecycle = 'suspended'
                AND s.lifecycle_changed_at = $3
                AND s.current_local_artifact_id IS NULL
                AND EXISTS (
                    SELECT 1
                      FROM snapshots sn
                     WHERE sn.local_artifact_id = $2
                       AND {pinnable}
                )"
        ))
    });
    let result = sqlx::query(sqlx::AssertSqlSafe(PIN.as_str()))
        .bind(sandbox_id)
        .bind(artifact.as_uuid())
        .bind(expected_lifecycle_changed_at)
        .execute(executor)
        .await?;
    Ok(result.rows_affected() == 1)
}

#[cfg(test)]
mod tests;
