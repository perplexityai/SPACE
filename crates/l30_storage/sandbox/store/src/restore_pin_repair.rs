//! Reads and the guarded write of the restore-pin repair: pinning an
//! unpinned suspended sandbox, on either substrate, to the artifact its node
//! holds.
//!
//! A suspend the node finalized without pinning the row leaves it to
//! restore by capture order alone, which a re-upload of
//! an older capture can mislead. The node that last held the sandbox still
//! keeps the artifact it suspended in its own row. The repair loop asks that
//! node, and [`repair_restore_pin`] pins the control-plane row to it when the
//! evidence here agrees.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{LocalArtifactId, SandboxId};
use pplx_space_sandbox_portability::node_held_capture_is_latest;

/// An unpinned suspended sandbox, the node it's placed on, and when it last
/// changed lifecycle, which the repair's write requires unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct RestorePinRepairCandidate {
    pub sandbox_id: SandboxId,
    pub node_id: NodeId,
    pub lifecycle_changed_at: time::OffsetDateTime,
}

/// Up to `limit` unpinned suspended `managed_by` sandboxes after `after`, in
/// id order, that are placed on a node (`target_node_id`, else
/// `active_node_id`) and have a ready full suspend backup with an artifact
/// id and a live location: the rows a repair could pin.
pub async fn restore_pin_repair_candidates(
    executor: impl sqlx::PgExecutor<'_>,
    managed_by: &str,
    after: Option<SandboxId>,
    limit: i64,
) -> Result<Vec<RestorePinRepairCandidate>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT s.id AS sandbox_id,
                COALESCE(s.target_node_id, s.active_node_id) AS node_id,
                s.lifecycle_changed_at
           FROM sandboxes s
          WHERE s.managed_by = $3
            AND s.lifecycle = 'suspended'
            AND s.lifecycle_changed_at IS NOT NULL
            AND s.current_local_artifact_id IS NULL
            AND COALESCE(s.target_node_id, s.active_node_id) IS NOT NULL
            AND ($1::UUID IS NULL OR s.id > $1)
            AND EXISTS (
                SELECT 1
                  FROM snapshots sn
                  JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
                 WHERE sn.sandbox_id = s.id
                   AND sn.type = 'full'
                   AND sn.state = 'ready'
                   AND sn.local_artifact_id IS NOT NULL
                   AND sn.artifact_kind = 'full'
                   AND sn.artifact_origin = 'suspend'
                   AND ssl.orphaned_at IS NULL
            )
          ORDER BY s.id
          LIMIT $2"
    ))
    .bind(after)
    .bind(limit)
    .bind(managed_by)
    .fetch_all(executor)
    .await
}

/// **Guarded write.** Pin `sandbox_id` to `artifact`, the full suspend
/// artifact `node_id` reports holding for it, and return whether it did.
///
/// It pins only while the row is still `suspended` in the same suspend
/// (`lifecycle_changed_at` unchanged since the candidate was read, so the
/// node's answer describes it), unpinned and placed on `node_id`, and when a
/// backup of `artifact` from `node_id` is still the sandbox's latest capture
/// ([`node_held_capture_is_latest`]). Otherwise it writes nothing, and the
/// sandbox is left for an operator.
///
/// One statement, and its row lock serializes it with retention's prune
/// claim, which locks the same row and then sees the pin. The `snapshots`
/// reads see the statement's snapshot, taken before any wait on that lock: a
/// prune that commits while this waits isn't seen. That's harmless: a
/// capture this pins has no capture of another artifact registered at or
/// after it and nothing outranks it, so it's the one capture retention's
/// unpinned arm shields as a possible pick, and no prune claim takes it.
pub async fn repair_restore_pin(
    executor: impl sqlx::PgExecutor<'_>,
    candidate: RestorePinRepairCandidate,
    artifact: LocalArtifactId,
) -> Result<bool, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static REPAIR: LazyLock<String> = LazyLock::new(|| {
        let held = node_held_capture_is_latest("s", "sn", "$3", "$2");
        sql!(format!(
            "UPDATE sandboxes s
                SET current_local_artifact_id = $3,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE s.id = $1
                AND s.lifecycle = 'suspended'
                AND s.lifecycle_changed_at = $4
                AND s.current_local_artifact_id IS NULL
                AND COALESCE(s.target_node_id, s.active_node_id) = $2
                AND EXISTS (
                    SELECT 1
                      FROM snapshots sn
                     WHERE sn.sandbox_id = s.id
                       AND {held}
                )"
        ))
    });
    let result = sqlx::query(sqlx::AssertSqlSafe(REPAIR.as_str()))
        .bind(candidate.sandbox_id)
        .bind(candidate.node_id)
        .bind(artifact.as_uuid())
        .bind(candidate.lifecycle_changed_at)
        .execute(executor)
        .await?;
    Ok(result.rows_affected() == 1)
}

#[cfg(test)]
mod tests;
