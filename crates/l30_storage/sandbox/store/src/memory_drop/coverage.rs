//! Cluster-wide coverage query for retiring an environment's checkpoint memory.
//!
//! Retiring an environment's memory makes every subsequent restore of that
//! environment drop its checkpoint memory inline rather than booting the old
//! runtime, so the drop has no row to skip: before retirement commits, every
//! runnable sandbox attributed to the environment must either already boot on a
//! surviving environment or satisfy the preconditions restore-time memory drop
//! requires.

use std::sync::LazyLock;

use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_portability::{
    failed_repair_revert_eligibility, no_newer_full_snapshot,
    no_newer_full_snapshot_ignoring_failed_retry, restore_retry_source_snapshot_is_current,
    restore_retry_suspended_revert_snapshot_exists, snapshot_is_current_local_artifact,
};
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use sqlx::types::JsonValue;

use super::{FULL_SUSPEND_PIN, LEASE_EXPIRED, NODE_GONE, UNPLACED, durable_copy};

/// Whether restore-time memory drop covers checkpoint rows (`needs_droppable_pin`)
/// whose source node is still registered in `node_k8s`. Flipped once the restore
/// request names the pinned artifact so a source node can't boot local
/// old-runtime memory instead of the disk artifact; live, `stopped`, and
/// active-lease `restoring` rows still wait for drain or runtime upgrade.
pub const COVERS_NODE_HELD_ROWS: bool = false;

/// Whether restore-time memory drop covers volume-encrypted sandboxes.
/// Flipped once the encrypted disk-only projection lands.
pub const COVERS_ENCRYPTED_ROWS: bool = false;

/// Aggregated runnable non-already-dropped sandbox rows grouped by recorded
/// execution metadata and memory-drop eligibility dimensions.
#[derive(Clone, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct MemoryDropCoverageGroup {
    /// Execution metadata JSON from the restore-source snapshot, `stopped` disk
    /// artifact, observation, or in-flight snapshot fallback that binds this
    /// group, or `None` when none of those carry metadata.
    pub execution_metadata: Option<JsonValue>,
    /// Runtime name recorded on the binding snapshot or observation, if any.
    pub runtime: Option<String>,
    /// Runtime version recorded on the binding snapshot or observation, if any.
    pub runtime_version: Option<String>,
    /// State-format name recorded on the binding snapshot or observation, if any.
    pub state_format_name: Option<String>,
    /// State-format version recorded on the binding snapshot or observation, if any.
    pub state_format_version: Option<String>,
    /// Whether any binding source (`snapshots` or `sandbox_observed_states`)
    /// matched the row. Template releases are intentionally not consulted,
    /// because release twin tags share `(template_name, hash)` across old and
    /// new environments.
    pub recorded: bool,
    /// `s.managed_by = SubstrateKind::Kubernetes`.
    pub kubernetes: bool,
    /// `COALESCE(s.volume_encryption, FALSE)`.
    pub encrypted: bool,
    /// Unplaced `suspended`, lease-expired `restoring`, or repairable `failed`
    /// row whose last node is gone. A `restoring` row with a live restore lease
    /// is actively mid-restore and evaluates `false` so retirement waits for it.
    pub unplaced_on_gone_node: bool,
    /// Whether the row's next activation restores from a checkpoint (`suspended`,
    /// `restoring`, or repairable `failed`) and therefore can be converted by
    /// restore-time memory drop when it has a droppable pin.
    pub needs_droppable_pin: bool,
    /// Whether the row currently carries a revertable `full` `suspend` pin
    /// backed by a ready, unorphaned per-attempt S3 snapshot URI that is the
    /// newest full snapshot.
    pub droppable_pin: bool,
    /// Number of sandboxes in this group.
    #[sqlx(try_from = "i64")]
    pub sandboxes: u64,
}

static COVERAGE_QUERY: LazyLock<String> = LazyLock::new(|| {
    let marked = snapshot_is_current_local_artifact("s", "sn");
    let retry_source = restore_retry_source_snapshot_is_current("s", "sn");
    let safe_revert = restore_retry_suspended_revert_snapshot_exists("s");
    let newest_full = no_newer_full_snapshot("s", "sn");
    let newest_ignoring_failed = no_newer_full_snapshot_ignoring_failed_retry("s", "sn");
    let durable = durable_copy();
    let repair = failed_repair_revert_eligibility("s");
    let (repairable, stale_marker) = (repair.eligible, repair.clear_marker);
    sql!(format!(
        "SELECT binding.metadata AS execution_metadata,
                binding.runtime AS runtime,
                binding.runtime_version AS runtime_version,
                binding.state_format_name AS state_format_name,
                binding.state_format_version AS state_format_version,
                binding.recorded AS recorded,
                (s.managed_by = $2) AS kubernetes,
                COALESCE(s.volume_encryption, FALSE) AS encrypted,
                COALESCE((
                    (
                        s.lifecycle = 'suspended'
                        OR (s.lifecycle = 'restoring' AND {LEASE_EXPIRED})
                        OR (s.lifecycle = 'failed' AND sos.sandbox_id IS NULL AND NOT live.execution)
                    )
                    AND {UNPLACED}
                    AND {NODE_GONE}
                ), FALSE) AS unplaced_on_gone_node,
                (
                    s.lifecycle IN ('suspended', 'restoring')
                    OR (s.lifecycle = 'failed' AND sos.sandbox_id IS NULL AND NOT live.execution)
                ) AS needs_droppable_pin,
                COALESCE((
                    {FULL_SUSPEND_PIN}
                    AND (s.lifecycle <> 'failed' OR NOT ({stale_marker}))
                    AND (s.lifecycle <> 'restoring' OR ({safe_revert}))
                    AND COALESCE((
                        SELECT ssl.storage_connection_string ~ (
                                   '^s3://[^/?]+/(?:[^?]*/)?'
                                   || s.id::text || '/' || sn.id::text
                                   || '/*\\?(?:&*(?:region=[^&]*|endpoint=[^&]*|path_style=(?:true|false)))*&*$'
                               )
                           AND ssl.storage_connection_string ~ '^[^?]*\\?(?:[^&]*&)*region='
                          FROM snapshots sn
                          JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
                         WHERE sn.sandbox_id = s.id
                           AND {durable}
                           AND (
                               s.lifecycle <> 'restoring'
                               OR s.lifecycle_changed_at IS NULL
                               OR sn.timestamp < s.lifecycle_changed_at
                           )
                           AND {newest_ignoring_failed}
                         ORDER BY sn.timestamp DESC, sn.id DESC, ssl.created_at DESC, ssl.id DESC
                         LIMIT 1
                    ), FALSE)
                ), FALSE) AS droppable_pin,
                COUNT(*) AS sandboxes
           FROM sandboxes s
           LEFT JOIN sandbox_observed_states sos
                  ON sos.sandbox_id = s.id
                 AND (s.lifecycle NOT IN ('suspended', 'restoring', 'failed')
                      OR (s.lifecycle = 'failed'
                          AND (sos.status IS DISTINCT FROM 'failed'
                               OR sos.pid IS NOT NULL
                               OR s.active_node_id IS NOT NULL)))
           -- `Start` on a `stopped` Kubernetes sandbox resolves the current
           -- ready `disk` snapshot's environment before falling back to the
           -- last published execution (`KubernetesSubstrate::stopped_environment`).
           LEFT JOIN LATERAL (
               SELECT sn.id,
                      sn.execution_metadata,
                      sn.runtime,
                      sn.runtime_version,
                      sn.state_format_name,
                      sn.state_format_version
                 FROM snapshots sn
                WHERE sn.sandbox_id = s.id
                  AND sn.local_artifact_id = s.current_local_artifact_id
                  AND sn.type = 'disk'
                  AND sn.state = 'ready'
                ORDER BY sn.timestamp DESC, sn.id DESC
                LIMIT 1
           ) stopped_disk ON s.lifecycle = 'stopped'
           CROSS JOIN LATERAL (
               SELECT EXISTS (
                   SELECT 1
                     FROM sandbox_k8s_executions e
                    WHERE e.sandbox_id = s.id
                      AND (e.active OR e.pod_ip IS NOT NULL)
               ) AS execution
           ) live
           -- Primary restore-source selector matching `sandbox_population` and
           -- substrate restore selection, so a newer `failed`/`creating` retry
           -- snapshot never shadows an older ready `E0` checkpoint.
           LEFT JOIN LATERAL (
               SELECT sn.id,
                      sn.execution_metadata,
                      sn.runtime,
                      sn.runtime_version,
                      sn.state_format_name,
                      sn.state_format_version
                 FROM snapshots sn
                WHERE sn.sandbox_id = s.id
                  AND sn.type = 'full'
                  AND (
                      (s.lifecycle IN ('suspended', 'restoring')
                       AND (
                           (s.managed_by = $1 AND (
                               s.lifecycle = 'suspended' OR {retry_source}
                           ))
                           OR (s.managed_by = $2
                               AND sn.state = 'ready'
                               AND EXISTS (
                                   SELECT 1
                                     FROM snapshot_storage_locations ssl
                                    WHERE ssl.snapshot_id = sn.id AND ssl.orphaned_at IS NULL
                               )
                               AND (
                                   (s.evacuation_restore_target IS NOT NULL AND (
                                       {marked}
                                       OR (s.current_local_artifact_id IS NULL AND {newest_full})
                                   ))
                                   OR (s.evacuation_restore_target IS NULL
                                       AND s.lifecycle = 'suspended'
                                       AND (
                                           {marked}
                                           OR (
                                               s.current_local_artifact_id IS NULL
                                               AND (s.placement_changed_at IS NULL
                                                    OR sn.timestamp >= s.placement_changed_at)
                                           )
                                       ))
                                   OR (s.evacuation_restore_target IS NULL
                                       AND s.lifecycle = 'restoring'
                                       AND {retry_source})
                               ))
                       ))
                      OR (s.lifecycle = 'failed'
                          AND s.managed_by = $2
                          AND sos.sandbox_id IS NULL
                          AND NOT live.execution
                          AND {repairable}
                          AND sn.state = 'ready'
                          AND EXISTS (
                              SELECT 1
                                FROM snapshot_storage_locations ssl
                               WHERE ssl.snapshot_id = sn.id AND ssl.orphaned_at IS NULL
                          )
                          AND ({marked} OR s.current_local_artifact_id IS NULL OR {stale_marker}))
                  )
                ORDER BY sn.timestamp DESC, sn.id DESC
                LIMIT 1
           ) snap ON TRUE
           -- Fallback only when no ready restore-source snapshot and no
           -- observation exist, so an in-flight `creating` suspend on `E0` is
           -- still attributed to `E0` (`unpinned_or_not_ready`) instead of
           -- falling into `unattributed`.
           LEFT JOIN LATERAL (
               SELECT sn.id,
                      sn.execution_metadata,
                      sn.runtime,
                      sn.runtime_version,
                      sn.state_format_name,
                      sn.state_format_version
                 FROM snapshots sn
                WHERE sn.sandbox_id = s.id
                  AND sn.type = 'full'
                  AND (
                      CASE
                          WHEN s.lifecycle = 'restoring'
                              THEN (s.lifecycle_changed_at IS NULL OR sn.timestamp < s.lifecycle_changed_at)
                          WHEN s.current_local_artifact_id IS NOT NULL THEN {marked}
                          ELSE TRUE
                      END
                  )
                ORDER BY sn.timestamp DESC, sn.id DESC
                LIMIT 1
           ) pending_snap ON (
               snap.id IS NULL
               AND sos.sandbox_id IS NULL
               AND (
                   s.lifecycle IN ('suspended', 'restoring')
                   OR (s.lifecycle = 'failed' AND NOT live.execution)
               )
           )
           CROSS JOIN LATERAL (
               SELECT CASE
                          WHEN snap.id IS NOT NULL THEN snap.execution_metadata
                          WHEN stopped_disk.id IS NOT NULL
                               AND (stopped_disk.execution_metadata ->> 'environment' IS NOT NULL
                                    OR sos.sandbox_id IS NULL)
                              THEN stopped_disk.execution_metadata
                          WHEN sos.sandbox_id IS NOT NULL THEN sos.execution_metadata
                          ELSE pending_snap.execution_metadata
                      END AS metadata,
                      CASE
                          WHEN snap.id IS NOT NULL THEN snap.runtime
                          WHEN stopped_disk.id IS NOT NULL
                               AND (stopped_disk.execution_metadata ->> 'environment' IS NOT NULL
                                    OR sos.sandbox_id IS NULL)
                              THEN stopped_disk.runtime
                          WHEN sos.sandbox_id IS NOT NULL THEN sos.runtime
                          ELSE pending_snap.runtime
                      END AS runtime,
                      CASE
                          WHEN snap.id IS NOT NULL THEN snap.runtime_version
                          WHEN stopped_disk.id IS NOT NULL
                               AND (stopped_disk.execution_metadata ->> 'environment' IS NOT NULL
                                    OR sos.sandbox_id IS NULL)
                              THEN stopped_disk.runtime_version
                          WHEN sos.sandbox_id IS NOT NULL THEN sos.runtime_version
                          ELSE pending_snap.runtime_version
                      END AS runtime_version,
                      CASE
                          WHEN snap.id IS NOT NULL THEN snap.state_format_name
                          WHEN stopped_disk.id IS NOT NULL
                               AND (stopped_disk.execution_metadata ->> 'environment' IS NOT NULL
                                    OR sos.sandbox_id IS NULL)
                              THEN stopped_disk.state_format_name
                          WHEN sos.sandbox_id IS NOT NULL THEN sos.state_format_name
                          ELSE pending_snap.state_format_name
                      END AS state_format_name,
                      CASE
                          WHEN snap.id IS NOT NULL THEN snap.state_format_version
                          WHEN stopped_disk.id IS NOT NULL
                               AND (stopped_disk.execution_metadata ->> 'environment' IS NOT NULL
                                    OR sos.sandbox_id IS NULL)
                              THEN stopped_disk.state_format_version
                          WHEN sos.sandbox_id IS NOT NULL THEN sos.state_format_version
                          ELSE pending_snap.state_format_version
                      END AS state_format_version,
                      (
                          snap.id IS NOT NULL
                          OR stopped_disk.id IS NOT NULL
                          OR sos.sandbox_id IS NOT NULL
                          OR pending_snap.id IS NOT NULL
                      ) AS recorded
           ) binding
          WHERE s.lifecycle NOT IN ('deleted', 'deleting')
            AND NOT (
                s.lifecycle IN ('suspended', 'restoring')
                AND s.current_local_artifact_kind IS NOT DISTINCT FROM 'disk'
            )
            AND (
                s.lifecycle <> 'failed'
                OR sos.sandbox_id IS NOT NULL
                OR live.execution
                OR (s.managed_by = $2 AND {repairable})
            )
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11"
    ))
});

/// Summarize runnable non-already-dropped sandboxes for an environment
/// memory-retirement coverage check.
pub async fn coverage_groups(
    executor: impl PgExecutor<'_>,
) -> Result<Vec<MemoryDropCoverageGroup>, sqlx::Error> {
    sqlx::query_as(sqlx::AssertSqlSafe(COVERAGE_QUERY.as_str()))
        .bind(SubstrateKind::Space)
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(executor)
        .await
}
