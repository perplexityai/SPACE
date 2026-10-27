//! Capture-CPU evidence for a full snapshot: the identity recorded at capture,
//! and the uploading node a snapshot without one falls back to.

use super::*;
use pplx_space_common_core::types::HostCpu;
use pplx_space_sandbox_portability::newest_suspend_snapshot_activity_current;

/// What a full snapshot row says about the host that captured its vCPU state.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::controllers::substrate::kubernetes) struct SnapshotCaptureHost {
    /// Recorded at capture. Absent on snapshots that predate the field and on
    /// full backups whose metadata was rebuilt without one.
    pub(in crate::controllers::substrate::kubernetes) recorded: Option<HostCpu>,
    /// Kubernetes name of the node that uploaded the snapshot, if that node
    /// is still mapped. Its live label stands in for an unrecorded CPU.
    pub(in crate::controllers::substrate::kubernetes) source_node_name: Option<String>,
    /// The snapshot's timestamp, in Unix seconds. A Node object created after
    /// it can't be the machine that captured it, even under the same name.
    pub(in crate::controllers::substrate::kubernetes) snapshot_epoch_seconds: f64,
}

impl SnapshotCaptureHost {
    /// For this raw read, a recorded value that isn't a valid identity reads
    /// as unknown, with a warning, rather than failing the restore: the writer
    /// validates it, so only an out-of-band edit gets here, and unknown is the
    /// state every snapshot from before the field already restores in.
    fn new(
        recorded: Option<String>,
        source_node_name: Option<String>,
        snapshot_epoch_seconds: f64,
    ) -> Self {
        let recorded = recorded.and_then(|value| match HostCpu::parse(value.as_str()) {
            Ok(host_cpu) => Some(host_cpu),
            Err(error) => {
                tracing::warn!(
                    recorded = %value,
                    %error,
                    "ignoring an invalid recorded snapshot capture host CPU"
                );
                None
            }
        });
        Self {
            recorded,
            source_node_name,
            snapshot_epoch_seconds,
        }
    }
}

impl K8sStore {
    /// Evidence of which CPU captured the full snapshot stored at
    /// `storage_uri`: the identity recorded in its `execution_metadata`, and
    /// the Kubernetes name of the node that uploaded it, for a snapshot with
    /// no recorded CPU.
    ///
    /// Keyed by the exact URI rather than re-running a selector, because the
    /// restore plan may come from a live pod's annotation instead of from
    /// [`Self::selected_suspend_restore_storage_uri`]. `None` when no full
    /// snapshot of this sandbox has that location.
    pub(in crate::controllers::substrate::kubernetes) async fn snapshot_capture_host(
        &self,
        sandbox_id: SandboxId,
        storage_uri: &str,
    ) -> Result<Option<SnapshotCaptureHost>, SubstrateError> {
        let row: Option<(Option<String>, Option<String>, f64)> = sqlx::query_as(sql!(
            "SELECT sn.execution_metadata->>'host_cpu', nk.k8s_node_name,
                    EXTRACT(EPOCH FROM sn.timestamp)::float8
               FROM snapshots sn
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
               LEFT JOIN node_k8s nk ON nk.node_id = sn.source_node_id
              WHERE sn.sandbox_id = $1
                AND sn.type = 'full'
                AND ssl.storage_connection_string = $2
              ORDER BY sn.timestamp DESC
              LIMIT 1"
        ))
        .bind(sandbox_id)
        .bind(storage_uri)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to query Kubernetes restore snapshot capture host")?;
        Ok(
            row.map(|(host_cpu, source_node_name, snapshot_epoch_seconds)| {
                SnapshotCaptureHost::new(host_cpu, source_node_name, snapshot_epoch_seconds)
            }),
        )
    }

    /// The newest ready full snapshot of a markerless sandbox, when restoring
    /// from it provably loses no work: it covers the last recorded command and
    /// access, and no other kind of full snapshot ties or beats it
    /// ([`newest_suspend_snapshot_activity_current`], the same proof the
    /// failed-restore repair loop restores by). Returns its location and
    /// capture evidence, or `None` when the proof doesn't hold.
    ///
    /// The proof is only sound while no runtime can change the sandbox's
    /// state; the caller establishes that before calling. For a `restoring`
    /// row it also requires that no full snapshot postdates the restore's
    /// start, which keeps an artifact published by the restore itself out.
    ///
    /// The placement clock is deliberately not consulted. A cross-node restore
    /// that failed on its target moves `placement_changed_at` past the
    /// snapshot it was restoring, which is what turns the next wake into a
    /// local-only restore on that target; see
    /// `KubernetesSubstrate::cpu_incompatible_local_source_redirect`.
    pub(in crate::controllers::substrate::kubernetes) async fn activity_current_markerless_snapshot(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<(String, SnapshotCaptureHost)>, SubstrateError> {
        let activity_current = newest_suspend_snapshot_activity_current("s");
        let row: Option<(String, Option<String>, Option<String>, f64)> =
            sqlx::query_as(sqlx::AssertSqlSafe(sql!(format!(
                "SELECT ssl.storage_connection_string, sn.execution_metadata->>'host_cpu',
                        nk.k8s_node_name, EXTRACT(EPOCH FROM sn.timestamp)::float8
                   FROM sandboxes s
                   JOIN snapshots sn ON sn.sandbox_id = s.id
                   JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
                   LEFT JOIN node_k8s nk ON nk.node_id = sn.source_node_id
                  WHERE s.id = $1
                    AND s.current_local_artifact_id IS NULL
                    -- A restore under way can publish an artifact of its own
                    -- (a failed branch). The restore retry selector skips such
                    -- rows; this refuses to redirect at all while one exists.
                    AND NOT (
                        s.lifecycle = 'restoring'
                        AND EXISTS (
                            SELECT 1
                              FROM snapshots later
                             WHERE later.sandbox_id = s.id
                               AND later.type = 'full'
                               AND later.timestamp >= s.lifecycle_changed_at
                        )
                    )
                    AND ssl.orphaned_at IS NULL
                    AND sn.type = 'full'
                    AND sn.state = 'ready'
                    -- A newer full snapshot still uploading, or failed, may hold
                    -- later state, so it blocks falling back to an older ready
                    -- one. Stricter than the restore retry selector, which
                    -- ignores some failed retries; stricter only withholds the
                    -- redirect.
                    AND NOT EXISTS (
                        SELECT 1
                          FROM snapshots newer
                         WHERE newer.sandbox_id = s.id
                           AND newer.type = 'full'
                           AND newer.state <> 'ready'
                           AND newer.timestamp > sn.timestamp
                    )
                    AND {activity_current}
                  -- The fragment's EXISTS proves some snapshot covers the
                  -- activity clocks with no other kind of full snapshot tying
                  -- or beating it, so the newest ready full row selected here
                  -- is a suspend (or legacy) snapshot at least that new.
                  ORDER BY sn.timestamp DESC
                  LIMIT 1"
            ))))
            .bind(sandbox_id)
            .fetch_optional(&self.pool)
            .await
            .db_context("failed to query Kubernetes activity-current restore snapshot")?;
        let Some((storage_uri, host_cpu, source_node_name, snapshot_epoch_seconds)) = row else {
            return Ok(None);
        };
        StorageUri::parse(&storage_uri).map_err(|err| SubstrateError::Failed {
            message: format!("invalid activity-current restore storage URI: {err}"),
        })?;
        Ok(Some((
            storage_uri,
            SnapshotCaptureHost::new(host_cpu, source_node_name, snapshot_epoch_seconds),
        )))
    }
}
