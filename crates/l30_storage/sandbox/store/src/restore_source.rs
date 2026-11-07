//! Reads of `sandboxes` that resolve what the Kubernetes substrate restores an
//! unplaced or evacuated sandbox from, by the node-drain
//! `evacuation_restore_target` marker: the unplaced restore plan of a `stopped`
//! row, and the disk artifact a stopped evacuation is re-placed from.
//!
//! The Kubernetes store keeps the rest of restore source selection: the
//! suspend and stopped storage-URI selectors, the node-name fallback, the
//! missing-mapping warning and the validation of a selected storage URI. Both
//! reads here are plain reads: they take no lock, and run on the executor the
//! caller passes.

use pplx_space_accounts::NodeId;
use pplx_space_common_core::types::{LocalArtifactId, SandboxId};

use pplx_space_common_core::sql;
use pplx_space_sandbox_portability::snapshot_is_current_local_artifact;
use sqlx::PgExecutor;

/// What [`restore_source_plan`] reads of one row: the two node-name
/// candidates, kept apart rather than `COALESCE`d, the placement's node id,
/// and whether a stopped evacuation has left the row unplaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreSourcePlanRow {
    /// The Kubernetes node mapped to `COALESCE(target_node_id,
    /// active_node_id)`.
    pub mapped_name: Option<String>,
    /// The node the newest execution that named one ran on.
    pub execution_name: Option<String>,
    /// `COALESCE(target_node_id, active_node_id)`.
    pub placement_node_id: Option<NodeId>,
    /// The marker is `'stopped'` and `active_node_id` is NULL.
    pub stopped_evacuation_unplaced: bool,
}

/// Read the restore source of a `stopped` sandbox's row, for a restart that
/// may have to pin its pod to a node. `None` when no row exists.
///
/// Only `'stopped'` counts as the marker. The other two values belong to the
/// suspend → restore pipeline, which parks its rows in `suspended` and
/// resolves them through the storage-URI path; reading them here would refuse
/// restores that path already handles.
///
/// When it holds, the drain has released the node holding the copy, so the
/// node name beside it is not a restore source at all.
///
/// **`active_node_id IS NULL` is the load-bearing half, not a belt-and-braces
/// extra.** The marker alone says an evacuation was *claimed*, which is not the
/// same as finished, and a claim can outlive the drain that made it: an
/// operator's `:undrain` between the claim and the teardown leaves the row
/// `stopped`, marked, and still holding its rootfs — and **nothing clears that
/// marker**. Every writer that clears it requires a lifecycle change or a
/// lifecycle this row does not have (`suspending`, `suspended`, `restoring`),
/// and the drain reconciler never visits an undrained node again. Refusing on
/// the marker alone would therefore make `start` a permanent 503 for a sandbox
/// whose disk is sitting intact on a healthy node, with delete as the owner's
/// only exit. `active_node_id` alone, and not the placement `COALESCE`: a
/// re-placement with a target chosen and nothing active yet still has no local
/// copy anywhere.
pub async fn restore_source_plan(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<RestoreSourcePlanRow>, sqlx::Error> {
    type Row = (Option<String>, Option<String>, Option<NodeId>, bool);
    let row: Option<Row> = sqlx::query_as(sql!(
        "SELECT (
                 SELECT nk.k8s_node_name
                   FROM node_k8s nk
                  WHERE nk.node_id = COALESCE(s.target_node_id, s.active_node_id)
             ) AS mapped_name,
             (
                 SELECT ske.k8s_node_name
                   FROM sandbox_k8s_executions ske
                  WHERE ske.sandbox_id = s.id
                    AND ske.k8s_node_name IS NOT NULL
                  ORDER BY ske.generation DESC
                  LIMIT 1
             ) AS execution_name,
             COALESCE(s.target_node_id, s.active_node_id) AS placement_node_id,
             -- `active_node_id` alone, deliberately, and *not* the
             -- `COALESCE` above: a re-placement with a target chosen and
             -- nothing active yet still has no local copy anywhere. Pinned by
             -- the mid-re-placement phase of
             -- `a_stopped_evacuation_counts_as_unplaced_only_once_its_local_copy_is_gone`.
             COALESCE(
                 s.evacuation_restore_target = 'stopped' AND s.active_node_id IS NULL,
                 FALSE
             ) AS stopped_evacuation_unplaced
             FROM sandboxes s
             WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await?;
    Ok(row.map(
        |(mapped_name, execution_name, placement_node_id, stopped_evacuation_unplaced)| {
            RestoreSourcePlanRow {
                mapped_name,
                execution_name,
                placement_node_id,
                stopped_evacuation_unplaced,
            }
        },
    ))
}

/// The disk artifact [`stopped_evacuation_restore_source`] selects: the
/// storage location's connection string, unvalidated, and the row's pinned
/// artifact id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoppedEvacuationRestoreSourceRow {
    pub storage_connection_string: String,
    pub current_local_artifact_id: LocalArtifactId,
}

/// Select the disk artifact the evacuation restore sweep re-places an evacuated
/// `stopped` sandbox from, once the evacuation claim has moved the row to
/// `restoring`. `None` when nothing matches.
///
/// `evacuation_restore_target = 'stopped'` is what scopes this to the stopped
/// evacuation. A suspended evacuation's `restoring` row carries `'started'` or
/// `'paused'`, and a user restore clears the marker when it claims, so neither
/// can select a disk artifact here.
///
/// The pin ([`snapshot_is_current_local_artifact`]) is required: it is the only
/// thing that ties the URI to the bytes the drain published. The drain wrote it
/// (`record_stopped_evacuation_artifact`, which refuses to replace a pin with a
/// different id) before releasing the node, and the claim leaves it alone. The
/// id returned is the row's pin, not the snapshot's: the shared fragment makes
/// them equal, and the pin is the value sandbox-manager's own row carries.
///
/// Newest first, with an `sn.id` tiebreak: a retried backup of one artifact
/// mints a fresh `snapshots` row per attempt while keeping the same
/// `local_artifact_id`, so the pin can match several rows, and the latest
/// registered location is the one to load.
pub async fn stopped_evacuation_restore_source(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<StoppedEvacuationRestoreSourceRow>, sqlx::Error> {
    let pinned = snapshot_is_current_local_artifact("s", "sn");
    let query = sql!(format!(
        "SELECT ssl.storage_connection_string, s.current_local_artifact_id
               FROM sandboxes s
               JOIN snapshots sn ON sn.sandbox_id = s.id
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE s.id = $1
                AND s.lifecycle = 'restoring'
                AND s.evacuation_restore_target = 'stopped'
                AND ssl.orphaned_at IS NULL
                AND sn.type = 'disk'
                AND sn.state = 'ready'
                AND {pinned}
              ORDER BY sn.timestamp DESC, sn.id DESC
              LIMIT 1"
    ));
    let row: Option<(String, LocalArtifactId)> = sqlx::query_as(sqlx::AssertSqlSafe(query))
        .bind(sandbox_id)
        .fetch_optional(executor)
        .await?;
    Ok(
        row.map(|(storage_connection_string, current_local_artifact_id)| {
            StoppedEvacuationRestoreSourceRow {
                storage_connection_string,
                current_local_artifact_id,
            }
        }),
    )
}
