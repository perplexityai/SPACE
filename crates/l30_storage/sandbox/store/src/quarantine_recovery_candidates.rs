//! Which quarantined sandboxes the cluster controller may recover from an
//! abandoned suspend-snapshot publication: a single execution whose pod and
//! worker row can be proven gone, and a ready suspend snapshot that covers
//! the sandbox's recorded activity.
//!
//! The read joins tables other stores own (`sandbox_quarantine_state`,
//! `node_k8s`, `snapshots`, `snapshot_storage_locations`,
//! `public_operations`) and compares their ages on the database clock. The
//! caller keeps the absence probes, its recovery transaction and its locks,
//! and re-runs this read for one sandbox under them.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::{LocalArtifactId, SnapshotId};

use pplx_space_sql_locator::sql;
use uuid::Uuid;

/// One recovery candidate: the sandbox, its open episode, its node, its only
/// execution, and the snapshot a recovery would restore.
#[derive(Clone, Debug, PartialEq, Eq, sqlx::FromRow)]
pub struct Candidate {
    pub sandbox_id: SandboxId,
    pub quarantine_id: Uuid,
    pub node_id: NodeId,
    pub node_name: String,
    pub execution_id: Uuid,
    pub generation: i64,
    pub pod_name: String,
    pub pod_namespace: String,
    pub pod_uid: Option<String>,
    pub snapshot_id: SnapshotId,
    pub artifact_id: LocalArtifactId,
}

/// One execution means the absence probes cover every possible resident owner.
/// Multiple executions require a broader proof and remain quarantined. A ready
/// snapshot must predate quarantine, cover all recorded activity, and have no
/// newer full snapshot or unfinished upload. Lifecycle/placement timestamps are
/// not artifact freshness: the failed publication itself advances those clocks.
///
/// Pages after `after`, or reads only `only`, in id order, up to `limit`
/// rows of the `managed_by` substrate.
pub async fn list<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    after: Option<SandboxId>,
    only: Option<SandboxId>,
    limit: i64,
    managed_by: SubstrateKind,
) -> Result<Vec<Candidate>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT s.id AS sandbox_id, q.quarantine_id, s.active_node_id AS node_id,
                n.k8s_node_name AS node_name, e.id AS execution_id, e.generation,
                e.pod_name, e.pod_namespace, e.pod_uid,
                sn.id AS snapshot_id, sn.local_artifact_id AS artifact_id
           FROM sandboxes s
           JOIN sandbox_quarantine_state q ON q.sandbox_id = s.id
           JOIN node_k8s n ON n.node_id = s.active_node_id
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
           JOIN snapshots sn ON sn.sandbox_id = s.id
          WHERE s.managed_by = $4 AND s.lifecycle = 'quarantined'
            AND s.current_local_artifact_id IS NULL
            AND s.current_local_artifact_kind IS NULL
            AND s.current_local_artifact_origin IS NULL
            AND (s.target_node_id IS NULL OR s.target_node_id = s.active_node_id)
            AND (s.restore_lease_expires_at IS NULL OR s.restore_lease_expires_at < NOW())
            AND q.reason_code = 'suspend_snapshot_publication_failed'
            AND q.entered_at < NOW() - INTERVAL '10 minutes'
            AND q.next_action_owner IS NULL AND q.claim_token IS NULL
            AND e.active AND e.state NOT IN ('creating', 'recovering')
            AND e.created_at < NOW() - INTERVAL '10 minutes'
            AND e.activation_attempt_id IS NULL
            AND (e.k8s_node_name IS NULL OR e.k8s_node_name = n.k8s_node_name)
            AND NOT EXISTS (
                SELECT 1 FROM sandbox_k8s_executions other
                 WHERE other.sandbox_id = s.id AND other.id <> e.id)
            AND NOT EXISTS (
                SELECT 1 FROM public_operations op
                 WHERE op.target_id IN (s.id::TEXT, 'sbx_' || s.id::TEXT)
                   AND op.state IN ('queued', 'running'))
            AND sn.state = 'ready' AND sn.type = 'full'
            AND sn.artifact_kind = 'full' AND sn.artifact_origin = 'suspend'
            AND sn.local_artifact_id IS NOT NULL AND sn.size_bytes > 0
            AND sn.timestamp <= q.entered_at
            AND (s.last_command_at IS NULL OR sn.timestamp >= s.last_command_at)
            AND (s.last_access_at IS NULL OR sn.timestamp >= s.last_access_at)
            AND EXISTS (
                SELECT 1 FROM snapshot_storage_locations l
                 WHERE l.snapshot_id = sn.id AND l.orphaned_at IS NULL)
            AND NOT EXISTS (
                SELECT 1 FROM snapshots newer WHERE newer.sandbox_id = s.id
                  AND (newer.state = 'creating'
                       OR (newer.type = 'full' AND newer.timestamp > sn.timestamp)))
            AND ($1::UUID IS NULL OR s.id > $1)
            AND ($2::UUID IS NULL OR s.id = $2)
          ORDER BY s.id LIMIT $3"
    ))
    .bind(after)
    .bind(only)
    .bind(limit)
    .bind(managed_by)
    .fetch_all(executor)
    .await
}
