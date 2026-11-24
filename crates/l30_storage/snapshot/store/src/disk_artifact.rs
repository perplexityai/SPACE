//! Disk artifacts a fork reads from its source or copies for its target.

use pplx_space_accounts::OrgId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::{LocalArtifactId, SnapshotId};
use pplx_space_sql_locator::sql;
use uuid::Uuid;

/// A ready disk snapshot and the live storage location that holds its bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadyDiskArtifact {
    pub snapshot_id: SnapshotId,
    pub storage_location_id: Uuid,
    pub storage_connection_string: String,
}

impl From<(SnapshotId, Uuid, String)> for ReadyDiskArtifact {
    fn from(
        (snapshot_id, storage_location_id, storage_connection_string): (SnapshotId, Uuid, String),
    ) -> Self {
        Self {
            snapshot_id,
            storage_location_id,
            storage_connection_string,
        }
    }
}

/// The ready disk backup of `sandbox_id`'s local artifact
/// `local_artifact_id` whose live storage location was created last.
pub async fn ready_for_local_id(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
) -> Result<Option<ReadyDiskArtifact>, sqlx::Error> {
    let row = sqlx::query_as::<_, (SnapshotId, Uuid, String)>(sql!(
        "SELECT sn.id, ssl.id, ssl.storage_connection_string
             FROM snapshots sn
             JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
             WHERE sn.sandbox_id = $1
               AND sn.local_artifact_id = $2
               AND sn.type = 'disk'
               AND sn.artifact_kind = 'disk'
               AND sn.state = 'ready'
               AND ssl.orphaned_at IS NULL
             ORDER BY ssl.created_at DESC
             LIMIT 1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(local_artifact_id.as_uuid())
    .fetch_optional(executor)
    .await?;
    Ok(row.map(ReadyDiskArtifact::from))
}

/// Snapshot `snapshot_id` at storage location `storage_location_id`, when
/// it is still a ready disk snapshot or fork copy and the location is live.
pub async fn ready_at_location(
    executor: impl sqlx::PgExecutor<'_>,
    snapshot_id: SnapshotId,
    storage_location_id: Uuid,
) -> Result<Option<ReadyDiskArtifact>, sqlx::Error> {
    let row = sqlx::query_as::<_, (SnapshotId, Uuid, String)>(sql!(
        "SELECT sn.id, ssl.id, ssl.storage_connection_string
             FROM snapshots sn
             JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
             WHERE sn.id = $1
               AND ssl.id = $2
               AND sn.type = 'disk'
               AND sn.artifact_kind = 'disk'
               AND sn.artifact_origin IN ('snapshot', 'fork')
               AND sn.state = 'ready'
               AND ssl.orphaned_at IS NULL"
    ))
    .bind(snapshot_id)
    .bind(storage_location_id)
    .fetch_optional(executor)
    .await?;
    Ok(row.map(ReadyDiskArtifact::from))
}

/// Register `snapshot_id` as a `creating` fork copy of a disk artifact owned
/// by `owner_sandbox_id`. A row already registered under that id is left as
/// it is, so a retried copy reuses it.
pub async fn insert_creating_fork_copy(
    executor: impl sqlx::PgExecutor<'_>,
    snapshot_id: SnapshotId,
    owner_sandbox_id: SandboxId,
    org_id: OrgId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO snapshots (
                 id, sandbox_id, type, state, timestamp, artifact_kind, artifact_origin, org_id
             )
             VALUES ($1, $2, 'disk', 'creating', NOW(), 'disk', 'fork', $3)
             ON CONFLICT (id) DO NOTHING"
    ))
    .bind(snapshot_id)
    .bind(owner_sandbox_id.as_uuid())
    .bind(org_id)
    .execute(executor)
    .await?;
    Ok(())
}
