//! Disk artifacts a fork reads from its source or copies for its target, and
//! the guarded transitions of a fork copy's catalog rows.

use pplx_space_accounts::OrgId;
use pplx_space_postgres_pool::{DbError, Executor};
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
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
) -> Result<Option<ReadyDiskArtifact>, DbError> {
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
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(row.map(ReadyDiskArtifact::from))
}

/// Snapshot `snapshot_id` at storage location `storage_location_id`, when
/// it is still a ready disk snapshot or fork copy and the location is live.
pub async fn ready_at_location(
    executor: impl Executor<'_>,
    snapshot_id: SnapshotId,
    storage_location_id: Uuid,
) -> Result<Option<ReadyDiskArtifact>, DbError> {
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
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(row.map(ReadyDiskArtifact::from))
}

/// Register `snapshot_id` as a `creating` fork copy of a disk artifact owned
/// by `owner_sandbox_id`. A row already registered under that id is left as
/// it is, so a retried copy reuses it.
pub async fn insert_creating_fork_copy(
    executor: impl Executor<'_>,
    snapshot_id: SnapshotId,
    owner_sandbox_id: SandboxId,
    org_id: OrgId,
) -> Result<(), DbError> {
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
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Point storage location `storage_location_id` at fork copy `snapshot_id`
/// with route `storage_connection_string`. A new id is inserted; an existing
/// one is taken over only while it is an orphan (no snapshot and
/// `orphaned_at` set), so a retried copy re-adopts the location its earlier
/// attempt retired and a live location is left as it is. `true` when the id
/// was new or an orphan was re-adopted; `false` when a location with that id
/// exists and isn't an orphan, which is left as it is.
pub async fn attach_fork_copy_location(
    executor: impl Executor<'_>,
    storage_location_id: Uuid,
    snapshot_id: SnapshotId,
    storage_connection_string: &str,
    org_id: OrgId,
) -> Result<bool, DbError> {
    let attached = sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (
                 id, snapshot_id, storage_connection_string, org_id
             ) VALUES ($1, $2, $3, $4)
             ON CONFLICT (id) DO UPDATE
             SET snapshot_id = EXCLUDED.snapshot_id,
                 storage_connection_string = EXCLUDED.storage_connection_string,
                 org_id = EXCLUDED.org_id,
                 orphaned_at = NULL
             WHERE snapshot_storage_locations.snapshot_id IS NULL
               AND snapshot_storage_locations.orphaned_at IS NOT NULL"
    ))
    .bind(storage_location_id)
    .bind(snapshot_id)
    .bind(storage_connection_string)
    .bind(org_id)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(attached == 1)
}

/// Mark fork copy `snapshot_id`, owned by `owner_sandbox_id`, `ready` with
/// `size_bytes`. A copy already `ready` is marked again, so a retried
/// completion succeeds. `false` when no `creating` or `ready` fork copy with
/// that id and owner exists.
pub async fn mark_fork_copy_ready(
    executor: impl Executor<'_>,
    snapshot_id: SnapshotId,
    owner_sandbox_id: SandboxId,
    size_bytes: i64,
) -> Result<bool, DbError> {
    let updated = sqlx::query(sql!(
        "UPDATE snapshots
             SET state = 'ready', size_bytes = $2
             WHERE id = $1
               AND sandbox_id = $3
               AND type = 'disk'
               AND artifact_kind = 'disk'
               AND artifact_origin = 'fork'
               AND state IN ('creating', 'ready')"
    ))
    .bind(snapshot_id)
    .bind(size_bytes)
    .bind(owner_sandbox_id.as_uuid())
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(updated == 1)
}

/// Hand ready fork copy `snapshot_id` from `source_sandbox_id` to
/// `target_sandbox_id`. A copy the target already owns is rebound to it
/// again, so a retried handoff succeeds. `false` when no ready fork copy with
/// that id belongs to either sandbox.
pub async fn rebind_fork_copy_owner(
    executor: impl Executor<'_>,
    snapshot_id: SnapshotId,
    source_sandbox_id: SandboxId,
    target_sandbox_id: SandboxId,
) -> Result<bool, DbError> {
    let updated = sqlx::query(sql!(
        "UPDATE snapshots
             SET sandbox_id = $3
             WHERE id = $1
               AND sandbox_id IN ($2, $3)
               AND type = 'disk'
               AND artifact_kind = 'disk'
               AND artifact_origin = 'fork'
               AND state = 'ready'"
    ))
    .bind(snapshot_id)
    .bind(source_sandbox_id.as_uuid())
    .bind(target_sandbox_id.as_uuid())
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(updated == 1)
}

/// Detach live storage location `storage_location_id` from fork copy
/// `snapshot_id` and stamp it orphaned at the database clock, leaving its
/// bytes to the storage sweeper. `false` when the location no longer belongs
/// to that snapshot or is already orphaned.
pub async fn orphan_fork_copy_location(
    executor: impl Executor<'_>,
    snapshot_id: SnapshotId,
    storage_location_id: Uuid,
) -> Result<bool, DbError> {
    let updated = sqlx::query(sql!(
        "UPDATE snapshot_storage_locations
             SET snapshot_id = NULL, orphaned_at = NOW()
             WHERE id = $1 AND snapshot_id = $2 AND orphaned_at IS NULL"
    ))
    .bind(storage_location_id)
    .bind(snapshot_id)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(updated == 1)
}

/// Delete fork copy `snapshot_id` once no live storage location holds it.
/// `false` when there's no such fork copy or a live location still points at
/// it.
pub async fn delete_unlocated_fork_copy(
    executor: impl Executor<'_>,
    snapshot_id: SnapshotId,
) -> Result<bool, DbError> {
    let deleted = sqlx::query(sql!(
        "DELETE FROM snapshots s
             WHERE s.id = $1
               AND s.artifact_origin = 'fork'
               AND NOT EXISTS (
                   SELECT 1 FROM snapshot_storage_locations ssl
                   WHERE ssl.snapshot_id = s.id AND ssl.orphaned_at IS NULL
               )"
    ))
    .bind(snapshot_id)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(deleted == 1)
}

#[cfg(test)]
mod tests;
