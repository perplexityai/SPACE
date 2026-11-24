//! The suspend snapshot a fork copies from its frozen source.

use std::sync::LazyLock;

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::no_newer_full_snapshot;
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;
use uuid::Uuid;

/// A suspended source's current suspend snapshot and the live storage
/// location that holds its bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SuspendedSourceArtifact {
    pub snapshot_id: SnapshotId,
    pub storage_location_id: Uuid,
}

/// The ready suspend snapshot `source_sandbox_id` would restore from while it
/// is `suspended`, with its newest live storage location; `None` when it isn't
/// suspended or has no such snapshot.
///
/// The snapshot is the one the sandbox's suspend marker names, or with no
/// marker the newest one taken since its last placement change, and no newer
/// full snapshot may exist.
pub async fn suspended_artifact(
    executor: impl sqlx::PgExecutor<'_>,
    source_sandbox_id: SandboxId,
) -> Result<Option<SuspendedSourceArtifact>, sqlx::Error> {
    static SUSPENDED_ARTIFACT: LazyLock<String> = LazyLock::new(|| {
        let no_newer = no_newer_full_snapshot("s", "sn");
        sql!(format!(
            "SELECT sn.id, ssl.id
             FROM sandboxes s
             JOIN snapshots sn
               ON sn.sandbox_id = s.id
              AND sn.artifact_kind = 'full'
              AND sn.type = 'full'
              AND sn.state = 'ready'
              AND (sn.artifact_origin IS NULL OR sn.artifact_origin = 'suspend')
             JOIN LATERAL (
                 SELECT id
                 FROM snapshot_storage_locations
                 WHERE snapshot_id = sn.id AND orphaned_at IS NULL
                 ORDER BY created_at DESC, id DESC
                 LIMIT 1
             ) ssl ON TRUE
             WHERE s.id = $1
               AND s.lifecycle = 'suspended'
               AND (
                   (s.current_local_artifact_id IS NOT NULL
                    AND s.current_local_artifact_origin = 'suspend'
                    AND sn.local_artifact_id = s.current_local_artifact_id)
                   OR (s.current_local_artifact_id IS NULL
                       AND (s.placement_changed_at IS NULL
                            OR sn.timestamp >= s.placement_changed_at))
               )
               AND {no_newer}
             ORDER BY sn.timestamp DESC, sn.id DESC
             LIMIT 1"
        ))
    });
    let row =
        sqlx::query_as::<_, (SnapshotId, Uuid)>(sqlx::AssertSqlSafe(SUSPENDED_ARTIFACT.as_str()))
            .bind(source_sandbox_id.as_uuid())
            .fetch_optional(executor)
            .await?;
    Ok(row.map(
        |(snapshot_id, storage_location_id)| SuspendedSourceArtifact {
            snapshot_id,
            storage_location_id,
        },
    ))
}
