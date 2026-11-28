//! The snapshot a quarantined sandbox recovers to: the node-authored recovery
//! plan's lookups, and the share locks the controller's publication recovery
//! takes on it.

use std::sync::LazyLock;

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::{
    pre_quarantine_suspend_snapshot_current, quarantine_recovery_suspend_snapshot_current,
};
use pplx_space_snapshot_model::{LocalArtifactId, SnapshotId};
use pplx_space_sql_locator::sql;
use sqlx::FromRow;
use time::OffsetDateTime;

/// A recovery-policy-compatible suspend snapshot and its storage route.
#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct RecoverySnapshot {
    pub snapshot_id: SnapshotId,
    pub local_artifact_id: Option<LocalArtifactId>,
    /// The location row's `storage_connection_string`.
    pub storage_uri: String,
    pub size_bytes: Option<i64>,
}

/// The newest snapshot of `sandbox_id` that names local artifact
/// `local_artifact_id` and is still current recovery evidence: a ready suspend
/// snapshot that covers all recorded activity, with no newer full snapshot
/// other than a failed retry of the same artifact. `None` when there's none.
pub async fn exact_artifact_snapshot(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
) -> Result<Option<RecoverySnapshot>, DbError> {
    static EXACT_ARTIFACT_SNAPSHOT: LazyLock<String> = LazyLock::new(|| {
        let current = quarantine_recovery_suspend_snapshot_current("current_sandbox", "snapshot");
        sql!(format!(
            "SELECT snapshot.id AS snapshot_id,
                snapshot.local_artifact_id,
                location.storage_connection_string AS storage_uri,
                snapshot.size_bytes
           FROM snapshots snapshot
           JOIN snapshot_storage_locations location
             ON location.snapshot_id = snapshot.id
           JOIN sandboxes current_sandbox
             ON current_sandbox.id = snapshot.sandbox_id
          WHERE current_sandbox.id = $1
            AND snapshot.local_artifact_id = $2 AND {current}
          ORDER BY snapshot.timestamp DESC
          LIMIT 1"
        ))
    });
    Ok(
        sqlx::query_as(sqlx::AssertSqlSafe(EXACT_ARTIFACT_SNAPSHOT.as_str()))
            .bind(sandbox_id.as_uuid())
            .bind(local_artifact_id)
            .fetch_optional(executor.into_sqlx())
            .await?,
    )
}

/// The newest snapshot of `sandbox_id` that is still current recovery
/// evidence and was taken no later than `quarantine_entered_at`, when its
/// quarantine episode began. `None` when there's none.
pub async fn pre_quarantine_snapshot(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    quarantine_entered_at: OffsetDateTime,
) -> Result<Option<RecoverySnapshot>, DbError> {
    static PRE_QUARANTINE_SNAPSHOT: LazyLock<String> = LazyLock::new(|| {
        let current = pre_quarantine_suspend_snapshot_current("current_sandbox", "snapshot", "$2");
        sql!(format!(
            "SELECT snapshot.id AS snapshot_id,
                snapshot.local_artifact_id,
                location.storage_connection_string AS storage_uri,
                snapshot.size_bytes
           FROM snapshots snapshot
           JOIN snapshot_storage_locations location
             ON location.snapshot_id = snapshot.id
           JOIN sandboxes current_sandbox
             ON current_sandbox.id = snapshot.sandbox_id
          WHERE current_sandbox.id = $1
            AND {current}
          ORDER BY snapshot.timestamp DESC
          LIMIT 1"
        ))
    });
    Ok(
        sqlx::query_as(sqlx::AssertSqlSafe(PRE_QUARANTINE_SNAPSHOT.as_str()))
            .bind(sandbox_id.as_uuid())
            .bind(quarantine_entered_at)
            .fetch_optional(executor.into_sqlx())
            .await?,
    )
}

/// Lock snapshot `snapshot_id` `FOR SHARE`, so it can't change or be deleted
/// while recovery re-checks it. The lock lasts until the caller's transaction
/// ends; a missing row locks nothing.
///
/// The caller must pass its open transaction.
pub async fn share_lock_snapshot<C>(tx: &mut C, snapshot_id: SnapshotId) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!("SELECT id FROM snapshots WHERE id = $1 FOR SHARE"))
        .bind(snapshot_id)
        .execute(tx.into_sqlx())
        .await?;
    Ok(())
}

/// Lock `FOR SHARE` the storage location of snapshot `snapshot_id`, so it
/// can't be orphaned or deleted while recovery re-checks it. The lock lasts
/// until the caller's transaction ends; a missing row locks nothing.
///
/// The caller must pass its open transaction.
pub async fn share_lock_storage_location<C>(
    tx: &mut C,
    snapshot_id: SnapshotId,
) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "SELECT snapshot_id FROM snapshot_storage_locations WHERE snapshot_id = $1 FOR SHARE"
    ))
    .bind(snapshot_id)
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}
