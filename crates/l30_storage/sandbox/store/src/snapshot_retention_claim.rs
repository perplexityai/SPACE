//! Snapshot retention's lock of the `sandboxes` row when it claims one of
//! the sandbox's snapshots for pruning.
//!
//! Retention keeps the rest of its claim transaction: the fork-source
//! advisory lock it takes first, the re-checks under the lock, the snapshot
//! and storage-location locks, and the claim write.

use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;

use pplx_space_sql_locator::sql;
use sqlx::PgConnection;

/// Lock the sandbox's row `FOR UPDATE OF sb` when `snapshot_id` is one of
/// its snapshots, and return its id; `None` when the sandbox or that
/// snapshot of it is absent. The snapshot row is joined, not locked. The lock
/// lasts until the caller's transaction ends.
pub async fn lock_snapshot_owner(
    conn: &mut PgConnection,
    snapshot_id: SnapshotId,
    sandbox_id: SandboxId,
) -> Result<Option<SandboxId>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT sb.id
           FROM sandboxes sb
           JOIN snapshots cur ON cur.sandbox_id = sb.id
          WHERE cur.id = $1
            AND sb.id = $2
          FOR UPDATE OF sb"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .fetch_optional(conn)
    .await
}
