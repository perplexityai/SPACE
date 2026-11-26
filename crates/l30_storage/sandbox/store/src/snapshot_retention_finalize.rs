//! The sandbox row lock snapshot retention takes first when it finalizes a
//! prune, before it locks and deletes the snapshot row. Every finalizer
//! takes the sandbox row before the snapshot row, so concurrent finalizers
//! cannot deadlock with each other.

use pplx_space_snapshot_model::SnapshotId;

use std::sync::LazyLock;

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_portability::snapshot_is_current_local_artifact;
use pplx_space_sql_locator::sql;

/// Lock the sandbox that owns snapshot `snapshot_id` and return whether
/// that sandbox, unless it is `restoring`, marks the snapshot as its current
/// local artifact ([`snapshot_is_current_local_artifact`]). `None` when the
/// snapshot is gone.
///
/// A `restoring` sandbox answers `false`: its restore selector ignores the
/// marker and reads the pre-restore window, so the marker does not name
/// its restore source.
///
/// `FOR UPDATE OF sb`: the sandbox row lock lasts until the caller's
/// transaction ends, so no marker writer changes the answer before the
/// caller's delete commits. Only the sandbox row is locked; the snapshot's
/// artifact identity columns are immutable after insertion. The caller must
/// pass its open transaction.
pub async fn lock_owner_marks_snapshot<C>(
    tx: &mut C,
    snapshot_id: SnapshotId,
) -> Result<Option<bool>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    /// Built once: every input is a literal or a bind placeholder.
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT (sb.lifecycle <> 'restoring' AND {marked})
               FROM snapshots cur
               JOIN sandboxes sb ON sb.id = cur.sandbox_id
              WHERE cur.id = $1
                FOR UPDATE OF sb",
            marked = snapshot_is_current_local_artifact("sb", "cur"),
        ))
    });
    Ok(
        sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(QUERY.as_str()))
            .bind(snapshot_id)
            .fetch_optional(tx.into_sqlx())
            .await?,
    )
}
