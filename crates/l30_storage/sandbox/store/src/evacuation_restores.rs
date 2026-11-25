//! Reads of node drain's cohort restore sweep: which evacuated sandboxes are
//! ready to be placed again, and whether one still is.
//!
//! An evacuated row carries `evacuation_restore_target`. A `started` or
//! `paused` evacuation suspended the VM and waits on its newest full snapshot
//! ([`latest_full_snapshot_ready`]); a `stopped` one published its disk
//! artifact instead and waits on that artifact's backup
//! ([`current_disk_artifact_backup_ready`]). Neither read is scoped to a node:
//! the restore reads from the durable copy, not from the source node.

use std::sync::LazyLock;

use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use pplx_space_sandbox_portability::{
    current_disk_artifact_backup_ready, latest_full_snapshot_ready,
};
use pplx_space_sql_locator::sql;

/// One evacuated sandbox ready for its cohort restore, and the lifecycle it
/// restores to, as stored.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct RestorePendingEvacuation {
    pub id: SandboxId,
    pub evacuation_restore_target: String,
}

/// No binds. The sweep with its retry backoff honoured: a row whose
/// `next_restore_attempt_at` hasn't passed is left out, and rows sort by
/// `restore_attempt_count` first so a serial failer can't take the slots ahead
/// of never-failed evacuations. Within a tier, most recently responsible
/// first: the stopped arm's key is `placement_changed_at`, because its claim
/// and teardown leave `lifecycle_changed_at` at the user's original stop.
///
/// The two arms, the per-arm recency key and the backoff's escape hatch are
/// argued on `SubstrateSink::list_restore_pending_evacuations` in
/// `pplx-space-cluster-controller-server`'s `node_drain/store.rs`.
static PENDING_HONORING_BACKOFF: LazyLock<String> = LazyLock::new(|| {
    let snapshot_exists = latest_full_snapshot_ready("s");
    let disk_backup_ready = current_disk_artifact_backup_ready("s");
    sql!(format!(
        "SELECT s.id, s.evacuation_restore_target
           FROM sandboxes s
          WHERE s.evacuation_restore_target IS NOT NULL
            AND s.lifecycle IN ('suspended', 'restoring', 'stopped')
            AND (s.next_restore_attempt_at IS NULL OR s.next_restore_attempt_at <= NOW())
            AND (
                (
                    s.evacuation_restore_target <> 'stopped'
                    AND s.lifecycle IN ('suspended', 'restoring')
                    AND {snapshot_exists}
                )
                OR (
                    s.evacuation_restore_target = 'stopped'
                    AND (
                        (s.lifecycle = 'stopped' AND s.active_node_id IS NULL)
                        -- A re-placement the claim already took: one whose
                        -- controller died mid-restore, or whose attempt failed
                        -- after the claim (a fresh stopped claim is never rolled
                        -- back). This arm is how it is re-driven; omitting it
                        -- would strand exactly those rows.
                        OR s.lifecycle = 'restoring'
                    )
                    AND {disk_backup_ready}
                )
            )
          ORDER BY s.restore_attempt_count ASC,
                   CASE
                       WHEN s.evacuation_restore_target = 'stopped'
                       THEN COALESCE(s.placement_changed_at, s.lifecycle_changed_at)
                       ELSE s.lifecycle_changed_at
                   END DESC,
                   s.id DESC"
    ))
});

/// No binds. [`PENDING_HONORING_BACKOFF`] with the backoff ignored entirely:
/// no window gate and no attempt-tier key, so stale windows and frozen attempt
/// counts written before the backoff was disabled stop mattering at once.
static PENDING_IGNORING_BACKOFF: LazyLock<String> = LazyLock::new(|| {
    let snapshot_exists = latest_full_snapshot_ready("s");
    let disk_backup_ready = current_disk_artifact_backup_ready("s");
    sql!(format!(
        "SELECT s.id, s.evacuation_restore_target
           FROM sandboxes s
          WHERE s.evacuation_restore_target IS NOT NULL
            AND s.lifecycle IN ('suspended', 'restoring', 'stopped')
            AND (
                (
                    s.evacuation_restore_target <> 'stopped'
                    AND s.lifecycle IN ('suspended', 'restoring')
                    AND {snapshot_exists}
                )
                OR (
                    s.evacuation_restore_target = 'stopped'
                    AND (
                        (s.lifecycle = 'stopped' AND s.active_node_id IS NULL)
                        -- A re-placement the claim already took: one whose
                        -- controller died mid-restore, or whose attempt failed
                        -- after the claim (a fresh stopped claim is never rolled
                        -- back). This arm is how it is re-driven; omitting it
                        -- would strand exactly those rows.
                        OR s.lifecycle = 'restoring'
                    )
                    AND {disk_backup_ready}
                )
            )
          ORDER BY
                   CASE
                       WHEN s.evacuation_restore_target = 'stopped'
                       THEN COALESCE(s.placement_changed_at, s.lifecycle_changed_at)
                       ELSE s.lifecycle_changed_at
                   END DESC,
                   s.id DESC"
    ))
});

/// Every evacuated sandbox, cluster-wide, whose restore source is ready, in
/// the order the sweep fills its slots. With `honor_backoff_windows`, rows
/// still inside their retry window are left out and attempt tiers sort first.
/// A read; it takes no lock, and the window compares against the executor's
/// `NOW()`.
pub async fn pending(
    executor: impl sqlx::PgExecutor<'_>,
    honor_backoff_windows: bool,
) -> Result<Vec<RestorePendingEvacuation>, sqlx::Error> {
    let statement = if honor_backoff_windows {
        PENDING_HONORING_BACKOFF.as_str()
    } else {
        PENDING_IGNORING_BACKOFF.as_str()
    };
    sqlx::query_as(sqlx::AssertSqlSafe(statement))
        .fetch_all(executor)
        .await
}

/// `$1` = sandbox id. A suspend evacuation's eligibility: marked, with its
/// newest full snapshot ready.
static ELIGIBLE_FROM_SNAPSHOT: LazyLock<String> = LazyLock::new(|| {
    let ready_exists = latest_full_snapshot_ready("s");
    sql!(format!(
        "SELECT (s.evacuation_restore_target IS NOT NULL AND {ready_exists}) \
         FROM sandboxes s WHERE s.id = $1"
    ))
});

/// `$1` = sandbox id. A stopped evacuation's eligibility: marked, with its
/// current disk artifact's backup ready.
static ELIGIBLE_FROM_DISK_ARTIFACT: LazyLock<String> = LazyLock::new(|| {
    let ready_exists = current_disk_artifact_backup_ready("s");
    sql!(format!(
        "SELECT (s.evacuation_restore_target IS NOT NULL AND {ready_exists}) \
         FROM sandboxes s WHERE s.id = $1"
    ))
});

/// Whether `sandbox_id` still carries its evacuation marker and the restore
/// source for `target_lifecycle` is ready: the disk artifact's backup for a
/// `stopped` target, the newest full snapshot otherwise. `None` when the row
/// is gone. A read; it takes no lock.
pub async fn restore_eligible(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    target_lifecycle: SandboxLifecycle,
) -> Result<Option<bool>, sqlx::Error> {
    let statement = if target_lifecycle == SandboxLifecycle::Stopped {
        ELIGIBLE_FROM_DISK_ARTIFACT.as_str()
    } else {
        ELIGIBLE_FROM_SNAPSHOT.as_str()
    };
    sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(statement))
        .bind(sandbox_id)
        .fetch_optional(executor)
        .await
}
