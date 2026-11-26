//! Restore-time memory drop: a user restore that claimed a `restoring` row
//! whose environment's memory is retired drops the checkpoint's memory under
//! that same restore lease before any pod is created.

use std::time::Duration;

use pplx_space_accounts::OrgId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::restore_retry_suspended_revert_snapshot_exists;
use pplx_space_snapshot_model::{LocalArtifactId, SnapshotId};
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use uuid::Uuid;

use super::{
    CandidateRead, MemoryDropCandidate, MemoryDropRefusal, NODE_GONE, UNPLACED, candidate_from_row,
    candidate_sql, millis, refusal_from_tail, refusal_tail_columns_with,
};

const LIVE_RESTORE_LEASE: &str = "s.lifecycle = 'restoring' AND s.restore_lease_token = $3 AND s.restore_lease_expires_at > NOW()";

fn pre_restore_snapshot_guard() -> String {
    let safe_revert = restore_retry_suspended_revert_snapshot_exists("s");
    format!(
        "({safe_revert})
            AND (s.lifecycle_changed_at IS NULL OR sn.timestamp < s.lifecycle_changed_at)"
    )
}

fn live_restore_lease_and_revert_guard() -> String {
    let pre_restore = pre_restore_snapshot_guard();
    format!(
        "{LIVE_RESTORE_LEASE}
            AND {pre_restore}"
    )
}

/// Why a `restoring` row can't have its memory dropped under `token`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreDropRefusal {
    /// The row's memory is already dropped (`current_local_artifact_kind = 'disk'`).
    AlreadyDropped,
    /// The caller no longer holds a live restore lease on a `restoring` row.
    LostLease,
    /// Another memory-drop precondition failed on the row.
    Refused(MemoryDropRefusal),
}

/// Read the memory-drop candidate of a `restoring` row owned by `token`.
///
/// The caller already claimed `suspended` → `restoring` (or re-claimed a
/// markerless `restoring` row) under `token` and has not created a pod yet.
/// Requires the row's current pin to match a pre-`lifecycle_changed_at`
/// revertable checkpoint ([`restore_retry_suspended_revert_snapshot_exists`]),
/// so a `restoring` row whose `current_local_artifact_id` was overwritten by a
/// post-claim failed restore attempt is never projected over the pre-restore
/// checkpoint. Every other precondition matches [`super::candidate`]:
/// Kubernetes, unplaced, unencrypted, pinned to a ready durable `full`
/// `suspend` artifact that is the newest full snapshot, and off a gone node.
pub async fn candidate_for_restore(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    token: Uuid,
    read: CandidateRead,
) -> Result<Option<MemoryDropCandidate>, sqlx::Error> {
    let guard = live_restore_lease_and_revert_guard();
    let query = candidate_sql(&guard, read);
    type Row = (OrgId, LocalArtifactId, SnapshotId, String);
    let row: Option<Row> = sqlx::query_as(sqlx::AssertSqlSafe(query))
        .bind(sandbox_id)
        .bind(SubstrateKind::Kubernetes)
        .bind(token)
        .fetch_optional(executor)
        .await?;
    Ok(row.map(candidate_from_row))
}

/// Why the row isn't a [`candidate_for_restore`] under `token`, or `None` when
/// it is one. Plain diagnostic read.
pub async fn refusal_for_restore(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    token: Uuid,
) -> Result<Option<RestoreDropRefusal>, sqlx::Error> {
    let pre_restore = pre_restore_snapshot_guard();
    let tail = refusal_tail_columns_with(&pre_restore);
    let query = sql!(format!(
        "SELECT COALESCE((s.managed_by = $2), FALSE) AS kubernetes,
                COALESCE((
                    s.lifecycle IN ('suspended', 'restoring')
                    AND s.current_local_artifact_kind = 'disk'
                ), FALSE) AS dropped,
                COALESCE(({LIVE_RESTORE_LEASE}), FALSE) AS lease_live,
                {tail}
           FROM sandboxes s
          WHERE s.id = $1"
    ));
    type Row = (bool, bool, bool, bool, bool, bool, bool, bool, bool);
    let row: Option<Row> = sqlx::query_as(sqlx::AssertSqlSafe(query))
        .bind(sandbox_id)
        .bind(SubstrateKind::Kubernetes)
        .bind(token)
        .fetch_optional(executor)
        .await?;
    let Some((
        kubernetes,
        dropped,
        lease_live,
        unplaced,
        unencrypted,
        full_suspend_pin,
        durable,
        newest,
        node_gone,
    )) = row
    else {
        return Ok(Some(RestoreDropRefusal::Refused(
            MemoryDropRefusal::NotFound,
        )));
    };
    if !kubernetes {
        return Ok(Some(RestoreDropRefusal::Refused(
            MemoryDropRefusal::NotKubernetes,
        )));
    }
    if dropped {
        return Ok(Some(RestoreDropRefusal::AlreadyDropped));
    }
    if !lease_live {
        return Ok(Some(RestoreDropRefusal::LostLease));
    }
    Ok(refusal_from_tail((
        unplaced,
        unencrypted,
        full_suspend_pin,
        durable,
        newest,
        node_gone,
    ))
    .map(RestoreDropRefusal::Refused))
}

/// **Guarded transition.** Extend the caller's restore lease on `restoring` by
/// at least `ttl` from now while the disk copy runs, never shortening an
/// existing longer user-restore lease. Returns `false` when the row is no
/// longer `restoring` under `token`.
pub async fn renew_for_restore(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    token: Uuid,
    ttl: Duration,
) -> Result<bool, sqlx::Error> {
    let renewed = sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = GREATEST(
                    COALESCE(restore_lease_expires_at, NOW()),
                    NOW() + ($3::bigint * INTERVAL '1 millisecond')
                )
          WHERE id = $1
            AND lifecycle = 'restoring'
            AND restore_lease_token = $2"
    ))
    .bind(sandbox_id)
    .bind(token)
    .bind(millis(ttl))
    .execute(executor)
    .await?;
    Ok(renewed.rows_affected() == 1)
}

/// **Guarded transition.** Re-pin the `restoring` row owned by `token` to the
/// `disk` `stop` artifact `disk_artifact_id` and park it back in `suspended`,
/// releasing the restore lease in the same statement.
///
/// Parking in the commit transaction leaves no window where a pod-less
/// `restoring` row is pinned to `disk`: either the transaction rolls back and
/// the full pin remains intact, or it commits and the row has the exact
/// [`super::dropped`] shape (`suspended`, unplaced, `disk` `stop` pin).
pub async fn commit_for_restore(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    token: Uuid,
    candidate: &MemoryDropCandidate,
    disk_artifact_id: LocalArtifactId,
) -> Result<bool, sqlx::Error> {
    let safe_revert = restore_retry_suspended_revert_snapshot_exists("s");
    let result = sqlx::query(sqlx::AssertSqlSafe(sql!(format!(
        "UPDATE sandboxes s
            SET lifecycle = 'suspended',
                placement = 'unplaced',
                placement_changed_at = NULL,
                current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop',
                restore_lease_token = NULL,
                restore_lease_expires_at = NULL
          WHERE s.id = $1
            AND s.lifecycle = 'restoring'
            AND {UNPLACED}
            AND s.restore_lease_token = $4
            AND s.current_local_artifact_id = $3
            AND s.current_local_artifact_kind = 'full'
            AND {NODE_GONE}
            AND {safe_revert}"
    ))))
    .bind(sandbox_id)
    .bind(disk_artifact_id)
    .bind(candidate.pinned_artifact_id)
    .bind(token)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// **Guarded transition.** Park an unplaced `restoring` row owned by `token`
/// back to `suspended` without changing its artifact pin when a pre-pod
/// restore-time memory drop fails retryably, provided its pre-restore snapshot
/// is still revertable.
pub async fn park_unplaced_restore_as_suspended(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    token: Uuid,
) -> Result<bool, sqlx::Error> {
    let safe_revert = restore_retry_suspended_revert_snapshot_exists("s");
    let result = sqlx::query(sqlx::AssertSqlSafe(sql!(format!(
        "UPDATE sandboxes s
            SET lifecycle = 'suspended',
                placement = 'unplaced',
                placement_changed_at = NULL,
                restore_lease_token = NULL,
                restore_lease_expires_at = NULL
          WHERE s.id = $1
            AND s.lifecycle = 'restoring'
            AND s.restore_lease_token = $2
            AND {UNPLACED}
            AND {safe_revert}"
    ))))
    .bind(sandbox_id)
    .bind(token)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() == 1)
}
