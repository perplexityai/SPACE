//! The restore episode of a sandbox a forced node drain suspended: the claim
//! that opens it, the commit that lands it in its target lifecycle, and the
//! read the operation decides from before it tries the claim.
//!
//! The drain's v2 brings each evacuated sandbox back through a durable
//! operation of its own rather than v1's `evacuation_restore_target` marker,
//! which it never writes. The operation names the target lifecycle
//! ([`DrainRestoreTarget`]) and the suspend snapshot the drain left, S. Its
//! claim opens a restore episode that writes the target to
//! `sandboxes.restore_episode_target` (see
//! [`EvacuationRestoreTarget`](crate::sweeper::EvacuationRestoreTarget) for
//! what every other reader makes of it) and a restore lease the operation
//! owns. The commit, and every statement that otherwise ends the episode,
//! clears the target.
//!
//! The transient-lifecycle sweeper never touches a `restoring` row with a
//! target ([`crate::sweeper::evacuation_guards`]): its decision table fails a
//! `restoring` row without a ready snapshot, which would drop the user's
//! state. The operation ages its own episodes out instead. A failed attempt
//! reverts its claim, fenced by the lease token, and an attempt that may have
//! reached the node leaves the row `restoring` with its lease released, which
//! the claim's re-drive arm takes back.

use std::fmt;
use std::str::FromStr;
use std::sync::LazyLock;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{SandboxId, SandboxLifecycle, SandboxLifecycleParseError};
use pplx_space_sandbox_portability::{no_newer_full_snapshot, snapshot_is_current_local_artifact};
use sqlx::PgExecutor;
use uuid::Uuid;

use crate::restore_lease::RestoreClaimRow;
use crate::sweeper::EvacuationRestoreTarget;

/// The lifecycle a drain restore lands a suspended sandbox in: the one it had
/// before the drain suspended it. A drained `stopped` sandbox comes back
/// through a stopped start from its stop artifact, not through this restore.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DrainRestoreTarget {
    Started,
    Paused,
}

impl DrainRestoreTarget {
    pub const ALL: [Self; 2] = [Self::Started, Self::Paused];

    /// The stored string, which [`FromStr`] accepts back. It is both the
    /// `restore_episode_target` the claim writes and the lifecycle the
    /// commit writes.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Paused => "paused",
        }
    }

    pub const fn lifecycle(self) -> SandboxLifecycle {
        match self {
            Self::Started => SandboxLifecycle::Started,
            Self::Paused => SandboxLifecycle::Paused,
        }
    }

    /// The same value as the episode target every reader of the column
    /// parses.
    pub const fn episode_target(self) -> EvacuationRestoreTarget {
        match self {
            Self::Started => EvacuationRestoreTarget::Started,
            Self::Paused => EvacuationRestoreTarget::Paused,
        }
    }
}

impl fmt::Display for DrainRestoreTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A value that isn't a [`DrainRestoreTarget`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DrainRestoreTargetParseError {
    #[error(transparent)]
    UnknownLifecycle(#[from] SandboxLifecycleParseError),
    #[error("sandbox lifecycle '{0}' is not a drain restore target")]
    NotATarget(SandboxLifecycle),
}

/// Parses as [`SandboxLifecycle`] does, then keeps `started` and `paused`.
impl FromStr for DrainRestoreTarget {
    type Err = DrainRestoreTargetParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.parse::<SandboxLifecycle>()? {
            SandboxLifecycle::Started => Ok(Self::Started),
            SandboxLifecycle::Paused => Ok(Self::Paused),
            other => Err(DrainRestoreTargetParseError::NotATarget(other)),
        }
    }
}

/// Claim the drain restore of `sandbox_id` toward `target` from snapshot
/// `snapshot_id` under a new restore lease `restore_lease_token`. Writes an
/// **operation step** (the claim). Returns the row's lifecycle before the
/// claim and the token, or `None` when nothing may be claimed; the row is
/// then unchanged.
///
/// Two arms, each requiring no evacuation marker, no live restore lease, no
/// active execution in `creating`/`created`/`recovering`/`running` updated
/// within `execution_ownership_ttl_secs`, and the snapshot the drain's
/// restore selector would load (`NewestRemote`, the Kubernetes store's
/// `selected_suspend_restore_storage_uri`) to be `snapshot_id`:
///
/// * **Fresh**: a `suspended` row with no episode target. It moves to
///   `restoring` with `target` as its episode target, which opens the
///   episode.
/// * **Re-drive**: a `restoring` row whose episode target is already
///   `target`, an episode whose attempt ended without settling it. It takes
///   a new lease. Only this claim writes a `started` or `paused` episode
///   target, and an operation's executions on one sandbox never overlap, so
///   such a row is an earlier attempt's, or that of an operation that is
///   gone.
///
/// The selector subquery repeats the selector's predicate whole rather than
/// a fragment of it, apart from the shared `pplx_space_sandbox_portability`
/// predicates both read.
pub async fn claim_drain_restore(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    snapshot_id: Uuid,
    target: DrainRestoreTarget,
    lease_ttl_secs: i64,
    restore_lease_token: Uuid,
    execution_ownership_ttl_secs: i64,
) -> Result<Option<RestoreClaimRow>, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static CLAIM: LazyLock<String> = LazyLock::new(|| {
        let current_artifact = snapshot_is_current_local_artifact("s", "sn");
        let no_newer = no_newer_full_snapshot("s", "sn");
        sql!(format!(
            "WITH candidate AS (
                 SELECT s.id, s.lifecycle
                   FROM sandboxes s
                  WHERE s.id = $1
                    AND s.evacuation_restore_target IS NULL
                    AND (
                      (s.lifecycle = 'suspended' AND s.restore_episode_target IS NULL)
                      OR (s.lifecycle = 'restoring' AND s.restore_episode_target = $3)
                    )
                    AND (
                      s.restore_lease_expires_at IS NULL
                      OR s.restore_lease_expires_at <= NOW()
                    )
                    AND NOT EXISTS (
                      SELECT 1
                        FROM sandbox_k8s_executions e
                       WHERE e.sandbox_id = s.id
                         AND e.active
                         AND e.state IN ('creating', 'created', 'recovering', 'running')
                         AND e.updated_at > NOW() - make_interval(secs => $6)
                    )
                    AND (
                      SELECT sn.id
                        FROM snapshots sn
                        JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
                       WHERE sn.sandbox_id = s.id
                         AND ssl.orphaned_at IS NULL
                         AND sn.type = 'full'
                         AND sn.state = 'ready'
                         AND (
                           {current_artifact}
                           OR (s.current_local_artifact_id IS NULL AND {no_newer})
                         )
                       ORDER BY sn.timestamp DESC
                       LIMIT 1
                    ) = $2
                  FOR UPDATE OF s
             )
             UPDATE sandboxes s
                SET lifecycle = 'restoring',
                    restore_episode_target = $3,
                    restore_lease_expires_at = NOW() + ($4::bigint * INTERVAL '1 second'),
                    restore_lease_token = $5
               FROM candidate c
              WHERE s.id = c.id
              RETURNING c.lifecycle, s.restore_lease_token"
        ))
    });
    sqlx::query_as::<_, RestoreClaimRow>(sqlx::AssertSqlSafe(CLAIM.as_str()))
        .bind(sandbox_id)
        .bind(snapshot_id)
        .bind(target.as_str())
        .bind(lease_ttl_secs)
        .bind(restore_lease_token)
        .bind(execution_ownership_ttl_secs)
        .fetch_optional(executor)
        .await
}

/// Land the drain restore whose lease `restore_lease_token` holds in
/// `target`, on its node, and end the episode. Writes an **operation step**
/// (the claim's completion). Returns whether the row moved.
///
/// The row must still be `restoring` under that token with `target` as its
/// episode target. A row a revert, a delete or a user takeover has moved on
/// matches nothing, and leaving `restoring` clears the lease through the
/// `sandboxes_clear_restore_lease` trigger.
pub async fn complete_drain_restore(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    restore_lease_token: Uuid,
    target: DrainRestoreTarget,
) -> Result<bool, sqlx::Error> {
    // `$3` is both the lifecycle the row lands in and the episode target it
    // must hold: the two spell each target the same way.
    let completed = sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = $3,
                placement = 'on_node',
                restore_episode_target = NULL
          WHERE id = $1
            AND lifecycle = 'restoring'
            AND restore_lease_token = $2
            AND restore_episode_target = $3"
    ))
    .bind(sandbox_id)
    .bind(restore_lease_token)
    .bind(target.as_str())
    .execute(executor)
    .await?
    .rows_affected();
    Ok(completed == 1)
}

/// What [`read_drain_restore_gate`] reads of a sandbox and the snapshot a
/// drain restore names.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct DrainRestoreGateRow {
    pub lifecycle: String,
    /// Whether the v1 drain's `evacuation_restore_target` marker is set.
    pub evacuation_marked: bool,
    pub restore_episode_target: Option<String>,
    /// The snapshot the drain's restore selector would load now, as the
    /// claim reads it.
    pub selected_snapshot: Option<Uuid>,
    /// The named snapshot's state, when it is a full snapshot of this
    /// sandbox.
    pub source_state: Option<String>,
    /// Whether the named snapshot has a storage location that isn't
    /// orphaned.
    pub source_located: bool,
    /// Whether the sandbox has a full snapshot, in any state, newer than the
    /// named one.
    pub newer_full_snapshot: bool,
    /// Whether the named snapshot is the sandbox's current local artifact,
    /// or the sandbox records none.
    pub source_is_current_artifact: bool,
}

/// Read what decides whether a drain restore of `sandbox_id` from
/// `snapshot_id` may try its claim, and if not, why. A plain
/// read: it takes no lock, and the claim re-checks everything it needs.
/// `None` when no row exists.
pub async fn read_drain_restore_gate(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    snapshot_id: Uuid,
) -> Result<Option<DrainRestoreGateRow>, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static GATE: LazyLock<String> = LazyLock::new(|| {
        let current_artifact = snapshot_is_current_local_artifact("s", "sn");
        let no_newer = no_newer_full_snapshot("s", "sn");
        sql!(format!(
            "SELECT s.lifecycle,
                    s.evacuation_restore_target IS NOT NULL AS evacuation_marked,
                    s.restore_episode_target,
                    (
                      SELECT sn.id
                        FROM snapshots sn
                        JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
                       WHERE sn.sandbox_id = s.id
                         AND ssl.orphaned_at IS NULL
                         AND sn.type = 'full'
                         AND sn.state = 'ready'
                         AND (
                           {current_artifact}
                           OR (s.current_local_artifact_id IS NULL AND {no_newer})
                         )
                       ORDER BY sn.timestamp DESC
                       LIMIT 1
                    ) AS selected_snapshot,
                    src.state AS source_state,
                    EXISTS (
                      SELECT 1
                        FROM snapshot_storage_locations ssl
                       WHERE ssl.snapshot_id = src.id
                         AND ssl.orphaned_at IS NULL
                    ) AS source_located,
                    EXISTS (
                      SELECT 1
                        FROM snapshots newer
                       WHERE newer.sandbox_id = s.id
                         AND newer.type = 'full'
                         AND newer.timestamp > src.timestamp
                    ) AS newer_full_snapshot,
                    (
                      s.current_local_artifact_id IS NULL
                      OR (
                        src.local_artifact_id IS NOT DISTINCT FROM s.current_local_artifact_id
                        AND src.artifact_kind IS NOT DISTINCT FROM s.current_local_artifact_kind
                      )
                    ) AS source_is_current_artifact
               FROM sandboxes s
               LEFT JOIN snapshots src
                 ON src.id = $2
                AND src.sandbox_id = s.id
                AND src.type = 'full'
              WHERE s.id = $1"
        ))
    });
    sqlx::query_as::<_, DrainRestoreGateRow>(sqlx::AssertSqlSafe(GATE.as_str()))
        .bind(sandbox_id)
        .bind(snapshot_id)
        .fetch_optional(executor)
        .await
}

/// Whether `sandbox_id` is `restoring` in a drain restore's episode: one whose
/// target is `started` or `paused`. Such an episode is settled only by the
/// operation that opened it (or a later one for the same sandbox), since the
/// transient sweeper leaves it alone. A plain read.
pub async fn drain_restore_episode_open(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT COALESCE(
                    lifecycle = 'restoring'
                    AND restore_episode_target IN ('started', 'paused'),
                    FALSE
                )
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await?
    .unwrap_or(false))
}
