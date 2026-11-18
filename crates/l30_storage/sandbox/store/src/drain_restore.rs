//! A forced v2 drain's restore of a sandbox it suspended.
//!
//! The drain's v2 brings each suspended sandbox it found `started` back
//! through a durable operation of its own rather than v1's
//! `evacuation_restore_target` marker, which it never writes. The operation
//! names the suspend snapshot the drain left, S. Each attempt claims the row
//! the way a user's restore does, `suspended` to `restoring` under a restore
//! lease the operation owns ([`claim_drain_restore`]), and lands it `started`
//! ([`complete_drain_restore`]). A sandbox the drain found `paused` gets no
//! drain restore: it stays `suspended`, and its next resume or wake restores
//! it.
//!
//! An attempt that fails before any Wake was sent reverts to `suspended`
//! through the restore claim's ordinary cleanup, and the operation retries.
//! One whose Wake was sent keeps `restoring` under its lease, since the Wake
//! may still land, and so does a crashed attempt. The node's receipt then
//! lands the row `started`, or the lease lapses and the transient-lifecycle
//! sweeper settles the row as it settles any restore; the operation's next
//! attempt reads whichever it finds. So between attempts the row is
//! `suspended`, a sandbox every reader already handles, or `restoring` under
//! the operation's lease, which every reader handles as a restore in flight.

use std::sync::LazyLock;

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::{no_newer_full_snapshot, snapshot_is_current_local_artifact};
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use uuid::Uuid;

/// Why [`claim_drain_restore`] took nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainRestoreRefusal {
    /// The row is gone or being deleted.
    Deleted,
    /// The sandbox left `suspended` from S some other way: a user's call, a
    /// newer snapshot, the v1 drain's marker, or an earlier attempt's Wake
    /// that landed after the attempt gave up, which nothing on the row tells
    /// apart from a user's. The restore is not needed.
    Superseded,
    /// A restore, a lease or an execution holds the row: wait for it to
    /// settle and read it again.
    InFlight,
    /// S is still uploading or registering.
    SourcePending,
    /// S can't be loaded: missing, failed or deleting, or stored nowhere, and
    /// nothing newer exists.
    SourceLost,
}

/// What [`claim_drain_restore`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainRestoreClaim {
    /// The row is `restoring` under the operation's lease.
    Claimed,
    Refused(DrainRestoreRefusal),
}

#[derive(sqlx::FromRow)]
struct ClaimRow {
    claimed: bool,
    lifecycle: Option<String>,
    marked: bool,
    in_flight: bool,
    selected_snapshot: Option<Uuid>,
    source_state: Option<String>,
    source_located: bool,
    newer_full_snapshot: bool,
    source_is_current_artifact: bool,
}

/// Claim the drain restore of `sandbox_id` from snapshot `snapshot_id` under
/// the restore lease `restore_lease_token`, as a user's restore claims a
/// suspended row. Writes an **operation step** (the claim). One statement:
/// it claims a `suspended` row with no evacuation marker, no live restore
/// lease and no active execution in `creating`/`created`/`recovering`/
/// `running` updated within `execution_ownership_ttl_secs`, whose drain
/// restore selector (`NewestRemote`, the Kubernetes store's
/// `selected_suspend_restore_storage_uri`) loads `snapshot_id`; otherwise it
/// returns what it read and the row is unchanged, and the refusal says why.
pub async fn claim_drain_restore(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    snapshot_id: Uuid,
    lease_ttl_secs: i64,
    restore_lease_token: Uuid,
    execution_ownership_ttl_secs: i64,
) -> Result<DrainRestoreClaim, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static CLAIM: LazyLock<String> = LazyLock::new(|| {
        let current_artifact = snapshot_is_current_local_artifact("s", "sn");
        let no_newer = no_newer_full_snapshot("s", "sn");
        sql!(format!(
            "WITH facts AS (
                 SELECT s.id,
                        s.lifecycle,
                        s.evacuation_restore_target IS NOT NULL AS marked,
                        -- A restore lease lives only on a `restoring` row (a
                        -- trigger clears it on every other), so a `suspended`
                        -- row's restore in flight is a live execution.
                        EXISTS (
                          SELECT 1
                            FROM sandbox_k8s_executions e
                           WHERE e.sandbox_id = s.id
                             AND e.active
                             AND e.state IN ('creating', 'created', 'recovering', 'running')
                             AND e.updated_at > NOW() - make_interval(secs => $5)
                        ) AS in_flight,
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
                        ) AS selected_snapshot
                   FROM sandboxes s
                  WHERE s.id = $1
                  FOR UPDATE OF s
             ), claimed AS (
                 UPDATE sandboxes s
                    SET lifecycle = 'restoring',
                        restore_lease_expires_at = NOW() + ($3::bigint * INTERVAL '1 second'),
                        restore_lease_token = $4
                   FROM facts f
                  WHERE s.id = f.id
                    AND f.lifecycle = 'suspended'
                    AND NOT f.marked
                    AND NOT f.in_flight
                    AND f.selected_snapshot = $2
                 RETURNING s.id
             )
             SELECT EXISTS (SELECT 1 FROM claimed) AS claimed,
                    f.lifecycle,
                    COALESCE(f.marked, FALSE) AS marked,
                    COALESCE(f.in_flight, FALSE) AS in_flight,
                    f.selected_snapshot,
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
                       WHERE newer.sandbox_id = $1
                         AND newer.type = 'full'
                         AND newer.timestamp > src.timestamp
                    ) AS newer_full_snapshot,
                    (
                      cur.current_local_artifact_id IS NULL
                      OR (
                        src.local_artifact_id IS NOT DISTINCT FROM cur.current_local_artifact_id
                        AND src.artifact_kind IS NOT DISTINCT FROM cur.current_local_artifact_kind
                      )
                    ) AS source_is_current_artifact
               FROM (SELECT 1) AS one
               LEFT JOIN facts f ON TRUE
               LEFT JOIN sandboxes cur ON cur.id = $1
               LEFT JOIN snapshots src
                 ON src.id = $2
                AND src.sandbox_id = $1
                AND src.type = 'full'"
        ))
    });
    let row = sqlx::query_as::<_, ClaimRow>(sqlx::AssertSqlSafe(CLAIM.as_str()))
        .bind(sandbox_id)
        .bind(snapshot_id)
        .bind(lease_ttl_secs)
        .bind(restore_lease_token)
        .bind(execution_ownership_ttl_secs)
        .fetch_one(executor)
        .await?;
    Ok(decide_claim(&row, snapshot_id))
}

fn decide_claim(row: &ClaimRow, snapshot_id: Uuid) -> DrainRestoreClaim {
    use DrainRestoreRefusal as R;
    if row.claimed {
        return DrainRestoreClaim::Claimed;
    }
    let refusal = match row.lifecycle.as_deref() {
        None | Some("deleting") | Some("deleted") => R::Deleted,
        _ if row.marked => R::Superseded,
        // A restore in flight, the user's or an earlier attempt's, lands or
        // reverts; the next read decides.
        Some("restoring") => R::InFlight,
        Some("suspended") if row.in_flight => R::InFlight,
        Some("suspended") => match (row.selected_snapshot, row.source_state.as_deref()) {
            (Some(selected), _) if selected != snapshot_id => R::Superseded,
            // Selected and claimable: the claim lost a race with itself.
            (Some(_), _) => R::InFlight,
            // The sandbox has state newer than S, uploading or not.
            (None, _) if row.newer_full_snapshot || !row.source_is_current_artifact => {
                R::Superseded
            }
            (None, Some("creating")) => R::SourcePending,
            // Ready and stored but not selected: mid-registration.
            (None, Some("ready")) if row.source_located => R::SourcePending,
            (None, _) => R::SourceLost,
        },
        Some(_) => R::Superseded,
    };
    DrainRestoreClaim::Refused(refusal)
}

/// How [`complete_drain_restore`] found the row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainRestoreCommit {
    /// The row was `restoring` under the lease and moved to `started`.
    Landed,
    /// The row was already `started` on the restore's own execution: the
    /// node's activation receipt
    /// ([`release_completed_activations`](crate::ingest::release_completed_activations))
    /// landed it first.
    AlreadyLanded,
    /// Neither: a revert, a delete, a user's call or another execution has
    /// moved the row on. Nothing was written.
    Lost,
}

impl DrainRestoreCommit {
    /// Whether the restore is `started`.
    pub const fn landed(self) -> bool {
        matches!(self, Self::Landed | Self::AlreadyLanded)
    }
}

/// Land the drain restore whose lease `restore_lease_token` holds `started`,
/// on its node, and release the activation attempt `activation_attempt_id`
/// the restore ran under on execution `generation`. Writes an **operation
/// step** (the claim's completion).
///
/// Lands a row still `restoring` under that token whose active execution is
/// `generation`; leaving `restoring` clears the lease through the
/// `sandboxes_clear_restore_lease` trigger. Reports
/// [`DrainRestoreCommit::AlreadyLanded`] for a row already `started` with no
/// v1 marker whose active execution is still `generation` and running. Every
/// other row is [`DrainRestoreCommit::Lost`]: a user's restore runs on a new
/// generation, and a user's pause or suspend moves the lifecycle.
///
/// The attempt is released only with a landed row, so a connect never meets
/// the restore's attempt on a row it may use, nor a released attempt on a
/// row the operation will still retry. The row lock comes first, in its own
/// statement: a receipt that lands the row concurrently has committed by the
/// time the commit's own statement takes its snapshot.
pub async fn complete_drain_restore<'c>(
    conn: impl sqlx::Acquire<'c, Database = sqlx::Postgres>,
    sandbox_id: SandboxId,
    restore_lease_token: Uuid,
    generation: i64,
    activation_attempt_id: Option<Uuid>,
) -> Result<DrainRestoreCommit, sqlx::Error> {
    let mut tx = conn.begin().await?;
    sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .execute(&mut *tx)
        .await?;
    let (landed, already): (bool, bool) = sqlx::query_as(sql!(
        "WITH landed AS (
             UPDATE sandboxes s
                SET lifecycle = 'started',
                    placement = 'on_node'
              WHERE s.id = $1
                AND s.evacuation_restore_target IS NULL
                AND s.lifecycle = 'restoring'
                AND s.restore_lease_token = $2
                AND EXISTS (
                  SELECT 1
                    FROM sandbox_k8s_executions e
                   WHERE e.sandbox_id = s.id
                     AND e.active
                     AND e.generation = $3
                )
              RETURNING s.id
         ), already AS (
             SELECT s.id
               FROM sandboxes s
              WHERE s.id = $1
                AND s.lifecycle = 'started'
                AND s.evacuation_restore_target IS NULL
                AND EXISTS (
                  SELECT 1
                    FROM sandbox_k8s_executions e
                   WHERE e.sandbox_id = s.id
                     AND e.active
                     AND e.generation = $3
                     AND e.state = 'running'
                )
         ), released AS (
             UPDATE sandbox_k8s_executions e
                SET activation_attempt_id = NULL,
                    activation_attempt_expires_at = NULL,
                    activation_attempt_target_state = NULL
              WHERE e.sandbox_id = $1
                AND e.active
                AND e.generation = $3
                AND e.activation_attempt_id = $4
                AND (EXISTS (SELECT 1 FROM landed) OR EXISTS (SELECT 1 FROM already))
              RETURNING e.id
         )
         SELECT EXISTS (SELECT 1 FROM landed),
                EXISTS (SELECT 1 FROM already)
           FROM (SELECT COUNT(*) FROM released) AS released_count"
    ))
    .bind(sandbox_id)
    .bind(restore_lease_token)
    .bind(generation)
    .bind(activation_attempt_id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(if landed {
        DrainRestoreCommit::Landed
    } else if already {
        DrainRestoreCommit::AlreadyLanded
    } else {
        DrainRestoreCommit::Lost
    })
}
