//! A forced v2 drain's restore of a sandbox it suspended, and its release of
//! a stopped one.
//!
//! The drain's v2 brings each suspended sandbox back through a durable
//! operation of its own rather than v1's `evacuation_restore_target` marker,
//! which it never writes. The operation names the target lifecycle, `started`
//! or `paused` (an [`EvacuationRestoreTarget`]), and the suspend snapshot the
//! drain left, S. Each attempt claims the row the way a user's restore does,
//! `suspended` to `restoring` under a restore lease the operation owns
//! ([`claim_drain_restore`]), and lands it in the target
//! ([`complete_drain_restore`]). An attempt that fails reverts to `suspended`
//! through the restore claim's ordinary cleanup, so between attempts the row
//! is a suspended sandbox every reader already handles, and the operation
//! retries. A crashed attempt leaves `restoring` under a lease that lapses,
//! which the transient-lifecycle sweeper settles as it settles any restore.
//!
//! A stopped sandbox isn't re-placed at all: the drain pins the disk its node
//! published and releases the node ([`evacuate_v2_stopped`]), leaving the row
//! `stopped` and unplaced, and its next start materializes the disk on any
//! node.

use std::sync::LazyLock;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_sandbox_portability::{no_newer_full_snapshot, snapshot_is_current_local_artifact};
use sqlx::PgExecutor;
use uuid::Uuid;

use crate::sweeper::EvacuationRestoreTarget;

/// Why [`claim_drain_restore`] took nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainRestoreRefusal {
    /// The row is gone or being deleted.
    Deleted,
    /// The sandbox left `suspended` from S some other way: a user's call, a
    /// newer snapshot, or the v1 drain's marker. The restore is not needed.
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
    /// The row was `restoring` under the lease, or `started` on the
    /// restore's own execution toward a `paused` target, and moved to the
    /// target.
    Landed,
    /// The row was already in the target on the restore's own execution: the
    /// node's activation receipt
    /// ([`release_completed_activations`](crate::ingest::release_completed_activations))
    /// landed it first.
    AlreadyLanded,
    /// Neither: a revert, a delete, a user's call or another execution has
    /// moved the row on. Nothing was written.
    Lost,
}

impl DrainRestoreCommit {
    /// Whether the restore is in its target.
    pub const fn landed(self) -> bool {
        matches!(self, Self::Landed | Self::AlreadyLanded)
    }
}

/// Land the drain restore whose lease `restore_lease_token` holds in
/// `target`, on its node, and release the activation attempt
/// `activation_attempt_id` the restore ran under on execution `generation`.
/// Writes an **operation step** (the claim's completion).
///
/// Lands a row still `restoring` under that token whose active execution is
/// `generation`; leaving `restoring` clears the lease through the
/// `sandboxes_clear_restore_lease` trigger. A `paused` target wakes its VM
/// `started` and pauses it before this commit, so the node's receipt may
/// have landed the row `started` first: that row, with no v1 marker and
/// `generation` still active and running, lands `paused` too. Reports
/// [`DrainRestoreCommit::AlreadyLanded`] for a row already in `target` with
/// no v1 marker whose active execution is still `generation` and running.
/// Every other row is [`DrainRestoreCommit::Lost`]: a user's restore runs on
/// a new generation, and a user's pause, resume or suspend moves the
/// lifecycle.
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
    target: EvacuationRestoreTarget,
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
                SET lifecycle = $3,
                    placement = 'on_node'
              WHERE s.id = $1
                AND s.evacuation_restore_target IS NULL
                AND (
                  (s.lifecycle = 'restoring' AND s.restore_lease_token = $2)
                  OR (s.lifecycle = 'started' AND $3 = 'paused')
                )
                AND EXISTS (
                  SELECT 1
                    FROM sandbox_k8s_executions e
                   WHERE e.sandbox_id = s.id
                     AND e.active
                     AND e.generation = $4
                     AND (s.lifecycle = 'restoring' OR e.state = 'running')
                )
              RETURNING s.id
         ), already AS (
             SELECT s.id
               FROM sandboxes s
              WHERE s.id = $1
                AND s.lifecycle = $3
                AND s.evacuation_restore_target IS NULL
                AND EXISTS (
                  SELECT 1
                    FROM sandbox_k8s_executions e
                   WHERE e.sandbox_id = s.id
                     AND e.active
                     AND e.generation = $4
                     AND e.state = 'running'
                )
         ), released AS (
             UPDATE sandbox_k8s_executions e
                SET activation_attempt_id = NULL,
                    activation_attempt_expires_at = NULL,
                    activation_attempt_target_state = NULL
              WHERE e.sandbox_id = $1
                AND e.active
                AND e.generation = $4
                AND e.activation_attempt_id = $5
                AND (EXISTS (SELECT 1 FROM landed) OR EXISTS (SELECT 1 FROM already))
              RETURNING e.id
         )
         SELECT EXISTS (SELECT 1 FROM landed),
                EXISTS (SELECT 1 FROM already)
           FROM (SELECT COUNT(*) FROM released) AS released_count"
    ))
    .bind(sandbox_id)
    .bind(restore_lease_token)
    .bind(target.as_str())
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

/// What [`evacuate_v2_stopped`] did with a stopped sandbox a v2 drain's node
/// reported evacuated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum V2StoppedEvacuation {
    /// The row now pins the reported disk and is released from the node:
    /// `stopped`, unplaced, its pre-stop execution retired. Also the answer
    /// for a row this already released.
    Released,
    /// The row isn't the node's any more: gone, started, or stopped
    /// somewhere else. Nothing to do.
    MovedOn,
    /// The row is still the node's, but the report can't be taken: the
    /// reason. Nothing was written.
    Refused(&'static str),
}

/// Release stopped sandbox `sandbox_id` from node `node_id` (Kubernetes name
/// `k8s_node_name`) on its v2 drain's report that the disk `artifact` it
/// published is durable. Writes an **observed projection** (the node's
/// published disk) and an **operation step** (the release), in the caller's
/// transaction.
///
/// Fenced on the row's home and its stop: the row must be `stopped` with
/// `active_node_id = node_id` (a v1 stopped marker an undrain left on it is
/// taken over and cleared); its active execution must be on
/// `k8s_node_name` and predate the stop (`created_at <= lifecycle_changed_at`)
/// with no live activation attempt; and the disk must be a ready, stored
/// `disk` snapshot of `artifact`, taken since the stop
/// (`timestamp >= lifecycle_changed_at`) when the row pins no disk yet. A
/// report from an earlier stop, or replayed after a start, finds another
/// stop or another home and changes nothing. The pin must be unset or
/// already `artifact`: a row pinning another disk is refused, never settled.
/// A row already pinning `artifact` (a stopped re-placement onto the node
/// landed it, after the disk's publication) is released on the disk's
/// durability alone. Then, in one statement each, the
/// row takes the pin and is unplaced, and the pre-stop execution is retired;
/// its next start materializes the disk on any node.
pub async fn evacuate_v2_stopped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    node_id: NodeId,
    k8s_node_name: &str,
    artifact: Uuid,
) -> Result<V2StoppedEvacuation, sqlx::Error> {
    type Row = (
        String,
        Option<NodeId>,
        bool,
        Option<Uuid>,
        Option<String>,
        bool,
        Option<Uuid>,
        bool,
    );
    let row: Option<Row> = sqlx::query_as(sql!(
        "SELECT s.lifecycle,
                s.active_node_id,
                s.evacuation_restore_target IS NOT NULL,
                s.current_local_artifact_id,
                s.current_local_artifact_kind,
                EXISTS (
                  SELECT 1
                    FROM snapshots sn
                   WHERE sn.sandbox_id = s.id
                     AND sn.local_artifact_id = $2
                     AND sn.type = 'disk'
                     AND sn.state = 'ready'
                     -- A row already pinning the disk took it at its stop,
                     -- or at a re-placement onto this node, whose landing
                     -- moved `lifecycle_changed_at` past the disk's
                     -- publication. Only an unpinned row's disk must be
                     -- this stop's, from its time.
                     AND (s.current_local_artifact_id IS NOT NULL
                          OR sn.timestamp >= s.lifecycle_changed_at)
                     AND EXISTS (
                       SELECT 1 FROM snapshot_storage_locations ssl
                        WHERE ssl.snapshot_id = sn.id AND ssl.orphaned_at IS NULL
                     )
                ),
                (
                  SELECT e.id
                    FROM sandbox_k8s_executions e
                   WHERE e.sandbox_id = s.id
                     AND e.active
                     AND e.k8s_node_name = $3
                     AND e.created_at <= s.lifecycle_changed_at
                     AND (
                       e.activation_attempt_id IS NULL
                       OR e.activation_attempt_expires_at <= NOW()
                     )
                   ORDER BY e.generation DESC
                   LIMIT 1
                ),
                EXISTS (
                  SELECT 1
                    FROM sandbox_k8s_executions e
                   WHERE e.sandbox_id = s.id
                     AND e.active
                     AND (e.k8s_node_name IS DISTINCT FROM $3 OR e.created_at > s.lifecycle_changed_at)
                )
           FROM sandboxes s
          WHERE s.id = $1
          FOR UPDATE OF s"
    ))
    .bind(sandbox_id)
    .bind(artifact)
    .bind(k8s_node_name)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((lifecycle, home, marked, pin, pin_kind, disk_durable, source, other_execution)) = row
    else {
        return Ok(V2StoppedEvacuation::MovedOn);
    };
    if lifecycle != "stopped" {
        return Ok(V2StoppedEvacuation::MovedOn);
    }
    let pinned_here = pin == Some(artifact) && pin_kind.as_deref() == Some("disk");
    match home {
        // Released by an earlier tick of this drain.
        None if pinned_here && !marked => return Ok(V2StoppedEvacuation::Released),
        Some(home) if home == node_id => {}
        _ => return Ok(V2StoppedEvacuation::MovedOn),
    }
    // A v1 stopped evacuation's marker on a row still stopped at home is one
    // an undrain abandoned before v1 claimed the row (its claim moves the row
    // to `restoring`): the release below takes it over and clears it.
    if pin.is_some() && !pinned_here {
        return Ok(V2StoppedEvacuation::Refused(
            "the row pins another artifact than the disk the node published",
        ));
    }
    if !disk_durable {
        return Ok(V2StoppedEvacuation::Refused(
            "the disk isn't a ready, stored disk of this stop",
        ));
    }
    let Some(source) = source.filter(|_| !other_execution) else {
        return Ok(V2StoppedEvacuation::Refused(
            "the row's execution isn't the node's pre-stop one, or an activation holds it",
        ));
    };
    // The read above holds the row's lock, so what it decided on still holds.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop',
                active_node_id = NULL,
                target_node_id = NULL,
                placement = 'unplaced',
                command_ready_pending = FALSE,
                placement_changed_at = NULL,
                evacuation_restore_target = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact)
    .execute(&mut **tx)
    .await?;
    crate::dead_node::retire_source_execution(tx, sandbox_id, source).await?;
    Ok(V2StoppedEvacuation::Released)
}

/// Whether row `s` is an unplaced stopped sandbox whose next start
/// materializes its disk on any node: `stopped` with no marker, no placement
/// and no active execution, pinning a `disk` artifact with a ready, stored
/// snapshot. What a v2 drain's release ([`evacuate_v2_stopped`]) or the
/// dead-node verb's leaves. A drain phase on the node it last ran on doesn't
/// hold such a row, as a stored snapshot doesn't hold a suspended one.
pub fn materializable_unplaced_stopped(s: &str) -> String {
    format!(
        "({s}.lifecycle = 'stopped'
          AND {s}.evacuation_restore_target IS NULL
          AND {s}.active_node_id IS NULL
          AND {s}.target_node_id IS NULL
          AND {s}.current_local_artifact_kind = 'disk'
          AND NOT EXISTS (
            SELECT 1 FROM sandbox_k8s_executions e
             WHERE e.sandbox_id = {s}.id AND e.active
          )
          AND EXISTS (
            SELECT 1
              FROM snapshots sn
              JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
             WHERE sn.sandbox_id = {s}.id
               AND sn.local_artifact_id = {s}.current_local_artifact_id
               AND sn.type = 'disk'
               AND sn.state = 'ready'
               AND ssl.orphaned_at IS NULL
          ))"
    )
}

/// Make an unplaced stopped sandbox whose pinned disk is durable a pending
/// stopped re-placement, for the start that materializes it: set the v1
/// drain's `stopped` marker, which the stopped re-placement claims
/// (`begin_evacuation_restore_lifecycle`). Writes an **operation step** (the
/// start's claim on the re-placement). Only a row
/// [`materializable_unplaced_stopped`] admits. Returns whether it was marked.
pub async fn mark_unplaced_stopped_for_materialization(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    static MARK: LazyLock<String> = LazyLock::new(|| {
        let materializable = materializable_unplaced_stopped("s");
        sql!(format!(
            "UPDATE sandboxes s
                SET evacuation_restore_target = 'stopped'
              WHERE s.id = $1
                AND {materializable}"
        ))
    });
    Ok(sqlx::query(sqlx::AssertSqlSafe(MARK.as_str()))
        .bind(sandbox_id)
        .execute(executor)
        .await?
        .rows_affected()
        == 1)
}

/// Undo [`mark_unplaced_stopped_for_materialization`] after a
/// materialization that failed before it claimed the row: back to an
/// unplaced stopped row with no marker, which the next start tries again.
/// A row the re-placement moved on matches nothing.
pub async fn unmark_unplaced_stopped(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = NULL
          WHERE id = $1
            AND lifecycle = 'stopped'
            AND evacuation_restore_target = 'stopped'
            AND active_node_id IS NULL"
    ))
    .bind(sandbox_id)
    .execute(executor)
    .await?
    .rows_affected()
        == 1)
}
