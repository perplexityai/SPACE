//! A v2 drain's stopped handoff: how a stopped sandbox on a drained node
//! moves to another node.
//!
//! A v2 drain doesn't move a stopped sandbox itself, and its controller
//! writes no sandbox row while it drains. The node makes the sandbox's disk
//! durable, and the sandbox moves on its next start. Once the node is
//! drained, the start hands the row off ([`hand_off_stopped`]): one
//! row-locked transaction pins the durable disk, sets v1's `stopped`
//! evacuation marker, unplaces the row and retires its execution. The start
//! then claims the marked row with v1's own fenced claim
//! (`restore_lease::claim_evacuation_restore`) and re-places it through v1's
//! stopped re-placement. The terminate's fence transaction hands off
//! whatever stopped rows remain with the same function.
//!
//! The evidence for a handoff is the v2 drain of the row's home node, not a
//! clock: [`stopped_handoff_evidence`] names a stopped row at home on a node
//! whose v2 drain is `drained` or `terminating` and whose disk is durable
//! since its stop ([`stopped_disk_durable`]). A handed-off row is v1's
//! released stopped evacuation in every column. v1 and v2 drains never run
//! together: with `SPACE_DRAIN_V2_ENABLED` on, the admin API refuses v1's
//! `:drain` and v1's restore sweep leaves every marked stopped row alone, so
//! each one is a v2 handoff and only its start drives it.

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use uuid::Uuid;

/// Boolean SQL condition over snapshot `sn` of the row aliased `s`: it is
/// this stop's durable disk. A ready `disk` snapshot of the sandbox with a
/// local artifact, taken since the stop (`timestamp >= lifecycle_changed_at`,
/// whether or not the row pins it: a publication carries no capture time and
/// is assembled across calls, so only the catalog's clock places a disk after
/// the stop), with a storage location that isn't orphaned, no snapshot of the
/// same artifact `deleting`, and either the disk the row pins or, for a row
/// that pins nothing, any such disk. A row pinning another disk, or a full
/// snapshot, has no durable disk here: what it pins isn't this stop's disk,
/// and the handoff refuses it rather than choose.
fn this_stops_disk(s: &str, sn: &str) -> String {
    format!(
        "{sn}.sandbox_id = {s}.id
         AND {sn}.type = 'disk'
         AND {sn}.state = 'ready'
         AND {sn}.local_artifact_id IS NOT NULL
         AND {sn}.timestamp >= {s}.lifecycle_changed_at
         AND (
           {s}.current_local_artifact_id IS NULL
           OR (
             {s}.current_local_artifact_kind = 'disk'
             AND {sn}.local_artifact_id = {s}.current_local_artifact_id
           )
         )
         AND EXISTS (
           SELECT 1 FROM snapshot_storage_locations ssl
            WHERE ssl.snapshot_id = {sn}.id AND ssl.orphaned_at IS NULL
         )
         AND NOT EXISTS (
           SELECT 1 FROM snapshots deleting
            WHERE deleting.sandbox_id = {s}.id
              AND deleting.local_artifact_id = {sn}.local_artifact_id
              AND deleting.state = 'deleting'
         )"
    )
}

/// Boolean SQL fragment over the row aliased `s`: its disk is durable since
/// its stop, a snapshot holding this stop's durable disk exists
/// ([`this_stops_disk`]: ready, stored, taken since the stop, not being
/// deleted, and the row's pin if it pins one).
pub fn stopped_disk_durable(s: &str) -> String {
    let disk = this_stops_disk(s, "sn");
    format!("EXISTS (SELECT 1 FROM snapshots sn WHERE {disk})")
}

/// Boolean SQL fragment over the row aliased `s`: artifact `artifact` (an SQL
/// expression) is this stop's durable disk ([`this_stops_disk`]: a ready,
/// stored `disk` snapshot of it taken since the stop, not being deleted, and
/// the row's pin if it pins one). It needs no pin: the v2 drain's sink
/// records a stopped sandbox's evacuation once the disk its node reports
/// passes it, and the handoff pins that disk later.
pub fn stopped_disk_durable_artifact(s: &str, artifact: &str) -> String {
    let disk = this_stops_disk(s, "sn");
    format!(
        "EXISTS (SELECT 1 FROM snapshots sn WHERE sn.local_artifact_id = {artifact} AND {disk})"
    )
}

/// Boolean SQL fragment over the row aliased `s`: a start may hand it off.
/// It is `stopped` at home on a node (`active_node_id`), carries no
/// evacuation marker but v1's `stopped` one, which the handoff takes over
/// ([`hand_off_stopped`]), its home node's drain is v2 and `drained` or
/// `terminating`, and its disk is durable since its stop
/// ([`stopped_disk_durable`]). False for every row of a cluster with no v2
/// drain.
pub fn stopped_handoff_evidence(s: &str) -> String {
    let durable = stopped_disk_durable(s);
    format!(
        "({s}.lifecycle = 'stopped'
          AND {s}.evacuation_restore_target IS DISTINCT FROM 'started'
          AND {s}.evacuation_restore_target IS DISTINCT FROM 'paused'
          AND {s}.active_node_id IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM nodes hn
             WHERE hn.id = {s}.active_node_id
               AND hn.drain_engine = 'v2'
               AND hn.drain_phase IN ('drained', 'terminating')
          )
          AND {durable})"
    )
}

/// Boolean SQL fragment over the row aliased `s`: a handed-off stopped row
/// whose re-placement hasn't landed. It carries v1's `stopped` marker and is
/// `stopped` on no node, or `restoring`: a re-placement claimed it. v1's
/// released stopped evacuation is the same shape.
pub fn stopped_handoff_pending(s: &str) -> String {
    format!(
        "({s}.evacuation_restore_target = 'stopped'
          AND (
            ({s}.lifecycle = 'stopped' AND {s}.active_node_id IS NULL)
            OR {s}.lifecycle = 'restoring'
          ))"
    )
}

/// What [`hand_off_stopped`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoppedHandoff {
    /// The row pins the disk and is handed off: `stopped`, unplaced,
    /// marked for v1's stopped re-placement, its execution on the node
    /// retired. Also the answer for a row already handed off from the node,
    /// which this writes nothing to.
    HandedOff(Uuid),
    /// The row isn't stopped at home on the node any more: gone, started,
    /// placed elsewhere, or re-placed. Nothing to do.
    MovedOn,
    /// The row is still the node's, but can't be handed off: the reason.
    /// Nothing was written.
    Refused(&'static str),
}

/// Hand stopped sandbox `sandbox_id` off from its home node `node_id`, in
/// the caller's transaction: the stopped handoff, which the API's start and
/// the terminate's fence transaction both call. Writes an **operation step**
/// (the handoff).
///
/// It takes the node's row lock `FOR SHARE` first, the order the fence
/// transaction locks in, so an undrain waits for it; then the sandbox's
/// `FOR UPDATE`. Under both, the node's drain must be v2 and `drained` or
/// `terminating`, and the row `stopped` at home on the node, carrying no
/// evacuation marker but v1's `stopped` one. The disk must be durable since
/// the stop ([`stopped_disk_durable`]): the row's disk pin, or with no pin
/// the newest such disk, taken since the stop either way. Its active
/// executions must be on the node's Kubernetes name, created no later than
/// the stop, with no live activation attempt. The disk and the executions
/// are read in a statement after the row lock's, never in the statement
/// that takes it: retention claims a disk for deletion under the same row
/// lock, and a statement that waited for the lock reads other tables as of
/// before the wait. Then one statement pins the disk (`disk`, origin
/// `stop`), sets v1's `stopped` marker (`evacuation_restore_target`) and
/// unplaces the row, stamping its placement clock as v1's teardown does, and
/// the execution on the node is retired
/// (`drain_terminate::retire_source_execution`).
///
/// A v1 `stopped` marker on a row still at home is one an undrain abandoned
/// before v1's teardown released the row: the handoff takes it over.
pub async fn hand_off_stopped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> Result<StoppedHandoff, sqlx::Error> {
    let node: Option<(Option<String>, Option<String>, Option<String>)> = sqlx::query_as(sql!(
        "SELECT n.drain_engine, n.drain_phase,
                (SELECT nk.k8s_node_name FROM node_k8s nk WHERE nk.node_id = n.id)
           FROM nodes n
          WHERE n.id = $1
          FOR SHARE OF n"
    ))
    .bind(node_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((engine, phase, node_name)) = node else {
        return Ok(StoppedHandoff::Refused("the node's row is gone"));
    };
    if engine.as_deref() != Some("v2")
        || !matches!(phase.as_deref(), Some("drained" | "terminating"))
    {
        return Ok(StoppedHandoff::Refused(
            "the node's drain isn't a drained or terminating v2 drain",
        ));
    }
    // The row lock first, in a statement of its own. Under READ COMMITTED a
    // statement that waits for a row lock refreshes only the locked row, so
    // what it read of `snapshots` and executions would be as of before the
    // wait. Retention claims a disk for deletion under this same lock (it
    // takes the sandbox's `FOR UPDATE` before it marks a snapshot
    // `deleting`): read under one statement, a claim that committed while
    // this waited would go unseen and the handoff would pin a doomed disk.
    type Locked = (
        String,
        Option<NodeId>,
        Option<Uuid>,
        Option<String>,
        Option<String>,
        bool,
    );
    let locked: Option<Locked> = sqlx::query_as(sql!(
        "SELECT s.lifecycle,
                s.active_node_id,
                s.current_local_artifact_id,
                s.current_local_artifact_kind,
                s.evacuation_restore_target,
                COALESCE(
                  s.evacuation_restore_target = 'stopped'
                  AND s.active_node_id IS NULL
                  AND s.current_local_artifact_kind = 'disk',
                  FALSE
                )
           FROM sandboxes s
          WHERE s.id = $1
          FOR UPDATE OF s"
    ))
    .bind(sandbox_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((lifecycle, home, pin, pin_kind, marker, handed_off)) = locked else {
        return Ok(StoppedHandoff::MovedOn);
    };
    if lifecycle != "stopped" {
        return Ok(StoppedHandoff::MovedOn);
    }
    if home != Some(node_id) {
        // Handed off already, by an earlier call or the fence; with no home
        // it can't be told from another node's handoff, and nothing is
        // written either way.
        return Ok(match (handed_off, pin) {
            (true, Some(pin)) if home.is_none() => StoppedHandoff::HandedOff(pin),
            _ => StoppedHandoff::MovedOn,
        });
    }
    if marker.as_deref().is_some_and(|marker| marker != "stopped") {
        // Main's trigger clears the marker as the row enters `stopped`, so
        // this shouldn't be reachable; the handoff never overwrites another
        // evacuation's marker.
        return Ok(StoppedHandoff::Refused(
            "the row carries another evacuation's marker",
        ));
    }
    if pin.is_some() && pin_kind.as_deref() != Some("disk") {
        return Ok(StoppedHandoff::Refused(
            "the row pins an artifact that isn't a disk",
        ));
    }
    /// `$1` = sandbox id, `$2` = the node's Kubernetes name. Read after the
    /// row lock, so it sees every retention claim committed before it, and
    /// none can commit until this transaction ends.
    static READ: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        let disk = this_stops_disk("s", "sn");
        sql!(format!(
            "SELECT (
                      SELECT sn.local_artifact_id
                        FROM snapshots sn
                       WHERE {disk}
                       ORDER BY sn.timestamp DESC, sn.id DESC
                       LIMIT 1
                    ),
                    (
                      SELECT e.id
                        FROM sandbox_k8s_executions e
                       WHERE e.sandbox_id = s.id
                         AND e.active
                         AND e.k8s_node_name = $2
                       ORDER BY e.generation DESC
                       LIMIT 1
                    ),
                    EXISTS (
                      SELECT 1
                        FROM sandbox_k8s_executions e
                       WHERE e.sandbox_id = s.id
                         AND e.active
                         AND (
                           e.k8s_node_name IS DISTINCT FROM $2
                           OR e.created_at > s.lifecycle_changed_at
                           OR (
                             e.activation_attempt_id IS NOT NULL
                             AND e.activation_attempt_expires_at > NOW()
                           )
                         )
                    )
               FROM sandboxes s
              WHERE s.id = $1"
        ))
    });
    let (disk, source, execution_moving): (Option<Uuid>, Option<Uuid>, bool) =
        sqlx::query_as(sqlx::AssertSqlSafe(READ.as_str()))
            .bind(sandbox_id)
            .bind(node_name.as_deref())
            .fetch_one(&mut **tx)
            .await?;
    let Some(disk) = disk else {
        return Ok(StoppedHandoff::Refused(
            "the row has no ready, stored disk of this stop yet",
        ));
    };
    if execution_moving {
        return Ok(StoppedHandoff::Refused(
            "an execution of the row isn't the node's pre-stop one, or an activation holds it",
        ));
    }
    // The reads above hold the node's and the row's locks, and the disk was
    // read after both, so what they decided on still holds.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop',
                evacuation_restore_target = 'stopped',
                active_node_id = NULL,
                target_node_id = NULL,
                placement = 'unplaced',
                command_ready_pending = FALSE,
                placement_changed_at = NOW()
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(disk)
    .execute(&mut **tx)
    .await?;
    if let Some(source) = source {
        crate::drain_terminate::retire_source_execution(tx, sandbox_id, source).await?;
    }
    Ok(StoppedHandoff::HandedOff(disk))
}

/// What a start of a stopped or restoring row reads to decide on the
/// handoff, in one statement ([`stopped_handoff_state`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoppedHandoffState {
    /// The row's home node, when it is stopped at home and the evidence for
    /// a handoff holds ([`stopped_handoff_evidence`]).
    pub hand_off_from: Option<NodeId>,
    /// The row is a handed-off row whose re-placement hasn't landed
    /// ([`stopped_handoff_pending`]): the start resumes it.
    pub pending: bool,
}

/// Read the handoff state of sandbox `sandbox_id`; `None` when the row is
/// gone. A read: it takes no lock. The handoff and the claim re-check
/// everything under their locks.
pub async fn stopped_handoff_state(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<StoppedHandoffState>, sqlx::Error> {
    static STATE: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        let evidence = stopped_handoff_evidence("s");
        let pending = stopped_handoff_pending("s");
        sql!(format!(
            "SELECT CASE WHEN {evidence} THEN s.active_node_id END,
                    COALESCE({pending}, FALSE)
               FROM sandboxes s
              WHERE s.id = $1"
        ))
    });
    let row: Option<(Option<NodeId>, bool)> = sqlx::query_as(sqlx::AssertSqlSafe(STATE.as_str()))
        .bind(sandbox_id)
        .fetch_optional(executor)
        .await?;
    Ok(row.map(|(hand_off_from, pending)| StoppedHandoffState {
        hand_off_from,
        pending,
    }))
}

/// Land a start's re-placement of handed-off stopped sandbox `sandbox_id`,
/// in the caller's transaction: `stopped`, `on_node`, the marker cleared, as
/// v1's [`crate::drain::finalize_evacuation_restore`] does toward `stopped`,
/// but only for the claim that drove it. Returns whether the row moved.
/// Writes an **operation step** (the re-placement's end).
///
/// v1's claim writes no lease and takes a claimed row again, so two starts of
/// the same row can both hold it. Each claim drives its own execution, and
/// `execution_generation` names this one's: the row lands only while that
/// execution is still the row's active one, so an earlier start can't land
/// a row a later claim is re-placing. It locks the sandbox row, then that
/// execution `FOR SHARE`, the order execution mutations lock in: a later
/// claim's drive that retires it commits first, or waits for this to end.
/// The row must still be `restoring` with v1's `stopped` marker.
///
/// v1's controller keeps its own finalize, unchanged: with the switch on it
/// drives no marked stopped row, and with it off a start drives none.
pub async fn finalize_stopped_handoff_restore(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    execution_generation: i64,
) -> Result<bool, sqlx::Error> {
    let row: Option<Uuid> =
        sqlx::query_scalar(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
            .bind(sandbox_id)
            .fetch_optional(&mut **tx)
            .await?;
    if row.is_none() {
        return Ok(false);
    }
    let claimed: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
            AND generation = $2
            AND active
          FOR SHARE"
    ))
    .bind(sandbox_id)
    .bind(execution_generation)
    .fetch_optional(&mut **tx)
    .await?;
    if claimed.is_none() {
        return Ok(false);
    }
    let finalized = sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'stopped',
                placement = 'on_node',
                evacuation_restore_target = NULL
          WHERE id = $1
            AND lifecycle = 'restoring'
            AND evacuation_restore_target = 'stopped'"
    ))
    .bind(sandbox_id)
    .execute(&mut **tx)
    .await?;
    Ok(finalized.rows_affected() == 1)
}
