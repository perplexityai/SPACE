//! A v2 drain's stopped handoff: how a stopped sandbox on a drained node
//! moves to another node.
//!
//! A v2 drain doesn't move a stopped sandbox itself, and its controller
//! writes no sandbox row while it drains. The node makes the sandbox's disk
//! durable, the internal API projects that disk onto the row as its pin
//! ([`project_stopped_disk_pins`], an observed projection, only while the
//! node's drain is v2), and the sandbox moves on its next start. Once the
//! node is drained, the start hands the row off ([`hand_off_stopped`]): one
//! row-locked transaction pins the durable disk, sets v1's `stopped`
//! evacuation marker, unplaces the row and retires its execution. The start
//! then claims the marked row ([`claim_handed_off_stopped`]) and re-places it
//! through v1's stopped re-placement. The terminate's fence transaction
//! hands off whatever stopped rows remain with the same function.
//!
//! The evidence for a handoff is the v2 drain of the row's home node, not a
//! clock: [`stopped_handoff_evidence`] names a stopped row at home on a node
//! whose v2 drain is `drained` or `terminating` and whose disk is durable
//! since its stop ([`stopped_disk_durable`]). A handed-off row is v1's
//! released stopped evacuation in every column, so v1's restore sweep may
//! re-place it too; the start's claim carries a restore lease the sweep's
//! claim honours (`restore_lease::claim_evacuation_restore`), so one of
//! them drives each re-placement.

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use sqlx::types::{Json, JsonValue};
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
/// `terminating`, and the row `stopped` at home on the node. The disk must
/// be durable since the stop ([`stopped_disk_durable`]): the row's disk pin,
/// or with no pin the newest such disk, taken since the stop either way. Its
/// active executions must be on the node's Kubernetes name, created no later
/// than the stop, with no live activation attempt. Then one statement pins
/// the disk (`disk`, origin `stop`), sets v1's `stopped` marker
/// (`evacuation_restore_target`) and unplaces the row, stamping its placement
/// clock as v1's teardown does, and the execution on the node is retired
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
    type Row = (
        String,
        Option<NodeId>,
        Option<Uuid>,
        Option<String>,
        Option<Uuid>,
        Option<Uuid>,
        bool,
        bool,
    );
    /// `$1` = sandbox id, `$2` = the node's Kubernetes name.
    static READ: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        let disk = this_stops_disk("s", "sn");
        sql!(format!(
            "SELECT s.lifecycle,
                    s.active_node_id,
                    s.current_local_artifact_id,
                    s.current_local_artifact_kind,
                    (
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
                    ),
                    (
                      s.evacuation_restore_target = 'stopped'
                      AND s.active_node_id IS NULL
                      AND s.current_local_artifact_kind = 'disk'
                    )
               FROM sandboxes s
              WHERE s.id = $1
              FOR UPDATE OF s"
        ))
    });
    let row: Option<Row> = sqlx::query_as(sqlx::AssertSqlSafe(READ.as_str()))
        .bind(sandbox_id)
        .bind(node_name.as_deref())
        .fetch_optional(&mut **tx)
        .await?;
    let Some((lifecycle, home, pin, pin_kind, disk, source, execution_moving, handed_off)) = row
    else {
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
    if pin.is_some() && pin_kind.as_deref() != Some("disk") {
        return Ok(StoppedHandoff::Refused(
            "the row pins an artifact that isn't a disk",
        ));
    }
    let Some(disk) = disk else {
        return Ok(StoppedHandoff::Refused(
            "the row has no ready, stored disk of this stop",
        ));
    };
    if execution_moving {
        return Ok(StoppedHandoff::Refused(
            "an execution of the row isn't the node's pre-stop one, or an activation holds it",
        ));
    }
    // The reads above hold the node's and the row's locks, so what they
    // decided on still holds.
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

/// Project the stopped disk pin from node `node_id`'s publication onto its
/// stopped rows, returning the ids written. **Observed projection**: the
/// node reported the disk it holds for a stopped sandbox. The internal API's
/// status ingest calls it only for a node whose drain is v2, with only the
/// rows the node published `stopped`, never a degraded publication's (the
/// ingest keeps those out of its rows); the statement re-checks the v2
/// drain (`drain_engine = 'v2'`), so a publication from any other node
/// writes nothing.
///
/// `rows` is a recordset of `{sandbox_id, published_lifecycle,
/// current_local_artifact_id, current_local_artifact_kind,
/// current_local_artifact_origin, execution_generation, pod_uid}`. Every
/// write is fenced as the handoff is, under the row's lock: the row is still
/// `stopped` at home on the node with no marker; it has an active execution
/// on the node created no later than the stop, and no active execution of it
/// is elsewhere, created since the stop, or held by a live activation (the
/// same stop); and when the publication carries an execution identity, that
/// execution is the one it names, by generation and pod UID
/// (`node_published_execution_identity_matches`, the rule
/// `finalize_suspended` uses). A stopped sandbox's publication carries none:
/// the node clears its route identity at the stop, so the same-stop fences
/// and the disk's freshness bind the publication to this stop. Then:
///
/// * a row that pins nothing takes the published artifact as its pin
///   (`disk`, origin `stop`) when it is this stop's durable disk: a ready,
///   stored `disk` snapshot taken since the stop, with no snapshot of the
///   artifact `deleting`;
/// * a row pinning a `disk` of origin `stop` the node no longer publishes
///   for it has that stale pin cleared.
///
/// A row pinning anything else, a suspend's pin among them, is never
/// written: no pin is replaced by another. A row whose pin already matches
/// what the node publishes is written nothing, so the census republishing
/// every sandbox writes nothing new.
///
/// The pin also protects the disk from snapshot retention, which never
/// prunes a row's pinned artifact.
pub async fn project_stopped_disk_pins(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    rows: Json<JsonValue>,
) -> Result<Vec<SandboxId>, sqlx::Error> {
    /// `$1` = node id, `$2` = the recordset.
    static PROJECTION: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        let disk = this_stops_disk("s", "sn");
        // A NULL pod UID on the execution is accepted as `finalize_suspended`
        // accepts it, for an execution whose UID a repair cleared. A
        // publication with no identity at all is a stopped sandbox's: the
        // node clears its route identity at the stop, and the same-stop
        // fences below bind it to this stop's execution instead.
        let identity = pplx_space_sandbox_portability::node_published_execution_identity_matches(
            "e",
            "input.execution_generation",
            "input.pod_uid",
            "TRUE",
        );
        sql!(format!(
            "WITH input AS (
                 SELECT *
                 FROM jsonb_to_recordset($2::jsonb) AS input(
                     sandbox_id UUID,
                     published_lifecycle TEXT,
                     current_local_artifact_id UUID,
                     current_local_artifact_kind TEXT,
                     current_local_artifact_origin TEXT,
                     execution_generation BIGINT,
                     pod_uid TEXT
                 )
             ),
             node AS (
                 SELECT n.id, nk.k8s_node_name
                   FROM nodes n
                   JOIN node_k8s nk ON nk.node_id = n.id
                  WHERE n.id = $1 AND n.drain_engine = 'v2'
             ),
             candidate AS (
                 SELECT s.id,
                        CASE WHEN s.current_local_artifact_id IS NULL
                             THEN input.current_local_artifact_id END AS pin
                   FROM sandboxes s
                   JOIN input ON input.sandbox_id = s.id
                   JOIN node ON node.id = s.active_node_id
                  WHERE input.published_lifecycle = 'stopped'
                    AND s.lifecycle = 'stopped'
                    AND s.evacuation_restore_target IS NULL
                    AND EXISTS (
                        SELECT 1
                          FROM sandbox_k8s_executions e
                         WHERE e.sandbox_id = s.id
                           AND e.active
                           AND e.k8s_node_name = node.k8s_node_name
                           AND e.created_at <= s.lifecycle_changed_at
                           AND (
                             (input.execution_generation IS NULL AND input.pod_uid IS NULL)
                             OR ({identity})
                           )
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM sandbox_k8s_executions e
                         WHERE e.sandbox_id = s.id
                           AND e.active
                           AND (
                             e.k8s_node_name IS DISTINCT FROM node.k8s_node_name
                             OR e.created_at > s.lifecycle_changed_at
                             OR (
                               e.activation_attempt_id IS NOT NULL
                               AND e.activation_attempt_expires_at > NOW()
                             )
                           )
                    )
                    AND (
                        (
                          s.current_local_artifact_id IS NULL
                          AND input.current_local_artifact_kind = 'disk'
                          AND input.current_local_artifact_origin = 'stop'
                          AND EXISTS (
                              SELECT 1 FROM snapshots sn
                               WHERE sn.local_artifact_id = input.current_local_artifact_id
                                 AND {disk}
                          )
                        )
                        OR (
                          s.current_local_artifact_kind = 'disk'
                          AND s.current_local_artifact_origin = 'stop'
                          AND input.current_local_artifact_id
                              IS DISTINCT FROM s.current_local_artifact_id
                        )
                    )
                  FOR UPDATE OF s
             )
             UPDATE sandboxes s
                SET current_local_artifact_id = c.pin,
                    current_local_artifact_kind = CASE WHEN c.pin IS NULL THEN NULL ELSE 'disk' END,
                    current_local_artifact_origin = CASE WHEN c.pin IS NULL THEN NULL ELSE 'stop' END
               FROM candidate c
              WHERE s.id = c.id
                AND s.current_local_artifact_id IS DISTINCT FROM c.pin
              RETURNING s.id"
        ))
    });
    sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(PROJECTION.as_str()))
        .bind(node_id)
        .bind(rows)
        .fetch_all(executor)
        .await
}

/// The start's claim on a handed-off stopped row, under a restore lease:
/// move it to `restoring` and write the lease `restore_lease_token`, valid
/// for `lease_ttl_secs`. Writes an **operation step** (the claim). Returns
/// the row's lifecycle before the claim, or `None` when it lost.
///
/// Takes a row carrying v1's `stopped` marker that is either `stopped` on
/// no node (unclaimed: handed off, or a v1 teardown's release) or
/// `restoring` under a lapsed lease a start wrote (a start that failed or
/// died after its claim). Never a `restoring` row with no lease, which is
/// v1's restore sweep driving it, nor one whose lease is live. v1's claim
/// (`restore_lease::claim_evacuation_restore`) leaves a live lease alone and
/// clears a lapsed one, so at any time one of them owns the row.
pub async fn claim_handed_off_stopped(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    lease_ttl_secs: i64,
    restore_lease_token: Uuid,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "WITH candidate AS (
             SELECT id, lifecycle
               FROM sandboxes
              WHERE id = $1
                AND evacuation_restore_target = 'stopped'
                AND (
                  (lifecycle = 'stopped' AND active_node_id IS NULL)
                  OR (
                    lifecycle = 'restoring'
                    AND restore_lease_token IS NOT NULL
                    AND restore_lease_expires_at <= NOW()
                  )
                )
              FOR UPDATE
         )
         UPDATE sandboxes s
            SET lifecycle = 'restoring',
                restore_lease_token = $3,
                restore_lease_expires_at = NOW() + make_interval(secs => $2)
           FROM candidate c
          WHERE s.id = c.id
          RETURNING c.lifecycle"
    ))
    .bind(sandbox_id)
    .bind(lease_ttl_secs)
    .bind(restore_lease_token)
    .fetch_optional(executor)
    .await
}

/// Lapse the start's lease `restore_lease_token` on its claim of a
/// handed-off row, keeping the token, so the next start may take the claim
/// over at once. Writes an **operation step** (the claim's release). Only
/// while the row is still `restoring` under that token; returns whether it
/// lapsed.
pub async fn lapse_handed_off_stopped_claim(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    restore_lease_token: Uuid,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = LEAST(restore_lease_expires_at, NOW())
          WHERE id = $1
            AND lifecycle = 'restoring'
            AND evacuation_restore_target = 'stopped'
            AND restore_lease_token = $2"
    ))
    .bind(sandbox_id)
    .bind(restore_lease_token)
    .execute(executor)
    .await?
    .rows_affected()
        == 1)
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
