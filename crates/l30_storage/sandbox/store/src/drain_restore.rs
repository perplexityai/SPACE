//! A v2 drain's release of a stopped sandbox.
//!
//! A v2 drain doesn't re-place a stopped sandbox: the drain pins the disk
//! its node published and releases the node ([`evacuate_v2_stopped`]),
//! leaving the row `stopped` and unplaced, and its next start materializes
//! the disk on any node ([`materializable_unplaced_stopped`],
//! [`mark_unplaced_stopped_for_materialization`]).

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use uuid::Uuid;

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
                ),
                s.placement_changed_at IS NULL
           FROM sandboxes s
          WHERE s.id = $1
          FOR UPDATE OF s"
    ))
    .bind(sandbox_id)
    .bind(artifact)
    .bind(k8s_node_name)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((
        lifecycle,
        home,
        marked,
        pin,
        pin_kind,
        disk_durable,
        source,
        other_execution,
        released_clock,
    )) = row
    else {
        return Ok(V2StoppedEvacuation::MovedOn);
    };
    if lifecycle != "stopped" {
        return Ok(V2StoppedEvacuation::MovedOn);
    }
    let pinned_here = pin == Some(artifact) && pin_kind.as_deref() == Some("disk");
    match home {
        // Released by an earlier tick of this drain, whose release left the
        // placement clock unset ([`materializable_unplaced_stopped`] says why
        // that is the release's evidence). A row main's own paths unplaced
        // keeps its clock: it moved on, as it does under a v1 drain.
        None if pinned_here && !marked && released_clock => {
            return Ok(V2StoppedEvacuation::Released);
        }
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
    crate::drain_terminate::retire_source_execution(tx, sandbox_id, source).await?;
    Ok(V2StoppedEvacuation::Released)
}

/// Whether row `s` is an unplaced stopped sandbox whose next start
/// materializes its disk on any node: `stopped` with no marker, no placement
/// and no active execution, pinning a `disk` artifact with a ready, stored
/// snapshot, and released by a v2 drain ([`evacuate_v2_stopped`]). A drain
/// phase on the node it last ran on doesn't hold such a row, as a stored
/// snapshot doesn't hold a suspended one.
///
/// The release's evidence is the placement clock it leaves unset
/// (`placement_changed_at IS NULL`). Main reaches the rest of this shape only
/// through v1's stopped evacuation, the one writer of a stopped row's `disk`
/// pin: its re-placement lands the row through a pod bind, which stamps the
/// clock (`wake::record_pod_node_placement`), and no later writer clears the
/// clock and leaves the row `stopped`. So a row main unplaced afterwards (a
/// pod teardown) keeps a clock, and keeps main's start, pinned to the node it
/// last ran on, and main's drain gate.
pub fn materializable_unplaced_stopped(s: &str) -> String {
    format!(
        "({s}.lifecycle = 'stopped'
          AND {s}.evacuation_restore_target IS NULL
          AND {s}.placement_changed_at IS NULL
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
