//! Reads that resolve what a sandbox restores from.
//!
//! [`select_restore_source`] is the one rule every substrate restores a
//! suspended sandbox's memory state by: the Kubernetes suspend selector, the
//! Direct substrate's remote candidate and the node-manager's
//! `GetSuspendSnapshotLocation` fallback all call it, and supply only where
//! the sandbox may be placed.
//!
//! The other two reads resolve what the Kubernetes substrate restores an
//! unplaced or evacuated sandbox from, by the node-drain
//! `evacuation_restore_target` marker: the unplaced restore plan of a `stopped`
//! row, and the disk artifact a stopped evacuation is re-placed from.
//!
//! The Kubernetes store keeps the rest of restore source selection: the
//! stopped storage-URI selector, the node-name fallback, the missing-mapping
//! warning and the validation of a selected storage URI. Every read here is a
//! plain read: it takes no lock, and runs on the executor the caller passes.

use pplx_space_accounts::NodeId;
use std::sync::LazyLock;

use pplx_space_common_core::types::{
    ArtifactOrigin, LocalArtifactId, SandboxId, SandboxLifecycle, SnapshotId,
};

use pplx_space_common_core::sql;
use pplx_space_sandbox_portability::{
    capture_rank, capture_rank_in_retry_window, capture_registered_at,
    full_snapshot_outranked_by_generation, no_newer_suspend_capture,
    registered_before_restore_claim, restore_order_unordered,
    restore_order_unordered_in_retry_window, restore_retry_source_snapshot_is_current,
    snapshot_is_current_local_artifact,
};
use sqlx::PgExecutor;

/// How [`select_restore_source`] chooses: one of the four ways a caller
/// restores. A pinned row restores its pinned artifact under each, except as
/// [`Self::Retry`] says. An unpinned row restores only from suspend captures
/// under each: a named snapshot is taken while the sandbox runs, so one
/// registered after a suspend capture of the same execution is older state,
/// and a stop's backup holds no memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestoreSourceMode {
    /// A user restore's first attempt on the sandbox's own node. An unpinned
    /// row takes its latest capture once that capture registered at or after
    /// the last placement and no full snapshot in any state outranks it by
    /// capture generation, so a `wake → modify → re-suspend` cycle can't
    /// restore the previous cycle's bytes while a later execution's upload is
    /// in flight or failed. A later capture of the same execution that is
    /// still uploading doesn't hold it.
    PlacementFresh,
    /// A first attempt away from the sandbox's node: an evacuation, or a
    /// Direct restore on another node, and the node-drain cohort restore on
    /// every attempt. An unpinned row takes only the newest full snapshot,
    /// once it is ready ([`no_newer_suspend_capture`]), so a stale prior-cycle
    /// snapshot can't be read while the current suspend's upload is in flight
    /// or failed.
    NewestFull,
    /// A retry of a restore the caller already claimed: the newest capture in
    /// the row's pre-claim source window
    /// ([`restore_retry_source_snapshot_is_current`]), or the pinned artifact
    /// when the window holds it. A failed restore branch can publish a later
    /// artifact, and the retry must load the state the restore started from.
    Retry,
    /// A node-manager asking where to restore from, which it does on behalf
    /// of a restore the control plane already started: a claimed restore, or
    /// the recovery of a `started` sandbox whose runtime was lost. A
    /// `restoring` row selects as [`Self::Retry`] does, also accepting any
    /// backup of its source's artifact, the same bytes. A row in any other
    /// lifecycle selects as [`Self::PlacementFresh`] does without the
    /// placement comparison (the restore placed the sandbox before the node
    /// asks, so every capture predates that placement). The row needn't be
    /// `suspended` or `restoring`.
    ///
    /// A claimed restore selects from its source window on every attempt, as
    /// the Kubernetes substrate does once it re-reads the claimed row, and
    /// that window can hold no current source where the pre-claim
    /// placement-fresh selection found one: a newer window row that isn't
    /// ready and records no capture generation (other than a failed retry of
    /// the same artifact) blocks every older row in the window, while the
    /// placement-fresh rule skips it.
    NodeLookup,
}

impl RestoreSourceMode {
    /// [`Self::Retry`] for a caller retrying a claimed restore, else
    /// `first_attempt`.
    pub const fn or_retry(first_attempt: Self, retry: bool) -> Self {
        if retry { Self::Retry } else { first_attempt }
    }
}

/// Where the restoring caller requires the sandbox to be placed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestoreSourcePlacement<'a> {
    /// Anywhere: the control plane is choosing.
    Any,
    /// Active on, or the target of a pending migration or restore to, one of
    /// these nodes: a node asking about its own sandboxes. A sandbox placed
    /// elsewhere selects nothing.
    PlacedOn(&'a [NodeId]),
}

/// The snapshot [`select_restore_source`] chose, and the storage location to
/// load it from. The connection string is unvalidated.
#[derive(Clone, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct SelectedRestoreSource {
    pub snapshot_id: SnapshotId,
    pub storage_connection_string: String,
    pub size_bytes: Option<i64>,
}

/// What [`select_restore_source`] found.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RestoreSourceSelection {
    /// The capture to restore.
    Selected(SelectedRestoreSource),
    /// Nothing qualifies yet: no ready capture, the latest capture's upload
    /// is in flight or failed, or the row isn't restorable by the rule.
    Unavailable,
    /// The row would restore by capture order, and its history is unordered
    /// ([`restore_order_unordered`]): a capture without a generation registered
    /// after one with a generation, with no later execution's capture after
    /// it. Either could be the latest, so the restore refuses rather than risk
    /// loading the older one. Terminal until an operator pins a capture
    /// (`restore_pin`), the restore-pin repair pins the node's, or a later
    /// execution's capture registers.
    Unordered,
}

/// The operator-facing reason for [`RestoreSourceSelection::Unordered`].
pub fn unordered_restore_source_message(sandbox_id: SandboxId) -> String {
    format!(
        "refusing to restore sandbox {sandbox_id}: its captures can't be ordered (a capture with \
         no recorded capture generation registered after one with a generation), so the control \
         plane won't guess which is the latest; an operator can pin one through the admin \
         restore-pin route"
    )
}

impl RestoreSourceSelection {
    /// The selected capture, if any.
    pub fn selected(self) -> Option<SelectedRestoreSource> {
        match self {
            Self::Selected(selected) => Some(selected),
            Self::Unavailable | Self::Unordered => None,
        }
    }
}

/// The selection query's one row: the pick, if any, and whether the row's
/// unpinned history is unordered.
#[derive(sqlx::FromRow)]
struct SelectionRow {
    snapshot_id: Option<SnapshotId>,
    storage_connection_string: Option<String>,
    size_bytes: Option<i64>,
    pinned: Option<bool>,
    unordered: Option<bool>,
}

/// One mode's selection statements: anywhere, and placed on `$2`.
struct ModeStatements {
    any: String,
    placed_on: String,
}

impl ModeStatements {
    fn build(mode: RestoreSourceMode) -> Self {
        Self {
            any: selection_statement(mode, false),
            placed_on: selection_statement(mode, true),
        }
    }

    fn get(&self, placed_on: bool) -> &str {
        if placed_on {
            &self.placed_on
        } else {
            &self.any
        }
    }
}

/// Every mode's statements, built once: every input is a literal or a bind
/// placeholder.
struct SelectionStatements {
    placement_fresh: ModeStatements,
    newest_full: ModeStatements,
    retry: ModeStatements,
    node_lookup: ModeStatements,
}

static SELECTION_STATEMENTS: LazyLock<SelectionStatements> =
    LazyLock::new(|| SelectionStatements {
        placement_fresh: ModeStatements::build(RestoreSourceMode::PlacementFresh),
        newest_full: ModeStatements::build(RestoreSourceMode::NewestFull),
        retry: ModeStatements::build(RestoreSourceMode::Retry),
        node_lookup: ModeStatements::build(RestoreSourceMode::NodeLookup),
    });

/// The statement [`select_restore_source`] runs for `mode`.
fn selection_query(mode: RestoreSourceMode, placed_on: bool) -> &'static str {
    let statements = &*SELECTION_STATEMENTS;
    match mode {
        RestoreSourceMode::PlacementFresh => &statements.placement_fresh,
        RestoreSourceMode::NewestFull => &statements.newest_full,
        RestoreSourceMode::Retry => &statements.retry,
        RestoreSourceMode::NodeLookup => &statements.node_lookup,
    }
    .get(placed_on)
}

/// The selection statement for `mode`, restricted to sandboxes placed on
/// `$2` when `placed_on`.
fn selection_statement(mode: RestoreSourceMode, placed_on: bool) -> String {
    let pinned = snapshot_is_current_local_artifact("s", "sn");
    // A retry ranks its candidates as its pre-claim window sees them.
    let rank = match mode {
        RestoreSourceMode::PlacementFresh | RestoreSourceMode::NewestFull => capture_rank("sn"),
        RestoreSourceMode::Retry => capture_rank_in_retry_window("s", "sn"),
        RestoreSourceMode::NodeLookup => format!(
            "CASE WHEN s.lifecycle = '{}' THEN {} ELSE {} END",
            SandboxLifecycle::Restoring.as_str(),
            capture_rank_in_retry_window("s", "sn"),
            capture_rank("sn")
        ),
    };
    let captured = capture_registered_at("sn");
    let unordered = restore_order_unordered("s");
    // A retry's source is ambiguous only by rows in its pre-claim window.
    let unordered_in_window = restore_order_unordered_in_retry_window("s");
    // Placement freshness is a property of the capture restore loads, so it's
    // checked after the generation comparison, not as a candidate filter, and
    // against the capture's first registration: a re-upload of an older
    // capture registered after the placement isn't fresh.
    let outranked = full_snapshot_outranked_by_generation("s", "sn");
    let suspend = ArtifactOrigin::Suspend.as_str();
    let placement_fresh = format!(
        "({pinned}\n\
          OR ((s.placement_changed_at IS NULL\n\
               OR {captured} >= s.placement_changed_at)\n\
              AND NOT {outranked}))"
    );
    let latest_current = format!("({pinned} OR NOT {outranked})");
    // What an unpinned row may restore from, before the latest capture wins:
    // only a suspend capture. A named snapshot is taken while the sandbox
    // runs, so one registered after a suspend capture of the same execution
    // is older state; a stop's backup holds no memory.
    let suspend_capture =
        format!("(sn.artifact_origin IS NULL OR sn.artifact_origin = '{suspend}')");
    let unpinned = match mode {
        RestoreSourceMode::PlacementFresh
        | RestoreSourceMode::Retry
        | RestoreSourceMode::NodeLookup => suspend_capture,
        RestoreSourceMode::NewestFull => {
            format!(
                "({suspend_capture} AND {})",
                no_newer_suspend_capture("s", "sn")
            )
        }
    };
    let current = format!(
        "({pinned}\n\
          OR (s.current_local_artifact_id IS NULL AND {unpinned}))"
    );
    let restoring = SandboxLifecycle::Restoring.as_str();
    let suspended = SandboxLifecycle::Suspended.as_str();
    let restorable = format!("s.lifecycle IN ('{suspended}', '{restoring}')");
    // Rows that restore by capture order: an unpinned row, or a retry,
    // which ignores the pin unless the window holds the pinned artifact.
    // A pin with a backup registered before the claim is the state the
    // restore started from, so a retry restores only that artifact, or
    // nothing. Only a pin whose artifact the failed branch published after
    // the claim gives way to the window's rule.
    let pin_pre_claim = format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots claimed_pin\n\
              WHERE claimed_pin.sandbox_id = s.id\n\
                AND claimed_pin.type = 'full'\n\
                AND {}\n\
                AND {}\n\
         )",
        snapshot_is_current_local_artifact("s", "claimed_pin"),
        registered_before_restore_claim("s", "claimed_pin")
    );
    let first_attempt_by_order =
        format!("{restorable} AND s.current_local_artifact_id IS NULL AND {unordered}");
    let by_order = match mode {
        RestoreSourceMode::PlacementFresh | RestoreSourceMode::NewestFull => first_attempt_by_order,
        RestoreSourceMode::Retry => {
            format!("{restorable} AND NOT {pin_pre_claim} AND {unordered_in_window}")
        }
        RestoreSourceMode::NodeLookup => format!(
            "((s.lifecycle = '{restoring}' AND NOT {pin_pre_claim} AND {unordered_in_window})\n\
              OR (s.lifecycle <> '{restoring}'\n\
                  AND s.current_local_artifact_id IS NULL\n\
                  AND {unordered}))"
        ),
    };
    // A retry loads the pinned artifact when its window holds it: the state
    // the first attempt loaded, whatever ranks above it in the window.
    let window = registered_before_restore_claim("s", "sn");
    let pinned_in_window = format!("({pinned} AND {window})");
    let source = match mode {
        RestoreSourceMode::PlacementFresh | RestoreSourceMode::NewestFull => {
            format!("{restorable} AND {current}")
        }
        RestoreSourceMode::Retry => {
            let retry = restore_retry_source_snapshot_is_current("s", "sn");
            format!(
                "{restorable}\n\
                 AND CASE WHEN {pin_pre_claim} THEN {pinned_in_window}\n\
                          ELSE ({retry} OR {pinned_in_window}) END"
            )
        }
        RestoreSourceMode::NodeLookup => {
            let retry = restore_retry_source_snapshot_is_current("s", "sn");
            // A node asking during the first attempt may find the claim's
            // source re-uploaded since the claim: the same bytes, outside the
            // window.
            // The claim's source, by the window's rule or its pin override.
            let source_reupload = format!(
                "({} OR ({} AND {}))",
                restore_retry_source_snapshot_is_current("s", "source"),
                snapshot_is_current_local_artifact("s", "source"),
                registered_before_restore_claim("s", "source")
            );
            format!(
                "((s.lifecycle = '{restoring}'\n\
                   AND {pin_pre_claim}\n\
                   AND {pinned})\n\
                  OR (s.lifecycle = '{restoring}'\n\
                   AND NOT {pin_pre_claim}\n\
                   AND ({retry}\n\
                        OR {pinned_in_window}\n\
                        OR (sn.local_artifact_id IS NOT NULL\n\
                            AND EXISTS (\n\
                                SELECT 1\n\
                                  FROM snapshots source\n\
                                 WHERE source.sandbox_id = s.id\n\
                                   AND source.type = 'full'\n\
                                   AND source.local_artifact_id = sn.local_artifact_id\n\
                                   AND source.artifact_kind = sn.artifact_kind\n\
                                   AND {source_reupload}\n\
                            ))))\n\
                  OR (s.lifecycle <> '{restoring}' AND {current}))"
            )
        }
    };
    let fresh = match mode {
        RestoreSourceMode::PlacementFresh => placement_fresh,
        RestoreSourceMode::NewestFull | RestoreSourceMode::Retry => "TRUE".to_owned(),
        // The node asks on behalf of a restore the control plane already
        // started, after the placement it made for it: a capture still has
        // to be the latest, but not newer than that placement.
        RestoreSourceMode::NodeLookup => {
            format!("(s.lifecycle = '{restoring}' OR {latest_current})")
        }
    };
    let placed = if placed_on {
        "(s.active_node_id = ANY($2) OR s.target_node_id = ANY($2))"
    } else {
        "TRUE"
    };
    sql!(format!(
        "WITH candidates AS (
             SELECT sn.id AS snapshot_id,
                    ssl.storage_connection_string,
                    sn.size_bytes,
                    {rank} AS rank,
                    {captured} AS captured_at,
                    {pinned} AS pinned,
                    sn.timestamp,
                    ssl.created_at AS location_created_at,
                    ssl.id AS location_id,
                    {fresh} AS fresh
               FROM sandboxes s
               JOIN snapshots sn ON sn.sandbox_id = s.id
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE s.id = $1
                AND {placed}
                AND ssl.orphaned_at IS NULL
                AND sn.type = 'full'
                AND sn.state = 'ready'
                AND {source}
         ),
         pick AS (
             SELECT c.snapshot_id, c.storage_connection_string, c.size_bytes, c.pinned
               FROM candidates c
              WHERE (c.pinned
                     OR NOT EXISTS (
                         SELECT 1
                           FROM candidates later
                          WHERE later.rank > c.rank
                     ))
                AND c.fresh
              ORDER BY c.pinned DESC, c.captured_at DESC, c.timestamp DESC, c.snapshot_id DESC,
                       c.location_created_at DESC, c.location_id DESC
              LIMIT 1
         )
         SELECT pick.snapshot_id, pick.storage_connection_string, pick.size_bytes,
                pick.pinned,
                (SELECT {by_order}
                   FROM sandboxes s
                  WHERE s.id = $1 AND {placed}) AS unordered
           FROM (SELECT 1) AS one
           LEFT JOIN pick ON TRUE"
    ))
}

/// Select the capture a sandbox restores from, and its storage
/// location. The row must be `suspended` or `restoring`, except under
/// [`RestoreSourceMode::NodeLookup`].
///
/// A candidate is a `ready` full snapshot with a storage location that isn't
/// orphaned. Of those:
///
/// 1. A retry ([`RestoreSourceMode::Retry`]) takes the newest in its restore's
///    pre-claim source window, or the pinned artifact when the window holds
///    it: the state its first attempt loaded. A node's lookup during a claimed restore also
///    accepts any backup of that source's artifact, the same bytes.
/// 2. Otherwise a pinned row takes only snapshots of its pinned artifact
///    ([`snapshot_is_current_local_artifact`]). A re-upload of an older
///    capture, or a backup of a full named snapshot, carries another artifact
///    id and can't match.
/// 3. Otherwise an unpinned row applies its mode's rule.
///
/// Then the latest capture wins: a candidate another candidate outranks by
/// capture rank ([`capture_rank`]) is dropped, and the rest are ordered by
/// their captures' first registrations ([`capture_registered_at`]), newest
/// first, so a re-upload of a capture's artifact doesn't make it newer. An
/// unpinned placement-fresh rule also drops a candidate any full snapshot,
/// in any state, outranks, so a later execution's capture holds the restore
/// while its upload is in flight, and for good once it's failed (a later
/// capture of the same execution doesn't, since it doesn't outrank), and,
/// outside a node's lookup, one registered before the last placement. A history where no capture has a generation keeps
/// registration order.
/// The retry and newest-full guards rank the same way; in the retry window
/// only rows inside the window can outrank one. See "Capture generation" in
/// `pplx_space_sandbox_portability`'s docs.
///
/// When the pick isn't the pinned artifact and the row's history is
/// unordered ([`restore_order_unordered`]), the result is
/// [`RestoreSourceSelection::Unordered`], whatever the pick: capture order
/// can't choose there.
///
/// Backup attempts of one artifact are the same bytes; the `id` tiebreaks
/// make the choice among them, and among one snapshot's locations,
/// deterministic.
pub async fn select_restore_source(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    mode: RestoreSourceMode,
    placement: RestoreSourcePlacement<'_>,
) -> Result<RestoreSourceSelection, sqlx::Error> {
    let placed_on = matches!(placement, RestoreSourcePlacement::PlacedOn(_));
    let query = selection_query(mode, placed_on);
    let select = sqlx::query_as::<_, SelectionRow>(sqlx::AssertSqlSafe(query)).bind(sandbox_id);
    let select = match placement {
        RestoreSourcePlacement::Any => select,
        RestoreSourcePlacement::PlacedOn(node_ids) => select.bind(node_ids),
    };
    let row = select.fetch_one(executor).await?;
    let pick = match (row.snapshot_id, row.storage_connection_string) {
        (Some(snapshot_id), Some(storage_connection_string)) => Some(SelectedRestoreSource {
            snapshot_id,
            storage_connection_string,
            size_bytes: row.size_bytes,
        }),
        _ => None,
    };
    Ok(match pick {
        Some(pick) if row.pinned == Some(true) => RestoreSourceSelection::Selected(pick),
        _ if row.unordered == Some(true) => RestoreSourceSelection::Unordered,
        Some(pick) => RestoreSourceSelection::Selected(pick),
        None => RestoreSourceSelection::Unavailable,
    })
}

/// What [`restore_source_plan`] reads of one row: the two node-name
/// candidates, kept apart rather than `COALESCE`d, the placement's node id,
/// and whether a stopped evacuation has left the row unplaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreSourcePlanRow {
    /// The Kubernetes node mapped to `COALESCE(target_node_id,
    /// active_node_id)`.
    pub mapped_name: Option<String>,
    /// The node the newest execution that named one ran on.
    pub execution_name: Option<String>,
    /// `COALESCE(target_node_id, active_node_id)`.
    pub placement_node_id: Option<NodeId>,
    /// The marker is `'stopped'` and `active_node_id` is NULL.
    pub stopped_evacuation_unplaced: bool,
}

/// Read the restore source of a `stopped` sandbox's row, for a restart that
/// may have to pin its pod to a node. `None` when no row exists.
///
/// Only `'stopped'` counts as the marker. The other two values belong to the
/// suspend → restore pipeline, which parks its rows in `suspended` and
/// resolves them through the storage-URI path; reading them here would refuse
/// restores that path already handles.
///
/// When it holds, the drain has released the node holding the copy, so the
/// node name beside it is not a restore source at all.
///
/// **`active_node_id IS NULL` is the load-bearing half, not a belt-and-braces
/// extra.** The marker alone says an evacuation was *claimed*, which is not the
/// same as finished, and a claim can outlive the drain that made it: an
/// operator's `:undrain` between the claim and the teardown leaves the row
/// `stopped`, marked, and still holding its rootfs — and **nothing clears that
/// marker**. Every writer that clears it requires a lifecycle change or a
/// lifecycle this row does not have (`suspending`, `suspended`, `restoring`),
/// and the drain reconciler never visits an undrained node again. Refusing on
/// the marker alone would therefore make `start` a permanent 503 for a sandbox
/// whose disk is sitting intact on a healthy node, with delete as the owner's
/// only exit. `active_node_id` alone, and not the placement `COALESCE`: a
/// re-placement with a target chosen and nothing active yet still has no local
/// copy anywhere.
pub async fn restore_source_plan(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<RestoreSourcePlanRow>, sqlx::Error> {
    type Row = (Option<String>, Option<String>, Option<NodeId>, bool);
    let row: Option<Row> = sqlx::query_as(sql!(
        "SELECT (
                 SELECT nk.k8s_node_name
                   FROM node_k8s nk
                  WHERE nk.node_id = COALESCE(s.target_node_id, s.active_node_id)
             ) AS mapped_name,
             (
                 SELECT ske.k8s_node_name
                   FROM sandbox_k8s_executions ske
                  WHERE ske.sandbox_id = s.id
                    AND ske.k8s_node_name IS NOT NULL
                  ORDER BY ske.generation DESC
                  LIMIT 1
             ) AS execution_name,
             COALESCE(s.target_node_id, s.active_node_id) AS placement_node_id,
             -- `active_node_id` alone, deliberately, and *not* the
             -- `COALESCE` above: a re-placement with a target chosen and
             -- nothing active yet still has no local copy anywhere. Pinned by
             -- the mid-re-placement phase of
             -- `a_stopped_evacuation_counts_as_unplaced_only_once_its_local_copy_is_gone`.
             COALESCE(
                 s.evacuation_restore_target = 'stopped' AND s.active_node_id IS NULL,
                 FALSE
             ) AS stopped_evacuation_unplaced
             FROM sandboxes s
             WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await?;
    Ok(row.map(
        |(mapped_name, execution_name, placement_node_id, stopped_evacuation_unplaced)| {
            RestoreSourcePlanRow {
                mapped_name,
                execution_name,
                placement_node_id,
                stopped_evacuation_unplaced,
            }
        },
    ))
}

/// The disk artifact [`stopped_evacuation_restore_source`] selects: the
/// storage location's connection string, unvalidated, and the row's pinned
/// artifact id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoppedEvacuationRestoreSourceRow {
    pub storage_connection_string: String,
    pub current_local_artifact_id: LocalArtifactId,
}

/// Select the disk artifact the evacuation restore sweep re-places an evacuated
/// `stopped` sandbox from, once the evacuation claim has moved the row to
/// `restoring`. `None` when nothing matches.
///
/// `evacuation_restore_target = 'stopped'` is what scopes this to the stopped
/// evacuation. A suspended evacuation's `restoring` row carries `'started'` or
/// `'paused'`, and a user restore clears the marker when it claims, so neither
/// can select a disk artifact here.
///
/// The pin ([`snapshot_is_current_local_artifact`]) is required: it is the only
/// thing that ties the URI to the bytes the drain published. The drain wrote it
/// (`record_stopped_evacuation_artifact`, which refuses to replace a pin with a
/// different id) before releasing the node, and the claim leaves it alone. The
/// id returned is the row's pin, not the snapshot's: the shared fragment makes
/// them equal, and the pin is the value sandbox-manager's own row carries.
///
/// Newest first, with an `sn.id` tiebreak: a retried backup of one artifact
/// mints a fresh `snapshots` row per attempt while keeping the same
/// `local_artifact_id`, so the pin can match several rows, and the latest
/// registered location is the one to load.
pub async fn stopped_evacuation_restore_source(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<StoppedEvacuationRestoreSourceRow>, sqlx::Error> {
    let pinned = snapshot_is_current_local_artifact("s", "sn");
    let query = sql!(format!(
        "SELECT ssl.storage_connection_string, s.current_local_artifact_id
               FROM sandboxes s
               JOIN snapshots sn ON sn.sandbox_id = s.id
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE s.id = $1
                AND s.lifecycle = 'restoring'
                AND s.evacuation_restore_target = 'stopped'
                AND ssl.orphaned_at IS NULL
                AND sn.type = 'disk'
                AND sn.state = 'ready'
                AND {pinned}
              ORDER BY sn.timestamp DESC, sn.id DESC
              LIMIT 1"
    ));
    let row: Option<(String, LocalArtifactId)> = sqlx::query_as(sqlx::AssertSqlSafe(query))
        .bind(sandbox_id)
        .fetch_optional(executor)
        .await?;
    Ok(
        row.map(|(storage_connection_string, current_local_artifact_id)| {
            StoppedEvacuationRestoreSourceRow {
                storage_connection_string,
                current_local_artifact_id,
            }
        }),
    )
}

#[cfg(test)]
mod tests;
