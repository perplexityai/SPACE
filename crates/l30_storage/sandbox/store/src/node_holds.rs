//! A read of the `sandboxes` rows that hold one node, with the gate terms that
//! count each row and the facts behind them.
//!
//! The node-release gates answer with counts only: node-drain's `draining →
//! drained` advance, the admin `:terminate` guard and the admin drainability
//! counters each report how many rows hold a node, not which. This read lists
//! those rows. It writes nothing and decides nothing.
//!
//! # The rows and their terms
//!
//! A row is listed when one of the gates would count it without the census
//! discount: its `active_node_id` names the node in a lifecycle
//! [`active_resident_predicate`] counts, or it is one of the node's drain-gate
//! blockers ([`drain_gate_blockers_for_node`]). Each listed row carries three
//! terms, each the gates' own shared fragment evaluated for that row under the
//! caller's [`ResidencyEvidence`]:
//!
//! - [`NodeHoldTerms::resident`]: node-drain's resident scan
//!   (`active_node_id`, [`resident_lifecycle`] and [`occupies_node_predicate`]).
//!   The admin drainability `live_residents`.
//! - [`NodeHoldTerms::drain_blocker`]: [`drain_gate_blockers_for_node`]. The
//!   drainability `drain_blockers`, and the `:terminate` 409's
//!   `sandboxes_drain_blockers`.
//! - [`NodeHoldTerms::active_resident`]: [`active_resident_predicate`]. The
//!   drainability `terminate_blockers`, the 409's `sandboxes_active`, and
//!   node-drain's fourth gate term while the cluster-controller's
//!   termination-blocking gate is on.
//!
//! Which gates those terms add up to is [`NodeHoldTerms`]'s methods. This
//! read does not know the cluster-controller's gate setting, so it reports
//! the drain both ways rather than guessing.
//!
//! So a row the census discount removes is still listed, with `resident` and
//! `active_resident` false. One input to the drain gate is not here: the
//! cluster-controller's in-flight evacuation set is process memory. A row it
//! is evacuating carries `evacuation_restore_target`.
//!
//! # Cost
//!
//! Every hold row of the node is read, because [`NodeHolds::reason_counts`]
//! and [`NodeHolds::term_counts`] are over all of them; `limit` bounds the
//! rows returned, not the scan. The scan is the node's active-placement rows
//! in the occupancy scope plus its drain-gate blockers, the same rows the
//! `:terminate` guard counts.

use std::cmp::Ordering;
use std::fmt;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId, SandboxLifecycle};
use pplx_space_sandbox_portability::{
    drain_gate_blockers_for_node, inflight_suspend_upload_from_node_exists,
    snapshot_durable_for_source_release,
};
use pplx_space_sandbox_residency::{
    RESIDENT_LIFECYCLES, ResidencyEvidence, active_resident_predicate, node_proves_sandbox_absent,
    non_resident_lifecycle_exclusion, occupies_node_predicate, resident_lifecycle,
};
use serde::Serialize;
use time::OffsetDateTime;

/// How long a row may sit in a resident transient lifecycle before this read
/// calls it [`NodeHoldReason::StuckTransient`].
///
/// The cluster-controller's `node_drain::DEFAULT_STALE_ABANDON_SECS`, the value
/// `ReconcilerConfig::stale_abandon` always takes outside its tests. Both are
/// [`crate::drain::STALE_ABANDON_AFTER`], one constant. Under a forced drain,
/// the controller flips such a row to `failed` once it is this old.
pub const STUCK_TRANSIENT_AFTER: Duration = crate::drain::STALE_ABANDON_AFTER;

/// Why a row holds the node: the one sentence an operator reads first. The
/// gate terms that count it are [`NodeHoldTerms`]; a reason is not a term.
///
/// Variants are in the order [`NodeHoldReason::of`] tests them, which is also
/// the order [`list`] returns each group's rows in (see [`NodeHolds::rows`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHoldReason {
    /// `active_node_id` names the node, the node's fresh, complete census
    /// omits the sandbox ([`node_proves_sandbox_absent`]), and the row is not
    /// a drain-gate blocker, which the census never discounts. Counted by
    /// `active_resident`, and by `resident` in a resident lifecycle, under
    /// [`ResidencyEvidence::LifecycleOnly`]; counted by no term under
    /// [`ResidencyEvidence::NodeCensusAware`].
    CensusAbsent,
    /// The drain's evacuation claimed it (`evacuation_restore_target` is set).
    /// A suspending row counts as `resident` or `drain_blocker`; an
    /// evacuation `restoring` row is not a drain blocker; a claimed `stopped`
    /// row counts as `active_resident`. A marked `suspended` row lands here
    /// too. A forced drain's stale-abandon sweep still fails a marked
    /// `suspending` row older than [`STUCK_TRANSIENT_AFTER`] unless its
    /// suspend snapshot is recoverable.
    Evacuating,
    /// A full snapshot upload from this node is in flight and no durable
    /// snapshot exists yet: the in-flight arm of `drain_blocker`.
    UploadInFlight,
    /// A resident transient lifecycle older than [`STUCK_TRANSIENT_AFTER`].
    /// `resident` when its `active_node_id` names the node and the census does
    /// not discount it; `drain_blocker` when it is a `suspending`, or
    /// non-evacuation `restoring`, row pinned by `target_node_id`, whatever
    /// the census says.
    StuckTransient,
    /// The same, younger than [`STUCK_TRANSIENT_AFTER`].
    Transient,
    /// `suspended` and pinned by `target_node_id` with no durable snapshot:
    /// its only copy is on the node. The suspended arm of `drain_blocker`.
    LocalOnlySnapshot,
    /// `started` or `paused` on the node: `resident` and `active_resident`.
    Resident,
    /// Any other lifecycle on the node that [`active_resident_predicate`]
    /// counts (`created`, `stopped`, `deleting`, `quarantined`, or a value the
    /// enum does not model): `active_resident` only. The cluster-controller's
    /// `TerminationBlockingResidents`.
    TerminationBlocking,
}

impl NodeHoldReason {
    pub const ALL: [Self; 8] = [
        Self::CensusAbsent,
        Self::Evacuating,
        Self::UploadInFlight,
        Self::StuckTransient,
        Self::Transient,
        Self::LocalOnlySnapshot,
        Self::Resident,
        Self::TerminationBlocking,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CensusAbsent => "census_absent",
            Self::Evacuating => "evacuating",
            Self::UploadInFlight => "upload_in_flight",
            Self::StuckTransient => "stuck_transient",
            Self::Transient => "transient",
            Self::LocalOnlySnapshot => "local_only_snapshot",
            Self::Resident => "resident",
            Self::TerminationBlocking => "termination_blocking",
        }
    }

    /// The reason for one row [`list`] read. The first matching variant
    /// wins, in declaration order.
    fn of(facts: &NodeHoldFacts, lifecycle: &str, terms: NodeHoldTerms) -> Self {
        let lifecycle = SandboxLifecycle::from_str(lifecycle).ok();
        let resident = lifecycle.is_some_and(|lifecycle| RESIDENT_LIFECYCLES.contains(&lifecycle));
        if facts.census_absent && !terms.drain_blocker {
            Self::CensusAbsent
        } else if facts.evacuation_restore_target.is_some() {
            Self::Evacuating
        } else if facts.upload_in_flight && !facts.durable_snapshot {
            Self::UploadInFlight
        } else if resident && lifecycle.is_some_and(SandboxLifecycle::is_transient) {
            if facts.past_stuck_threshold {
                Self::StuckTransient
            } else {
                Self::Transient
            }
        } else if lifecycle == Some(SandboxLifecycle::Suspended) && !facts.durable_snapshot {
            Self::LocalOnlySnapshot
        } else if resident {
            Self::Resident
        } else {
            Self::TerminationBlocking
        }
    }
}

impl fmt::Display for NodeHoldReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which of the node's placement columns name it on this row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHoldPlacement {
    Active,
    Target,
    Both,
    /// Neither: the row is held only by a snapshot upload in flight from the
    /// node, after its `target_node_id` moved elsewhere.
    UploadSource,
}

impl NodeHoldPlacement {
    fn of(on_active: bool, on_target: bool) -> Self {
        match (on_active, on_target) {
            (true, true) => Self::Both,
            (true, false) => Self::Active,
            (false, true) => Self::Target,
            (false, false) => Self::UploadSource,
        }
    }
}

/// The gate terms that count one row; see the module docs for each.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct NodeHoldTerms {
    pub resident: bool,
    pub drain_blocker: bool,
    pub active_resident: bool,
}

impl NodeHoldTerms {
    /// Whether any term counts this row. Rows no term counts are listed
    /// after the rest.
    pub const fn counted(self) -> bool {
        self.resident || self.drain_blocker || self.active_resident
    }

    /// Whether node-drain's `draining → drained` gate holds the node for this
    /// row with the termination-blocking gate off, apart from its in-flight
    /// evacuation set.
    pub const fn drain(self) -> bool {
        self.resident || self.drain_blocker
    }

    /// Whether node-drain's `draining → drained` gate holds the node for this
    /// row when the cluster-controller's termination-blocking gate is on,
    /// apart from its in-flight evacuation set: [`Self::drain`] plus the
    /// fourth term, `active_resident`. Every `resident` row is also an
    /// `active_resident` row, so this is the same set [`Self::terminate`]
    /// counts.
    pub const fn drain_with_termination_blocking_gate(self) -> bool {
        self.drain() || self.active_resident
    }

    /// Whether the admin `:terminate` guard refuses the node for this row:
    /// it refuses on `sandboxes_active` and on `sandboxes_drain_blockers`.
    pub const fn terminate(self) -> bool {
        self.active_resident || self.drain_blocker
    }
}

/// What the reason is decided on, as the database saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHoldFacts {
    /// [`node_proves_sandbox_absent`] for a row whose `active_node_id` names
    /// the node; false otherwise. Evaluated in every [`ResidencyEvidence`].
    pub census_absent: bool,
    /// [`snapshot_durable_for_source_release`].
    pub durable_snapshot: bool,
    /// [`inflight_suspend_upload_from_node_exists`] for this node.
    pub upload_in_flight: bool,
    /// The `timestamp` of the newest `creating` full snapshot from this node,
    /// whether or not [`Self::upload_in_flight`] counts it.
    pub upload_started_at: Option<OffsetDateTime>,
    pub evacuation_restore_target: Option<String>,
    /// `None` while no cohort restore has failed (the column is zero).
    pub restore_attempt_count: Option<u32>,
    pub next_restore_attempt_at: Option<OffsetDateTime>,
    /// `lifecycle_changed_at` is older than [`STUCK_TRANSIENT_AFTER`], on the
    /// database clock, whatever the lifecycle: an old `started` or `stopped`
    /// row has it too.
    pub past_stuck_threshold: bool,
}

/// One row holding the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHold {
    pub sandbox_id: SandboxId,
    /// The stored value; it may be one [`SandboxLifecycle`] does not model.
    pub lifecycle: String,
    pub lifecycle_changed_at: OffsetDateTime,
    pub placement: NodeHoldPlacement,
    pub reason: NodeHoldReason,
    pub terms: NodeHoldTerms,
    pub facts: NodeHoldFacts,
}

/// How many rows each term counts, over every hold row.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct NodeHoldTermCounts {
    pub resident: u64,
    pub drain_blocker: u64,
    pub active_resident: u64,
}

/// The rows holding one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHolds {
    /// At most the caller's `limit`: rows any term counts first, then rows
    /// none counts; each group ordered by reason, then oldest
    /// `lifecycle_changed_at` first, then id.
    pub rows: Vec<NodeHold>,
    /// Rows per reason over every hold row, every reason present.
    pub reason_counts: Vec<(NodeHoldReason, u64)>,
    pub term_counts: NodeHoldTermCounts,
    /// Every hold row, returned or not.
    pub total: u64,
    pub truncated: bool,
}

#[derive(Debug, sqlx::FromRow)]
struct NodeHoldRow {
    sandbox_id: SandboxId,
    lifecycle: String,
    lifecycle_changed_at: OffsetDateTime,
    on_active: bool,
    on_target: bool,
    evacuation_restore_target: Option<String>,
    restore_attempt_count: i32,
    next_restore_attempt_at: Option<OffsetDateTime>,
    past_stuck_threshold: bool,
    census_absent: bool,
    resident: bool,
    drain_blocker: bool,
    active_resident: bool,
    durable_snapshot: bool,
    upload_in_flight: bool,
    upload_started_at: Option<OffsetDateTime>,
}

impl From<NodeHoldRow> for NodeHold {
    fn from(row: NodeHoldRow) -> Self {
        let facts = NodeHoldFacts {
            census_absent: row.census_absent,
            durable_snapshot: row.durable_snapshot,
            upload_in_flight: row.upload_in_flight,
            upload_started_at: row.upload_started_at,
            evacuation_restore_target: row.evacuation_restore_target,
            restore_attempt_count: u32::try_from(row.restore_attempt_count)
                .ok()
                .filter(|count| *count > 0),
            next_restore_attempt_at: row.next_restore_attempt_at,
            past_stuck_threshold: row.past_stuck_threshold,
        };
        let terms = NodeHoldTerms {
            resident: row.resident,
            drain_blocker: row.drain_blocker,
            active_resident: row.active_resident,
        };
        Self {
            sandbox_id: row.sandbox_id,
            reason: NodeHoldReason::of(&facts, &row.lifecycle, terms),
            lifecycle: row.lifecycle,
            lifecycle_changed_at: row.lifecycle_changed_at,
            placement: NodeHoldPlacement::of(row.on_active, row.on_target),
            terms,
            facts,
        }
    }
}

/// `$1` = node id, `$2` = [`STUCK_TRANSIENT_AFTER`] in seconds.
///
/// `candidates` is the union of the two attributions the gates count,
/// without the census term, so a discounted row is still read. Each term is
/// then the gate's own fragment for that row. The terms and `census_absent`
/// are `COALESCE`d because their `active_node_id = $1` comparisons are NULL
/// on a row with no active node. `census_absent` scopes the absence proof to
/// rows placed on the node, as its caller contract requires. LATERAL plus
/// `OFFSET 0` keeps each `sandboxes` lookup parameterized by the candidate id,
/// even in a generic plan, as in the drainability statement; it projects every
/// column the fragments and the select list read. The durability probe is a
/// scalar subquery so it stays a per-row probe keyed on the sandbox, as there.
fn holds_query(residency: ResidencyEvidence) -> String {
    let blockers = drain_gate_blockers_for_node("$1");
    let occupancy_scope = non_resident_lifecycle_exclusion("sandboxes");
    let resident_lifecycle = resident_lifecycle("s");
    let occupies = occupies_node_predicate("s", "$1", residency);
    let active_resident = active_resident_predicate("s", "$1", residency);
    let absent = node_proves_sandbox_absent("s", "$1");
    let durable = snapshot_durable_for_source_release("s");
    let upload_in_flight = inflight_suspend_upload_from_node_exists("s", "$1");
    sql!(format!(
        "WITH blockers AS MATERIALIZED (
{blockers}
             ), candidates AS MATERIALIZED (
                 SELECT id FROM sandboxes
                  WHERE active_node_id = $1 AND {occupancy_scope}
                 UNION
                 SELECT sandbox_id FROM blockers
             )
             SELECT s.id AS sandbox_id,
                    s.lifecycle,
                    s.lifecycle_changed_at,
                    COALESCE(s.active_node_id = $1, FALSE) AS on_active,
                    COALESCE(s.target_node_id = $1, FALSE) AS on_target,
                    s.evacuation_restore_target,
                    s.restore_attempt_count,
                    s.next_restore_attempt_at,
                    s.lifecycle_changed_at < NOW() - make_interval(secs => $2::BIGINT)
                        AS past_stuck_threshold,
                    COALESCE(s.active_node_id = $1 AND {absent}, FALSE) AS census_absent,
                    COALESCE(
                        s.active_node_id = $1 AND {resident_lifecycle} AND {occupies},
                        FALSE
                    ) AS resident,
                    EXISTS (
                        SELECT 1 FROM blockers b WHERE b.sandbox_id = s.id
                    ) AS drain_blocker,
                    COALESCE({active_resident}, FALSE) AS active_resident,
                    (SELECT {durable}) AS durable_snapshot,
                    {upload_in_flight} AS upload_in_flight,
                    (SELECT MAX(u.timestamp)
                       FROM snapshots u
                      WHERE u.sandbox_id = s.id
                        AND u.source_node_id = $1
                        AND u.type = 'full'
                        AND u.state = 'creating') AS upload_started_at
               FROM candidates c
               CROSS JOIN LATERAL (
                   SELECT id, lifecycle, lifecycle_changed_at, active_node_id,
                          target_node_id, evacuation_restore_target,
                          restore_attempt_count, next_restore_attempt_at,
                          placement_changed_at, current_local_artifact_id,
                          current_local_artifact_kind
                     FROM sandboxes WHERE id = c.id OFFSET 0
               ) s"
    ))
}

static HOLDS_LIFECYCLE_ONLY: LazyLock<String> =
    LazyLock::new(|| holds_query(ResidencyEvidence::LifecycleOnly));
static HOLDS_NODE_CENSUS_AWARE: LazyLock<String> =
    LazyLock::new(|| holds_query(ResidencyEvidence::NodeCensusAware));

/// The rows holding `node_id` under `residency`, at most `limit` of them.
/// A read: it takes no lock and writes nothing. One statement, so the rows
/// and the counts are one snapshot.
pub async fn list(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    residency: ResidencyEvidence,
    limit: NonZeroUsize,
) -> Result<NodeHolds, sqlx::Error> {
    let statement = match residency {
        ResidencyEvidence::LifecycleOnly => HOLDS_LIFECYCLE_ONLY.as_str(),
        ResidencyEvidence::NodeCensusAware => HOLDS_NODE_CENSUS_AWARE.as_str(),
    };
    let stuck_after_secs =
        i64::try_from(STUCK_TRANSIENT_AFTER.as_secs()).expect("ten minutes fits in i64");
    let rows = sqlx::query_as::<_, NodeHoldRow>(sqlx::AssertSqlSafe(statement))
        .bind(node_id)
        .bind(stuck_after_secs)
        .fetch_all(executor)
        .await?;
    Ok(collect(
        rows.into_iter().map(NodeHold::from).collect(),
        limit,
    ))
}

fn collect(mut rows: Vec<NodeHold>, limit: NonZeroUsize) -> NodeHolds {
    let mut reason_counts: Vec<(NodeHoldReason, u64)> =
        NodeHoldReason::ALL.into_iter().map(|r| (r, 0)).collect();
    let mut term_counts = NodeHoldTermCounts::default();
    for row in &rows {
        if let Some((_, count)) = reason_counts.iter_mut().find(|(r, _)| *r == row.reason) {
            *count += 1;
        }
        term_counts.resident += u64::from(row.terms.resident);
        term_counts.drain_blocker += u64::from(row.terms.drain_blocker);
        term_counts.active_resident += u64::from(row.terms.active_resident);
    }
    rows.sort_by(display_order);
    let total = rows.len() as u64;
    let truncated = rows.len() > limit.get();
    rows.truncate(limit.get());
    NodeHolds {
        rows,
        reason_counts,
        term_counts,
        total,
        truncated,
    }
}

fn display_order(a: &NodeHold, b: &NodeHold) -> Ordering {
    b.terms
        .counted()
        .cmp(&a.terms.counted())
        .then(a.reason.cmp(&b.reason))
        .then(a.lifecycle_changed_at.cmp(&b.lifecycle_changed_at))
        .then(a.sandbox_id.as_uuid().cmp(&b.sandbox_id.as_uuid()))
}

#[cfg(test)]
mod tests;
