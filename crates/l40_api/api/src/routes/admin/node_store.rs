//! Postgres-side queries that back the admin node endpoints.
//!
//! Handlers in [`super::nodes`] stay focused on HTTP, auth, the drain
//! state-machine, and audit logging; the SQL strings, row structs, and
//! transaction-bound helpers live here.

#[cfg(test)]
use pplx_space_sql_locator::sql;
use std::collections::HashMap;

use pplx_space_accounts::NodeId;
use pplx_space_admin_protocol::{
    AdminNodeCtlStatus, AdminNodeServiceHealth, ListAdminNodesFilters,
};
use pplx_space_node_execution_inventory_store::InstallationGuestRow;
use pplx_space_node_model::{NodeDrain, NodeDrainMode, NodeDrainPhase};
use pplx_space_node_registration_store::direct;
use pplx_space_node_registration_store::drain_reads::{self, NodeDrainColumns};
use pplx_space_node_registration_store::drain_write::{self, DrainColumnsUpdate};
use pplx_space_sandbox_model::SandboxId;
#[cfg(test)]
use pplx_space_sandbox_portability::drain_gate_blocker_pairs;
#[cfg(test)]
use pplx_space_sandbox_portability::snapshot_durable_for_source_release;
use pplx_space_sandbox_residency::ResidencyEvidence;
#[cfg(test)]
use pplx_space_sandbox_residency::{
    node_heartbeat_fresh, non_resident_lifecycle_exclusion, occupies_node_predicate,
    resident_lifecycle,
};
use pplx_space_sandbox_store::{admin_occupancy, node_placements};
use serde::Serialize;
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use std::time::Duration;
use time::OffsetDateTime;

/// The admin node row with api's node health types for its JSON columns.
pub(super) type AdminNodeRow =
    direct::AdminNodeRow<Vec<AdminNodeServiceHealth>, AdminNodeCtlStatus>;

#[cfg(test)]
mod fleet_occupancy;
#[cfg(test)]
mod single_node_occupancy;

/// The node's published execution policy and its receipt time. `None` when
/// the node published none, or published one this build can't decode: that
/// withholds the policy evidence rather than failing the node read.
pub(super) fn execution_policy(
    row: &AdminNodeRow,
) -> Option<(
    pplx_space_execution_spec::ExecutionPolicyObservation,
    Option<OffsetDateTime>,
)> {
    let value = row.execution_policy.as_ref()?;
    match serde_json::from_value(value.0.clone()) {
        Ok(policy) => Some((policy, row.execution_policy_received_at)),
        Err(error) => {
            tracing::warn!(node_id = %row.id, %error, "invalid stored execution policy; withholding policy evidence");
            None
        }
    }
}

/// Placement rows attributed to the selected node, without durability accounting.
#[derive(Debug, Serialize)]
pub(super) struct NodeSandboxRow {
    pub(super) id: SandboxId,
    pub(super) lifecycle: String,
    pub(super) active_node_id: Option<NodeId>,
    pub(super) target_node_id: Option<NodeId>,
}

impl From<node_placements::NodePlacementRow> for NodeSandboxRow {
    fn from(row: node_placements::NodePlacementRow) -> Self {
        Self {
            id: row.id,
            lifecycle: row.lifecycle.as_str().to_owned(),
            active_node_id: row.active_node_id,
            target_node_id: row.target_node_id,
        }
    }
}

/// Both occupancy counts that the admin response surfaces:
/// - `active` — sandboxes still occupying placement on this node (drain
///   threshold count; matches the RFC reconciler condition).
/// - `local_snapshots` — sandboxes whose snapshot is still pinned to this
///   node's local disk (strand-risk count). Mostly `suspended` rows, but not
///   only: the in-flight-upload arm imposes no lifecycle filter, so any sandbox
///   with a fresh non-durable `creating` snapshot sourced here counts, whatever
///   its lifecycle. Drops permanently to 0 once S3-export suspend is live
///   across the fleet.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub(super) struct SandboxOccupancy {
    pub(super) active: i64,
    pub(super) local_snapshots: i64,
}

pub(super) use pplx_space_sandbox_store::admin_occupancy::TerminateOccupancy;

/// The lifecycle scope of the `active` occupancy count: everything that has
/// neither terminated (`failed` / `deleted`) nor migrated off the node
/// (`suspended`).
///
/// `deleted` is not a value `SandboxLifecycle` produces: it is the tombstone
/// raw SQL writes into the TEXT column.
///
/// Shared by the batched listing count and this module's in-transaction
/// `:terminate` guard, so the *lifecycle scope* an operator reads and the one
/// that refuses an operator-driven termination cannot drift.
///
/// The two are no longer equal counts, and deliberately so. The
/// guard additionally applies the census discount, so it matches the listing's
/// `terminate_blockers` — not its `active`, which stays raw precisely so an
/// operator can see the phantom rows the discount drops. The figure that
/// refuses is therefore a figure the listing publishes; that equality is pinned
/// by `the_guard_refuses_exactly_what_the_listing_calls_terminate_blockers`.
///
/// The list itself now lives next to `RESIDENT_LIFECYCLES` in the shared
/// residency module as [`NON_RESIDENT_LIFECYCLES`], rendered by
/// [`non_resident_lifecycle_exclusion`] — the hoist this comment used to
/// prescribe. So `terminate_blockers` no longer merely *predicts* the
/// cluster-controller's drained-node terminator; it and that reconciler read the
/// same definition, and node-drain's `draining → drained` gate consults it too
/// before releasing a node. What remains local to this helper is only the
/// alias-shaped rendering.
///
/// This is deliberately *not* the scope of the live-resident drainability
/// count, which narrows to `RESIDENT_LIFECYCLES`: occupancy blocks
/// `:terminate`, while the ranking predicts what a drain waits on, and a
/// `stopped` sandbox does the first without doing the second.
///
/// `alias` is the SQL alias for the `sandboxes` row in the calling query — pass
/// the bare table name (`"sandboxes"`) when no alias is set.
#[cfg(test)]
fn active_occupancy_lifecycles(alias: &str) -> String {
    format!("({})", non_resident_lifecycle_exclusion(alias))
}

/// Ceiling on the long-runner age, applied before the value reaches
/// `make_interval`. Any threshold this large already selects nothing, so
/// clamping costs no expressiveness and keeps a hostile query parameter from
/// turning into a Postgres `interval out of range` error.
const MAX_LONG_RUNNER_AGE: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

/// Per-node live-resident counts backing [`AdminNodeDrainability`].
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct NodeLiveResidents {
    pub(super) live_residents: i64,
    pub(super) live_long_runners: i64,
    /// Rows holding this node's `draining → drained` gate open, counted with
    /// the reconciler's own shared predicate. Not a subset of
    /// `live_residents`: the states it counts can pin through `target_node_id`
    /// with `active_node_id` NULL.
    pub(super) drain_blockers: i64,
    /// Rows the drained-node terminator would block `drained → terminating` on:
    /// the occupancy scope with the same census discount it applies. Wider than
    /// `live_residents` (it keeps `stopped` / `created` / `deleting` /
    /// `quarantined`) and
    /// narrower than [`SandboxOccupancy::active`] (it drops phantoms).
    pub(super) terminate_blockers: i64,
    /// The policy these counts were computed under, so the response can say so
    /// rather than leaving a caller to guess which mode the cluster runs.
    pub(super) residency: ResidencyEvidence,
    /// The threshold `live_long_runners` was actually computed against, after
    /// clamping — reported for the same reason as `residency`, so a clamped or
    /// defaulted age is visible rather than silent.
    pub(super) long_runner_age: Duration,
}

/// One row of [`AdminNodeStore::occupancy_for_nodes`]'s result, named rather
/// than positional: the five `i64` counters are adjacent and interchangeable by
/// type, so a tuple would couple their order to a `SELECT` list dozens of lines
/// away in a `format!` string, with a silent miscount as the failure mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromRow)]
struct OccupancyRow {
    node_id: NodeId,
    active: i64,
    local_snapshots: i64,
    terminate_blockers: i64,
    live_residents: i64,
    live_long_runners: i64,
    drain_blockers: i64,
    heartbeat_fresh: bool,
}

impl From<admin_occupancy::NodeOccupancyCounts> for OccupancyRow {
    fn from(counts: admin_occupancy::NodeOccupancyCounts) -> Self {
        let admin_occupancy::NodeOccupancyCounts {
            node_id,
            active,
            local_snapshots,
            terminate_blockers,
            live_residents,
            live_long_runners,
            drain_blockers,
            heartbeat_fresh,
        } = counts;
        Self {
            node_id,
            active,
            local_snapshots,
            terminate_blockers,
            live_residents,
            live_long_runners,
            drain_blockers,
            heartbeat_fresh,
        }
    }
}

/// Everything one occupancy read reports for a node. `live` is `Some` only
/// when the caller asked for the drainability counters; both halves then come
/// from a single statement, which is what lets a caller subtract one from the
/// other.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct NodeOccupancy {
    pub(super) occupancy: SandboxOccupancy,
    /// Whether the node currently has a reachable, unexpired heartbeat — read
    /// in the same statement as the counts, because it is the ranking's first
    /// term and must describe the same snapshot the counts describe.
    ///
    /// `false` is the fail-closed reading for a node with no status row at all,
    /// which is also what [`Default`] yields.
    pub(super) heartbeat_fresh: bool,
    pub(super) live: Option<NodeLiveResidents>,
}

/// Whether the per-row occupancy CTE should be `MATERIALIZED`, as the SQL
/// keyword to splice in.
///
/// Materializing is load-bearing only when the row flags actually carry the
/// census absence proof. The aggregates over that CTE reference the projected
/// booleans three times, and an inlined CTE lets Postgres expand those aliases
/// and re-evaluate whatever they stand for per aggregate — restoring the per-row
/// SubPlan over the census relation and pushing the plan back over
/// `jit_above_cost`.
///
/// The node-drain reconciler avoids the same duplication the other way: its
/// resident-count query stays flat and keeps the absence proof under a single
/// `FILTER`, so no second reference to it exists for the planner to expand.
/// That shape is not available here — the ranking needs three counts that all
/// narrow the same proof, so the proof is referenced more than once by
/// construction and the fence is what stops the re-expansion.
///
/// In every other mode nothing on the row is worth fencing: the census flag is
/// the literal TRUE, and the two flags that remain — an `IN` test over a column
/// already on the row, and one `make_interval` comparison against a bound
/// parameter — are each referenced once by the aggregates, so there is no
/// repeated evaluation to prevent. Materializing would still cost: it would spool every active sandbox row in the listed set into a
/// tuplestore, which on a fleet listing is millions of rows and can spill to
/// temporary storage. The ordinary listing path — which asks for no drainability
/// counters at all — must therefore keep the streaming shape it had before the
/// ranking existed.
#[cfg(test)]
fn row_cte_materialization(ranking_requested: bool, residency: ResidencyEvidence) -> &'static str {
    if ranking_requested && residency.discounts_absent_rows() {
        " MATERIALIZED"
    } else {
        ""
    }
}

/// The age the statement actually applied, i.e. after the clamp. Reported back
/// so a caller can tell a clamped threshold from the one it asked for.
fn effective_long_runner_age(requested: Duration) -> Duration {
    requested.min(MAX_LONG_RUNNER_AGE)
}

/// Long-runner age as whole seconds for `make_interval`, clamped so a hostile
/// query parameter cannot reach Postgres as an out-of-range interval.
fn long_runner_age_secs(age: Duration) -> i64 {
    i64::try_from(age.min(MAX_LONG_RUNNER_AGE).as_secs())
        .expect("clamped to MAX_LONG_RUNNER_AGE, which fits in i64")
}

/// Pool-bound repository for admin node queries. Operations that run in the
/// caller's transaction (the writes made under [`direct::lock_admin_node`]'s
/// row lock) live as free functions below, since a `Transaction` cannot be
/// held inside `&self`.
#[derive(Clone)]
pub(super) struct AdminNodeStore {
    pool: PgPool,
}

impl AdminNodeStore {
    pub(super) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The node's placement rows by id, unfiltered:
    /// [`node_placements::list_by_id`] with an empty filter.
    pub(super) async fn list_sandboxes(
        &self,
        node_id: NodeId,
        after: Option<SandboxId>,
        limit: i64,
    ) -> Result<Vec<NodeSandboxRow>, sqlx::Error> {
        let rows = node_placements::list_by_id(
            &self.pool,
            node_id,
            &node_placements::NodePlacementFilter::default(),
            after,
            limit,
        )
        .await?;
        Ok(rows.into_iter().map(NodeSandboxRow::from).collect())
    }

    /// The node's admin row: [`direct::admin_node`].
    pub(super) async fn fetch(&self, node_id: NodeId) -> Result<Option<AdminNodeRow>, sqlx::Error> {
        direct::admin_node(&self.pool, node_id).await
    }

    /// The admin rows matching `filters`: [`direct::list_admin_nodes`].
    pub(super) async fn list(
        &self,
        filters: &ListAdminNodesFilters,
    ) -> Result<Vec<AdminNodeRow>, sqlx::Error> {
        // Three filterable axes, each bound as a nullable sentinel rather
        // than concatenated, to keep the prepared-statement cache useful.
        let phase_param: Option<&str> = match filters.drain_phase.as_deref() {
            Some("none") => Some(""),
            other => other,
        };
        direct::list_admin_nodes(
            &self.pool,
            filters.role.as_deref(),
            filters.include_deleted,
            phase_param,
        )
        .await
    }

    /// The runtime installations each of `node_ids` last reported, one row
    /// per guest platform; an installation with no guest row has `guest_os`
    /// and `guest_arch` NULL. Ordered by node, runtime and version.
    pub(super) async fn runtime_installations_for_nodes(
        &self,
        node_ids: &[NodeId],
    ) -> Result<Vec<InstallationGuestRow>, sqlx::Error> {
        pplx_space_node_execution_inventory_store::list_installations_with_guests(
            &self.pool, node_ids,
        )
        .await
    }

    /// Occupancy lookup keyed on `node_id`. Returns the count of
    /// live placements and the count of suspended-with-local-snapshot
    /// sandboxes in **one** Postgres round trip — the per-node version is
    /// reserved for the action path where the count has to share a
    /// transaction with the drain-phase write. Single-node reads use a separate
    /// scalar-UUID statement: fleet-sized generic plans cannot be reused for a
    /// diagnostic read. Fleet reads aggregate active placements directly and
    /// probe durability only for target/upload candidates, sharing those facts
    /// with ranked blockers instead of joining the retained snapshot catalog.
    /// **Every requested node id appears in the returned map**, counts
    /// zero-filled by the scalar aggregate or the batched scope join, so a node
    /// with no matching sandbox row still reports its real `heartbeat_fresh`
    /// rather than being absent. That matters for ranking — an absent node
    /// would fall to `unwrap_or_default()`, whose fail-closed `heartbeat_fresh:
    /// false` sorts it last as blind, and a node with zero residents is the
    /// *best* drain candidate, not the worst.
    ///
    /// `long_runner_age` opts into the drainability counters, which are
    /// computed by the **same statement** rather than a second query. That is
    /// not just a saved round trip: `live_residents` is a subset of `active`,
    /// and the two are only comparable when they are counted over one snapshot.
    /// Two autocommit statements can straddle a concurrent lifecycle or
    /// placement change and report `live_residents > active`, which is not a
    /// state the database was ever in and which inverts the phantom-row
    /// subtraction operators read off the pair. `None` folds every per-row flag
    /// to the literal FALSE and drops the blocker scan, so the default listing
    /// plans no census probe at all.
    ///
    /// It still evaluates `heartbeat_fresh` once per listed node even on that
    /// path, where no caller reads it — `admin_node_response` receives no block
    /// and the drain-action echo takes `.occupancy` alone. That is a small new
    /// cost on a listing that predates the ranking, kept rather than gated
    /// because the flag always describes the same snapshot as whatever counts
    /// accompany it.
    /// Gate it here if that cost is ever measured to matter;
    /// `heartbeat_freshness_is_reported_without_the_drainability_counters` pins
    /// the current choice and would move with it.
    pub(super) async fn occupancy_for_nodes(
        &self,
        node_ids: &[NodeId],
        long_runner_age: Option<Duration>,
        residency: ResidencyEvidence,
    ) -> Result<HashMap<NodeId, NodeOccupancy>, sqlx::Error> {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let long_runner_age_secs = long_runner_age.map(long_runner_age_secs).unwrap_or(0);
        let ranked = long_runner_age.is_some();
        let rows = match node_ids {
            [node_id] => vec![
                admin_occupancy::node_occupancy(
                    &self.pool,
                    *node_id,
                    long_runner_age_secs,
                    ranked,
                    residency,
                )
                .await?,
            ],
            _ => {
                admin_occupancy::fleet_occupancy(
                    &self.pool,
                    node_ids,
                    long_runner_age_secs,
                    ranked,
                    residency,
                )
                .await?
            }
        };
        Ok(rows
            .into_iter()
            .map(OccupancyRow::from)
            .map(|row| {
                (
                    row.node_id,
                    NodeOccupancy {
                        occupancy: SandboxOccupancy {
                            active: row.active,
                            local_snapshots: row.local_snapshots,
                        },
                        heartbeat_fresh: row.heartbeat_fresh,
                        // `None` when nothing asked for the counters, so a
                        // caller can never mistake "not computed" for the
                        // all-zero reading that marks the best candidate.
                        live: long_runner_age.map(|age| NodeLiveResidents {
                            live_residents: row.live_residents,
                            live_long_runners: row.live_long_runners,
                            drain_blockers: row.drain_blockers,
                            terminate_blockers: row.terminate_blockers,
                            residency,
                            long_runner_age: effective_long_runner_age(age),
                        }),
                    },
                )
            })
            .collect())
    }

    // Retained as the pre-optimization oracle for differential tests/benchmarks.
    #[cfg(test)]
    async fn batched_occupancy_rows(
        &self,
        node_ids: &[NodeId],
        long_runner_age: Option<Duration>,
        residency: ResidencyEvidence,
    ) -> Result<Vec<OccupancyRow>, sqlx::Error> {
        // `local_snapshot` counts strand-risk sandboxes for a node: a
        // suspended sandbox pinned to it whose snapshot is **not** yet
        // durably in S3 (attributed to `target_node_id`), plus any sandbox
        // whose suspend-snapshot upload is still in flight *from* it
        // (`snapshots.state='creating'`, attributed to `source_node_id`).
        // UNION deduplicates the suspended and in-flight branches per
        // (node, sandbox), matching the `:terminate` guard's per-sandbox count.
        // When the upload source differs from the target, both nodes count.
        // The shared marker-aware `snapshot_durable_for_source_release`
        // predicate is the single source of truth for "durably restorable from
        // S3": an evacuation-marked sandbox counts as strand risk until its
        // *newest* full snapshot is `ready` (matching the node-drain
        // source-release guards), while a plain user-suspended sandbox uses
        // placement-freshness.
        // Quarantined sandboxes are counted as active occupancy because
        // quarantine preserves the hot-node association for later inspection.
        //
        // Deliberately **not** census-aware, unlike the `:terminate` guard in
        // `admin_occupancy::terminate_occupancy`. This scan is multi-node
        // (`ANY($1::UUID[])`), so the discount's node expression would have to be
        // the per-row `active_node_id` column rather than a bound parameter —
        // the shape `sandbox_residency::node_heartbeat_fresh` documents as
        // costing 30–120 ms per call by forcing a hashed SubPlan over
        // `node_status_snapshots`. This listing refuses nothing, so over-counting
        // shows an operator a high number rather than blocking an operation. The
        // guard, which does refuse, pays for accuracy instead.
        let snapshot_exists = snapshot_durable_for_source_release("s");
        // Durability check for the in-flight UNION branch (which joins
        // `sandboxes s2`): a sandbox already durably restorable must not count
        // as strand risk just because an earlier `creating` row lingers. Uses
        // the same marker-aware predicate as the suspended branch and the
        // in-tx guard so all three agree. (Its inner `EXISTS` reuses the alias
        // `sn`, shadowing the outer `snapshots sn` within the subquery — the
        // subquery is self-contained, correlating only via `s2.id`.)
        let portable_for_snapshot = snapshot_durable_for_source_release("s2");
        let active_lifecycles = active_occupancy_lifecycles("sandboxes");
        // Per-row drainability flags, carried on the `active` branch only —
        // `live_residents` is by definition a subset of `active`, so deriving
        // both from the same scan is what makes the subset relation hold by
        // construction instead of by two queries agreeing. The census predicate
        // is read only in its TRUE direction, per that module's contract: `NOT
        // proven_absent` means "not proven absent", never "proven present".
        //
        // `present` is the census-aware "the node has not proven this row
        // absent" test, projected once per row. The three counts below narrow it
        // by ANDing the projected booleans rather than re-stating the predicate,
        // so the absence proof is written into the statement once and evaluated
        // once per row. Each narrowing answers a different gate's question:
        //
        // - `active` — no narrowing. The occupancy figure the `:terminate` guard
        //   enforces, phantoms included.
        // - `present` — what the drained-node terminator blocks on. It discounts
        //   phantom rows exactly as the terminator does, but keeps `stopped` /
        //   `created` / `deleting` / `quarantined`, which the census reports and
        //   the terminator therefore still refuses to advance past. Ranking on
        //   the raw occupancy count instead would prefer a node holding one
        //   reported `stopped` row over one holding fifty phantoms — backwards.
        //   Under `LifecycleOnly` the discount is a no-op: `present` is the
        //   literal TRUE and this count equals `active` exactly, so the phantom
        //   argument only bites on a cluster that has opted in.
        // - `present AND resident` — what a drain waits on. `RESIDENT_LIFECYCLES`
        //   drops `stopped` / `created` / `deleting` / `quarantined` because
        //   there is no VM the pipeline can evacuate. So
        //   `active` minus this is phantom rows *plus* those non-evacuable
        //   lifecycles: "not blocking a drain" is broader than "not really
        //   there".
        //
        // `residency` is the caller's, read from the same switch the drain
        // reconciler reads, so the counts describe whichever policy the
        // cluster's gates run under rather than a second opinion about it.
        //
        // Without a threshold every flag is the literal FALSE, so Postgres plans
        // no census probe at all and the default listing keeps its old cost.
        let (present_flag, resident_flag, aged_flag) = match long_runner_age {
            Some(_) => {
                // The reconciler's own policy branch, not a second copy of it:
                // under the default `LifecycleOnly` this is the literal `TRUE`,
                // so no census probe is planned at all and every occupying row
                // counts — which is what the drain and terminator gates do in
                // that mode. Reading the same switch is the only way the ranking
                // predicts what those gates will actually do.
                (
                    occupies_node_predicate("sandboxes", "sandboxes.active_node_id", residency),
                    resident_lifecycle("sandboxes"),
                    "sandboxes.lifecycle_changed_at < NOW() - make_interval(secs => $2::BIGINT)"
                        .to_string(),
                )
            }
            None => (
                "FALSE".to_string(),
                "FALSE".to_string(),
                "FALSE".to_string(),
            ),
        };
        let row_cte_materialization = row_cte_materialization(long_runner_age.is_some(), residency);
        // Per-node heartbeat freshness, evaluated once per listed node in the
        // `scope` CTE rather than derived later from the separate `list()`
        // read. It is the ranking's first term, so reading it from a different
        // statement than the counts it orders lets a node that flipped
        // reachable between the two sort by one snapshot's visibility and
        // another's occupancy.
        // The ranking's visibility term and the rows' discount term are the same
        // definition, evaluated here against the unnested node id.
        let heartbeat_fresh = node_heartbeat_fresh("scope_input.node_id");
        // The reconciler's own `draining -> drained` gate: two of the states it
        // counts pin through `target_node_id` while `active_node_id` is NULL, so
        // the occupancy branches cannot see them and a node whose drain is held
        // open would otherwise report zero everywhere.
        //
        // Counted from the batched pair form, grouped once for the whole node
        // set. The single-node predicate is not reusable per node here: its two
        // disjuncts attribute to different columns, so a correlated evaluation
        // leaves the planner no indexable driver on `sandboxes` and degrades to a
        // full scan of that table per listed node when sorting by drainability.
        //
        // Gated on the same opt-in as the census probes, and the count travels
        // inside the `Option` alongside the live counters, so a caller cannot
        // read a skipped computation as "no blockers".
        let blockers_cte = match long_runner_age {
            Some(_) => format!(
                "SELECT node_id, COUNT(*)::BIGINT AS drain_blockers\n\
                   FROM (\n{}\n) gate_blockers\n\
                  GROUP BY node_id",
                drain_gate_blocker_pairs("$1::UUID[]")
            ),
            // Shape-compatible and empty, so the join below yields no row and
            // the COALESCE reports zero without the scan.
            None => "SELECT NULL::UUID AS node_id, 0::BIGINT AS drain_blockers\n\
                      WHERE FALSE"
                .to_string(),
        };
        let query = format!(
            "WITH scope AS (
                 SELECT scope_input.node_id AS node_id,
                        {heartbeat_fresh} AS heartbeat_fresh
                   FROM UNNEST($1::UUID[]) AS scope_input(node_id)
             ),
             blockers AS (
             {blockers_cte}
             ),
             occupancy_rows AS{row_cte_materialization} (
                   -- `sandbox_id` here feeds no aggregate; it is what makes
                   -- this arm's arity match the deduped local-snapshot arm below
                   -- across the `UNION ALL`. Dropping it as unused breaks the
                   -- statement at runtime, not at compile time.
                   SELECT active_node_id AS node_id, id AS sandbox_id,
                          'active' AS kind,
                          ({present_flag}) AS present,
                          ({resident_flag}) AS resident,
                          ({aged_flag}) AS aged
                     FROM sandboxes
                    WHERE active_node_id = ANY($1::UUID[])
                      AND {active_lifecycles}
                   UNION ALL
                   -- One row per (node, sandbox), deduped *here* rather than by a
                   -- DISTINCT aggregate below. `local_snapshots` counts
                   -- sandboxes, and two of these branches can describe one
                   -- sandbox on one node. UNION collapses that pair without
                   -- a correlated in-flight-upload exclusion for every suspended
                   -- row. Keep both nodes when the target differs from the source.
                   --
                   -- Deduping here and keeping a plain `COUNT(*) FILTER` below is
                   -- deliberate: a `COUNT(DISTINCT …)` in the aggregate disables
                   -- hashed aggregation for the whole `GROUP BY`. Measured at
                   -- 200k active rows, that planned a `GroupAggregate` over a
                   -- Sort of *every* occupancy row — active rows included —
                   -- where the plain count plans a `HashAggregate` over the
                   -- `Append` (whose active arm still gathers in parallel). The
                   -- sort here is not removed so much as bounded: this `UNION`
                   -- plans as Sort → Unique over the strand-risk rows alone,
                   -- which is the trade. Paying the unbounded version on the
                   -- default listing, which asks for none of this, would be a
                   -- regression.
                   SELECT node_id, sandbox_id, 'local_snapshot' AS kind,
                          FALSE AS present, FALSE AS resident, FALSE AS aged
                     FROM (
                         SELECT target_node_id AS node_id, s.id AS sandbox_id
                           FROM sandboxes s
                          WHERE target_node_id = ANY($1::UUID[])
                            AND lifecycle = 'suspended'
                            AND NOT {snapshot_exists}
                         UNION
                         -- A sandbox with several `creating` rows from the same
                         -- source counts once, matching the in-tx guard's
                         -- per-sandbox EXISTS. Freshness-gated and
                         -- fresh-ready-excluded so stale prior-cycle remnants and
                         -- already-portable sandboxes don't count.
                         --
                         -- The gate here is the *legacy* one, while the in-tx
                         -- guard's in-flight term goes through the artifact-aware
                         -- fragment, so the two still disagree per node in one
                         -- case: a `creating` row naming an artifact other than
                         -- the sandbox's current one, sourced from a node that is
                         -- not also its target. This arm counts that source node;
                         -- the guard does not. The listing therefore
                         -- **over**-reports — an operator who expects
                         -- `:terminate` to refuse on the strength of it will find
                         -- the termination allowed. Closing that means routing
                         -- this arm through the shared fragment, which is what
                         -- the batched blocker form does and is a change to
                         -- pre-existing strand-risk counting, so it is tracked
                         -- rather than folded in here.
                         SELECT sn.source_node_id AS node_id, sn.sandbox_id
                           FROM snapshots sn
                           JOIN sandboxes s2 ON s2.id = sn.sandbox_id
                          WHERE sn.source_node_id = ANY($1::UUID[])
                            AND sn.type = 'full'
                            AND sn.state = 'creating'
                            AND (
                                s2.placement_changed_at IS NULL
                                OR sn.timestamp >= s2.placement_changed_at
                            )
                            AND NOT {portable_for_snapshot}
                     ) local_snapshot_dedup
             ),
             occupancy AS (
             SELECT node_id,
                    COUNT(*) FILTER (WHERE kind = 'active')::BIGINT AS active,
                    -- Safe as a row count because the branch above already
                    -- deduped per (node, sandbox); see the note there for why
                    -- the dedup is not a `COUNT(DISTINCT …)` here.
                    COUNT(*) FILTER (
                        WHERE kind = 'local_snapshot'
                    )::BIGINT AS local_snapshots,
                    COUNT(*) FILTER (
                        WHERE kind = 'active' AND present
                    )::BIGINT AS terminate_blockers,
                    COUNT(*) FILTER (
                        WHERE kind = 'active' AND present AND resident
                    )::BIGINT AS live_residents,
                    COUNT(*) FILTER (
                        WHERE kind = 'active' AND present AND resident AND aged
                    )::BIGINT AS live_long_runners
               FROM occupancy_rows
              WHERE node_id IS NOT NULL
              GROUP BY node_id
             )
             SELECT scope.node_id,
                    COALESCE(occupancy.active, 0)::BIGINT AS active,
                    COALESCE(occupancy.local_snapshots, 0)::BIGINT AS local_snapshots,
                    COALESCE(occupancy.terminate_blockers, 0)::BIGINT AS terminate_blockers,
                    COALESCE(occupancy.live_residents, 0)::BIGINT AS live_residents,
                    COALESCE(occupancy.live_long_runners, 0)::BIGINT AS live_long_runners,
                    COALESCE(blockers.drain_blockers, 0)::BIGINT AS drain_blockers,
                    scope.heartbeat_fresh
               FROM scope
               LEFT JOIN occupancy ON occupancy.node_id = scope.node_id
               LEFT JOIN blockers ON blockers.node_id = scope.node_id"
        );
        let mut statement =
            sqlx::query_as::<_, OccupancyRow>(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(node_ids);
        // Bound only when the threshold is in play, because `$2` appears in the
        // statement only then — sqlx rejects a bind the SQL never references.
        if let Some(age) = long_runner_age {
            statement = statement.bind(long_runner_age_secs(age));
        }
        statement.fetch_all(&self.pool).await
    }

    /// Best-effort: persists `message` into `nodes.drain_last_error` for the
    /// node-drain reconciler to pick up. A failure to write the error must
    /// not mask the original substrate convergence failure, so this swallows
    /// the sqlx error and logs.
    pub(super) async fn record_drain_last_error(&self, node_id: NodeId, message: &str) {
        if let Err(err) = drain_write::set_drain_last_error(&self.pool, node_id, message).await {
            tracing::warn!(
                %node_id,
                error = %err,
                "failed to persist drain_last_error after substrate convergence failure",
            );
        }
    }
}

/// `true` if the user-API drain gate is currently refusing
/// `start`/`wake`/`resume`/`restore` for sandboxes pinned to this node.
///
/// `:force-upload-local-snapshots` calls this per-sandbox during its fan-out so a
/// concurrent drain transition short-circuits the remaining uploads rather than
/// letting them register a snapshot whose on-disk bytes may already be racing
/// with a user wake.
///
/// This asks about the **gate**, not the phase. A non-NULL `drain_phase` used to
/// imply the gate was closed; with drain modes it does not — a gentle drain keeps
/// serving user traffic by design. Reading the phase alone here would let an
/// `:undrain` + gentle `:drain` landing mid-fan-out look like the safe regime it
/// isn't, which is the whole failure this re-check exists to prevent. Resolved
/// through the same [`NodeDrain::refuses_activations`] the gate itself uses,
/// from the node's shared drain columns.
pub(super) async fn is_node_wake_gate_closed(
    pool: &PgPool,
    node_id: NodeId,
) -> Result<bool, sqlx::Error> {
    let Some(NodeDrainColumns {
        drain_phase,
        drain_mode,
        drain_engine,
    }) = drain_reads::drain_columns(pool, node_id).await?
    else {
        return Ok(false);
    };
    // An unknown phase fails the check with a decode error on column 0, the
    // phase's position in `drain_columns`, so the fan-out aborts instead of
    // reading the gate as open.
    let phase = drain_phase
        .as_deref()
        .map(NodeDrainPhase::parse)
        .transpose()
        .map_err(|source| sqlx::Error::ColumnDecode {
            index: "0".to_owned(),
            source: source.into(),
        })?;
    // So does an engine this build doesn't know, on column 2, the engine's
    // position: this build is the old replica relative to any newer engine.
    match NodeDrain::from_columns(
        drain_engine.as_deref(),
        phase.map(NodeDrainPhase::as_str),
        drain_mode.as_deref(),
    ) {
        Some(NodeDrain::Unknown) => Err(sqlx::Error::ColumnDecode {
            index: "2".to_owned(),
            source: format!("unrecognized drain engine {drain_engine:?}").into(),
        }),
        drain => Ok(drain.is_some_and(NodeDrain::refuses_activations)),
    }
}

/// The in-place update for a node that is already `draining`: rewrite
/// `drain_mode` and clear `drain_last_error`, and touch nothing else that
/// describes *this* drain — including `terminate_decrement_capacity`, which is
/// fixed for a drain's lifetime.
///
/// Also reached by a `:drain` that re-asserts the mode it already had, where the
/// handler passes `mode: None` — **not** the resolved current mode. That
/// distinction is load-bearing: writing the resolved mode back would replace a
/// raw column this build cannot parse with `'gentle'`, disarming a drain the
/// newer pod that wrote it treats as destructive. The write is still worth
/// making on that path, because it clears `drain_last_error` — a repeated
/// request is a fresh operator intent, and the reconciler re-derives the error
/// on its next tick if the condition persists. Two exceptions, both leaving the
/// row with no error rather than a stale one: a same-name kube Node replacement
/// is skipped silently by design, and a cluster with `SPACE_NODE_DRAIN_ENABLED`
/// off has no tick to re-derive it at all — the conjunct
/// [`super::nodes`]' cordon-retry predicate names as the one the api-server
/// cannot see.
///
/// A named wrapper rather than an [`update_drain_phase`] call with the right
/// arguments at the call site, because "the right arguments" is the whole
/// contract and four of them are silent when wrong.
///
/// Start with the one this wrapper exists to pin: `new_phase` binds straight
/// into `SET drain_phase = $2`, so unlike the two `COALESCE`d arguments below, a
/// `None` there **clears** the phase rather than preserving it. Passing `None`
/// from this edge would undrain the node mid-drain while the handler still
/// answered 200 and audited `to_phase: draining`, because the caller hardcodes
/// the phase it reports rather than reading back what was written.
///
/// `clear_started_at` is the adjacent trap: `true` there NULLs
/// `drain_started_at` **and** `drain_mode`, disarming the very drain this
/// function exists to escalate.
///
/// The other two:
///
/// - `set_started_at: false` preserves `drain_started_at`. An escalation makes a
///   drain more aggressive; it does not start a new one, and the column is the
///   drain's only record of when it began. Four readers, and only the last is
///   not an operator's eyes:
///   - The admin node response echoes it (`admin_node_response`).
///   - `space-admin-cli` renders it as `Drain Started` — the age an operator reads to
///     decide whether a drain has sat too long. Re-stamping resets that reading
///     to seconds-ago at the exact moment they concluded it had.
///   - The management dashboard computes the same age for its drain column and
///     hover tooltip (`ui/src/app/dashboard/nodes/page.tsx`).
///   - `scripts/space-rollout-mng.py`'s `detect_stuck` computes
///     `now - drain_started_at` and, past `--stuck-threshold`, flags the node and
///     emits `NODE_STUCK`. That flag is what stops a stuck node consuming a
///     drain slot in `pick_drain_candidates`, so re-stamping before the threshold
///     elapses keeps a wedged node holding a slot and throttles the whole wave.
///     (The flag is sticky once set, so a re-stamp cannot un-flag a node that was
///     already caught — the exposure is nodes that had not been caught yet.)
/// - `terminate_decrement_capacity` is always `None` here: this edge never
///   moves the replacement intent. A request that expressed one disagreeing
///   with the stored value is refused before reaching this function
///   (`in_flight_replace_conflict`), so the choice is between preserving and
///   409ing, never between preserving and silently overwriting.
///
///   Applying an expressed value looks safer and is not. Every `space-admin-cli`
///   built before `replace_after_terminate` became an `Option` sends an
///   explicit `!no_replace_after_terminate`, so a plain `--forced` from a stale
///   binary carries `true` — and CLI binaries roll independently of the
///   api-server. Honouring it would let a stale client silently re-enable ASG
///   replacement on a blue-green shrink, which is the failure the `Option` was
///   introduced to prevent, arriving from the other direction.
///
/// The phase is re-asserted at `draining` rather than skipped so the write still
/// clears `drain_last_error`: a fresh operator intent should not inherit the
/// previous tick's substrate failure.
pub(super) async fn update_in_flight_drain(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    mode: Option<NodeDrainMode>,
) -> Result<(), sqlx::Error> {
    update_drain_phase(
        tx,
        node_id,
        Some(NodeDrainPhase::Draining.as_str()),
        false,
        false,
        None,
        mode,
    )
    .await
}

pub(super) async fn update_drain_phase(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    new_phase: Option<&'static str>,
    set_started_at: bool,
    clear_started_at: bool,
    terminate_decrement_capacity: Option<bool>,
    drain_mode: Option<NodeDrainMode>,
) -> Result<(), sqlx::Error> {
    // Two callers: the phase-moving edges in `apply_drain_action`, and
    // [`update_in_flight_drain`] for a `:drain` against a node already
    // `draining`. Which columns move is a property of the arguments rather than
    // of the edge, so read this with a caller's arguments in hand.
    //
    // `terminate_decrement_capacity` is refreshed on the `NULL -> draining` edge
    // only, which is the one that carries an initiating `replace_after_terminate`.
    // Every other caller — `drained -> terminating`, `:undrain`, and
    // [`update_in_flight_drain`] — passes `None` and leaves the column at
    // whatever the initiating drain wrote, so the terminator honors that intent
    // regardless of when the ASG call actually runs.
    //
    // `drain_mode` is written on the `NULL -> draining` edge and by an in-place
    // escalation (a same-mode repeat passes `None` and leaves it),
    // cleared by `:undrain` alongside the phase, and left alone by
    // `drained -> terminating`. Clearing it on undrain matters because the
    // user-API gates read the mode: a stale `gentle` left on an undrained node
    // would be read by nothing today, but a later drain that failed to write the
    // column would silently inherit it. The undrained steady state is "no drain,
    // no mode".
    drain_write::update_drain_columns(
        &mut **tx,
        node_id,
        DrainColumnsUpdate {
            phase: new_phase,
            set_started_at,
            clear_started_at,
            terminate_decrement_capacity,
            drain_mode,
        },
    )
    .await?;
    Ok(())
}

/// Postgres-backed regression test for the admin `:terminate` occupancy
/// guard: an in-flight suspend-snapshot upload from a node must count as
/// strand risk so the endpoint refuses to terminate the node — including
/// when the sandbox's `target_node_id` has drifted to another node, which
/// the `target_node_id = $1` clause alone would miss.
#[cfg(test)]
mod inflight_upload_occupancy_tests;

/// Postgres-backed truth table for the fail-closed liveness discount behind the
/// drainability counters.
#[cfg(test)]
mod live_residents_tests;

/// Postgres-backed contract for the in-place `gentle → forced` escalation: the
/// mode is rewritten, and both `drain_started_at` and the initiating drain's
/// `terminate_decrement_capacity` survive whatever the request carried.
#[cfg(test)]
mod in_flight_drain_tests;
#[cfg(test)]
mod node_holds_agreement_tests;
/// Postgres-backed plan-structure and page-shape regression for the
/// `:force-upload-local-snapshots` enumeration: the scan stays bounded by the
/// node's own pinned history rather than the fleet, the in-flight probe runs
/// only for non-durable rows, and the page stays the ascending-id prefix the
/// handler's `deferred[]` tail depends on.
#[cfg(test)]
mod pinned_local_only_plan_tests;
/// The `:terminate` guard and the node-drain gate must count the same rows.
/// Postgres-backed, and each case also shows the pre-change guard disagreed —
/// otherwise the agreement proves nothing.
#[cfg(test)]
pub(super) mod terminate_gate_agreement_tests;
/// Postgres-backed decoding contract for [`is_node_wake_gate_closed`].
#[cfg(test)]
mod wake_gate_tests;
