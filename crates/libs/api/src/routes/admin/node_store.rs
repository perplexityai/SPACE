//! Postgres-side queries that back the admin node endpoints.
//!
//! Handlers in [`super::nodes`] stay focused on HTTP, auth, the drain
//! state-machine, and audit logging; the SQL strings, row structs, and
//! transaction-bound helpers live here.

use pplx_space_common_core::sql;
use std::collections::HashMap;
use std::num::NonZeroUsize;

use pplx_space_common_core::types::{
    NodeAddress, NodeDrainMode, NodeDrainPhase, NodeDrainState, NodeId, NodeManagedBy, NodeRole,
    SandboxId,
    admin::{AdminNodeCtlStatus, AdminNodeServiceHealth, ListAdminNodesFilters},
};
use pplx_space_common_svc::sql::sandbox_residency::{
    ResidencyEvidence, active_resident_predicate, non_resident_lifecycle_exclusion,
};
#[cfg(test)]
use pplx_space_common_svc::sql::sandbox_residency::{
    node_heartbeat_fresh, occupies_node_predicate, resident_lifecycle,
};
#[cfg(test)]
use pplx_space_sandbox_portability::drain_gate_blocker_pairs;
use pplx_space_sandbox_portability::{
    drain_gate_blockers_for_node, inflight_suspend_upload_exists,
    snapshot_durable_for_source_release,
};
use serde::Serialize;
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use std::time::Duration;
use time::OffsetDateTime;
use uuid::Uuid;

mod fleet_occupancy;
mod single_node_occupancy;

#[derive(Debug, Clone, FromRow)]
pub(super) struct AdminNodeRow {
    pub(super) id: NodeId,
    pub(super) name: Option<String>,
    pub(super) address: Option<NodeAddress>,
    pub(super) node_api_endpoint: Option<String>,
    pub(super) role: NodeRole,
    pub(super) managed_by: NodeManagedBy,
    pub(super) state: Option<String>,
    pub(super) scheduling_status: Option<String>,
    pub(super) scheduling_reason: Option<String>,
    pub(super) status_stale_after: Option<OffsetDateTime>,
    pub(super) status_reachable: Option<bool>,
    pub(super) status_error: Option<String>,
    pub(super) status_role: Option<String>,
    pub(super) status_node_id_reported: Option<String>,
    pub(super) network_slots_ready: Option<i32>,
    pub(super) network_pool_min_ready: Option<i32>,
    pub(super) network_slots_degraded: Option<i32>,
    pub(super) network_slots_quarantined: Option<i32>,
    pub(super) network_slots_total: Option<i32>,
    pub(super) network_slots_leased: Option<i32>,
    pub(super) network_pool_max_slots: Option<i32>,
    pub(super) drain_phase: Option<String>,
    /// Raw column value, not a decoded [`NodeDrainMode`]: a typed decode fails
    /// the whole row on a value this build does not recognize, which would turn
    /// one bad column into a 500 for the entire node listing. Callers resolve
    /// it through `NodeDrainMode::from_column_str`.
    pub(super) drain_mode: Option<String>,
    pub(super) drain_started_at: Option<OffsetDateTime>,
    pub(super) drain_last_error: Option<String>,
    pub(super) terminate_decrement_capacity: bool,
    pub(super) metadata: Option<sqlx::types::Json<serde_json::Value>>,
    pub(super) last_heartbeat: Option<OffsetDateTime>,
    pub(super) status_observed_at: Option<OffsetDateTime>,
    // Decode diagnostics after reading the row: unknown policy schemas must
    // not prevent node inspection or the lock used by administrative actions.
    pub(super) execution_policy: Option<sqlx::types::Json<serde_json::Value>>,
    pub(super) execution_policy_received_at: Option<OffsetDateTime>,
    pub(super) execution_inventory_status: Option<String>,
    pub(super) execution_inventory_received_at: Option<OffsetDateTime>,
    pub(super) status_health_healthy: Option<bool>,
    pub(super) status_services: Option<sqlx::types::Json<Vec<AdminNodeServiceHealth>>>,
    pub(super) status_node_ctl: Option<sqlx::types::Json<AdminNodeCtlStatus>>,
    pub(super) created_at: OffsetDateTime,
    pub(super) updated_at: Option<OffsetDateTime>,
    pub(super) deleted_at: Option<OffsetDateTime>,
    pub(super) substrate_node_id: Option<String>,
}

impl AdminNodeRow {
    /// The node's published execution policy and its receipt time. `None`
    /// when the node published none, or published one this build can't
    /// decode: that withholds the policy evidence rather than failing the
    /// node read.
    pub(super) fn execution_policy(
        &self,
    ) -> Option<(
        pplx_space_common_core::types::ExecutionPolicyObservation,
        Option<OffsetDateTime>,
    )> {
        let value = self.execution_policy.as_ref()?;
        match serde_json::from_value(value.0.clone()) {
            Ok(policy) => Some((policy, self.execution_policy_received_at)),
            Err(error) => {
                tracing::warn!(node_id = %self.id, %error, "invalid stored execution policy; withholding policy evidence");
                None
            }
        }
    }
}

/// Placement rows attributed to the selected node, without durability accounting.
#[derive(Debug, Serialize, FromRow)]
pub(super) struct NodeSandboxRow {
    pub(super) id: SandboxId,
    pub(super) lifecycle: String,
    pub(super) active_node_id: Option<NodeId>,
    pub(super) target_node_id: Option<NodeId>,
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

/// What the `:terminate` guard counts, kept separate from
/// [`SandboxOccupancy`] on purpose.
///
/// The two are produced by different statements for different jobs.
/// `SandboxOccupancy` is what the node *listing* reports, and its
/// `local_snapshots` is a diagnostic an operator reads. This one is what the
/// 409 is *decided* on, and its second term is the shared node-drain gate
/// (`drain_gate_blockers_for_node`) so the endpoint and the reconciler cannot
/// disagree about whether a node is safe to terminate.
///
/// Sharing one struct would have forced one meaning on both, and the
/// meanings are genuinely different — see the comment on the statement.
#[derive(Debug)]
pub(super) struct TerminateOccupancy {
    pub(super) active: i64,
    /// Rows the node-drain gate counts against this node. Named for what it
    /// is rather than for the field it replaced: it is not a count of local
    /// snapshots and reading it as one is how the previous divergence went
    /// unnoticed.
    pub(super) drain_blockers: i64,
}

/// The lifecycle scope of the `active` occupancy count: everything that has
/// neither terminated (`failed` / `cleanup_required` / `deleted`) nor migrated
/// off the node (`suspended`).
///
/// `cleanup_required` and `deleted` are not values `SandboxLifecycle` produces
/// today — the column is TEXT and they are retained defensively for rows an
/// older schema may have written. Keeping them costs nothing and dropping them
/// would silently start counting any such row as occupancy.
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

const ADMIN_NODE_SELECT: &str = "SELECT n.id,
       nd.name,
       host(n.address) AS address,
       nd.node_api_endpoint,
       n.role,
       n.managed_by,
       n.state,
       nss.scheduling_status,
       nss.scheduling_reason,
       nss.stale_after AS status_stale_after,
       nss.reachable AS status_reachable,
       nss.error AS status_error,
       nss.role AS status_role,
       nss.node_id_reported AS status_node_id_reported,
       nss.network_slots_ready,
       nss.network_pool_min_ready,
       nss.network_slots_degraded,
       nss.network_slots_quarantined,
       nss.network_slots_total,
       nss.network_slots_leased,
       nss.network_pool_max_slots,
       n.drain_phase,
       n.drain_mode,
       n.drain_started_at,
       n.drain_last_error,
       n.terminate_decrement_capacity,
       nd.metadata,
       n.last_heartbeat,
       nss.observed_at AS status_observed_at,
       nss.execution_policy,
       nss.execution_policy_received_at,
       nss.execution_inventory_status,
       nss.execution_inventory_received_at,
       nss.health_healthy AS status_health_healthy,
       nss.services AS status_services,
       nss.node_ctl AS status_node_ctl,
       n.created_at,
       nd.updated_at,
       nd.deleted_at,
       nk.k8s_node_name AS substrate_node_id
FROM nodes n
LEFT JOIN node_direct nd ON nd.node_id = n.id
LEFT JOIN node_k8s nk ON nk.node_id = n.id
LEFT JOIN node_status_snapshots nss ON nss.node_id = n.id";

/// Pool-bound repository for admin node queries. Operations that need their
/// own transaction (locking + writing under `FOR UPDATE`) live as free
/// functions below, since a `Transaction` cannot be held inside `&self`.
#[derive(Clone)]
pub(super) struct AdminNodeStore {
    pool: PgPool,
}

impl AdminNodeStore {
    pub(super) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Each branch filters on one indexed placement column and returns at most
    /// one page. UNION deduplicates rows active and targeted on the same node.
    pub(super) async fn list_sandboxes(
        &self,
        node_id: NodeId,
        after: Option<SandboxId>,
        limit: i64,
    ) -> Result<Vec<NodeSandboxRow>, sqlx::Error> {
        sqlx::query_as(sql!(
            "SELECT id, lifecycle, active_node_id, target_node_id FROM (
                (SELECT id, lifecycle, active_node_id, target_node_id
                 FROM sandboxes
                 WHERE active_node_id = $1 AND id > $2
                 ORDER BY id LIMIT $3)
                UNION
                (SELECT id, lifecycle, active_node_id, target_node_id
                 FROM sandboxes
                 WHERE target_node_id = $1 AND id > $2
                 ORDER BY id LIMIT $3)
             ) node_sandboxes ORDER BY id LIMIT $3"
        ))
        .bind(node_id)
        .bind(after.map(|id| id.as_uuid()).unwrap_or(Uuid::nil()))
        .bind(limit)
        .fetch_all(&self.pool)
        .await
    }

    pub(super) async fn fetch(&self, node_id: NodeId) -> Result<Option<AdminNodeRow>, sqlx::Error> {
        sqlx::query_as::<_, AdminNodeRow>(sqlx::AssertSqlSafe(sql!(format!(
            "{ADMIN_NODE_SELECT}\nWHERE n.id = $1"
        ))))
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub(super) async fn list(
        &self,
        filters: &ListAdminNodesFilters,
    ) -> Result<Vec<AdminNodeRow>, sqlx::Error> {
        // Three filterable axes layered onto the base SELECT. We bind
        // sentinels rather than concatenate to keep the prepared-statement
        // cache useful.
        let phase_param: Option<String> = match filters.drain_phase.as_deref() {
            Some("none") => Some(String::new()),
            Some(other) => Some(other.to_string()),
            None => None,
        };
        let query = format!(
            "{ADMIN_NODE_SELECT}\n\
             WHERE ($1::TEXT IS NULL OR n.role = $1)\n\
               AND ($2::BOOL OR nd.deleted_at IS NULL)\n\
               AND (\n\
                 $3::TEXT IS NULL\n\
                 OR ($3 = '' AND n.drain_phase IS NULL)\n\
                 OR n.drain_phase = $3\n\
               )\n\
             ORDER BY n.role ASC,\n\
                      COALESCE(nd.name, nk.k8s_node_name) ASC NULLS LAST,\n\
                      nd.node_api_endpoint ASC NULLS LAST"
        );
        sqlx::query_as::<_, AdminNodeRow>(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(filters.role.as_deref())
            .bind(filters.include_deleted)
            .bind(phase_param)
            .fetch_all(&self.pool)
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
        let rows = match node_ids {
            [node_id] => {
                let query = single_node_occupancy::query(long_runner_age.is_some(), residency);
                let row =
                    sqlx::query_as::<_, OccupancyRow>(sqlx::AssertSqlSafe(sql!(query.as_str())))
                        .bind(node_id)
                        .bind(long_runner_age.map(long_runner_age_secs).unwrap_or(0))
                        .fetch_one(&self.pool)
                        .await?;
                vec![row]
            }
            _ => {
                let query = fleet_occupancy::query(long_runner_age.is_some(), residency);
                sqlx::query_as::<_, OccupancyRow>(sqlx::AssertSqlSafe(sql!(query.as_str())))
                    .bind(node_ids)
                    .bind(long_runner_age.map(long_runner_age_secs).unwrap_or(0))
                    .fetch_all(&self.pool)
                    .await?
            }
        };
        Ok(rows
            .into_iter()
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
        // `sandbox_occupancy_for_node_in_tx`. This scan is multi-node
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
        if let Err(err) = sqlx::query(sql!("UPDATE nodes SET drain_last_error = $1 WHERE id = $2"))
            .bind(message)
            .bind(node_id)
            .execute(&self.pool)
            .await
        {
            tracing::warn!(
                %node_id,
                error = %err,
                "failed to persist drain_last_error after substrate convergence failure",
            );
        }
    }
}

/// `SELECT ... FOR UPDATE OF n` — admin handlers lock the row for the duration
/// of the validate + write window so two operators cannot race transitions.
pub(super) async fn lock_node_for_update(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
) -> Result<Option<AdminNodeRow>, sqlx::Error> {
    sqlx::query_as::<_, AdminNodeRow>(sqlx::AssertSqlSafe(sql!(format!(
        "{ADMIN_NODE_SELECT}\nWHERE n.id = $1\nFOR UPDATE OF n"
    ))))
    .bind(node_id)
    .fetch_optional(&mut **tx)
    .await
}

/// In-transaction occupancy lookup used by the `:terminate` guard so the
/// strand-risk count is observed under the same `FOR UPDATE` lock as the
/// drain-phase write.
pub(super) async fn sandbox_occupancy_for_node_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    residency: ResidencyEvidence,
) -> Result<TerminateOccupancy, sqlx::Error> {
    // Both properties the hand-written predicate used to provide are now
    // properties of the shared gate, so they are asserted there rather than
    // restated here: an evacuation-marked sandbox blocks until its newest
    // full snapshot is `ready` (`snapshot_durable_for_source_release`), and a
    // suspend upload still in flight *from* this node blocks even when the
    // sandbox's `target_node_id` has since drifted elsewhere — the gate's
    // second arm drives from `snapshots.source_node_id` precisely so that
    // drift cannot hide it. Terminating mid-upload kills the worker holding
    // the only durable copy.
    //
    // Quarantined sandboxes remain active placement blockers by design:
    // quarantine preserves the hot-node association until a quarantine-aware
    // release or cleanup path decides it is safe to drop.
    // `active` routes through the shared `active_resident_predicate` rather than
    // through this module's `active_occupancy_lifecycles`. The two agree on the
    // exclusion list — the helper is exactly that list — but the shared
    // predicate also carries `active_node_id = $1` and, load-bearing here, the
    // census term. So this guard counts exactly the rows node-drain's
    // `draining → drained` gate and `mark_terminating_query` count.
    //
    // The census term is why: this endpoint now *refuses* on `active`, and
    // `space-rollout-mng.py` drives it automatically. Without the discount, a
    // worker whose own census disproves its phantom `active_node_id` rows
    // clears the census-aware drain gate, reaches `drained`, and would then be
    // refused here forever wherever `SPACE_NODE_DRAIN_NODE_CENSUS_RESIDENCY` is
    // on. `$1` is a bound parameter, which is the shape that keeps the heartbeat
    // subquery a once-per-execution InitPlan.
    let active = active_resident_predicate("sandboxes", "$1", residency);
    // The second count is `drain_gate_blockers_for_node`, not a third
    // hand-written gate. Two things change by routing it here.
    //
    // **It stops disagreeing with the reconciler.** The previous predicate
    // counted `lifecycle = 'suspended'` only, so a `suspending` row, or a
    // non-evacuation `restoring` row, pinned to the node by `target_node_id`
    // was invisible to it. When such a row's `active_node_id` is NULL, the
    // `active` half, which keys on `active_node_id`, cannot see it either; an
    // in-flight user restore of a suspended sandbox is the common case. The
    // node-drain gate counts those rows (`drain_gate_target_pinned_blocks`),
    // so `:terminate` would accept a `drained` node that had picked up such a
    // row since it drained, one the gate would hold, and `space-rollout-mng.py`
    // drives that endpoint unattended.
    //
    // **It stops scanning the table.** The old statement had no `WHERE` at
    // all — the node predicates lived inside the `FILTER`s — so both counts
    // were paid over the whole `sandboxes` table on every `:terminate`. Each
    // count is now its own node-scoped subquery.
    //
    // What this deliberately does *not* do is redefine the listing's
    // `local_snapshots`. That number is reported, not gated on, and it and
    // `drain_blockers` legitimately differ — the listing's in-flight branch
    // inlines the legacy freshness gate while the blocker fragment takes the
    // artifact-exact arm (see
    // `live_residents_tests::an_artifact_mismatched_inflight_upload_counts_the_sandbox_once`).
    // Collapsing the reported count into the gate would erase a distinction
    // an operator reading the listing may need; collapsing the *decision*
    // into the gate is the whole point.
    let blockers = drain_gate_blockers_for_node("$1");
    let query = format!(
        "SELECT
     (SELECT COUNT(*)::BIGINT FROM sandboxes WHERE {active}) AS active,
     (SELECT COUNT(*)::BIGINT FROM (
{blockers}
          ) gate_blockers) AS drain_blockers"
    );
    let row: (i64, i64) = sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id)
        .fetch_one(&mut **tx)
        .await?;
    Ok(TerminateOccupancy {
        active: row.0,
        drain_blockers: row.1,
    })
}

/// `$1` = node id. Suspended sandboxes whose `target_node_id` points at this
/// node and whose snapshot has not yet been registered to S3, as the lowest
/// `limit` sandbox ids. Used by the `:force-upload-local-snapshots` admin
/// handler to enumerate the sandboxes that still need a durable upload before
/// the source node can terminate.
///
/// Built as a `String` rather than issued here so the plan-structure regression
/// in `pinned_local_only_plan_tests` can `EXPLAIN` the statement body the
/// handler runs — `sql!` prepends a locator comment to it, which changes no
/// plan. `sql!` stays at the query site in
/// [`list_pinned_local_only_sandboxes`], so this statement keeps its own
/// call-site locator instead of collapsing onto a shared one.
///
/// The caller passes `limit = enumeration_cap + 1` so a returned slice
/// of length `enumeration_cap + 1` is the unambiguous "more pinned
/// sandboxes exist beyond what's listed" signal — the handler trims
/// the sentinel row and surfaces `truncated: true` to the operator.
/// This bounds DB transfer and JSON response size to O(1) regardless
/// of how big the node's pinned-list actually is.
///
/// ## The page is the ascending-id prefix, and that is a contract
///
/// `ORDER BY s.id` makes the page stable across re-invocations: the handler
/// slices `to_process` from the head and the `deferred` tail in Rust, so a
/// non-deterministic row order would publish a `deferred[]` array that is a
/// sample rather than the work the next call picks up.
///
/// A head that keeps failing therefore keeps being retried while the rows
/// behind it wait. That residual is deliberate and it is loud rather than
/// silent — the same ids come back in `failed[]` and the same `deferred[]` on
/// every call. Reshuffling the page so a stuck sandbox is eventually skipped
/// would hide it instead.
///
/// ## What makes the ordering cheap
///
/// `idx_sandboxes_target_node_ordered` covers `(target_node_id, id)`. The equality
/// on `$1` fixes the leading column, so the scan emits the node's rows already
/// ordered by `id`: the ordering needs no `Sort`, and the `LIMIT` stops at the
/// page size. Work is bounded by the node's own pinned history — which is the
/// question being asked — rather than by the table.
///
/// The index makes that plan available; the predicate's shape keeps the
/// planner on it. Written as two bare conjuncts, the probes can be pulled up
/// into a join-driven plan that reads the whole table even with this index
/// present, and on the test fixture a generic plan does exactly that. The
/// `CASE` and the scalar-subquery fences below each rule that plan out.
///
/// The planner may still pick the narrow `idx_sandboxes_target_node` plus a
/// sort when the table is small enough that the two cost the same. That plan
/// is node-bounded too, which is the property that matters, so the sort's
/// absence is the common case rather than an invariant.
///
/// Without that index the planner takes the ordering from the primary key's
/// own index instead, expecting the `LIMIT` to stop early. For a node whose
/// non-durable set is empty there is nothing to stop early on, so that plan
/// filters the whole table and runs past the 10 s admin-handler budget. Empty
/// is the ordinary answer here: it is what an operator gets once the backlog
/// has cleared, and it was the true answer on the node whose forced drain this
/// query family stalled.
///
/// ## Why both probes are wrapped in scalar subqueries
///
/// The `CASE` described below already keeps both checks out of reach of
/// sublink pull-up, which Postgres applies only to a top-level `WHERE` or
/// `JOIN … ON` conjunct, including a top-level `NOT` directly over an
/// `EXISTS`. The fences are a second guard for the conjunct form: if the
/// predicate is ever written as conjuncts, they keep the plan this
/// section describes. The rest of the section is about that form.
///
/// `(SELECT {snapshot_exists})` rather than a bare `{snapshot_exists}`, and
/// the same for the in-flight check. The two forms are identical in truth —
/// `EXISTS` yields a non-NULL boolean and the scalar subquery returns exactly
/// one row — but as a top-level conjunct the bare sublink is *pullable*:
/// Postgres may realise it as a nested loop, a hash anti-join, or a merge
/// anti-join over the whole `snapshots` ⋈
/// `snapshot_storage_locations` product. A nested loop keeps the work bounded
/// to the requested sandboxes. Inside a scalar subquery it cannot be pulled up
/// at all, so it is always a per-row `SubPlan` whose own `sn.sandbox_id = s.id`
/// makes each execution a `sandbox_id`-keyed index probe.
///
/// The node-drain twin fences the same predicate for the same reason. Updated
/// statistics can otherwise make a merge anti-join cheaper in the planner's
/// estimate without any query change. The fence removes that alternative.
///
/// The in-flight check is fenced for the same reason, and here this statement
/// departs from its node-drain twin, which leaves that check bare. Bare, it is
/// the planner's cost choice. It can read the cluster's `state='creating'` full
/// snapshots once and anti-join against them, which is cheap, but the
/// node-ordered scan beneath it also makes a merge anti-join
/// along `idx_snapshots_sandbox_id` available, and that one walks every
/// snapshot whose sandbox id sorts below the page, whichever node owns it.
/// Fenced, each execution is correlated on the one sandbox being asked about.
/// That doesn't make each execution cheap. Which index serves it is still a
/// cost choice: the test fixtures' plans use a scan keyed on
/// `idx_snapshots_sandbox_id`, but a planner that overestimates how many
/// snapshots a sandbox has can instead `BitmapAnd` that index with
/// `idx_snapshots_state_type`, whose side reads the cluster's whole in-flight
/// set on every execution. The test fixture doesn't reproduce that plan, and
/// the plan tests' read counts don't include a bitmap index scan's reads,
/// which is why they also bound how often the check runs. The fence bounds which
/// sandboxes the check is about; the evaluation order below bounds how often
/// it runs.
///
/// ## Why the in-flight check sits in a `CASE` branch
///
/// `CASE WHEN (SELECT {snapshot_exists}) THEN false ELSE NOT (SELECT
/// {inflight_upload}) END` rather than two `AND NOT` conjuncts. The two forms
/// are identical in truth, since neither scalar subquery can yield NULL. But
/// Postgres orders the conjuncts of a filter by its own cost estimate, so it's
/// free to evaluate the in-flight check first, on every suspended row the node
/// ever pinned. A long-lived node's pinned history is almost entirely durable,
/// so that order runs the check once per row of history. Under the bitmap plan
/// above, each of those executions also passes over the in-flight set, and the
/// statement's cost becomes history × in-flight set.
///
/// Postgres documents that a `CASE` evaluates only the subexpressions needed
/// to determine its result, in order, so the in-flight check runs only for
/// rows the durability check didn't exclude. That's the node's backlog, which
/// is what an operator came to drain, and it holds whichever plan the planner
/// picks for the check itself. Postgres calls the rule not ironclad and names
/// constant subexpressions folded at planning time and aggregates as the
/// cases that break it. Neither applies: both conditions read the row.
///
/// The fixed order has a cost of its own: the durability check runs on
/// every suspended row, including those the in-flight check alone would have
/// excluded. That cost is bounded by the node's rows, and each execution has
/// an index that fits it exactly: `idx_snapshots_sandbox_ready_latest_covering`
/// is keyed on `sandbox_id` with the probe's own `state = 'ready' AND type =
/// 'full'` as its predicate, and it serves the probe in every plan the tests
/// measure. `snapshot_scans_stay_within_the_nodes_snapshots` and
/// `inflight_check_runs_only_for_non_durable_rows` both bound its reads per
/// scan; only the first bounds the in-flight check's the same way.
///
/// ## Why `limit` is a literal
///
/// `limit` is interpolated as a literal rather than bound. `LIMIT $2` is not
/// a constant when Postgres builds a *generic* plan, so there is no
/// `tuple_fraction` to apply and the planner loses the low-startup bias the
/// cap exists to buy — and sqlx prepares every statement, so the plan cache
/// decides which kind of plan this gets. The value is an in-process
/// constant, never operator-supplied, and `NonZeroUsize` renders as digits.
///
/// ## What the predicates mean
///
/// Routes through the shared marker-aware
/// [`snapshot_durable_for_source_release`] fragment so the "still needs a
/// durable upload" predicate here matches the admin occupancy counters and the
/// node-drain drain-blocker / terminator source-release guards exactly —
/// drifting them produces a node the counter says is stuck while the handler
/// enumerates nothing (or vice versa). An evacuation-marked sandbox whose
/// newest snapshot is already `ready` is **not** enumerated (it needs no
/// further upload), even if a failed-restore placement bump made it
/// non-placement-fresh. (The API drain-gate bypass and restore-affinity hint
/// answer a different question — user-op admission and placement — and keep the
/// freshness predicate; they are intentionally not source-release guards.)
///
/// This is deliberately the authoritative predicate rather than the per-node
/// backup-pending queue the drain reconciler consumes. The endpoint exists to
/// rescue what the automated path missed, so reading the derived state it
/// exists to rescue would make exactly the situation an operator reaches for it
/// in the situation it cannot fix. Keeping the two definitions separate also
/// makes this a drift oracle against that queue.
///
/// Excludes sandboxes whose durable upload is already in flight from any node
/// (`NOT inflight_suspend_upload_exists`), mirroring the node-drain reupload
/// queue (`node_drain::store::pinned_local_only_sandboxes_query`):
/// force-reuploading a sandbox
/// whose original upload is still `creating` is redundant and racy, and a
/// resulting newer `failed` snapshot would make `latest_full_snapshot_ready`
/// stop seeing the in-flight upload and could strand an evacuation restore.
fn pinned_local_only_sandboxes_query(limit: NonZeroUsize) -> String {
    let snapshot_exists = snapshot_durable_for_source_release("s");
    let inflight_upload = inflight_suspend_upload_exists("s");
    format!(
        "SELECT s.id
           FROM sandboxes s
          WHERE s.target_node_id = $1
            AND s.lifecycle = 'suspended'
            AND CASE WHEN (SELECT {snapshot_exists}) THEN false
                     ELSE NOT (SELECT {inflight_upload}) END
          ORDER BY s.id
          LIMIT {limit}"
    )
}

/// List up to `limit` suspended sandboxes on this node that still need a
/// durable upload, lowest sandbox id first. See
/// [`pinned_local_only_sandboxes_query`] for the statement and its contract.
pub(super) async fn list_pinned_local_only_sandboxes(
    pool: &PgPool,
    node_id: NodeId,
    limit: NonZeroUsize,
) -> Result<Vec<SandboxId>, sqlx::Error> {
    let query = sql!(pinned_local_only_sandboxes_query(limit));
    sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(query))
        .bind(node_id)
        .fetch_all(pool)
        .await
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
/// through the same [`NodeDrainState::blocks_placement`] the gate itself uses.
pub(super) async fn is_node_wake_gate_closed(
    pool: &PgPool,
    node_id: NodeId,
) -> Result<bool, sqlx::Error> {
    let row: Option<(Option<NodeDrainPhase>, Option<String>)> = sqlx::query_as(sql!(
        "SELECT drain_phase, drain_mode FROM nodes WHERE id = $1"
    ))
    .bind(node_id)
    .fetch_optional(pool)
    .await?;
    Ok(row
        .and_then(|(phase, mode)| NodeDrainState::from_columns(phase, mode.as_deref()))
        .is_some_and(NodeDrainState::blocks_placement))
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
    sqlx::query(sql!(
        "UPDATE nodes
            SET drain_phase = $2,
                drain_started_at = CASE
                    WHEN $3 THEN NOW()
                    WHEN $4 THEN NULL
                    ELSE drain_started_at
                END,
                drain_last_error = NULL,
                terminate_decrement_capacity = COALESCE($5, terminate_decrement_capacity),
                drain_mode = CASE
                    WHEN $4 THEN NULL
                    ELSE COALESCE($6, drain_mode)
                END
          WHERE id = $1"
    ))
    .bind(node_id)
    .bind(new_phase)
    .bind(set_started_at)
    .bind(clear_started_at)
    .bind(terminate_decrement_capacity)
    .bind(drain_mode)
    .execute(&mut **tx)
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
mod terminate_gate_agreement_tests;
