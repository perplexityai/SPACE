//! Postgres-side queries that back the admin node endpoints.
//!
//! Handlers in [`super::nodes`] stay focused on HTTP, auth, the drain
//! state-machine, and audit logging; the row structs and transaction-bound
//! helpers live here, and the SQL they run lives in the stores they call.

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
use pplx_space_sandbox_residency::ResidencyEvidence;
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
    match serde_json::from_value(value.clone()) {
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
/// type, so a tuple would couple their order to a `SELECT` list in another
/// crate, with a silent miscount as the failure mode.
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
        direct::admin_node(&self.pool, node_id)
            .await
            .map_err(sqlx::Error::from)
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
        .map_err(sqlx::Error::from)
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
        let rows = pplx_space_sandbox_store::test_support::admin_occupancy::batched_occupancy(
            &self.pool,
            node_ids,
            long_runner_age.map(long_runner_age_secs),
            residency,
        )
        .await?;
        Ok(rows.into_iter().map(OccupancyRow::from).collect())
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
/// The `:terminate` guard and the node-drain gate must count the same rows.
/// Postgres-backed, and each case also shows the pre-change guard disagreed —
/// otherwise the agreement proves nothing.
#[cfg(test)]
pub(super) mod terminate_gate_agreement_tests;
/// Postgres-backed decoding contract for [`is_node_wake_gate_closed`].
#[cfg(test)]
mod wake_gate_tests;
