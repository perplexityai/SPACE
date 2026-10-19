//! Shared SQL fragment for "is this sandbox row still on the node it names?".
//!
//! `sandboxes.active_node_id` is desired-and-last-known placement, not an
//! observation. A row keeps pointing at a node long after its VM is gone —
//! a lost suspend completion, a crashed wake, a node replaced under the same
//! `nodes.id` — so any consumer that treats `active_node_id = <node>` as
//! "occupies the node" can over-count without bound.
//!
//! The node itself publishes the only observation that can settle the
//! question: a full sandbox census (`sandbox_observed_states`, one row per
//! sandbox the node reports it holds, pruned against the node's own complete
//! inventory) alongside a status heartbeat (`node_status_snapshots`, with
//! `reachable`, `stale_after`, and `sandbox_inventory_pruned_at`), all written
//! in one transaction by `ClusterApi/PublishNodeState`.
//!
//! # Direction matters: discount only on positive evidence
//!
//! The fragments below are deliberately asymmetric. A row is discounted only
//! when the node **is** reachable with a fresh heartbeat, **has** published a
//! recent complete census, **and** that census carries no row for that sandbox.
//! Every other state — unreachable node, stale heartbeat, no heartbeat row at
//! all, a census that has never been accepted or has aged out, a census row in a
//! status that disagrees with the cluster's lifecycle, a census row published by
//! another node — keeps the sandbox counted.
//!
//! The asymmetry is what makes a wrong answer survivable. Over-counting slows
//! a drain; under-counting ends a drain early, and a drain that completes on a
//! node still holding a live sandbox terminates that node and destroys the
//! sandbox with it. A brief node outage must therefore never read as "every
//! sandbox on this node is gone".
//!
//! # Why census absence is the evidence and execution rows are not
//!
//! `sandbox_k8s_executions` looks like a second liveness source and is not:
//! its rows drift with the sandbox rows they describe. Execution rows can
//! retain a running state and pod IP after the pod disappears. A node's own
//! complete census provides independent evidence of the sandboxes it holds.
//!
//! # The heartbeat proves the node answered, not that the census is current
//!
//! `node_status_snapshots` is refreshed by *every* `PublishNodeState`, with
//! `reachable` set unconditionally TRUE — including publishes from the node's
//! status, network-policy, and template facets, all of which carry
//! `sandbox_inventory_complete = false`. A fresh heartbeat therefore proves the
//! node answered and nothing about how current its sandbox census is. Two
//! separate windows follow from that, and each gets its own term:
//!
//! * **A sandbox placed since the last complete census.** Ordinary lag, seconds
//!   to a minute, and it happens on healthy nodes. Closed by
//!   [`CENSUS_PLACEMENT_SETTLE`]: a row whose placement is younger than that
//!   window is never proven absent, whatever the census says.
//! * **A node whose complete-inventory publish is wedged while its other facets
//!   keep succeeding.** Closed by [`CENSUS_INVENTORY_MAX_AGE`] over
//!   `node_status_snapshots.sandbox_inventory_pruned_at` — stamped by the
//!   internal API only inside the transaction that actually prunes the census,
//!   so the timestamp and `sandbox_observed_states` cannot disagree. A node
//!   whose inventory publisher has stopped, or whose inventories are being
//!   rejected, ages out of that window and discounts nothing.
//!
//! Those two terms are kept separate rather than collapsed into one comparison,
//! and the ordering between the windows is load-bearing — see
//! [`CENSUS_INVENTORY_MAX_AGE`].
//!
//! # A second definition, deliberately left alone
//!
//! The orphaned-sandbox sweeper carries its own inline census test that also
//! treats a `sandbox_observed_states` row as "leave this row alone", but with
//! two differences: it applies no heartbeat gate, and it counts a census row
//! naming *any* node as presence when `active_node_id IS NULL`. Both suit a
//! sweeper whose action is deleting a sandbox row rather than releasing a node,
//! and changing them would change what that sweeper deletes. Porting it onto
//! these fragments is its own change, not a cleanup — until then, this module
//! is the definition for node-release decisions only.

use pplx_space_common_core::types::SandboxLifecycle;
use std::time::Duration;

/// Env var selecting [`ResidencyEvidence`], read **once** by the
/// cluster-controller wiring and passed to every store that answers the
/// residency question.
///
/// One var rather than one per reconciler: the node-drain gate and the
/// drained-node terminator evaluate this at consecutive gates, so a
/// per-reconciler switch would let an operator disable one and leave a node
/// that converges to `drained` and then parks there forever.
pub const NODE_CENSUS_RESIDENCY_ENV: &str = "SPACE_NODE_DRAIN_NODE_CENSUS_RESIDENCY";

/// What evidence is required before a `sandboxes` row stops counting against
/// its `active_node_id`.
///
/// Defaults to [`Self::LifecycleOnly`], and stays opt-in per environment: this
/// is the switch that decides whether a drain may end while `sandboxes` rows
/// still name the node, so each cluster turns it on deliberately after the
/// census-freshness terms are deployed and observed there.
///
/// The default is conservatism about rollout, not a known gap. Both windows a
/// fresh heartbeat leaves open — ordinary census lag and a wedged inventory
/// publisher — now have terms in [`node_proves_sandbox_absent`]; see the module
/// docs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ResidencyEvidence {
    /// Count every row whose `active_node_id` names the node and whose
    /// lifecycle says it occupies one. The behavior that predates the census
    /// join, and the default.
    #[default]
    LifecycleOnly,
    /// Additionally discount rows [`node_proves_sandbox_absent`] proves absent.
    /// Fail-closed in one direction only — see the module docs.
    NodeCensusAware,
}

impl ResidencyEvidence {
    /// Parse the raw value of [`NODE_CENSUS_RESIDENCY_ENV`], mirroring the
    /// cluster-controller's `env_config::truthy_default` vocabulary exactly.
    ///
    /// Unset, empty, and unrecognized values all fall back to the default
    /// rather than guessing, so a typo (`SPACE_..._RESIDENCY=yes`) leaves the
    /// safe mode in place instead of silently enabling the discount.
    pub fn from_env_value(value: Option<&str>) -> Self {
        match value {
            Some("1" | "true" | "True" | "TRUE") => Self::NodeCensusAware,
            Some("0" | "false" | "False" | "FALSE") => Self::LifecycleOnly,
            _ => Self::default(),
        }
    }

    /// Whether the node-census discount applies. SQL builders branch on this
    /// rather than on a bare boolean so the disabled case has a name.
    pub const fn discounts_absent_rows(self) -> bool {
        matches!(self, Self::NodeCensusAware)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LifecycleOnly => "lifecycle_only",
            Self::NodeCensusAware => "node_census_aware",
        }
    }
}

impl std::fmt::Display for ResidencyEvidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Boolean SQL fragment: does the row aliased `sandbox_alias` still count
/// against the node named by `node_expr`?
///
/// `TRUE` under [`ResidencyEvidence::LifecycleOnly`]; otherwise the negation of
/// [`node_proves_sandbox_absent`]. The single home for that policy branch, so
/// the drain gate and the terminator gate cannot answer it differently and a
/// future evidence mode cannot be handled in one and forgotten in the other.
///
/// Callers compose this into a query already scoped to one node — see
/// [`node_proves_sandbox_absent`]'s caller contract.
pub fn occupies_node_predicate(
    sandbox_alias: &str,
    node_expr: &str,
    evidence: ResidencyEvidence,
) -> String {
    if evidence.discounts_absent_rows() {
        format!(
            "NOT {}",
            node_proves_sandbox_absent(sandbox_alias, node_expr)
        )
    } else {
        "TRUE".to_string()
    }
}

/// The lifecycles in which a `sandboxes` row occupies the node its
/// `active_node_id` names.
///
/// This is residency through `active_node_id` only, which is **not** the whole
/// drain gate: `pplx_space_sandbox_portability::drain_gate_blocks_source_release`
/// covers a second, differently-attributed population that pins through
/// `target_node_id` while `active_node_id` is NULL. A drain waits on the union
/// of the two, so neither set alone answers "what does this node still hold".
///
/// Excluded deliberately, and the reasons are why this is a closed include-list
/// rather than a "not terminal" exclusion:
///
/// - `suspended` — the sandbox is no longer tied to a node once suspend
///   completes (`active_node_id` is NULL).
/// - `failed` — terminal.
/// - `quarantined` — not evacuable: the row is held for investigation rather
///   than moved. Note this is *not* the same as terminal, and the
///   drained-node terminator deliberately keeps counting it for placement
///   because it preserves local bytes and hot-node affiliation.
/// - `created` — the row exists but no pod has been placed, so there is
///   nothing on the node to evacuate. If `target_node_id` happens to point at
///   the draining node, kube-scheduler already skips it, because the drain
///   cordons the node.
/// - `deleting` — the row is already being torn down, so there is no state a
///   drain could preserve by evacuating it. Rows stranded in `deleting` are
///   swept by the orphaned-sandbox reconciler, not by the drain.
/// - `stopped` — the VM is not running and sandbox-manager refuses
///   `suspend_sandbox` from this state ("cannot suspend: sandbox is {}, must be
///   started or paused", in `grpc_lifecycle`'s suspend handler), so counting it
///   makes a drain wait on something the pipeline cannot service.
///
///   **No *reconciler* sweeps a stranded `stopped` row, and nothing backs one
///   up.** The orphaned-sandbox reconciler matches no branch for it: `stopped`
///   is absent from that module's `ACTIVE_EXECUTION_REQUIRED_LIFECYCLES`,
///   `LIVE_POD_REQUIRED_LIFECYCLES` and `PENDING_POD_LIFECYCLES`, and its one
///   remaining branch is `lifecycle = 'deleting'`. Nor does anything publish the
///   sandbox's disk: the only caller that asks sandbox-manager to back up a
///   sandbox's *current instance* as an `ArtifactKind::Disk` is the Kubernetes
///   substrate's `retire_execution`, whose only production call sites are the
///   transient-lifecycle sweeper's two sink methods. Those are driven from
///   several passes in that module, and not all of them from
///   `SWEEPABLE_LIFECYCLES` — the command-ready-pending reap fires on `started`
///   rows and the failed-execution cleanup pass on `failed` ones. Every one of
///   those list queries binds a lifecycle set that excludes `stopped`, which is
///   what matters here; the point is that the reach is wider than the sweep
///   list, so check the passes rather than the list if one is added. (The suspended-execution reaper's
///   similarly-named `retire_stale_execution` is a Postgres-only state
///   transition and reaches no substrate at all.) The fork executor also
///   publishes `Disk` artifacts, but from a named
///   `BackupSource::LocalArtifact` on an explicit user fork, so it is not a
///   path a resting `stopped` row ever takes.
///
///   A `stopped` row's only copy therefore stays on its node, which is by
///   design: the stop handler admits the cleanup with `keep_filesystem = true`
///   and `CleanupCompletion::MarkStopped`, which is what makes `stop` → `start`
///   a valid recovery in the first place.
///
///   What keeps that copy alive through a drain is [`active_resident_predicate`]
///   below, which counts `stopped` and is what every refusal on the way out of
///   the fleet reads. So the failure mode of a `stopped` resident is a node that
///   does not converge on its own, not a sandbox destroyed with its node — that
///   predicate's docs carry which refusals, what each of the four lifecycles it
///   counts is waiting for, and the one configured case that does clear itself.
///
///   **The exclusion outlived half its reason.** `suspend` still refuses a
///   `stopped` sandbox, but the drain no longer only knows how to suspend:
///   where `SPACE_NODE_DRAIN_EVACUATE_STOPPED_RESIDENTS` is on, node-drain
///   publishes such a sandbox's disk artifact to S3, releases the node holding
///   its copy once those bytes are durable, and hands the row to the restore sweep
///   to re-place it `stopped` elsewhere — no suspend anywhere in it. That also
///   makes the "nor does anything publish the sandbox's disk" above true only
///   where the flag is off. `stopped` is deliberately still
///   absent from this list all the same: what holds the node long enough for
///   that to run is [`active_resident_predicate`] via node-drain's fourth gate
///   term, not this include-list, and adding it here would hold a node in every
///   environment that has not enabled the path with nothing to service it.
///
/// The node-drain reconciler's residency and evacuation queries read this, and
/// so must anything that predicts which nodes that reconciler can drain: a
/// predictor that disagrees with the reconciler is worse than no predictor at
/// all. An include-list is what keeps the two from drifting when a lifecycle is
/// added — an exclusion list silently admits the new value on one side only.
///
/// **This is not the only residency definition in the drain path, and the
/// others are not derived from it.** The drained-node terminator's
/// [`active_resident_predicate`] is still an exclusion list, so `created`,
/// `stopped`, `deleting`, and `quarantined` are resident to that gate and
/// non-resident here. That difference is deliberate for all four, and that
/// predicate's docs carry the per-lifecycle exit — reconciling the two lists is
/// its own change, with its own behaviour to justify. Until then, treat the
/// include-list as governing the `draining → drained` gate rather than every
/// stage of a drain. The module header records the orphaned-sandbox sweeper's
/// third definition for the same reason.
pub const RESIDENT_LIFECYCLES: &[SandboxLifecycle] = &[
    SandboxLifecycle::Creating,
    SandboxLifecycle::Starting,
    SandboxLifecycle::Started,
    SandboxLifecycle::Pausing,
    SandboxLifecycle::Paused,
    SandboxLifecycle::Resuming,
    SandboxLifecycle::Stopping,
    SandboxLifecycle::Suspending,
    SandboxLifecycle::Restoring,
    SandboxLifecycle::Checkpointing,
];

/// The wire forms of [`RESIDENT_LIFECYCLES`], for binding as a `text[]`
/// parameter. The conversion happens here, at the SQL boundary, so callers
/// never hold the vocabulary as strings.
pub fn resident_lifecycle_strs() -> Vec<&'static str> {
    RESIDENT_LIFECYCLES
        .iter()
        .map(|lifecycle| lifecycle.as_str())
        .collect()
}

/// Boolean SQL fragment: TRUE iff `{sandbox_alias}.lifecycle` is one of
/// [`RESIDENT_LIFECYCLES`].
///
/// The values are compile-time constants from this module, so they are inlined
/// as literals rather than bound — which keeps the fragment composable into
/// queries whose remaining parameter numbering it must not disturb. This
/// function and [`resident_lifecycle_strs`] are the only places the set becomes
/// text; everything upstream of them holds [`SandboxLifecycle`].
pub fn resident_lifecycle(sandbox_alias: &str) -> String {
    let values = RESIDENT_LIFECYCLES
        .iter()
        .map(|lifecycle| format!("'{}'", lifecycle.as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{sandbox_alias}.lifecycle IN ({values})")
}

/// The lifecycles in which a `sandboxes` row has stopped occupying the node its
/// `active_node_id` names — the exclusion list [`active_resident_predicate`]
/// renders.
///
/// `&[&str]` rather than `&[SandboxLifecycle]` because two of the four are not
/// enum variants and should not become ones: `deleted` is a tombstone written
/// by raw SQL in the internal API's status handler and pruned later by
/// `tombstone_retention`, and `cleanup_required` is a node-local
/// sandbox-manager state that reaches this column from nowhere. Adding variants
/// for either would force every `match` in the repo to handle a lifecycle the
/// cluster never assigns.
///
/// A constant rather than a literal inside the `format!` so this set is the same
/// kind of thing as the include-list it is deliberately not
/// ([`RESIDENT_LIFECYCLES`], in this module), and so the difference between them
/// can be computed rather than described.
pub const NON_RESIDENT_LIFECYCLES: &[&str] =
    &["failed", "cleanup_required", "deleted", "suspended"];

/// Renders [`NON_RESIDENT_LIFECYCLES`] as a `lifecycle NOT IN (…)` fragment for
/// the `sandboxes` row aliased `sandbox_alias`.
///
/// Exists so the list has exactly one renderer as well as one definition. Three
/// call sites format it, and they need three different node expressions — which
/// is why the seam is here rather than in [`active_resident_predicate`], whose
/// hard-coded `active_node_id = {node_expr}` none of the other two can use:
///
/// - [`active_resident_predicate`] here — a single node, `= $1`.
/// - `pplx_space_api`'s admin occupancy count — a batch, `ANY($1::UUID[])`.
///   Counts what an operator reads; the predicate above counts what refuses
///   them, and those two agreeing is the property this module exists to hold.
/// - `pplx_space_api`'s node-group guard — `IN (SELECT id FROM nodes WHERE
///   node_group_id = $1)`.
///
/// A second `map(format!).join(", ")` is enough surface for them to disagree
/// over quoting or ordering, which is the whole reason this is a function and
/// not three literals.
///
/// Pass the bare table name (`"sandboxes"`) when the query sets no alias.
pub fn non_resident_lifecycle_exclusion(sandbox_alias: &str) -> String {
    let non_resident = NON_RESIDENT_LIFECYCLES
        .iter()
        .map(|lifecycle| format!("'{lifecycle}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{sandbox_alias}.lifecycle NOT IN ({non_resident})")
}

/// Boolean SQL fragment: TRUE iff the row aliased `sandbox_alias` is a
/// genuinely-active, non-`suspended` sandbox still pinned through
/// `active_node_id` to the node named by `node_expr`.
///
/// This is the residency question asked at **both** gates a node passes on its
/// way out of the fleet, and it is deliberately an **exclusion** list rather
/// than node-drain's resident include-list ([`RESIDENT_LIFECYCLES`]). The
/// excluded set is
/// [`NON_RESIDENT_LIFECYCLES`]; `suspended` is the one whose exclusion is not
/// simply terminality — its pin runs through `target_node_id`, which
/// `pplx_space_sandbox_portability::snapshot_durable_for_source_release` and the
/// drain's snapshot gate answer instead.
///
/// The exclusion shape is the fail-closed one for a value neither list knows: a
/// lifecycle nobody has classified counts here, so it holds the node rather
/// than silently releasing it.
///
/// **`SPACE_NODE_DRAIN_TERMINATION_BLOCKING_GATE` reaches exactly one decision
/// among this predicate's readers: node-drain's `draining → drained` advance**
/// — both halves of it, the reconciler's pre-check and the completion write's
/// guard scope. Nothing else consults the flag, so it cannot decide whether a
/// node holding one of the four lifecycles below is *destroyed*; only whether
/// that node parks in `draining` or in `drained`, which is what the
/// reachability paragraph further down is about.
///
/// The ungated readers are what make that true, and some of them are
/// destructive: the drained-node terminator's `drained → terminating` flip, the
/// admin `POST :terminate` 409, and node-drain's pre-terminate recheck all
/// refuse on a match, and the terminator's KubeNodeGone reap **deletes** the
/// `nodes` row behind the same `resident_blocking_clause`. Two ungated readers
/// refuse nothing at all — node-drain's per-lifecycle gauge and the terminator's
/// `delete_refusal_diagnostics_query`, which exist so an environment can measure
/// this population before it holds anything on it. So neither "it refuses" nor
/// "the flag governs it" generalizes across the set; grep the callers rather
/// than trusting this list, which has grown before.
///
/// `created`, `stopped`, `deleting`, and `quarantined` are therefore resident
/// **here** and non-resident to that include-list. Naming all four separately is
/// the point: the drain pipeline services none of them, so "the drain will get
/// to it" is the wrong answer for every one, and their exits differ.
///
/// - `deleting` — converges on its own. The lifecycle claim is durable delete
///   intent and its completion removes the row, so the node releases with no
///   intervention.
/// - `quarantined` — held for as long as the investigation lasts. Recovery is
///   driven by the *owning* node republishing state, so evacuating the row would
///   destroy its only exit path. Refusing the node **is** the whole preservation
///   mechanism: nothing copies quarantined bytes anywhere, they survive because
///   the instance does.
/// - `stopped` — the same "refusing the node is the preservation", with no
///   investigation to end. Its disk is node-local (see [`RESIDENT_LIFECYCLES`]
///   for why no reconciler publishes or sweeps it) and the start path hard-pins
///   a `stopped` sandbox to its source node, so no restore can rebuild it
///   elsewhere. That pin is three hops and worth naming, because it is not
///   visible from any one of them: `stopped_restore_plan` returns a
///   `source_node_name` and never a `storage_uri`; `parse_pod_create_context`
///   routes a source node with no storage URI to `direct_node_name`; and the pod
///   builder renders `direct_node_name` as `spec.nodeName` — a bound pod, not a
///   scheduler preference. Two exits are the owner's — start it, which moves it into an
///   evacuable lifecycle, or delete it. A third is automatic but only when the
///   sandbox is configured for it: an armed auto-lifecycle `Delete` stage fires
///   from *any* state, because sandbox-manager's `Delete` arm — unlike the
///   `Suspend` and `Stop` arms beside it — carries no `SandboxState` guard. So
///   a node holding a `stopped` resident converges unattended only if that
///   resident happens to have a delete policy due; with none, it waits on a
///   human.
/// - `created` — like every lifecycle here it is counted only against the node
///   its `active_node_id` names, which for `created` means something placed the
///   row before it was ever started. Same exits as `stopped`, and nothing on the
///   node to preserve either way.
///
/// **Which phase the node is held in decides whether those exits are reachable.**
/// `NodeDrainState::blocks_placement` 409s a `start` on a node in `drained` or
/// `terminating` in either mode, and on a `draining` node only in forced mode. A
/// `stopped` or `created` resident whose node parked in `drained` is therefore
/// unreachable to its owner for as long as the node parks — which, since this
/// predicate refuses the terminator on every tick, is indefinitely. Delete stays
/// reachable throughout, and for a structural reason rather than a set-membership
/// one: the user-API lifecycle gate that would refuse it,
/// `controllers::sandbox::lifecycle::ensure_resolved_node_not_draining`,
/// dispatches on `SandboxAction` at all four of its call sites, and delete is
/// not a `SandboxAction` variant at all. Other `blocks_placement` readers exist
/// that take no `SandboxAction` — the service-route guard, the wake gate — but
/// none of them sits on the delete path. So the choice is not between reachable
/// and trapped: holding the node in gentle `draining` instead (node-drain's
/// fourth gate term, behind `SPACE_NODE_DRAIN_TERMINATION_BLOCKING_GATE`) is
/// what keeps the owner able to **keep** the sandbox by starting it, rather than
/// only to discard it. Under a forced drain it buys nothing here, since
/// `draining` blocks the `start` too.
///
/// **With the gate on**, both node-release gates count these rows, and that is
/// the point of the fourth term: a node this guard would refuse stays in
/// `draining`, where the stuck-drain warning and the metrics can see it, rather
/// than reaching `drained` where the terminator's UPDATE matches zero rows on
/// every tick and node-drain has stopped looking at it.
///
/// **With the gate off — the chart default — the advance does not read this
/// predicate at all.** The completion write renders the resident include-list
/// instead (`node_drain::store`'s `GuardScope::Resident`), deliberately, so the
/// flag is not a no-op at the SQL layer while the reconciler-side hold is
/// withheld. Such a node therefore does reach `drained` and parks at the
/// terminator. Do not read the paragraph above as describing both settings.
///
/// The census term ([`occupies_node_predicate`]) is what keeps the two gates in
/// lockstep on the *other* axis: a node whose drain converged only because the
/// census discounted its phantom `active_node_id` rows must not then be refused
/// here for those same rows. The discount direction is unchanged and
/// fail-closed — see the module docs.
///
/// See [`node_heartbeat_fresh`] on why `node_expr` wants a bound parameter.
pub fn active_resident_predicate(
    sandbox_alias: &str,
    node_expr: &str,
    evidence: ResidencyEvidence,
) -> String {
    let occupies = occupies_node_predicate(sandbox_alias, node_expr, evidence);
    let non_resident = non_resident_lifecycle_exclusion(sandbox_alias);
    format!(
        "(\n\
             {sandbox_alias}.active_node_id = {node_expr}\n\
             AND {non_resident}\n\
             AND {occupies}\n\
         )"
    )
}

/// How long a row's placement must have been settled before the census may be
/// read as proof that the node does not hold it.
///
/// A node publishes its complete sandbox inventory from a facet on a ~60 s
/// poll, separately from the status heartbeat, so a sandbox placed moments ago
/// is legitimately absent from the newest census while the heartbeat is fresh.
/// Ten minutes is ten of those cycles: long enough that ordinary lag, a
/// retried publish, or a node-ctl restart cannot manufacture a false absence,
/// short enough that it costs a drain one tick's worth of recently-placed rows.
///
/// This bounds the *lag* window only. A node whose inventory publish is wedged
/// for longer than this still reports an empty census for rows it holds — see
/// the module docs.
pub const CENSUS_PLACEMENT_SETTLE: Duration = Duration::from_mins(10);

/// Boolean SQL fragment: TRUE iff the sandbox row aliased `sandbox_alias` has
/// been placed long enough that a census omitting it means something.
///
/// A NULL `placement_changed_at` is treated as not-settled: unknown placement
/// age is not evidence, and the fail-closed reading of no evidence is "keep
/// counting".
pub fn placement_settled(sandbox_alias: &str, settle: Duration) -> String {
    format!(
        "(\n\
             {sandbox_alias}.placement_changed_at IS NOT NULL\n\
             AND {sandbox_alias}.placement_changed_at < NOW() - make_interval(secs => {})\n\
         )",
        settle.as_secs()
    )
}

/// Boolean SQL fragment: TRUE iff the node named by `node_expr` has a status
/// heartbeat that is both `reachable` and not yet stale.
///
/// FALSE when the node has no `node_status_snapshots` row at all, when the last
/// poll found it unreachable, or when `stale_after` has passed. Callers must
/// treat FALSE as "no evidence either way", never as "the node is empty".
///
/// `node_expr` is any SQL expression yielding the node's UUID. **Prefer a bound
/// parameter** (`"$1"`) over a per-row column reference: with a parameter
/// Postgres evaluates this once as an InitPlan, while a per-row `Var` forces a
/// hashed SubPlan over the whole `node_status_snapshots` relation on every
/// execution. The higher plan cost can also push the enclosing drain query
/// past `jit_above_cost`, adding JIT compilation overhead.
pub fn node_heartbeat_fresh(node_expr: &str) -> String {
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM node_status_snapshots rnss\n\
              WHERE rnss.node_id = {node_expr}\n\
                AND rnss.reachable\n\
                AND rnss.stale_after > NOW()\n\
         )"
    )
}

/// How stale a node's last accepted sandbox census may be and still count as
/// evidence about now.
///
/// `node_status_snapshots.sandbox_inventory_pruned_at` is stamped only in the
/// transaction that prunes `sandbox_observed_states` against a complete
/// inventory the server accepted, so its age is the age of the census itself —
/// the fact the heartbeat cannot supply.
///
/// Five of node-ctl's 60 s reconcile ticks, on which the sandbox-inventory facet
/// publishes a full census every time. Enough slack for a retried publish, a
/// node-ctl restart, or a few rejected inventories without a healthy node
/// flickering out of the discount; short enough that a publisher which has
/// genuinely stopped is distrusted within minutes. `NODE_STATUS_STALE_AFTER_SECS`
/// (30 s) is deliberately **not** reused: it is half a poll interval, so a
/// healthy node would read as "no census evidence" about half the time.
///
/// # Why this must not exceed [`CENSUS_PLACEMENT_SETTLE`]
///
/// Together the two terms have to exclude the case that motivates them: a live
/// sandbox placed *after* the node's last complete census, on a node whose
/// inventory publisher then wedged. Write `C` for the census timestamp, `P` for
/// the row's placement, `W` for this window and `S` for the settle window. That
/// case has `C < P`, and a discount needs `C > NOW - W` (census still trusted)
/// and `P < NOW - S` (placement settled), which together force `S < W`. So
/// while `W <= S` holds, such a row can never be discounted — the hole is
/// closed rather than merely narrowed. The assertion below is what keeps a
/// later edit to either constant from reopening it.
///
/// # Why the two terms are not collapsed
///
/// `sandbox_inventory_pruned_at > <row>.placement_changed_at` would express
/// "the census was taken after this sandbox was placed" directly, in one term.
/// It is not used because it correlates the `node_status_snapshots` subquery
/// with a per-row column: the fragment below keys on `node_expr` alone, which
/// Postgres hoists into a once-per-execution InitPlan, and a per-row `Var`
/// turns that back into a hashed SubPlan over the whole relation on every row.
/// Two uncorrelated terms buy the same guarantee at InitPlan cost.
pub const CENSUS_INVENTORY_MAX_AGE: Duration = Duration::from_mins(5);

const _: () = assert!(
    CENSUS_INVENTORY_MAX_AGE.as_secs() <= CENSUS_PLACEMENT_SETTLE.as_secs(),
    "CENSUS_INVENTORY_MAX_AGE must not exceed CENSUS_PLACEMENT_SETTLE, or a \
     sandbox placed after a node's last complete census can be discounted \
     while that node's inventory publisher is wedged"
);

/// Boolean SQL fragment: TRUE iff the node named by `node_expr` published a
/// sandbox census recently enough for its contents to mean something.
///
/// FALSE when the node has no `node_status_snapshots` row, when
/// `sandbox_inventory_pruned_at` is NULL (no complete inventory has ever been
/// accepted from it — the state of every row when the column shipped), and when
/// that timestamp is older than [`CENSUS_INVENTORY_MAX_AGE`]. All three are "no
/// census evidence", which callers must resolve toward keeping the sandbox
/// counted.
///
/// The explicit `IS NOT NULL` is redundant against the comparison, which yields
/// NULL and therefore not-TRUE on a NULL timestamp. It is spelled out because
/// the fail-closed reading of a never-published census must be intentional
/// rather than a by-product of three-valued logic — the same reason
/// [`placement_settled`] spells its own out.
///
/// See [`node_heartbeat_fresh`] on why `node_expr` wants a bound parameter.
pub fn node_census_fresh(node_expr: &str) -> String {
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM node_status_snapshots cnss\n\
              WHERE cnss.node_id = {node_expr}\n\
                AND cnss.sandbox_inventory_pruned_at IS NOT NULL\n\
                AND cnss.sandbox_inventory_pruned_at > NOW() - make_interval(secs => {})\n\
         )",
        CENSUS_INVENTORY_MAX_AGE.as_secs()
    )
}

/// Boolean SQL fragment: TRUE iff the node named by `node_expr` still reports
/// the sandbox aliased `sandbox_alias` in its published census.
///
/// Presence is deliberately tested without matching
/// `sandbox_observed_states.status` against the cluster lifecycle. A census row
/// in a *disagreeing* status is still the node saying it holds state for that
/// sandbox — the observed `suspended` / cluster `started` divergence, for
/// instance, means a local snapshot sits on that node's disk. Treating a status
/// mismatch as absence would discount exactly the rows whose only copy is
/// node-local.
///
/// A census row naming a different node is not presence: the table is keyed by
/// `sandbox_id`, so the node that published last owns the row, and this node's
/// own complete-inventory prune would have dropped it.
///
/// See [`node_heartbeat_fresh`] on why `node_expr` wants a bound parameter.
pub fn node_observes_sandbox(sandbox_alias: &str, node_expr: &str) -> String {
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM sandbox_observed_states rsos\n\
              WHERE rsos.sandbox_id = {sandbox_alias}.id\n\
                AND rsos.node_id = {node_expr}\n\
         )"
    )
}

/// Boolean SQL fragment: TRUE iff the sandbox row is **provably** no longer on
/// `node_expr` — the node answered recently, and its answer did not include
/// this sandbox.
///
/// Only ever safe to read in the TRUE direction; FALSE means "not proven
/// absent", which includes every case where the control plane cannot see the
/// node. See the module docs.
///
/// **Caller contract:** the enclosing query must already constrain the row's
/// placement to `node_expr` (`WHERE active_node_id = $1`, typically). This
/// fragment answers "is the sandbox on *that* node"; passing a `node_expr`
/// unrelated to the row's own placement asks a different question than the
/// name promises.
///
/// Four conjuncts, each removable only in the data-loss direction: the node
/// answered recently ([`node_heartbeat_fresh`]), the census it answered with is
/// itself recent ([`node_census_fresh`]), the row has been placed long enough
/// for that census to mean something ([`CENSUS_PLACEMENT_SETTLE`]), and the
/// census omitted the sandbox.
///
/// The heartbeat term is kept alongside the census term rather than replaced by
/// it. It costs almost nothing — every accepted publication, the census facet's
/// included, refreshes `stale_after`, so a node publishing censuses has a fresh
/// heartbeat by construction — and it keeps "the last poll found this node
/// unreachable" out of the discount, which is the direction this module is
/// asymmetric in.
pub fn node_proves_sandbox_absent(sandbox_alias: &str, node_expr: &str) -> String {
    let heartbeat_fresh = node_heartbeat_fresh(node_expr);
    let census_fresh = node_census_fresh(node_expr);
    let settled = placement_settled(sandbox_alias, CENSUS_PLACEMENT_SETTLE);
    let observed = node_observes_sandbox(sandbox_alias, node_expr);
    format!(
        "(\n\
             {heartbeat_fresh}\n\
             AND {census_fresh}\n\
             AND {settled}\n\
             AND NOT {observed}\n\
         )"
    )
}

#[cfg(test)]
mod tests;
