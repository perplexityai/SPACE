//! Periodic reconciler that establishes the org-scoped node-group identity of
//! the shared worker pool. Ships default-disabled.
//!
//! Each tick: seed the singleton default generic `node_groups` row (idempotent,
//! UUIDv7); label shared worker Nodes with [`NODE_GROUP_LABEL`].
//!
//! The seed is the one `node_groups` write in SPACE that does not go through
//! the admin API (`/admin/api/v1` on the shared host, `/v1` on the dedicated
//! admin host), and it has to exist before an operator could make it — nothing
//! resolves a `NULL` `nodes.node_group_id` until that row does. So it emits
//! its own entry on the `space.admin.audit` target, under the `system:` actor
//! namespace instead of a user (`record_node_group_system`), and only on the
//! tick that actually inserted — see [`EnsuredDefaultGroup`].
//!
//! It never writes `nodes.node_group_id`. That column has exactly one writer —
//! the `node_group_mirror` reconciler, which mirrors this label into it — and
//! that reconciler ships default-disabled too
//! (`SPACE_NODE_GROUP_MIRROR_ENABLED`). So enabling this one alone produces
//! labels and an untouched column: both have to be on for `node_group_id` to
//! populate.
//!
//! ## Why control-plane-side
//!
//! The label sits under the `node-restriction.kubernetes.io/` prefix, which the
//! NodeRestriction plugin forbids a **kubelet** credential from setting on its
//! own Node. A launch template passing it in `--node-labels` therefore can't work
//! — the plugin rejects the Node registration rather than dropping the label.
//! Patching each Node with control-plane credentials does work, and that is what
//! this reconciler does.
//!
//! EKS managed node-group labels can also set this protected key because EKS
//! reconciles them with a control-plane identity. A capacity-group label may
//! not match the SPACE node-group identity, however. Such a worker is reported
//! as `foreign_group`: this reconciler never overwrites an existing label, so
//! required node-group affinity can leave it unable to host sandboxes.
//!
//! Remove conflicting labels through the control plane that manages them
//! before enabling the reconciler. Creating database groups merely to match
//! those labels disables default-group labeling through the singleton gate
//! below. Removing a label declaration alone is insufficient; verify that the
//! change has reconciled onto the live Nodes.
//!
//! NodeRestriction protects against kubelet writes, not every worker-held
//! credential. A worker-resident ServiceAccount with unrestricted `patch nodes`
//! permission can still forge membership. Using required node-group affinity
//! as a tenant boundary also requires preventing those writes through RBAC or
//! admission policy; this reconciler cannot enforce that condition.
//!
//! ## Scope, and what it is not
//!
//! Labeling only runs while the default group is the **only** group (the gate
//! in [`Reconciler::tick`] — see [`NodeGroupSink::non_default_group_count`] for
//! why an unlabeled node is otherwise ambiguous). So this is a migration-era
//! rollout, not open-ended self-healing: once any other group exists, new shared
//! workers stop being labeled, and with required affinity live such a node hosts
//! no sandboxes. [`TickReport::label_suppressed`] counts them and is published
//! as a gauge for that reason. Closing it needs the per-node pool-intent signal
//! that ships with dedicated node-group provisioning.
//!
//! An existing label is never overwritten, so a label that is already wrong is
//! never corrected here — only a missing one is ever written.
//!
//! ## Ordering
//!
//! The node-groups schema must exist before this reconciler can seed the
//! default group and label workers. Enable `node_group_mirror` separately to
//! populate `nodes.node_group_id`. Every shared worker must carry the label
//! before the pod spec requires node-group affinity, or generic creates become
//! unschedulable.
//!
//! ## Seams
//!
//! Shaped after `orphaned_nodes`: [`NodeGroupSink`] for Postgres,
//! [`WorkerNodeLabels`] for kube (reflector scan, not a per-tick LIST).
//! Production impls in [`store`] and [`kube`]; fakes in tests.

use pplx_space_common_core::types::NodeGroupId;

use std::time::Duration;

use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;

use pplx_space_api::routes::admin::audit::{
    SYSTEM_ACTOR_NODE_GROUP_LABELER, SystemNodeGroupAuditEntry, record_node_group_system,
};
use pplx_space_api::routes::admin::node_groups::group_state_summary;
use pplx_space_common_core::types::{
    NodeGroupName, NodeGroupNameParseError, NodeGroupTemplatePolicy, PositiveDuration,
};

use crate::env_config;

#[cfg(feature = "kubernetes")]
pub mod kube;
pub mod store;
mod tick;

#[cfg(test)]
mod tests;

#[cfg(feature = "kubernetes")]
pub use kube::KubeWorkerNodes;
pub use store::NodeGroupDbStore;
pub use tick::Reconciler;

/// Node label that binds a kube Node to its SPACE node group, re-exported from
/// the canonical definition in `common-core`'s placement module.
///
/// Not defined here on purpose: `build_sandbox_pod` renders required
/// nodeAffinity over this same key, so the writer and the reader must be the
/// same string by construction rather than by two constants agreeing.
#[cfg(feature = "kubernetes")]
pub use pplx_space_common_core::kubernetes::SANDBOX_NODE_GROUP_LABEL as NODE_GROUP_LABEL;

/// Taint key carried by dedicated (org-owned) node-group nodes, re-exported
/// from `common-core`'s placement module. The value is the owning org's UUID.
///
/// Deliberately not `space.pplx.net/dedicated`, which is the cred-authority
/// fence: only one taint per key+effect is allowed, and a reconciler enumerating
/// the old key to reason about org ownership would read `cred-authority` and
/// could act on a fence needing the credmgr owner's approval.
///
/// Excluding tainted nodes is a **best-effort narrowing, not a guarantee** — the
/// taint is applied control-plane-side so it can land after a node registers,
/// leaving an untainted, label-less window. The `non_default_group_count` gate is
/// the safety property, not this check.
#[cfg(feature = "kubernetes")]
pub use pplx_space_common_core::kubernetes::DEDICATED_NODE_TAINT_KEY as DEDICATED_TAINT_KEY;

pub(crate) const DEFAULT_TICK: Duration = Duration::from_secs(30);
/// Label patches in flight at once. Independent single-object writes, which the
/// repo's async guidance says to fan out rather than `await` in a loop; 8 clears
/// a fleet in one pass at normal apiserver latency without becoming a burst.
///
/// Worst case is far lower and worth stating: with the defaults a tick attempts
/// at least `8 × floor(30s / 5s)` = **48** patches even if every one burns the
/// full [`LABEL_WRITE_TIMEOUT`], deferring the rest.
pub(crate) const DEFAULT_WRITE_CONCURRENCY: usize = 8;
/// Ceiling on [`ReconcilerConfig::write_concurrency`], applied by
/// [`clamp_write_concurrency`] everywhere the value is read.
///
/// Nothing rejects an absurd value on the way in: the field isn't read from
/// the environment, but it is `pub`. So a configured write concurrency of
/// 5,000 would fan 5,000 concurrent Node PATCHes at the apiserver from one
/// reconciler that has no rate limiter of its own — and first enablement is exactly when the pending list is the whole
/// fleet, so the fan-out would actually reach that width.
///
/// 8× the default, because a caller's legitimate reason to raise it is
/// widening a first-enablement window: throughput is `write_concurrency` ×
/// patches per second for as long as the phase's budget lasts (see
/// [`LABEL_WRITE_TIMEOUT`]), so 64 clears a four-figure fleet in a tick or two
/// at normal apiserver latency, while keeping a careless caller from turning a
/// rollout into a write storm.
pub(crate) const MAX_WRITE_CONCURRENCY: usize = 64;

/// Bound `write_concurrency` into `1..=`[`MAX_WRITE_CONCURRENCY`].
///
/// Both ends matter and for different reasons. A zero wedges rather than fails:
/// `buffer_unordered(0)` never fills its queue, so it returns `Pending` forever
/// with no waker and the tick hangs instead of erroring. The ceiling is the
/// apiserver's protection above. Called wherever the value is read, because
/// the field is `pub` and nothing validates it on the way in.
pub(crate) fn clamp_write_concurrency(value: usize) -> usize {
    value.clamp(1, MAX_WRITE_CONCURRENCY)
}

/// Ceiling on one Node label write.
///
/// Every patch is bounded by `min(LABEL_WRITE_TIMEOUT, time left in the label
/// phase)`, and `remaining` is re-read per node, so the phase does not run
/// longer than one `config.tick` and the rest defers — for as long as
/// [`WorkerNodeLabels::current_worker_node`] holds to its documented contract of
/// being a store read. `remaining` is snapshotted *before* that await and the
/// write budget computed from it afterwards, so an impl that turned it into a
/// network call would let a write overshoot the deadline by whatever the call
/// cost; that method's docs carry the reasoning.
///
/// Without a bound the only ceiling is the kube client's own read deadline —
/// `kube_client::Config`'s `DEFAULT_READ_TIMEOUT`, 295 s, set by every `Config`
/// constructor including the `Config::infer` that `common-svc`'s
/// `traced_kube_client_from_env` builds this process's client from, and applied
/// by `ClientBuilder`. Nothing in SPACE
/// overrides it, so one wedged patch would stall the rollout for that long.
///
/// This bounds the *phase*, not the tick: the budget is anchored when the phase
/// starts (after the seed, the store scan and the census) and the Postgres calls
/// have no timeout, so a slow database still stretches a tick past the interval.
/// Bounding the whole tick would let it eat the rollout's write budget instead.
///
/// A cancelled shutdown token tightens it further: `buffer_unordered` keeps
/// admitting the remaining futures, but each returns `NotAttempted` without
/// issuing a request, so all that is left to wait on is whatever is already in
/// flight — bounded by this constant concurrently rather than in series. See the
/// `biased;` comment in [`run`] for how that adds up against the supervisor's
/// drain budget.
const LABEL_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// How many failed/indeterminate patches log a per-node line in one tick;
/// beyond it the outcome is still counted, just not named.
///
/// Caps the *width* of a tick's diagnostics, not their duration: the budget is
/// re-armed every tick, so a persistent fleet-wide failure still emits this many
/// warns per tick for as long as it lasts (10 per 30s tick is ~28,800 a day).
/// Deliberate — unlike the standing conditions that got cross-tick baselines
/// (`suppressed_last_tick`, `foreign_group_last_tick`), a fleet-wide patch
/// failure is an active incident where a recurring signal is the point. What the
/// cap prevents is one line *per worker*: on a 1,000-node fleet that is the
/// difference between 10 lines a tick and 1,000.
const LABEL_FAILURE_LOGGED_NODES: usize = 10;

/// Per-tick budget for the "dedicated-tainted node carries the DEFAULT label"
/// warn, kept separate from [`LABEL_FAILURE_LOGGED_NODES`]. That condition is
/// standing and never reconciled away, so one shared budget lets it consume
/// every slot before the patch phase runs — leaving `label_errors` climbing with
/// no line carrying the cause, on the one rollout where the cause is what you
/// need.
const DEDICATED_MISLABEL_LOGGED_NODES: usize = 5;

/// How many node names the gate-race error log names explicitly. The alert
/// asks for a label audit, so the names are the useful part — but a
/// full-fleet tick could otherwise emit thousands in one line, so the rest
/// are reported as a count.
const GATE_RACE_LOGGED_NODES: usize = 20;
/// How many newly-mislabeled node names the foreign-group warn lists before
/// falling back to a count. Same reasoning as [`GATE_RACE_LOGGED_NODES`]: the
/// names are the actionable part — the alternative is a fleet-wide
/// `kubectl get nodes -L node-restriction.kubernetes.io/space-node-group
/// --context <cluster-context>` — but a whole MNG joining at once must not emit
/// an unbounded line.
const FOREIGN_GROUP_LOGGED_NODES: usize = 20;
/// Name seeded for the default generic node group: it maps the existing
/// shared `workers` pool (`space.pplx.net/node-pool=workers`). Only used
/// when no default group row exists yet — once seeded, the row is the
/// source of truth and config/env changes do not rename it.
pub(crate) const DEFAULT_GROUP_NAME: &str = "workers";

/// `action` on the seed's `space.admin.audit` entry.
///
/// Follows the `node-group-<verb>` shape the admin node-group handlers already
/// emit (`node-group-create`, `node-group-assign-org`,
/// `node-group-set-default`) so a consumer filtering that vocabulary sees the
/// seed too. A dotted
/// `node_group.seed_default` would sort outside the family it belongs to.
const SEED_DEFAULT_AUDIT_ACTION: &str = "node-group-seed-default";

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Master enable gate. Off by default so the reconciler is inert until
    /// the operator explicitly opts in via
    /// `SPACE_NODE_GROUP_LABELER_ENABLED=true` (after the node-groups
    /// schema migration is applied — see module docs).
    pub enabled: bool,
    /// Interval between ticks, and the budget for the label phase within one.
    /// `PositiveDuration` because `tokio::time::interval` panics on zero and
    /// this field is `pub`: the same "one careless caller away" reasoning that
    /// earned `write_concurrency` its clamp, except the failure here kills the
    /// spawned task rather than wedging it.
    pub tick: PositiveDuration,
    /// Name to seed the default generic group with when no default exists
    /// yet. Once a default row exists, the row's name wins and this value
    /// is ignored. A [`NodeGroupName`], so an invalid name cannot reach a
    /// Node label from here.
    pub default_group_name: NodeGroupName,
    /// Label patches issued concurrently. See [`DEFAULT_WRITE_CONCURRENCY`],
    /// and [`clamp_write_concurrency`] for the bounds every reader applies —
    /// this field is `pub`, so neither end is enforced here.
    pub write_concurrency: usize,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick: PositiveDuration::new_panic(DEFAULT_TICK),
            // A compile-time constant known to satisfy the grammar; the
            // expect documents that rather than pushing a Result onto every
            // caller of `default()`.
            default_group_name: NodeGroupName::parse(DEFAULT_GROUP_NAME)
                .expect("DEFAULT_GROUP_NAME is a valid node-group name"),
            write_concurrency: DEFAULT_WRITE_CONCURRENCY,
        }
    }
}

impl ReconcilerConfig {
    /// Read the surviving master gate and use fixed reconciler settings.
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            enabled: env_config::truthy_default(
                "SPACE_NODE_GROUP_LABELER_ENABLED",
                default.enabled,
            ),
            ..default
        }
    }
}

/// The singleton default generic node group, as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultNodeGroup {
    pub id: NodeGroupId,
    /// `node_groups.name` — the value written to [`NODE_GROUP_LABEL`].
    /// Parsed at the SQL boundary, so a row created out-of-band with an
    /// invalid slug surfaces as [`EnsureDefaultGroupError::InvalidName`]
    /// instead of reaching a Node label.
    pub name: NodeGroupName,
}

/// How [`NodeGroupSink::ensure_default_group`] resolved the default group:
/// this call created the row, or found one already there.
///
/// The distinction exists for the admin audit trail. Seeding is a durable
/// management-state write — it decides what every `NULL`
/// `nodes.node_group_id` resolves to, cluster-wide — so it owes an entry on
/// `space.admin.audit` exactly as an operator's node-group create through the
/// admin API does. Resolving an existing group owes
/// nothing: every steady-state tick does it, so auditing that would post an
/// event every [`DEFAULT_TICK`], forever.
///
/// An enum rather than `(DefaultNodeGroup, bool)`: the variant names say which
/// case a call site is handling, `match` makes forgetting one a compile error,
/// and a bare bool that decides whether an audit event fires is exactly the
/// kind that gets read backwards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnsuredDefaultGroup {
    /// This call inserted the row: the bootstrap write, and the only variant
    /// that is audit-worthy. Normally once per cluster — a re-seed happens
    /// only if the default row is deleted while it is the sole group, and that
    /// re-seed is a new write that owes its own entry.
    Seeded(DefaultNodeGroup),
    /// A default already existed: the lock-free fast path, or the re-read
    /// under the seed lock after another replica won the race. Steady state.
    Existing(DefaultNodeGroup),
}

impl EnsuredDefaultGroup {
    /// The resolved group, however it was obtained. Everything downstream of
    /// the audit emission treats both variants identically — the group's name
    /// is the label value either way.
    pub fn into_group(self) -> DefaultNodeGroup {
        match self {
            Self::Seeded(group) | Self::Existing(group) => group,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EnsureDefaultGroupError {
    #[error("db error: {0}")]
    Db(#[from] sqlx::Error),
    /// No default exists and the seed INSERT conflicted. Needs an operator
    /// decision, so retrying won't clear it.
    ///
    /// Unreachable by construction, kept as a fail-closed arm: the seed holds
    /// `SHARE ROW EXCLUSIVE` across both under-lock re-reads and the INSERT, so
    /// no writer can commit a colliding group in that window. If it ever fires,
    /// the cause is a constraint added since — not a group an operator can find
    /// and rename, which is what the message used to send them looking for.
    #[error(
        "seeding the default node group '{name}' hit a name conflict the under-lock check \
         should have excluded — suspect a constraint added since, not a group to rename"
    )]
    NameConflict { name: String },
    /// The stored default row's `name` does not satisfy the node-group slug
    /// grammar, so it cannot be used as a Node label value. Only reachable
    /// via an out-of-band write (the table has its own CHECK on `name`), and
    /// it needs an operator fix — retrying won't help.
    #[error("stored default node group name '{name}' is not a valid node-group name: {source}")]
    InvalidName {
        name: String,
        #[source]
        source: NodeGroupNameParseError,
    },
    /// No default group exists, but other groups do — so seeding one now would
    /// silently reassign every un-grouped node. `nodes.node_group_id IS NULL`
    /// resolves to the default group, so creating that row retroactively
    /// changes what every `NULL` means, cluster-wide, with no operator action.
    /// Which group should be the default is a decision, not a guess: needs
    /// `:set-default` on an existing group, or an explicit accepted seed.
    #[error(
        "no default node group exists but {other_groups} non-default group(s) do; \
         refusing to seed a default because that would reassign every un-grouped node"
    )]
    SeedBlockedByExistingGroups { other_groups: i64 },
}

/// Postgres-side surface. Production impl: [`NodeGroupDbStore`].
pub trait NodeGroupSink: Send + Sync + 'static {
    /// Return the singleton default generic group, seeding it (UUIDv7,
    /// `template_policy = 'default'`, `is_default = TRUE`, no org) under
    /// `seed_name` if no default exists yet. An existing default row wins
    /// over `seed_name`.
    ///
    /// The returned [`EnsuredDefaultGroup`] must report `Seeded` **only** when
    /// this call performed the INSERT. Every other success — including
    /// finding a default that another replica seeded moments earlier — is
    /// `Existing`. The audit emission in [`Reconciler::tick`] reads nothing
    /// else, so an impl that reports `Seeded` for a read would post an event
    /// per tick.
    fn ensure_default_group(
        &self,
        seed_name: &NodeGroupName,
    ) -> impl std::future::Future<Output = Result<EnsuredDefaultGroup, EnsureDefaultGroupError>> + Send;

    /// Number of `node_groups` rows other than the singleton default — the
    /// labeling safety gate.
    ///
    /// An unlabeled, selector-matched, untainted node is only unambiguously a
    /// default-pool worker while the default is the **only** group: another
    /// generic group's freshly-joined nodes look identical, and so does a
    /// dedicated node whose taint hasn't landed. Non-zero suppresses labeling
    /// rather than guessing — see the gate in [`Reconciler::tick`].
    fn non_default_group_count(
        &self,
    ) -> impl std::future::Future<Output = Result<i64, sqlx::Error>> + Send;
}

/// One worker Node as seen on the kube side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerNode {
    pub name: String,
    /// `metadata.uid` — the kube Node object's identity, and the key the
    /// cross-tick baselines (`suppressed_last_tick`, `foreign_group_last_tick`)
    /// are held under instead of the name. EKS derives node names from private
    /// IPs, which the VPC reuses, so a replacement instance can carry a retired
    /// one's node name: keyed on name, a 1:1 replacement would look like the
    /// same node and go unreported.
    pub uid: String,
    /// `metadata.resourceVersion` as of whichever read produced this value.
    /// Sent back as an optimistic-concurrency precondition on the label patch so
    /// the "label only if it had no node-group label" decision can't race a
    /// concurrent labeler (e.g. the operator flow assigning a dedicated group
    /// between our read and our patch): any intervening change to the Node bumps
    /// the resourceVersion and the patch fails with a conflict instead of
    /// overwriting.
    ///
    /// The version the patch carries comes from
    /// [`WorkerNodeLabels::current_worker_node`], re-read immediately before the
    /// write — **not** from the tick-opening snapshot. The snapshot's version is
    /// up to a whole `config.tick` old, and every unrelated Node write in that
    /// window (`node_ctl`'s template-presence labeler patches Node labels from a
    /// worker-resident DaemonSet, fleet-wide) would fail the precondition. Those
    /// 409s are indistinguishable from an RBAC 403 in
    /// `label_write{outcome:error}`, so the narrow read keeps the precondition
    /// meaning "someone assigned a group" rather than "someone touched the
    /// Node".
    pub resource_version: String,
    /// Current [`NODE_GROUP_LABEL`] value, if any.
    pub node_group: Option<String>,
    /// Whether the node carries the [`DEDICATED_TAINT_KEY`] taint.
    /// Dedicated nodes are owned by the operator dedicated-group flow
    /// and are never touched by this reconciler — in particular not
    /// while they sit in the freshly-joined window where they match the
    /// worker selector but don't yet carry their dedicated group label.
    pub dedicated: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerNodeLabelsError {
    #[error("kube node operation failed: {0}")]
    Backend(String),
}

/// Kube-side surface. Production impl: [`KubeWorkerNodes`].
pub trait WorkerNodeLabels: Send + Sync + 'static {
    /// List the Nodes of the shared sandbox-eligible worker pool (the
    /// sandbox node-selector label) with their current node-group label.
    fn list_worker_nodes(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<WorkerNode>, WorkerNodeLabelsError>> + Send;

    /// Re-read one Node from the same source [`Self::list_worker_nodes`]
    /// scanned, as of *now*, so the patch's precondition is a fresh
    /// resourceVersion rather than the tick-opening snapshot's.
    ///
    /// `None` means "this node is no longer the thing the snapshot classified"
    /// and the patch is skipped rather than issued blind:
    /// - it is gone from the store (the Node object was deleted), or
    /// - the object under that name is a *different* one — `uid` differs, which
    ///   is a replacement instance reusing a retired node's name (EKS derives
    ///   node names from VPC-reused private IPs), so this tick never classified
    ///   it, or
    /// - it no longer matches the worker selector.
    ///
    /// A cache read, not an apiserver call: the production impl reads the same
    /// reflector store, so this is not bounded by the per-write timeout and does
    /// not add apiserver load. An impl that made this a network call would put an
    /// unbounded await inside the label phase — and would break the phase's
    /// clamp as well, not merely lengthen it: `label_one_node` snapshots the
    /// time left in the phase *before* this call and computes the write budget
    /// (`min(LABEL_WRITE_TIMEOUT, remaining)`) from that snapshot afterwards, so
    /// whatever this await costs is time the budget still believes it has. The
    /// write could then overshoot the phase deadline by that much, which is
    /// exactly the bound `t38_label_phase_is_clamped_to_the_tick_not_the_write_timeout`
    /// pins. Both consequences are why this seam must stay a store read; the
    /// order is not worth restructuring while it is one.
    fn current_worker_node(
        &self,
        node: &WorkerNode,
    ) -> impl std::future::Future<Output = Option<WorkerNode>> + Send;

    /// Set [`NODE_GROUP_LABEL`]` = value` on one Node. Only ever called for
    /// nodes that had no node-group label as of the [`Self::current_worker_node`]
    /// read immediately preceding it; the patch must carry
    /// `node.resource_version` as a precondition so a concurrent label by another
    /// actor landing in the sliver between that read and this write fails the
    /// patch (conflict) rather than being overwritten — the next tick re-lists
    /// and re-evaluates.
    fn label_node(
        &self,
        node: &WorkerNode,
        value: String,
    ) -> impl std::future::Future<Output = Result<(), WorkerNodeLabelsError>> + Send;
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    /// Worker Nodes returned by the list.
    pub workers: usize,
    /// Nodes that were missing the label and got it this tick.
    pub labeled: usize,
    /// Nodes already labeled with the default group.
    pub already_labeled: usize,
    /// Nodes labeled with a *different* group. Left alone.
    pub foreign_group: usize,
    /// Nodes that *newly* appeared in [`Self::foreign_group`] this tick while
    /// the default was the only group — i.e. labeled with a group that does not
    /// exist. What decides whether the warn fires, since the condition is
    /// standing; the log reads this field so the two cannot diverge.
    pub newly_foreign_group: usize,
    /// Nodes carrying the dedicated taint — out of this reconciler's
    /// scope entirely (label state owned by the operator dedicated-group
    /// flow), including the freshly-joined window where they have no
    /// node-group label yet.
    pub skipped_dedicated: usize,
    /// The **subset** of [`Self::skipped_dedicated`] that carries the *default*
    /// group's label — dedicated hardware advertising itself as shared pool.
    /// Not a replacement for that counter: every node here is also counted
    /// there, so the two are never added together.
    ///
    /// The one condition this reconciler's own warn calls "needs operator
    /// attention", and the one it can't repair — a label is written only when
    /// absent, so a wrong one is never corrected. The warn is rationed to
    /// [`DEDICATED_MISLABEL_LOGGED_NODES`] names per tick, so on a fleet with
    /// dozens of them the log names a few and the gauge built on this field is
    /// the only signal for the rest.
    pub mislabeled_dedicated: usize,
    /// Per-node label patch failures, timeouts included (tick continues
    /// with the rest).
    pub label_errors: usize,
    /// Nodes whose patch was never issued: the label phase's budget ran out
    /// first, the shutdown token had fired, or the pre-write re-read contradicted
    /// the snapshot. Not an error, and nothing changed server-side — they are
    /// simply re-evaluated on the next tick, by this process or by whichever
    /// replica takes the lease. See [`LABEL_WRITE_TIMEOUT`].
    ///
    /// The total of all three reasons, including
    /// [`Self::label_deferred_shutdown`] and [`Self::label_superseded`].
    pub label_deferred: usize,
    /// The subset of [`Self::label_deferred`] whose turn came after the shutdown
    /// token was cancelled, rather than after the phase's budget was spent.
    ///
    /// The two are indistinguishable in `label_deferred` and in the
    /// `node_group_labeler.deferred_nodes` gauge, and they call for opposite
    /// responses: a spent budget is the rollout back-pressuring against
    /// apiserver latency, and a sustained one means waiting more ticks (or a
    /// wider `write_concurrency` from a programmatic caller; it isn't read from
    /// the environment), while a shutdown deferral is an orderly wind-down
    /// that the next leader resumes with no operator action at all.
    ///
    /// A report field and a summary-log field, deliberately not a metric: the
    /// tick that sets this is the last one the process runs, so a gauge would
    /// carry one point and then go stale with the pod.
    pub label_deferred_shutdown: usize,
    /// The subset of [`Self::label_deferred`] whose pre-write re-read
    /// contradicted the snapshot — see [`DeferReason::Superseded`]. Nodes leaving
    /// the fleet, or being assigned a group or the dedicated taint, mid-tick.
    ///
    /// A report field and a summary-log field rather than a metric, like
    /// [`Self::label_deferred_shutdown`], but for the opposite reason: this one
    /// is routine and self-clearing. The next tick rebuilds the pending list from
    /// a fresh snapshot, where such a node is either absent or classified as
    /// `already_labeled` / `foreign_group` / `skipped_dedicated` — so it cannot
    /// sustain, and the gauge it feeds (`deferred_nodes`) is alerted on for
    /// *sustained* non-zero.
    pub label_superseded: usize,
    /// Patches that were issued but whose outcome we never learned (client
    /// timeout). The label may or may not have been written, so these force
    /// the post-label census re-check exactly as a success would.
    pub label_indeterminate: usize,
    /// Unlabeled nodes the tick refused to touch because a non-default
    /// node group exists, so "unlabeled" no longer implies "default pool".
    /// See [`NodeGroupSink::non_default_group_count`].
    pub label_suppressed: usize,
    /// Names of the nodes this tick patched **and got an ack for**. `Failed`
    /// and `Indeterminate` nodes were also patched and may have landed a label
    /// (see [`LabelOutcome`]), so this is the confirmed set, not the whole
    /// write set — which is why the log field is called `confirmed_nodes`.
    ///
    /// On an audit-owing tick these are the nodes to inspect first — the
    /// already-labeled majority carries a label this tick did not write and is
    /// not in doubt. Left in `buffer_unordered` completion order, so a
    /// truncated audit list is not stable run to run.
    pub newly_labeled: Vec<String>,
    /// Whether this tick produced a fleet measurement it can stand behind.
    /// Gates gauge publication. Three paths converge on `false`:
    /// - an early return at or before the store scan — a seed failure, which
    ///   returns before it, or the scan itself failing, which returns at it —
    ///   carries `default()` in every remaining field, and publishing those
    ///   zeros would overwrite a true standing value, silencing a
    ///   "sustained > 0" monitor for as long as the failure persists;
    /// - the *pre*-write census failure returns after the scan, so its counts
    ///   are real, but the gate they would be read against is unknown;
    /// - the *post*-write census arms likewise carry measured counts, but the
    ///   census they were classified against is the one in doubt.
    ///
    /// Counters publish either way.
    ///
    /// A tick whose label phase wound down for shutdown is still a measurement:
    /// the fleet counts were taken before the phase and only
    /// [`Self::label_deferred`] is elevated. This field is about whether the
    /// census those counts were classified against still holds, not about how
    /// much of the write work got done.
    pub completed: bool,
    /// Non-default node groups observed by this tick's gate. Zero is the only
    /// value in which any label is written, so this is the gate's state.
    pub other_groups: i64,
    /// Nodes that entered the suppressed set *this* tick — the actionable
    /// part of [`Self::label_suppressed`], and what decides whether the
    /// suppression is re-reported at error level. Exposed so the
    /// membership-vs-cardinality distinction is assertable.
    pub newly_suppressed: usize,
    /// Set when the post-write re-check ended the tick for a reason that owes
    /// **no** audit: on a tick that wrote nothing, either a non-default group
    /// appeared or the re-read failed outright. Either way the census these
    /// counts were classified against can no longer be relied on, and no label
    /// can be in doubt — hence `unusable` rather than `changed`, which only
    /// describes the first arm. Distinct from [`Self::gate_race_detected`]
    /// because that one's metric contract promises a human label audit on every
    /// occurrence — emitting it here would raise a tenant-isolation alarm during
    /// ordinary group creation.
    pub census_unusable_no_writes: bool,
    /// Set when this tick wrote labels and a non-default group appeared between
    /// the two census reads, so the gate cannot be confirmed to have held. The
    /// labels stand and cannot be attributed, so this owes an operator label
    /// audit. Only ever set on a tick that issued at least one patch; the
    /// re-check's *failed read* books [`Self::census_after_write_errors`]
    /// instead, since no group was observed to appear.
    pub gate_race_detected: bool,
    /// **Transient** seed failures that ended the tick: the `Db` arm alone.
    /// Distinct from [`Self::census_errors`] because this is the counter that
    /// carries the *seed*'s database failures, first-enablement's missing
    /// node-groups migration among them, and folding a census failure in would
    /// send an operator to the schema for a problem that isn't there.
    ///
    /// The arm is wider than that one cause, and the tick's warn says so: a
    /// `lock_timeout`/`statement_timeout` expiry while another writer holds
    /// `node_groups` (seeding path only — the steady-state fast path takes no
    /// lock) and an ordinary connection/pool failure land here too. Telling
    /// them apart needs the error string, not the counter.
    ///
    /// Distinct from [`Self::seed_blocked_errors`] for the same reason read the
    /// other way: this counter climbing means look at the database — starting
    /// with the migration — and that one climbing means the schema is fine and
    /// an operator has to decide something.
    pub ensure_errors: usize,
    /// Seed failures that ended the tick and will **never** clear on their own:
    /// `SeedBlockedByExistingGroups`, `InvalidName`, `NameConflict`. Published
    /// as `tick_error{kind:seed_blocked}` so a monitor can page on the arm that
    /// needs a human without also firing for the environment that simply has
    /// not run the migration yet.
    pub seed_blocked_errors: usize,
    /// Non-default-group census failures on a tick that wrote **nothing** —
    /// the pre-labeling gate read, or the post-label re-check on a quiet tick.
    /// Nothing was written when this is non-zero.
    pub census_errors: usize,
    /// Post-label census failures on a tick that **did** write labels, so the
    /// gate cannot be confirmed to have held. Kept separate from
    /// [`Self::census_errors`] because that counter's contract — in its doc and
    /// in the metrics catalog — is "nothing was written", and from
    /// [`Self::gate_race_detected`] because no group was actually observed to
    /// appear. Owes the same operator label audit as a race.
    pub census_after_write_errors: usize,
    /// Node-list failures (a cold reflector included). Published as
    /// `tick_error{kind:list}`; the tick early-returns, so no gauge is emitted
    /// alongside it — a cold cache must not read as an empty fleet.
    pub list_errors: usize,
}

/// Result of one node's label patch attempt.
///
/// The distinction that matters is **did a request reach the apiserver**, not
/// whether it succeeded. A client-side timeout cancels our future but says
/// nothing about the server: the patch may well have been applied. So a
/// timeout is [`Self::Indeterminate`], not a failure, and the post-label
/// census re-check has to treat it as "a label may have landed".
enum LabelOutcome {
    /// The apiserver acknowledged the patch. Carries the node name, for the
    /// audit diagnostics.
    Labeled(String),
    /// The request was issued and returned an error — usually meaning no label
    /// was written, but **not** proof of it: the kube seam flattens every
    /// `kube::Error` into one variant, so a transport reset or a 5xx following a
    /// commit lands here too. Counts as "a label may have landed" for the
    /// post-label re-check, same as [`Self::Indeterminate`].
    Failed,
    /// The request was issued but we never learned its fate — the client-side
    /// bound elapsed first, whether that was [`LABEL_WRITE_TIMEOUT`] or what
    /// was left of the tick. The label may or may not exist now, and the next
    /// tick re-reads the node to find out.
    Indeterminate,
    /// No request was issued: the tick budget was already spent, shutdown had
    /// begun, or the fresh read contradicted the snapshot, when this node's turn
    /// came. Nothing can have changed server-side.
    NotAttempted(DeferReason),
}

/// Why a node's patch was never issued.
///
/// Every arm defers the node to a later tick and every arm counts in
/// [`TickReport::label_deferred`]; they are told apart because they mean
/// different things to an operator — see
/// [`TickReport::label_deferred_shutdown`] and
/// [`TickReport::label_superseded`].
enum DeferReason {
    /// The label phase's budget — `min(config.tick, ...)` anchored when the
    /// phase started — was spent.
    BudgetSpent,
    /// The shutdown token was cancelled, so the phase is winding down inside
    /// the supervisor's drain budget.
    Shutdown,
    /// The re-read immediately before the write contradicted the snapshot this
    /// node was classified from: it is gone from the store (or is a different
    /// object under the same name), it no longer matches the worker selector
    /// (relabeled out of the sandbox pool mid-tick), it has acquired a
    /// node-group label since, or it has acquired the dedicated taint since. In
    /// every case the snapshot's premise — "an unlabeled, untainted shared
    /// worker" — no longer holds, so there is nothing to write.
    Superseded,
}

/// Publish one tick's report.
///
/// Gauges for the standing conditions, counters for the things that happen
/// once. The split matters: "N workers will never be labeled" persists until
/// an operator acts, so a monitor over a counter (or a log line) fires once
/// and recovers while the capacity is still dark.
fn publish_metrics(report: &TickReport) {
    use pplx_space_metrics::node_group_labeler as m;

    // Gauges describe state, so they may only be published by a tick that
    // actually measured it. An early return carries `default()` in every
    // remaining field, and republishing those zeros would silence the very
    // monitors these gauges exist for — see `TickReport::completed`. A tick
    // that bailed leaves the last good values standing and skips its
    // `tick_completed` increment, which is what makes staleness detectable.
    if report.completed {
        m::tick_completed();
        m::workers(report.workers);
        m::labeled_nodes(report.labeled + report.already_labeled);
        m::suppressed_nodes(report.label_suppressed);
        m::deferred_nodes(report.label_deferred);
        m::foreign_group_nodes(report.foreign_group);
        m::dedicated_skipped_nodes(report.skipped_dedicated);
        // Inside the gate with the other state gauges, deliberately: this is a
        // standing condition alerted on as "sustained > 0", so a zero from a
        // tick that never measured the fleet would silence it for as long as
        // the failure lasts.
        m::dedicated_mislabeled_nodes(report.mislabeled_dedicated);
        m::other_groups(report.other_groups);
    }

    m::label_writes(m::LabelWriteOutcome::Applied, report.labeled as u64);
    m::label_writes(m::LabelWriteOutcome::Error, report.label_errors as u64);
    m::label_writes(
        m::LabelWriteOutcome::Indeterminate,
        report.label_indeterminate as u64,
    );

    m::tick_errors(m::TickErrorKind::Seed, report.ensure_errors as u64);
    m::tick_errors(
        m::TickErrorKind::SeedBlocked,
        report.seed_blocked_errors as u64,
    );
    m::tick_errors(m::TickErrorKind::Census, report.census_errors as u64);
    m::tick_errors(
        m::TickErrorKind::CensusAfterWrite,
        report.census_after_write_errors as u64,
    );
    m::tick_errors(m::TickErrorKind::List, report.list_errors as u64);

    if report.gate_race_detected {
        m::gate_race_detected();
    }
}

/// Drive the reconciler loop. Returns when `shutdown_token` fires.
///
/// When `config.enabled = false` (the default), logs the inert state once
/// and waits on the shutdown token without ever running a tick — same shape
/// as `orphaned_nodes::run`.
pub async fn run<S: NodeGroupSink, L: WorkerNodeLabels>(
    config: ReconcilerConfig,
    sink: S,
    labels: L,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        // Deliberately does not name a cause: besides an unset or false
        // SPACE_NODE_GROUP_LABELER_ENABLED, a programmatic `ReconcilerConfig`
        // can disable it, and claiming "unset" would mislead that caller.
        tracing::info!(
            "node-group-labeler reconciler is disabled; waiting on shutdown \
             without running any ticks"
        );
        shutdown_token.cancelled().await;
        tracing::info!("node-group-labeler reconciler stopping");
        return;
    }

    tracing::info!(
        tick_secs = config.tick.get().as_secs(),
        default_group_name = %config.default_group_name,
        // The clamped width, which is readable nowhere else: there is no
        // gauge and no endpoint for it, and `clamp_write_concurrency` reduces
        // an over-large configured value in silence, so without this line an
        // over-large value looks like it took effect.
        write_concurrency = clamp_write_concurrency(config.write_concurrency),
        "starting node-group-labeler reconciler"
    );
    let reconciler = Reconciler::new(config.clone(), sink, labels, shutdown_token.clone());
    let mut interval = tokio::time::interval(config.tick.get());
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            // Shutdown first: both branches are ready whenever a tick overruns
            // its period, and an unbiased `select!` could start another full
            // tick — up to a whole `tick` of writes plus untimed Postgres — past
            // the supervisor's drain window. Siblings leave theirs unbiased;
            // their ticks are short enough not to matter.
            //
            // This branch stops the loop from starting another tick. The tick
            // already in flight is not dropped: it holds a clone of this same
            // token and winds itself down through its normal exits — the label
            // phase's `buffer_unordered` keeps admitting its remaining futures,
            // but each returns without issuing a request and books
            // `label_deferred_shutdown`, and the post-write census re-check and
            // the return below still run. Dropping the future on the token
            // instead would abandon it at an arbitrary await point and skip that
            // re-check, which is what makes a gate race auditable.
            //
            // So what a shutdown still has to wait for is the patches already in
            // flight — each bounded by `LABEL_WRITE_TIMEOUT` (5s), and
            // concurrently, so at most one such bound in wall time — plus the
            // untimed Postgres calls: the seed and the two census reads. That is
            // what has to fit inside `reconcilers::SHUTDOWN_DRAIN_TIMEOUT`
            // (10s), which is the binding constraint here rather than the pod's
            // termination grace period (Kubernetes' 30s default; the chart sets
            // none): overrunning it makes the controller *abandon* its leader
            // lease instead of resigning it, so every singleton reconciler in
            // the process stays stopped until the lease TTL expires. A wedged
            // Postgres is the one thing left that can still overrun it, and it
            // overruns every reconciler in the process alike.
            biased;

            _ = shutdown_token.cancelled() => {
                break;
            }

            // The interval's immediate first fire is deliberate: a fresh
            // deploy should label any waiting nodes right away — once pod
            // specs carry required node-group affinity, an unlabeled
            // worker serves no sandboxes.
            _ = interval.tick() => {
                let report = reconciler.tick().await;
                // Emitted unconditionally: a cold reflector or a mis-selected
                // pool produces an all-zero tick, and suppressing those made
                // "is it running and what does it see?" unanswerable.
                publish_metrics(&report);
                tracing::info!(
                    workers = report.workers,
                    labeled = report.labeled,
                    already_labeled = report.already_labeled,
                    foreign_group = report.foreign_group,
                    newly_foreign_group = report.newly_foreign_group,
                    skipped_dedicated = report.skipped_dedicated,
                    mislabeled_dedicated = report.mislabeled_dedicated,
                    label_errors = report.label_errors,
                    label_deferred = report.label_deferred,
                    label_deferred_shutdown = report.label_deferred_shutdown,
                    label_superseded = report.label_superseded,
                    label_indeterminate = report.label_indeterminate,
                    label_suppressed = report.label_suppressed,
                    newly_suppressed = report.newly_suppressed,
                    gate_race_detected = report.gate_race_detected,
                    census_unusable_no_writes = report.census_unusable_no_writes,
                    ensure_errors = report.ensure_errors,
                    seed_blocked_errors = report.seed_blocked_errors,
                    census_errors = report.census_errors,
                    census_after_write_errors = report.census_after_write_errors,
                    completed = report.completed,
                    list_errors = report.list_errors,
                    "node_group_labeler: tick complete"
                );
            }
        }
    }

    tracing::info!("node-group-labeler reconciler stopping");
}
