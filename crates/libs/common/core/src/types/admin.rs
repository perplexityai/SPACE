//! Wire types for the `/admin/api/v1` operator surface.
//!
//! These structs are the shared contract between the api-server
//! (`crates/libs/api/src/routes/admin/`) and any client that speaks the
//! admin HTTP API — currently the `space-admin-cli` operator CLI
//! (`crates/apps/admin/cli/`). Keeping both ends pointed at one definition
//! avoids the drift trap called out in the crate-level module docs:
//! per-crate parallel types fragment the vocabulary and force lossy
//! conversions at every boundary.
//!
//! Types here are intentionally simple: `String` for RFC 3339 timestamps,
//! `serde_json::Value` for the freeform `metadata` blob, plain `Uuid` for
//! IDs. The api-server formats `time::OffsetDateTime` into these strings
//! before serializing; clients can parse them back if they need a typed
//! time value.

#![cfg(feature = "serde")]

use super::{NodeAddress, NodeDrainMode, NodeId, NodeManagedBy, NodeRole};
use serde::{Deserialize, Serialize};

use crate::types::node_group::{NodeGroupName, NodeGroupTemplatePolicy};
use crate::types::template::{TemplateName, TemplateVisibility};

/// Response shape for `GET /admin/api/v1/nodes` (each element) and
/// `GET /admin/api/v1/nodes/{node}`.
///
/// Action endpoints (`POST .../{node}:drain`, `:terminate`, `:undrain`) also
/// return this shape on success so callers can read the post-transition
/// state without an extra round trip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminNodeResponse {
    pub id: NodeId,
    pub name: Option<String>,
    pub address: Option<NodeAddress>,
    pub node_api_endpoint: Option<String>,
    pub role: NodeRole,
    pub managed_by: NodeManagedBy,
    pub state: Option<String>,
    pub scheduling_status: Option<String>,
    pub scheduling_reason: Option<String>,
    pub drain_phase: Option<String>,
    /// How aggressively the reconciler empties this node, as chosen by the
    /// operator on the `:drain` request. `None` means the column is NULL: no
    /// drain in progress, or a drain that started before the column existed.
    /// Drain-decision code resolves that to [`NodeDrainMode::Gentle`] — a
    /// missing choice never selects the evacuating mode — but the field stays
    /// `None` on the wire so an operator can still tell "nobody set a mode"
    /// apart from "someone chose gentle".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain_mode: Option<NodeDrainMode>,
    pub drain_started_at: Option<String>,
    pub drain_last_error: Option<String>,
    /// Value the cluster controller passes to AWS Auto Scaling's
    /// `should_decrement_desired_capacity` when terminating this node. `false`
    /// is the single-node-drain default (the ASG auto-replaces the terminated
    /// instance). The operator opts in to `true` per drain via the `:drain`
    /// request body field `replace_after_terminate = false`, which is the
    /// two-MNG blue-green rollout case where the incumbent ASG must shrink
    /// monotonically rather than spawning a like-for-like replacement.
    #[serde(default)]
    pub terminate_decrement_capacity: bool,
    pub k8s_node_name: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
    pub last_heartbeat: Option<String>,
    pub created_at: String,
    pub updated_at: Option<String>,
    pub deleted_at: Option<String>,
    /// None on inventory-only responses; never interpret unknown as zero.
    /// Sandboxes still occupying placement on this node — anything that has
    /// not terminated (`failed`/`cleanup_required`/`deleted`) and has not
    /// migrated off (`suspended`). This is the count the drain reconciler
    /// uses as the threshold for advancing `draining → drained`.
    pub sandboxes_active: Option<i64>,
    /// None on inventory-only responses.
    /// Sandboxes whose durable snapshot is still pinned to this node's local
    /// disk (`lifecycle = 'suspended'` with `target_node_id` matching).
    pub sandboxes_local_snapshots: Option<i64>,
    /// Drain-candidate ranking facts, present only when the caller asked for
    /// them: `sort=drainability` on the list endpoint, always on the
    /// single-node GET unless `include_occupancy=false`.
    ///
    /// Carries its own `#[serde(default)]` rather than relying on a
    /// container-level one: that would need `Default` on this whole response,
    /// which would let a caller build a node record with no id or role. Every
    /// field added here must therefore keep defaulting individually — the
    /// `space-admin-cli` CLI ships independently of the api-server and hard-fails a
    /// listing on any deserialize error.
    ///
    /// `None` means "not computed" and is deliberately distinct from
    /// "computed, and every count is zero" — the all-zero case is exactly
    /// what makes a node the best gentle-drain candidate, so a client must
    /// not read an absent block as one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drainability: Option<AdminNodeDrainability>,
    /// Which residency policy produced [`Self::drainability`], as the shared
    /// `ResidencyEvidence` wire string (`lifecycle_only` / `node_census_aware`).
    /// `None` whenever `drainability` is.
    ///
    /// A `String` rather than the enum because that type lives in
    /// `pplx-space-common-svc`, which this crate is a dependency *of*. The value
    /// is always that enum's own `as_str()`, never hand-written here.
    ///
    /// Load-bearing for interpreting the counts: under `lifecycle_only` — the
    /// default — nothing is discounted for census absence, so `live_residents`
    /// counts phantom rows too, exactly as the drain gate does in that mode. It
    /// sits out here rather than inside the block because it describes the
    /// cluster's configuration, not the node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residency_evidence: Option<String>,
    /// Node-local network slot-pool counters reported by node-manager. Public
    /// sandbox create admission depends on this pre-advertised capacity rather
    /// than synchronous host network setup, so operators need it next to drain
    /// and occupancy state when diagnosing placement stalls.
    #[serde(default)]
    pub network_slots: AdminNodeNetworkSlots,
    /// Cached node status snapshot last published by node-ctl. This is
    /// nullable for cluster-role nodes and for rows that predate node status
    /// publication, but worker/builder rows should normally carry it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<AdminNodeStatusSnapshot>,
}

/// How easily a node can be drained, in the terms an operator ranks candidates
/// by. The counts answer "will this node's residents actually leave?", which is
/// the question a gentle drain (evict-nothing, wait for natural departure)
/// turns on.
///
/// **Read [`AdminNodeResponse::residency_evidence`] before reading any count
/// here.** Under `node_census_aware` the counts discount sandbox rows that a
/// currently reachable, unexpired-heartbeat node does not report in its
/// published inventory. Under `lifecycle_only` — the default every cluster
/// ships with — **nothing is discounted for census absence** and phantom rows
/// count too. Either way the discount is one-directional: rows on an
/// unreachable or heartbeat-expired node all keep counting, because absence
/// cannot be proven, so a node outage never reads as an empty node.
///
/// The fields do not share one lifecycle scope, and the differences are the
/// point — each is scoped to the gate it predicts. [`Self::live_residents`]
/// narrows to the lifecycles the drain reconciler waits on, so
/// `sandboxes_active` minus it covers both the phantom rows and the `stopped` /
/// `created` / `deleting` / `quarantined` sandboxes that occupy the node
/// without being evacuable;
/// [`Self::terminate_blockers`] keeps those non-evacuable rows because the
/// terminator does; [`Self::drain_blockers`] is not a subset of either.
/// `#[serde(default)]` at the container level is deliberate forward-compat: the
/// `space-admin-cli` CLI ships independently of the api-server and hard-fails a whole
/// listing on any deserialize error, so without it the *next* field added here
/// would break every new CLI talking to a not-yet-rolled server.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AdminNodeDrainability {
    /// Sandboxes a drain would actually wait on: reported in the node's own
    /// census, and in a lifecycle the suspend → restore pipeline can service.
    /// Fewer is better; zero means the drain has nothing to do whatever
    /// `sandboxes_active` says.
    pub live_residents: i64,
    /// Live residents that have sat in their **current lifecycle** longer than
    /// the requested long-runner age (24 h by default) — a sandbox this stale
    /// is unlikely to depart on its own, so one of them can park a gentle
    /// drain indefinitely, which is why a zero here outranks any number of
    /// short-lived residents.
    ///
    /// Time-in-current-lifecycle is not sandbox age, and the difference cuts
    /// both ways: `sandboxes.lifecycle_changed_at` is reset by a trigger on
    /// every lifecycle change, so a week-old sandbox that pauses and resumes
    /// hourly never counts here, while a `paused` row nobody has touched for
    /// 25 h does. Read it as "nothing has moved this sandbox lately", not as
    /// "this sandbox has proven it will never suspend".
    pub live_long_runners: i64,
    /// The long-runner threshold, in hours, that actually produced
    /// [`Self::live_long_runners`] — echoed for the same reason
    /// [`AdminNodeResponse::residency_evidence`] is. A caller cannot otherwise
    /// recover it: the single-node route uses a fixed default it never asked
    /// for, and the list route clamps an absurd `long_runner_age_hours` rather
    /// than rejecting it, so two listings taken at different thresholds would
    /// be indistinguishable from their payloads.
    pub long_runner_age_hours: u32,
    /// Mirror of [`AdminNodeResponse::sandboxes_local_snapshots`], repeated
    /// here so the whole ranking key travels in one block. Suspended
    /// sandboxes pinned to this node's disk do not block a gentle drain from
    /// converging, but they do block the subsequent `:terminate`.
    pub local_snapshots: i64,
    /// Whether the node had a reachable, unexpired heartbeat when these counts
    /// were taken. `false` means the control plane cannot currently see the
    /// node, so the counts above are upper bounds — every resident row keeps
    /// counting because absence cannot be proven — and the node is ranked last
    /// regardless of them: nothing evacuates off a node the evacuation RPCs
    /// cannot reach.
    ///
    /// Reported from the same statement as the counts so a caller ordering by
    /// it is not mixing two snapshots. That is also why it may legitimately
    /// disagree with [`AdminNodeStatusSnapshot::reachable`] / `stale` in the
    /// same response, which the listing read separately: this field describes
    /// the snapshot the counts describe, and a node that flipped between the
    /// two reads shows both answers. Order by this one, not by those.
    pub heartbeat_fresh: bool,
    /// Rows holding this node's `draining → drained` gate open, counted with the
    /// node-drain reconciler's own predicate rather than an approximation of it.
    ///
    /// Neither a subset nor a superset of `live_residents` — the two overlap,
    /// and both halves of that matter. `suspended`-not-durable and
    /// non-evacuation `restoring` rows pin only through `target_node_id` with
    /// `active_node_id` NULL (an in-flight user restore is the common case), so
    /// a node can hold a blocked drain gate while every residency-based count
    /// reads zero. A `suspending` row, by contrast, still carries its
    /// `active_node_id` — that column is cleared when suspend *completes*, not
    /// when it starts — and `suspending` is a resident lifecycle, so it is
    /// counted both here and in `live_residents`. The two columns therefore
    /// cannot be added together.
    ///
    /// The drained-node terminator applies a snapshot guard of the same shape, so
    /// this count and [`Self::terminate_blockers`] together are what predict a
    /// `:terminate` — neither alone does.
    ///
    /// This is **not** a superset of [`Self::local_snapshots`], and the two are
    /// counted differently on purpose: this fragment matches a `creating`
    /// snapshot only when it names the sandbox's current local artifact, while
    /// `local_snapshots` also counts an artifact-mismatched one. So a sandbox
    /// can appear there and not here. What keeps `local_snapshots` out of the
    /// rank defensible is that the terminator's snapshot guard is *this*
    /// predicate, not that one — a row only `local_snapshots` sees blocks
    /// neither gate.
    pub drain_blockers: i64,
    /// Rows the drained-node terminator would refuse to advance past, i.e. the
    /// occupancy scope with the same census discount the terminator applies.
    ///
    /// Sits between `live_residents` and `sandboxes_active`: it keeps `stopped`
    /// / `created` / `deleting` / `quarantined` rows, which the census reports
    /// and the drain
    /// pipeline ignores, and drops phantom rows, which the terminator discounts.
    /// Ranking on the raw `sandboxes_active` instead would prefer a node holding
    /// one reported `stopped` row over one holding fifty phantoms — backwards
    /// from what the terminator does with them.
    pub terminate_blockers: i64,
}

/// Ordering for the admin node listing (`/admin/api/v1/nodes` on the
/// shared host, `/v1/nodes` on a dedicated admin host).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminNodeSort {
    /// Role, then display name — the stable listing order.
    #[default]
    Name,
    /// Best gentle-drain candidate first, on five terms in this order:
    /// nodes the control plane cannot see (`heartbeat_fresh == false`) sort
    /// **last** outright, then fewest
    /// [`AdminNodeDrainability::live_long_runners`], fewest
    /// [`AdminNodeDrainability::live_residents`], fewest
    /// [`AdminNodeDrainability::drain_blockers`], fewest
    /// [`AdminNodeDrainability::terminate_blockers`]. The sort is stable, so
    /// [`AdminNodeSort::Name`] order is preserved within a tie. Populates
    /// [`AdminNodeResponse::drainability`].
    ///
    /// **Pair this with a `role` filter.** The key carries no role term, and
    /// builder and cluster-role nodes normally host no sandboxes at all, so they
    /// win every count and head an unfiltered listing. `role=worker` is what
    /// makes "best candidate first" mean a worker.
    ///
    /// [`AdminNodeDrainability::local_snapshots`] is reported but is **not** a
    /// ranking term: a local-only pinned snapshot is a strand risk to weigh by
    /// hand, not something a gentle drain waits out, so letting it outrank a
    /// live long-runner would order candidates by the wrong cost.
    ///
    /// Every term is also a field on [`AdminNodeDrainability`], so the order can
    /// be read off the response rather than taken on trust. `space-admin-cli list-nodes
    /// --sort drainability` renders a column per term, and refuses a non-empty
    /// response in which no row carries a block, rather than presenting name
    /// order as a ranking. An empty listing is just an empty listing.
    Drainability,
}

impl AdminNodeSort {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Drainability => "drainability",
        }
    }

    /// Every accepted value, for error messages and exhaustive tests.
    pub const ALL: &'static [Self] = &[Self::Name, Self::Drainability];

    /// Whether this ordering needs the drainability counters computed.
    pub fn needs_drainability(self) -> bool {
        match self {
            Self::Name => false,
            Self::Drainability => true,
        }
    }
}

impl std::fmt::Display for AdminNodeSort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The accepted set is rendered from [`AdminNodeSort::ALL`] rather than written
/// out, so a new variant cannot ship a 400 that omits itself.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "unknown sort {value:?}; expected one of: {}",
    AdminNodeSort::ALL.iter().map(|sort| sort.as_str()).collect::<Vec<_>>().join(", ")
)]
pub struct UnknownAdminNodeSort {
    pub value: String,
}

impl std::str::FromStr for AdminNodeSort {
    type Err = UnknownAdminNodeSort;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .iter()
            .copied()
            .find(|sort| sort.as_str() == value)
            .ok_or_else(|| UnknownAdminNodeSort {
                value: value.to_string(),
            })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AdminNodeNetworkSlots {
    pub ready: Option<i32>,
    pub min_ready: Option<i32>,
    pub degraded: Option<i32>,
    pub quarantined: Option<i32>,
    pub total: Option<i32>,
    pub leased: Option<i32>,
    pub max_slots: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminNodeStatusSnapshot {
    pub observed_at: String,
    pub stale_after: String,
    pub stale: bool,
    pub reachable: bool,
    pub error: Option<String>,
    pub role: Option<String>,
    pub node_id_reported: Option<String>,
    pub scheduling: Option<AdminNodeSchedulingStatus>,
    pub health: Option<AdminNodeHealthStatus>,
    /// Independent collection evidence, not a readiness or upgrade verdict.
    /// Missing on older API servers and nodes that have not reported inventory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_inventory: Option<AdminExecutionInventoryObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminExecutionInventoryObservation {
    /// Diagnostic label: complete, unavailable, or legacy. Preserve future
    /// labels for display during mixed-version rollout; never infer eligibility.
    pub status: String,
    /// Control-plane receipt time, independent of the node heartbeat.
    pub received_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminNodeSchedulingStatus {
    pub status: String,
    pub reason: Option<String>,
    pub network_slots_ready: Option<i32>,
    pub network_pool_min_ready: Option<i32>,
    pub network_slots_degraded: Option<i32>,
    pub network_slots_quarantined: Option<i32>,
    pub network_slots_total: Option<i32>,
    pub network_slots_leased: Option<i32>,
    pub network_pool_max_slots: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminNodeHealthStatus {
    pub reachable: bool,
    pub healthy: bool,
    pub error: Option<String>,
    pub services: Vec<AdminNodeServiceHealth>,
    pub node_ctl: Option<AdminNodeCtlStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminNodeServiceHealth {
    pub name: String,
    pub reachable: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminNodeCtlStatus {
    pub reachable: bool,
    pub error: Option<String>,
    pub reconciliations: Vec<AdminNodeReconciliationStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminNodeReconciliationStatus {
    pub name: String,
    pub poll_interval_secs: u64,
    pub last_started_at_unix_secs: u64,
    pub last_completed_at_unix_secs: u64,
    pub last_success_at_unix_secs: u64,
    pub next_due_at_unix_secs: u64,
    pub running: bool,
    pub templates_seen: u64,
    pub sandboxes_seen: u64,
    pub templates_applied: u64,
    pub sandboxes_applied: u64,
    pub last_error: String,
}

/// Optional JSON body for `POST /admin/api/v1/nodes/{id}:drain`.
///
/// `replace_after_terminate` controls whether the eventual ASG
/// `TerminateInstanceInAutoScalingGroup` call decrements the desired capacity:
///
/// - `true` (default) — preserve current single-node behavior: the ASG
///   auto-replaces the terminated instance.
/// - `false` — two-MNG blue-green rollout: the incumbent ASG must shrink
///   monotonically. Persisted on the node row so the terminator honors the
///   operator's original intent regardless of when the actual termination
///   call runs.
///
/// `mode` selects how aggressively the reconciler empties the node:
///
/// - `gentle` (default) — cordon and wait. Resident sandboxes keep serving
///   traffic and leave on their own (auto-suspend on inactivity, a user
///   suspend, or a delete); the user-API drain gates stay open for them.
/// - `forced` — evacuate every resident `started`/`paused` sandbox
///   immediately, and 409 every placement-affecting and access path to a
///   resident sandbox for the whole drain window. Per-node disruption is
///   proportional to resident live-sandbox count, so this is the opt-in.
///
/// Persisted on the node row, so the reconciler and the user-API gates both
/// read the operator's original intent rather than a process-wide setting.
///
/// `deny_unknown_fields` so a misspelled or camelCase key (e.g.
/// `replaceAfterTerminate`) surfaces as a 400 instead of silently falling
/// back to the default `true`. A rollout script that thinks it asked for
/// monotonic shrink and got auto-replace instead is the exact failure mode
/// this flag exists to avoid. The same reasoning covers `mode`: a typo'd
/// `"mode": "gentel"` is a 400, not a silent forced drain.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrainRequest {
    /// `None` means the request expressed no opinion, which is **not** the same
    /// as `Some(true)` even though both start a drain that auto-replaces.
    ///
    /// `:drain` has two edges, and the distinction is what keeps them both
    /// honest. On the edge that *starts* a drain, `None` resolves to the default
    /// `true` (see [`Self::replace_after_terminate_or_default`]). On a node
    /// already draining the intent is fixed — the terminator may act on it at
    /// any moment once the node reaches `drained`, so there is no window in
    /// which changing it has a defined meaning — and `None` is what lets an
    /// escalation say nothing about it. `Some(v)` disagreeing with the stored
    /// value is a 409 rather than a silent overwrite.
    ///
    /// A bare `bool` cannot express that. With no way to tell an omitted field
    /// from an explicit `true`, the endpoint has to choose between refusing
    /// every bodiless repeat against a shrinking node, silently applying a stale
    /// client's default (every `space-admin-cli` older than this field sends an
    /// explicit `!no_replace_after_terminate`), and silently dropping a
    /// deliberate correction. None of those surfaces as an error, which is
    /// exactly the case [`Option`] exists for.
    ///
    /// `skip_serializing_if` is load-bearing, not tidiness. `space-admin-cli` serializes
    /// this struct whole, and serde's `default` fires only on an **absent** key —
    /// an explicit `null` against an api-server that still types the field as
    /// `bool` is `invalid type: null, expected a boolean`, i.e. a 400 on every
    /// bodiless drain. CLI binaries roll independently of the api-server, so that
    /// skew window is real; omitting the key keeps the request byte-identical to
    /// what the previous release accepts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace_after_terminate: Option<bool>,
    /// `NodeDrainMode`'s own `Default` is gentle, and it is the single statement
    /// of "a missing mode is gentle" that the NULL-column read also defers to —
    /// so this does not restate the policy.
    #[serde(default)]
    pub mode: NodeDrainMode,
    /// `true` makes `:drain` a start-only request: it succeeds only against a
    /// node with no drain phase, and any existing drain is a 409 instead of
    /// being escalated or repeated. The check runs under the handler's row
    /// lock, so it is atomic with the write. That is what an orchestrator
    /// needs to start drains without ever escalating one somebody else
    /// started between its read and its POST.
    ///
    /// `false` (the default) keeps the escalate-in-place and same-mode edges.
    /// Skipped when `false`, so a request that doesn't ask for it stays
    /// byte-identical to what the previous release accepts. One that does ask
    /// is a 400 from an api-server that predates the field (this struct is
    /// `deny_unknown_fields`), which fails closed.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub require_undrained: bool,
}

impl DrainRequest {
    /// The replacement intent to persist when *starting* a drain: an unexpressed
    /// opinion means the ASG auto-replaces the terminated instance, which
    /// preserves single-node-drain semantics.
    pub fn replace_after_terminate_or_default(&self) -> bool {
        self.replace_after_terminate.unwrap_or(true)
    }
}

/// Response body for `POST /admin/api/v1/nodes/{id}:force-upload-local-snapshots`.
///
/// Lists every local-only suspended sandbox pinned to the node — i.e.
/// `lifecycle = 'suspended'`, `target_node_id = $node`, no `state='ready'`
/// snapshots row — and the outcome of the per-sandbox reupload trigger:
///
/// - `reuploaded` — the handler enqueued a fresh upload on the on-node
///   sbx-mgr's durable worker. Once that worker registers the snapshot
///   with cluster-api, the sandbox stops counting toward
///   `sandboxes_local_snapshots` and the source node is free to drain.
/// - `failed` — the node-api was unreachable or timed out, or the
///   sbx-mgr refused with a precondition error (snapshot files pruned,
///   no upload config). The sandbox id and the surfaced error are
///   reported per-row so the operator can act on each blocker. If the
///   error is `required on-disk artifact missing`, the sandbox cannot
///   be promoted from this node — operator handles via `drain-keep` or
///   accept-data-loss + manual terminate.
/// - `deferred` — the request hit the per-invocation cap (sized to
///   keep the handler inside the 10 s admin-handler contract). Listed
///   sandbox IDs were left untouched; re-invoke the endpoint or wait
///   for the reconciler's per-tick promotion to pick them up.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ForceUploadLocalSnapshotsResponse {
    pub node_id: crate::types::NodeId,
    pub reuploaded: Vec<crate::types::SandboxId>,
    pub failed: Vec<ForceUploadFailure>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deferred: Vec<crate::types::SandboxId>,
    /// `true` when the server-side enumeration cap on the pinned-list
    /// query was hit and additional pinned sandboxes exist beyond
    /// what's listed in `reuploaded` + `failed` + `deferred`. Lets
    /// the operator know that re-invoking will keep finding new work
    /// even after the listed sandboxes are processed, and bounds the
    /// DB transfer and JSON response size for nodes with very large
    /// local-only backlogs (everything stays O(1) in the pinned-list
    /// size).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForceUploadFailure {
    pub sandbox_id: crate::types::SandboxId,
    pub error: String,
}

/// Query-parameter filters for `GET /admin/api/v1/nodes`.
///
/// `drain_phase` accepts the canonical phase values (`draining`, `drained`,
/// `terminating`) or `none` to match rows with no drain in progress
/// (`drain_phase IS NULL` server-side). `role` accepts `worker`, `builder`,
/// or `cluster`. `include_deleted` defaults to `false` and pulls in rows
/// soft-deleted from `node_direct`.
///
/// `sort` accepts the [`AdminNodeSort`] values and is parsed server-side, so
/// an unknown value is a 400 naming the accepted set rather than a
/// deserialization error. `long_runner_age_hours` tunes the
/// [`AdminNodeDrainability::live_long_runners`] threshold. It is validated on
/// every request — `0` is a 400 whatever the sort is — but only changes the
/// response when the drainability counters are computed.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListAdminNodesFilters {
    /// Legacy callers receive counts; inventory clients opt out explicitly.
    /// `sort=drainability` always computes counts, even when this is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_occupancy: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain_phase: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub include_deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub long_runner_age_hours: Option<u32>,
}

/// Single-node reads retain legacy diagnostics unless explicitly disabled.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GetAdminNodeFilters {
    pub include_occupancy: Option<bool>,
}

#[cfg(test)]
mod tests;

fn is_false(value: &bool) -> bool {
    !*value
}

/// Response shape for `GET /admin/api/v1/node-groups` (each element),
/// `GET /admin/api/v1/node-groups/{id}`, and the node-group write endpoints
/// (which return the post-write state so callers can read it without an
/// extra round trip).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminNodeGroupResponse {
    pub id: crate::types::NodeGroupId,
    pub name: NodeGroupName,
    /// `None` ⇒ generic (shared) group; `Some` ⇒ dedicated group for that org.
    pub org_id: Option<crate::types::OrgId>,
    pub template_policy: NodeGroupTemplatePolicy,
    pub is_default: bool,
    pub created_at: String,
    /// Number of worker `nodes` rows whose mirrored `node_group_id` points
    /// at this group (node groups are worker pools; builder/cluster rows
    /// never count). For the singleton default group this includes
    /// NULL-membership workers (an unlabeled worker resolves to the default
    /// generic group). Reporting only — the node label is the scheduling
    /// source of truth and the mirror can lag a relabel.
    pub nodes_count: i64,
    /// The `restricted` allowlist (template names). Empty unless
    /// `template_policy` is `restricted`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compat_templates: Vec<String>,
}

/// JSON body for `POST /admin/api/v1/node-groups`.
///
/// The row is created in lockstep with the Terraform/MNG change that
/// provisions the matching nodes (the `space-node-group` label, and for
/// org-owned groups the dedicated taint); this endpoint only manages the
/// control-plane row. `deny_unknown_fields` so a typoed key surfaces as a
/// 400 instead of silently creating a differently-shaped group.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateNodeGroupRequest {
    pub name: NodeGroupName,
    /// Omit (or `null`) to create a generic group; set to create a group
    /// dedicated to that org.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_id: Option<crate::types::OrgId>,
    /// Defaults to `default`. `restricted` is only legal for org-owned
    /// groups.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template_policy: Option<NodeGroupTemplatePolicy>,
    /// Make this group the singleton cluster default (generic groups only).
    /// Fails with a conflict if another default already exists — use
    /// `:set-default` to move the flag.
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_default: bool,
}

/// JSON body for `POST /admin/api/v1/node-groups/{id}:assign-org`.
///
/// `{"org_id": "<uuid>"}` dedicates the group to that org; `{}` or
/// `{"org_id": null}` releases it back to generic. `deny_unknown_fields` so
/// a typoed key cannot silently turn an assignment into a release.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssignOrgRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_id: Option<crate::types::OrgId>,
}

/// JSON body for `PUT /admin/api/v1/node-groups/{id}/template-policy`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetTemplatePolicyRequest {
    pub template_policy: NodeGroupTemplatePolicy,
}

/// JSON body for `PUT /admin/api/v1/node-groups/{id}/compat`. Full-replace
/// semantics: `templates` is the complete allowlist after the call.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetCompatRequest {
    pub templates: Vec<TemplateName>,
}

/// JSON body for `PUT /admin/api/v1/templates/{name}:visibility`.
///
/// `org_id` may only accompany `visibility = "org_specific"` and assigns the
/// owning org in the same write (an org-specific template must have an
/// owner; supplying it here satisfies the
/// `org_specific_requires_owner` CHECK for previously ownerless
/// templates).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetTemplateVisibilityRequest {
    pub visibility: TemplateVisibility,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_id: Option<crate::types::OrgId>,
}

/// Response shape for `PUT /admin/api/v1/templates/{name}:visibility`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminTemplateVisibilityResponse {
    pub name: String,
    pub visibility: TemplateVisibility,
    pub org_id: Option<crate::types::OrgId>,
    /// Compat allowlist rows removed by a narrowing flip: rows that listed
    /// this template in groups not owned by the new owning org became
    /// illegal (the hard cross-org rule) and were pruned in the same
    /// transaction.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub pruned_compat_rows: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}
