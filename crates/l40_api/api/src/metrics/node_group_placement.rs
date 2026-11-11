//! Per-sandbox node-group placement resolution, as performed by every service
//! that builds a sandbox pod (`space-api` on create/restore/evacuation,
//! `space-cluster-controller` on warm-pool fill).
//!
//! Counters, not gauges: each series here is one pod build's decision, and the
//! interesting quantity is the rate at which each outcome occurs. That is the
//! opposite of `node_group_labeler`, whose conditions are standing fleet states.
//!
//! Nothing is emitted while `SPACE_SANDBOX_NODE_GROUP_PLACEMENT` is off — the
//! resolver returns before it queries anything — so total silence is the
//! signal that the gate is closed in an environment, and the appearance of
//! `outcome:unprovisioned` is the signal that it was opened before any node
//! group existed.

use pplx_space_metrics_emit::{counter, tag};

/// What resolving one sandbox's candidate set produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveOutcome {
    /// Resolved to a set that includes the caller org's own dedicated groups,
    /// so the pod carries the dedicated preference and toleration.
    Dedicated,
    /// Resolved to shared (generic) capacity only.
    Shared,
    /// No **generic** node group admits this template, so the pod carries no
    /// node-group constraints because the generic group is not provisioned.
    ///
    /// Almost always cluster-wide: with no generic group, every generic
    /// template resolves this way for every caller. But it is resolved
    /// per-caller, so it is *not* strictly a cluster property — an org that
    /// owns a compatible dedicated group resolves `dedicated` on the same
    /// cluster in the same instant, so this outcome and `dedicated` can both
    /// be non-zero at once. Read a non-zero rate as a stage-ordering fault to
    /// correct rather than as a per-sandbox condition, but do not read it as
    /// proof that *nothing* is enforced anywhere.
    Unprovisioned,
    /// No node group admits this (org, template) pair: the template is
    /// org-specific and its org has no compatible dedicated group. Fails the
    /// operation closed — there is no shared fallback for an org-specific
    /// template — except on `path:pool_claim`, where it is the expected
    /// decline and the create builds cold instead.
    ///
    /// It does **not** imply the cluster has generic capacity: the resolver
    /// does not check for a generic group before booking this, so an
    /// org-specific template on a cluster with no node groups at all reports it
    /// too, alongside `Unprovisioned` from that cluster's generic templates.
    NoCompatibleGroup,
    /// The `templates` row the sandbox names is gone, so its visibility is
    /// unknowable and no candidate set can be derived. Fails closed.
    TemplateAbsent,
    /// Resolution itself failed (database, decode, or a rejected candidate).
    /// Fails closed.
    Error,
}

impl ResolveOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dedicated => "dedicated",
            Self::Shared => "shared",
            Self::Unprovisioned => "unprovisioned",
            Self::NoCompatibleGroup => "no_compatible_group",
            Self::TemplateAbsent => "template_absent",
            Self::Error => "error",
        }
    }
}

impl std::fmt::Display for ResolveOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which pod build asked for the candidate set. Kept separate from the
/// outcome so a `no_compatible_group` on `create` (a user-visible 409) can be
/// told apart from one on `restore` (an existing sandbox that can no longer be
/// placed and needs migrating) without reading logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvePath {
    /// A user create, resolved from the create request in the substrate
    /// adapter. The only path whose failure is a client-visible `409`.
    Create,
    /// The reconcile-side materialization of an on-demand sandbox's pod, which
    /// has no create request in hand.
    Materialize,
    /// The warm-pool adoption check on the create path: is the node a claimed
    /// slot already runs on inside this sandbox's candidate set? Resolved for
    /// the sandbox's own org, and only once a slot has actually been claimed.
    ///
    /// Distinct from [`Self::WarmPool`], which is the cluster-controller's
    /// per-config fill decision — this one is per *create* and emitted by
    /// `space-api`, so folding the two would sum a per-tick controller rate and
    /// a per-create API rate into one unreadable series.
    ///
    /// The adoption *decision* is not here: this counter only says a set was
    /// resolved. See [`pool_claim_admission`] for what happened to the slot.
    PoolClaim,
    Restore,
    Evacuation,
    WarmPool,
}

impl ResolvePath {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Materialize => "materialize",
            Self::PoolClaim => "pool_claim",
            Self::Restore => "restore",
            Self::Evacuation => "evacuation",
            Self::WarmPool => "warm_pool",
        }
    }
}

impl std::fmt::Display for ResolvePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One candidate-set resolution.
pub fn resolved(path: ResolvePath, outcome: ResolveOutcome) {
    counter(
        "node_group_placement.resolved",
        1,
        &[tag("path", path.as_str()), tag("outcome", outcome.as_str())],
    );
}

/// Verdict of the pre-flight a local-only restore runs before it pins
/// `spec.nodeName` to its source node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalRestorePreflight {
    /// The source node's live label names a group inside the candidate set;
    /// the pin stands.
    InSet,
    /// The source node's label names a group outside the candidate set. The
    /// restore is refused: its bytes are on that node and nowhere else, so the
    /// pin cannot be relaxed, and kubelet would reject the pod anyway.
    OutOfSet,
    /// The source node carries no node-group label at all — mid-rollout, or a
    /// worker the labeler's gate suppressed. Refused for the same reason as
    /// [`Self::OutOfSet`]: an `In (…)` term does not match a node lacking the
    /// key.
    LabelAbsent,
    /// The node reflector had not completed its initial `LIST`, so the live
    /// label is unknown. The pin stands and kubelet admission remains the
    /// backstop — a cold cache is a control-plane state and must not convert
    /// into a restore failure.
    CacheCold,
    /// The source node is absent from a warm cache: it is gone, and the
    /// restore fails on binding rather than on placement. The pin stands so
    /// the failure keeps its existing shape.
    NodeAbsent,
}

impl LocalRestorePreflight {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InSet => "in_set",
            Self::OutOfSet => "out_of_set",
            Self::LabelAbsent => "label_absent",
            Self::CacheCold => "cache_cold",
            Self::NodeAbsent => "node_absent",
        }
    }
}

impl std::fmt::Display for LocalRestorePreflight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Verdict on adopting a warm-pool slot whose pod is already running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolClaimAdmission {
    /// The slot's node is inside the claiming sandbox's candidate set; adopted.
    Admitted,
    /// The slot's node belongs to a node group while no generic group admits
    /// this template — so a dedicated group exists without a default generic
    /// one. The slot is released and the sandbox builds cold, because with no
    /// candidate set there is nothing to test membership against and the node
    /// is, by elimination, another org's dedicated hardware.
    ///
    /// **Any occurrence is a provisioning-order fault**, not a per-sandbox
    /// condition. Create the default generic group before dedicated groups,
    /// or disable the gate until provisioning is complete.
    UnprovisionedLabeledNode,
    /// The gate is on but this cluster has no generic node group, so the
    /// resolution imposed no boundary and the slot was adopted without its
    /// node being read.
    ///
    /// Booked so this counter still totals placement decisions when the gate
    /// is enabled before a generic group exists: `admitted` is necessarily
    /// zero while adoption works normally. Nothing is booked when the gate is
    /// *off*: there is no decision to record, and the series stays silent until
    /// a cluster enables
    /// the feature.
    Unconstrained,
    /// The template is one no pooled pod can ever host — it is org-specific.
    /// Reached two ways: the sandbox's set resolved dedicated-only and the
    /// pool's own org-less re-resolve then refused, or the sandbox's own
    /// resolve refused outright (which, for a caller that has an org, can only
    /// mean an org-specific template). A pool config is per template-release,
    /// so every slot under it serves that one template and none of them can be
    /// adopted by anybody: the slot is invalidated, one per create, and the
    /// config's refill is suppressed for the same reason, so the pool drains to
    /// zero. Those slots also hold a private rootfs on shared workers, which is
    /// the second reason not to leave them parked.
    OrgSpecificTemplate,
    /// The sandbox's set is dedicated-only, but the template itself is generic
    /// and the pool can still host it for a caller that owns no dedicated
    /// capacity. Reachable when a cluster has a dedicated group and **no**
    /// generic group: this caller's candidates are all its own hardware, while
    /// an org-less resolve of the same template reports `unprovisioned` and
    /// keeps filling.
    ///
    /// Released, not invalidated. The refusal is about *this caller*, not about
    /// the slot, and destroying a pod another org can still adopt would churn
    /// the pool once per create forever.
    CallerDedicatedOnly,
    /// A resolution failed for a reason that is **not** proof the config is
    /// dead, so the slot is handed back rather than taken out of the pool.
    ///
    /// This verdict is about the *slot's* fate and says nothing on its own
    /// about what the client saw — a momentary catalog failure and a reaped
    /// `templates` row both land here, and only the first is retryable. What
    /// separates them is the refusal the claim also reports, which carries the
    /// answer's class; this crate cannot name that type, since it sits above
    /// this one in the dependency graph, so see `PooledClaimRefusal` in
    /// `pplx-space-api`. Every cause:
    ///
    /// * The sandbox's own resolution hit a **catalog failure** — momentary, no
    ///   refusal reported, and the cold create is retried against the same
    ///   catalog and may well succeed.
    /// * The sandbox's own resolution found a **reaped `templates` row** or a
    ///   predicate disagreement. Standing, and reported as a refusal so a
    ///   strict `boot_mode=pool` create gets the cold path's own 500.
    /// * The claim's **second** resolution — for the pool's org-less subject —
    ///   failed for any reason, on a sandbox whose own set already resolved
    ///   dedicated-only. There the cold create succeeds on its dedicated
    ///   affinity and only the slot was affected.
    ///
    /// An org-specific template whose org has no compatible dedicated group is
    /// *not* here: that failure proves the template unhostable by any pooled
    /// pod, so it books [`Self::OrgSpecificTemplate`] and invalidates. The two
    /// verdicts therefore do not divide on whether a set was produced — neither
    /// produced one — but on whether the reason rules out every future caller.
    Unresolved,
    /// The slot's node is outside the set. The slot is taken out of the pool so
    /// the reconciler replaces it somewhere admissible; sustained non-zero
    /// means a node group was reassigned under resident warm slots.
    OutsideSet,
    /// The slot's node carries no node-group label. The adoption is refused —
    /// an `In (…)` term excludes an unlabeled node — but the slot is
    /// **released**, not invalidated, because this is the labeler lagging
    /// rather than the node changing groups.
    ///
    /// Expected in bursts: EKS strips an MNG-set label from live nodes on an
    /// unrelated MNG change, and the labeler repairs it on its next tick. A
    /// burst that decays is that window. Sustained non-zero means the labeler
    /// is not repairing — compare `node_group_labeler.suppressed_nodes` — and
    /// is worth acting on, because every claim on those nodes builds cold.
    LabelAbsent,
    /// The slot's node could not be read: the reflector has no entry for it.
    /// (An unscheduled pod also reaches the same branch, but the claim's
    /// usability gate makes that unreachable in production — see
    /// `pooled_pod_node_admission`.) The slot is handed back
    /// untouched and the create builds cold. A burst on process start is the
    /// node cache filling; sustained non-zero is not.
    Unknown,
}

impl PoolClaimAdmission {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Unconstrained => "unconstrained",
            Self::UnprovisionedLabeledNode => "unprovisioned_labeled_node",
            Self::OrgSpecificTemplate => "org_specific_template",
            Self::CallerDedicatedOnly => "caller_dedicated_only",
            Self::Unresolved => "unresolved",
            Self::OutsideSet => "outside_set",
            Self::LabelAbsent => "label_absent",
            Self::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for PoolClaimAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One warm-pool adoption decision.
///
/// Separate from [`resolved`] because that counter says only that a candidate
/// set was computed: `outcome:shared` increments identically whether the pod
/// was then adopted or refused, so without this one an adoption rate of zero
/// across a healthy pool is invisible.
pub fn pool_claim_admission(verdict: PoolClaimAdmission) {
    counter(
        "node_group_placement.pool_claim_admission",
        1,
        &[tag("verdict", verdict.as_str())],
    );
}

/// One restore whose node-group narrowing removed **every** fallback, leaving
/// the shortlist as the bare source pin.
///
/// Untagged and deliberately rare. It separates "the candidate set excluded
/// every fallback" from "the fleet is genuinely full", which produce the same
/// rendered pod: a required source pin with no alternatives, `Unschedulable` if
/// the source cannot take it. The first is a provisioning gap — a group with no
/// schedulable node, or a labeler that has not caught up — and the second is
/// ordinary load. Non-zero means restores are one busy node away from failing
/// for a reason no other series reports.
pub fn restore_shortlist_emptied() {
    counter("node_group_placement.restore_shortlist_emptied", 1, &[]);
}

/// One local-only restore pre-flight against the source node's live label.
pub fn local_restore_preflight(verdict: LocalRestorePreflight) {
    counter(
        "node_group_placement.local_restore_preflight",
        1,
        &[tag("verdict", verdict.as_str())],
    );
}
