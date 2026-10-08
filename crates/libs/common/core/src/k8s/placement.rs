//! Node-group placement inputs for sandbox pods.
//!
//! Placement derives, per sandbox, the set of node groups its template and
//! calling org are compatible with, and [`build_sandbox_pod`] renders that
//! set as scheduling constraints: a required nodeAffinity term over the
//! whole compatible set (the security boundary), a preferred term biasing
//! toward the caller's dedicated groups, and a toleration for the dedicated
//! taint only when the caller's own dedicated group is targeted.
//!
//! [`build_sandbox_pod`]: super::build_sandbox_pod

use k8s_openapi::api::core::v1::{
    Node, NodeSelectorRequirement, NodeSelectorTerm, PreferredSchedulingTerm,
};

use super::PodToleration;
use crate::types::{NodeGroup, NodeGroupName, OrgId, TemplateCompatSpec, TemplateName};

/// Node label naming the node group a worker node belongs to.
///
/// The key sits under the `node-restriction.kubernetes.io/` prefix, which the
/// NodeRestriction admission plugin forbids a **kubelet** credential
/// (`system:node:<name>`, group `system:nodes`) from setting on its own Node.
/// EKS enables that plugin by default, and it closes the kubelet path.
///
/// It closes only that path. NodeRestriction keys off the *credential*, so it
/// says nothing about any other identity, and Kubernetes RBAC has no
/// field-level granularity: any ServiceAccount holding `patch` on `nodes` can
/// write this label and strip [`DEDICATED_NODE_TAINT_KEY`] on any node. In this
/// cluster the node-ctl DaemonSet is exactly such an identity, and it is
/// worker-resident — so a guest escape that reaches its projected token could
/// forge group membership, which is the one attack the prefix is often assumed
/// to prevent. Treating the required nodeAffinity term as a hard boundary
/// therefore depends on a second condition beyond NodeRestriction, and RBAC
/// cannot express it.
///
/// A `ValidatingAdmissionPolicy` supplies it (see
/// `infra/space/kind/charts/space-node-ctl-local/`), pinning each node-ctl identity
/// to its own Node and to the metadata keys its reconcilers own — this prefix
/// and [`DEDICATED_NODE_TAINT_KEY`] excluded. That file states each rule; do
/// not restate them here, where nothing ties the description to the policy.
///
/// The grant itself is unchanged and deliberately so: `kubectl auth can-i patch
/// nodes` still answers `yes` for that ServiceAccount, and admission is what
/// refuses. So a cluster whose fence is disabled or absent has the exposure back
/// in full: treating the required nodeAffinity term as a hard boundary is a
/// claim about the policy being installed, not about the prefix.
pub const SANDBOX_NODE_GROUP_LABEL: &str = "node-restriction.kubernetes.io/space-node-group";

/// Taint key marking a node as dedicated to a single organization.
///
/// The taint value is the owning org's UUID and the effect is `NoSchedule`.
/// Like the node-group label, the taint is set by the cluster control plane
/// when a node joins a dedicated group — never by the node itself. It is the
/// defense-in-depth half of the *taint-excludes, affinity-attracts* pair: the
/// required node-group affinity keeps a sandbox on legal groups, while the
/// taint keeps every other org's pods off dedicated nodes by default —
/// **except** for a pre-bound pod, where `NoSchedule` is never evaluated (see
/// the `direct_node_name` handling in the pod builder), so there the affinity
/// conjunct and kubelet admission are the whole story.
///
/// Deliberately **not** `space.pplx.net/dedicated`: that key is already live as
/// the cred-authority authorization fence
/// (`space.pplx.net/dedicated=cred-authority:NoSchedule` on the
/// credential-authority node group). Kubernetes allows only one taint per
/// key+effect on a node, so reusing it would make a node unable to be both, and
/// a reconciler enumerating that key to sync org UUIDs would see
/// `cred-authority`, fail to parse it as an org, and could strip a fence whose
/// crossing is supposed to need the credmgr owner's approval.
pub const DEDICATED_NODE_TAINT_KEY: &str = "space.pplx.net/dedicated-org";

/// Weight of the preferred-affinity term biasing a sandbox toward the
/// caller's dedicated node groups. 100 is the Kubernetes per-term maximum, so
/// the dedicated preference scores at least as high as any other single
/// preference (template cache, restore source node). Because they share that
/// cap, a shared node that already has the template cached *ties* an uncached
/// dedicated node on the on-demand path, where the cache preference is also
/// 100 — and a tie hands the choice to the resource-scoring plugins, so a
/// shared node can win. (On a restore with no source node the cache preference
/// is 50, so the dedicated term leads inside the NodeAffinity plugin — though
/// that plugin's score is only one weighted vote among several; a restore that
/// *has* a source
/// node suppresses the dedicated term entirely, so there is no contest.) That is acceptable: spilling to the
/// shared pool is only legal for generic templates in the first place, and an
/// org-specific template has no shared groups in its candidate set at all.
pub const DEDICATED_NODE_GROUP_PREFERENCE_WEIGHT: i32 = 100;

/// Gate on resolving a node-group candidate set at all, read once per process
/// by every service that builds sandbox pods.
///
/// This renderer is inert without a candidate set, and nothing computes one
/// unless this flag is on: with it off the resolver never queries
/// `node_groups`, every pod build passes `node_group_placement: None`, and the
/// rendered pod is byte-identical to a pre-node-group pod. Enabling node-group
/// placement is a per-environment operator action rather than a merge.
///
/// **Order matters and this flag is the only thing enforcing it.** The required
/// nodeAffinity term names [`SANDBOX_NODE_GROUP_LABEL`], so on a cluster whose
/// workers do not carry that label every scheduler-placed sandbox is
/// `Unschedulable`. Enable it only after verifying the labeler has applied
/// the required labels. A cluster with no generic node group is handled
/// without an operator: the resolver reports the feature unprovisioned and
/// renders nothing, so flipping this on a cluster whose `node_groups` table is
/// empty is a no-op rather than a cluster-wide create outage.
///
/// Both `space-api` and `space-cluster-controller` read it, and the chart sets
/// it on both so one values edit moves the whole cluster. It is a process env
/// var: the flip takes effect when the pod restarts, not when the values change
/// merges.
pub const SANDBOX_NODE_GROUP_PLACEMENT_ENV: &str = "SPACE_SANDBOX_NODE_GROUP_PLACEMENT";

/// Node-group placement ships disabled: see
/// [`SANDBOX_NODE_GROUP_PLACEMENT_ENV`] for the ordering constraint that makes
/// the default non-negotiable.
pub const DEFAULT_SANDBOX_NODE_GROUP_PLACEMENT: bool = false;

/// The live node-group label on a Node object, as kubelet will evaluate it.
///
/// Read from the Node rather than from the mirrored `nodes.node_group_id`
/// column: the label is the scheduling source of truth and the column is a
/// reconciled copy that lags a reassignment, so only the label agrees with what
/// kubelet admission will do.
///
/// Lives here, beside [`SANDBOX_NODE_GROUP_LABEL`] and
/// [`NodeGroupPlacement::admits_group`], so the reader and the membership test
/// it feeds can be changed and tested as a pair. The labeler reads the same key
/// inline rather than through this function, so what the two share today is the
/// constant, not the accessor.
pub fn node_group_label(node: &Node) -> Option<&str> {
    node.metadata
        .labels
        .as_ref()?
        .get(SANDBOX_NODE_GROUP_LABEL)
        .map(String::as_str)
}

/// The caller's own dedicated node groups inside a placement's candidate set.
///
/// Private: [`NodeGroupPlacement::for_caller`] is the only way to build one, so
/// exposing it would offer a second path that skips the compatibility check.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DedicatedNodeGroups {
    /// Org that owns the dedicated groups; doubles as the dedicated taint
    /// value the pod must tolerate. Typed, because the pod builder compares it
    /// against the pod's runtime org to decide whether to emit that toleration
    /// — an authorization decision, not a bookkeeping field.
    org_id: OrgId,
    /// The dedicated groups themselves. Must be a non-empty subset of the
    /// placement's candidate set.
    groups: Vec<NodeGroupName>,
}

/// Failure constructing a [`NodeGroupPlacement`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NodeGroupPlacementError {
    /// The computed candidate set is empty: no node group admits this
    /// (caller org, template) pair. Placement fails closed — there is no
    /// fallback to the shared pool for an org-specific template whose org
    /// has no compatible dedicated group.
    #[error("no compatible node group admits this sandbox")]
    NoCompatibleNodeGroup,
    /// A candidate group does not satisfy `compatible(caller_org, template,
    /// group)`. Rejected rather than silently dropped: the rendered boundary
    /// would otherwise admit capacity the relation excludes — another org's
    /// dedicated hardware (leaving its `NoSchedule` taint as the only control —
    /// and that taint is scheduler-only, so it is no control at all for a
    /// pre-bound pod, while for a scheduler-placed one the group label and the
    /// taint are two independent control-plane writes, so a
    /// labeled-but-not-yet-tainted node would admit the pod), or
    /// shared capacity for a template that must never touch it.
    #[error(
        "candidate node group {group} is not compatible with template {template} for caller \
         {caller:?}"
    )]
    IncompatibleCandidate {
        group: NodeGroupName,
        template: TemplateName,
        /// `None` for a caller with no org identity — a warm-pool slot.
        caller: Option<OrgId>,
    },
    /// One name appeared twice with different owners, so the rendered boundary
    /// would admit whichever node carries the label regardless of policy.
    #[error("candidate node group {group} appears with two different owners")]
    ConflictingGroupPolicy { group: NodeGroupName },
}

/// Compatible node-group set for one sandbox placement, plus the caller's
/// dedicated subset.
///
/// Constructed once per pod build from the *current* template visibility and
/// caller org (never persisted on the sandbox), and rendered by
/// [`build_sandbox_pod`] as:
///
/// * a **required** nodeAffinity term `SANDBOX_NODE_GROUP_LABEL In
///   (candidates)` — the security boundary;
/// * a **preferred** term weighting the dedicated subset, so a
///   generic-template sandbox from an org with dedicated capacity biases
///   toward its own nodes but can still spill to the shared pool — emitted
///   only when the pod's runtime org matches this placement's, the pod has no
///   source-node pin or preference of its own, and (when the node-services
///   readiness bias is on) AND-ed with that readiness requirement;
/// * a **toleration** for `DEDICATED_NODE_TAINT_KEY=<org>`, emitted only when
///   this placement has a dedicated subset *and* the pod's runtime org matches
///   the org it was built for.
///
/// The constructor enforces the placement invariants, so a rendered pod can
/// never carry an empty candidate list or tolerate a dedicated taint without
/// targeting that org's group.
///
/// [`build_sandbox_pod`]: super::build_sandbox_pod
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeGroupPlacement {
    /// Template this candidate set was computed for. Retained for the same
    /// reason as `caller_org`: the set is only valid for the (org, template)
    /// pair it was derived from, so the pod builder cross-checks both against
    /// the pod it is actually rendering. Validating at construction and then
    /// discarding the template would leave "shared capacity offered to a
    /// template that may not touch it" representable again — via a pod built
    /// with a *different* template than the one the set was resolved for.
    template: TemplateName,
    /// Org this candidate set was computed for, or `None` for a caller with no
    /// org identity — a warm-pool slot, resolved before anyone claims it.
    /// Recorded even when the set is entirely shared capacity, because the pod
    /// builder compares it against the pod's own runtime org (itself an
    /// `Option`, absent for exactly the same pods): a placement resolved for
    /// the wrong org is a caller bug whether or not it happens to include
    /// dedicated groups.
    caller_org: Option<OrgId>,
    candidate_groups: Vec<NodeGroupName>,
    dedicated: Option<DedicatedNodeGroups>,
}

impl NodeGroupPlacement {
    /// Builds a placement from the compatible node groups and the org the
    /// candidate set was computed for.
    ///
    /// Every candidate is re-checked against
    /// [`NodeGroup::compatible`] — the same predicate that *defines* the
    /// candidate set — so a caller cannot hand in a group the relation excludes
    /// and have the rendered boundary admit it anyway. The dedicated subset is
    /// then **derived**, not supplied: a group is dedicated capacity for this
    /// placement exactly when its owning org is the caller's. Together that
    /// makes three classes of caller bug unrepresentable: a dedicated target
    /// outside the candidate set, another org's dedicated group among the
    /// candidates, and shared capacity offered to a template that may not touch
    /// it. Both group lists are sorted and deduplicated so the rendered pod
    /// spec is deterministic.
    ///
    /// What it cannot check is whether `template` *describes* the template it
    /// names. The spec's `visibility` and `org_id` are the caller's assertion
    /// about a `templates` row, and only the name survives into the placement,
    /// because the name is the only one of the three the pod builder can compare
    /// against — a pod carries a template name and an org, never a visibility.
    /// So a spec that calls an org-specific template generic is validated
    /// against its own claim, and no downstream check can recover: pass one
    /// read from the template record, not one assembled from request fields.
    pub fn for_caller(
        caller_org: Option<OrgId>,
        template: &TemplateCompatSpec,
        candidates: &[NodeGroup],
    ) -> Result<Self, NodeGroupPlacementError> {
        let mut candidate_groups: Vec<NodeGroupName> = Vec::with_capacity(candidates.len());
        let mut dedicated_groups: Vec<NodeGroupName> = Vec::new();
        let mut seen_policy: std::collections::BTreeMap<NodeGroupName, Option<OrgId>> =
            std::collections::BTreeMap::new();
        for group in candidates {
            // The candidate set is *defined* as the groups satisfying
            // `compatible(caller_org, template, group)`, so re-check the whole
            // relation here rather than the org half of it. A caller that
            // flattened in a shared group for an org-specific template, or an
            // owned group whose restricted allowlist excludes the template, has
            // produced a set the rendered boundary would otherwise admit — the
            // data-isolation violation this boundary exists to prevent.
            if !group.compatible(caller_org, template) {
                return Err(NodeGroupPlacementError::IncompatibleCandidate {
                    group: group.name.clone(),
                    template: template.name.clone(),
                    caller: caller_org,
                });
            }
            // The caller's own dedicated capacity: the subset the preferred
            // term biases toward. `compatible` has already established that no
            // *other* org's dedicated group can reach this point.
            //
            // Matched through `Some(caller_org)` rather than comparing the two
            // `Option`s, because a generic group's `org_id()` is also `None` —
            // so `None == None` would collect every generic group into
            // `dedicated_groups` for an org-less caller.
            //
            // Redundant as a *safety* guard, and deliberately kept: the
            // construction below is what gates the toleration and the preferred
            // term, and it yields `None` for an org-less caller whatever this
            // vector holds. What this line buys is that `dedicated_groups`
            // means what its name says at every point, so a later reader of it
            // does not inherit a silent lie.
            if let Some(caller_org) = caller_org
                && group.org_id() == Some(caller_org)
            {
                dedicated_groups.push(group.name.clone());
            }
            if let Some(previous) = seen_policy.insert(group.name.clone(), group.org_id())
                && previous != group.org_id()
            {
                // Same name, two different owners. `node_groups.name` is UNIQUE
                // in the schema so this is unreachable from a real read, but the
                // whole point of this constructor is that a caller bug cannot
                // produce a placement whose boundary admits the wrong nodes.
                return Err(NodeGroupPlacementError::ConflictingGroupPolicy {
                    group: group.name.clone(),
                });
            }
            candidate_groups.push(group.name.clone());
        }
        candidate_groups.sort();
        candidate_groups.dedup();
        if candidate_groups.is_empty() {
            return Err(NodeGroupPlacementError::NoCompatibleNodeGroup);
        }
        dedicated_groups.sort();
        dedicated_groups.dedup();
        // A dedicated subset exists only for a caller that has an org, which is
        // what lets `DedicatedNodeGroups::org_id` stay a concrete `OrgId`: it is
        // the taint value, and there is no taint to tolerate without an owner.
        let dedicated = caller_org
            .filter(|_| !dedicated_groups.is_empty())
            .map(|org_id| DedicatedNodeGroups {
                org_id,
                groups: dedicated_groups,
            });
        Ok(Self {
            template: template.name.clone(),
            caller_org,
            candidate_groups,
            dedicated,
        })
    }

    /// Required nodeAffinity match expression over the candidate set. AND-ed
    /// with any other required expressions (a drain-cohort pin, the source-node
    /// pin, the ssd placement block) inside a single node-selector term, and
    /// repeated into every ORed term when a restore shortlist produces more
    /// than one.
    pub fn required_match_expression(&self) -> NodeSelectorRequirement {
        NodeSelectorRequirement {
            key: SANDBOX_NODE_GROUP_LABEL.to_string(),
            operator: "In".to_string(),
            values: Some(
                self.candidate_groups
                    .iter()
                    .map(|group| group.as_str().to_string())
                    .collect(),
            ),
        }
    }

    /// Org this placement was computed for. The pod builder cross-checks it
    /// against the pod's trusted runtime org for **every** placement, not only
    /// ones with dedicated capacity. Note what that check is and isn't buying:
    /// a `Generic` group's compatibility has no caller-org term at all, so an
    /// all-shared set for the same generic template is the set *any* org would
    /// have computed, and reusing it across orgs admits nothing extra. The
    /// check is there because an org disagreement means the caller resolved the
    /// set against a different sandbox than the one being rendered, and a
    /// boundary derived from the wrong question is not one to render on a guess
    /// — it is cheap strictness at a security boundary, not escape
    /// prevention.
    pub fn caller_org(&self) -> Option<OrgId> {
        self.caller_org
    }

    /// Template this placement was computed for. The pod builder compares it
    /// against the template it is rendering; a mismatch means the candidate set
    /// belongs to a different sandbox shape and none of it can be trusted.
    pub fn template(&self) -> &TemplateName {
        &self.template
    }

    /// Whether `group` is inside the candidate set — exactly the membership
    /// [`Self::required_match_expression`] renders, so a caller can answer
    /// "would kubelet admit this node?" without re-deriving the set.
    ///
    /// This exists for the one caller that has to: a local-only restore pins
    /// `spec.nodeName`, which skips kube-scheduler, so the pin has to be
    /// pre-checked against the source node's live node-group label. A node
    /// whose label is absent or names a group outside the set is not a node to
    /// pin to — see the `Restore` variant's obligation on
    /// `SandboxPodRequest`.
    pub fn admits_group(&self, group: &str) -> bool {
        self.candidate_groups
            .iter()
            .any(|candidate| candidate.as_str() == group)
    }

    /// Whether the candidate set includes capacity that is **not** the caller's
    /// own dedicated hardware — so whether a pod already running on shared
    /// capacity could satisfy this boundary at all.
    ///
    /// Exact rather than heuristic: the dedicated subset is a deduplicated
    /// subset of the candidate set, both built in the same constructor, so a
    /// strictly larger candidate set is precisely "there is a non-dedicated
    /// group in here".
    ///
    /// The caller this exists for is warm-pool adoption. A pooled pod is built
    /// for no org and lands on shared capacity, so a sandbox whose set is
    /// dedicated-only — every org-specific template — can never adopt one.
    /// The claim has already happened by then, so what answering from the set
    /// alone saves is not the claim but the node read: the verdict holds for
    /// every slot under that pool config, whatever node each one landed on.
    pub fn admits_shared_capacity(&self) -> bool {
        let dedicated = self
            .dedicated
            .as_ref()
            .map_or(0, |dedicated| dedicated.groups.len());
        self.candidate_groups.len() > dedicated
    }

    /// Whether this placement targets dedicated capacity, which is what makes a
    /// dedicated toleration and preference applicable at all.
    pub fn targets_dedicated_capacity(&self) -> bool {
        self.dedicated.is_some()
    }

    /// Preferred scheduling term biasing toward the dedicated subset, when
    /// one is targeted.
    pub fn preferred_dedicated_term(&self) -> Option<PreferredSchedulingTerm> {
        self.dedicated
            .as_ref()
            .map(|dedicated| PreferredSchedulingTerm {
                weight: DEDICATED_NODE_GROUP_PREFERENCE_WEIGHT,
                preference: NodeSelectorTerm {
                    match_expressions: Some(vec![NodeSelectorRequirement {
                        key: SANDBOX_NODE_GROUP_LABEL.to_string(),
                        operator: "In".to_string(),
                        values: Some(
                            dedicated
                                .groups
                                .iter()
                                .map(|group| group.as_str().to_string())
                                .collect(),
                        ),
                    }]),
                    ..Default::default()
                },
            })
    }

    /// Toleration for the owning org's dedicated taint, emitted only when the
    /// placement targets that org's dedicated groups. Sandboxes without a
    /// dedicated target never tolerate any dedicated taint, so a
    /// scheduler-placed pod cannot land on reserved hardware that carries the
    /// taint, even if an affinity term were miscomputed. The qualifier matters:
    /// the group label and the taint are two independent control-plane writes,
    /// so a node already labeled but not yet tainted admits such a pod, which
    /// is why the required boundary is the primary fence and this is a
    /// backstop. It does **not** extend to a
    /// pre-bound pod: `NoSchedule` is scheduler-only, so there a miscomputed
    /// affinity term leaves only kubelet admission between the pod and the
    /// node.
    pub fn dedicated_toleration(&self) -> Option<PodToleration> {
        self.dedicated.as_ref().map(|dedicated| {
            PodToleration::equal_no_schedule(DEDICATED_NODE_TAINT_KEY, dedicated.org_id.to_string())
        })
    }
}

#[cfg(test)]
// Explicit because this module is itself reached through `#[path]`, so rustc
// would resolve a bare `mod tests;` in this file's own directory.
#[path = "placement/tests.rs"]
mod tests;
