//! The sandbox pod `nodeSelector` and the node filter that mirrors it.
//!
//! Two kinds of caller need the same answer to "which nodes can host an
//! unpinned sandbox pod": [`build_sandbox_pod`], which renders the selector
//! onto every sandbox pod, and the Kubernetes substrate's node filters, which
//! decide the placement candidate set, the eligible-start capacity count that
//! gates admission, and the Node set node-local state is published onto.
//!
//! Divergence between them is not cosmetic. A filter narrower than the
//! rendered selector under-counts eligible capacity and rejects creates the
//! scheduler would have placed; a filter wider than it counts nodes no
//! sandbox pod can reach.
//!
//! [`SandboxNodeSelector`] and both `base_node_selector` entry points are
//! `pub`, and deliberately so: `pplx-space-api`'s substrate both composes the
//! selector and matches nodes against it from another crate. A composition
//! only some callers can reach is what makes the rest write their own, so a
//! future node-group worker labeler — which needs this same answer to decide
//! which nodes carry the boundary label — should call these rather than mirror
//! them. `node_group_mirror` is not that consumer; see Scope below.
//!
//! Scope: this is the *placement* set, and it is not the only sandbox-node
//! question in the tree. Node registration and inventory (`node_sync`, the
//! warm pool's template-write sweep, `node_group_mirror`) list on the
//! discriminator label alone — deliberately, since a worker missing an extra
//! selector label still needs its `nodes` row and its template prefetch.
//! Normally that makes their set a superset of this one. It stops being a
//! superset if an extra label is configured on the discriminator's own key
//! with a different value, because extras win that collision here (below) and
//! those listers read the configured discriminator value: the two sets then
//! disjoin, and sandbox pods render onto nodes registration never sees.
//! Nothing rejects that configuration today. Do not "fix" those listers by
//! pointing them here — narrowing them drops workers out of inventory, which
//! for `node_group_mirror` means silently retaining a stale `node_group_id`,
//! since it only writes rows it observed.
//!
//! [`build_sandbox_pod`]: super::build_sandbox_pod

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Node;

use super::{SandboxPodConfig, SandboxPodConfigValues};

/// The labels a sandbox pod requires of a node, as `nodeSelector` equality
/// terms.
///
/// A newtype rather than a bare `BTreeMap` so that the empty selector is
/// unrepresentable outside this module. "Carries every entry" degenerates into
/// "matches every node" once there are no entries, and that is the dangerous
/// direction: every filter built on this would *widen* in silence —
/// over-counting eligible-start capacity, shortlisting control-plane nodes for
/// a restore, handing non-worker Nodes to the node-services-ready labeler —
/// and none of them would report it. The two `base_node_selector` entry points
/// are the only constructors reachable from outside this module, and both
/// insert the discriminator entry first, so no caller in another crate can
/// produce an empty one and no call site needs a defensive check.
///
/// That construction guarantee is the whole protection, so keep it: **do not
/// derive `Default`** (the derive is generated inside this module, so it sees
/// the private field, and `SandboxNodeSelector::default()` would then be
/// callable from every crate that can name the type) and do not add an
/// in-module builder. [`Self::matches`]
/// refuses an empty selector as cheap insurance, but it only covers its own
/// exit — [`Self::into_labels`] would still hand `build_sandbox_pod` an empty
/// map, and that renders as `nodeSelector: {}` on an unpinned create, which
/// constrains nothing at all (a pool-pinned one keeps only the pool entry, so
/// it widens to that whole pool). Of the two exits the filter fails closed and
/// the renderer fails open.
///
/// The invariant is "at least one entry", which is not "matches at least one
/// node". A configured empty *value* — `SPACE_SANDBOX_NODE_SELECTOR_LABELS`
/// holding `some.key=` — is a legal selector that no node normally carries
/// (one labelled `some.key: ""` would match, both sides comparing as
/// `Some("")`). That misconfiguration at least fails in the visible
/// direction: eligible-start capacity goes to a confident zero, which cluster
/// admission rejects as `empty_cluster` — while the limiter is enabled. With
/// it disabled the creates are admitted and the pods sit `Unschedulable`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxNodeSelector(BTreeMap<String, String>);

impl SandboxNodeSelector {
    /// True when `node` carries every entry of the selector with the exact
    /// value.
    ///
    /// This is the rule kube-scheduler applies to `nodeSelector`: equality on
    /// every entry, no subset or prefix matching, so a node missing one entry
    /// is not a placement candidate. A node with no labels at all matches
    /// nothing.
    pub fn matches(&self, node: &Node) -> bool {
        // `all()` over no entries is vacuously true, so without this an empty
        // selector would match every node, control-plane and unlabeled ones
        // included. Unreachable through the constructors — see the type doc for
        // what that rests on, and for why this covers only the filter exit —
        // but O(1) to refuse, and refusing is the safe direction here. A plain
        // `if` rather than a `debug_assert!` so it holds in release and stays
        // assertable through this method in the debug profile CI builds.
        if self.0.is_empty() {
            return false;
        }
        let labels = node.metadata.labels.as_ref();
        self.0
            .iter()
            .all(|(key, value)| labels.and_then(|labels| labels.get(key)) == Some(value))
    }

    /// The selector's labels, to render onto a pod spec or to build a matching
    /// node fixture from.
    pub fn into_labels(self) -> BTreeMap<String, String> {
        self.0
    }
}

/// One composition body behind the two `base_node_selector` entry points.
fn compose_base_node_selector(
    key: &str,
    value: &str,
    extra: &BTreeMap<String, String>,
) -> SandboxNodeSelector {
    let mut selector = BTreeMap::new();
    selector.insert(key.to_string(), value.to_string());
    selector.extend(extra.iter().map(|(k, v)| (k.clone(), v.clone())));
    SandboxNodeSelector(selector)
}

// The composition has one body but two entry points, because the two config
// shapes cannot reach each other: `build_sandbox_pod` takes the borrowed
// `SandboxPodConfig<'_>` while the substrate's node filters hold the owned
// `SandboxPodConfigValues`, and converting either direction costs a namespace
// or a clone that the selector has no use for. Inherent methods rather than a
// shared trait so cross-crate callers need no import to reach them.
impl SandboxPodConfig<'_> {
    /// The `nodeSelector` every sandbox pod carries regardless of the request:
    /// the worker discriminator label plus every configured extra label. An
    /// extra label wins a collision on the discriminator's key.
    ///
    /// This is the *fleet-wide* placement set: only what every sandbox pod
    /// asks for, and none of the narrower constraints [`build_sandbox_pod`]
    /// also renders as required. Those are the create-time isolation-pool pin
    /// ([`SANDBOX_PLACEMENT_POOL_LABEL_KEY`], layered onto these labels
    /// afterwards), the node-group affinity term, the evacuation-cohort
    /// affinity, and — each conditionally — the restore source-node
    /// `matchFields` pin (`restore_source_node_hard_pin`, default off) and the
    /// `ssd-placement-blocked` `NotIn` term (`ssd_headroom_tier_affinity`,
    /// default on, and suppressed anyway when the pod is pre-bound to a
    /// `spec.nodeName`, which is narrower than any of them). A fleet-wide
    /// filter wants exactly this base and not a rendered pod's constraints,
    /// which are narrower and would hide every worker outside that one
    /// request's pool, group, cohort, or shortlist.
    ///
    /// The converse is the thing to check before reusing it: this base is
    /// *too wide* to decide a single request, and the filters built on it
    /// compensate only partially. `eligible_start_node_count` and
    /// `network_slot_capacity_preflight` both count fleet capacity for creates
    /// that may carry any of those narrowings, so each can admit a request
    /// whose own subset is full and leave the pod `Unschedulable` rather than
    /// rejecting it up front. Of the five narrowings, only
    /// `ssd-placement-blocked` has a filter-side counterpart at all
    /// (`node_not_placement_blocked`), and the five selector consumers do not
    /// agree on applying it: `eligible_start_node_count`,
    /// `restore_node_shortlist`, and `started_recovery_source_node_available`
    /// do; `network_slot_capacity_preflight` omits it; and
    /// `sandbox_selector_nodes` omits it deliberately, for the reason
    /// documented at its own site. A sixth narrowing would have to be threaded
    /// through each of them by hand.
    ///
    /// One of those consumers runs the *other* way, the narrower-than-the-pod
    /// direction the module header warns about. With `ssd_headroom_tier_affinity`
    /// on — the default, and the condition all three of them gate that check on
    /// — `started_recovery_source_node_available` requires the source node not
    /// be placement-blocked, while the restore it gates pre-binds
    /// `spec.nodeName` and so renders no `ssd-placement-blocked` term at all.
    /// The filter is stricter than the pod it admits, and the cost is a
    /// `restore_source_pending` handoff rather than a rejected create.
    ///
    /// [`SANDBOX_PLACEMENT_POOL_LABEL_KEY`]: super::SANDBOX_PLACEMENT_POOL_LABEL_KEY
    /// [`build_sandbox_pod`]: super::build_sandbox_pod
    pub fn base_node_selector(&self) -> SandboxNodeSelector {
        compose_base_node_selector(
            self.sandbox_node_selector_key,
            self.sandbox_node_selector_value,
            self.extra_node_selector_labels,
        )
    }
}

impl SandboxPodConfigValues {
    /// See [`SandboxPodConfig::base_node_selector`].
    pub fn base_node_selector(&self) -> SandboxNodeSelector {
        compose_base_node_selector(
            &self.sandbox_node_selector_key,
            &self.sandbox_node_selector_value,
            &self.extra_node_selector_labels,
        )
    }
}

#[cfg(test)]
mod tests;
