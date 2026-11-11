//! Kubernetes implementation of [`super::WorkerNodeLabels`].
//!
//! Reads the shared worker pool out of the cluster-controller's existing
//! watcher-backed Node reflector (`K8sCaches::nodes`) and patches the
//! [`super::NODE_GROUP_LABEL`] with a JSON merge patch, using the
//! cluster-controller's service-account credentials — the only way to set a
//! label under the NodeRestriction-protected prefix.
//!
//! Reflector read, not a per-tick LIST: `substrate/AGENTS.md` forbids naked
//! apiserver LISTs on reconcile-tick paths, and the controller already runs an
//! unselected Node watch, so a second LIST is pure additive load. Shape follows
//! warm-pool's `cached_cordoned_node_names`. A **cold** store is reported as an
//! error rather than an empty fleet — safe either way, but an empty read is
//! indistinguishable from a mis-selected pool, which is the expensive ambiguity
//! during a rollout. Not covered: a reflector whose writer dies after initial
//! sync leaves `nodes_ready()` true and the store frozen.
//!
//! The patch carries the *cached* Node's `resourceVersion` as an
//! optimistic-concurrency precondition, so any intervening modification — most
//! importantly another actor assigning a different group — fails it with a
//! conflict instead of overwriting. Reading that version from a watch-backed
//! cache can only make it staler, never newer, so the guard fails closed.
//!
//! That version is re-read per node immediately before its patch
//! ([`WorkerNodeLabels::current_worker_node`]) rather than taken from the
//! tick-opening scan. The scan's version can be a whole tick old, and the
//! precondition does not distinguish *what* changed: `node_ctl`'s
//! template-presence labeler patches Node labels from a DaemonSet on every
//! worker, so a stale version turns routine unrelated writes into 409s that are
//! indistinguishable from an RBAC 403 in `label_write{outcome:error}`.

use k8s_openapi::api::core::v1::Node;
use kube::Api;
use kube::api::{Patch, PatchParams};
use kube::runtime::reflector::ObjectRef;
use pplx_space_api::controllers::substrate::kubernetes::K8sCaches;
use pplx_space_common_core::kubernetes::{SandboxNodeSelector, SandboxPodConfigValues};

use super::{
    DEDICATED_TAINT_KEY, NODE_GROUP_LABEL, WorkerNode, WorkerNodeLabels, WorkerNodeLabelsError,
};

pub struct KubeWorkerNodes {
    client: kube::Client,
    caches: std::sync::Arc<K8sCaches>,
    selector: SandboxNodeSelector,
}

/// Project a cached Node into a [`WorkerNode`].
///
/// `None` when the object has no name, no uid, or no resourceVersion. The API
/// server sets all three on stored objects; a missing resourceVersion would
/// mean the patch precondition can't be made real, and a missing uid would
/// leave the node out of the cross-tick suppressed/foreign-group baselines
/// (which are keyed on uid, since node names are reused), so the node is
/// skipped and picked up on a later tick rather than acted on unguarded. The
/// explicit three-way check exists only to make the skip loggable; the `?`
/// operators below already yield `None` on their own.
///
/// The skip is effectively unobservable: such a node is absent from
/// `report.workers` and from every gauge, and the only trace is a `debug` line
/// production levels don't emit. Acceptable only because the apiserver sets all
/// three fields on stored objects, so this is unreachable rather than merely
/// rare — if that ever stops holding, the skip needs its own counter.
fn worker_node(node: &Node) -> Option<WorkerNode> {
    if node.metadata.name.is_none()
        || node.metadata.uid.is_none()
        || node.metadata.resource_version.is_none()
    {
        tracing::debug!(
            node = ?node.metadata.name,
            has_name = node.metadata.name.is_some(),
            has_uid = node.metadata.uid.is_some(),
            has_resource_version = node.metadata.resource_version.is_some(),
            "node_group_labeler: skipping worker Node missing name, uid or \
             resourceVersion"
        );
        return None;
    }
    let node_group = node
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(NODE_GROUP_LABEL))
        .cloned();
    let dedicated = node
        .spec
        .as_ref()
        .and_then(|spec| spec.taints.as_ref())
        .is_some_and(|taints| taints.iter().any(|taint| taint.key == DEDICATED_TAINT_KEY));
    Some(WorkerNode {
        name: node.metadata.name.clone()?,
        uid: node.metadata.uid.clone()?,
        resource_version: node.metadata.resource_version.clone()?,
        node_group,
        dedicated,
    })
}

impl KubeWorkerNodes {
    /// Takes the whole [`SandboxPodConfigValues`] so the labeled set is
    /// composed by the shared [`SandboxNodeSelector`] — the same
    /// definition an unpinned sandbox pod is scheduled against. Matching on the
    /// primary `key=value` alone would over-match: the EKS defaults add
    /// `space.pplx.net/subnet-tier=secondary` and `workload=workers` as extras,
    /// and a node carrying only the primary label is not sandbox-schedulable,
    /// so labeling it would put a tenant-isolation label on a node no sandbox
    /// can use. (A create-time isolation-pool pin adds
    /// `SANDBOX_PLACEMENT_POOL_LABEL_KEY` on top, so a pinned pod carries
    /// strictly more constraints and lands on a subset of these nodes —
    /// labeling the wider unpinned set is harmless.)
    pub fn new(
        client: kube::Client,
        caches: std::sync::Arc<K8sCaches>,
        sandbox_pod_config: &SandboxPodConfigValues,
    ) -> Self {
        Self {
            client,
            caches,
            selector: sandbox_pod_config.base_node_selector(),
        }
    }

    fn nodes(&self) -> Api<Node> {
        Api::all(self.client.clone())
    }
}

/// Merge-patch body that sets [`NODE_GROUP_LABEL`]` = value` guarded by the
/// observed `resourceVersion`. Including the resourceVersion in the patched
/// object makes the API server reject the patch with a 409 Conflict when the
/// Node has changed since it was read — the atomicity guard behind the
/// reconciler's never-overwrite invariant.
///
/// Takes the whole [`WorkerNode`] rather than a bare version string on
/// purpose: the guard is the entire point of this body, and a signature that
/// accepts the version separately lets a caller build a valid-looking patch
/// without one. Routing it through here means dropping the precondition
/// requires editing this function, whose test asserts the field is present.
fn label_patch(node: &WorkerNode, value: &str) -> serde_json::Value {
    serde_json::json!({
        "metadata": {
            "resourceVersion": node.resource_version,
            "labels": { NODE_GROUP_LABEL: value }
        }
    })
}

/// Readiness gate plus selector filter, split out from
/// [`WorkerNodeLabels::list_worker_nodes`] so both are testable: a `K8sCaches`
/// has no unit-test-usable constructor out of crate. `K8sCaches::spawn` is
/// `pub`, but it needs a live `kube::Client` and spawns reflector tasks; the
/// one constructor that doesn't is `#[cfg(test)] pub(super)` to
/// `pplx-space-api`. So a test against the trait impl would otherwise be
/// impossible and the gate would ship unasserted.
///
/// A cold cache is an error rather than an empty fleet. Returning `Ok(vec![])`
/// would be *safe* — the reconciler only ever adds labels — but it would be
/// indistinguishable from a genuinely empty or mis-selected worker pool, which
/// is the reading an operator most needs during the rollout.
fn scan_worker_nodes(
    nodes_ready: bool,
    nodes: &[std::sync::Arc<Node>],
    selector: &SandboxNodeSelector,
) -> Result<Vec<WorkerNode>, WorkerNodeLabelsError> {
    if !nodes_ready {
        return Err(WorkerNodeLabelsError::Backend(
            "node reflector cache is not ready; an empty scan would be \
             indistinguishable from an empty cluster"
                .to_string(),
        ));
    }
    Ok(nodes
        .iter()
        .filter(|node| selector.matches(node.as_ref()))
        .filter_map(|node| worker_node(node.as_ref()))
        .collect())
}

/// Re-project a cached Node for the pre-write refresh, keeping every gate the
/// tick-opening scan applied.
///
/// `None` unless the cached object is still a worker-selector match, still
/// projectable, and still the *same* object (`uid`). The uid check is the one
/// that is not merely a repeat of the scan: node names are reused across
/// replacement instances, so without it a refresh could hand back a node this
/// tick never classified — and the gate that makes labeling safe was evaluated
/// against the classified set.
fn refreshed_worker_node(
    cached: &Node,
    uid: &str,
    selector: &SandboxNodeSelector,
) -> Option<WorkerNode> {
    if !selector.matches(cached) {
        return None;
    }
    let fresh = worker_node(cached)?;
    (fresh.uid == uid).then_some(fresh)
}

impl WorkerNodeLabels for KubeWorkerNodes {
    async fn list_worker_nodes(&self) -> Result<Vec<WorkerNode>, WorkerNodeLabelsError> {
        scan_worker_nodes(
            self.caches.nodes_ready(),
            &self.caches.nodes.state(),
            &self.selector,
        )
    }

    async fn current_worker_node(&self, node: &WorkerNode) -> Option<WorkerNode> {
        // A keyed store lookup rather than a rescan of `state()`: this runs once
        // per pending node, and `state()` clones the whole fleet each call.
        //
        // No readiness gate here. `list_worker_nodes` already refused a cold
        // cache, and a store that went cold *after* it can only under-report,
        // which skips the patch — the safe direction.
        let cached = self.caches.nodes.get(&ObjectRef::<Node>::new(&node.name))?;
        refreshed_worker_node(cached.as_ref(), &node.uid, &self.selector)
    }

    async fn label_node(
        &self,
        node: &WorkerNode,
        value: String,
    ) -> Result<(), WorkerNodeLabelsError> {
        let patch = label_patch(node, &value);
        self.nodes()
            .patch(&node.name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
            .map_err(|err| WorkerNodeLabelsError::Backend(err.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
