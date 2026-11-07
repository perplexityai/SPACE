//! On Kubernetes, how many bound sandbox pods already on a node, running or
//! still starting, are on nodes the environment placement gate, on, would not
//! have placed them on, as the
//! `environment_placement.running_pods` gauge
//! (`pplx_space_api::metrics::environment_placement`).
//!
//! kube-scheduler chooses a pod's node, so on Kubernetes "would the gate
//! refuse this placement" can be asked only after the fact. While the gate is
//! shadow or on, every bound pod records its environment in
//! `SANDBOX_ENVIRONMENT_ANNOTATION`, and each node-ctl labels its node with the
//! environments it can run (`environment_label_key`). Each tick compares the
//! two for every such pod, from the controller's pod and Node
//! reflectors: no apiserver reads. A node with no environment label at all
//! counts as `no_evidence`, and one labeled with other environments as
//! `environment_not_usable`. Labels carry no receipt time, so stale and
//! incomplete evidence, which Direct tells apart, read the same here.
//!
//! A running pod is one the gate already let through, so the same tick also
//! counts the work the gate holds back, as `environment_placement.pending_pods`:
//! bound pods built with the gate on, ruled unschedulable, whose environment
//! label no node carries. With no environment label anywhere in the fleet
//! that's `no_evidence`, otherwise `environment_not_usable`. A pod whose label
//! some node carries is left out: something else keeps it off that node.
//!
//! A Kubernetes loop over Kubernetes objects, so it runs only on that
//! substrate; Direct's shadow mode counts its refusals as it places. It runs
//! on the elected leader only (`run_reconcilers`). Combine replicas with `max`,
//! as for the other censuses. Off, it emits nothing: no pod records an
//! environment.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kube::runtime::reflector::Store;
use pplx_space_api::controllers::substrate::environment_placement::EnvironmentPlacementGate;
use pplx_space_api::controllers::substrate::kubernetes::{
    K8sCaches, ReflectorReady, SandboxPodKey, SandboxPodView,
};
use pplx_space_api::metrics::environment_placement::{
    self as metrics, PendingPodReason, RunningPodPlacement,
};
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_sandbox_pod::{node_publishes_any_environment, node_publishes_environment};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests;

/// One census a minute.
const TICK: Duration = Duration::from_secs(60);

/// Every placement the census reports, in the order it reports them.
pub(crate) const PLACEMENTS: [RunningPodPlacement; 3] = [
    RunningPodPlacement::Admitted,
    RunningPodPlacement::NoEvidence,
    RunningPodPlacement::EnvironmentNotUsable,
];

/// Every reason the census reports a pending pod for, in the order it
/// reports them.
pub(crate) const PENDING_REASONS: [PendingPodReason; 2] = [
    PendingPodReason::NoEvidence,
    PendingPodReason::EnvironmentNotUsable,
];

/// One tick's counts, every placement and reason present, zero included,
/// so a gauge falls to zero rather than holding its last value.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Census {
    pub(crate) running: [(RunningPodPlacement, usize); 3],
    pub(crate) pending: [(PendingPodReason, usize); 2],
}

/// The reflectors the census reads.
pub(crate) struct Caches {
    pub(crate) pods: Store<SandboxPodView>,
    pub(crate) pods_ready: ReflectorReady,
    pub(crate) nodes: Arc<K8sCaches>,
}

pub(crate) async fn run(
    gate: EnvironmentPlacementGate,
    caches: Caches,
    shutdown_token: CancellationToken,
) {
    if !gate.evaluates() {
        tracing::info!(%gate, "environment placement census disabled with the gate off");
        return;
    }
    tracing::info!(%gate, tick_secs = TICK.as_secs(), "starting environment placement census");
    let mut interval = tokio::time::interval(TICK);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => tick(&caches),
            _ = shutdown_token.cancelled() => {
                tracing::info!("environment placement census stopping");
                return;
            }
        }
    }
}

/// Count and emit. A cold reflector emits nothing rather than counts it
/// didn't read.
fn tick(caches: &Caches) {
    if !caches.pods_ready.is_ready() || !caches.nodes.nodes_ready() {
        tracing::debug!("environment placement census: reflectors not ready; skipping");
        return;
    }
    let pods = caches.pods.state();
    let nodes = caches.nodes.nodes.state();
    let node_labels: BTreeMap<&str, &BTreeMap<String, String>> = nodes
        .iter()
        .filter_map(|node| {
            let name = node.metadata.name.as_deref()?;
            Some((name, node.metadata.labels.as_ref().unwrap_or(&NO_LABELS)))
        })
        .collect();
    let counts = census(pods.iter().map(|pod| pod.as_ref()), &node_labels);
    for (placement, count) in counts.running {
        metrics::running_pods(placement, count);
    }
    for (reason, count) in counts.pending {
        metrics::pending_pods(reason, count);
    }
}

/// The labels of a node that has none.
static NO_LABELS: BTreeMap<String, String> = BTreeMap::new();

/// How many bound pods on a node sit on nodes of each [`RunningPodPlacement`],
/// and how many pending ones no node can take, by [`PendingPodReason`].
/// `nodes` is every node the cache has, by name, with its labels.
///
/// A pod on a node counts when it records an environment, is bound to a node
/// the cache has, isn't being deleted, and hasn't finished. A pending pod
/// counts when it records an environment, requires its label, has no node,
/// isn't being deleted, the scheduler has ruled it unschedulable, and no node
/// publishes its environment. A pod whose annotation doesn't parse is logged
/// and skipped.
pub(crate) fn census<'a>(
    pods: impl IntoIterator<Item = &'a SandboxPodView>,
    nodes: &BTreeMap<&str, &BTreeMap<String, String>>,
) -> Census {
    let mut counts = Census {
        running: PLACEMENTS.map(|placement| (placement, 0)),
        pending: PENDING_REASONS.map(|reason| (reason, 0)),
    };
    for pod in pods {
        let Some(recorded) = pod.annotation(SandboxPodKey::Environment) else {
            continue;
        };
        if pod.is_deleting() || pod.is_terminal() {
            continue;
        }
        let environment: EnvironmentReference = match recorded.parse() {
            Ok(environment) => environment,
            Err(error) => {
                tracing::warn!(pod = pod.name(), %error, recorded, "environment placement census: unparseable environment annotation");
                continue;
            }
        };
        match pod.node_name() {
            Some(node) => {
                if let Some(labels) = nodes.get(node) {
                    bump(&mut counts.running, placement(labels, &environment));
                }
            }
            None => {
                if let Some(reason) = pending_reason(pod, &environment, nodes) {
                    bump(&mut counts.pending, reason);
                }
            }
        }
    }
    counts
}

fn bump<K: PartialEq>(counts: &mut [(K, usize)], key: K) {
    if let Some((_, count)) = counts.iter_mut().find(|(each, _)| *each == key) {
        *count += 1;
    }
}

/// Why no node can take `pod`, which isn't scheduled yet, or `None` when the
/// environment isn't
/// what holds it: it doesn't require the label, the scheduler hasn't ruled it
/// unschedulable, or some node publishes the environment.
fn pending_reason(
    pod: &SandboxPodView,
    environment: &EnvironmentReference,
    nodes: &BTreeMap<&str, &BTreeMap<String, String>>,
) -> Option<PendingPodReason> {
    if !pod.requires_environment_label()
        || pod.unschedulable_message().is_none()
        || nodes
            .values()
            .any(|labels| node_publishes_environment(labels, environment))
    {
        return None;
    }
    Some(
        if nodes
            .values()
            .any(|labels| node_publishes_any_environment(labels))
        {
            PendingPodReason::EnvironmentNotUsable
        } else {
            PendingPodReason::NoEvidence
        },
    )
}

fn placement(
    labels: &BTreeMap<String, String>,
    environment: &EnvironmentReference,
) -> RunningPodPlacement {
    if node_publishes_environment(labels, environment) {
        RunningPodPlacement::Admitted
    } else if node_publishes_any_environment(labels) {
        RunningPodPlacement::EnvironmentNotUsable
    } else {
        RunningPodPlacement::NoEvidence
    }
}
