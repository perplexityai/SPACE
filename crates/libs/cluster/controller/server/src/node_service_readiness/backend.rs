//! Live backend: a watcher-backed view of the node-local DaemonSet pods, the
//! node reflector the substrate already runs, and the Node label write.

use std::str::FromStr;
use std::sync::Arc;

use k8s_openapi::api::core::v1::{Node, Pod};
use kube::Api;
use kube::api::{Patch, PatchParams};
use kube::runtime::reflector::Store;
use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;

use super::{
    NodeLabelState, NodeLocalComponent, NodeServicesReady, ReconcilerConfig, ServicePod,
    node_label_apply,
};

/// Owns exactly the `node-services-ready` label key, so server-side apply
/// never contends with node-ctl's own placement labels on the same Node.
const FIELD_MANAGER: &str = "space-cluster-controller-node-services-ready";

const COMPONENT_LABEL: &str = "app.kubernetes.io/component";

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// The apiserver rejected the label patch. The source is preserved via
    /// `#[from]` and rendered by `{0}`, so the logged line carries the kube
    /// status an operator needs to tell RBAC (`nodes/patch` missing) apart from
    /// throttling or a server error.
    #[error("kube api error: {0}")]
    Kube(#[from] kube::Error),
}

/// Dependency-injection seam so the reconcile core is testable without a live
/// cluster.
pub trait ReadinessBackend: Send + Sync {
    /// True once the node-local service pod watch has completed its initial
    /// `LIST`. Acting on a cold pod cache would mark the whole fleet not-ready.
    fn caches_ready(&self) -> bool;

    /// Worker Nodes to publish a verdict on, with their current label value.
    /// `None` while the node reflector is cold.
    fn placement_nodes(&self) -> Option<Vec<NodeLabelState>>;

    /// Node-local service pods that are serving right now.
    fn serving_pods(&self) -> Vec<ServicePod>;

    /// One node's verdict from the *current* cache, for the re-check the writer
    /// does immediately before patching. The default body goes through
    /// [`Self::serving_pods`]; the live backend overrides it with a scan that
    /// materializes nothing, since this runs once per patch.
    fn verdict_now(&self, node_name: &str, components: &[NodeLocalComponent]) -> NodeServicesReady {
        super::verdict_from_pods(node_name, &self.serving_pods(), components)
    }

    fn set_label(
        &self,
        node_name: &str,
        desired: NodeServicesReady,
    ) -> impl std::future::Future<Output = Result<(), BackendError>> + Send;
}

pub struct KubernetesReadinessBackend {
    substrate: Arc<KubernetesSubstrate>,
    pods: Store<Pod>,
    pods_ready: Arc<std::sync::atomic::AtomicBool>,
}

impl KubernetesReadinessBackend {
    /// Spawn the node-local service pod reflector. Scoped by label selector to
    /// the configured components, so the watch carries the DaemonSet pods only
    /// — not the sandbox pods, which are far more numerous and already
    /// reflected elsewhere.
    pub fn spawn(substrate: Arc<KubernetesSubstrate>, config: &ReconcilerConfig) -> Self {
        let client = substrate.k8s.client.clone();
        let namespace = node_service_namespace();
        let api: Api<Pod> = Api::namespaced(client, &namespace);
        let (store, pods_ready) = crate::pod::spawn_labeled_pod_reflector(
            api,
            &component_selector(config),
            "node-local service pods",
        );

        tracing::info!(%namespace, "node-local service pod reflector started");
        Self {
            substrate,
            pods: store,
            pods_ready,
        }
    }
}

impl ReadinessBackend for KubernetesReadinessBackend {
    fn caches_ready(&self) -> bool {
        self.pods_ready.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn placement_nodes(&self) -> Option<Vec<NodeLabelState>> {
        Some(
            self.substrate
                .sandbox_selector_nodes()?
                .iter()
                .filter_map(|node| node_label_state(node))
                .collect(),
        )
    }

    fn serving_pods(&self) -> Vec<ServicePod> {
        self.pods
            .state()
            .iter()
            .filter_map(|pod| serving_pod(pod))
            .collect()
    }

    /// Allocation-free override of the default: the pre-patch re-check runs once
    /// per patch, so it scans the store for this node's serving components
    /// instead of materializing the whole fleet's pod list each time.
    fn verdict_now(&self, node_name: &str, components: &[NodeLocalComponent]) -> NodeServicesReady {
        let serving_here: Vec<NodeLocalComponent> = self
            .pods
            .state()
            .iter()
            .filter_map(|pod| serving_pod(pod))
            .filter(|pod| pod.node_name == node_name)
            .map(|pod| pod.component)
            .collect();
        if components
            .iter()
            .all(|component| serving_here.contains(component))
        {
            NodeServicesReady::Ready
        } else {
            NodeServicesReady::NotReady
        }
    }

    async fn set_label(
        &self,
        node_name: &str,
        desired: NodeServicesReady,
    ) -> Result<(), BackendError> {
        let nodes: Api<Node> = Api::all(self.substrate.k8s.client.clone());
        nodes
            .patch(
                node_name,
                &PatchParams::apply(FIELD_MANAGER).force(),
                &Patch::Apply(node_label_apply(node_name, desired)),
            )
            .await?;
        Ok(())
    }
}

/// Namespace the node-local DaemonSets run in. Mirrors how the substrate
/// resolves the node-api namespace, since these are the same worker-release
/// pods — including the precedence: an explicit `SPACE_NODE_API_NAMESPACE`
/// wins, `POD_NAMESPACE` (the controller's own namespace) is the fallback.
fn node_service_namespace() -> String {
    std::env::var("SPACE_NODE_API_NAMESPACE")
        .or_else(|_| std::env::var("POD_NAMESPACE"))
        .ok()
        .map(|namespace| namespace.trim().to_string())
        .filter(|namespace| !namespace.is_empty())
        .unwrap_or_else(|| "space".to_string())
}

/// Set-based label selector over the configured components, so the watch
/// carries one stream for all of them.
pub(super) fn component_selector(config: &ReconcilerConfig) -> String {
    let components: Vec<&str> = config
        .components
        .iter()
        .map(|component| component.as_str())
        .collect();
    format!("{COMPONENT_LABEL} in ({})", components.join(","))
}

pub(super) fn node_label_state(node: &Node) -> Option<NodeLabelState> {
    let name = node.metadata.name.clone()?;
    let current = node
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| {
            labels.get(pplx_space_common_core::kubernetes::NODE_SERVICES_READY_LABEL)
        })
        .cloned();
    Some(NodeLabelState { name, current })
}

/// A pod counts as serving only when it is bound to a node, not terminating,
/// and reporting `Ready`. Terminating matters: a pod under deletion keeps its
/// `Ready` condition until the container actually stops, so without this check
/// the signal would trail the outage it exists to predict.
pub(super) fn serving_pod(pod: &Pod) -> Option<ServicePod> {
    if pod.metadata.deletion_timestamp.is_some() {
        return None;
    }
    let node_name = pod.spec.as_ref()?.node_name.clone()?;
    let component = pod
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(COMPONENT_LABEL))
        .and_then(|value| NodeLocalComponent::from_str(value).ok())?;
    let conditions = pod
        .status
        .as_ref()
        .and_then(|status| status.conditions.as_deref());
    if !crate::pod::pod_ready(conditions) {
        return None;
    }
    Some(ServicePod {
        node_name,
        component,
    })
}
