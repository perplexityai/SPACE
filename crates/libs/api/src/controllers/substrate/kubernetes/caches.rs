//! Watcher-backed caches for node-api Pods and cluster Nodes — used to
//! resolve per-node-api gRPC endpoints and look up Nodes without hitting the
//! API server on every cluster-plane request.

use std::time::Duration;

use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{
    Api, Client,
    runtime::{
        reflector::{self, Store},
        watcher,
    },
};
use pplx_space_common_core::types::NodeRole as SpaceNodeRole;
use pplx_space_common_svc::kube_client::spawn_reflector;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const NODE_API_COMPONENT_LABEL: &str = "app.kubernetes.io/component=node-api";
const STORE_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Shared, watcher-backed views over the Kubernetes objects the cluster plane
/// reads frequently. Constructed once at startup and cloned via `Arc`.
#[derive(Clone)]
pub struct K8sCaches {
    pub node_api_pods: Store<Pod>,
    pub nodes: Store<Node>,
    /// Flips true once the node reflector's initial `LIST` completes. Read by
    /// the eligible-start-node count to tell a *cold* cache (LIST not done →
    /// capacity unknown → admission fails open) apart from a *warm* cache that
    /// genuinely holds zero nodes (a confident empty cluster). A bare
    /// `nodes.state().is_empty()` collapses those two, so a total node outage
    /// would read as "unknown" and fail open forever.
    nodes_ready: super::pod_reflector::ReflectorReady,
}

impl K8sCaches {
    /// Spawn the reflector tasks. Does not block on first-list — call
    /// [`K8sCaches::wait_until_ready`] if the caller wants to wait.
    pub fn spawn(client: Client, node_api_namespace: &str) -> Self {
        let pod_api: Api<Pod> = Api::namespaced(client.clone(), node_api_namespace);
        let pod_config = watcher::Config::default().labels(NODE_API_COMPONENT_LABEL);
        let (pod_store, pod_writer) = reflector::store::<Pod>();
        spawn_reflector(
            "node-api pods",
            pod_writer,
            pod_api,
            pod_config,
            CancellationToken::new(),
        );

        let node_api: Api<Node> = Api::all(client);
        let node_config = watcher::Config::default();
        let (node_store, node_writer) = reflector::store::<Node>();
        spawn_reflector(
            "nodes",
            node_writer,
            node_api,
            node_config,
            CancellationToken::new(),
        );

        // Mark the node cache ready once its initial LIST populates the store.
        // Independent of the best-effort startup `wait_until_ready` gate below:
        // this flag stays a durable readiness signal for the lifetime of the
        // process even if that startup wait timed out on a cold cache.
        let nodes_ready = super::pod_reflector::ReflectorReady::new();
        let nodes_ready_task = nodes_ready.clone();
        let node_store_for_ready = node_store.clone();
        tokio::spawn(async move {
            match node_store_for_ready.wait_until_ready().await {
                Ok(()) => {
                    nodes_ready_task.mark_ready();
                    tracing::info!(
                        node_count = node_store_for_ready.len(),
                        "node reflector initial sync complete; marked ready"
                    );
                }
                Err(error) => {
                    nodes_ready_task.mark_writer_dropped();
                    tracing::warn!(
                        %error,
                        "node reflector writer dropped before initial sync; readiness will stay false until the process is restarted"
                    );
                }
            }
        });

        Self {
            node_api_pods: pod_store,
            nodes: node_store,
            nodes_ready,
        }
    }

    /// Whether the node reflector has completed its initial `LIST`. When false,
    /// an empty node store means "capacity unknown", not "zero nodes".
    pub fn nodes_ready(&self) -> bool {
        self.nodes_ready.is_ready()
    }

    /// Assemble caches from pre-populated stores, with explicit node readiness.
    /// The `nodes_ready` field is private, so sibling test modules build caches
    /// through this constructor instead of a struct literal.
    #[cfg(test)]
    pub(super) fn for_test(
        node_api_pods: Store<Pod>,
        nodes: Store<Node>,
        nodes_ready: bool,
    ) -> Self {
        let ready = super::pod_reflector::ReflectorReady::new();
        if nodes_ready {
            ready.mark_ready();
        }
        Self {
            node_api_pods,
            nodes,
            nodes_ready: ready,
        }
    }

    /// Wait for both stores' initial list (with timeout). Returns `Err` if the
    /// writer dropped or the deadline elapses; callers can choose whether to
    /// fail startup or proceed with a cold cache.
    pub async fn wait_until_ready(&self) -> Result<(), CacheReadyError> {
        let pods_ready = self.node_api_pods.wait_until_ready();
        let nodes_ready = self.nodes_ready.wait_until_ready();
        match timeout(STORE_READY_TIMEOUT, async {
            tokio::try_join!(
                async { pods_ready.await.map_err(|_| CacheReadyError::WriterDropped) },
                async {
                    nodes_ready
                        .await
                        .map_err(|_| CacheReadyError::WriterDropped)
                },
            )?;
            Ok(())
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(CacheReadyError::Timeout),
        }
    }

    /// Find a Ready node-api Pod for a specific (k8s node, SPACE role) pair.
    /// Worker and builder DaemonSets can land on the same Kubernetes node in
    /// dev clusters, so their recognized `app.kubernetes.io/name` identities
    /// disambiguate them.
    pub fn ready_node_api_pod_on(
        &self,
        k8s_node_name: &str,
        role: &str,
    ) -> Option<std::sync::Arc<Pod>> {
        let expected_role = SpaceNodeRole::parse(role).ok()?;
        self.node_api_pods.find(|pod| {
            if pod.spec.as_ref().and_then(|s| s.node_name.as_deref()) != Some(k8s_node_name) {
                return false;
            }
            if pod_app_name(pod).and_then(role_from_app_name) != Some(expected_role.as_str()) {
                return false;
            }
            pod_is_ready(pod)
        })
    }

    /// Look up a cluster Node by name from the watcher-backed store.
    pub fn node_by_name(&self, name: &str) -> Option<std::sync::Arc<Node>> {
        self.nodes
            .find(|n| n.metadata.name.as_deref() == Some(name))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CacheReadyError {
    #[error("reflector writer dropped before first list completed")]
    WriterDropped,
    #[error("timed out waiting for reflector first list")]
    Timeout,
}

/// Read the pod IP from a Pod's status, if assigned.
pub fn pod_ip(pod: &Pod) -> Option<&str> {
    pod.status.as_ref().and_then(|s| s.pod_ip.as_deref())
}

const NODE_API_PORT_ANNOTATION: &str = "space.pplx.net/node-api-port";

/// Read the node-api gRPC port advertised by a Pod, if present.
///
/// The node-api DaemonSet intentionally does not declare `containerPort`: for
/// hostNetwork pods Kubernetes defaults declared container ports into hostPorts,
/// which blocks DaemonSet surge scheduling. The chart advertises the listener
/// port as a pod annotation instead. Older pods may still carry the named
/// `grpc` containerPort, so keep that as a backwards-compatible fallback.
pub fn pod_node_api_port(pod: &Pod) -> Option<u16> {
    pod.metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(NODE_API_PORT_ANNOTATION))
        .and_then(|port| port.parse::<u16>().ok())
        .or_else(|| pod_grpc_port(pod))
}

fn pod_grpc_port(pod: &Pod) -> Option<u16> {
    pod.spec
        .as_ref()?
        .containers
        .iter()
        .flat_map(|c| c.ports.as_ref().into_iter().flatten())
        .find(|p| p.name.as_deref() == Some("grpc"))
        .and_then(|p| u16::try_from(p.container_port).ok())
}

/// Read the `app.kubernetes.io/name` label off a Pod, if present.
pub fn pod_app_name(pod: &Pod) -> Option<&str> {
    pod.metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get("app.kubernetes.io/name"))
        .map(String::as_str)
}

/// Map a recognized node-api application identity to its SPACE node role.
pub fn role_from_app_name(app_name: &str) -> Option<&'static str> {
    match app_name {
        "space-worker" | "space-worker-canary" => Some(SpaceNodeRole::Worker.as_str()),
        "space-builder" => Some(SpaceNodeRole::Builder.as_str()),
        _ => None,
    }
}

/// True when the Pod has a `Ready=True` condition.
pub fn pod_is_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .map(|conds| {
            conds
                .iter()
                .any(|c| c.type_ == "Ready" && c.status == "True")
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests;
