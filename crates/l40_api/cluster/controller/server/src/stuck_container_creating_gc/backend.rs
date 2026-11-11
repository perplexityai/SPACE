//! Live `GcBackend` backed by the Kubernetes substrate: enumerates sandbox
//! pods from the reflector cache, reads node state from the node reflector
//! cache, re-reads pods authoritatively, and deletes pods via the raw kube
//! client with a UID precondition.

use std::sync::Arc;

use k8s_openapi::api::core::v1::{Node, Pod};
use kube::api::{Api, DeleteParams, Preconditions};
use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;
use pplx_space_common_core::types::PodUid;

use super::classify::{NodeEligibility, PodSnapshot};

/// Outcome of a pod delete attempt against the apiserver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    /// 404 — the pod was already gone (benign).
    NotFound,
    /// 409 — a delete precondition (UID or resourceVersion) failed; the object
    /// changed under us (benign; we skip it).
    StaleIdentity,
}

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// An unexpected apiserver error on the verify GET or DELETE. The benign
    /// 404 / 409 delete outcomes are mapped to [`DeleteOutcome`] before this, so
    /// this variant is the genuinely-unexpected case; the source is preserved
    /// via `#[from]` (so `.source()` chains) and rendered by `{0}` (so
    /// `to_string()` — what the reconciler logs/emits — carries the kube status
    /// and message operators need to diagnose auth / throttling / server errors).
    #[error("kube api error: {0}")]
    Kube(#[from] kube::Error),
}

/// Dependency-injection seam so the reconciler core is testable without a live
/// kube cluster.
pub trait GcBackend: Send + Sync {
    /// True once both the sandbox-pod and node reflector caches have completed
    /// their initial list. The loop skips ticks until then so it never acts on
    /// a cold cache.
    fn caches_ready(&self) -> bool;

    /// Snapshot every sandbox pod currently in the reflector cache.
    fn list_sandbox_pods(&self) -> impl std::future::Future<Output = Vec<PodSnapshot>> + Send;

    /// Node eligibility for `node_name`, from the node reflector cache.
    fn node_eligibility(&self, node_name: &str) -> NodeEligibility;

    /// Authoritative re-read of a single pod (cache-bypassing GET) for the
    /// verify-before-destroy check.
    fn read_pod_fresh(
        &self,
        pod_name: &str,
    ) -> impl std::future::Future<Output = Result<Option<PodSnapshot>, BackendError>> + Send;

    /// Delete the pod, guarded by UID + resourceVersion preconditions so a
    /// same-UID status update between the verify GET and the delete produces a
    /// benign conflict instead of removing a recovered pod.
    fn delete_pod(
        &self,
        pod_name: &str,
        pod_uid: &PodUid,
        resource_version: Option<&str>,
    ) -> impl std::future::Future<Output = Result<DeleteOutcome, BackendError>> + Send;
}

pub struct KubernetesGcBackend {
    substrate: Arc<KubernetesSubstrate>,
}

impl KubernetesGcBackend {
    pub fn new(substrate: Arc<KubernetesSubstrate>) -> Self {
        Self { substrate }
    }
}

impl GcBackend for KubernetesGcBackend {
    fn caches_ready(&self) -> bool {
        self.substrate.readiness().is_ready() && self.substrate.k8s.caches.nodes_ready()
    }

    async fn list_sandbox_pods(&self) -> Vec<PodSnapshot> {
        self.substrate
            .sandbox_pod_store()
            .state()
            .iter()
            .map(|pod| PodSnapshot::from_pod(pod))
            .collect()
    }

    fn node_eligibility(&self, node_name: &str) -> NodeEligibility {
        match self.substrate.k8s.caches.node_by_name(node_name) {
            None => NodeEligibility::Unknown,
            Some(node) => node_eligibility(&node),
        }
    }

    async fn read_pod_fresh(&self, pod_name: &str) -> Result<Option<PodSnapshot>, BackendError> {
        match self.substrate.read_sandbox_pod_uncached(pod_name).await {
            Ok(Some(pod)) => Ok(Some(PodSnapshot::from_pod(&pod))),
            Ok(None) => Ok(None),
            Err(err) => Err(BackendError::Kube(err)),
        }
    }

    async fn delete_pod(
        &self,
        pod_name: &str,
        pod_uid: &PodUid,
        resource_version: Option<&str>,
    ) -> Result<DeleteOutcome, BackendError> {
        let pods: Api<Pod> = Api::namespaced(
            self.substrate.k8s.client.clone(),
            &self.substrate.k8s.sandbox_namespace,
        );
        let params = DeleteParams {
            preconditions: Some(Preconditions {
                uid: Some(pod_uid.as_str().to_string()),
                resource_version: resource_version.map(str::to_string),
            }),
            ..DeleteParams::default()
        };
        match pods.delete(pod_name, &params).await {
            Ok(_) => Ok(DeleteOutcome::Deleted),
            // 404: the pod is already gone. 409: a precondition failed — either
            // a different pod object now holds this name (UID), or the pod's
            // resourceVersion advanced because its status changed (e.g. it went
            // Running) between the verify GET and here. Both are benign.
            Err(kube::Error::Api(err)) if err.code == 404 => Ok(DeleteOutcome::NotFound),
            Err(kube::Error::Api(err)) if err.code == 409 => Ok(DeleteOutcome::StaleIdentity),
            Err(err) => Err(BackendError::Kube(err)),
        }
    }
}

/// Classify a node's fitness for GC action. `node_ready`/`node_schedulable` in
/// the substrate crate are `pub(super)` and unreachable here, so we re-derive
/// the `Ready` condition and `unschedulable` checks (as `kube_node_sync` and
/// `node_drain` do).
fn node_eligibility(node: &Node) -> NodeEligibility {
    if node_cordoned(node) {
        NodeEligibility::Cordoned
    } else if node_ready(node) {
        NodeEligibility::Eligible
    } else {
        NodeEligibility::NotReady
    }
}

fn node_ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        })
}

fn node_cordoned(node: &Node) -> bool {
    node.spec
        .as_ref()
        .and_then(|spec| spec.unschedulable)
        .unwrap_or(false)
}
