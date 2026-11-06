//! Dependency-injection seams so the reconciler core is testable without a
//! live cluster or Postgres:
//!
//! - [`PodBackend`]: enumerate sandbox pods from the reflector cache, re-read
//!   one authoritatively, and delete one under UID + resourceVersion
//!   preconditions. Live impl: [`KubernetesPodBackend`].
//! - [`ExecutionStore`]: read the execution rows that name a batch of pods.
//!   Live impl: [`super::store::ExecutionDbStore`].

use std::collections::HashMap;
use std::sync::Arc;

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, Preconditions};
use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;
use pplx_space_common_core::types::PodUid;

use super::classify::{ExecutionRow, PodSnapshot};

/// Outcome of a pod delete attempt against the apiserver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    /// 404: already gone (benign).
    NotFound,
    /// 409: a UID or resourceVersion precondition failed (benign; skipped).
    StaleIdentity,
}

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("kube api error: {0}")]
    Kube(#[from] kube::Error),
}

pub trait PodBackend: Send + Sync {
    /// True once the sandbox-pod reflector cache has completed its initial
    /// list. The loop skips ticks until then so it never acts on a cold cache.
    fn caches_ready(&self) -> bool;

    /// Snapshot every sandbox pod in the reflector cache that could classify
    /// as reapable (see [`PodSnapshot::worth_classifying`]).
    fn list_pods(&self) -> impl std::future::Future<Output = Vec<PodSnapshot>> + Send;

    /// Authoritative, cache-bypassing re-read of one pod.
    fn read_pod_fresh(
        &self,
        pod_name: &str,
    ) -> impl std::future::Future<Output = Result<Option<PodSnapshot>, BackendError>> + Send;

    /// Delete the pod under UID + resourceVersion preconditions.
    fn delete_pod(
        &self,
        pod_name: &str,
        pod_uid: &PodUid,
        resource_version: Option<&str>,
    ) -> impl std::future::Future<Output = Result<DeleteOutcome, BackendError>> + Send;
}

pub trait ExecutionStore: Send + Sync {
    /// The Kubernetes-managed execution rows whose `pod_name` is in
    /// `pod_names`, keyed by pod name. Names with no row are absent. No lock:
    /// the GC only reaps pods whose execution is retired, and a retired
    /// generation never becomes active again, so a stale read can't cause a
    /// live pod to be reaped.
    fn rows_for_pods(
        &self,
        pod_names: &[String],
    ) -> impl std::future::Future<Output = Result<HashMap<String, ExecutionRow>, sqlx::Error>> + Send;
}

pub struct KubernetesPodBackend {
    substrate: Arc<KubernetesSubstrate>,
}

impl KubernetesPodBackend {
    pub fn new(substrate: Arc<KubernetesSubstrate>) -> Self {
        Self { substrate }
    }

    pub fn namespace(&self) -> &str {
        &self.substrate.k8s.sandbox_namespace
    }
}

impl PodBackend for KubernetesPodBackend {
    fn caches_ready(&self) -> bool {
        self.substrate.readiness().is_ready()
    }

    async fn list_pods(&self) -> Vec<PodSnapshot> {
        self.substrate
            .sandbox_pod_store()
            .state()
            .iter()
            // Skip the snapshot build for pods that can't be reapable; a busy
            // cluster's cache is dominated by Running pods.
            .filter(|pod| !pod.is_deleting() && pod.phase() != Some("Running"))
            .map(|pod| PodSnapshot::from_pod(pod))
            .collect()
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
            Err(kube::Error::Api(err)) if err.code == 404 => Ok(DeleteOutcome::NotFound),
            Err(kube::Error::Api(err)) if err.code == 409 => Ok(DeleteOutcome::StaleIdentity),
            Err(err) => Err(BackendError::Kube(err)),
        }
    }
}
