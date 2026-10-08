//! Dependency-injection seams so the reconciler core is testable without a
//! live cluster or Postgres:
//!
//! - [`PodBackend`] — enumerate restore pods from the reflector cache, re-read
//!   one authoritatively, and delete one with a UID + resourceVersion
//!   precondition. Live impl: [`KubernetesPodBackend`].
//! - [`SandboxStore`] — classify the owning `sandboxes` rows for a batch of
//!   ids. Live impl: [`super::store::SandboxDbStore`].

use std::collections::HashMap;
use std::sync::Arc;

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, Preconditions};
use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;
use pplx_space_common_core::types::{PodUid, SandboxId};

use super::classify::{RestorePodSnapshot, SandboxRestoreState};

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
    /// 404 / 409 delete outcomes are mapped to [`DeleteOutcome`] before this,
    /// so this variant is the genuinely-unexpected case; the source is
    /// preserved via `#[from]` and rendered by `{0}` so `to_string()` carries
    /// the kube status/message operators need.
    #[error("kube api error: {0}")]
    Kube(#[from] kube::Error),
}

/// Kubernetes pod access. All reads are cache-first except [`read_pod_fresh`],
/// which is an authoritative cache-bypassing GET for the verify-before-destroy
/// step.
pub trait PodBackend: Send + Sync {
    /// True once the sandbox-pod reflector cache has completed its initial
    /// list. The loop skips ticks until then so it never acts on a cold cache.
    fn caches_ready(&self) -> bool;

    /// Snapshot every *restore* pod currently in the reflector cache
    /// (fresh-create pods carry no restore annotation and are excluded).
    fn list_restore_pods(
        &self,
    ) -> impl std::future::Future<Output = Vec<RestorePodSnapshot>> + Send;

    /// Authoritative re-read of a single pod (cache-bypassing GET) for the
    /// verify-before-destroy check.
    fn read_pod_fresh(
        &self,
        pod_name: &str,
    ) -> impl std::future::Future<Output = Result<Option<RestorePodSnapshot>, BackendError>> + Send;

    /// Delete the pod, guarded by UID + resourceVersion preconditions so a
    /// same-name pod from a newer generation, or a same-UID pod whose status
    /// changed (e.g. it bound to a node) between the verify GET and the delete,
    /// produces a benign conflict instead of a collateral removal.
    fn delete_pod(
        &self,
        pod_name: &str,
        pod_uid: &PodUid,
        resource_version: Option<&str>,
    ) -> impl std::future::Future<Output = Result<DeleteOutcome, BackendError>> + Send;
}

/// Postgres access: classify the owning rows for a set of sandbox ids.
pub trait SandboxStore: Send + Sync {
    /// Look up the lifecycle + active-execution generation for each id in
    /// `ids`. Ids with no `sandboxes` row
    /// are simply absent from the map (the caller treats a missing entry as
    /// `SandboxMissing`). No lock: the reaper only deletes provably-superseded
    /// pods (see [`super::classify::classify_disposition`]), which no restore
    /// can adopt, so a stale read can never cause a live pod to be reaped.
    fn classify_sandboxes(
        &self,
        ids: &[SandboxId],
    ) -> impl std::future::Future<
        Output = Result<HashMap<SandboxId, SandboxRestoreState>, sqlx::Error>,
    > + Send;
}

pub struct KubernetesPodBackend {
    substrate: Arc<KubernetesSubstrate>,
}

impl KubernetesPodBackend {
    pub fn new(substrate: Arc<KubernetesSubstrate>) -> Self {
        Self { substrate }
    }
}

impl PodBackend for KubernetesPodBackend {
    fn caches_ready(&self) -> bool {
        self.substrate.readiness().is_ready()
    }

    async fn list_restore_pods(&self) -> Vec<RestorePodSnapshot> {
        self.substrate
            .sandbox_pod_store()
            .state()
            .iter()
            // Gate the (allocating) snapshot build on the cheap annotation
            // check so the per-tick cost scales with the restore-pod subset,
            // not every sandbox pod in the cache. A restored sandbox's pod
            // keeps the annotation after the restore completes, so the
            // sandbox-row disposition, not this filter, decides orphanhood.
            .filter(|pod| pod.has_restore_annotation())
            .map(|pod| RestorePodSnapshot::from_pod(pod))
            .collect()
    }

    async fn read_pod_fresh(
        &self,
        pod_name: &str,
    ) -> Result<Option<RestorePodSnapshot>, BackendError> {
        match self.substrate.read_sandbox_pod_uncached(pod_name).await {
            Ok(Some(pod)) => Ok(Some(RestorePodSnapshot::from_pod(&pod))),
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
