//! Live backend: a watcher-backed view of the data-plane pods, the HTTP read of
//! each pod's internal stream-load report, and the annotation patch.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::api::{Patch, PatchParams};
use kube::runtime::reflector::Store;
use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;
use pplx_space_common_core::data_plane_load::{
    DATA_PLANE_INTERNAL_PORT_NAME, DATA_PLANE_STREAM_LOAD_PATH, DataPlaneStreamLoad,
};
use tracing::Instrument;

use super::{
    DataPlanePod, DataPlanePods, DeletionCost, POD_DELETION_COST_ANNOTATION, ReconcilerConfig,
};

const FIELD_MANAGER: &str = "space-cluster-controller-data-plane-deletion-cost";

#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    #[error("pod IP {0:?} is not an IP address")]
    PodIp(String),
    #[error("stream-load request failed: {0}")]
    Request(#[source] reqwest::Error),
    #[error("stream-load request returned HTTP {0}")]
    Status(u16),
    #[error("stream-load body is not a valid report: {0}")]
    Body(#[source] reqwest::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// The apiserver rejected the patch. A UID mismatch (the pod was replaced
    /// under the same name) surfaces here too, which is the intended outcome.
    #[error("kube api error: {0}")]
    Kube(#[from] kube::Error),
}

/// Dependency-injection seam so the reconcile core is testable without a live
/// cluster.
pub trait DeletionCostBackend: Send + Sync {
    /// The data-plane pods, or `None` while the pod reflector has not finished
    /// its initial `LIST`.
    fn pods(&self) -> Option<DataPlanePods>;

    fn probe(
        &self,
        pod: &DataPlanePod,
    ) -> impl std::future::Future<Output = Result<DataPlaneStreamLoad, ProbeError>> + Send;

    fn set_cost(
        &self,
        pod: &DataPlanePod,
        cost: DeletionCost,
    ) -> impl std::future::Future<Output = Result<(), BackendError>> + Send;
}

pub struct KubernetesDeletionCostBackend {
    pods_api: Api<Pod>,
    pods: Store<Pod>,
    pods_ready: Arc<AtomicBool>,
    http: reqwest::Client,
}

impl KubernetesDeletionCostBackend {
    /// Spawn the data-plane pod reflector, scoped by label selector to one
    /// namespace.
    pub fn spawn(
        substrate: Arc<KubernetesSubstrate>,
        config: &ReconcilerConfig,
    ) -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            // Pod IPs are in-cluster; an `HTTP_PROXY` in the controller's
            // environment must not route the probe out of it.
            .no_proxy()
            // The reconciler bounds each probe with `probe_timeout`; this is
            // the backstop for a caller that does not. Longer than the outer
            // bound so a hung probe is always classified as a timeout.
            .timeout(config.probe_timeout + Duration::from_secs(1))
            .build()?;

        let pods_api: Api<Pod> = Api::namespaced(substrate.k8s.client.clone(), &config.namespace);
        // A reflector rather than a per-tick `LIST`, per the substrate's
        // API-access rule; the selector keeps it to the data-plane replicas.
        let (store, pods_ready) = crate::pod::spawn_labeled_pod_reflector(
            pods_api.clone(),
            &config.pod_selector,
            "data-plane pods",
        );

        tracing::info!(
            namespace = %config.namespace,
            selector = %config.pod_selector,
            "data-plane pod reflector started"
        );
        Ok(Self {
            pods_api,
            pods: store,
            pods_ready,
            http,
        })
    }
}

impl DeletionCostBackend for KubernetesDeletionCostBackend {
    fn pods(&self) -> Option<DataPlanePods> {
        if !self.pods_ready.load(Ordering::Relaxed) {
            return None;
        }
        let pods = self.pods.state();
        Some(DataPlanePods {
            candidates: pods.iter().filter_map(|pod| candidate_pod(pod)).collect(),
            live_uids: pods.iter().filter_map(|pod| live_uid(pod)).collect(),
        })
    }

    async fn probe(&self, pod: &DataPlanePod) -> Result<DataPlaneStreamLoad, ProbeError> {
        let ip: IpAddr = pod
            .pod_ip
            .parse()
            .map_err(|_| ProbeError::PodIp(pod.pod_ip.clone()))?;
        // `SocketAddr`'s `Display` brackets an IPv6 address, which a URL needs.
        let url = format!(
            "http://{}{DATA_PLANE_STREAM_LOAD_PATH}",
            SocketAddr::new(ip, pod.port)
        );
        let span = tracing::info_span!(
            "http.client",
            otel.kind = "client",
            otel.name = "GET data-plane stream-load",
            otel.status_code = tracing::field::Empty,
            "operation.name" = "http.client",
            http.request.method = "GET",
            server.address = %ip,
            server.port = pod.port,
            k8s.pod.name = %pod.name,
        );
        let result = async {
            let response = self
                .http
                .get(&url)
                .send()
                .await
                .map_err(ProbeError::Request)?;
            let status = response.status();
            if !status.is_success() {
                return Err(ProbeError::Status(status.as_u16()));
            }
            response
                .json::<DataPlaneStreamLoad>()
                .await
                .map_err(ProbeError::Body)
        }
        .instrument(span.clone())
        .await;
        span.record(
            "otel.status_code",
            if result.is_ok() { "OK" } else { "ERROR" },
        );
        result
    }

    async fn set_cost(&self, pod: &DataPlanePod, cost: DeletionCost) -> Result<(), BackendError> {
        self.pods_api
            .patch(
                &pod.name,
                &PatchParams {
                    field_manager: Some(FIELD_MANAGER.to_string()),
                    ..PatchParams::default()
                },
                &Patch::Merge(deletion_cost_patch(&pod.uid, cost)),
            )
            .await?;
        Ok(())
    }
}

/// Merge patch for one pod's annotation. `metadata.uid` makes it conditional on
/// the pod the plan was made for: the apiserver rejects the patch if a different
/// pod now carries the name.
pub(super) fn deletion_cost_patch(uid: &str, cost: DeletionCost) -> serde_json::Value {
    serde_json::json!({
        "metadata": {
            "uid": uid,
            "annotations": {
                POD_DELETION_COST_ANNOTATION: cost.as_str(),
            },
        }
    })
}

/// A pod is a candidate when it is Ready, not terminating, has an IP, and
/// declares a container port named [`DATA_PLANE_INTERNAL_PORT_NAME`]. The
/// ReplicaSet controller already removes not-Ready pods before Ready ones, so
/// the annotation only orders the Ready set; a terminating pod is already on its
/// way out. Taking the port from the pod spec lets each pod say where its own
/// listener is, so a rollout that moves it needs no matching controller change,
/// and a pod without the listener is skipped rather than probed and failed.
pub(super) fn candidate_pod(pod: &Pod) -> Option<DataPlanePod> {
    if pod.metadata.deletion_timestamp.is_some() {
        return None;
    }
    let status = pod.status.as_ref()?;
    if !crate::pod::pod_ready(status.conditions.as_deref()) {
        return None;
    }
    let pod_ip = status.pod_ip.clone().filter(|ip| !ip.is_empty())?;
    let port = pod
        .spec
        .as_ref()?
        .containers
        .iter()
        .flat_map(|container| container.ports.iter().flatten())
        .find(|port| port.name.as_deref() == Some(DATA_PLANE_INTERNAL_PORT_NAME))
        .and_then(|port| u16::try_from(port.container_port).ok())
        .filter(|port| *port != 0)?;
    Some(DataPlanePod {
        name: pod.metadata.name.clone()?,
        uid: pod.metadata.uid.clone()?,
        pod_ip,
        port,
        current: pod
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(POD_DELETION_COST_ANNOTATION))
            .cloned(),
    })
}

/// A pod that still exists and is not terminating, whatever its readiness. Its
/// per-pod history is kept while it is live, so a brief NotReady does not reset
/// its write interval or its run of failed probes.
pub(super) fn live_uid(pod: &Pod) -> Option<String> {
    if pod.metadata.deletion_timestamp.is_some() {
        return None;
    }
    pod.metadata.uid.clone()
}
