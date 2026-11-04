//! Kubernetes substrate fixtures for tests of shared lifecycle logic: a
//! scripted apiserver, a live sandbox's execution row and pod, and a
//! node-api simulator registered as a Kubernetes worker node.
//!
//! A live sandbox is an active `sandbox_k8s_executions` row plus the Running
//! pod that both the substrate's reflector and the scripted apiserver hold
//! for it, at the loopback address the simulator listens on; the worker
//! node's node-api pod points at the same simulator.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::http::{Method, StatusCode};
use k8s_openapi::api::core::v1::Pod;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, NodeManagedBy, SandboxId};
use pplx_space_node_api_sim::{SandboxRecord, SimState, SimWorkerHarness};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use crate::controllers::substrate::Substrate;
use crate::controllers::substrate::kubernetes::KubernetesSubstrate;
use crate::controllers::substrate::kubernetes::adapter_characterization_tests::sandbox_pod;
use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
    KubeLog, KubeReply, KubeRequest, kube_status, scripted_kube_client,
};
use crate::controllers::substrate::kubernetes::tests::{
    node_api_pod_on_node, substrate_with_store_client_proxy_port_and_pods,
};

/// The Kubernetes node the simulated worker and every sandbox pod run on.
pub(crate) const WORKER_NODE: &str = "node-a";
/// The pod IP the node-api simulator answers on.
pub(crate) const POD_IP: &str = "127.0.0.1";
const PODS_PATH: &str = "/api/v1/namespaces/space/pods";

/// The one place tests built on these fixtures construct a `Substrate`.
pub(crate) fn kubernetes(substrate: KubernetesSubstrate) -> Substrate {
    Substrate::from_kubernetes(substrate)
}

/// How many pod creates reached the apiserver.
pub(crate) fn pod_creates(log: &KubeLog) -> usize {
    log.lock()
        .expect("kube log mutex poisoned")
        .iter()
        .filter(|request| request.method == Method::POST && request.path == PODS_PATH)
        .count()
}

/// An apiserver that has no sandbox pod and refuses every pod create with a
/// 503, so no runtime can be found or made.
pub(crate) fn unavailable_apiserver() -> (kube::Client, KubeLog) {
    scripted_kube_client(|request: &KubeRequest| {
        if request.method == Method::POST {
            kube_status(StatusCode::SERVICE_UNAVAILABLE, "ServiceUnavailable")
        } else {
            kube_status(StatusCode::NOT_FOUND, "NotFound")
        }
    })
}

/// An apiserver holding an in-memory set of sandbox pods, seeded with `pods`.
/// A create stores the requested pod as Running at [`POD_IP`]; reads and
/// deletes act on the stored pods. It also serves the worker's Node, whose
/// identity pod placement records.
///
/// It takes shortcuts a real apiserver does not. A created pod is Running at
/// once, with no Pending phase for the substrate to wait out, and with no
/// scheduler, a pod created without `spec.nodeName` is never bound to a node.
/// A delete removes the pod at once instead of leaving it Terminating. Like a
/// real apiserver, it refuses with a 409 a delete whose `preconditions.uid`
/// names another pod; it does not check `preconditions.resourceVersion`.
pub(crate) fn pod_store_apiserver(pods: &[Pod]) -> TestResult<(kube::Client, KubeLog)> {
    let mut stored = HashMap::new();
    for pod in pods {
        let name = pod.metadata.name.clone().ok_or("a seeded pod has a name")?;
        stored.insert(name, serde_json::to_value(pod)?);
    }
    let stored = Arc::new(Mutex::new(stored));
    let node_path = format!("/api/v1/nodes/{WORKER_NODE}");
    Ok(scripted_kube_client(move |request: &KubeRequest| {
        let mut stored = stored.lock().expect("pod store mutex poisoned");
        let name = request
            .path
            .strip_prefix(PODS_PATH)
            .and_then(|rest| rest.strip_prefix('/'));
        match (&request.method, name) {
            (&Method::POST, None) if request.path == PODS_PATH => {
                let mut created = request.body.clone().unwrap_or_default();
                let name = created["metadata"]["name"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                created["metadata"]["uid"] = serde_json::json!(format!("{name}-uid"));
                created["status"] = serde_json::json!({
                    "phase": "Running",
                    "podIP": POD_IP,
                    "podIPs": [{ "ip": POD_IP }],
                });
                stored.insert(name, created.clone());
                KubeReply::Json(StatusCode::CREATED, created)
            }
            (&Method::GET, Some(name)) => match stored.get(name) {
                Some(pod) => KubeReply::Json(StatusCode::OK, pod.clone()),
                None => kube_status(StatusCode::NOT_FOUND, "NotFound"),
            },
            (&Method::DELETE, Some(name)) => {
                let Some(pod) = stored.get(name) else {
                    return kube_status(StatusCode::NOT_FOUND, "NotFound");
                };
                let precondition_uid = request
                    .body
                    .as_ref()
                    .and_then(|body| body["preconditions"]["uid"].as_str());
                if precondition_uid.is_some_and(|uid| Some(uid) != pod["metadata"]["uid"].as_str())
                {
                    return kube_status(StatusCode::CONFLICT, "Conflict");
                }
                stored.remove(name);
                kube_status(StatusCode::OK, "Success")
            }
            (&Method::GET, None) if request.path == node_path => KubeReply::Json(
                StatusCode::OK,
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": { "name": WORKER_NODE, "uid": format!("{WORKER_NODE}-uid") },
                    "status": { "addresses": [{ "type": "InternalIP", "address": POD_IP }] },
                }),
            ),
            _ => kube_status(StatusCode::BAD_REQUEST, "BadRequest"),
        }
    }))
}

/// The name the substrate gives generation 1's pod.
fn pod_name(sandbox_id: SandboxId) -> String {
    format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple())
}

fn pod_uid(sandbox_id: SandboxId) -> String {
    format!("{sandbox_id}-uid")
}

/// Whether a seeded execution has published its route identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouteIdentity {
    /// `route_activation_id` names the attempt the resident reports, as for
    /// an execution the current api activated; its next activation keeps that
    /// route (`ActivationRoutePublication::PreserveExisting`).
    Published,
    /// No `route_activation_id`, as for an execution recorded before route
    /// identities existed: its next activation adopts the resident and
    /// publishes the route only after the node acknowledged that
    /// activation's Wake (`publish_legacy_route_identity_after_ack`). Tests
    /// that hook that publication need this shape.
    Legacy,
}

/// A seeded live execution: the pod the substrate should hold for it, and
/// the route attempt the resident should report.
pub(crate) struct LiveExecution {
    pub(crate) pod: Pod,
    pub(crate) route_activation_id: Option<Uuid>,
}

/// Seeds the active execution of a live sandbox at generation 1.
pub(crate) async fn insert_live_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    route: RouteIdentity,
) -> TestResult<LiveExecution> {
    let route_activation_id = (route == RouteIdentity::Published).then(Uuid::now_v7);
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
              k8s_node_name, state, active, route_activation_id)
         VALUES ($1, 1, $2, 'space', $3, $4::INET, $5, 'running', TRUE, $6)"
    ))
    .bind(sandbox_id)
    .bind(pod_name(sandbox_id))
    .bind(pod_uid(sandbox_id))
    .bind(POD_IP)
    .bind(WORKER_NODE)
    .bind(route_activation_id)
    .execute(pool)
    .await?;
    let pod = sandbox_pod(
        &pod_name(sandbox_id),
        &pod_uid(sandbox_id),
        sandbox_id,
        1,
        "Running",
        Some(POD_IP),
    );
    Ok(LiveExecution {
        pod,
        route_activation_id,
    })
}

/// The worker node's ready node-api pod, on the simulator's address.
fn node_api_pod() -> Pod {
    let mut pod = node_api_pod_on_node("space-worker-node-a", WORKER_NODE);
    if let Some(status) = pod.status.as_mut() {
        status.pod_ip = Some(POD_IP.to_owned());
    }
    pod
}

/// A substrate that holds `pods` and the worker's node-api pod, in its
/// reflectors and in a [`pod_store_apiserver`], and reaches the simulator
/// listening on `worker_port` through them.
///
/// The reflectors are seeded once: pods the apiserver double later creates or
/// deletes never reach them, so the substrate sees those changes only through
/// its uncached apiserver reads. Every pod is at [`POD_IP`], so the simulator
/// cannot tell which pod a call was routed through. The apiserver double
/// creates pods already Running and deletes them without a Terminating phase
/// (see [`pod_store_apiserver`]).
pub(crate) fn substrate_over_pods(
    pool: &PgPool,
    worker_port: u16,
    pods: Vec<Pod>,
) -> TestResult<(KubernetesSubstrate, KubeLog)> {
    let pods: Vec<Pod> = pods.into_iter().chain([node_api_pod()]).collect();
    let (client, log) = pod_store_apiserver(&pods)?;
    let substrate =
        substrate_with_store_client_proxy_port_and_pods(pool.clone(), client, worker_port, pods);
    Ok((substrate, log))
}

/// Registers [`WORKER_NODE`] as a ready Kubernetes worker node and returns
/// its id.
pub(crate) async fn insert_kubernetes_worker_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = pplx_space_common_svc::types::k8s::space_k8s_node_id(WORKER_NODE, "worker");
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, $2::INET, 'worker', 'k8s', 'ready')"
    ))
    .bind(node_id)
    .bind(POD_IP)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(WORKER_NODE)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A Kubernetes worker node backed by the in-process simulator. Callers must
/// have entered `enter_sim_worker_process` first.
pub(crate) async fn start_kubernetes_sim_worker(
    pool: &PgPool,
) -> TestResult<(SimWorkerHarness, NodeId)> {
    let harness = SimWorkerHarness::builder(WORKER_NODE)
        .with_internal_api()
        .start()
        .await?;
    let node_id = insert_kubernetes_worker_node(pool).await?;
    Ok((harness, node_id))
}

/// A resident the simulator reports with a Kubernetes route identity at the
/// seeded execution's generation and, when the execution published one, its
/// route attempt.
pub(crate) async fn insert_kubernetes_resident(
    state: &SimState,
    sandbox_id: SandboxId,
    status: &str,
    route_activation_id: Option<Uuid>,
) -> TestResult {
    let mut record = SandboxRecord::new_creating(
        sandbox_id.to_string(),
        "seed".to_owned(),
        "fc".to_owned(),
        "root".to_owned(),
        Some("hash-seed".to_owned()),
        false,
    )
    .with_route_identity_provenance(1, 1, Some(NodeManagedBy::Kubernetes));
    record.status = status.to_owned();
    record.activation_attempt_id = route_activation_id.map(|id| id.to_string());
    state.insert_new(record).await?;
    Ok(())
}
