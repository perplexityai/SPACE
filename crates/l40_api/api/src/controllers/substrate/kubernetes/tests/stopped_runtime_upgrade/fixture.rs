//! The scripted node, Kubernetes API and stopped sandbox the pod runtime
//! upgrade tests run against.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::{Method, StatusCode};
use pplx_space_common_core::kubernetes::{
    RUNTIME_UPGRADE_CHECKPOINT, SANDBOX_CHECKPOINT_ANNOTATION, SANDBOX_NETWORK_SLOTS_RESOURCE,
    SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
    SANDBOX_RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION,
};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_node_api_client::proto::worker_api_internal_server::WorkerApiInternal;
use pplx_space_sbx_mgr_client::proto::{
    GetRuntimeUpgradeRequest, GetRuntimeUpgradeResponse, RuntimeUpgradeStatus, WakeRequest,
    WakeResponse, WakeTransition,
};

use super::super::runtime_upgrade_dispatch::{E0, E1, block_e0_for_e1, publish_environment};
use super::super::*;
use crate::controllers::substrate::SubstrateAdapter;
use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
    BoxError, KubeReply, KubeRequest, ScriptedReply, ScriptedWorker, ScriptedWorkerConfig,
    WORKER_NODE, kube_status, scripted_kube_client_async, start_scripted_worker_serving,
};
use crate::controllers::substrate::runtime_upgrade::StoppedRuntimeUpgrade;

pub(in crate::controllers::substrate::kubernetes) const UPGRADE_TIMEOUT: Duration =
    Duration::from_secs(3);
/// The budget of an attempt the test expects to end unsettled: it runs to its
/// deadline, so a short one keeps the test fast.
const UNSETTLED_TIMEOUT: Duration = Duration::from_millis(800);

pub(in crate::controllers::substrate::kubernetes) const GET_RUNTIME_UPGRADE: &str =
    "WorkerApiInternal/GetRuntimeUpgrade";
pub(in crate::controllers::substrate::kubernetes) const WAKE: &str =
    "WorkerApiInternal/WakeSandbox";
pub(in crate::controllers::substrate::kubernetes) const ENQUEUE_BACKUP: &str =
    "WorkerApi/EnqueueSandboxArtifactBackup";

/// What the node reports for the upgrade a pod carries.
#[derive(Clone, Copy, Debug)]
pub(in crate::controllers::substrate::kubernetes) enum Report {
    /// Upgraded. With `publish`, the node publishes the sandbox stopped under
    /// E1, as node-ctl does once the upgrade marks it dirty.
    Upgraded {
        publish: bool,
    },
    AlreadyOnTarget,
    /// Rolled back for the pod at the name, which the report names. The
    /// shim's create failed, so the pod stays pending in its container's
    /// create while kubelet retries it, as a real one does: no container
    /// ever ends.
    RolledBack,
    /// Refused before anything was fenced, for the pod at the name, which
    /// the report names; the pod stays pending as for [`Self::RolledBack`].
    Refused,
    /// Upgraded (or, with `already_on_target`, found on the target), but the
    /// node refused the create of the pod at the name, which the report
    /// names as refused: the pod stays pending as for [`Self::RolledBack`].
    PodRefused {
        already_on_target: bool,
    },
    /// As [`Self::PodRefused`], but the refused pod is an earlier attempt's.
    /// The pod at the name is left as it is.
    EarlierPodRefused,
    /// An earlier pod's rollback, which the node reports for the operation
    /// until a retry's shim begins it again. The pod is left as it is.
    EarlierRollback,
    /// A rollback reported by a node that predates the pod field: it names
    /// no pod. The pod is left as it is.
    UnnamedRollback,
    Absent,
    InProgress,
    /// The node-api can't answer.
    Unreachable,
    /// The node-api takes the call and never answers it.
    Hangs,
}

/// The pod an earlier attempt's report names: never one the API created.
pub(in crate::controllers::substrate::kubernetes) const EARLIER_POD_UID: &str = "earlier-pod-uid";

/// A node whose `GetRuntimeUpgrade` answers [`Report`] and whose Wake
/// acknowledges the route it's named.
pub(in crate::controllers::substrate::kubernetes) struct Node {
    report: Mutex<Report>,
    pub(in crate::controllers::substrate::kubernetes) wakes: Mutex<Vec<WakeRequest>>,
    publisher: (sqlx::PgPool, NodeId),
    pods: Arc<Pods>,
}

impl Node {
    pub(in crate::controllers::substrate::kubernetes) fn report(&self, report: Report) {
        *self.report.lock().unwrap() = report;
    }
}

pub(in crate::controllers::substrate::kubernetes) struct ScriptedNode(
    pub(in crate::controllers::substrate::kubernetes) Arc<Node>,
);

#[tonic::async_trait]
impl WorkerApiInternal for ScriptedNode {
    async fn get_runtime_upgrade(
        &self,
        request: tonic::Request<GetRuntimeUpgradeRequest>,
    ) -> Result<tonic::Response<GetRuntimeUpgradeResponse>, tonic::Status> {
        let report = *self.0.report.lock().unwrap();
        let status = match report {
            Report::Upgraded { .. } | Report::AlreadyOnTarget => {
                if matches!(
                    report,
                    Report::Upgraded { publish: true } | Report::AlreadyOnTarget
                ) {
                    let (pool, node_id) = &self.0.publisher;
                    let sandbox_id = request
                        .get_ref()
                        .sandbox_id
                        .parse()
                        .map_err(|_| tonic::Status::invalid_argument("sandbox_id"))?;
                    // Internal-api accepts a publication only from the node
                    // the sandbox is placed on.
                    let placed_here: bool = sqlx::query_scalar(sql!(
                        "SELECT active_node_id IS NOT DISTINCT FROM $2 FROM sandboxes WHERE id = $1"
                    ))
                    .bind(sandbox_id)
                    .bind(*node_id)
                    .fetch_one(pool)
                    .await
                    .map_err(|error| tonic::Status::internal(error.to_string()))?;
                    if placed_here {
                        publish_environment(pool, sandbox_id, *node_id, E1)
                            .await
                            .map_err(|error| tonic::Status::internal(error.to_string()))?;
                    }
                }
                if matches!(report, Report::AlreadyOnTarget) {
                    RuntimeUpgradeStatus::AlreadyOnTarget
                } else {
                    RuntimeUpgradeStatus::Upgraded
                }
            }
            Report::PodRefused { already_on_target } => {
                self.0.pods.stay_creating();
                if already_on_target {
                    RuntimeUpgradeStatus::AlreadyOnTarget
                } else {
                    RuntimeUpgradeStatus::Upgraded
                }
            }
            Report::EarlierPodRefused => RuntimeUpgradeStatus::Upgraded,
            Report::RolledBack => {
                self.0.pods.stay_creating();
                RuntimeUpgradeStatus::RolledBack
            }
            Report::Refused => {
                self.0.pods.stay_creating();
                RuntimeUpgradeStatus::Refused
            }
            Report::EarlierRollback | Report::UnnamedRollback => RuntimeUpgradeStatus::RolledBack,
            Report::Absent => RuntimeUpgradeStatus::Absent,
            Report::InProgress => RuntimeUpgradeStatus::InProgress,
            Report::Unreachable => return Err(tonic::Status::unavailable("node-api is down")),
            Report::Hangs => std::future::pending().await,
        };
        let pod_uid = match report {
            Report::EarlierRollback => Some(EARLIER_POD_UID.to_string()),
            Report::UnnamedRollback | Report::Absent | Report::InProgress => None,
            _ => self.0.pods.current_uid(),
        };
        Ok(tonic::Response::new(GetRuntimeUpgradeResponse {
            status: status as i32,
            phase: 0,
            failure: match report {
                Report::RolledBack | Report::EarlierRollback | Report::UnnamedRollback => {
                    Some("boot failed".to_string())
                }
                Report::Refused => Some("cleanup owns this sandbox".to_string()),
                _ => None,
            },
            refused_pod_uid: match report {
                Report::PodRefused { .. } => self.0.pods.current_uid(),
                Report::EarlierPodRefused => Some(EARLIER_POD_UID.to_string()),
                _ => None,
            },
            refused_pod_failure: matches!(
                report,
                Report::PodRefused { .. } | Report::EarlierPodRefused
            )
            .then(|| "the sandbox runs another pod's execution".to_string()),
            pod_uid,
            // A started target's `UpgradeRuntime` that found the sandbox on
            // the target claims its start.
            activation_claimed: matches!(
                report,
                Report::AlreadyOnTarget
                    | Report::PodRefused {
                        already_on_target: true
                    }
            ) && self
                .0
                .pods
                .created_annotation(SANDBOX_RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION)
                .as_deref()
                == Some("started"),
        }))
    }

    async fn adopt_pool_sandbox(
        &self,
        _request: tonic::Request<pplx_space_sbx_mgr_client::proto::AdoptPoolSandboxRequest>,
    ) -> Result<
        tonic::Response<pplx_space_sbx_mgr_client::proto::AdoptPoolSandboxResponse>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented("not used by this test"))
    }

    async fn wake_sandbox(
        &self,
        request: tonic::Request<WakeRequest>,
    ) -> Result<tonic::Response<WakeResponse>, tonic::Status> {
        let request = request.into_inner();
        self.0.wakes.lock().unwrap().push(request.clone());
        Ok(tonic::Response::new(WakeResponse {
            sandbox_id: request.sandbox_id,
            transition: WakeTransition::Noop as i32,
            route_identity: request
                .start_context
                .and_then(|context| context.route_identity)
                .map(|mut identity| {
                    identity.current_node_id = Some(self.0.publisher.1.to_string());
                    identity
                }),
        }))
    }
}

/// The enqueue reply of a node whose backups succeed.
pub(in crate::controllers::substrate::kubernetes) fn backup_enqueued() -> ScriptedReply {
    ScriptedReply::message(
        &pplx_space_sbx_mgr_client::proto::EnqueueArtifactBackupResponse {
            local_artifact_id: pplx_space_common_core::types::LocalArtifactId::new().to_string(),
            artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk as i32,
            backup_attempt_id: uuid::Uuid::now_v7().to_string(),
        },
    )
}

/// A Kubernetes API holding at most one sandbox pod. A created pod keeps the
/// annotations it was created with and lands on [`WORKER_NODE`], running,
/// until [`Pods::end_container`].
#[derive(Default)]
pub(in crate::controllers::substrate::kubernetes) struct Pods {
    pub(in crate::controllers::substrate::kubernetes) pod: Mutex<Option<serde_json::Value>>,
    /// Every pod create's request body, in order.
    pub(in crate::controllers::substrate::kubernetes) created: Mutex<Vec<serde_json::Value>>,
    /// The UID the API gave each pod it created, in order. A pod created
    /// again at a name gets a new one, as from a real API.
    created_uids: Mutex<Vec<String>>,
    pub(in crate::controllers::substrate::kubernetes) creates: AtomicUsize,
    pub(in crate::controllers::substrate::kubernetes) deletes: AtomicUsize,
    container_ended: AtomicBool,
    /// The pod's container is still being created: its shim's create hasn't
    /// returned.
    container_creating: AtomicBool,
    /// Pod deletes the API refuses with a 500 before it serves them again.
    pub(in crate::controllers::substrate::kubernetes) failing_deletes: AtomicUsize,
    /// Pod deletes the API refuses with a 409 for their UID precondition, as
    /// for a pod at the name with another UID, before it serves them again.
    pub(in crate::controllers::substrate::kubernetes) precondition_failing_deletes: AtomicUsize,
    /// The worker's Node is gone from the API: its GET answers 404.
    pub(in crate::controllers::substrate::kubernetes) node_gone: AtomicBool,
    /// The API takes a pod GET and never answers it.
    pub(in crate::controllers::substrate::kubernetes) pod_reads_hang: AtomicBool,
    /// The API commits a pod create and answers it with a 500.
    pub(in crate::controllers::substrate::kubernetes) creates_fail_after_commit: AtomicBool,
    /// The API commits a pod create and never answers it.
    pub(in crate::controllers::substrate::kubernetes) creates_hang_after_commit: AtomicBool,
    /// The pod has no address: kubelet posts it only once the shim's
    /// pod-sandbox create returns, and a `stopped` upgrade settles inside it.
    pub(in crate::controllers::substrate::kubernetes) addressless: AtomicBool,
}

impl Pods {
    pub(in crate::controllers::substrate::kubernetes) fn exists(&self) -> bool {
        self.pod.lock().unwrap().is_some()
    }

    /// Set `annotations` on the pod, as the create that made it rendered them.
    pub(in crate::controllers::substrate::kubernetes) fn annotate(
        &self,
        annotations: &[(&str, &str)],
    ) {
        if let Some(pod) = self.pod.lock().unwrap().as_mut() {
            for (key, value) in annotations {
                pod["metadata"]["annotations"][*key] = serde_json::json!(value);
            }
        }
    }

    /// The pod requests `slots` network slots, as every sandbox pod's
    /// container does.
    pub(in crate::controllers::substrate::kubernetes) fn request_network_slots(&self, slots: u64) {
        if let Some(pod) = self.pod.lock().unwrap().as_mut() {
            pod["spec"]["containers"] = serde_json::json!([{
                "name": "sandbox",
                "image": "sandbox",
                "resources": {
                    "requests": {
                        SANDBOX_NETWORK_SLOTS_RESOURCE: slots.to_string(),
                    },
                },
            }]);
        }
    }

    /// The pod fails, as its kubelet reports a pod whose sandbox can't run.
    pub(in crate::controllers::substrate::kubernetes) fn fail(&self) {
        if let Some(pod) = self.pod.lock().unwrap().as_mut() {
            pod["status"]["phase"] = serde_json::json!("Failed");
        }
    }

    /// The pod as the API serves it, for a reflector that has seen it.
    fn current(&self) -> Option<Pod> {
        let pod = self.pod.lock().unwrap().clone()?;
        Some(serde_json::from_value(self.rendered(&pod)).expect("the pod fixture parses"))
    }

    /// The pod's container ends, as for a pod that fails or whose container
    /// exits. A shim's failed create never does this: see
    /// [`Self::stay_creating`].
    pub(in crate::controllers::substrate::kubernetes) fn end_container(&self) {
        self.container_ended.store(true, Ordering::SeqCst);
    }

    /// The pod stays pending in its container's create, as one whose shim's
    /// create failed does while kubelet retries it.
    pub(in crate::controllers::substrate::kubernetes) fn stay_creating(&self) {
        self.container_ended.store(false, Ordering::SeqCst);
        self.container_creating.store(true, Ordering::SeqCst);
    }

    /// The UID of the pod at the name, if any.
    pub(in crate::controllers::substrate::kubernetes) fn current_uid(&self) -> Option<String> {
        self.pod
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|pod| pod.pointer("/metadata/uid"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    }

    /// The UID of every pod created, in order.
    pub(in crate::controllers::substrate::kubernetes) fn created_uids(&self) -> Vec<String> {
        self.created_uids.lock().unwrap().clone()
    }

    /// Pods created from now on stay in their container's create, as one
    /// does while its shim runs the upgrade, until [`Self::end_container`].
    pub(in crate::controllers::substrate::kubernetes) fn keep_creating(&self) {
        self.container_ended.store(false, Ordering::SeqCst);
        self.container_creating.store(true, Ordering::SeqCst);
    }

    /// Pods created from now on run their container, as a retry's new pod
    /// does once its shim's create returns.
    pub(in crate::controllers::substrate::kubernetes) fn run_containers(&self) {
        self.container_ended.store(false, Ordering::SeqCst);
        self.container_creating.store(false, Ordering::SeqCst);
    }

    /// The annotation `key` of every pod created, in order.
    pub(in crate::controllers::substrate::kubernetes) fn created_annotations(
        &self,
        key: &str,
    ) -> Vec<Option<String>> {
        self.created
            .lock()
            .unwrap()
            .iter()
            .map(|pod| annotation(pod, key).map(str::to_owned))
            .collect()
    }

    /// The annotation `key` of the last pod created.
    pub(in crate::controllers::substrate::kubernetes) fn created_annotation(
        &self,
        key: &str,
    ) -> Option<String> {
        self.created_annotations(key).pop().flatten()
    }

    /// Every pod created carried the runtime-upgrade checkpoint, `operation`
    /// and `target_state`.
    pub(in crate::controllers::substrate::kubernetes) fn assert_every_create_upgrades(
        &self,
        operation: &str,
        target_state: &str,
    ) {
        let created = self.created.lock().unwrap();
        assert!(!created.is_empty(), "no pod was created");
        for pod in created.iter() {
            let annotation = |key: &str| annotation(pod, key);
            assert_eq!(
                annotation(SANDBOX_CHECKPOINT_ANNOTATION),
                Some(RUNTIME_UPGRADE_CHECKPOINT),
                "{pod}"
            );
            assert_eq!(
                annotation(SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION),
                Some(operation),
                "{pod}"
            );
            assert_eq!(
                annotation(SANDBOX_RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION),
                Some(target_state),
                "{pod}"
            );
        }
    }

    fn rendered(&self, pod: &serde_json::Value) -> serde_json::Value {
        let mut pod = pod.clone();
        if self.addressless.load(Ordering::SeqCst)
            && let Some(status) = pod["status"].as_object_mut()
        {
            status.remove("podIP");
            status.remove("podIPs");
        }
        let state = if self.container_ended.load(Ordering::SeqCst) {
            serde_json::json!({ "terminated": { "exitCode": 128 } })
        } else if self.container_creating.load(Ordering::SeqCst) {
            pod["status"]["phase"] = serde_json::json!("Pending");
            serde_json::json!({ "waiting": { "reason": "ContainerCreating" } })
        } else {
            serde_json::json!({ "running": {} })
        };
        pod["status"]["containerStatuses"] = serde_json::json!([{
            "name": "sandbox",
            "image": "sandbox",
            "imageID": "",
            "ready": false,
            "restartCount": 0,
            "state": state,
        }]);
        pod
    }
}

/// The annotation `key` of a pod's JSON.
fn annotation<'a>(pod: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    pod.pointer("/metadata/annotations")
        .and_then(|annotations| annotations.get(key))
        .and_then(serde_json::Value::as_str)
}

pub(in crate::controllers::substrate::kubernetes) fn pods_client(pods: Arc<Pods>) -> kube::Client {
    let (client, _log) = scripted_kube_client_async(move |request: KubeRequest| {
        let pods = Arc::clone(&pods);
        async move {
            let pods_path = "/api/v1/namespaces/space/pods";
            match request.method {
                Method::GET
                    if request.path == format!("/api/v1/nodes/{WORKER_NODE}")
                        && pods.node_gone.load(Ordering::SeqCst) =>
                {
                    kube_status(StatusCode::NOT_FOUND, "NotFound")
                }
                // The worker the sandbox's disk is on, ready and schedulable.
                Method::GET if request.path == format!("/api/v1/nodes/{WORKER_NODE}") => {
                    KubeReply::Json(
                        StatusCode::OK,
                        serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Node",
                            "metadata": { "name": WORKER_NODE, "uid": "node-a-uid" },
                            "status": {
                                "conditions": [{ "type": "Ready", "status": "True" }],
                                "addresses": [{ "type": "InternalIP", "address": "127.0.0.1" }]
                            }
                        }),
                    )
                }
                Method::GET if request.path.starts_with(pods_path) => {
                    if pods.pod_reads_hang.load(Ordering::SeqCst) {
                        std::future::pending::<()>().await;
                    }
                    let pod = pods.pod.lock().unwrap().clone();
                    let wanted = request.path.rsplit('/').next().unwrap_or_default();
                    match pod.filter(|pod| {
                        pod.pointer("/metadata/name")
                            .and_then(serde_json::Value::as_str)
                            == Some(wanted)
                    }) {
                        Some(pod) => KubeReply::Json(StatusCode::OK, pods.rendered(&pod)),
                        None => kube_status(StatusCode::NOT_FOUND, "NotFound"),
                    }
                }
                Method::POST if request.path == pods_path => {
                    let requested = request.body.unwrap_or_default();
                    let name = requested
                        .pointer("/metadata/name")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let exists = pods
                        .pod
                        .lock()
                        .unwrap()
                        .as_ref()
                        .and_then(|pod| pod.pointer("/metadata/name"))
                        .and_then(serde_json::Value::as_str)
                        == Some(name.as_str());
                    if exists {
                        return kube_status(StatusCode::CONFLICT, "AlreadyExists");
                    }
                    let uid = {
                        let mut uids = pods.created_uids.lock().unwrap();
                        let earlier = uids
                            .iter()
                            .filter(|uid| uid.starts_with(&format!("{name}-uid")))
                            .count();
                        let uid = if earlier == 0 {
                            format!("{name}-uid")
                        } else {
                            format!("{name}-uid-{earlier}")
                        };
                        uids.push(uid.clone());
                        uid
                    };
                    let created = serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "name": name,
                            "namespace": "space",
                            "uid": uid,
                            "annotations": requested.pointer("/metadata/annotations"),
                        },
                        "spec": {
                            "nodeName": WORKER_NODE,
                            "containers": [{ "name": "sandbox", "image": "sandbox" }],
                        },
                        "status": {
                            "phase": "Running",
                            "podIP": "127.0.0.1",
                            "podIPs": [{ "ip": "127.0.0.1" }]
                        }
                    });
                    pods.created.lock().unwrap().push(requested);
                    pods.creates.fetch_add(1, Ordering::SeqCst);
                    *pods.pod.lock().unwrap() = Some(created.clone());
                    if pods.creates_hang_after_commit.load(Ordering::SeqCst) {
                        std::future::pending::<()>().await;
                    }
                    if pods.creates_fail_after_commit.load(Ordering::SeqCst) {
                        return kube_status(StatusCode::INTERNAL_SERVER_ERROR, "InternalError");
                    }
                    KubeReply::Json(StatusCode::CREATED, pods.rendered(&created))
                }
                Method::DELETE
                    if request.path.starts_with(pods_path)
                        && pods
                            .failing_deletes
                            .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                                left.checked_sub(1)
                            })
                            .is_ok() =>
                {
                    kube_status(StatusCode::INTERNAL_SERVER_ERROR, "InternalError")
                }
                Method::DELETE
                    if request.path.starts_with(pods_path)
                        && pods
                            .precondition_failing_deletes
                            .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                                left.checked_sub(1)
                            })
                            .is_ok() =>
                {
                    kube_status(StatusCode::CONFLICT, "Conflict")
                }
                Method::DELETE if request.path.starts_with(pods_path) => {
                    // A delete under a UID precondition leaves a pod with
                    // another UID alone, as the API does.
                    let wanted_uid = request
                        .body
                        .as_ref()
                        .and_then(|body| body.pointer("/preconditions/uid"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned);
                    let mut pod = pods.pod.lock().unwrap();
                    let current_uid = pod
                        .as_ref()
                        .and_then(|pod| pod.pointer("/metadata/uid"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned);
                    if pod.is_some() && wanted_uid.is_some() && wanted_uid != current_uid {
                        return kube_status(StatusCode::CONFLICT, "Conflict");
                    }
                    pods.deletes.fetch_add(1, Ordering::SeqCst);
                    match pod.take() {
                        Some(_) => kube_status(StatusCode::OK, ""),
                        None => kube_status(StatusCode::NOT_FOUND, "NotFound"),
                    }
                }
                method => kube_status(
                    StatusCode::BAD_REQUEST,
                    &format!("unexpected Kubernetes request {method} {}", request.path),
                ),
            }
        }
    });
    client
}

/// A ready worker node-api pod on [`WORKER_NODE`] that advertises `port`, so
/// the substrate reaches the node by name.
pub(in crate::controllers::substrate::kubernetes) fn node_api_pod(port: u16) -> Pod {
    let mut pod = node_api_pod_on_node("space-worker-node-a", WORKER_NODE);
    pod.metadata.annotations = Some(std::collections::BTreeMap::from([(
        "space.pplx.net/node-api-port".to_string(),
        port.to_string(),
    )]));
    pod.status.as_mut().expect("node-api pod has status").pod_ip = Some("127.0.0.1".to_string());
    pod
}

/// [`WORKER_NODE`] as the Node reflector holds it: Ready, schedulable,
/// inside the default sandbox node selector, with free network slots, a UID
/// and an internal address.
pub(in crate::controllers::substrate::kubernetes) fn worker_node()
-> k8s_openapi::api::core::v1::Node {
    let labels = SandboxPodConfigValues::default()
        .base_node_selector()
        .into_labels();
    let mut node =
        (*node_with_network_slots(WORKER_NODE, labels, Some(8), None, true, false)).clone();
    node.metadata.uid = Some("node-a-uid".to_string());
    if let Some(status) = node.status.as_mut() {
        status.addresses = Some(vec![k8s_openapi::api::core::v1::NodeAddress {
            type_: "InternalIP".to_string(),
            address: "127.0.0.1".to_string(),
        }]);
    }
    node
}

/// A scripted node and Kubernetes API for `pool`, whose node answers backup
/// enqueues with `enqueue`.
pub(in crate::controllers::substrate::kubernetes) struct Cluster {
    pub(in crate::controllers::substrate::kubernetes) node: Arc<Node>,
    pub(in crate::controllers::substrate::kubernetes) worker: ScriptedWorker,
    pub(in crate::controllers::substrate::kubernetes) pods: Arc<Pods>,
}

impl Cluster {
    pub(in crate::controllers::substrate::kubernetes) async fn start(
        pool: &sqlx::PgPool,
        node_id: NodeId,
        enqueue: ScriptedReply,
    ) -> Result<Self, BoxError> {
        let pods = Arc::new(Pods::default());
        let node = Arc::new(Node {
            report: Mutex::new(Report::Upgraded { publish: true }),
            wakes: Mutex::default(),
            publisher: (pool.clone(), node_id),
            pods: Arc::clone(&pods),
        });
        let worker = start_scripted_worker_serving(
            ScriptedWorkerConfig::new([(ENQUEUE_BACKUP, enqueue)]),
            ScriptedNode(Arc::clone(&node)),
        )
        .await?;
        Ok(Self { node, worker, pods })
    }

    /// A substrate over `pool` that reaches this node by name and through
    /// its pods, with an empty policy cache, whose Node reflector holds
    /// [`worker_node`].
    pub(in crate::controllers::substrate::kubernetes) fn substrate(
        &self,
        pool: &sqlx::PgPool,
    ) -> KubernetesSubstrate {
        self.substrate_caching(pool, Vec::new(), vec![worker_node()])
    }

    /// [`Self::substrate`] whose synced Node reflector holds `nodes` instead.
    pub(in crate::controllers::substrate::kubernetes) fn substrate_with_nodes(
        &self,
        pool: &sqlx::PgPool,
        nodes: Vec<k8s_openapi::api::core::v1::Node>,
    ) -> KubernetesSubstrate {
        self.substrate_caching(pool, Vec::new(), nodes)
    }

    /// [`Self::substrate`] whose pod reflector has also seen `sandbox_pods`
    /// and whose Node reflector holds `nodes`.
    fn substrate_caching(
        &self,
        pool: &sqlx::PgPool,
        sandbox_pods: Vec<Pod>,
        nodes: Vec<k8s_openapi::api::core::v1::Node>,
    ) -> KubernetesSubstrate {
        let mut pods = vec![node_api_pod(self.worker.port)];
        pods.extend(sandbox_pods);
        let mut substrate = substrate_with_store_client_proxy_port_pods_and_nodes(
            pool.clone(),
            pods_client(Arc::clone(&self.pods)),
            self.worker.port,
            pods,
            nodes,
        );
        // A regression that ignores the node-api pod's advertised port fails
        // to connect rather than reaching whatever listens on the default.
        substrate.k8s.node_api_port = 1;
        substrate
    }
}

/// A stopped sandbox whose disk is on `node-a` and whose last execution E0
/// ran, with E0 blocked in favor of the default E1 along a certified edge.
pub(super) struct Subject {
    pub(super) db: pplx_space_db_test_fixtures::TestDb,
    pub(super) sandbox_id: SandboxId,
    pub(super) node_id: NodeId,
    pub(super) substrate: KubernetesSubstrate,
    pub(super) cluster: Cluster,
}

pub(super) async fn subject() -> Result<Subject, BoxError> {
    subject_whose_backups(backup_enqueued()).await
}

/// A sandbox in `lifecycle` on [`WORKER_NODE`], whose last execution there
/// ran under E0, with E0 blocked in favor of the default E1 along a certified
/// edge. Answers the sandbox and its node.
pub(in crate::controllers::substrate::kubernetes) async fn seed_sandbox_on_e0(
    pool: &sqlx::PgPool,
    lifecycle: &str,
) -> Result<(SandboxId, NodeId), BoxError> {
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(pool, sandbox_id, lifecycle).await?;
    let node_id = crate::test_support::kubernetes::insert_kubernetes_worker_node(pool).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    // The execution the sandbox stopped from, on the node holding its disk.
    let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
    insert_k8s_execution(pool, sandbox_id, 1, &pod_name, "uid-stopped", None, false).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET k8s_node_name = $2 WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .bind(WORKER_NODE)
    .execute(pool)
    .await?;
    publish_environment(pool, sandbox_id, node_id, E0).await?;
    block_e0_for_e1(pool, &[E1]).await?;
    Ok((sandbox_id, node_id))
}

/// [`subject`] whose node answers backup enqueues with `enqueue`.
pub(super) async fn subject_whose_backups(enqueue: ScriptedReply) -> Result<Subject, BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();
    let (sandbox_id, node_id) = seed_sandbox_on_e0(&pool, "stopped").await?;
    let cluster = Cluster::start(&pool, node_id, enqueue).await?;
    let substrate = cluster.substrate(&pool);
    Ok(Subject {
        db,
        sandbox_id,
        node_id,
        substrate,
        cluster,
    })
}

impl Subject {
    pub(super) fn node(&self) -> &Node {
        &self.cluster.node
    }

    pub(super) fn pods(&self) -> &Pods {
        &self.cluster.pods
    }

    /// A substrate over the same database, node and pods, whose policy cache
    /// is empty, as another replica's would be.
    pub(super) fn forget_cached_policy(&mut self) {
        self.substrate = self.cluster.substrate(&self.db.pool);
    }

    /// A substrate like [`Self::forget_cached_policy`]'s whose pod reflector
    /// has seen the sandbox's current pod, as a running replica's has.
    pub(super) fn cache_the_pod(&mut self) {
        let pod = self.pods().current().into_iter().collect();
        self.substrate = self
            .cluster
            .substrate_caching(&self.db.pool, pod, vec![worker_node()]);
    }

    /// [`Self::cache_the_pod`] whose [`worker_node`] advertises only
    /// `capacity` network slots.
    pub(super) fn cache_the_pod_with_network_slots(&mut self, capacity: u64) {
        let pod = self.pods().current().into_iter().collect();
        let mut node = worker_node();
        if let Some(status) = node.status.as_mut() {
            let slots = std::collections::BTreeMap::from([(
                SANDBOX_NETWORK_SLOTS_RESOURCE.to_string(),
                k8s_openapi::apimachinery::pkg::api::resource::Quantity(capacity.to_string()),
            )]);
            status.capacity = Some(slots.clone());
            status.allocatable = Some(slots);
        }
        self.substrate = self
            .cluster
            .substrate_caching(&self.db.pool, pod, vec![node]);
    }

    pub(super) async fn upgrade(&self) -> Result<StoppedRuntimeUpgrade, SubstrateError> {
        self.substrate
            .upgrade_stopped_runtime(self.sandbox_id, UPGRADE_TIMEOUT)
            .await
    }

    /// An attempt the test expects to end unsettled, under a short deadline.
    pub(super) async fn upgrade_unsettled(&self) -> SubstrateError {
        self.substrate
            .upgrade_stopped_runtime(self.sandbox_id, UNSETTLED_TIMEOUT)
            .await
            .expect_err("the upgrade settled")
    }

    pub(super) async fn lifecycle(&self) -> Result<String, BoxError> {
        Ok(
            sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                .bind(self.sandbox_id)
                .fetch_one(&self.db.pool)
                .await?,
        )
    }

    /// The active execution's generation, attempt target and whether its
    /// lease still runs.
    pub(super) async fn active_execution(
        &self,
    ) -> Result<Option<(i64, Option<String>, bool)>, BoxError> {
        Ok(sqlx::query_as(sql!(
            "SELECT generation, activation_attempt_target_state,
                    COALESCE(activation_attempt_expires_at > NOW(), FALSE)
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(self.sandbox_id)
        .fetch_optional(&self.db.pool)
        .await?)
    }

    /// The upgrade the active execution records: operation, target and
    /// authorization.
    pub(super) async fn recorded_upgrade(
        &self,
    ) -> Result<(Option<uuid::Uuid>, Option<String>, Option<String>), BoxError> {
        Ok(sqlx::query_as(sql!(
            "SELECT runtime_upgrade_operation_id, runtime_upgrade_target_environment_ref,
                    runtime_upgrade_authorization_id
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(self.sandbox_id)
        .fetch_one(&self.db.pool)
        .await?)
    }

    /// The backup enqueues the node received, in order.
    pub(super) fn enqueued_backups(
        &self,
    ) -> Vec<pplx_space_sbx_mgr_client::proto::EnqueueArtifactBackupRequest> {
        self.cluster
            .worker
            .received(ENQUEUE_BACKUP)
            .into_iter()
            .map(|message| prost::Message::decode(message).unwrap())
            .collect()
    }

    /// No pod was created and nothing was asked of the node.
    pub(super) fn assert_nothing_placed_or_sent(&self, context: &str) {
        assert_eq!(self.pods().creates.load(Ordering::SeqCst), 0, "{context}");
        for method in [GET_RUNTIME_UPGRADE, WAKE, ENQUEUE_BACKUP] {
            assert!(
                self.cluster.worker.first_call(method).is_none(),
                "{context}: the upgrade called {method}"
            );
        }
    }

    /// The row is stopped, and no execution or pod is left.
    pub(super) async fn assert_stopped_without_execution(
        &self,
        context: &str,
    ) -> Result<(), BoxError> {
        assert_eq!(self.lifecycle().await?, "stopped", "{context}");
        assert_eq!(self.active_execution().await?, None, "{context}");
        assert!(
            !self.pods().exists(),
            "{context}: a pod outlived the upgrade"
        );
        Ok(())
    }

    /// The upgrade kept its pod and its execution, whose claim's lease is
    /// over, for a retry.
    pub(super) async fn assert_kept_for_a_retry(&self, context: &str) -> Result<(), BoxError> {
        assert_eq!(self.lifecycle().await?, "stopped", "{context}");
        assert!(self.pods().exists(), "{context}: the pod is gone");
        let (_, target, live) = self
            .active_execution()
            .await?
            .unwrap_or_else(|| panic!("{context}: no execution"));
        assert_eq!(
            (target.as_deref(), live),
            (Some("stopped"), false),
            "{context}"
        );
        assert!(self.recorded_upgrade().await?.0.is_some(), "{context}");
        Ok(())
    }

    /// An attempt that ended unsettled kept its pod and its execution, whose
    /// claim's lease still runs, so nothing takes the execution mid-upgrade.
    pub(super) async fn assert_kept_under_its_lease(&self, context: &str) -> Result<(), BoxError> {
        assert_eq!(self.lifecycle().await?, "stopped", "{context}");
        assert!(self.pods().exists(), "{context}: the pod is gone");
        let (_, target, live) = self
            .active_execution()
            .await?
            .unwrap_or_else(|| panic!("{context}: no execution"));
        assert_eq!(
            (target.as_deref(), live),
            (Some("stopped"), true),
            "{context}"
        );
        assert!(self.recorded_upgrade().await?.0.is_some(), "{context}");
        Ok(())
    }

    /// The active execution's claim's lease runs out, as it does once its
    /// time is up.
    pub(super) async fn end_the_lease(&self) -> Result<(), BoxError> {
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET activation_attempt_expires_at = NOW() - INTERVAL '1 second'
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(self.sandbox_id)
        .execute(&self.db.pool)
        .await?;
        Ok(())
    }

    /// The node published the sandbox stopped under E1.
    pub(super) async fn publish_upgraded(&self) -> Result<(), BoxError> {
        publish_environment(&self.db.pool, self.sandbox_id, self.node_id, E1).await
    }
}

pub(super) fn upgraded(outcome: &StoppedRuntimeUpgrade) -> uuid::Uuid {
    match outcome {
        StoppedRuntimeUpgrade::Upgraded {
            target,
            operation_id,
        } => {
            assert_eq!(target.to_string(), E1);
            *operation_id
        }
        StoppedRuntimeUpgrade::AsRecorded => panic!("expected an upgrade, got {outcome:?}"),
    }
}
