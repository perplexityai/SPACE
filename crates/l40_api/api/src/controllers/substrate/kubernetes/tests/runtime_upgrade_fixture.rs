//! The scripted node and Kubernetes API the pod runtime upgrade tests run
//! against.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::http::{Method, StatusCode};
use pplx_space_accounts::NodeId;
use pplx_space_node_api_client::proto::worker_api_internal_server::WorkerApiInternal;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_pod::{
    RUNTIME_UPGRADE_CHECKPOINT, SANDBOX_CHECKPOINT_ANNOTATION, SANDBOX_NETWORK_SLOTS_RESOURCE,
    SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
    SANDBOX_RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION,
};
use pplx_space_sbx_mgr_client::proto::{
    AbandonRuntimeUpgradeRequest, AbandonRuntimeUpgradeResponse, GetRuntimeUpgradeRequest,
    GetRuntimeUpgradeResponse, RuntimeUpgradeStatus, WakeRequest, WakeResponse, WakeTransition,
};
use pplx_space_sql_locator::sql;

use super::runtime_upgrade_dispatch::{E0, E1, block_e0_for_e1, publish_environment};
use super::*;
use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
    BoxError, KubeReply, KubeRequest, ScriptedReply, ScriptedWorker, ScriptedWorkerConfig,
    WORKER_NODE, kube_status, scripted_kube_client_async, start_scripted_worker_serving,
};

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
    /// Absent, the sandbox's row gone with the report, carrying the refused
    /// create the node kept for the pod at the name: the pod stays pending
    /// as for [`Self::RolledBack`].
    KeptPodRefused,
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
    /// The pod a [`Report::RolledBack`] or [`Report::Refused`] is the outcome
    /// of: the pod at the name when it was reported, or with none there, the
    /// first pod that reads it. A pod that later replaces it at the name
    /// reads it as another pod's, as a real node reports it, until a test
    /// reports that pod's own outcome.
    report_pod_uid: Mutex<Option<String>>,
    pub(in crate::controllers::substrate::kubernetes) wakes: Mutex<Vec<WakeRequest>>,
    /// The `attempt_id` every `GetRuntimeUpgrade` asked for, in order: the
    /// pod UID, as the shim names its attempt. The
    /// report doesn't filter on it: it answers as a node that predates the
    /// field does, with the operation's latest outcome.
    pub(in crate::controllers::substrate::kubernetes) asked_pod_uids: Mutex<Vec<Option<String>>>,
    publisher: (sqlx::PgPool, NodeId),
    pods: Arc<Pods>,
    /// Every operation id an `AbandonRuntimeUpgrade` named, in order, and
    /// whether the pod carrying the upgrade was gone when it arrived.
    abandons: Mutex<Vec<(String, bool)>>,
    /// What `AbandonRuntimeUpgrade` answers; `None` succeeds.
    abandon_refusal: Mutex<Option<tonic::Code>>,
    /// Operation ids the node records for another sandbox, which
    /// `AbandonRuntimeUpgrade` refuses as sandbox-manager does
    /// (`OperationNamesAnother`, `INVALID_ARGUMENT`).
    operations_of_another_sandbox: Mutex<Vec<String>>,
}

impl Node {
    pub(in crate::controllers::substrate::kubernetes) fn report(&self, report: Report) {
        *self.report.lock().unwrap() = report;
        *self.report_pod_uid.lock().unwrap() = self.pods.current_uid();
    }

    /// Every operation id an `AbandonRuntimeUpgrade` named, with whether the
    /// upgrade's pod was gone by then.
    pub(in crate::controllers::substrate::kubernetes) fn abandons(&self) -> Vec<(String, bool)> {
        self.abandons.lock().unwrap().clone()
    }

    /// Answer every later `AbandonRuntimeUpgrade` with `code`.
    pub(in crate::controllers::substrate::kubernetes) fn refuse_abandons(&self, code: tonic::Code) {
        *self.abandon_refusal.lock().unwrap() = Some(code);
    }

    /// Record `operation_id` for another sandbox, as a node would that ran
    /// it for one, so an abandon naming this sandbox is refused.
    pub(in crate::controllers::substrate::kubernetes) fn record_for_another_sandbox(
        &self,
        operation_id: &str,
    ) {
        self.operations_of_another_sandbox
            .lock()
            .unwrap()
            .push(operation_id.to_owned());
    }

    /// The pod the reported rollback or refusal is the outcome of, pinned on
    /// the first read when no pod was there to report it for.
    fn reported_pod_uid(&self) -> Option<String> {
        let mut pinned = self.report_pod_uid.lock().unwrap();
        if pinned.is_none() {
            *pinned = self
                .pods
                .current_uid()
                .or_else(|| self.pods.created_uids().last().cloned());
        }
        pinned.clone()
    }
}

pub(in crate::controllers::substrate::kubernetes) struct ScriptedNode(
    pub(in crate::controllers::substrate::kubernetes) Arc<Node>,
);

#[tonic::async_trait]
impl WorkerApiInternal for ScriptedNode {
    async fn drain_node(
        &self,
        _request: tonic::Request<pplx_space_sbx_mgr_client::proto::DrainNodeRequest>,
    ) -> Result<tonic::Response<pplx_space_sbx_mgr_client::proto::DrainNodeResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("not used by this test"))
    }

    async fn preview_drain_node(
        &self,
        _request: tonic::Request<pplx_space_sbx_mgr_client::proto::PreviewDrainNodeRequest>,
    ) -> Result<
        tonic::Response<pplx_space_sbx_mgr_client::proto::PreviewDrainNodeResponse>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented("not used by this test"))
    }

    async fn abandon_runtime_upgrade(
        &self,
        request: tonic::Request<AbandonRuntimeUpgradeRequest>,
    ) -> Result<tonic::Response<AbandonRuntimeUpgradeResponse>, tonic::Status> {
        let pod_gone = !self.0.pods.exists();
        self.0
            .abandons
            .lock()
            .unwrap()
            .push((request.get_ref().operation_id.clone(), pod_gone));
        let operation_id = &request.get_ref().operation_id;
        if self
            .0
            .operations_of_another_sandbox
            .lock()
            .unwrap()
            .contains(operation_id)
        {
            return Err(tonic::Status::invalid_argument(format!(
                "runtime upgrade {operation_id} names another sandbox"
            )));
        }
        match *self.0.abandon_refusal.lock().unwrap() {
            Some(code) => Err(tonic::Status::new(code, "scripted abandon refusal")),
            None => Ok(tonic::Response::new(AbandonRuntimeUpgradeResponse {})),
        }
    }

    async fn get_runtime_upgrade(
        &self,
        request: tonic::Request<GetRuntimeUpgradeRequest>,
    ) -> Result<tonic::Response<GetRuntimeUpgradeResponse>, tonic::Status> {
        self.0
            .asked_pod_uids
            .lock()
            .unwrap()
            .push(request.get_ref().attempt_id.clone());
        // A node answers only for an operation it was asked to run: one no
        // pod here carries has no outcome.
        if !self
            .0
            .pods
            .carries_operation(&request.get_ref().operation_id)
        {
            return Ok(tonic::Response::new(GetRuntimeUpgradeResponse {
                status: RuntimeUpgradeStatus::Absent as i32,
                ..Default::default()
            }));
        }
        // As a real node: an abandoned operation never fences again, so one
        // it never saw stays absent, while a rollback or refusal it recorded
        // stays as it was.
        let abandoned = self
            .0
            .abandons
            .lock()
            .unwrap()
            .iter()
            .any(|(operation_id, _)| *operation_id == request.get_ref().operation_id);
        let scripted = *self.0.report.lock().unwrap();
        let report = match scripted {
            Report::RolledBack
            | Report::Refused
            | Report::EarlierRollback
            | Report::UnnamedRollback
            | Report::Absent => scripted,
            _ if abandoned => Report::Absent,
            _ => scripted,
        };
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
            Report::KeptPodRefused => {
                self.0.pods.stay_creating();
                RuntimeUpgradeStatus::Absent
            }
            Report::InProgress => RuntimeUpgradeStatus::InProgress,
            Report::Unreachable => return Err(tonic::Status::unavailable("node-api is down")),
            Report::Hangs => std::future::pending().await,
        };
        let pod_uid = match report {
            Report::EarlierRollback => Some(EARLIER_POD_UID.to_string()),
            Report::UnnamedRollback
            | Report::Absent
            | Report::KeptPodRefused
            | Report::InProgress => None,
            // A node keeps reporting a pod's rollback or refusal after the
            // pod is deleted, until a new pod's shim reaches it.
            Report::RolledBack | Report::Refused => self.0.reported_pod_uid(),
            _ => self.0.pods.current_uid(),
        };
        Ok(tonic::Response::new(GetRuntimeUpgradeResponse {
            status: status as i32,
            phase: 0,
            failure: match report {
                Report::RolledBack | Report::EarlierRollback | Report::UnnamedRollback => {
                    Some("boot failed".to_string())
                }
                Report::Refused => Some("E0 -> E1 is not a certified upgrade edge".to_string()),
                _ => None,
            },
            refused_attempt_id: match report {
                Report::PodRefused { .. } | Report::KeptPodRefused => self.0.pods.current_uid(),
                Report::EarlierPodRefused => Some(EARLIER_POD_UID.to_string()),
                _ => None,
            },
            refused_attempt_failure: matches!(
                report,
                Report::PodRefused { .. } | Report::EarlierPodRefused | Report::KeptPodRefused
            )
            .then(|| "the sandbox runs another pod's execution".to_string()),
            attempt_id: pod_uid,
            abandoned,
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
            local_artifact_id: pplx_space_snapshot_model::LocalArtifactId::new().to_string(),
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
    /// The UID of every pod a delete removed, a seeded one included, so a
    /// later create never reuses it.
    deleted_uids: Mutex<Vec<String>>,
    /// Another pod replaces the one at the name, under a new UID, just
    /// before each delete reaches the API, so the delete's UID precondition
    /// decides whether the replacement survives.
    pub(in crate::controllers::substrate::kubernetes) replace_before_deletes: AtomicBool,
    /// The worker's Node is gone from the API: its GET answers 404.
    pub(in crate::controllers::substrate::kubernetes) node_gone: AtomicBool,
    /// The API holds every pod GET while this is set, and answers each one
    /// it held once it's cleared. Left set, a GET is never answered.
    pub(in crate::controllers::substrate::kubernetes) pod_reads_hang: AtomicBool,
    /// Pod GETs the API answers before it drops the pod, as a forced delete
    /// takes a pod from the API with nothing confirmed on the node. Zero
    /// never drops it.
    pub(in crate::controllers::substrate::kubernetes) pod_gone_after_reads: AtomicUsize,
    /// The API commits a pod create and answers it with a 500.
    pub(in crate::controllers::substrate::kubernetes) creates_fail_after_commit: AtomicBool,
    /// A pod the API creates is already terminal, as a kubelet that failed
    /// it before any read reports it.
    pub(in crate::controllers::substrate::kubernetes) created_pods_fail: AtomicBool,
    /// The pod has no address: kubelet posts it only once the shim's
    /// pod-sandbox create returns, and a `stopped` upgrade settles inside it.
    pub(in crate::controllers::substrate::kubernetes) addressless: AtomicBool,
    /// The API answers a pod create with a 500 before it commits, as the
    /// confirming read sees a create whose answer was lost while the API
    /// server was still processing it.
    pub(in crate::controllers::substrate::kubernetes) creates_fail_uncommitted: AtomicBool,
    /// The API refuses a pod create with a 403, as a quota or an admission
    /// webhook does, and commits nothing.
    pub(in crate::controllers::substrate::kubernetes) creates_refused: AtomicBool,
    /// The API answers a pod create with a 500, its outcome unknown, while
    /// another pod, one that carries no runtime upgrade, holds the name.
    pub(in crate::controllers::substrate::kubernetes) creates_fail_behind_a_stale_pod: AtomicBool,
    /// A stale pod without a runtime upgrade takes the name just before the
    /// next pod create, which the API then answers with a 409.
    pub(in crate::controllers::substrate::kubernetes) creates_conflict_with_a_stale_pod: AtomicBool,
    /// The pod creates the API refused.
    pub(in crate::controllers::substrate::kubernetes) refused_creates: AtomicUsize,
    /// A statement run once, after the API commits the next pod create and
    /// before it answers: another activation's write landing in that window.
    pub(in crate::controllers::substrate::kubernetes) on_create_sql:
        Mutex<Option<(sqlx::PgPool, String)>>,
    /// The API commits a pod create and never answers it.
    pub(in crate::controllers::substrate::kubernetes) creates_hang_after_commit: AtomicBool,
}

impl Pods {
    pub(in crate::controllers::substrate::kubernetes) fn exists(&self) -> bool {
        self.pod.lock().unwrap().is_some()
    }

    /// The pod as the API serves it now, for seeding a reflector that saw it
    /// before the API replaced it.
    pub(in crate::controllers::substrate::kubernetes) fn snapshot(&self) -> Option<Pod> {
        self.current()
    }

    /// The pod as the API serves it, for a reflector that has seen it.
    pub(in crate::controllers::substrate::kubernetes) fn current(&self) -> Option<Pod> {
        let pod = self.pod.lock().unwrap().clone()?;
        Some(serde_json::from_value(self.rendered(&pod)).expect("the pod fixture parses"))
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

    /// Whether a pod created here, or the pod at the name, carries
    /// `operation_id`'s runtime upgrade.
    pub(in crate::controllers::substrate::kubernetes) fn carries_operation(
        &self,
        operation_id: &str,
    ) -> bool {
        let carries = |pod: &serde_json::Value| {
            annotation(pod, SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION) == Some(operation_id)
        };
        self.created.lock().unwrap().iter().any(carries)
            || self.pod.lock().unwrap().as_ref().is_some_and(carries)
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
                    while pods.pod_reads_hang.load(Ordering::SeqCst) {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                    let pod = {
                        let mut stored = pods.pod.lock().unwrap();
                        let pod = stored.clone();
                        if pods.pod_gone_after_reads.try_update(
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            |left| left.checked_sub(1),
                        ) == Ok(1)
                        {
                            *stored = None;
                        }
                        pod
                    };
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
                    if pods.creates_fail_uncommitted.load(Ordering::SeqCst) {
                        return kube_status(StatusCode::INTERNAL_SERVER_ERROR, "InternalError");
                    }
                    let stale_in_the_way =
                        pods.creates_fail_behind_a_stale_pod.load(Ordering::SeqCst)
                            || pods
                                .creates_conflict_with_a_stale_pod
                                .swap(false, Ordering::SeqCst);
                    if stale_in_the_way {
                        let conflicts =
                            !pods.creates_fail_behind_a_stale_pod.load(Ordering::SeqCst);
                        *pods.pod.lock().unwrap() = Some(serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "metadata": {
                                "name": name,
                                "namespace": "space",
                                "uid": format!("{name}-stale-uid"),
                            },
                            "spec": {
                                "nodeName": WORKER_NODE,
                                "containers": [{ "name": "sandbox", "image": "sandbox" }],
                            },
                            "status": { "phase": "Failed" },
                        }));
                        return if conflicts {
                            kube_status(StatusCode::CONFLICT, "AlreadyExists")
                        } else {
                            kube_status(StatusCode::INTERNAL_SERVER_ERROR, "InternalError")
                        };
                    }
                    if pods.creates_refused.load(Ordering::SeqCst) {
                        pods.refused_creates.fetch_add(1, Ordering::SeqCst);
                        return kube_status(StatusCode::FORBIDDEN, "Forbidden");
                    }
                    // Every create gets a UID no pod here has had, a seeded
                    // one at the name included, as the API server gives
                    // each pod its own.
                    let uid = {
                        let seeded = pods.current_uid();
                        let deleted = pods.deleted_uids.lock().unwrap().clone();
                        let mut uids = pods.created_uids.lock().unwrap();
                        let uid = (0..)
                            .map(|earlier| {
                                if earlier == 0 {
                                    format!("{name}-uid")
                                } else {
                                    format!("{name}-uid-{earlier}")
                                }
                            })
                            .find(|uid| {
                                !uids.contains(uid)
                                    && !deleted.contains(uid)
                                    && seeded.as_ref() != Some(uid)
                            })
                            .expect("an unused UID");
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
                    let mut created = created;
                    if pods.created_pods_fail.load(Ordering::SeqCst) {
                        created["status"]["phase"] = serde_json::json!("Failed");
                    }
                    pods.created.lock().unwrap().push(requested);
                    pods.creates.fetch_add(1, Ordering::SeqCst);
                    *pods.pod.lock().unwrap() = Some(created.clone());
                    if pods.creates_hang_after_commit.load(Ordering::SeqCst) {
                        std::future::pending::<()>().await;
                    }
                    if pods.creates_fail_after_commit.load(Ordering::SeqCst) {
                        return kube_status(StatusCode::INTERNAL_SERVER_ERROR, "InternalError");
                    }
                    let on_create = pods.on_create_sql.lock().unwrap().take();
                    if let Some((pool, statement)) = on_create {
                        sqlx::query(sqlx::AssertSqlSafe(sql!(statement.as_str())))
                            .execute(&pool)
                            .await
                            .expect("the on-create statement runs");
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
                    if pods.replace_before_deletes.load(Ordering::SeqCst)
                        && let Some(pod) = pod.as_mut()
                    {
                        let replacement = format!(
                            "{}-replacement",
                            pod.pointer("/metadata/uid")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or_default()
                        );
                        pod["metadata"]["uid"] = serde_json::json!(replacement);
                    }
                    let current_uid = pod
                        .as_ref()
                        .and_then(|pod| pod.pointer("/metadata/uid"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned);
                    if pod.is_some() && wanted_uid.is_some() && wanted_uid != current_uid {
                        return kube_status(StatusCode::CONFLICT, "Conflict");
                    }
                    pods.deletes.fetch_add(1, Ordering::SeqCst);
                    if let Some(uid) = current_uid {
                        pods.deleted_uids.lock().unwrap().push(uid);
                    }
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
            report_pod_uid: Mutex::default(),
            wakes: Mutex::default(),
            asked_pod_uids: Mutex::default(),
            publisher: (pool.clone(), node_id),
            pods: Arc::clone(&pods),
            abandons: Mutex::default(),
            abandon_refusal: Mutex::default(),
            operations_of_another_sandbox: Mutex::default(),
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

    /// [`Self::substrate`] whose pod reflector has also seen the pod the API
    /// holds now, as it is now.
    pub(in crate::controllers::substrate::kubernetes) fn substrate_caching_the_pod(
        &self,
        pool: &sqlx::PgPool,
    ) -> KubernetesSubstrate {
        let cached = self.pods.current().into_iter().collect();
        self.substrate_caching(pool, cached, vec![worker_node()])
    }

    /// [`Self::substrate`] whose pod reflector holds `sandbox_pods`, which
    /// may be pods the API has since replaced.
    pub(in crate::controllers::substrate::kubernetes) fn substrate_caching_pods(
        &self,
        pool: &sqlx::PgPool,
        sandbox_pods: Vec<Pod>,
    ) -> KubernetesSubstrate {
        self.substrate_caching(pool, sandbox_pods, vec![worker_node()])
    }

    /// A substrate over `pool` whose sandbox-pod reflector holds `sandbox_pods`
    /// and whose Node reflector holds `nodes`.
    pub(in crate::controllers::substrate::kubernetes) fn substrate_caching(
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
