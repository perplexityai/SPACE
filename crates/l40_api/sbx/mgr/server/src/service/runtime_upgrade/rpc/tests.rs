//! `UpgradeRuntime` and `GetRuntimeUpgrade` as a Kubernetes pod's shim calls
//! them: with the pod's network namespace and its Kubernetes route identity,
//! never a Direct one, against a test-served node-manager.

use std::sync::{Arc, Mutex};

use pplx_space_common_core::types::{ExecutionMetadata, LocalArtifactId, SandboxId};
use pplx_space_node_mgr_client::proto::{
    CreateBtrfsSnapshotRequest, CreateBtrfsSnapshotResponse, DeleteBtrfsSnapshotRequest,
    DeleteBtrfsSnapshotResponse, ResolveExecutionEnvironmentRequest,
    ResolveExecutionEnvironmentResponse, RestoreInstanceSnapshotRequest,
    RestoreInstanceSnapshotResponse,
};
use tonic::Status;

use super::super::node_effects::tests::{
    Fixture, certified_target, environments, fixture, recording,
};
use crate::db::{RuntimeUpgradeOperationId, RuntimeUpgradePhase};
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::proto::{
    GetRuntimeUpgradeRequest, RuntimeUpgradeAuthorization, RuntimeUpgradeOutcome,
    RuntimeUpgradeStatus, SandboxSubstrateKind, SpaceApiRouteIdentity, StartRequest,
    UpgradeRuntimeRequest, WakeTargetState,
};
use crate::service::SandboxManagerService;
use crate::service::activation_support::internal_request;
use crate::state::{SandboxState, SandboxStatus};

const POD_NETNS: &str = "/var/run/netns/cni-runtime-upgrade";
const POD_UID: &str = "4f6b0d4e-6a59-4b5c-9e43-3a1d0d6f2a10";
const OTHER_POD_UID: &str = "9d2c6f1b-0c1e-4a57-8b0e-5f7a3c2d1e40";

/// The route identity a pod's shim sends: Kubernetes, with the pod's UID.
fn pod_identity(sandbox_id: &str, pod_uid: &str, generation: i64) -> SpaceApiRouteIdentity {
    SpaceApiRouteIdentity {
        sandbox_id: Some(sandbox_id.to_string()),
        route_generation: Some(generation.cast_unsigned()),
        current_node_id: Some(crate::service::test_options::test_node_id().to_string()),
        substrate_kind: SandboxSubstrateKind::Kubernetes as i32,
        execution_generation: Some(generation),
        pod_uid: Some(pod_uid.to_string()),
        activation_attempt_id: None,
    }
}

/// The `UpgradeRuntime` a runtime upgrade pod's shim sends: the pod's own
/// cold start, with its namespace and route identity.
pub(in crate::service::runtime_upgrade) fn pod_upgrade(
    sandbox_id: &str,
    target: &ExecutionMetadata,
    operation_id: RuntimeUpgradeOperationId,
    target_state: WakeTargetState,
) -> UpgradeRuntimeRequest {
    UpgradeRuntimeRequest {
        sandbox_id: sandbox_id.to_string(),
        operation_id: operation_id.to_string(),
        target_environment: target.environment().unwrap().to_string(),
        authorization: Some(authorization()),
        start: Some(StartRequest {
            sandbox_id: sandbox_id.to_string(),
            blocking: true,
            netns_path: Some(POD_NETNS.to_string()),
            route_identity: Some(pod_identity(sandbox_id, POD_UID, 4)),
            ..Default::default()
        }),
        target_state: target_state as i32,
        stage_from: None,
        checkpoint_local_artifact_id: None,
        attempt_id: None,
    }
}

pub(in crate::service::runtime_upgrade) fn operation() -> RuntimeUpgradeOperationId {
    RuntimeUpgradeOperationId::from_uuid(uuid::Uuid::now_v7())
}

pub(in crate::service::runtime_upgrade) fn authorization() -> RuntimeUpgradeAuthorization {
    RuntimeUpgradeAuthorization {
        authorization_id: "environment-block-e0".into(),
        reason: "e0 runs a blocked runtime".into(),
        memory_loss: true,
    }
}

impl Fixture {
    /// Write a complete local suspend snapshot, so a restore uses it rather
    /// than staging one from object storage, which this harness can't.
    fn materialize_local_snapshot(&self) {
        for artifact in crate::snapshot_upload::COMMON_VM_ARTIFACTS {
            std::fs::write(self.checkpoint().join(artifact.local_name), b"snapshot").unwrap();
        }
    }

    fn assert_no_node_calls(&self) {
        assert!(self._node.calls().is_empty(), "{:?}", self._node.calls());
    }

    /// Save `target` as the sandbox's consumer, as a sandbox already on it.
    fn put_on(&self, target: &ExecutionMetadata) {
        crate::db::upsert_sandbox_execution_metadata(
            &self.service.db.lock().unwrap(),
            &self.sandbox_id,
            target,
        )
        .unwrap();
    }

    /// The fixture's service, seeing every runtime as live.
    fn live_service(&self) -> SandboxManagerService {
        SandboxManagerService {
            sandbox_runtime_live: Arc::new(|_| true),
            ..self.service.clone()
        }
    }
}

/// Send `request` as a pod's shim does: a Kubernetes substrate derives the
/// attempt id from the pod UID, so a request that names none runs as the
/// attempt of the pod its start names. A test of a request that names no
/// attempt calls the service directly.
pub(in crate::service::runtime_upgrade) async fn upgrade(
    service: &SandboxManagerService,
    mut request: UpgradeRuntimeRequest,
) -> Result<crate::proto::UpgradeRuntimeResponse, Status> {
    if request.attempt_id.is_none() {
        request.attempt_id = request
            .start
            .as_ref()
            .and_then(|start| start.route_identity.as_ref())
            .and_then(|identity| identity.pod_uid.clone());
    }
    service
        .upgrade_runtime(internal_request(request))
        .await
        .map(tonic::Response::into_inner)
}

pub(in crate::service::runtime_upgrade) fn report(
    service: &SandboxManagerService,
    sandbox_id: &str,
    operation_id: RuntimeUpgradeOperationId,
) -> crate::proto::GetRuntimeUpgradeResponse {
    service
        .get_runtime_upgrade(&internal_request(GetRuntimeUpgradeRequest {
            sandbox_id: sandbox_id.to_string(),
            operation_id: operation_id.to_string(),
            attempt_id: None,
        }))
        .unwrap()
}

/// A node-manager that answers the upgrade's disk copy and the target's
/// resolution, so the upgrade reaches its cold boot, and records the disk
/// restore a rollback from the boot phase performs.
fn node_reaching_the_boot(
    target: &ExecutionMetadata,
    calls: &Arc<Mutex<Vec<String>>>,
) -> FakeNodeManager {
    use pplx_space_node_runtime_core::catalog::CompiledEnvironment;
    let compiled =
        CompiledEnvironment::for_architecture(pplx_space_common_core::types::Arch::X86_64).unwrap();
    let definition = compiled
        .iter()
        .find(|environment| Some(environment.definition().reference()) == target.environment())
        .unwrap()
        .definition()
        .clone();
    let execution = definition
        .bind_execution(definition.execution_requirements().unwrap())
        .unwrap();
    let resolved = ResolveExecutionEnvironmentResponse {
        accepted_selection: 0,
        definition: Some((&definition).into()),
        execution: Some((&execution).into()),
    };
    let (deleted, created, restored) = (calls.clone(), calls.clone(), calls.clone());
    FakeNodeManager::default()
        .unary(
            "DeleteBtrfsSnapshot",
            move |request: DeleteBtrfsSnapshotRequest| {
                recording(&deleted, format!("delete {}", request.snapshot_name));
                Err::<DeleteBtrfsSnapshotResponse, _>(Status::not_found("absent"))
            },
        )
        .unary(
            "CreateBtrfsSnapshot",
            move |request: CreateBtrfsSnapshotRequest| {
                recording(&created, format!("create {}", request.snapshot_name));
                Ok(CreateBtrfsSnapshotResponse {})
            },
        )
        .unary(
            "ResolveExecutionEnvironment",
            move |_: ResolveExecutionEnvironmentRequest| Ok(resolved.clone()),
        )
        .unary(
            "RestoreInstanceSnapshot",
            move |request: RestoreInstanceSnapshotRequest| {
                recording(&restored, format!("restore {}", request.snapshot_name));
                Ok(RestoreInstanceSnapshotResponse {})
            },
        )
}

/// The upgrade's boot runs with the pod's namespace: a pod's cold start gets
/// past the runtime-context check a namespace-less Kubernetes Wake fails, and
/// the upgrade reaches its boot. This harness can't launch a microVM, so the
/// boot fails and the upgrade rolls back from its boot phase (restoring the
/// preserved disk), leaving the old pair and the rollback as the operation's
/// recorded outcome, for the pod that ran it. The pod's create failed, so
/// kubelet retries it under the same pod UID: that retry gets the rollback
/// back and never fences or touches the disk again.
#[tokio::test]
async fn a_pods_upgrade_boots_in_the_pods_namespace_under_the_target() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (source, _) = environments();
    let target = certified_target(&source);
    let f = fixture(
        SandboxState::Stopped,
        false,
        node_reaching_the_boot(&target, &calls),
    );
    let operation_id = operation();

    let error = upgrade(
        &f.service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Stopped,
        ),
    )
    .await
    .expect_err("the harness can't boot a sandbox");

    assert!(
        !error
            .message()
            .contains(pplx_space_common_core::wake_signals::MISSING_RUNTIME_CONTEXT_MARKER),
        "{error}"
    );
    assert!(error.message().contains("rolled back"), "{error}");
    assert!(error.message().contains("cold boot failed"), "{error}");
    let preserved = super::super::preserved_disk_name(operation_id);
    assert!(
        calls
            .lock()
            .unwrap()
            .contains(&format!("restore {preserved}")),
        "the rollback must restore the disk the boot may have written: {:?}",
        calls.lock().unwrap()
    );
    let row = f.row();
    assert_eq!(row.state, SandboxStatus::from(SandboxState::Stopped));
    assert_eq!(
        row.execution_metadata
            .as_ref()
            .and_then(ExecutionMetadata::environment),
        f.source.environment()
    );
    let reported = report(&f.service, &f.sandbox_id, operation_id);
    assert_eq!(reported.status, RuntimeUpgradeStatus::RolledBack as i32);
    assert!(
        reported
            .failure
            .as_deref()
            .is_some_and(|failure| failure.contains("cold boot failed")),
        "{reported:?}"
    );
    assert_eq!(reported.attempt_id.as_deref(), Some(POD_UID));

    let disk_calls = calls.lock().unwrap().len();
    let retry = upgrade(
        &f.service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Stopped,
        ),
    )
    .await
    .expect_err("a retry from the same pod gets its rollback back");
    assert_eq!(retry.code(), tonic::Code::FailedPrecondition, "{retry}");
    assert!(retry.message().contains("cold boot failed"), "{retry}");
    assert_eq!(calls.lock().unwrap().len(), disk_calls, "{:?}", calls);
    assert_eq!(report(&f.service, &f.sandbox_id, operation_id), reported);
}

/// A request that names no attempt runs as a fresh one under an id the node
/// generates: the response returns it and the report names it. A rollback
/// is final for that id as for any other, so a retry that names it gets the
/// rollback back without fencing or touching the disk again.
#[tokio::test]
async fn an_omitted_attempt_id_runs_under_one_the_node_generates() {
    // `pod_upgrade` names no attempt; [`upgrade`] would derive one from the
    // pod UID, so these call the service directly.
    // Already on the target: the response carries the generated id.
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);
    let operation_id = operation();
    let response = f
        .service
        .upgrade_runtime(internal_request(pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Stopped,
        )))
        .await
        .unwrap()
        .into_inner();
    let generated = response.attempt_id.expect("the node names the attempt");
    assert!(!generated.is_empty());
    assert_ne!(generated, POD_UID, "the node doesn't read the pod UID");
    assert_eq!(
        report(&f.service, &f.sandbox_id, operation_id)
            .attempt_id
            .as_deref(),
        Some(generated.as_str())
    );

    // Rolled back: the report names the generated id, and a retry under it
    // gets the rollback back.
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (source, _) = environments();
    let target = certified_target(&source);
    let f = fixture(
        SandboxState::Stopped,
        false,
        node_reaching_the_boot(&target, &calls),
    );
    let operation_id = operation();
    let error = f
        .service
        .upgrade_runtime(internal_request(pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Stopped,
        )))
        .await
        .expect_err("the harness can't boot a sandbox");
    assert!(error.message().contains("cold boot failed"), "{error}");
    let reported = report(&f.service, &f.sandbox_id, operation_id);
    assert_eq!(reported.status, RuntimeUpgradeStatus::RolledBack as i32);
    let generated = reported
        .attempt_id
        .clone()
        .expect("the rollback names its attempt");
    assert!(!generated.is_empty());
    assert_ne!(generated, POD_UID, "the node doesn't read the pod UID");

    let disk_calls = calls.lock().unwrap().len();
    let mut retry = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation_id,
        WakeTargetState::Stopped,
    );
    retry.attempt_id = Some(generated);
    let retried = f
        .service
        .upgrade_runtime(internal_request(retry))
        .await
        .expect_err("the attempt's rollback is final");
    assert_eq!(retried.code(), tonic::Code::FailedPrecondition, "{retried}");
    assert!(
        retried.message().contains("only a new attempt retries it")
            && retried.message().contains("cold boot failed"),
        "{retried}"
    );
    assert_eq!(calls.lock().unwrap().len(), disk_calls, "{:?}", calls);
    assert_eq!(report(&f.service, &f.sandbox_id, operation_id), reported);
}

/// A sandbox already on the target fences nothing, records that, and leaves a
/// stopped target as it is. Asked again under the same operation, the node
/// answers the same without touching the disk.
#[tokio::test]
async fn an_upgrade_already_on_its_target_is_recorded_and_replays_the_same() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);
    let operation_id = operation();
    assert_eq!(
        report(&f.service, &f.sandbox_id, operation_id).status,
        RuntimeUpgradeStatus::Absent as i32
    );

    for _ in 0..2 {
        let response = upgrade(
            &f.service,
            pod_upgrade(
                &f.sandbox_id,
                &target,
                operation_id,
                WakeTargetState::Stopped,
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            response.outcome,
            RuntimeUpgradeOutcome::AlreadyOnTarget as i32
        );
        assert_eq!(response.start, None);
        assert_eq!(
            report(&f.service, &f.sandbox_id, operation_id).status,
            RuntimeUpgradeStatus::AlreadyOnTarget as i32
        );
    }
    f.assert_no_node_calls();
    assert_eq!(f.row().state, SandboxStatus::from(SandboxState::Stopped));
    // An operation names one sandbox.
    let other = SandboxId::new().to_string();
    let refused = f
        .service
        .get_runtime_upgrade(&internal_request(GetRuntimeUpgradeRequest {
            sandbox_id: other,
            operation_id: operation_id.to_string(),
            attempt_id: None,
        }))
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument, "{refused}");
}

/// A started target on a stopped sandbox already on it is the pod's
/// activation: the same `UpgradeRuntime` cold-starts it with the pod's start,
/// rather than answering with nothing and leaving the shim to issue a second
/// RPC. This harness can't launch, so the start's own failure is what the
/// call returns, after the upgrade recorded it found its target.
#[tokio::test]
async fn a_started_target_already_reached_is_activated_in_place() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);
    let operation_id = operation();
    let request = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation_id,
        WakeTargetState::Started,
    );
    let plain = f
        .service
        .start_with_runtime_upgrade(
            internal_request(request.start.clone().unwrap()),
            crate::service::start::StartAdmission::default(),
        )
        .await
        .expect_err("the harness can't boot a sandbox");

    let activated = upgrade(&f.service, request)
        .await
        .expect_err("the harness can't boot a sandbox");

    // The same start, so the same refusal.
    assert_eq!(activated.code(), plain.code(), "{activated} / {plain}");
    assert_eq!(activated.message(), plain.message());
    // The start was claimed before it ran. This harness refuses it before
    // the activation begins, so the sandbox is still at rest on the execution
    // it had, and a retry of the operation from a new pod may try it again.
    // The refused pod's own retry gets its refusal back.
    let mut from_new_pod = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation_id,
        WakeTargetState::Started,
    );
    from_new_pod.start.as_mut().unwrap().route_identity =
        Some(pod_identity(&f.sandbox_id, OTHER_POD_UID, 4));
    let retried = upgrade(&f.service, from_new_pod).await.unwrap_err();
    assert_eq!(retried.code(), plain.code(), "{retried} / {plain}");
    assert_eq!(retried.message(), plain.message());
    let same_pod = upgrade(
        &f.service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Started,
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(same_pod.code(), tonic::Code::FailedPrecondition);
    assert!(
        same_pod.message().contains("only a new attempt retries it")
            && same_pod.message().contains(plain.message()),
        "{same_pod}"
    );
    // The control plane reads that the operation claimed its start.
    let reported = report(&f.service, &f.sandbox_id, operation_id);
    assert_eq!(
        reported.status,
        RuntimeUpgradeStatus::AlreadyOnTarget as i32
    );
    assert!(reported.activation_claimed, "{reported:?}");
    assert_eq!(
        report(&f.service, &f.sandbox_id, operation_id).status,
        RuntimeUpgradeStatus::AlreadyOnTarget as i32
    );
    assert!(
        !f._node
            .calls()
            .iter()
            .any(|call| call.contains("BtrfsSnapshot") || call.contains("InstanceSnapshot")),
        "{:?}",
        f._node.calls()
    );
}

/// Give the fixture's sandbox a live started execution with route
/// `identity`, as a verified upgrade boot leaves it.
fn run_as(f: &Fixture, identity: &SpaceApiRouteIdentity) {
    let db = f.service.db.lock().unwrap();
    crate::db::update_runtime_fields(
        &db,
        f.sandbox_id.parse().unwrap(),
        crate::db::RuntimeFields {
            uid: 10000,
            slot: 7,
            guest_cid: 37,
            pid: std::process::id(),
            spaced_host_pid: Some(10001),
            netgtw_pid: 10002,
            vsock_uds_path: "/space/run/firecracker/sandboxes/test/root/v.sock",
            api_socket_path: "/space/run/firecracker/sandboxes/test/api.sock",
            spaced_endpoint_proof: None,
            space_api_route_identity:
                crate::service::activation_support::space_api_route_identity_from_wire(Some(
                    identity,
                ))
                .unwrap(),
            route_identity_provenance:
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        },
    )
    .unwrap();
    db.execute(
        "UPDATE sandboxes SET state = 'started' WHERE id = ?1",
        crate::db::fixture::params![f.sandbox_id],
    )
    .unwrap();
}

/// Record a retired upgrade of the fixture's sandbox under `operation_id`.
fn retire(
    f: &Fixture,
    target: &ExecutionMetadata,
    operation_id: RuntimeUpgradeOperationId,
    target_state: crate::db::RuntimeUpgradeTargetState,
) {
    let db = f.service.db.lock().unwrap();
    let sandbox = crate::db::read::sandbox(&db, &f.sandbox_id)
        .unwrap()
        .unwrap();
    let begun = crate::db::begin_runtime_upgrade(
        &db,
        crate::db::NewRuntimeUpgrade {
            operation_id,
            sandbox_id: &f.sandbox_id,
            observed: crate::db::ObservedSandbox::of(&sandbox),
            target_environment: target.environment().unwrap(),
            target_state,
            authorization: &crate::db::RuntimeUpgradeAuthorization {
                id: "environment-block-e0".into(),
                reason: "e0 runs a blocked runtime".into(),
                memory_loss: true,
            },
            checkpoint_dir_identity: None,
            previous_execution: &f.source,
            attempt_id: &crate::db::new_runtime_upgrade_attempt_id(),
        },
    )
    .unwrap();
    assert!(
        matches!(begun, crate::db::BeginRuntimeUpgrade::Fenced(_)),
        "{begun:?}"
    );
    assert!(
        crate::db::advance_runtime_upgrade(
            &db,
            operation_id,
            RuntimeUpgradePhase::Fenced,
            RuntimeUpgradePhase::Preserved
        )
        .unwrap()
    );
    assert!(crate::db::bind_runtime_upgrade_consumer(&db, operation_id, target).unwrap());
    for (from, to) in [
        (RuntimeUpgradePhase::Booting, RuntimeUpgradePhase::Verified),
        (RuntimeUpgradePhase::Verified, RuntimeUpgradePhase::Retired),
    ] {
        assert!(crate::db::advance_runtime_upgrade(&db, operation_id, from, to).unwrap());
    }
}

/// Who runs the sandbox when a finished upgrade is replayed.
#[derive(Clone, Copy, Debug)]
enum Runner {
    /// Nothing: the sandbox is stopped, as the upgrade left it.
    Nobody,
    /// The pod with this UID, started.
    Pod(&'static str),
    /// The replaying pod's UID at an earlier execution generation.
    EarlierGeneration,
    /// The replaying pod's execution, stopped since.
    StoppedSince,
    /// A started execution replayed by a request that names no pod, as an
    /// operator's for a Direct sandbox: it gets the live execution.
    Unidentified,
}

/// A replay of an upgrade that already finished never moves the sandbox
/// again. For a started target it reports the execution the boot left
/// running, but only to the pod that runs it: a fresh pod replaying the same
/// operation is refused cleanly, so the control plane re-places it, rather
/// than handed another pod's execution. A stopped target answers upgraded
/// with nothing to run.
#[tokio::test]
async fn a_replay_of_a_finished_upgrade_answers_only_the_pod_that_runs_it() {
    use crate::db::RuntimeUpgradeTargetState::{Started, Stopped};
    for (target_state, runner, expect) in [
        (Started, Runner::Pod(POD_UID), Ok(true)),
        (Started, Runner::Pod(OTHER_POD_UID), Err(())),
        (Started, Runner::EarlierGeneration, Err(())),
        (Started, Runner::StoppedSince, Err(())),
        (Started, Runner::Nobody, Err(())),
        (Started, Runner::Unidentified, Ok(true)),
        (Stopped, Runner::Nobody, Ok(false)),
    ] {
        let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
        let target = certified_target(&f.source);
        let operation_id = operation();
        retire(&f, &target, operation_id, target_state);
        let wire_state = match target_state {
            Started => WakeTargetState::Started,
            Stopped => WakeTargetState::Stopped,
        };
        let mut request = pod_upgrade(&f.sandbox_id, &target, operation_id, wire_state);
        match runner {
            Runner::Nobody => {}
            Runner::Pod(pod_uid) => run_as(&f, &pod_identity(&f.sandbox_id, pod_uid, 4)),
            Runner::EarlierGeneration => run_as(&f, &pod_identity(&f.sandbox_id, POD_UID, 3)),
            Runner::StoppedSince => {
                run_as(&f, &pod_identity(&f.sandbox_id, POD_UID, 4));
                f.service
                    .db
                    .lock()
                    .unwrap()
                    .execute(
                        "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
                        crate::db::fixture::params![f.sandbox_id],
                    )
                    .unwrap();
            }
            Runner::Unidentified => {
                let mut identity = pod_identity(&f.sandbox_id, POD_UID, 4);
                identity.pod_uid = None;
                identity.substrate_kind = SandboxSubstrateKind::Unspecified as i32;
                run_as(&f, &identity);
                request.start.as_mut().unwrap().route_identity = Some(identity);
            }
        }
        let service = f.live_service();
        let before = f.row();

        let outcome = upgrade(&service, request).await;

        match (expect, outcome) {
            (Ok(attached), Ok(response)) => {
                assert_eq!(response.outcome, RuntimeUpgradeOutcome::Upgraded as i32);
                assert_eq!(
                    response.start.as_ref().map(|start| start.pid),
                    attached.then(std::process::id),
                    "{target_state:?} {runner:?}"
                );
            }
            (Err(()), Err(error)) => {
                assert_eq!(
                    error.code(),
                    tonic::Code::FailedPrecondition,
                    "{runner:?}: {error}"
                );
                assert!(error.message().contains("not running this pod"), "{error}");
            }
            (expect, outcome) => {
                panic!("{target_state:?} {runner:?}: expected {expect:?}, got {outcome:?}")
            }
        }
        let after = f.row();
        assert_eq!(
            after.state, before.state,
            "a replay must not move the sandbox"
        );
        assert_eq!(after.node_execution_id, before.node_execution_id);
        assert_eq!(
            report(&f.service, &f.sandbox_id, operation_id).status,
            RuntimeUpgradeStatus::Upgraded as i32
        );
        f.assert_no_node_calls();
    }
}

/// A started sandbox already on the target is the pod's only if it runs the
/// pod's execution: the same pod gets its execution back, any other pod is
/// refused, and nothing is stopped or started either way.
#[tokio::test]
async fn a_started_sandbox_already_on_its_target_answers_only_its_own_pod() {
    for (runner, attached) in [(POD_UID, true), (OTHER_POD_UID, false)] {
        let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
        let target = certified_target(&f.source);
        f.put_on(&target);
        run_as(&f, &pod_identity(&f.sandbox_id, runner, 4));
        let service = f.live_service();
        let operation_id = operation();

        let outcome = upgrade(
            &service,
            pod_upgrade(
                &f.sandbox_id,
                &target,
                operation_id,
                WakeTargetState::Started,
            ),
        )
        .await;

        if attached {
            let response = outcome.unwrap();
            assert_eq!(
                response.outcome,
                RuntimeUpgradeOutcome::AlreadyOnTarget as i32
            );
            assert_eq!(
                response.start.map(|start| start.pid),
                Some(std::process::id())
            );
        } else {
            let error = outcome.unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
            assert!(
                error
                    .message()
                    .contains("already on the runtime upgrade's target"),
                "{error}"
            );
        }
        assert_eq!(f.row().state, SandboxStatus::from(SandboxState::Started));
        let reported = report(&f.service, &f.sandbox_id, operation_id);
        assert_eq!(
            reported.status,
            RuntimeUpgradeStatus::AlreadyOnTarget as i32
        );
        // An answer with the running execution is the operation's
        // activation; a refusal leaves it unspent.
        assert_eq!(reported.activation_claimed, attached, "{reported:?}");
        f.assert_no_node_calls();
    }
}

/// An operation answered with the execution already running never starts
/// the sandbox again: a retry after a later stop, from a caller that names
/// no pod and so would take any execution, is refused rather than undoing
/// that stop.
#[tokio::test]
async fn an_operation_answered_with_a_running_execution_never_starts_it_after_a_stop() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);
    let mut direct = pod_identity(&f.sandbox_id, POD_UID, 4);
    direct.pod_uid = None;
    direct.substrate_kind = SandboxSubstrateKind::Direct as i32;
    run_as(&f, &direct);
    let service = f.live_service();
    let operation_id = operation();
    let request = || {
        let mut request = pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Started,
        );
        request.start.as_mut().unwrap().route_identity = Some(direct.clone());
        request
    };
    let attached = upgrade(&service, request()).await.unwrap();
    assert_eq!(
        attached.start.map(|start| start.pid),
        Some(std::process::id())
    );
    // The sandbox is stopped after the answer, which the caller lost.
    service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
            crate::db::fixture::params![f.sandbox_id],
        )
        .unwrap();

    let error = upgrade(&service, request()).await.unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("already activated"), "{error}");
    assert_eq!(f.row().state, SandboxStatus::from(SandboxState::Stopped));
    f.assert_no_node_calls();
}

/// A suspended sandbox whose checkpoint the target produced is activated by
/// restoring that checkpoint, with the pod's start: the `Start` a restore
/// pod sends, keeping the sandbox's memory.
#[tokio::test]
async fn a_suspended_sandbox_already_on_its_target_is_restored_in_place() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    std::fs::write(
        f.checkpoint().join("runtime.json"),
        serde_json::to_vec(
            &pplx_space_common_core::types::SnapshotExecutionMetadata::new(target.clone()).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let checkpoint = f.adopt_checkpoint();
    f.materialize_local_snapshot();
    let operation_id = operation();
    let mut request = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation_id,
        WakeTargetState::Started,
    );
    request.checkpoint_local_artifact_id = Some(checkpoint.to_string());
    let activated = upgrade(&f.service, request)
        .await
        .expect_err("the harness can't restore a sandbox");

    // The checkpoint launch's template lookup is what this harness can't
    // answer; a cold start of the suspended row is refused before any.
    assert!(activated.message().contains("GetTemplate"), "{activated}");
    assert_eq!(
        report(&f.service, &f.sandbox_id, operation_id).status,
        RuntimeUpgradeStatus::AlreadyOnTarget as i32
    );
}

/// A suspended sandbox absent from the node is staged from `stage_from`
/// before the upgrade looks for it. This harness has no object store, so
/// staging is what fails; without `stage_from` the upgrade finds no sandbox.
#[tokio::test]
async fn an_absent_suspended_sandbox_is_staged_before_the_upgrade() {
    let (source, _) = environments();
    let target = certified_target(&source);
    let service = SandboxManagerService::new_with_options(
        crate::db::test_conn(),
        crate::service::SandboxManagerServiceOptions::for_test(),
    );
    let sandbox_id = SandboxId::new().to_string();
    let request = || {
        let mut request = pod_upgrade(&sandbox_id, &target, operation(), WakeTargetState::Started);
        request.checkpoint_local_artifact_id = Some(LocalArtifactId::new().to_string());
        request
    };

    let unstaged = upgrade(&service, request()).await.unwrap_err();
    assert_eq!(unstaged.code(), tonic::Code::NotFound, "{unstaged}");

    let mut staged = request();
    staged.stage_from = Some(crate::proto::RuntimeUpgradeStageFrom {
        suspend_restore_storage_uri: format!(
            "s3://space-test/suspend/{sandbox_id}/{}?region=us-east-1",
            uuid::Uuid::now_v7()
        ),
        suspend_restore_source_node: None,
    });
    let error = upgrade(&service, staged).await.unwrap_err();
    assert_ne!(error.code(), tonic::Code::NotFound, "{error}");
    assert!(error.message().contains("stage"), "{error}");
}

/// A suspended sandbox is upgraded only from the checkpoint the upgrade was
/// decided from, as the Wake's upgrade is: a copy pinning another artifact,
/// or an upgrade naming none, is refused before anything is fenced.
#[tokio::test]
async fn a_pods_upgrade_of_a_suspended_sandbox_needs_the_decided_checkpoint() {
    // (named checkpoint, whether the row pins a checkpoint of its own)
    for (decided, pinned) in [
        (Some(LocalArtifactId::new()), true),
        (None, true),
        (None, false),
    ] {
        let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
        if pinned {
            f.adopt_checkpoint();
        }
        let target = certified_target(&f.source);
        let mut request = pod_upgrade(
            &f.sandbox_id,
            &target,
            operation(),
            WakeTargetState::Started,
        );
        request.checkpoint_local_artifact_id = decided.map(|id| id.to_string());

        let error = upgrade(&f.service, request).await.unwrap_err();

        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
        assert!(error.message().contains("decided from"), "{error}");
        assert_eq!(f.row().state, SandboxStatus::from(SandboxState::Suspended));
        f.assert_no_node_calls();
    }

    // The decided checkpoint passes the check and reaches the upgrade's node
    // work, which this fake doesn't answer.
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let checkpoint = f.adopt_checkpoint();
    let mut request = pod_upgrade(
        &f.sandbox_id,
        &certified_target(&f.source),
        operation(),
        WakeTargetState::Started,
    );
    request.checkpoint_local_artifact_id = Some(checkpoint.to_string());
    let error = upgrade(&f.service, request).await.unwrap_err();
    assert!(
        f._node
            .calls()
            .iter()
            .any(|call| call.contains("BtrfsSnapshot")),
        "{error}: {:?}",
        f._node.calls()
    );
}

/// `stage_from` stages only a sandbox absent from the node; with a row here
/// it is ignored, and the upgrade goes on to the row the node holds.
#[tokio::test]
async fn staging_leaves_a_present_sandbox_to_the_upgrade() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);
    let mut request = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation(),
        WakeTargetState::Stopped,
    );
    request.stage_from = Some(crate::proto::RuntimeUpgradeStageFrom {
        suspend_restore_storage_uri: "s3://space-test/suspend-snapshots/current?region=us-east-1"
            .into(),
        suspend_restore_source_node: Some("worker-source".into()),
    });
    let unnamed = request.clone();
    request.checkpoint_local_artifact_id = Some(LocalArtifactId::new().to_string());

    // Staging names the checkpoint it stages; refused before any transfer.
    let refused = upgrade(&f.service, unnamed).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument, "{refused}");
    assert!(
        refused
            .message()
            .contains("requires checkpoint_local_artifact_id"),
        "{refused}"
    );

    let response = upgrade(&f.service, request).await.unwrap();

    assert_eq!(
        response.outcome,
        RuntimeUpgradeOutcome::AlreadyOnTarget as i32
    );
    f.assert_no_node_calls();

    let mut malformed = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation(),
        WakeTargetState::Stopped,
    );
    malformed.checkpoint_local_artifact_id = Some(LocalArtifactId::new().to_string());
    malformed.stage_from = Some(crate::proto::RuntimeUpgradeStageFrom {
        suspend_restore_storage_uri: " ".into(),
        suspend_restore_source_node: None,
    });
    let refused = upgrade(&f.service, malformed).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument, "{refused}");
}

/// A Kubernetes sandbox launches in its pod's shim, which never sees a Wake,
/// so a Wake carrying a runtime upgrade with a Kubernetes route identity is
/// refused before anything is staged, fenced or booted, with or without a
/// namespace. A Direct identity keeps its Wake path.
#[tokio::test]
async fn a_kubernetes_wake_refuses_a_runtime_upgrade() {
    for netns_path in [None, Some(POD_NETNS.to_string())] {
        let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
        let target = certified_target(&f.source);
        let request = crate::proto::WakeRequest {
            sandbox_id: f.sandbox_id.clone(),
            blocking: true,
            start_context: Some(crate::proto::WakeStartContext {
                route_identity: Some(pod_identity(&f.sandbox_id, POD_UID, 4)),
                netns_path,
                ..Default::default()
            }),
            runtime_upgrade: Some(crate::proto::WakeRuntimeUpgrade {
                operation_id: operation().to_string(),
                target_environment: target.environment().unwrap().to_string(),
                authorization: Some(authorization()),
                checkpoint_local_artifact_id: None,
            }),
            ..Default::default()
        };

        let error = f.service.wake(internal_request(request)).await.unwrap_err();

        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
        assert!(error.message().contains("never from a Wake"), "{error}");
        assert_eq!(f.row().state, SandboxStatus::from(SandboxState::Stopped));
        f.assert_no_node_calls();
        assert_eq!(
            crate::db::read::runtime_upgrade_fence(&f.service.db.lock().unwrap(), &f.sandbox_id)
                .unwrap(),
            None
        );
    }
}

/// The sandbox's own row decides too: a Wake that names a Direct identity
/// for a sandbox whose row records a Kubernetes pod is still refused.
#[tokio::test]
async fn a_wake_for_a_kubernetes_row_refuses_a_runtime_upgrade_whatever_it_claims() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    run_as(&f, &pod_identity(&f.sandbox_id, POD_UID, 4));
    f.service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
            crate::db::fixture::params![f.sandbox_id],
        )
        .unwrap();
    let mut direct = pod_identity(&f.sandbox_id, POD_UID, 4);
    direct.pod_uid = None;
    direct.substrate_kind = SandboxSubstrateKind::Direct as i32;
    let request = crate::proto::WakeRequest {
        sandbox_id: f.sandbox_id.clone(),
        blocking: true,
        start_context: Some(crate::proto::WakeStartContext {
            route_identity: Some(direct),
            ..Default::default()
        }),
        runtime_upgrade: Some(crate::proto::WakeRuntimeUpgrade {
            operation_id: operation().to_string(),
            target_environment: target.environment().unwrap().to_string(),
            authorization: Some(authorization()),
            checkpoint_local_artifact_id: None,
        }),
        ..Default::default()
    };

    let error = f.service.wake(internal_request(request)).await.unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("never from a Wake"), "{error}");
    f.assert_no_node_calls();
}

/// What a shim that predates the runtime-upgrade checkpoint sends for an
/// upgrade pod: the checkpoint passed through as a `Start` checkpoint, with
/// the pod's namespace and identity. sandbox-manager refuses it for a stopped
/// row and for a suspended one, without staging or launching anything, so an
/// old shim fails closed whatever the row.
#[tokio::test]
async fn an_old_shims_runtime_upgrade_start_is_refused_on_a_stopped_suspended_or_absent_sandbox() {
    for state in [SandboxState::Stopped, SandboxState::Suspended] {
        let f = fixture(state, false, FakeNodeManager::default());
        let before = f.row();
        let error = f
            .service
            .start_with_runtime_upgrade(
                internal_request(StartRequest {
                    sandbox_id: f.sandbox_id.clone(),
                    checkpoint: Some("runtime-upgrade".to_string()),
                    netns_path: Some(POD_NETNS.to_string()),
                    suspend_restore_storage_uri: Some(
                        "s3://space-test/suspend-snapshots/current?region=us-east-1".into(),
                    ),
                    route_identity: Some(pod_identity(&f.sandbox_id, POD_UID, 4)),
                    ..Default::default()
                }),
                crate::service::start::StartAdmission::default(),
            )
            .await
            .expect_err("an unknown checkpoint must not start or restore the sandbox");

        assert_eq!(
            error.code(),
            tonic::Code::FailedPrecondition,
            "{state}: {error}"
        );
        assert!(
            error.message().contains("runtime-upgrade"),
            "{state}: {error}"
        );
        let after = f.row();
        assert_eq!(after.state, SandboxStatus::from(state));
        assert_eq!(after.pid, None, "nothing may have launched");
        assert_eq!(
            after.current_local_artifact_id,
            before.current_local_artifact_id
        );
        f.assert_no_node_calls();
    }

    // Nor is an absent sandbox materialized from it.
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let absent = SandboxId::new().to_string();
    let error = f
        .service
        .start_with_runtime_upgrade(
            internal_request(StartRequest {
                sandbox_id: absent.clone(),
                checkpoint: Some("runtime-upgrade".to_string()),
                netns_path: Some(POD_NETNS.to_string()),
                suspend_restore_storage_uri: Some(
                    "s3://space-test/suspend-snapshots/current?region=us-east-1".into(),
                ),
                route_identity: Some(pod_identity(&absent, POD_UID, 4)),
                ..Default::default()
            }),
            crate::service::start::StartAdmission::default(),
        )
        .await
        .expect_err("an unknown checkpoint must not materialize a sandbox");
    assert_eq!(error.code(), tonic::Code::NotFound, "{error}");
    assert!(
        crate::db::get_sandbox(&f.service.db.lock().unwrap(), &absent)
            .unwrap()
            .is_none()
    );
    f.assert_no_node_calls();
}

/// A stage interrupted after it adopted the row leaves it `restoring`. A
/// retry with `stage_from` resumes that stage instead of handing the upgrade
/// a row it refuses. This harness has no object store, so the resumed stage
/// is what fails.
#[tokio::test]
async fn an_interrupted_stage_is_resumed_before_the_upgrade() {
    let f = fixture(SandboxState::Restoring, false, FakeNodeManager::default());
    {
        // A remote stage that adopted the row and recorded its metadata
        // staging, then stopped.
        let db = f.service.db.lock().unwrap();
        db.execute(
            "UPDATE sandboxes
                SET restore_origin = 'remote',
                    restore_phase = 'metadata_staging',
                    restore_storage_uri = ?2
              WHERE id = ?1",
            crate::db::fixture::params![
                f.sandbox_id,
                format!("s3://space-test/suspend/{}?region=us-east-1", f.sandbox_id)
            ],
        )
        .unwrap();
        assert!(crate::db::record_remote_restore_artifact_staging(&db, &f.sandbox_id).unwrap());
    }
    let operation_id = operation();
    let mut request = pod_upgrade(
        &f.sandbox_id,
        &certified_target(&f.source),
        operation_id,
        WakeTargetState::Started,
    );
    request.checkpoint_local_artifact_id = Some(LocalArtifactId::new().to_string());
    request.stage_from = Some(crate::proto::RuntimeUpgradeStageFrom {
        suspend_restore_storage_uri: format!(
            "s3://space-test/suspend/{}/{}?region=us-east-1",
            f.sandbox_id,
            uuid::Uuid::now_v7()
        ),
        suspend_restore_source_node: None,
    });

    let error = upgrade(&f.service, request).await.unwrap_err();

    assert!(
        error
            .message()
            .contains("resume interrupted cross-node restore staging"),
        "{error}"
    );
    assert_eq!(f.row().state, SandboxStatus::from(SandboxState::Restoring));
    assert_eq!(
        crate::db::read::runtime_upgrade_fence(&f.service.db.lock().unwrap(), &f.sandbox_id)
            .unwrap(),
        None
    );
    // A stage that failed rather than refused the start is a retry: the pod
    // that sent it gets nothing recorded.
    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    assert_eq!(
        report(&f.service, &f.sandbox_id, operation_id).status,
        RuntimeUpgradeStatus::Absent as i32
    );
}

/// Staging's refusals of a start this node can't stage, `FAILED_PRECONDITION`
/// and `INVALID_ARGUMENT`, are final for the attempt; any other code, busy or
/// failing, is retried.
#[test]
fn staging_refusals_are_final_only_for_a_start_this_node_cant_stage() {
    for (code, last) in [
        (tonic::Code::FailedPrecondition, true),
        (tonic::Code::InvalidArgument, true),
        (tonic::Code::Unavailable, false),
        (tonic::Code::Internal, false),
        (tonic::Code::Aborted, false),
    ] {
        let refusal = super::staging_refusal(Status::new(code, "staging"));
        assert_eq!(
            matches!(refusal, super::super::Refusal::Final(_)),
            last,
            "{code:?}"
        );
    }
}

/// A staging refusal no retry changes is final for its attempt, as any
/// refusal before the fence is. A stage interrupted after it adopted the row
/// is resumed, and refused here because the start names another node.
#[tokio::test]
async fn a_final_staging_refusal_is_recorded_for_its_attempt() {
    let f = fixture(SandboxState::Restoring, false, FakeNodeManager::default());
    {
        let db = f.service.db.lock().unwrap();
        db.execute(
            "UPDATE sandboxes
                SET restore_origin = 'remote',
                    restore_phase = 'metadata_staging',
                    restore_storage_uri = ?2
              WHERE id = ?1",
            crate::db::fixture::params![
                f.sandbox_id,
                format!("s3://space-test/suspend/{}?region=us-east-1", f.sandbox_id)
            ],
        )
        .unwrap();
        assert!(crate::db::record_remote_restore_artifact_staging(&db, &f.sandbox_id).unwrap());
    }
    let operation_id = operation();
    let request = || {
        let mut request = pod_upgrade(
            &f.sandbox_id,
            &certified_target(&f.source),
            operation_id,
            WakeTargetState::Started,
        );
        request.checkpoint_local_artifact_id = Some(LocalArtifactId::new().to_string());
        request.stage_from = Some(crate::proto::RuntimeUpgradeStageFrom {
            suspend_restore_storage_uri: format!(
                "s3://space-test/suspend/{}/{}?region=us-east-1",
                f.sandbox_id,
                uuid::Uuid::now_v7()
            ),
            suspend_restore_source_node: None,
        });
        request
            .start
            .as_mut()
            .unwrap()
            .route_identity
            .as_mut()
            .unwrap()
            .current_node_id = Some(uuid::Uuid::now_v7().to_string());
        request
    };

    let refused = upgrade(&f.service, request()).await.unwrap_err();

    assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
    assert!(refused.message().contains("targets node"), "{refused}");
    let reported = report(&f.service, &f.sandbox_id, operation_id);
    assert_eq!(reported.status, RuntimeUpgradeStatus::Refused as i32);
    assert_eq!(reported.attempt_id.as_deref(), Some(POD_UID));
    let retried = upgrade(&f.service, request()).await.unwrap_err();
    assert!(
        retried.message().contains("was refused for attempt"),
        "{retried}"
    );
}

/// The node's report of `operation_id` for the attempt `attempt_id`.
fn report_for_attempt(
    service: &SandboxManagerService,
    sandbox_id: &str,
    operation_id: RuntimeUpgradeOperationId,
    attempt_id: &str,
) -> crate::proto::GetRuntimeUpgradeResponse {
    service
        .get_runtime_upgrade(&internal_request(GetRuntimeUpgradeRequest {
            sandbox_id: sandbox_id.to_string(),
            operation_id: operation_id.to_string(),
            attempt_id: Some(attempt_id.to_string()),
        }))
        .unwrap()
}

/// A caller that names its pod gets that pod's own answer, whatever a later
/// pod recorded since: its own refused create, never another pod's, and its
/// own refusal before the fence, or `ABSENT` in place of another pod's.
#[tokio::test]
async fn a_report_for_a_named_attempt_is_that_attempts_own() {
    const LATER_POD_UID: &str = "2c8f5a1d-7b3e-4f6a-9d0c-1e2b3a4c5d60";
    const UNSEEN_POD_UID: &str = "6e1d2c3b-4a5f-4e6d-8c7b-9a0f1e2d3c70";

    // Refused creates after the operation upgraded the sandbox.
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let operation_id = operation();
    retire(
        &f,
        &target,
        operation_id,
        crate::db::RuntimeUpgradeTargetState::Started,
    );
    {
        let db = f.service.db.lock().unwrap();
        for (pod_uid, failure) in [
            (POD_UID, "the first create failed"),
            (LATER_POD_UID, "the later create failed"),
        ] {
            assert!(
                crate::db::record_runtime_upgrade_refused_attempt(
                    &db,
                    operation_id,
                    &f.sandbox_id,
                    pod_uid,
                    failure
                )
                .unwrap()
            );
        }
    }
    let latest = report(&f.service, &f.sandbox_id, operation_id);
    assert_eq!(latest.refused_attempt_id.as_deref(), Some(LATER_POD_UID));
    let own = report_for_attempt(&f.service, &f.sandbox_id, operation_id, POD_UID);
    assert_eq!(own.status, RuntimeUpgradeStatus::Upgraded as i32);
    assert_eq!(own.refused_attempt_id.as_deref(), Some(POD_UID));
    assert_eq!(
        own.refused_attempt_failure.as_deref(),
        Some("the first create failed")
    );
    let unseen = report_for_attempt(&f.service, &f.sandbox_id, operation_id, UNSEEN_POD_UID);
    assert_eq!(unseen.status, RuntimeUpgradeStatus::Upgraded as i32);
    assert_eq!(unseen.refused_attempt_id, None);

    // Refusals before the fence, the later pod's replacing the earlier's as
    // the operation's latest.
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let operation_id = operation();
    {
        let db = f.service.db.lock().unwrap();
        let identity = crate::db::RuntimeUpgradeRequestIdentity {
            operation_id,
            sandbox_id: f.sandbox_id.clone(),
            target_environment: target.environment().unwrap(),
            target_state: crate::db::RuntimeUpgradeTargetState::Started,
            authorization_id: authorization().authorization_id,
        };
        for (pod_uid, failure) in [
            (POD_UID, "the first refusal"),
            (LATER_POD_UID, "the later refusal"),
        ] {
            assert!(
                crate::db::record_runtime_upgrade_refusal(&db, &identity, pod_uid, failure)
                    .unwrap()
            );
        }
    }
    let latest = report(&f.service, &f.sandbox_id, operation_id);
    assert_eq!(latest.status, RuntimeUpgradeStatus::Refused as i32);
    assert_eq!(latest.failure.as_deref(), Some("the later refusal"));
    let own = report_for_attempt(&f.service, &f.sandbox_id, operation_id, POD_UID);
    assert_eq!(own.status, RuntimeUpgradeStatus::Refused as i32);
    assert_eq!(own.failure.as_deref(), Some("the first refusal"));
    assert_eq!(own.attempt_id.as_deref(), Some(POD_UID));
    let unseen = report_for_attempt(&f.service, &f.sandbox_id, operation_id, UNSEEN_POD_UID);
    assert_eq!(unseen.status, RuntimeUpgradeStatus::Absent as i32);
    assert_eq!(unseen.attempt_id, None);
    assert_eq!(unseen.failure, None);

    // A pod's rollback still on its live row, its disk discard not yet
    // finished, so its history isn't written: that pod is told, another
    // isn't.
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let operation_id = operation();
    {
        let db = f.service.db.lock().unwrap();
        let sandbox = crate::db::read::sandbox(&db, &f.sandbox_id)
            .unwrap()
            .unwrap();
        let begun = crate::db::begin_runtime_upgrade(
            &db,
            crate::db::NewRuntimeUpgrade {
                operation_id,
                sandbox_id: &f.sandbox_id,
                observed: crate::db::ObservedSandbox::of(&sandbox),
                target_environment: target.environment().unwrap(),
                target_state: crate::db::RuntimeUpgradeTargetState::Started,
                authorization: &crate::db::RuntimeUpgradeAuthorization {
                    id: "environment-block-e0".into(),
                    reason: "e0 runs a blocked runtime".into(),
                    memory_loss: true,
                },
                checkpoint_dir_identity: None,
                previous_execution: &f.source,
                attempt_id: POD_UID,
            },
        )
        .unwrap();
        assert!(
            matches!(begun, crate::db::BeginRuntimeUpgrade::Fenced(_)),
            "{begun:?}"
        );
        db.execute(
            "UPDATE sandbox_runtime_upgrades
                SET phase = 'rolled_back', failure = 'the boot never became ready'
              WHERE operation_id = ?1",
            crate::db::fixture::params![operation_id.to_string()],
        )
        .unwrap();
    }
    let own = report_for_attempt(&f.service, &f.sandbox_id, operation_id, POD_UID);
    assert_eq!(own.status, RuntimeUpgradeStatus::RolledBack as i32);
    assert_eq!(own.failure.as_deref(), Some("the boot never became ready"));
    assert_eq!(own.attempt_id.as_deref(), Some(POD_UID));
    let unseen = report_for_attempt(&f.service, &f.sandbox_id, operation_id, UNSEEN_POD_UID);
    assert_eq!(unseen.status, RuntimeUpgradeStatus::Absent as i32);
}

/// An operation that claimed the start of a sandbox it found on its target
/// never starts it again: a retry finds it stopped since and is refused, and
/// the pod whose execution still runs gets that execution back.
#[tokio::test]
async fn an_activated_upgrade_never_starts_its_sandbox_again() {
    for running in [false, true] {
        let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
        let target = certified_target(&f.source);
        f.put_on(&target);
        let operation_id = operation();
        {
            let db = f.service.db.lock().unwrap();
            assert!(
                crate::db::record_runtime_upgrade_already_on_target(
                    &db,
                    &crate::db::RuntimeUpgradeRequestIdentity {
                        operation_id,
                        sandbox_id: f.sandbox_id.clone(),
                        target_environment: target.environment().unwrap(),
                        target_state: crate::db::RuntimeUpgradeTargetState::Started,
                        authorization_id: authorization().authorization_id,
                    },
                    &crate::db::new_runtime_upgrade_attempt_id(),
                )
                .unwrap()
            );
            assert!(
                crate::db::claim_runtime_upgrade_activation(
                    &db,
                    operation_id,
                    &f.sandbox_id,
                    // Another execution than the row's: something started
                    // the sandbox since the claim.
                    crate::db::RuntimeUpgradeActivationClaim::Start {
                        from_execution: Some(
                            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
                        ),
                    },
                )
                .unwrap()
            );
        }
        if running {
            run_as(&f, &pod_identity(&f.sandbox_id, POD_UID, 4));
        }

        let outcome = upgrade(
            &f.live_service(),
            pod_upgrade(
                &f.sandbox_id,
                &target,
                operation_id,
                WakeTargetState::Started,
            ),
        )
        .await;

        if running {
            assert_eq!(
                outcome.unwrap().start.map(|start| start.pid),
                Some(std::process::id())
            );
        } else {
            let error = outcome.unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
            assert!(error.message().contains("already activated"), "{error}");
            assert_eq!(f.row().state, SandboxStatus::from(SandboxState::Stopped));
        }
        f.assert_no_node_calls();
    }
}

/// A Wake replaying an upgrade that already retired must still name the
/// checkpoint the node holds: the replay itself moves nothing, and the Wake
/// goes on to restore whatever the row pins. A Direct sandbox that suspended
/// again since pins a newer checkpoint, and a replay naming the older one is
/// refused before anything is restored.
#[tokio::test]
async fn a_wake_replay_of_a_retired_upgrade_never_restores_a_newer_checkpoint() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let operation_id = operation();
    retire(
        &f,
        &target,
        operation_id,
        crate::db::RuntimeUpgradeTargetState::Started,
    );
    // The sandbox suspended again since, to a checkpoint the node now pins.
    f.adopt_checkpoint();
    let mut direct = pod_identity(&f.sandbox_id, POD_UID, 4);
    direct.pod_uid = None;
    direct.substrate_kind = SandboxSubstrateKind::Direct as i32;
    let request = crate::proto::WakeRequest {
        sandbox_id: f.sandbox_id.clone(),
        blocking: true,
        start_context: Some(crate::proto::WakeStartContext {
            route_identity: Some(direct),
            ..Default::default()
        }),
        runtime_upgrade: Some(crate::proto::WakeRuntimeUpgrade {
            operation_id: operation_id.to_string(),
            target_environment: target.environment().unwrap().to_string(),
            authorization: Some(authorization()),
            checkpoint_local_artifact_id: Some(LocalArtifactId::new().to_string()),
        }),
        ..Default::default()
    };

    let error = f.service.wake(internal_request(request)).await.unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("decided from"), "{error}");
    assert_eq!(f.row().state, SandboxStatus::from(SandboxState::Suspended));
    f.assert_no_node_calls();
}

/// A retry under an operation the node recorded an outcome for must ask for
/// what the operation asked for: one that found its sandbox on the target for
/// a stopped lifecycle can't come back as a start of it.
#[tokio::test]
async fn a_recorded_operation_cant_be_retried_as_another() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);
    let operation_id = operation();
    let stopped = upgrade(
        &f.service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Stopped,
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        stopped.outcome,
        RuntimeUpgradeOutcome::AlreadyOnTarget as i32
    );

    let started = upgrade(
        &f.service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Started,
        ),
    )
    .await
    .unwrap_err();

    assert_eq!(started.code(), tonic::Code::InvalidArgument, "{started}");
    assert_eq!(f.row().state, SandboxStatus::from(SandboxState::Stopped));
    f.assert_no_node_calls();
}

/// A replay that restores nothing answers whatever checkpoint the sandbox
/// holds since: a stopped-target upgrade that retired is still upgraded after
/// the sandbox suspended to a newer checkpoint.
#[tokio::test]
async fn a_stopped_target_replay_ignores_a_newer_checkpoint() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let operation_id = operation();
    retire(
        &f,
        &target,
        operation_id,
        crate::db::RuntimeUpgradeTargetState::Stopped,
    );
    // The sandbox suspended again since, to a checkpoint the node now pins,
    // newer than the one the upgrade was decided from.
    f.adopt_checkpoint();
    let mut request = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation_id,
        WakeTargetState::Stopped,
    );
    request.checkpoint_local_artifact_id = Some(LocalArtifactId::new().to_string());

    let response = upgrade(&f.service, request).await.unwrap();

    assert_eq!(response.outcome, RuntimeUpgradeOutcome::Upgraded as i32);
    assert_eq!(response.start, None);
    assert_eq!(f.row().state, SandboxStatus::from(SandboxState::Suspended));
    f.assert_no_node_calls();
}

/// A Direct Wake that replays a retired upgrade from the checkpoint the node
/// pins goes on to restore that checkpoint, as a Wake without the upgrade
/// would. CI builds these tests unoptimized, and the replay and the restore
/// together must still fit the test thread's stack. This harness has no
/// template-manager, so the checkpoint launch's template lookup is what
/// fails.
#[tokio::test]
async fn a_wake_replay_of_a_retired_upgrade_goes_on_to_restore() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let operation_id = operation();
    let pin = f.adopt_checkpoint();
    f.materialize_local_snapshot();
    retire(
        &f,
        &target,
        operation_id,
        crate::db::RuntimeUpgradeTargetState::Started,
    );
    let mut direct = pod_identity(&f.sandbox_id, POD_UID, 4);
    direct.pod_uid = None;
    direct.substrate_kind = SandboxSubstrateKind::Direct as i32;
    let request = crate::proto::WakeRequest {
        sandbox_id: f.sandbox_id.clone(),
        blocking: true,
        start_context: Some(crate::proto::WakeStartContext {
            route_identity: Some(direct),
            ..Default::default()
        }),
        runtime_upgrade: Some(crate::proto::WakeRuntimeUpgrade {
            operation_id: operation_id.to_string(),
            target_environment: target.environment().unwrap().to_string(),
            authorization: Some(authorization()),
            checkpoint_local_artifact_id: Some(pin.to_string()),
        }),
        ..Default::default()
    };

    let error = f.service.wake(internal_request(request)).await.unwrap_err();

    assert!(
        error.message().contains("GetTemplate"),
        "the Wake must reach the checkpoint launch: {error}"
    );
}

/// An operation id names one upgrade of one sandbox. A staged request that
/// reuses one another sandbox's upgrade recorded is refused before anything
/// is staged for the sandbox it names.
#[tokio::test]
async fn a_reused_operation_id_stages_nothing() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);
    let operation_id = operation();
    upgrade(
        &f.service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Stopped,
        ),
    )
    .await
    .unwrap();

    let absent = SandboxId::new().to_string();
    let mut request = pod_upgrade(&absent, &target, operation_id, WakeTargetState::Started);
    request.checkpoint_local_artifact_id = Some(LocalArtifactId::new().to_string());
    request.stage_from = Some(crate::proto::RuntimeUpgradeStageFrom {
        suspend_restore_storage_uri: format!(
            "s3://space-test/suspend/{absent}/{}?region=us-east-1",
            uuid::Uuid::now_v7()
        ),
        suspend_restore_source_node: None,
    });

    let error = upgrade(&f.service, request).await.unwrap_err();

    assert_eq!(error.code(), tonic::Code::InvalidArgument, "{error}");
    assert!(
        f.service.lookup_sandbox(&absent).is_err(),
        "nothing may have been staged"
    );
}

/// A stopped-target upgrade that finds the sandbox running on its target is
/// refused rather than answered as stopped: the pod would otherwise stand in
/// for a stopped sandbox while a microVM runs. The operation is recorded as
/// already on target, and nothing is recorded for the attempt: a stopped
/// target never sets `refused_attempt_id`.
#[tokio::test]
async fn a_stopped_target_never_answers_for_a_running_sandbox() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);
    run_as(&f, &pod_identity(&f.sandbox_id, OTHER_POD_UID, 4));
    let operation_id = operation();

    let error = upgrade(
        &f.live_service(),
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Stopped,
        ),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("not stopped"), "{error}");
    assert_eq!(f.row().state, SandboxStatus::from(SandboxState::Started));
    f.assert_no_node_calls();
    let reported = report(&f.service, &f.sandbox_id, operation_id);
    assert_eq!(
        reported.status,
        RuntimeUpgradeStatus::AlreadyOnTarget as i32
    );
    assert_eq!(reported.refused_attempt_id, None);
}

/// A replay of a retired stopped-target upgrade answers only for a sandbox at
/// rest: one started since is refused, not reported as stopped.
#[tokio::test]
async fn a_stopped_target_replay_never_answers_for_a_running_sandbox() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let operation_id = operation();
    retire(
        &f,
        &target,
        operation_id,
        crate::db::RuntimeUpgradeTargetState::Stopped,
    );
    run_as(&f, &pod_identity(&f.sandbox_id, OTHER_POD_UID, 4));

    let error = upgrade(
        &f.live_service(),
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Stopped,
        ),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("not stopped"), "{error}");
    assert_eq!(f.row().state, SandboxStatus::from(SandboxState::Started));
    f.assert_no_node_calls();
}

/// Staging runs a restore with the start's namespace and identity, so it
/// takes the authority a `Start` carrying them does. A local operator may
/// upgrade, but may not stage.
#[tokio::test]
async fn staging_takes_a_starts_authority() {
    use pplx_space_uds_peer_auth::{LocalRole, UdsPeerAccount, UdsPeerAuthorizer};
    let (source, _) = environments();
    let target = certified_target(&source);
    let mut service = SandboxManagerService::new_with_options(
        crate::db::test_conn(),
        crate::service::SandboxManagerServiceOptions::for_test(),
    );
    service.peer_authorizer = UdsPeerAuthorizer::new(
        "runtime-upgrade-test",
        [UdsPeerAccount::role_with_fallback_uid(
            "runtime-upgrade-test-operator",
            LocalRole::Operator,
            16001,
        )],
    );
    let sandbox_id = SandboxId::new().to_string();
    let mut request = pod_upgrade(&sandbox_id, &target, operation(), WakeTargetState::Started);
    request.checkpoint_local_artifact_id = Some(LocalArtifactId::new().to_string());
    request.stage_from = Some(crate::proto::RuntimeUpgradeStageFrom {
        suspend_restore_storage_uri: format!(
            "s3://space-test/suspend/{sandbox_id}/{}?region=us-east-1",
            uuid::Uuid::now_v7()
        ),
        suspend_restore_source_node: None,
    });
    let mut operator = tonic::Request::new(request);
    operator
        .extensions_mut()
        .insert(pplx_space_uds_peer_auth::PeerIdentity::new(
            16001, 16001, None,
        ));

    let error = service.upgrade_runtime(operator).await.unwrap_err();

    assert_eq!(error.code(), tonic::Code::PermissionDenied, "{error}");
    assert!(
        service.lookup_sandbox(&sandbox_id).is_err(),
        "nothing may have been staged"
    );
}

/// An operation that found its sandbox on the target never boots it: replayed
/// after the sandbox moved back to another environment, it's refused before
/// anything is fenced rather than undoing that move.
#[tokio::test]
async fn an_operation_that_found_its_target_never_boots_on_a_replay() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);
    let operation_id = operation();
    upgrade(
        &f.service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Stopped,
        ),
    )
    .await
    .unwrap();
    // Another operation moved the sandbox off the target since.
    f.put_on(&f.source);

    let error = upgrade(
        &f.service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Stopped,
        ),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("on its target before"), "{error}");
    assert_eq!(
        crate::db::read::runtime_upgrade_fence(&f.service.db.lock().unwrap(), &f.sandbox_id)
            .unwrap(),
        None
    );
    f.assert_no_node_calls();
}

/// A sandbox another activation is moving is answered with a retryable
/// refusal, not a terminal one: a retry finds where it lands.
#[tokio::test]
async fn an_activation_meeting_a_moving_sandbox_is_retryable() {
    let f = fixture(SandboxState::Starting, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);

    let error = upgrade(
        &f.service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation(),
            WakeTargetState::Started,
        ),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    assert_eq!(f.row().state, SandboxStatus::from(SandboxState::Starting));
    f.assert_no_node_calls();
}

/// A stopped target is answered only for a sandbox at rest; one another
/// stop or activation is still moving, or whose pending cleanup recovery is
/// still driving, is refused as retryable, not as a sandbox that runs.
#[tokio::test]
async fn a_stopped_target_meeting_a_moving_sandbox_is_retryable() {
    for (mut state, delete_row) in [
        (SandboxState::Stopping.into(), false),
        (SandboxStatus::CLEANUP_REQUIRED, false),
        (SandboxStatus::CLEANUP_FAILED, false),
        (SandboxStatus::CLEANUP_REQUIRED, true),
        (SandboxStatus::CLEANUP_FAILED, true),
    ] {
        let f = fixture(state, false, FakeNodeManager::default());
        if delete_row {
            assert!(
                crate::db::set_cleanup_intent(
                    &f.service.db.lock().unwrap(),
                    &f.sandbox_id,
                    false,
                    crate::db::CleanupCompletion::DeleteRow,
                )
                .unwrap()
            );
            state =
                SandboxStatus::owned(crate::state::Teardown::Deleting, state.cleanup().unwrap());
        }
        let target = certified_target(&f.source);
        f.put_on(&target);

        let error = upgrade(
            &f.service,
            pod_upgrade(
                &f.sandbox_id,
                &target,
                operation(),
                WakeTargetState::Stopped,
            ),
        )
        .await
        .unwrap_err();

        assert_eq!(error.code(), tonic::Code::Unavailable, "{state}: {error}");
        assert_eq!(f.row().state, state);
        f.assert_no_node_calls();
    }
}

/// `GetRuntimeUpgrade` answers only the upgrade's own principals: an
/// operator reads an operation's report, and a peer the node doesn't know is
/// refused before anything is read.
#[tokio::test]
async fn the_runtime_upgrade_report_is_read_only_by_the_upgrades_principals() {
    use pplx_space_uds_peer_auth::{LocalRole, UdsPeerAccount, UdsPeerAuthorizer};
    let mut service = SandboxManagerService::new_with_options(
        crate::db::test_conn(),
        crate::service::SandboxManagerServiceOptions::for_test(),
    );
    service.peer_authorizer = UdsPeerAuthorizer::new(
        "runtime-upgrade-test",
        [UdsPeerAccount::role_with_fallback_uid(
            "runtime-upgrade-test-operator",
            LocalRole::Operator,
            16001,
        )],
    );
    let as_peer = |uid| {
        let mut request = tonic::Request::new(GetRuntimeUpgradeRequest {
            sandbox_id: SandboxId::new().to_string(),
            operation_id: operation().to_string(),
            attempt_id: None,
        });
        request
            .extensions_mut()
            .insert(pplx_space_uds_peer_auth::PeerIdentity::new(uid, uid, None));
        request
    };

    let read = service.get_runtime_upgrade(&as_peer(16001)).unwrap();
    assert_eq!(read.status, RuntimeUpgradeStatus::Absent as i32);
    let error = service.get_runtime_upgrade(&as_peer(16002)).unwrap_err();
    assert_eq!(error.code(), tonic::Code::PermissionDenied, "{error}");
}

/// An activation in place answers from the sandbox once its start returned,
/// never from the start's own response: a stop or suspend that finished in
/// between keeps the pod's identity on the row while the runtime is gone, and
/// the pod must not get a pid for a sandbox that no longer runs. A sandbox
/// still running the pod's execution answers with its live runtime.
#[tokio::test]
async fn an_activation_in_place_answers_only_a_sandbox_still_running_the_pods_execution() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let identity = pod_identity(&f.sandbox_id, POD_UID, 4);
    run_as(&f, &identity);
    let service = f.live_service();
    let start = StartRequest {
        sandbox_id: f.sandbox_id.clone(),
        route_identity: Some(identity),
        ..Default::default()
    };

    let answered = service
        .answer_after_activation(operation(), &start)
        .unwrap();
    assert_eq!(answered.pid, std::process::id());

    // Torn down before the answer: the row is at rest with the pod's
    // identity still on it.
    for state in ["stopped", "suspended"] {
        service
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE sandboxes SET state = ?2 WHERE id = ?1",
                crate::db::fixture::params![f.sandbox_id, state],
            )
            .unwrap();
        let refused = service
            .answer_after_activation(operation(), &start)
            .unwrap_err();
        assert_eq!(
            refused.code(),
            tonic::Code::FailedPrecondition,
            "{state}: {refused}"
        );
    }

    // Started, but its runtime is gone.
    service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET state = 'started' WHERE id = ?1",
            crate::db::fixture::params![f.sandbox_id],
        )
        .unwrap();
    let gone = f
        .service
        .answer_after_activation(operation(), &start)
        .unwrap_err();
    assert_eq!(gone.code(), tonic::Code::FailedPrecondition, "{gone}");
}

/// How a started target reached its target before refusing the pod's create.
#[derive(Clone, Copy, Debug)]
enum Reached {
    /// The node upgraded the sandbox (the operation retired).
    Upgraded,
    /// The node found the sandbox on the target already.
    AlreadyOnTarget,
}

/// A started target's operation that reached its target, upgraded or found
/// there, but refused the pod's create (here, the sandbox runs another
/// pod's execution) leaves that pod pending in its create, which kubelet
/// retries under the same pod UID. The node records the refusal for that pod
/// and reports it, so the control plane can settle on it, and the refusal is
/// final for the pod: its retry gets it back even once the sandbox would run
/// for it, while a retry from a new pod is answered afresh. A request whose
/// start names no pod records its refusal under the attempt the node
/// generated; a stopped target's records nothing.
#[tokio::test]
async fn a_started_targets_refused_activation_is_recorded_and_final_for_its_attempt() {
    const NEW_POD_UID: &str = "1b7e4c2a-3f5d-4e6a-9c8b-7d6e5f4a3b20";
    for reached in [Reached::Upgraded, Reached::AlreadyOnTarget] {
        let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
        let target = certified_target(&f.source);
        let operation_id = operation();
        let expected_status = match reached {
            Reached::Upgraded => {
                retire(
                    &f,
                    &target,
                    operation_id,
                    crate::db::RuntimeUpgradeTargetState::Started,
                );
                RuntimeUpgradeStatus::Upgraded
            }
            Reached::AlreadyOnTarget => {
                f.put_on(&target);
                RuntimeUpgradeStatus::AlreadyOnTarget
            }
        };
        run_as(&f, &pod_identity(&f.sandbox_id, OTHER_POD_UID, 4));
        let service = f.live_service();
        let request = |pod_uid: &str| {
            let mut request = pod_upgrade(
                &f.sandbox_id,
                &target,
                operation_id,
                WakeTargetState::Started,
            );
            request.start.as_mut().unwrap().route_identity =
                Some(pod_identity(&f.sandbox_id, pod_uid, 4));
            request
        };

        let refused = upgrade(&service, request(POD_UID)).await.unwrap_err();

        assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
        let reported = report(&f.service, &f.sandbox_id, operation_id);
        assert_eq!(reported.status, expected_status as i32, "{reached:?}");
        assert_eq!(reported.refused_attempt_id.as_deref(), Some(POD_UID));
        assert_eq!(
            reported.refused_attempt_failure.as_deref(),
            Some(refused.message()),
            "{reached:?}"
        );

        // Final for that pod: once the sandbox runs its execution, a fresh
        // answer would hand it over, but the retry gets the refusal back.
        run_as(&f, &pod_identity(&f.sandbox_id, POD_UID, 4));
        let retried = upgrade(&service, request(POD_UID)).await.unwrap_err();
        assert_eq!(retried.code(), tonic::Code::FailedPrecondition, "{retried}");
        assert!(
            retried.message().contains("only a new attempt retries it")
                && retried.message().contains(refused.message()),
            "{reached:?}: {retried}"
        );
        assert_eq!(
            f.row().state,
            crate::state::SandboxStatus::from(SandboxState::Started)
        );

        // A new pod is answered afresh, and its refusal is the one reported.
        let fresh = upgrade(&service, request(NEW_POD_UID)).await.unwrap_err();
        assert!(
            !fresh.message().contains("only a new attempt retries it"),
            "{reached:?}: {fresh}"
        );
        let reported = report(&f.service, &f.sandbox_id, operation_id);
        assert_eq!(reported.refused_attempt_id.as_deref(), Some(NEW_POD_UID));
        f.assert_no_node_calls();
    }

    // A started target's request whose start names no pod still runs as an
    // attempt, under an id the node generates, and its refused activation is
    // recorded for that attempt: no pod holds a VM for it. A stopped target's
    // records no refused activation.
    for (pod_uid, target_state, recorded) in [
        (None, WakeTargetState::Started, true),
        (Some(POD_UID), WakeTargetState::Stopped, false),
    ] {
        let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
        let target = certified_target(&f.source);
        f.put_on(&target);
        run_as(&f, &pod_identity(&f.sandbox_id, OTHER_POD_UID, 4));
        let operation_id = operation();
        let mut request = pod_upgrade(&f.sandbox_id, &target, operation_id, target_state);
        let identity = request
            .start
            .as_mut()
            .unwrap()
            .route_identity
            .as_mut()
            .unwrap();
        if pod_uid.is_none() {
            identity.pod_uid = None;
            identity.substrate_kind = SandboxSubstrateKind::Direct as i32;
        }

        // Its runtime isn't live, so not even a caller that names no pod
        // gets it.
        upgrade(&f.service, request).await.unwrap_err();

        let reported = report(&f.service, &f.sandbox_id, operation_id);
        assert_eq!(
            reported.status,
            RuntimeUpgradeStatus::AlreadyOnTarget as i32
        );
        assert_eq!(
            reported.refused_attempt_id.is_some(),
            recorded,
            "{target_state:?}"
        );
        assert_ne!(
            reported.refused_attempt_id.as_deref(),
            Some(POD_UID),
            "{target_state:?}"
        );
    }
}

/// A started-target request that reuses an operation id with another target
/// or authorization records no refused create for its pod, even once the
/// operation reached its target: it isn't one of the operation's pods, and a
/// recorded refusal would refuse that pod's own request and be reported as
/// the operation's.
#[tokio::test]
async fn a_request_naming_another_operation_records_no_refused_create() {
    for reached in [Reached::Upgraded, Reached::AlreadyOnTarget] {
        let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
        let target = certified_target(&f.source);
        let operation_id = operation();
        match reached {
            Reached::Upgraded => retire(
                &f,
                &target,
                operation_id,
                crate::db::RuntimeUpgradeTargetState::Started,
            ),
            Reached::AlreadyOnTarget => {
                f.put_on(&target);
                upgrade(
                    &f.service,
                    pod_upgrade(
                        &f.sandbox_id,
                        &target,
                        operation_id,
                        WakeTargetState::Stopped,
                    ),
                )
                .await
                .unwrap();
            }
        }
        let mut another = pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Started,
        );
        another.authorization.as_mut().unwrap().authorization_id = "another-authorization".into();

        let refused = upgrade(&f.service, another).await.unwrap_err();

        assert_eq!(
            refused.code(),
            tonic::Code::InvalidArgument,
            "{reached:?}: {refused}"
        );
        let reported = report(&f.service, &f.sandbox_id, operation_id);
        assert_eq!(reported.refused_attempt_id, None, "{reached:?}");
        assert!(
            crate::db::read::runtime_upgrade_refused_attempt(
                &f.service.db.lock().unwrap(),
                operation_id,
                POD_UID
            )
            .unwrap()
            .is_none(),
            "{reached:?}"
        );
    }
}

/// A sandbox that can't be read may hold the pod's VM, so a refused create
/// is recorded only once it can: reading it fails here (its table is hidden),
/// and the same refusal of the same stopped sandbox records once it's back.
#[tokio::test]
async fn an_unreadable_sandbox_records_no_refused_create() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let operation_id = operation();
    retire(
        &f,
        &target,
        operation_id,
        crate::db::RuntimeUpgradeTargetState::Started,
    );
    let start = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation_id,
        WakeTargetState::Started,
    )
    .start
    .unwrap();
    let refusal = Status::unavailable("the activation failed");
    let refused_pod = || {
        crate::db::read::runtime_upgrade_refused_attempt(
            &f.service.db.lock().unwrap(),
            operation_id,
            POD_UID,
        )
        .unwrap()
    };
    let rename = |from: &str, to: &str| {
        f.service
            .db
            .lock()
            .unwrap()
            .execute_batch(&format!("ALTER TABLE {from} RENAME TO {to}"))
            .unwrap();
    };

    rename("sandboxes", "sandboxes_hidden");
    f.service
        .record_refused_attempt(operation_id, POD_UID, &start, &refusal);
    rename("sandboxes_hidden", "sandboxes");
    assert!(
        refused_pod().is_none(),
        "an unreadable sandbox recorded a refusal"
    );

    f.service
        .record_refused_attempt(operation_id, POD_UID, &start, &refusal);
    assert!(
        refused_pod().is_some(),
        "a stopped sandbox records the refusal"
    );
}

/// An error for a pod whose execution the sandbox does run (a start that
/// failed after its guest was ready, say), in any state that may still hold
/// its VM, records no refusal: a caller that settled on one would delete the
/// pod and the VM with it. So does one for a sandbox an activation is
/// creating, starting or restoring, or a cleanup owns, whichever pod its
/// route identity names: an activation writes that identity only once it
/// runs. Only a sandbox at rest, or one running another pod's execution,
/// records it.
#[tokio::test]
async fn an_error_for_the_pod_the_sandbox_runs_records_no_refusal() {
    for (runner, state, cleanup_owned, recorded) in [
        (POD_UID, "started", false, false),
        (POD_UID, "paused", false, false),
        (POD_UID, "pausing", false, false),
        (POD_UID, "suspending", false, false),
        (POD_UID, "checkpointing", false, false),
        (POD_UID, "creating", false, false),
        (POD_UID, "starting", false, false),
        (POD_UID, "restoring", false, false),
        (POD_UID, "deleting", true, false),
        (POD_UID, "stopped", false, true),
        (POD_UID, "suspended", false, true),
        (OTHER_POD_UID, "started", false, true),
        (OTHER_POD_UID, "paused", false, true),
        (OTHER_POD_UID, "suspending", false, true),
        (OTHER_POD_UID, "creating", false, false),
        (OTHER_POD_UID, "starting", false, false),
        (OTHER_POD_UID, "restoring", false, false),
        (OTHER_POD_UID, "deleting", true, false),
        (OTHER_POD_UID, "quarantined", false, false),
        (OTHER_POD_UID, "failed", false, false),
        (OTHER_POD_UID, "stopped", false, true),
        (OTHER_POD_UID, "suspended", false, true),
    ] {
        let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
        let target = certified_target(&f.source);
        let operation_id = operation();
        retire(
            &f,
            &target,
            operation_id,
            crate::db::RuntimeUpgradeTargetState::Started,
        );
        run_as(&f, &pod_identity(&f.sandbox_id, runner, 4));
        f.service
            .db
            .lock()
            .unwrap()
            .execute(
                if cleanup_owned {
                    // A cleanup that deletes the row owns it in `deleting`.
                    "UPDATE sandboxes
                        SET state = ?2, cleanup_state = 'required',
                            cleanup_completion = 'delete_row', cleanup_keep_filesystem = 0
                      WHERE id = ?1"
                } else {
                    "UPDATE sandboxes SET state = ?2 WHERE id = ?1"
                },
                crate::db::fixture::params![f.sandbox_id, state],
            )
            .unwrap();
        assert_eq!(f.row().state.is_cleanup_owned(), cleanup_owned);
        let request = pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Started,
        );

        f.service.record_refused_attempt(
            operation_id,
            POD_UID,
            request.start.as_ref().unwrap(),
            &Status::internal("the gateway push failed after readiness"),
        );

        let reported = report(&f.service, &f.sandbox_id, operation_id);
        assert_eq!(
            reported.refused_attempt_id.is_some(),
            recorded,
            "{runner} {state} cleanup-owned={cleanup_owned}: {reported:?}"
        );
    }

    // The pod's own VM from an earlier execution generation is still in the
    // pod: deleting the pod would take it, so nothing is recorded.
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let operation_id = operation();
    retire(
        &f,
        &target,
        operation_id,
        crate::db::RuntimeUpgradeTargetState::Started,
    );
    run_as(&f, &pod_identity(&f.sandbox_id, POD_UID, 3));
    let request = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation_id,
        WakeTargetState::Started,
    );
    f.service.record_refused_attempt(
        operation_id,
        POD_UID,
        request.start.as_ref().unwrap(),
        &Status::internal("the gateway push failed after readiness"),
    );
    assert_eq!(
        report(&f.service, &f.sandbox_id, operation_id).refused_attempt_id,
        None,
        "the pod's earlier-generation VM is still in the pod"
    );
}

/// A delayed retry from a pod whose rollback is final, once a later pod's
/// attempt reached the target and that pod's create was refused, gets its own
/// rollback back and records nothing: recording it as a refused create would
/// replace the later pod, which a caller waits on, as the latest refused pod.
#[tokio::test]
async fn a_final_attempts_late_retry_never_replaces_the_latest_refused_attempt() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let operation_id = operation();
    retire(
        &f,
        &target,
        operation_id,
        crate::db::RuntimeUpgradeTargetState::Started,
    );
    f.service
        .db
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO sandbox_runtime_upgrade_attempt_outcomes
                 (operation_id, attempt_id, sandbox_id, outcome, failure, recorded_at)
             VALUES (?1, ?2, ?3, 'rolled_back', 'boot failed', '1')",
            crate::db::fixture::params![operation_id.to_string(), POD_UID, f.sandbox_id],
        )
        .unwrap();
    let mut later = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation_id,
        WakeTargetState::Started,
    );
    later.start.as_mut().unwrap().route_identity =
        Some(pod_identity(&f.sandbox_id, OTHER_POD_UID, 4));
    f.service.record_refused_attempt(
        operation_id,
        OTHER_POD_UID,
        later.start.as_ref().unwrap(),
        &Status::failed_precondition("the sandbox runs another pod's execution"),
    );
    assert_eq!(
        report(&f.service, &f.sandbox_id, operation_id)
            .refused_attempt_id
            .as_deref(),
        Some(OTHER_POD_UID)
    );

    let late = upgrade(
        &f.service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Started,
        ),
    )
    .await
    .unwrap_err();

    assert_eq!(late.code(), tonic::Code::FailedPrecondition, "{late}");
    assert!(late.message().contains("boot failed"), "{late}");
    assert_eq!(
        report(&f.service, &f.sandbox_id, operation_id)
            .refused_attempt_id
            .as_deref(),
        Some(OTHER_POD_UID),
        "the later pod stays the latest refused pod"
    );
}
