use super::*;
use crate::node_state_sync::tests::RecordingNodeStatusDirtySink;
use crate::service::tests::service_tests::{root_request, service_for_unit_tests};
use pplx_space_common_core::types::Arch;
use pplx_space_common_svc::PeerIdentity;
use pplx_space_common_svc::core::host::linux::identities::users;
use pplx_space_node_runtime_core::catalog::CompiledEnvironment;
use pplx_space_node_runtime_core::runtime_inventory::ExecutionSelectionMode;

const BOTH: &str = "e0,fc-1.17.0-linux-6.1.155,legacy";

fn catalog() -> Vec<CompiledEnvironment> {
    CompiledEnvironment::for_architecture(Arch::X86_64).unwrap()
}

fn reference(name: &str) -> EnvironmentReference {
    catalog()
        .iter()
        .find(|entry| entry.name() == name)
        .unwrap()
        .definition()
        .reference()
}

fn e0() -> EnvironmentReference {
    reference("e0")
}

fn e1() -> EnvironmentReference {
    reference("fc-1.17.0-linux-6.1.155")
}

fn startup(allowed: &str) -> ExecutionPolicy {
    ExecutionPolicy::parse(Some(allowed), None, &catalog()).unwrap()
}

fn wire(
    generation: u64,
    default: Option<EnvironmentReference>,
    executing: &[EnvironmentReference],
    legacy_unbound_allowed: Option<bool>,
) -> crate::proto::ClusterExecutionPolicy {
    crate::proto::ClusterExecutionPolicy {
        generation,
        default_environment_reference: default.map(|reference| reference.to_string()),
        executing_environment_references: executing.iter().map(ToString::to_string).collect(),
        legacy_unbound_allowed,
    }
}

fn apply_request(
    policy: Option<crate::proto::ClusterExecutionPolicy>,
) -> Request<ApplyExecutionPolicyRequest> {
    root_request(ApplyExecutionPolicyRequest { policy })
}

/// A test service enforcing `allowed` at startup, with its policy record in a
/// directory the test owns and its node-status marks counted.
async fn policy_service(
    allowed: &str,
) -> (
    tempfile::TempDir,
    NodeManagerService,
    Arc<RecordingNodeStatusDirtySink>,
) {
    let root = tempfile::tempdir().unwrap();
    let mut service = service_for_unit_tests().await;
    let marks = Arc::new(RecordingNodeStatusDirtySink::default());
    service.pool = crate::slot_pool::NetworkPool::open_in_memory_with_node_status_sink_for_test(
        crate::slot_pool::NetworkPoolConfig::new(1),
        marks.clone(),
    );
    service.execution_policy = Arc::new(ExecutionPolicyState::load(
        startup(allowed),
        root.path().join("execution-policy.json"),
    ));
    marks.take();
    (root, service, marks)
}

fn allowed(policy: &ExecutionPolicy) -> Vec<EnvironmentReference> {
    policy.allowed_references().collect()
}

#[tokio::test]
async fn only_root_and_node_ctl_apply_policy() {
    let (_root, service, _) = policy_service(BOTH).await;
    let request = |peer: Option<(u32, u32)>| {
        let mut request = Request::new(ApplyExecutionPolicyRequest { policy: None });
        if let Some((uid, gid)) = peer {
            request
                .extensions_mut()
                .insert(PeerIdentity::new(uid, gid, None));
        }
        request
    };
    assert_eq!(
        service
            .apply_execution_policy(request(None))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    for peer in [
        (
            users::SPACE_SBXMGR.uid,
            users::SPACE_SBXMGR.primary_group.gid,
        ),
        (
            users::SPACE_NODEAPI.uid,
            users::SPACE_NODEAPI.primary_group.gid,
        ),
    ] {
        assert_eq!(
            service
                .apply_execution_policy(request(Some(peer)))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }
    service
        .apply_execution_policy(request(Some((
            users::SPACE_NODECTL.uid,
            users::SPACE_NODECTL.primary_group.gid,
        ))))
        .await
        .unwrap();
    service
        .apply_execution_policy(apply_request(None))
        .await
        .unwrap();
}

#[tokio::test]
async fn uninstalled_cluster_default_is_refused_and_reported() {
    // No environment is installed under the test service's vacant roots.
    let (_root, service, marks) = policy_service(BOTH).await;
    let response = service
        .apply_execution_policy(apply_request(Some(wire(
            3,
            Some(e1()),
            &[e0(), e1()],
            None,
        ))))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.default_outcome(),
        crate::proto::ClusterDefaultOutcome::Unusable
    );
    let enforced =
        ExecutionPolicyObservation::try_from(response.enforced.as_ref().unwrap()).unwrap();
    assert_eq!(enforced.default_environment_reference(), Some(e0()));
    assert_eq!(enforced.applied_generation(), Some(3));
    let policy = service.execution_policy();
    assert_eq!(policy.default_reference(), Some(e0()));
    assert_eq!(allowed(&policy), vec![e0(), e1()]);
    assert_eq!(marks.take(), 1, "the applied generation is republished");

    // With E0 removed as well, the node has no default: unbound new work is
    // refused rather than run on E0 or on an unverified E1.
    let response = service
        .apply_execution_policy(apply_request(Some(wire(4, Some(e1()), &[e1()], None))))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.default_outcome(),
        crate::proto::ClusterDefaultOutcome::Unusable
    );
    assert_eq!(service.execution_policy().default_reference(), None);
    let error = service
        .allowed_definition(None, ExecutionSelectionMode::NewColdBoot)
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("cluster policy"), "{error}");
}

#[tokio::test]
async fn cluster_policy_narrows_admission_and_never_widens_it() {
    let (_root, service, _) = policy_service(BOTH).await;
    service
        .allowed_definition(Some(e1()), ExecutionSelectionMode::NewColdBoot)
        .unwrap();
    // Blocking E1: the cluster default is E0, which every executing set keeps.
    let response = service
        .apply_execution_policy(apply_request(Some(wire(
            5,
            Some(e0()),
            &[e0()],
            Some(false),
        ))))
        .await
        .unwrap()
        .into_inner();
    // E0 isn't installed here either, so it's refused, but it stays the
    // configured default and keeps executing.
    assert_eq!(service.execution_policy().default_reference(), Some(e0()));
    assert_eq!(
        response.default_outcome(),
        crate::proto::ClusterDefaultOutcome::Unusable
    );
    let error = service
        .allowed_definition(Some(e1()), ExecutionSelectionMode::NewColdBoot)
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(!service.execution_policy().legacy_unbound_allowed());

    // A node allowing only E0 can't be widened to E1.
    let (_root, narrow, _) = policy_service("e0").await;
    let response = narrow
        .apply_execution_policy(apply_request(Some(wire(
            6,
            Some(e1()),
            &[e0(), e1()],
            Some(true),
        ))))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.default_outcome(),
        crate::proto::ClusterDefaultOutcome::NotAllowed
    );
    let policy = narrow.execution_policy();
    assert_eq!(allowed(&policy), vec![e0()]);
    assert_eq!(policy.default_reference(), Some(e0()));
    assert!(!policy.legacy_unbound_allowed(), "legacy is never granted");
    narrow
        .allowed_definition(Some(e1()), ExecutionSelectionMode::NewColdBoot)
        .unwrap_err();
}

#[tokio::test]
async fn applied_policy_survives_restart_and_absence_restores_startup_settings() {
    let (root, service, marks) = policy_service(BOTH).await;
    let record = root.path().join("execution-policy.json");
    service
        .apply_execution_policy(apply_request(Some(wire(
            8,
            Some(e1()),
            &[e1()],
            Some(false),
        ))))
        .await
        .unwrap();
    assert!(record.exists());
    marks.take();
    // An unchanged policy is neither republished nor rewritten: remove the
    // record and it stays removed.
    std::fs::remove_file(&record).unwrap();
    service
        .apply_execution_policy(apply_request(Some(wire(
            8,
            Some(e1()),
            &[e1()],
            Some(false),
        ))))
        .await
        .unwrap();
    assert_eq!(marks.take(), 0);
    assert!(!record.exists(), "the unchanged record was rewritten");
    // A changed generation is persisted again.
    service
        .apply_execution_policy(apply_request(Some(wire(
            9,
            Some(e1()),
            &[e1()],
            Some(false),
        ))))
        .await
        .unwrap();
    assert_eq!(marks.take(), 1);

    let restarted = ExecutionPolicyState::load(startup(BOTH), record.clone());
    let policy = restarted.effective();
    assert_eq!(policy.applied_generation(), Some(9));
    assert_eq!(allowed(&policy), vec![e1()]);
    assert!(!policy.legacy_unbound_allowed());
    assert_eq!(
        policy.default_reference(),
        None,
        "a default refused before the restart stays refused"
    );
    // The record is re-narrowed against the new process's startup settings.
    let tightened = ExecutionPolicyState::load(startup("e0,legacy"), record.clone());
    assert_eq!(allowed(&tightened.effective()), Vec::new());
    assert_eq!(tightened.effective().default_reference(), None);

    // The cluster stopped serving a policy: back to the startup settings,
    // for this process and the next.
    let response = service
        .apply_execution_policy(apply_request(None))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.default_outcome(),
        crate::proto::ClusterDefaultOutcome::Unset
    );
    let enforced =
        ExecutionPolicyObservation::try_from(response.enforced.as_ref().unwrap()).unwrap();
    assert_eq!(enforced.schema_version(), EXECUTION_POLICY_SCHEMA_VERSION);
    assert_eq!(enforced.applied_generation(), None);
    assert_eq!(service.execution_policy().applied_generation(), None);
    assert_eq!(allowed(&service.execution_policy()), vec![e0(), e1()]);
    assert_eq!(marks.take(), 1);
    assert!(!record.exists());
    let restarted = ExecutionPolicyState::load(startup(BOTH), record);
    assert_eq!(restarted.effective().applied_generation(), None);
}

#[test]
fn verified_default_is_adopted_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("execution-policy.json");
    let record = PolicyRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        cluster: ClusterExecutionPolicy {
            generation: 2,
            default: Some(ClusterDefault::Environment(e1())),
            executing: [e0(), e1()].into(),
            legacy_unbound_allowed: None,
        },
        default_verified: true,
    };
    write_record(&path, Some(&record)).unwrap();
    let state = ExecutionPolicyState::load(startup(BOTH), path);
    let policy = state.effective();
    assert_eq!(policy.default_reference(), Some(e1()));
    assert_eq!(allowed(&policy), vec![e0(), e1()]);
    assert!(policy.legacy_unbound_allowed());
    let observation = observation(&policy).unwrap();
    assert_eq!(
        observation.schema_version(),
        EXECUTION_POLICY_APPLIED_SCHEMA_VERSION
    );
    assert_eq!(observation.applied_generation(), Some(2));
}

fn assert_refuses_everything(policy: &ExecutionPolicy) {
    assert!(policy.cluster_policy_unreadable());
    assert_eq!(allowed(policy), Vec::new());
    assert_eq!(policy.default_reference(), None);
    assert!(!policy.legacy_unbound_allowed());
    assert_eq!(policy.applied_generation(), None);
    let published = observation(policy).unwrap();
    assert_eq!(
        published.schema_version(),
        EXECUTION_POLICY_APPLIED_SCHEMA_VERSION
    );
    assert!(published.cluster_policy_unreadable());
    assert_eq!(published.applied_generation(), None);
}

const RECORD_DAMAGE: [&[u8]; 3] = [
    b"{",
    br#"{"schema_version":2,"cluster":{"generation":1,"default":null,"executing":[],"legacy_unbound_allowed":null},"default_verified":false}"#,
    br#"{"schema_version":1,"cluster":{"generation":1,"default":null,"executing":[],"legacy_unbound_allowed":null},"default_verified":false,"extra":1}"#,
];

#[tokio::test]
async fn corrupt_record_admits_nothing_new_and_the_next_application_recovers() {
    let oversized = vec![b' '; MAX_RECORD_BYTES as usize + 1];
    for (index, contents) in RECORD_DAMAGE
        .iter()
        .copied()
        .chain([oversized.as_slice()])
        .enumerate()
    {
        for recovery in [None, Some(wire(4, Some(e0()), &[e0(), e1()], None))] {
            let (root, mut service, marks) = policy_service(BOTH).await;
            let path = root.path().join("execution-policy.json");
            std::fs::write(&path, contents).unwrap();
            service.execution_policy =
                Arc::new(ExecutionPolicyState::load(startup(BOTH), path.clone()));
            assert_refuses_everything(&service.execution_policy());
            for reference in [None, Some(e0()), Some(e1())] {
                let error = service
                    .allowed_definition(reference, ExecutionSelectionMode::NewColdBoot)
                    .unwrap_err();
                assert!(error.message().contains("admits no new work"), "{error}");
            }
            // node-ctl's publication says so, which old readers withhold as a
            // future schema and new ones read as no usable evidence.
            let (_fixture, paths) = crate::service::tests::service_tests::installed_runtime();
            let inventory = pplx_space_node_runtime_core::runtime_inventory::RuntimeInventory::discover(
                [pplx_space_node_runtime_core::runtime_inventory::RuntimeInstallationRoot::new(
                    pplx_space_common_core::types::RuntimeKind::Firecracker,
                    &paths.vendor,
                )],
                &paths.assets,
                &paths.kernels,
            )
            .unwrap();
            service.runtime_paths = paths;
            service.runtime_inventory.store(Some(Arc::new(inventory)));
            let report = service
                .get_execution_inventory(root_request(
                    crate::proto::GetExecutionInventoryRequest {},
                ))
                .await
                .unwrap()
                .into_inner();
            let published =
                ExecutionPolicyObservation::try_from(report.policy.as_ref().unwrap()).unwrap();
            assert!(published.cluster_policy_unreadable(), "case {index}");

            // The next application recovers, and replaces or removes the file
            // even when it carries no policy.
            let expected = recovery.is_some();
            service
                .apply_execution_policy(apply_request(recovery))
                .await
                .unwrap();
            assert_eq!(marks.take(), 1, "case {index}");
            let policy = service.execution_policy();
            assert!(!policy.cluster_policy_unreadable());
            assert_eq!(policy.applied_generation().is_some(), expected);
            assert_eq!(path.exists(), expected, "case {index}");
            let restarted = ExecutionPolicyState::load(startup(BOTH), path.clone());
            assert!(
                !restarted.effective().cluster_policy_unreadable(),
                "case {index}"
            );
            assert_eq!(
                restarted.effective().applied_generation().is_some(),
                expected
            );
        }
    }
}

#[tokio::test]
async fn a_slow_earlier_application_never_replaces_a_later_one() {
    let (_root, mut service, _) = policy_service(BOTH).await;
    let (_fixture, paths) = crate::service::tests::service_tests::installed_runtime();
    service.runtime_paths = paths;
    // The earlier application's default check stalls for its whole budget,
    // before it takes the record lock.
    let discovery = service
        .runtime_inventory_refresh_lock
        .clone()
        .lock_owned()
        .await;
    let earlier = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .apply_execution_policy(apply_request(Some(wire(
                    1,
                    Some(e1()),
                    &[e0(), e1()],
                    None,
                ))))
                .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    // The later one needs no check, so it enforces and persists first.
    service
        .apply_execution_policy(apply_request(Some(wire(2, None, &[], Some(false)))))
        .await
        .unwrap();
    assert_eq!(service.execution_policy().applied_generation(), Some(2));
    let earlier = earlier.await.unwrap().unwrap_err();
    assert_eq!(earlier.code(), tonic::Code::Aborted);
    drop(discovery);
    let policy = service.execution_policy();
    assert_eq!(
        policy.applied_generation(),
        Some(2),
        "the later policy stays"
    );
    assert!(!policy.legacy_unbound_allowed());
}

#[tokio::test]
async fn hung_record_write_still_lets_a_block_be_enforced() {
    let (root, service, marks) = policy_service(BOTH).await;
    // A previous application's write holds the record lock.
    let _writing = service
        .execution_policy
        .persisted
        .clone()
        .lock_owned()
        .await;
    let error = tokio::time::timeout(
        RECORD_LOCK_BUDGET + USABILITY_BUDGET + std::time::Duration::from_secs(1),
        service.apply_execution_policy(apply_request(Some(wire(5, Some(e0()), &[e0()], None)))),
    )
    .await
    .expect("the record lock wait is bounded")
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(
        allowed(&service.execution_policy()),
        vec![e0()],
        "the block holds"
    );
    assert_eq!(marks.take(), 1);
    assert!(!root.path().join("execution-policy.json").exists());
}

#[tokio::test]
async fn long_accepted_policy_survives_restart() {
    let (root, service, _) = policy_service(BOTH).await;
    let mut long = wire(3, Some(e0()), &[e0()], None);
    long.executing_environment_references
        .extend((0..2000).map(|index| format!("sha256:{index:064x}")));
    service
        .apply_execution_policy(apply_request(Some(long)))
        .await
        .unwrap();
    let path = root.path().join("execution-policy.json");
    assert!(std::fs::metadata(&path).unwrap().len() > 64 * 1024);
    let restarted = ExecutionPolicyState::load(startup(BOTH), path);
    assert!(!restarted.effective().cluster_policy_unreadable());
    assert_eq!(restarted.effective().applied_generation(), Some(3));
    assert_eq!(allowed(&restarted.effective()), vec![e0()]);
}

#[test]
fn record_a_restart_would_refuse_is_never_written() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("execution-policy.json");
    let executing = (0..MAX_RECORD_BYTES / 64)
        .map(|index| format!("sha256:{index:064x}").parse().unwrap())
        .collect();
    let record = PolicyRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        cluster: ClusterExecutionPolicy {
            generation: 1,
            default: None,
            executing,
            legacy_unbound_allowed: None,
        },
        default_verified: false,
    };
    let error = write_record(&path, Some(&record)).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(!path.exists());
}

#[test]
fn linked_record_is_not_followed() {
    let root = tempfile::tempdir().unwrap();
    let elsewhere = root.path().join("elsewhere.json");
    let record = PolicyRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        cluster: ClusterExecutionPolicy {
            generation: 6,
            default: Some(ClusterDefault::Environment(e0())),
            executing: [e0()].into(),
            legacy_unbound_allowed: Some(false),
        },
        default_verified: true,
    };
    write_record(&elsewhere, Some(&record)).unwrap();
    let path = root.path().join("execution-policy.json");
    std::os::unix::fs::symlink(&elsewhere, &path).unwrap();
    let state = ExecutionPolicyState::load(startup(BOTH), path.clone());
    assert_refuses_everything(&state.effective());
    // Removing the record removes the link, never its target.
    write_record(&path, None).unwrap();
    std::fs::symlink_metadata(&path).unwrap_err();
    assert!(elsewhere.exists());
}

#[tokio::test]
async fn no_policy_is_refused_for_what_it_names() {
    let (_root, service, _) = policy_service(BOTH).await;
    // An unparsable default narrows like any default, and is refused.
    let response = service
        .apply_execution_policy(apply_request(Some(crate::proto::ClusterExecutionPolicy {
            default_environment_reference: Some("not-a-digest".into()),
            ..wire(1, None, &[e0()], None)
        })))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.default_outcome(),
        crate::proto::ClusterDefaultOutcome::NotAllowed
    );
    assert_eq!(
        allowed(&service.execution_policy()),
        vec![e0()],
        "the block lands"
    );
    // An unparsable executing reference is ignored, so the block still lands.
    let mut unparsable = wire(1, Some(e0()), &[e0()], None);
    unparsable
        .executing_environment_references
        .push("e1".into());
    service
        .apply_execution_policy(apply_request(Some(unparsable)))
        .await
        .unwrap();
    assert_eq!(allowed(&service.execution_policy()), vec![e0()]);
    // The default always executes, listed or not.
    service
        .apply_execution_policy(apply_request(Some(wire(1, Some(e1()), &[e0()], None))))
        .await
        .unwrap();
    assert_eq!(allowed(&service.execution_policy()), vec![e0(), e1()]);
    // A repeated reference is ignored, not refused.
    let mut repeated = wire(1, Some(e0()), &[e0()], None);
    repeated
        .executing_environment_references
        .push(e0().to_string());
    service
        .apply_execution_policy(apply_request(Some(repeated)))
        .await
        .unwrap();
    assert_eq!(allowed(&service.execution_policy()), vec![e0()]);
    // References this node doesn't compile are ignored, not refused.
    let foreign: EnvironmentReference = format!("sha256:{}", "ef".repeat(32)).parse().unwrap();
    service
        .apply_execution_policy(apply_request(Some(wire(1, None, &[foreign], None))))
        .await
        .unwrap();
    assert_eq!(service.execution_policy().applied_generation(), Some(1));
    // The cluster's store caps nothing, so neither does the node: a long
    // executing set still applies, rather than leaving a block unenforced.
    let mut long = wire(2, Some(e0()), &[e0()], None);
    long.executing_environment_references
        .extend((0..100).map(|index| format!("sha256:{index:064x}")));
    service
        .apply_execution_policy(apply_request(Some(long)))
        .await
        .unwrap();
    assert_eq!(allowed(&service.execution_policy()), vec![e0()]);
}

#[tokio::test]
async fn failed_persistence_still_enforces_the_policy_and_retries() {
    let (root, mut service, _) = policy_service(BOTH).await;
    let missing = root.path().join("missing/execution-policy.json");
    service.execution_policy = Arc::new(ExecutionPolicyState::load(startup(BOTH), missing.clone()));
    let request = || apply_request(Some(wire(2, Some(e0()), &[e0()], None)));
    service.apply_execution_policy(request()).await.unwrap_err();
    assert_eq!(
        allowed(&service.execution_policy()),
        vec![e0()],
        "the block holds"
    );
    std::fs::create_dir(missing.parent().unwrap()).unwrap();
    service.apply_execution_policy(request()).await.unwrap();
    assert!(missing.exists(), "the next application persists it");
}

#[tokio::test]
async fn a_slow_check_keeps_an_adopted_default_but_adopts_nothing_new() {
    let cluster = ClusterExecutionPolicy {
        generation: 7,
        default: Some(ClusterDefault::Environment(e1())),
        executing: [e1()].into(),
        legacy_unbound_allowed: None,
    };
    for adopted in [true, false] {
        let (_root, mut service, _) = policy_service(BOTH).await;
        if adopted {
            service.narrow_execution_for_test(&cluster);
        }
        let (_fixture, paths) = crate::service::tests::service_tests::installed_runtime();
        service.runtime_paths = paths;
        let _discovery = service
            .runtime_inventory_refresh_lock
            .clone()
            .lock_owned()
            .await;
        let response = service
            .apply_execution_policy(apply_request(Some(wire(7, Some(e1()), &[e1()], None))))
            .await
            .unwrap()
            .into_inner();
        let expected = if adopted {
            crate::proto::ClusterDefaultOutcome::Adopted
        } else {
            crate::proto::ClusterDefaultOutcome::Unusable
        };
        assert_eq!(response.default_outcome(), expected);
        assert_eq!(
            service.execution_policy().default_reference(),
            adopted.then(e1)
        );
    }
    // A startup default matching the cluster's was never verified, so a slow
    // check doesn't count as an adoption.
    let (_root, mut service, _) = policy_service(BOTH).await;
    let (_fixture, paths) = crate::service::tests::service_tests::installed_runtime();
    service.runtime_paths = paths;
    let _discovery = service
        .runtime_inventory_refresh_lock
        .clone()
        .lock_owned()
        .await;
    let response = service
        .apply_execution_policy(apply_request(Some(wire(8, Some(e0()), &[e0()], None))))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.default_outcome(),
        crate::proto::ClusterDefaultOutcome::Unusable
    );

    // A verdict restored from the record does count, until a check this
    // process completes says otherwise, so a slow first discovery after a
    // restart can't flap the default.
    let (root, mut service, _) = policy_service(BOTH).await;
    let path = root.path().join("execution-policy.json");
    write_record(
        &path,
        Some(&PolicyRecord {
            schema_version: RECORD_SCHEMA_VERSION,
            cluster: cluster.clone(),
            default_verified: true,
        }),
    )
    .unwrap();
    service.execution_policy = Arc::new(ExecutionPolicyState::load(startup(BOTH), path));
    assert_eq!(service.execution_policy().default_reference(), Some(e1()));
    let (_fixture, paths) = crate::service::tests::service_tests::installed_runtime();
    service.runtime_paths = paths;
    let _discovery = service
        .runtime_inventory_refresh_lock
        .clone()
        .lock_owned()
        .await;
    let response = service
        .apply_execution_policy(apply_request(Some(wire(7, Some(e1()), &[e1()], None))))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.default_outcome(),
        crate::proto::ClusterDefaultOutcome::Adopted
    );
    assert_eq!(service.execution_policy().default_reference(), Some(e1()));
}

#[tokio::test]
async fn slow_default_verification_refuses_the_default_but_still_narrows() {
    let (_root, mut service, _) = policy_service(BOTH).await;
    let (_fixture, paths) = crate::service::tests::service_tests::installed_runtime();
    service.runtime_paths = paths;
    // Discovery can't start while another holds the refresh lock.
    let _discovery = service
        .runtime_inventory_refresh_lock
        .clone()
        .lock_owned()
        .await;
    let applied = tokio::time::timeout(
        USABILITY_BUDGET + std::time::Duration::from_secs(5),
        service.apply_execution_policy(apply_request(Some(wire(3, Some(e1()), &[e1()], None)))),
    )
    .await
    .expect("verification is bounded")
    .unwrap()
    .into_inner();
    assert_eq!(
        applied.default_outcome(),
        crate::proto::ClusterDefaultOutcome::Unusable
    );
    assert_eq!(
        allowed(&service.execution_policy()),
        vec![e1()],
        "the block holds"
    );
}

#[test]
fn a_superseded_application_changes_neither_policy_nor_verdict() {
    let root = tempfile::tempdir().unwrap();
    let state =
        ExecutionPolicyState::load(startup(BOTH), root.path().join("execution-policy.json"));
    let later = Arc::new(
        startup(BOTH)
            .narrow(
                &ClusterExecutionPolicy {
                    generation: 2,
                    default: Some(ClusterDefault::Environment(e1())),
                    executing: [e1()].into(),
                    legacy_unbound_allowed: None,
                },
                true,
            )
            .policy,
    );
    state.enforce(2, later.clone(), Some(Some(e1()))).unwrap();
    assert!(
        state
            .enforce(1, Arc::new(startup(BOTH)), Some(None))
            .is_none()
    );
    assert!(Arc::ptr_eq(&state.effective(), &later));
    assert_eq!(*state.verified_default.lock().unwrap(), Some(e1()));
}

#[tokio::test]
async fn a_verdict_on_another_default_is_stale() {
    let (_root, mut service, _) = policy_service(BOTH).await;
    // E1 was verified, then the cluster moved its default away and back.
    service.narrow_execution_for_test(&ClusterExecutionPolicy {
        generation: 1,
        default: Some(ClusterDefault::Environment(e1())),
        executing: [e1()].into(),
        legacy_unbound_allowed: None,
    });
    service
        .apply_execution_policy(apply_request(Some(wire(2, None, &[], None))))
        .await
        .unwrap();
    let (_fixture, paths) = crate::service::tests::service_tests::installed_runtime();
    service.runtime_paths = paths;
    let _discovery = service
        .runtime_inventory_refresh_lock
        .clone()
        .lock_owned()
        .await;
    let response = service
        .apply_execution_policy(apply_request(Some(wire(3, Some(e1()), &[e1()], None))))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.default_outcome(),
        crate::proto::ClusterDefaultOutcome::Unusable
    );
    drop(_discovery);

    // Nor does it survive the cluster serving no policy at all.
    let (_root, mut service, _) = policy_service(BOTH).await;
    service.narrow_execution_for_test(&ClusterExecutionPolicy {
        generation: 1,
        default: Some(ClusterDefault::Environment(e1())),
        executing: [e1()].into(),
        legacy_unbound_allowed: None,
    });
    service
        .apply_execution_policy(apply_request(None))
        .await
        .unwrap();
    let (_fixture, paths) = crate::service::tests::service_tests::installed_runtime();
    service.runtime_paths = paths;
    let _discovery = service
        .runtime_inventory_refresh_lock
        .clone()
        .lock_owned()
        .await;
    let response = service
        .apply_execution_policy(apply_request(Some(wire(4, Some(e1()), &[e1()], None))))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.default_outcome(),
        crate::proto::ClusterDefaultOutcome::Unusable
    );
}
