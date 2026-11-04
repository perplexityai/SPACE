use pplx_space_common_core::types::{NodeId, SandboxId};

use super::*;

struct RefusedCreate {
    name: &'static str,
    req: CreateRequest,
    content_hash_resolved: Option<&'static str>,
    code: tonic::Code,
    message: &'static str,
}

fn create_request() -> CreateRequest {
    CreateRequest {
        template: "seed".to_owned(),
        runtime: "fc".to_owned(),
        ..Default::default()
    }
}

// A refusal ahead of the durable insert must leave neither a sandbox row nor a
// cleanup episode: a stranded `Creating` row would need cleanup that no caller
// requested. The Create RPC validates the request before it gets here, so only
// refusals that depend on the resolved template or the resolved sandbox id are
// listed. Successful logical creates use the test service’s cache root.
#[tokio::test]
async fn create_refusals_before_insert_persist_nothing() {
    let cases = [
        RefusedCreate {
            name: "template hash cannot be computed",
            req: create_request(),
            content_hash_resolved: None,
            code: tonic::Code::Internal,
            message: "compute template hash",
        },
        RefusedCreate {
            name: "route identity bound to another sandbox",
            req: CreateRequest {
                route_identity: Some(crate::proto::SpaceApiRouteIdentity {
                    sandbox_id: Some(SandboxId::new().to_string()),
                    ..Default::default()
                }),
                ..create_request()
            },
            content_hash_resolved: Some("hash"),
            code: tonic::Code::InvalidArgument,
            message: "does not match the resolved sandbox",
        },
    ];

    let template_dir = tempfile::tempdir().unwrap();
    let template_dir = template_dir.path().to_str().unwrap();
    let owner = SandboxOwner::parse("user").unwrap();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    for case in cases {
        let service = SandboxManagerService::new_with_options(
            crate::db::test_conn(),
            SandboxManagerServiceOptions {
                local_node_id: Some(NodeId::new()),
                ..SandboxManagerServiceOptions::for_test()
            },
        );
        let id = SandboxId::new().to_string();
        let error = service
            .create_with_id(CreateWithIdArgs {
                guest_image: Default::default(),
                req: &case.req,
                sandbox_owner: &owner,
                effective_mode: BootMode::Cold,
                id: id.clone(),
                template_dir,
                execution: None,
                snapshot_dir_resolved: None,
                snapshot_vcpu: None,
                content_hash_resolved: case.content_hash_resolved,
                auto_lifecycle: &auto_lifecycle,
                auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
                default_network_policy_authority: crate::db::PolicyAuthority::Cluster,
            })
            .await
            .expect_err(case.name);

        assert_eq!(error.code(), case.code, "{}: {error:?}", case.name);
        assert!(
            error.message().contains(case.message),
            "{}: {error:?}",
            case.name
        );
        let db = service.db_conn().unwrap();
        assert!(
            crate::db::get_sandbox(&db, &id).unwrap().is_none(),
            "{}: refused create persisted a sandbox row",
            case.name
        );
        assert!(
            crate::db::cleanup_episode(&db, &id).unwrap().is_none(),
            "{}: refused create persisted a cleanup episode",
            case.name
        );
    }
}

/// Stop at allocation's validated rollback boundary, before any VM or host
/// filesystem mutation. Both attempts must select from the same checkpoint.
/// E0 restores through a qualified compatible loader; a checkpoint of the
/// 1.17 runtime upgrade restores under its own producer, which that binding
/// qualifies for memory state.
#[tokio::test]
async fn restore_persists_consumer_before_allocation_and_reselects_from_checkpoint() {
    use pplx_space_common_core::types::{Arch, EnvironmentDescriptor, RuntimeBinding};
    use pplx_space_execution_common::environment::EnvironmentDefinition;
    if !crate::service::tests::enter_route_test_process(&[
        ("EPPO_SDK_KEY", None),
        ("SPACE_FLAGD_ENABLED", None),
    ]) {
        return;
    }
    let mut catalog =
        pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap();
    let upgrade = catalog
        .iter()
        .find(|entry| entry.name() == "fc-1.17.0-linux-6.1.155")
        .unwrap();
    assert!(upgrade.supports_memory_state());
    let upgrade = upgrade.definition().clone();
    let initial = catalog.remove(0);
    assert!(initial.is_initial());
    let mut spec = initial.definition().descriptor().spec().clone();
    let RuntimeBinding::ManagedRelease(release) = &mut spec.runtime;
    release.runtime.version = "1.15.2".parse().unwrap();
    let patched_e0 = EnvironmentDefinition::new(EnvironmentDescriptor::try_from(spec).unwrap());
    restore_selects_its_consumer_from_the_checkpoint(
        initial.definition().execution_requirements().unwrap(),
        patched_e0,
    )
    .await;
    restore_selects_its_consumer_from_the_checkpoint(
        upgrade.execution_requirements().unwrap(),
        upgrade,
    )
    .await;
}

async fn restore_selects_its_consumer_from_the_checkpoint(
    producer: pplx_space_common_core::types::ExecutionMetadata,
    consumer_definition: pplx_space_execution_common::environment::EnvironmentDefinition,
) {
    use crate::node_manager_transport::fake::FakeNodeManager;
    use pplx_space_common_core::types::{
        ContentHash, RuntimeKind, SandboxArtifactMetadata, SandboxRuntimeLabel,
        SnapshotExecutionMetadata,
    };
    use pplx_space_node_mgr_client::proto::{
        AllocateResponse, ExecutionEnvironmentSelection, NetworkPoolStatusResponse,
        ReleaseResponse, ResolveExecutionEnvironmentRequest, ResolveExecutionEnvironmentResponse,
        resolve_execution_environment_request::Compatible,
    };
    let consumer = consumer_definition.execution_requirements().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let cache = directory.path().join("cache");
    let db_path = directory.path().join("sandbox.sqlite");
    crate::db::create_test_schema_on_disk(&db_path);
    let conn = crate::db::Connection::open_at(&db_path).unwrap();
    let id = SandboxId::new();
    let template_hash: ContentHash = format!("sha256:{}", "ab".repeat(32)).parse().unwrap();
    crate::db::insert_sandbox(
        &conn,
        crate::db::InsertSandboxParams {
            guest_image: Default::default(),
            id: &id.to_string(),
            uuid8: &SandboxRuntimeLabel::from_sandbox_id(id),
            state: SandboxState::Suspended,
            template: "active-template",
            template_hash: Some(&template_hash.to_string()),
            user: &SandboxOwner::parse("user").unwrap(),
            runtime: RuntimeKind::Firecracker.alias(),
            snapshots_enabled: false,
            rootfs_size_mb: Some(2048),
            disk_io_limits: Default::default(),
            pool_template: Some("pool-template"),
            volume_encryption: false,
            process_env: &Default::default(),
            auto_lifecycle: &AutoLifecyclePolicy::disabled(),
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: crate::db::PolicyAuthority::Cluster,
            auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
        },
    )
    .unwrap();
    crate::db::upsert_sandbox_execution_metadata(&conn, &id.to_string(), &producer).unwrap();
    let instance = cache.join(id.to_string()).join("instance");
    let snapshot = cache.join(id.to_string()).join("snapshot");
    std::fs::create_dir_all(&instance).unwrap();
    std::fs::create_dir_all(&snapshot).unwrap();
    std::fs::write(instance.join("rootfs.ext4"), []).unwrap();
    let metadata = SandboxArtifactMetadata::new(
        id,
        producer.clone(),
        "active-template".parse().unwrap(),
        Some(template_hash),
    );
    std::fs::write(
        instance.parent().unwrap().join("metadata.json"),
        serde_json::to_vec(&metadata).unwrap(),
    )
    .unwrap();
    let checkpoint =
        serde_json::to_vec(&SnapshotExecutionMetadata::new(producer.clone()).unwrap()).unwrap();
    std::fs::write(snapshot.join("runtime.json"), &checkpoint).unwrap();
    let expected_producer = producer.clone();
    let expected_consumer = consumer.clone();
    let selected_consumer = consumer.clone();
    let observed_db = db_path.clone();
    let allocations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_allocations = allocations.clone();
    let node = FakeNodeManager::default()
        .unary("GetNetworkPoolStatus", |_: NetworkPoolStatusRequest| {
            Ok(NetworkPoolStatusResponse {
                sandbox_cgroup_memory_mib: 4096,
                sandbox_guest_memory_max_mib: 3968,
                sandbox_memory_headroom_mib: 128,
                ..Default::default()
            })
        })
        .unary(
            "ResolveExecutionEnvironment",
            move |request: ResolveExecutionEnvironmentRequest| {
                let Some(Compatible::RestoreSource(source)) = request.compatible else {
                    panic!("restore must use its operation-specific resolver");
                };
                assert_eq!(
                    pplx_space_common_core::types::ExecutionMetadata::try_from(&source).unwrap(),
                    expected_producer
                );
                Ok(ResolveExecutionEnvironmentResponse {
                    accepted_selection: ExecutionEnvironmentSelection::Restore as i32,
                    definition: Some((&consumer_definition).into()),
                    execution: Some((&selected_consumer).into()),
                })
            },
        )
        .unary("Allocate", move |request: AllocateRequest| {
            let db = crate::db::Connection::open_at(&observed_db).unwrap();
            let row = crate::db::get_sandbox(&db, &request.sandbox_id)
                .unwrap()
                .unwrap();
            assert_eq!(row.state, SandboxState::Restoring);
            assert_eq!(row.execution_metadata, Some(expected_consumer.clone()));
            observed_allocations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(AllocateResponse {
                sandbox_uid: 1,
                ..Default::default()
            })
        })
        .unary(
            "Release",
            |_: ReleaseRequest| Ok(ReleaseResponse::default()),
        )
        .serve();
    let service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_manager: node.connector(),
            create_cache_dir: Some(cache),
            ..SandboxManagerServiceOptions::for_test()
        },
    );
    for attempt in 1..=2 {
        let row = service.lookup_sandbox(&id.to_string()).unwrap();
        assert_eq!(row.state, SandboxState::Suspended);
        let error = tokio::time::timeout(
            Duration::from_secs(20),
            service.start_from_checkpoint(StartFromCheckpointLaunch {
                vcpu: None,
                activation_auto_lifecycle: None,
                operation_activation_attempt_id: None,
                sandbox: &row,
                blocking: false,
                netns_path: None,
                target_state: SandboxState::Started,
                execution_generation: None,
                space_api_route_identity: Default::default(),
                route_identity_provenance:
                    crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
                open_activation_claim_eligible: false,
                restored_snapshot: RestoredSnapshotProvenance::default(),
            }),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(error.message().contains("invalid sandbox uid"), "{error}");
        assert_eq!(
            allocations.load(std::sync::atomic::Ordering::SeqCst),
            attempt
        );
        assert_eq!(
            service
                .lookup_sandbox(&id.to_string())
                .unwrap()
                .execution_metadata,
            Some(consumer.clone())
        );
        assert_eq!(
            std::fs::read(snapshot.join("runtime.json")).unwrap(),
            checkpoint
        );
    }
    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(Duration::from_secs(20), service.task_tracker.wait())
        .await
        .unwrap();
}

/// A runtime upgrade's fence refuses a concurrent resume at the activation
/// claim: the restore answers with a retryable error before it saves a
/// consumer, allocates a slot or prepares any file.
#[tokio::test]
async fn a_runtime_upgrade_fence_refuses_a_concurrent_restore_before_allocation() {
    use crate::node_manager_transport::fake::FakeNodeManager;
    use pplx_space_common_core::types::{
        Arch, ContentHash, RuntimeKind, SandboxArtifactMetadata, SandboxRuntimeLabel,
        SnapshotExecutionMetadata,
    };
    use pplx_space_node_mgr_client::proto::{
        AllocateResponse, ExecutionEnvironmentSelection, NetworkPoolStatusResponse,
        ResolveExecutionEnvironmentRequest, ResolveExecutionEnvironmentResponse,
    };
    if !crate::service::tests::enter_route_test_process(&[
        ("EPPO_SDK_KEY", None),
        ("SPACE_FLAGD_ENABLED", None),
    ]) {
        return;
    }
    let compiled =
        pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap();
    let producer_definition = compiled[0].definition().clone();
    let producer = producer_definition.execution_requirements().unwrap();
    let target = compiled[1].definition().reference();
    let directory = tempfile::tempdir().unwrap();
    let cache = directory.path().join("cache");
    let db_path = directory.path().join("sandbox.sqlite");
    crate::db::create_test_schema_on_disk(&db_path);
    let conn = crate::db::Connection::open_at(&db_path).unwrap();
    let id = SandboxId::new();
    let template_hash: ContentHash = format!("sha256:{}", "ab".repeat(32)).parse().unwrap();
    crate::db::insert_sandbox(
        &conn,
        crate::db::InsertSandboxParams {
            guest_image: Default::default(),
            id: &id.to_string(),
            uuid8: &SandboxRuntimeLabel::from_sandbox_id(id),
            state: SandboxState::Suspended,
            template: "active-template",
            template_hash: Some(&template_hash.to_string()),
            user: &SandboxOwner::parse("user").unwrap(),
            runtime: RuntimeKind::Firecracker.alias(),
            snapshots_enabled: false,
            rootfs_size_mb: Some(2048),
            disk_io_limits: Default::default(),
            // A pool member skips the effective network policy lookup.
            pool_template: Some("pool-template"),
            volume_encryption: false,
            process_env: &Default::default(),
            auto_lifecycle: &AutoLifecyclePolicy::disabled(),
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: crate::db::PolicyAuthority::Cluster,
            auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
        },
    )
    .unwrap();
    crate::db::upsert_sandbox_execution_metadata(&conn, &id.to_string(), &producer).unwrap();
    let operation_id =
        crate::db::test_fixtures::fence_runtime_upgrade(&conn, &id.to_string(), target, None)
            .operation_id;
    let instance = cache.join(id.to_string()).join("instance");
    let snapshot = cache.join(id.to_string()).join("snapshot");
    std::fs::create_dir_all(&instance).unwrap();
    std::fs::create_dir_all(&snapshot).unwrap();
    std::fs::write(instance.join("rootfs.ext4"), []).unwrap();
    let metadata = SandboxArtifactMetadata::new(
        id,
        producer.clone(),
        "active-template".parse().unwrap(),
        Some(template_hash),
    );
    std::fs::write(
        instance.parent().unwrap().join("metadata.json"),
        serde_json::to_vec(&metadata).unwrap(),
    )
    .unwrap();
    std::fs::write(
        snapshot.join("runtime.json"),
        serde_json::to_vec(&SnapshotExecutionMetadata::new(producer.clone()).unwrap()).unwrap(),
    )
    .unwrap();
    let allocations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_allocations = allocations.clone();
    let selected = producer.clone();
    let node = FakeNodeManager::default()
        .unary("GetNetworkPoolStatus", |_: NetworkPoolStatusRequest| {
            Ok(NetworkPoolStatusResponse {
                sandbox_cgroup_memory_mib: 4096,
                sandbox_guest_memory_max_mib: 3968,
                sandbox_memory_headroom_mib: 128,
                ..Default::default()
            })
        })
        .unary(
            "ResolveExecutionEnvironment",
            move |_: ResolveExecutionEnvironmentRequest| {
                Ok(ResolveExecutionEnvironmentResponse {
                    accepted_selection: ExecutionEnvironmentSelection::Restore as i32,
                    definition: Some((&producer_definition).into()),
                    execution: Some((&selected).into()),
                })
            },
        )
        .unary("Allocate", move |_: AllocateRequest| {
            observed_allocations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(AllocateResponse::default())
        })
        .serve();
    let service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_manager: node.connector(),
            create_cache_dir: Some(cache),
            ..SandboxManagerServiceOptions::for_test()
        },
    );
    let row = service.lookup_sandbox(&id.to_string()).unwrap();

    let error = tokio::time::timeout(
        Duration::from_secs(20),
        service.start_from_checkpoint(StartFromCheckpointLaunch {
            vcpu: None,
            activation_auto_lifecycle: None,
            operation_activation_attempt_id: None,
            sandbox: &row,
            blocking: false,
            netns_path: None,
            target_state: SandboxState::Started,
            execution_generation: None,
            space_api_route_identity: Default::default(),
            route_identity_provenance:
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
            open_activation_claim_eligible: false,
            restored_snapshot: RestoredSnapshotProvenance::default(),
        }),
    )
    .await
    .unwrap()
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    assert!(
        error.message().contains(&operation_id.to_string()),
        "{error}"
    );
    assert_eq!(allocations.load(std::sync::atomic::Ordering::SeqCst), 0);
    let after = service.lookup_sandbox(&id.to_string()).unwrap();
    assert_eq!(after.state, SandboxState::Suspended);
    assert_eq!(after.execution_metadata, Some(producer));
    assert_eq!(after.cleanup_completion, None);
    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(Duration::from_secs(20), service.task_tracker.wait())
        .await
        .unwrap();
}

#[test]
fn restore_guest_memory_uses_capture_shape_or_legacy_payload_and_rejects_conflict() {
    use pplx_space_common_core::types::MemorySize;
    for (captured, file, expected) in [
        (None, None, Ok(None)),
        (None, Some(2048), Ok(Some(2048))),
        (Some(4096), None, Ok(Some(4096))),
        (Some(4096), Some(4096), Ok(Some(4096))),
        (Some(4096), Some(2048), Err(tonic::Code::FailedPrecondition)),
    ] {
        let captured = captured.map(|mib| MemorySize::mib(mib).unwrap());
        assert_eq!(
            super::restored_guest_memory_mib(captured, file).map_err(|e| e.code()),
            expected
        );
    }
}
