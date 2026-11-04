use pplx_space_common_core::types::SandboxId;

use super::*;
use crate::service::activation_support::internal_request;

#[test]
fn base_snapshot_staging_preserves_cpu_evidence_without_inventing_it() {
    use pplx_space_common_core::types::{
        Arch, ExecutionMetadata, SnapshotExecutionMetadata, VcpuCount,
    };

    let cpu = |count| VcpuCount::try_from_u32(count).unwrap();
    for (sidecar, selected_cpu, expected_cpu) in [
        (None, Some(8), Some(8)),
        (Some(None), Some(8), Some(8)),
        (Some(Some(8)), None, Some(8)),
        (None, None, None),
    ] {
        let source = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        if let Some(budget) = sidecar {
            let execution = SnapshotExecutionMetadata::new(ExecutionMetadata::initial_firecracker(
                Arch::Aarch64,
                vec![],
            ))
            .unwrap()
            .with_cpu_budget(budget.map(cpu));
            std::fs::write(
                source.path().join("metadata.json"),
                serde_json::to_vec(&execution).unwrap(),
            )
            .unwrap();
        }
        stage_base_snapshot_runtime_metadata(source.path(), target.path(), selected_cpu.map(cpu))
            .unwrap();
        let staged: SnapshotExecutionMetadata =
            serde_json::from_slice(&std::fs::read(target.path().join("runtime.json")).unwrap())
                .unwrap();
        assert_eq!(staged.cpu_budget(), expected_cpu.map(cpu));
    }

    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("metadata.json"), b"{broken").unwrap();
    assert_eq!(
        stage_base_snapshot_runtime_metadata(source.path(), target.path(), Some(cpu(8)))
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition,
    );
    assert!(!target.path().join("runtime.json").exists());
}

const MISSING_TEMPLATE_BUILD: &str =
    "build not found: app-runtime-restricted-automations (tag=latest (default))";
const INTERNAL_TEMPLATE_ERROR: &str = "template DB mutex poisoned";
const RETRYABLE_TEMPLATE_ERROR: &str = "local template database is unavailable";

fn wrapped_get_template_error(code: tonic::Code, message: &str) -> Status {
    Status::new(code, format!("GetTemplate failed: {message}"))
}

#[test]
fn start_template_not_found_is_a_terminal_sandbox_precondition() {
    let error = start_template_resolution_failed_status(wrapped_get_template_error(
        tonic::Code::NotFound,
        MISSING_TEMPLATE_BUILD,
    ));

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().starts_with("GetTemplate failed:"));
    assert!(error.message().contains(MISSING_TEMPLATE_BUILD));
    assert_eq!(error.message().matches("GetTemplate failed:").count(), 1);
}

#[test]
fn start_template_internal_failure_remains_internal() {
    let error = start_template_resolution_failed_status(wrapped_get_template_error(
        tonic::Code::Internal,
        INTERNAL_TEMPLATE_ERROR,
    ));

    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(error.message().starts_with("GetTemplate failed:"));
    assert!(error.message().contains(INTERNAL_TEMPLATE_ERROR));
}

#[test]
fn start_template_retryable_failure_remains_unavailable() {
    let error = start_template_resolution_failed_status(wrapped_get_template_error(
        tonic::Code::Unavailable,
        RETRYABLE_TEMPLATE_ERROR,
    ));

    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(error.message().starts_with("GetTemplate failed:"));
    assert!(error.message().contains(RETRYABLE_TEMPLATE_ERROR));
}

#[test]
fn pool_member_create_options_reject_incompatible_flags() {
    // Non-pool creates accept any combination.
    validate_pool_member_create_options(None, false, false).expect("plain create");
    validate_pool_member_create_options(None, true, false).expect("snapshotting create");
    validate_pool_member_create_options(None, false, true).expect("encrypted create");
    // Plain pool member is fine.
    validate_pool_member_create_options(Some("seed"), false, false).expect("pool member");
    // Pool members can neither snapshot nor run encrypted.
    let snapshots = validate_pool_member_create_options(Some("seed"), true, false)
        .expect_err("pool member with snapshots_enabled must be rejected");
    assert_eq!(snapshots.code(), tonic::Code::InvalidArgument);
    let encrypted = validate_pool_member_create_options(Some("seed"), false, true)
        .expect_err("pool member with volume_encryption must be rejected");
    assert_eq!(encrypted.code(), tonic::Code::InvalidArgument);
    assert!(
        encrypted.message().contains("volume_encryption"),
        "rejection must name the offending option: {}",
        encrypted.message()
    );
}

#[tokio::test]
async fn create_rejects_invalid_intent_before_resolving_a_template() {
    use crate::service::tests::service_tests::service_for_teardown_test;

    let service = service_for_teardown_test(crate::db::test_conn());
    let base = CreateRequest {
        template: "seed".to_string(),
        owner: "user:00000000-0000-0000-0000-000000000001".to_string(),
        runtime: "fc".to_string(),
        boot_mode: BootMode::Pool as i32,
        ..Default::default()
    };
    for (field, request, code) in [
        (
            "boot_mode",
            CreateRequest {
                boot_mode: 99,
                ..base.clone()
            },
            tonic::Code::InvalidArgument,
        ),
        (
            "runtime",
            CreateRequest {
                runtime: String::new(),
                ..base.clone()
            },
            tonic::Code::InvalidArgument,
        ),
        (
            "runtime",
            CreateRequest {
                runtime: "gvisor".to_string(),
                ..base.clone()
            },
            tonic::Code::Unimplemented,
        ),
        (
            "sandbox_id",
            CreateRequest {
                sandbox_id: Some("invalid".to_string()),
                ..base.clone()
            },
            tonic::Code::InvalidArgument,
        ),
        (
            "rootfs_size_mb",
            CreateRequest {
                rootfs_size_mb: Some(0),
                ..base.clone()
            },
            tonic::Code::InvalidArgument,
        ),
        (
            "disk I/O",
            CreateRequest {
                disk_io_max_operations_per_sec: Some(1),
                ..base.clone()
            },
            tonic::Code::InvalidArgument,
        ),
        (
            "rootfs_size_mb",
            CreateRequest {
                rootfs_size_mb: Some(128 * 1024 + 1),
                ..base.clone()
            },
            tonic::Code::InvalidArgument,
        ),
    ] {
        let error = service.create(internal_request(request)).await.unwrap_err();
        assert_eq!(error.code(), code, "{field}: {error}");
        assert!(error.message().contains(field), "{field}: {error}");
    }
    assert!(
        crate::db::list_sandboxes(&service.db.lock().unwrap())
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn unknown_mutation_enums_are_rejected_before_lookup_or_io() {
    use crate::service::tests::service_tests::service_for_teardown_test;
    let service = service_for_teardown_test(crate::db::test_conn());
    let sandbox_id = SandboxId::new().to_string();
    for value in [-1, 99, i32::MAX] {
        let results = [
            service
                .start(internal_request(StartRequest {
                    sandbox_id: sandbox_id.clone(),
                    target_state: value,
                    ..Default::default()
                }))
                .await
                .map(|_| ()),
            service
                .wake(internal_request(WakeRequest {
                    implicit_wake_source: 0,
                    sandbox_id: sandbox_id.clone(),
                    target_state: value,
                    ..Default::default()
                }))
                .await
                .map(|_| ()),
            service
                .create_snapshot(internal_request(crate::proto::CreateSnapshotRequest {
                    sandbox_id: sandbox_id.clone(),
                    r#type: value,
                    ..Default::default()
                }))
                .await
                .map(|_| ()),
            service
                .suspend(internal_request(SuspendRequest {
                    sandbox_id: sandbox_id.clone(),
                    format: value,
                    ..Default::default()
                }))
                .await
                .map(|_| ()),
        ];
        for (operation, result) in ["start", "wake", "snapshot", "suspend"]
            .into_iter()
            .zip(results)
        {
            assert_eq!(
                result.unwrap_err().code(),
                tonic::Code::InvalidArgument,
                "{operation}: {value}"
            );
        }
    }
    assert!(
        crate::db::list_sandboxes(&service.db.lock().unwrap())
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn create_intent_preserves_immutables_independently_of_pool_occupancy() {
    let id = Uuid::now_v7();
    let base = CreateRequest {
        runtime: "fc".to_string(),
        template: "seed".to_string(),
        ..Default::default()
    };
    let cases = [
        ("ordinary", base.clone(), false),
        (
            "sandbox_id",
            CreateRequest {
                sandbox_id: Some(id.to_string()),
                ..base.clone()
            },
            true,
        ),
        (
            "volume_encryption",
            CreateRequest {
                volume_encryption: true,
                ..base.clone()
            },
            true,
        ),
        (
            "snapshots_enabled",
            CreateRequest {
                snapshots_enabled: true,
                ..base.clone()
            },
            true,
        ),
        (
            "disk I/O",
            CreateRequest {
                disk_io_max_operations_per_sec: Some(1000),
                disk_io_max_bandwidth_mib_per_sec: Some(100),
                ..base.clone()
            },
            true,
        ),
        (
            "rootfs_size_mb",
            CreateRequest {
                rootfs_size_mb: Some(4096),
                ..base.clone()
            },
            true,
        ),
        (
            "pool_template",
            CreateRequest {
                pool_template: Some("seed".to_string()),
                ..base
            },
            true,
        ),
    ];
    let snapshot = tempfile::tempdir().unwrap();
    std::fs::write(snapshot.path().join("vmstate.base"), b"vmstate").unwrap();
    std::fs::write(snapshot.path().join("snapshot.base"), b"memory").unwrap();
    for populated_pool in [true, false] {
        let conn = crate::db::test_conn();
        if populated_pool {
            let donor = SandboxId::new();
            crate::db::insert_sandbox(
                &conn,
                crate::db::InsertSandboxParams {
                    guest_image: Default::default(),
                    id: &donor.to_string(),
                    uuid8: &SandboxRuntimeLabel::from_sandbox_id(donor),
                    state: SandboxState::Paused,
                    template: "seed",
                    template_hash: None,
                    user: &SandboxOwner::parse("user").unwrap(),
                    runtime: "fc",
                    snapshots_enabled: false,
                    rootfs_size_mb: None,
                    disk_io_limits: Default::default(),
                    pool_template: Some("seed"),
                    volume_encryption: false,
                    process_env: &BTreeMap::new(),
                    auto_lifecycle: &AutoLifecyclePolicy::disabled(),
                    network_policy_override: None,
                    network_policy_generation: 0,
                    network_policy_authority: crate::db::PolicyAuthority::Cluster,
                    auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
                },
            )
            .unwrap();
        }
        let service = SandboxManagerService::for_test(conn);
        let legacy_execution =
            pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
                pplx_space_common_core::types::Arch::X86_64,
                vec![],
            );
        for has_snapshot in [false, true] {
            let snapshot_dir = has_snapshot.then(|| snapshot.path().to_str().unwrap());
            for (field, request, incompatible_with_pool) in &cases {
                for requested_mode in [
                    BootMode::Unspecified,
                    BootMode::Cold,
                    BootMode::BaseSnapshot,
                    BootMode::Pool,
                ] {
                    let request = CreateRequest {
                        boot_mode: requested_mode as i32,
                        ..request.clone()
                    };
                    let (requested_mode, requested_id) = validate_create_request(&request).unwrap();
                    let intent = service.resolve_create_intent(
                        &request,
                        requested_mode,
                        requested_id,
                        CreateTarget {
                            snapshot_dir,
                            execution: &legacy_execution,
                            selected: false,
                        },
                    );
                    let context = format!(
                        "{field}: {requested_mode:?}, pool={populated_pool}, snapshot={has_snapshot}"
                    );
                    if request.rootfs_size_mb.is_some() && requested_mode == BootMode::BaseSnapshot
                    {
                        assert_eq!(
                            intent.unwrap_err().code(),
                            tonic::Code::InvalidArgument,
                            "{context}"
                        );
                        continue;
                    }
                    let expected = if request.rootfs_size_mb.is_some() {
                        BootMode::Cold
                    } else if requested_mode == BootMode::Pool
                        && populated_pool
                        && !incompatible_with_pool
                    {
                        BootMode::Pool
                    } else if matches!(requested_mode, BootMode::Pool | BootMode::BaseSnapshot)
                        && has_snapshot
                    {
                        BootMode::BaseSnapshot
                    } else {
                        BootMode::Cold
                    };
                    let (actual, intent) =
                        intent.unwrap_or_else(|error| panic!("{context}: {error}"));
                    assert_eq!(actual, expected, "{context}");
                    match intent {
                        SandboxCreateIntent::Create {
                            requested_id: actual,
                        } => {
                            assert_ne!(expected, BootMode::Pool, "{context}");
                            assert_eq!(actual, requested_id, "{context}");
                        }
                        SandboxCreateIntent::AdoptPool => {
                            assert_eq!(expected, BootMode::Pool, "{context}")
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn restore_provenance_classifies_fresh_boots() {
    let provenance = classify_restore_provenance("fresh", None, None, None);

    assert_eq!(provenance, SandboxRestoreProvenance::Fresh);
}

#[test]
fn restore_provenance_classifies_local_restore() {
    let provenance = classify_restore_provenance(
        "snapshot",
        None,
        None,
        Some("019ee1df-0c89-76f0-93c2-2b9c360a9aee"),
    );

    assert_eq!(provenance, SandboxRestoreProvenance::LocalOnly);
}

#[test]
fn restore_provenance_classifies_matching_remote_restore() {
    let artifact_id = "019ee1df-0c89-76f0-93c2-2b9c360a9aee";
    let provenance = classify_restore_provenance(
        "snapshot",
        Some("s3://bucket/suspend_snapshots/00/sandbox/snapshot"),
        Some(artifact_id),
        Some(artifact_id),
    );

    assert_eq!(provenance, SandboxRestoreProvenance::RemoteMatchesLocal);
}

#[test]
fn restore_provenance_classifies_remote_local_mismatch() {
    let provenance = classify_restore_provenance(
        "snapshot",
        Some("s3://bucket/suspend_snapshots/00/sandbox/snapshot"),
        Some("019ee1df-0c89-76f0-93c2-2b9c360a9aee"),
        Some("019ee11a-79bf-7d70-a05f-7786f6f62d10"),
    );

    assert_eq!(provenance, SandboxRestoreProvenance::RemoteLocalMismatch);
}

#[test]
fn restore_provenance_classifies_legacy_remote_metadata() {
    let provenance = classify_restore_provenance(
        "snapshot",
        Some("s3://bucket/suspend_snapshots/00/sandbox/snapshot"),
        None,
        Some("019ee1df-0c89-76f0-93c2-2b9c360a9aee"),
    );

    assert_eq!(provenance, SandboxRestoreProvenance::RemoteMetadataUnknown);
}

#[tokio::test]
async fn existing_sandbox_cannot_resolve_a_missing_artifact_by_latest_alias() {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(
        &conn,
        &id,
        crate::state::SandboxState::Stopped,
    );
    let sandbox = crate::db::get_sandbox(&conn, &id).unwrap().unwrap();
    // This must reject locally rather than asking template-manager to pick
    // the newest build after the sandbox lost its identity.
    let templates = crate::template_resolver::fake::FakeTemplateResolver::refusing(
        tonic::Status::not_found("a latest-alias lookup must not be sent"),
    );
    let error = resolve_template_paths_for_start(
        &templates,
        pplx_space_template_mgr_client::proto::TemplateRef {
            name: "app-runtime".into(),
            content_hash: None,
            tag: None,
        },
        &sandbox,
    )
    .await
    .err()
    .expect("missing identity cannot select a replacement artifact");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("saved template hash"));
    assert!(templates.requests().is_empty());
}

#[tokio::test]
async fn retained_rootfs_inspection_errors_defer_recovery_without_selecting_template() {
    use crate::node_manager_transport::fake;
    use crate::state::SandboxState;
    use crate::template_resolver::fake::FakeTemplateResolver;
    #[derive(Clone, Copy, Debug)]
    enum Failure {
        NotDirectory,
        SymlinkLoop,
    }
    for failure in [Failure::NotDirectory, Failure::SymlinkLoop] {
        let cache = tempfile::tempdir().unwrap();
        let id = SandboxId::new().to_string();
        let sibling = SandboxId::new().to_string();
        let conn = crate::db::test_conn();
        for sandbox in [&id, &sibling] {
            super::super::tests::service_tests::insert_test_sandbox(
                &conn,
                sandbox,
                SandboxState::Starting,
            );
        }
        let row = crate::db::get_sandbox(&conn, &id).unwrap().unwrap();
        assert!(
            row.execution_metadata.is_none(),
            "legacy record must remain unbound"
        );
        let root = cache.path().join(&id);
        std::fs::create_dir(&root).unwrap();
        let instance = root.join("instance");
        match failure {
            Failure::NotDirectory => std::fs::write(&instance, b"invalid instance").unwrap(),
            Failure::SymlinkLoop => std::os::unix::fs::symlink("instance", &instance).unwrap(),
        }
        sandbox_rootfs_exists_in(cache.path(), &id)
            .expect_err("an invalid instance must remain an inspection error");
        let sibling_instance = cache.path().join(&sibling).join("instance");
        std::fs::create_dir_all(&sibling_instance).unwrap();
        std::fs::write(sibling_instance.join("rootfs.ext4"), b"sibling disk").unwrap();
        let templates =
            FakeTemplateResolver::refusing(Status::not_found("must not consult template"));
        let node = fake::unbound(cache.path().join("unused.sock").as_path());
        let error = template_source_for_sandbox_prepare_in(&node, &templates, &row, cache.path())
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(templates.requests().is_empty());
        let service = super::super::SandboxManagerService::for_test(conn);
        let records = service
            .recover_prelaunch_startup_states_in(cache.path(), &Default::default())
            .unwrap_or_else(|error| panic!("{failure:?}: {error}"));
        assert_eq!(records.len(), 2);
        for record in records {
            let expected = if record.id == id {
                crate::db::StartupRecoveryOutcome::Deferred
            } else {
                assert_eq!(record.id, sibling);
                crate::db::StartupRecoveryOutcome::State(SandboxState::Stopped)
            };
            assert_eq!(record.outcome, expected, "{failure:?}");
        }
        let deferred = service.lookup_sandbox(&id).unwrap();
        assert_eq!(deferred.state, SandboxState::Starting);
        assert_eq!(deferred.execution_metadata, row.execution_metadata);
        // Repair only the invalid fixture, then retry the same startup pass.
        std::fs::remove_file(&instance).unwrap();
        std::fs::create_dir(&instance).unwrap();
        std::fs::write(instance.join("rootfs.ext4"), b"retained disk").unwrap();
        let records = service
            .recover_prelaunch_startup_states_in(cache.path(), &Default::default())
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].outcome,
            crate::db::StartupRecoveryOutcome::State(SandboxState::Stopped)
        );
        assert_eq!(
            std::fs::read(instance.join("rootfs.ext4")).unwrap(),
            b"retained disk"
        );
    }
}

#[tokio::test]
async fn retained_instance_permission_recovery_and_repair_preserve_disk() {
    use crate::node_manager_transport::fake::FakeNodeManager;
    use crate::template_resolver::fake::FakeTemplateResolver;
    use pplx_space_common_core::types::{Arch, SandboxArtifactMetadata};
    use pplx_space_node_mgr_client::proto::{
        PrepareSandboxArtifactDirRequest, PrepareSandboxArtifactDirResponse,
    };
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;

    // EACCES must be exercised without root's DAC override. The child creates
    // all fixtures itself and dials only its own fake node-manager socket.
    if nix::unistd::geteuid().is_root() {
        let test = std::thread::current().name().unwrap().to_owned();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(test)
            .arg("--nocapture")
            .uid(65534)
            .gid(65534)
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    struct RestoreTraversal(std::path::PathBuf);
    impl Drop for RestoreTraversal {
        fn drop(&mut self) {
            if let Err(error) =
                std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o700))
            {
                eprintln!("restore test fixture traversal: {error}");
            }
        }
    }
    let execution =
        pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap()[1]
            .definition()
            .execution_requirements()
            .unwrap();
    for bound_record in [false, true] {
        for repairs in [false, true] {
            let cache = tempfile::tempdir().unwrap();
            let id = SandboxId::new().to_string();
            let conn = crate::db::test_conn();
            super::super::tests::service_tests::insert_test_sandbox(
                &conn,
                &id,
                crate::state::SandboxState::Starting,
            );
            if bound_record {
                crate::db::upsert_sandbox_execution_metadata(&conn, &id, &execution).unwrap();
            }
            let row = crate::db::get_sandbox(&conn, &id).unwrap().unwrap();
            let sibling = SandboxId::new().to_string();
            super::super::tests::service_tests::insert_test_sandbox(
                &conn,
                &sibling,
                crate::state::SandboxState::Starting,
            );
            let sibling_instance = cache.path().join(&sibling).join("instance");
            std::fs::create_dir_all(&sibling_instance).unwrap();
            std::fs::write(sibling_instance.join("rootfs.ext4"), b"sibling disk").unwrap();
            let service = super::super::SandboxManagerService::for_test(conn);
            let root = cache.path().join(&id);
            let instance = root.join("instance");
            std::fs::create_dir_all(&instance).unwrap();
            std::fs::write(instance.join("rootfs.ext4"), b"retained disk").unwrap();
            let metadata = SandboxArtifactMetadata::new(
                id.parse().unwrap(),
                execution.clone(),
                "seed".parse().unwrap(),
                None,
            );
            std::fs::write(
                root.join("metadata.json"),
                serde_json::to_vec(&metadata).unwrap(),
            )
            .unwrap();
            std::fs::set_permissions(&instance, std::fs::Permissions::from_mode(0o000)).unwrap();
            let _restore_traversal = RestoreTraversal(instance.clone());
            assert_eq!(
                sandbox_rootfs_exists_in(cache.path(), &id)
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::PermissionDenied
            );
            let recover = || {
                service
                    .recover_prelaunch_startup_states_in(cache.path(), &Default::default())
                    .unwrap()
            };
            let records = recover();
            assert_eq!(records.len(), 2);
            for record in records {
                let expected = if record.id == id {
                    crate::db::StartupRecoveryOutcome::Deferred
                } else {
                    assert_eq!(record.id, sibling);
                    crate::db::StartupRecoveryOutcome::State(crate::state::SandboxState::Stopped)
                };
                assert_eq!(record.outcome, expected);
            }
            let deferred = service.lookup_sandbox(&id).unwrap();
            assert_eq!(deferred.state, crate::state::SandboxState::Starting);
            assert_eq!(deferred.execution_metadata, row.execution_metadata);
            let repair_dir = instance.clone();
            let expected_id = id.clone();
            let node = FakeNodeManager::default()
                .unary(
                    "PrepareSandboxArtifactDir",
                    move |request: PrepareSandboxArtifactDirRequest| {
                        assert_eq!(request.sandbox_id, expected_id);
                        if repairs {
                            std::fs::set_permissions(
                                &repair_dir,
                                std::fs::Permissions::from_mode(0o2710),
                            )
                            .unwrap();
                        }
                        Ok(PrepareSandboxArtifactDirResponse {})
                    },
                )
                .serve();
            let templates = FakeTemplateResolver::refusing(Status::not_found(
                "changed template must not be consulted",
            ));
            let result = template_source_for_sandbox_prepare_in(
                &node.connector(),
                &templates,
                &row,
                cache.path(),
            )
            .await;
            let records = recover();
            assert_eq!(records.len(), 1);
            let expected = if repairs {
                crate::db::StartupRecoveryOutcome::State(crate::state::SandboxState::Stopped)
            } else {
                crate::db::StartupRecoveryOutcome::Deferred
            };
            assert_eq!(records[0].outcome, expected);
            // Restore fixture ownership access even when an old node did not repair it.
            std::fs::set_permissions(&instance, std::fs::Permissions::from_mode(0o700)).unwrap();
            assert_eq!(node.calls(), ["PrepareSandboxArtifactDir"]);
            assert!(templates.requests().is_empty());
            if repairs {
                let source = result.unwrap();
                assert!(source.existing_artifact);
                assert_eq!(source.execution, execution);
                assert_eq!(
                    std::fs::read(instance.join("rootfs.ext4")).unwrap(),
                    b"retained disk"
                );
            } else {
                assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
            }
        }
    }
}

#[test]
fn template_provenance_does_not_overwrite_a_selected_cold_boot_consumer() {
    use pplx_space_common_core::types::Arch;
    use pplx_space_node_runtime_core::catalog::CompiledEnvironment;
    let catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let source = catalog[0].definition().execution_requirements().unwrap();
    let selected = catalog
        .iter()
        .find(|entry| {
            entry.release().release.version.as_str() == "1.17.0"
                && entry
                    .kernel()
                    .is_some_and(|kernel| kernel.version.as_str() == "6.1.155")
        })
        .unwrap()
        .definition()
        .execution_requirements()
        .unwrap();
    assert_ne!(source.runtime(), selected.runtime());
    assert_eq!(
        bind_disk_execution(Some(&selected), source.clone()).unwrap(),
        selected
    );
    let other_kernel = catalog
        .iter()
        .find(|entry| {
            entry
                .kernel()
                .is_some_and(|kernel| kernel.version.as_str() == "6.18.44")
        })
        .unwrap()
        .definition()
        .execution_requirements()
        .unwrap();
    bind_disk_execution(Some(&selected), other_kernel).unwrap_err();
    assert_eq!(bind_disk_execution(None, source.clone()).unwrap(), source);
}

#[rstest::rstest]
#[case::missing(None)]
#[case::wrong(Some(TemplateLookupScope::Cluster))]
#[case::matching(Some(TemplateLookupScope::LocalExecution))]
#[tokio::test]
async fn template_resolver_accepts_legacy_or_matching_scope_receipt(
    #[case] acknowledgement: Option<TemplateLookupScope>,
) {
    use crate::template_resolver::fake::FakeTemplateResolver;
    use pplx_space_template_mgr_client::proto::{GetTemplateResponse, TemplateRef};

    let directory = tempfile::tempdir().unwrap();
    let template_dir = directory.path().to_str().unwrap().to_owned();
    let scope = TemplateLookupScope::LocalExecution;
    let response_dir = template_dir.clone();
    let templates = FakeTemplateResolver::new(move || {
        let mut response = tonic::Response::new(GetTemplateResponse {
            template_dir: response_dir.clone(),
            arch: "x86_64".into(),
            network_policy_authority: pplx_space_net_common::proto::PolicyAuthority::Local as i32,
            auto_lifecycle_authority: pplx_space_net_common::proto::PolicyAuthority::Local as i32,
            ..Default::default()
        });
        if let Some(acknowledgement) = acknowledgement {
            acknowledgement.receipt(&mut response);
        }
        Ok(response)
    });

    let result = resolve_template_paths(
        &templates,
        TemplateRef {
            name: "app-runtime".into(),
            ..Default::default()
        },
        scope,
    )
    .await;

    if acknowledgement.is_none() || acknowledgement == Some(scope) {
        assert_eq!(result.unwrap().template_dir, template_dir);
    } else {
        let error = result.err().expect("unacknowledged scope must be rejected");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("scope receipt mismatch"));
    }
    let lookups = templates.lookups();
    assert_eq!(lookups.len(), 1);
    let mut request = tonic::Request::new(lookups[0].request.clone());
    *request.metadata_mut() = lookups[0].metadata.clone();
    assert_eq!(
        TemplateLookupScope::from_request(&request).unwrap(),
        Some(scope)
    );
}
