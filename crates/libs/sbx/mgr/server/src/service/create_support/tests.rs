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
                        snapshot_dir,
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
    )
    .await
    .err()
    .expect("missing identity cannot select a replacement artifact");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("saved template hash"));
    assert!(templates.requests().is_empty());
}
