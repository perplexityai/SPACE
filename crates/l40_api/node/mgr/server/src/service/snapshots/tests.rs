use super::{
    ENC_SNAPSHOT_JAIL_SUBDIR, ENC_SNAPSHOT_MEMORY_NAME, ENC_SNAPSHOT_VMSTATE_NAME,
    pin_encrypted_snapshot_bind, reown_encrypted_snapshot_inputs, restore_instance_snapshot_in,
    set_snapshot_export_modes, validate_cache_artifact_path,
};
use crate::proto::node_manager_server::NodeManager as _;
use std::os::unix::fs::PermissionsExt;

#[test]
fn snapshot_identity_inventory_survives_failed_or_exhausted_measurements() {
    let root = tempfile::tempdir().unwrap();
    let snapshots = root.path().join("snapshots");
    std::fs::create_dir(&snapshots).unwrap();
    for name in [
        "d-1m-001",
        "d-1m-002",
        "d-1m-003",
        "f-20260921T120000Z",
        "invalid",
    ] {
        std::fs::create_dir(snapshots.join(name)).unwrap();
    }
    for budget in [
        std::time::Duration::ZERO,
        std::time::Duration::from_secs(30),
    ] {
        let (paths, token, _) = super::super::snapshot_inventory::SnapshotInventory::default()
            .page(super::SandboxId::new(), root.path(), "")
            .unwrap();
        assert!(token.is_empty());
        let inventory = super::measure_snapshots(paths, budget, |path, _| {
            assert!(
                !budget.is_zero(),
                "exhausted or skipped measurement must not start work"
            );
            if path.ends_with("001") {
                Err("measurement timed out".into())
            } else if path.ends_with("002") {
                Ok(None)
            } else {
                Ok(Some(super::BtrfsDiskUsage {
                    exclusive_bytes: 0,
                    total_bytes: 12,
                }))
            }
        });
        assert_eq!(
            inventory
                .iter()
                .map(|snapshot| snapshot.name.as_str())
                .collect::<Vec<_>>(),
            ["d-1m-001", "d-1m-002", "d-1m-003", "f-20260921T120000Z"]
        );
        assert!(inventory[0].exclusive_bytes.is_none());
        assert!(inventory[1].exclusive_bytes.is_none());
        assert_eq!(
            inventory[2].exclusive_bytes,
            (!budget.is_zero()).then_some(0)
        );
        assert_eq!(
            inventory[2].referenced_bytes,
            (!budget.is_zero()).then_some(12)
        );
    }
    let invalid_root = root.path().join("invalid-root");
    std::fs::create_dir(&invalid_root).unwrap();
    std::fs::write(invalid_root.join("snapshots"), []).unwrap();
    let error = super::super::snapshot_inventory::SnapshotInventory::default()
        .page(super::SandboxId::new(), &invalid_root, "")
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
}

#[test]
fn plaintext_load_rejects_replaced_source_and_safely_retries_existing_destination() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let source = root.join("snapshot");
    let jail = root.join("jail");
    std::fs::create_dir(&source).unwrap();
    std::fs::create_dir(&jail).unwrap();
    std::fs::write(source.join("memory"), b"memory").unwrap();
    std::fs::write(source.join("vmstate"), b"snapshot").unwrap();
    let victim = root.join("victim");
    std::fs::write(&victim, b"private").unwrap();
    // Replace an input after the RPC's preliminary pathname validation.
    std::fs::rename(source.join("vmstate"), source.join("original")).unwrap();
    for hard in [false, true] {
        if hard {
            std::fs::hard_link(&victim, source.join("vmstate")).unwrap();
        } else {
            symlink(&victim, source.join("vmstate")).unwrap();
        }
        super::stage_plaintext_snapshot_inputs(&source, &jail).unwrap_err();
        assert!(!jail.join("vmstate").exists());
        std::fs::remove_file(source.join("vmstate")).unwrap();
    }
    std::fs::rename(source.join("original"), source.join("vmstate")).unwrap();
    for destination in ["absent", "regular", "symlink", "hardlink"] {
        if jail.join("vmstate").symlink_metadata().is_ok() {
            std::fs::remove_file(jail.join("vmstate")).unwrap();
        }
        match destination {
            "regular" => std::fs::write(jail.join("vmstate"), b"old").unwrap(),
            "symlink" => symlink(&victim, jail.join("vmstate")).unwrap(),
            "hardlink" => std::fs::hard_link(&victim, jail.join("vmstate")).unwrap(),
            _ => {}
        }
        super::stage_plaintext_snapshot_inputs(&source, &jail).unwrap();
        super::stage_plaintext_snapshot_inputs(&source, &jail).unwrap();
        assert_eq!(std::fs::read(jail.join("vmstate")).unwrap(), b"snapshot");
        assert_eq!(std::fs::read(&victim).unwrap(), b"private");
    }
}

#[test]
fn disk_snapshot_source_is_readable_by_the_shared_cache_group() {
    let temp = tempfile::tempdir().unwrap();
    let instance = temp.path().canonicalize().unwrap().join("instance");
    std::fs::create_dir(&instance).unwrap();
    let rootfs = instance.join("rootfs.ext4");
    std::fs::write(&rootfs, b"disk").unwrap();
    std::fs::set_permissions(&instance, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::set_permissions(&rootfs, std::fs::Permissions::from_mode(0o600)).unwrap();

    set_snapshot_export_modes(&instance).unwrap();

    assert_eq!(
        std::fs::metadata(&instance).unwrap().permissions().mode() & 0o770,
        0o770
    );
    assert_eq!(
        std::fs::metadata(&rootfs).unwrap().permissions().mode() & 0o660,
        0o660
    );
}

/// Validate both restored inputs before changing any ownership. A jailed
/// writer must not turn the privileged ownership transfer into a link escape.
#[test]
fn encrypted_load_rejects_links_before_changing_ownership() {
    use std::os::unix::fs::{MetadataExt, symlink};
    let temp = tempfile::tempdir().unwrap();
    let victim = temp.path().join("victim");
    std::fs::write(&victim, b"outside").unwrap();
    let original = std::fs::metadata(&victim).unwrap();
    for name in [ENC_SNAPSHOT_VMSTATE_NAME, ENC_SNAPSHOT_MEMORY_NAME] {
        for hard in [false, true] {
            let volume = tempfile::tempdir_in(temp.path()).unwrap();
            for input in [ENC_SNAPSHOT_VMSTATE_NAME, ENC_SNAPSHOT_MEMORY_NAME] {
                let path = volume.path().join(input);
                if input != name {
                    std::fs::write(path, b"snapshot").unwrap();
                } else if hard {
                    std::fs::hard_link(&victim, path).unwrap();
                } else {
                    symlink(&victim, path).unwrap();
                }
            }
            let pinned = std::fs::File::open(volume.path()).unwrap();
            let error = reown_encrypted_snapshot_inputs(&pinned, original.uid()).unwrap_err();
            assert!(
                error
                    .message()
                    .contains(if hard { "multiply linked" } else { "ELOOP" }),
                "{error}"
            );
            let after = std::fs::metadata(&victim).unwrap();
            assert_eq!(
                (after.uid(), after.gid(), after.mode()),
                (original.uid(), original.gid(), original.mode())
            );
        }
    }
}

#[test]
fn encrypted_bind_target_is_pinned_inside_the_jail() {
    use std::os::unix::fs::{MetadataExt, symlink};

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let (volume, jail, outside) = (root.join("volume"), root.join("jail"), root.join("outside"));
    for dir in [&volume, &jail, &outside] {
        std::fs::create_dir(dir).unwrap();
    }
    let target = jail.join(ENC_SNAPSHOT_JAIL_SUBDIR);
    symlink(&outside, &target).unwrap();

    // Suspend and restore share this pin; neither may follow the planted link.
    let mut reowned = false;
    let error = pin_encrypted_snapshot_bind(&volume, &jail, |_, _| {
        reowned = true;
        Ok(())
    })
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(!reowned, "volume re-owned before its mountpoint was pinned");
    assert!(std::fs::read_dir(&outside).unwrap().next().is_none());

    std::fs::remove_file(&target).unwrap();
    let (pinned_volume, pinned_target) =
        pin_encrypted_snapshot_bind(&volume, &jail, |_, _| Ok(())).unwrap();
    assert_eq!(
        pinned_volume.metadata().unwrap().ino(),
        std::fs::metadata(&volume).unwrap().ino()
    );
    let created = std::fs::symlink_metadata(&target).unwrap();
    assert!(created.is_dir());
    assert_eq!(pinned_target.metadata().unwrap().ino(), created.ino());
    // A retry reuses the existing mountpoint.
    let (_, retried) = pin_encrypted_snapshot_bind(&volume, &jail, |_, _| Ok(())).unwrap();
    assert_eq!(retried.metadata().unwrap().ino(), created.ino());
}

fn cache_root() -> &'static str {
    pplx_space_host_layout::paths::SPACE_CACHE_DIR
}

#[test]
fn accepts_paths_strictly_inside_the_cache_root() {
    let root = super::CACHE_DIR;
    let id = uuid::Uuid::now_v7();
    for suffix in [
        format!("{id}/snapshot"),
        // GC staging, in the fixed-length form and the earlier long form.
        format!("{id}/.gc-pending-{id}"),
        format!("{id}/snapshots/.gc-pending-{id}"),
        format!("{id}/snapshot.gc-pending-{id}"),
        format!("{id}/snapshots/name.gc-pending-x"),
        // Sandbox-manager retires superseded snapshots here for the sweep.
        format!("{id}/snapshots/.suspend-gc-pending-{id}"),
        format!(".fork-copies/{id}"),
    ] {
        validate_cache_artifact_path(&format!("{root}/{suffix}")).unwrap();
    }
    for path in [
        format!("{root}/{id}"),
        format!("{root}/{id}/instance"),
        format!("{root}/{id}/gc-pending-{id}"),
        format!("{root}/.fork-copies"),
        format!("{}/firecracker/templates/name/hash", cache_root()),
    ] {
        assert!(validate_cache_artifact_path(&path).is_err());
    }
}

/// `<root>/` and `<root>/.` both resolve to the cache root
/// itself — deleting it would wipe every cached artifact on the
/// node. The validator must reject anything that doesn't name an
/// entry strictly inside the root.
#[test]
fn rejects_the_cache_root_itself() {
    let root = cache_root();
    assert!(validate_cache_artifact_path(root).is_err());
    assert!(validate_cache_artifact_path(&format!("{root}/")).is_err());
    assert!(validate_cache_artifact_path(&format!("{root}/.")).is_err());
}

#[test]
fn rejects_traversal_and_degenerate_components() {
    let root = cache_root();
    assert!(validate_cache_artifact_path(&format!("{root}/foo/../bar")).is_err());
    assert!(validate_cache_artifact_path(&format!("{root}/foo/..")).is_err());
    assert!(validate_cache_artifact_path(&format!("{root}/./foo")).is_err());
    assert!(validate_cache_artifact_path(&format!("{root}//foo")).is_err());
    assert!(validate_cache_artifact_path(&format!("{root}/foo/")).is_err());
    assert!(validate_cache_artifact_path("/other/root/foo").is_err());
}

#[test]
fn snapshot_dispatch_uses_the_recorded_boot_contract() {
    use pplx_space_cpu_arch::Arch;
    use pplx_space_node_runtime_core::{boot::BootSource, catalog::CompiledEnvironment};
    let catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    for (name, expected) in [
        ("e0", None),
        ("ch-54.0-linux-7.2.8", Some(BootSource::LinuxDirect)),
        ("ch-54.0-windows-uefi", Some(BootSource::Uefi)),
    ] {
        let environment = catalog.iter().find(|entry| entry.name() == name).unwrap();
        let execution = environment.definition().execution_requirements().unwrap();
        assert_eq!(
            super::snapshot_boot_source(Some(&execution)).unwrap(),
            expected
        );
        if expected.is_some() {
            let mut retired = serde_json::to_value(execution).unwrap();
            retired["environment"] = serde_json::json!(format!("sha256:{}", "a".repeat(64)));
            let retired = serde_json::from_value(retired).unwrap();
            assert_eq!(
                super::snapshot_boot_source(Some(&retired))
                    .unwrap_err()
                    .code(),
                tonic::Code::FailedPrecondition
            );
        }
    }
    assert_eq!(super::snapshot_boot_source(None).unwrap(), None);
}

// A Firecracker API that accepts the connection and never answers fails the
// pause or resume at its deadline instead of holding the VM gate forever.
#[tokio::test]
async fn a_firecracker_that_never_answers_fails_a_pause_state_change_at_its_deadline() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("firecracker.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let silent = tokio::spawn(async move {
        let (connection, _) = listener.accept().await.unwrap();
        // Hold the connection open without answering.
        std::future::pending::<()>().await;
        drop(connection);
    });
    let client = crate::fc_api::FcApiClient::new(&socket);
    for paused in [false, true] {
        let started = std::time::Instant::now();
        let error =
            super::set_firecracker_paused(&client, paused, std::time::Duration::from_millis(200))
                .await
                .expect_err("a silent Firecracker fails the change");
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded, "{paused}");
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }
    silent.abort();
}

async fn service_with_stop(
    code: tonic::Code,
    failures: usize,
) -> (
    crate::service::NodeManagerService,
    pplx_space_common_core::types::SandboxId,
    tempfile::TempDir,
    crate::runtime_supervisor_client::tests::StubSupervisorServer,
) {
    let (socket, client, server) =
        crate::runtime_supervisor_client::tests::flaky_stop_client(code, failures).await;
    let mut service = crate::service::tests::service_tests::service_for_unit_tests().await;
    service.runtime_supervisor = client;
    let id = pplx_space_common_core::types::SandboxId::new();
    let fence = pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(
        pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        None,
    );
    service.pool.bind_execution(&id.to_string(), fence).unwrap();
    (service, id, socket, server)
}

/// The stub fails each stop at once, so the retry delay only paces attempts
/// and a short one leaves what the cap counts unchanged.
const TEST_STOP_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(1);

#[tokio::test]
async fn encrypted_seal_stop_retries_while_the_runtime_is_still_exiting() {
    for code in [tonic::Code::Internal, tonic::Code::Unavailable] {
        let (service, id, _socket, server) = service_with_stop(code, 2).await;
        super::stop_runtime_for_encrypted_seal_retrying_after(
            &service,
            &id.to_string(),
            TEST_STOP_RETRY_DELAY,
        )
        .await
        .expect("stop succeeds once the runtime finishes exiting");
        crate::runtime_supervisor_client::tests::stop_supervisor_server(server).await;
    }
}

#[tokio::test]
async fn encrypted_seal_stop_gives_up_after_the_attempt_cap() {
    let (service, id, _socket, server) =
        service_with_stop(tonic::Code::Internal, super::ENC_CAPTURE_STOP_ATTEMPTS).await;

    let error = super::stop_runtime_for_encrypted_seal_retrying_after(
        &service,
        &id.to_string(),
        TEST_STOP_RETRY_DELAY,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);

    crate::runtime_supervisor_client::tests::stop_supervisor_server(server).await;
}

#[tokio::test]
async fn encrypted_seal_stop_does_not_retry_other_failures() {
    let (service, id, _socket, server) =
        service_with_stop(tonic::Code::FailedPrecondition, 1).await;

    let error = super::stop_runtime_for_encrypted_seal(&service, &id.to_string())
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    crate::runtime_supervisor_client::tests::stop_supervisor_server(server).await;
}

fn snapshot_volume(sandbox_id: &str) -> String {
    format!("{}/{sandbox_id}/snapshot-vol", super::CACHE_DIR)
}

/// A service whose blocking host work is refused, with an allocated sandbox
/// and a second sandbox whose execution is bound but which holds no lease.
struct SnapshotFixture {
    service: crate::service::NodeManagerService,
    lease: crate::net::SlotInfo,
    fence: crate::proto::ExecutionFence,
    unallocated_id: String,
    unallocated_fence: crate::proto::ExecutionFence,
}

impl SnapshotFixture {
    async fn new() -> Self {
        use crate::service::tests::service_tests::{
            allocate_test_lease, bind_test_execution, refuse_host_work, service_for_unit_tests,
        };

        let mut service = service_for_unit_tests().await;
        refuse_host_work(&mut service).await;
        let id = super::SandboxId::new().to_string();
        let fence = bind_test_execution(&service, &id);
        let lease = allocate_test_lease(&service, &id);
        let unallocated_id = super::SandboxId::new().to_string();
        let unallocated_fence = bind_test_execution(&service, &unallocated_id);
        Self {
            service,
            lease,
            fence,
            unallocated_id,
            unallocated_fence,
        }
    }

    fn lease_disturbed(&self, case: &str) -> Option<String> {
        let current = self.service.pool.get(&self.lease.sandbox_id).unwrap();
        (current
            .as_ref()
            .map(|lease| (lease.slot, lease.generation, lease.sandbox_uid, lease.pid))
            != Some((
                self.lease.slot,
                self.lease.generation,
                self.lease.sandbox_uid,
                None,
            )))
        .then(|| format!("{case}: lease changed to {current:?}"))
    }
}

// No case may pass validation for the allocated sandbox: the next step dials
// the Firecracker API socket under the node's fixed jail directory. Rows whose
// guard precedes the lease lookup use the unallocated sandbox, so dropping the
// guard stops at the lease check instead. That includes the encrypted rows,
// whose capture would otherwise bind into the jail.
#[tokio::test]
async fn create_snapshot_refuses_invalid_requests_before_dialing_firecracker() {
    use crate::proto::CaptureVmStateRequest;
    use crate::service::tests::service_tests::{refusal_mismatch, root_request, stale_test_fence};
    use tonic::Code;

    let fixture = SnapshotFixture::new().await;
    let id = fixture.lease.sandbox_id.clone();
    let valid = CaptureVmStateRequest {
        sandbox_id: id.clone(),
        execution_fence: Some(fixture.fence.clone()),
        artifacts: crate::snapshot_request::tests::artifacts("/vmstate".into(), "/memory".into()),
        ..Default::default()
    };
    let unallocated = CaptureVmStateRequest {
        sandbox_id: fixture.unallocated_id.clone(),
        execution_fence: Some(fixture.unallocated_fence.clone()),
        ..valid.clone()
    };
    let cases = [
        (
            "superseded execution",
            CaptureVmStateRequest {
                execution_fence: Some(stale_test_fence()),
                ..unallocated.clone()
            },
            Code::FailedPrecondition,
            "sandbox execution identity is missing or superseded",
        ),
        (
            "vmstate path names another artifact",
            CaptureVmStateRequest {
                artifacts: crate::snapshot_request::tests::artifacts(
                    "/memory".into(),
                    "/memory".into(),
                ),
                ..valid.clone()
            },
            Code::InvalidArgument,
            "snapshot_path must reference Firecracker artifact 'vmstate'",
        ),
        (
            "memory path names another artifact",
            CaptureVmStateRequest {
                artifacts: crate::snapshot_request::tests::artifacts(
                    "/vmstate".into(),
                    "/vmstate".into(),
                ),
                ..valid.clone()
            },
            Code::InvalidArgument,
            "mem_file_path must reference Firecracker artifact 'memory'",
        ),
        (
            "destination outside the sandbox roots",
            CaptureVmStateRequest {
                storage: Some(crate::proto::VmStateStorage {
                    directory: Some(format!(
                        "{}/{}/snapshot",
                        super::CACHE_DIR,
                        fixture.unallocated_id
                    )),
                    ..Default::default()
                }),
                ..valid.clone()
            },
            Code::PermissionDenied,
            "dest_dir is outside allowed runtime roots",
        ),
        (
            "plaintext capture without a lease",
            unallocated.clone(),
            Code::Unavailable,
            "snapshot lease is no longer active",
        ),
        (
            "encrypted volume of another sandbox",
            CaptureVmStateRequest {
                storage: Some(crate::proto::VmStateStorage {
                    encrypted_volume_mount: Some(snapshot_volume(&id)),
                    ..Default::default()
                }),
                ..unallocated.clone()
            },
            Code::InvalidArgument,
            "path does not belong to this sandbox",
        ),
        (
            "encrypted capture without its required staging directory",
            CaptureVmStateRequest {
                storage: Some(crate::proto::VmStateStorage {
                    encrypted_volume_mount: Some(snapshot_volume(&fixture.unallocated_id)),
                    ..Default::default()
                }),
                ..unallocated.clone()
            },
            Code::InvalidArgument,
            "encrypted capture requires its staging directory",
        ),
    ];
    let mut mismatches = Vec::new();
    for (case, request, code, message) in cases {
        let outcome = fixture
            .service
            .capture_vm_state(root_request(request))
            .await;
        mismatches.extend(refusal_mismatch(case, outcome, code, message));
        mismatches.extend(fixture.lease_disturbed(case));
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}

// No invalid case may pass validation: the next step stages files or binds the
// volume into the node's fixed jail directory, or waits for the Firecracker
// socket. The valid encrypted loads show where validation ends: their volume
// bind is the first blocking host step, which the fixture refuses. Rows whose
// guard precedes the lease lookup and that stage nothing use the unallocated
// sandbox, so dropping the guard stops at the lease check instead of polling
// the jail's Firecracker socket. The encrypted rate-limit row shows the limit
// is refused before the refused bind. The checkpoint row only shows it is
// refused before the path checks: the source check first reads the sandbox's
// checkpoint files under the fixed cache root, which do not exist, so no
// plaintext row can reach the staging step, and a valid checkpoint load has no
// row because that read's outcome depends on the host.
#[tokio::test]
async fn load_snapshot_refuses_invalid_requests_before_staging_the_jail() {
    use crate::lifecycle_limiter::LifecycleMethod;
    use crate::proto::RestoreVmStateRequest;
    use crate::proto::{DriveRateLimit, TokenBucket};
    use crate::service::tests::service_tests::{refusal_mismatch, root_request, stale_test_fence};
    use tonic::Code;

    let fixture = SnapshotFixture::new().await;
    let id = fixture.lease.sandbox_id.clone();
    let valid = RestoreVmStateRequest {
        sandbox_id: id.clone(),
        execution_fence: Some(fixture.fence.clone()),
        artifacts: crate::snapshot_request::tests::artifacts("/vmstate".into(), "/memory".into()),
        ..Default::default()
    };
    let unallocated = RestoreVmStateRequest {
        sandbox_id: fixture.unallocated_id.clone(),
        execution_fence: Some(fixture.unallocated_fence.clone()),
        ..valid.clone()
    };
    let bucket = Some(TokenBucket {
        size: 1,
        refill_time_ms: 1,
    });
    let cases = [
        (
            "superseded execution",
            RestoreVmStateRequest {
                execution_fence: Some(stale_test_fence()),
                ..unallocated.clone()
            },
            Code::FailedPrecondition,
            "sandbox execution identity is missing or superseded",
        ),
        (
            "unallocated sandbox",
            unallocated.clone(),
            Code::Unavailable,
            "snapshot lease is no longer active",
        ),
        (
            "encrypted volume of another sandbox",
            RestoreVmStateRequest {
                storage: Some(crate::proto::VmStateStorage {
                    encrypted_volume_mount: Some(snapshot_volume(&fixture.unallocated_id)),
                    ..Default::default()
                }),
                ..valid.clone()
            },
            Code::InvalidArgument,
            "path does not belong to this sandbox",
        ),
        (
            "vmstate path names another artifact",
            RestoreVmStateRequest {
                artifacts: crate::snapshot_request::tests::artifacts(
                    "/memory".into(),
                    "/memory".into(),
                ),
                ..valid.clone()
            },
            Code::InvalidArgument,
            "snapshot_path must reference Firecracker artifact 'vmstate'",
        ),
        (
            "memory path names another artifact",
            RestoreVmStateRequest {
                artifacts: crate::snapshot_request::tests::artifacts(
                    "/vmstate".into(),
                    "/vmstate".into(),
                ),
                ..valid.clone()
            },
            Code::InvalidArgument,
            "mem_file_path must reference Firecracker artifact 'memory'",
        ),
        (
            "source other than the sandbox checkpoint",
            RestoreVmStateRequest {
                storage: Some(crate::proto::VmStateStorage {
                    directory: Some(format!(
                        "{}/{}/snapshot",
                        super::CACHE_DIR,
                        fixture.unallocated_id
                    )),
                    ..Default::default()
                }),
                ..valid.clone()
            },
            Code::PermissionDenied,
            "source_dir must be the sandbox checkpoint directory",
        ),
        (
            "rate limit without bandwidth",
            RestoreVmStateRequest {
                rootfs_rate_limit: Some(DriveRateLimit {
                    bandwidth: None,
                    operations: bucket,
                }),
                ..unallocated.clone()
            },
            Code::InvalidArgument,
            "rootfs_rate_limit.bandwidth is required",
        ),
        (
            "rate limit without operations",
            RestoreVmStateRequest {
                rootfs_rate_limit: Some(DriveRateLimit {
                    bandwidth: bucket,
                    operations: None,
                }),
                ..unallocated.clone()
            },
            Code::InvalidArgument,
            "rootfs_rate_limit.operations is required",
        ),
        (
            "checkpoint load with a rate limit without bandwidth",
            RestoreVmStateRequest {
                rootfs_rate_limit: Some(DriveRateLimit {
                    bandwidth: None,
                    operations: bucket,
                }),
                storage: Some(crate::proto::VmStateStorage {
                    directory: Some(format!("{}/{id}/snapshot", super::CACHE_DIR)),
                    ..Default::default()
                }),
                ..valid.clone()
            },
            Code::InvalidArgument,
            "rootfs_rate_limit.bandwidth is required",
        ),
        (
            "encrypted load with a rate limit without operations",
            RestoreVmStateRequest {
                rootfs_rate_limit: Some(DriveRateLimit {
                    bandwidth: bucket,
                    operations: None,
                }),
                storage: Some(crate::proto::VmStateStorage {
                    encrypted_volume_mount: Some(snapshot_volume(&id)),
                    ..Default::default()
                }),
                ..valid.clone()
            },
            Code::InvalidArgument,
            "rootfs_rate_limit.operations is required",
        ),
        (
            "valid encrypted load",
            RestoreVmStateRequest {
                storage: Some(crate::proto::VmStateStorage {
                    encrypted_volume_mount: Some(snapshot_volume(&id)),
                    ..Default::default()
                }),
                ..valid.clone()
            },
            Code::Cancelled,
            "artifact work cancelled",
        ),
        (
            "valid encrypted load with a rate limit",
            RestoreVmStateRequest {
                rootfs_rate_limit: Some(DriveRateLimit {
                    bandwidth: bucket,
                    operations: bucket,
                }),
                storage: Some(crate::proto::VmStateStorage {
                    encrypted_volume_mount: Some(snapshot_volume(&id)),
                    ..Default::default()
                }),
                ..valid.clone()
            },
            Code::Cancelled,
            "artifact work cancelled",
        ),
    ];
    let mut mismatches = Vec::new();
    for (case, request, code, message) in cases {
        let outcome = fixture
            .service
            .restore_vm_state(root_request(request))
            .await;
        mismatches.extend(refusal_mismatch(case, outcome, code, message));
        mismatches.extend(fixture.lease_disturbed(case));
        if fixture
            .service
            .lifecycle_limiter
            .snapshot(LifecycleMethod::RestoreVmState)
            != (0, 0)
        {
            mismatches.push(format!("{case}: refusal leaked its lifecycle permit"));
            // A leaked permit would starve the remaining rows.
            break;
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}

/// A rollback prunes every snapshot that sorts after its target. Restoring an
/// internal copy must leave the sandbox's other snapshots, and the copy
/// itself, where they are.
#[test]
fn instance_restore_replaces_only_the_instance() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    std::fs::create_dir(root.join("instance")).unwrap();
    std::fs::write(root.join("instance/rootfs.ext4"), "booted").unwrap();
    let preserved = "d-operation-runtime-upgrade-019a0000-0000-7000-8000-000000000001";
    let others = [
        "d-1m-20990101T000000Z",
        "f-1h-20990101T000000Z",
        "d-manual-20990101T000000Z",
        "d-stop-ffffffff-ffff-7fff-bfff-ffffffffffff",
        "zz-later",
    ];
    for name in std::iter::once(preserved).chain(others) {
        let snapshot = root.join("snapshots").join(name);
        std::fs::create_dir_all(&snapshot).unwrap();
        std::fs::write(snapshot.join("rootfs.ext4"), name).unwrap();
    }
    let sandbox = super::super::trusted_paths::open_directory(&root).unwrap();

    restore_instance_snapshot_in(
        &sandbox,
        preserved,
        nix::unistd::getegid().as_raw(),
        crate::service::artifact_fs::DiskState::RootfsOrCow,
    )
    .unwrap();

    assert_eq!(
        std::fs::read_to_string(root.join("instance/rootfs.ext4")).unwrap(),
        preserved
    );
    for name in std::iter::once(preserved).chain(others) {
        assert_eq!(
            std::fs::read_to_string(root.join("snapshots").join(name).join("rootfs.ext4")).unwrap(),
            name,
            "{name} must survive the restore"
        );
    }
    assert!(
        restore_instance_snapshot_in(
            &sandbox,
            "missing",
            0,
            crate::service::artifact_fs::DiskState::RootfsOrCow,
        )
        .unwrap_err()
        .code()
            != tonic::Code::Ok
    );
}

#[tokio::test]
async fn instance_restore_accepts_only_internal_snapshots() {
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    let sandbox_id = pplx_space_common_core::types::SandboxId::new().to_string();
    for name in [
        "d-1m-20260922T120000Z",
        "manual",
        "../d-operation-x",
        "d-operation",
    ] {
        let mut request = tonic::Request::new(crate::proto::RestoreInstanceSnapshotRequest {
            sandbox_id: sandbox_id.clone(),
            snapshot_name: name.into(),
            execution_fence: None,
        });
        request
            .extensions_mut()
            .insert(pplx_space_uds_peer_auth::PeerIdentity::new(0, 0, None));
        let error = service
            .restore_instance_snapshot(request)
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            tonic::Code::InvalidArgument,
            "{name}: {error}"
        );
    }
}

#[tokio::test]
async fn abandoned_capture_handler_finishes_queued_running_and_stopping_attempts() {
    use crate::runtime_supervisor_client::tests::{SupervisorScript, scripted_in_process_client};
    use crate::service::tests::service_tests::{
        allocate_test_lease, bind_test_execution, root_request,
    };
    use pplx_space_common_core::types::LocalArtifactId;
    use pplx_space_node_mgr_client::capture_receipt::{COMMITTED, INTENT, STAGING_PREFIX};
    use std::{
        future::{Future, poll_fn},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Poll,
        time::Duration,
    };

    for phase in [
        "queued",
        "capturing",
        "stopping",
        "stop_failed",
        "capture_failed",
        "fence_refused",
        "lease_refused",
    ] {
        let mut service = crate::service::tests::service_tests::service_for_unit_tests().await;
        let id = super::SandboxId::new().to_string();
        let mut fence = bind_test_execution(&service, &id);
        if phase != "lease_refused" {
            allocate_test_lease(&service, &id);
        }
        if phase == "fence_refused" {
            fence.execution_id = uuid::Uuid::now_v7().to_string();
        }
        let cache = tempfile::tempdir().unwrap();
        let artifact = LocalArtifactId::new();
        let staging = cache
            .path()
            .join(&id)
            .join("snapshots")
            .join(format!("{STAGING_PREFIX}{artifact}"));
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(
            staging.join(INTENT),
            serde_json::to_vec(
                &serde_json::json!({"sandbox_id": id, "local_artifact_id": artifact}),
            )
            .unwrap(),
        )
        .unwrap();
        let captures = Arc::new(AtomicUsize::new(0));
        let stops = Arc::new(AtomicUsize::new(0));
        let drains = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let stopping = Arc::new(tokio::sync::Notify::new());
        let finish = Arc::new(tokio::sync::Notify::new());
        service.encrypted_capture_io = Some(super::EncryptedCaptureTestIo {
            cache: cache.path().to_owned(),
            drain: Arc::new({
                let (drains, staging) = (drains.clone(), staging.clone());
                move || {
                    assert!(
                        staging.join(COMMITTED).exists(),
                        "bind drain follows durable completion"
                    );
                    drains.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async { Ok(()) })
                }
            }),
            capture: Arc::new({
                let (captures, started, finish) =
                    (captures.clone(), started.clone(), finish.clone());
                move || {
                    let (captures, started, finish) =
                        (captures.clone(), started.clone(), finish.clone());
                    Box::pin(async move {
                        captures.fetch_add(1, Ordering::SeqCst);
                        if phase == "capture_failed" {
                            return Err(tonic::Status::internal(
                                "synthetic in-place capture failure",
                            ));
                        }
                        started.notify_one();
                        if phase == "capturing" {
                            finish.notified().await;
                        }
                        Ok(())
                    })
                }
            }),
        });
        service.runtime_supervisor = scripted_in_process_client(SupervisorScript {
            stop_runtime: Some(Arc::new({
                let (stops, stopping, staging) = (stops.clone(), stopping.clone(), staging.clone());
                move |_| {
                    assert!(
                        !staging.join(COMMITTED).exists(),
                        "receipt must follow confirmed stop"
                    );
                    let n = stops.fetch_add(1, Ordering::SeqCst);
                    stopping.notify_one();
                    if phase == "stop_failed" {
                        return Err(tonic::Status::failed_precondition("synthetic stop failure"));
                    }
                    if phase == "stopping" && n == 0 {
                        return Err(tonic::Status::unavailable("still stopping"));
                    }
                    Ok(Default::default())
                }
            })),
            ..Default::default()
        });
        let queued = if phase == "queued" {
            Some(service.lock_sandbox_vm_cgroup_launch(&id).await)
        } else {
            None
        };
        let request = root_request(crate::proto::CaptureVmStateRequest {
            sandbox_id: id.clone(),
            execution_fence: Some(fence),
            artifacts: crate::snapshot_request::tests::artifacts(
                "/vmstate".into(),
                "/memory".into(),
            ),
            storage: Some(crate::proto::VmStateStorage {
                directory: Some(staging.to_string_lossy().into_owned()),
                encrypted_volume_mount: Some(snapshot_volume(&id)),
            }),
            ..Default::default()
        });
        let mut handler = Box::pin(service.capture_vm_state(request));
        poll_fn(|cx| {
            assert!(handler.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        if phase == "capturing" {
            tokio::time::timeout(Duration::from_secs(5), started.notified())
                .await
                .unwrap();
        }
        if phase == "stopping" {
            tokio::time::timeout(Duration::from_secs(5), stopping.notified())
                .await
                .unwrap();
        }
        drop(handler);
        drop(queued);
        finish.notify_one();
        service.device_op_tasks.close();
        tokio::time::timeout(Duration::from_secs(10), service.device_op_tasks.wait())
            .await
            .unwrap();
        let refused = matches!(phase, "fence_refused" | "lease_refused");
        let completed = !refused && !matches!(phase, "capture_failed" | "stop_failed");
        assert_eq!(
            captures.load(Ordering::SeqCst),
            usize::from(!refused),
            "{phase}"
        );
        assert_eq!(
            stops.load(Ordering::SeqCst),
            if refused || phase == "capture_failed" {
                0
            } else if phase == "stopping" {
                2
            } else {
                1
            },
            "{phase}"
        );
        assert_eq!(
            drains.load(Ordering::SeqCst),
            usize::from(completed),
            "{phase}"
        );
        assert_eq!(staging.join(COMMITTED).exists(), completed, "{phase}");
        assert!(
            !staging.join(INTENT).exists(),
            "{phase}: the owner durably settled"
        );
        assert_eq!(
            staging
                .join(pplx_space_node_mgr_client::capture_receipt::FAILED)
                .exists(),
            !refused && !completed,
            "{phase}"
        );
        assert_eq!(
            staging
                .join(pplx_space_node_mgr_client::capture_receipt::REFUSED)
                .exists(),
            refused,
            "{phase}"
        );
    }
}

#[tokio::test]
async fn cloud_hypervisor_pre_capture_refusal_never_stops_the_runtime() {
    use crate::runtime_supervisor_client::tests::{SupervisorScript, scripted_in_process_client};
    use crate::service::tests::service_tests::{bind_test_execution, service_for_unit_tests};
    use pplx_space_common_core::types::SandboxId;
    use pplx_space_node_mgr_client::capture_receipt::{INTENT, REFUSED, STAGING_PREFIX};
    let mut service = service_for_unit_tests().await;
    let sandbox = SandboxId::new();
    let artifact = pplx_space_common_core::types::LocalArtifactId::new();
    let root = tempfile::tempdir().unwrap();
    let staging = root
        .path()
        .join(sandbox.to_string())
        .join("snapshots")
        .join(format!("{STAGING_PREFIX}{artifact}"));
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::write(
        staging.join(INTENT),
        serde_json::to_vec(&serde_json::json!({
            "sandbox_id": sandbox.to_string(), "local_artifact_id": artifact
        }))
        .unwrap(),
    )
    .unwrap();
    let receipt =
        super::capture_receipt::CaptureReceipt::open_under(root.path(), sandbox, &staging).unwrap();
    service.runtime_supervisor = scripted_in_process_client(SupervisorScript {
        stop_runtime: Some(std::sync::Arc::new(|_| {
            panic!("a refusal must not stop the runtime")
        })),
        ..Default::default()
    });
    let request =
        crate::snapshot_request::StateCapture::try_from(crate::proto::CaptureVmStateRequest {
            sandbox_id: sandbox.to_string(),
            execution_fence: Some(bind_test_execution(&service, &sandbox.to_string())),
            storage: Some(crate::proto::VmStateStorage {
                directory: Some(staging.to_string_lossy().into_owned()),
                encrypted_volume_mount: Some(snapshot_volume(&sandbox.to_string())),
            }),
            artifacts: crate::snapshot_request::tests::artifacts(
                "/vmstate".into(),
                "/memory".into(),
            ),
            ..Default::default()
        })
        .unwrap();
    let mut destination = super::CaptureDestination::Encrypted(
        pplx_space_common_core::types::NodeSpacePath::parse(snapshot_volume(&sandbox.to_string()))
            .unwrap(),
        receipt,
    );
    let result = service
        .capture_cloud_hypervisor_state(
            tonic::Request::new(request),
            &mut destination,
            pplx_space_node_runtime_core::boot::BootSource::LinuxDirect,
        )
        .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
    let super::CaptureDestination::Encrypted(_, receipt) = destination else {
        unreachable!();
    };
    assert!(!receipt.capture_started());
    receipt.settle_error().await.unwrap();
    assert!(staging.join(REFUSED).exists());
    assert!(!staging.join(INTENT).exists());
}
