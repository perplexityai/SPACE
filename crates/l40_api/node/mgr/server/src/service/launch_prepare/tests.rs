use pplx_space_common_core::types::SandboxId;

use pplx_space_common_core::types::{Arch, ExecutionMetadata, RuntimeKind};

use super::{parse_runtime_request, prepare_sandbox_cache_paths, prepare_sandbox_owned_paths};

#[test]
fn oversized_rootfs_resize_is_rejected_before_touching_disk() {
    let directory = tempfile::tempdir().unwrap();
    let rootfs = directory.path().join("rootfs.ext4");
    std::fs::write(&rootfs, b"unchanged").unwrap();
    for mib in [0, 128 * 1024 + 1, u32::MAX] {
        let error = super::resize_guest_disk(
            rootfs.to_str().unwrap(),
            mib,
            pplx_space_common_core::types::guest_image::GuestDiskLayout::Ext4Filesystem,
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(std::fs::read(&rootfs).unwrap(), b"unchanged");
    }
}

#[test]
fn materialized_rootfs_policy_counts_sparse_logical_size() {
    let directory = tempfile::tempdir().unwrap();
    let rootfs = directory.path().join("rootfs.ext4");
    let file = std::fs::File::create(&rootfs).unwrap();
    for (size, accepted) in [
        (0, false),
        (1, true),
        (128_u64 * 1024 * 1024 * 1024 + 1, false),
    ] {
        file.set_len(size).unwrap();
        assert_eq!(
            super::validate_materialized_rootfs(&rootfs).is_ok(),
            accepted
        );
    }
}

async fn allocated_service() -> (super::NodeManagerService, crate::net::SlotInfo) {
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    service.pool.set_all_slots_ready_for_test();
    let id = SandboxId::new().to_string();
    let lease = service
        .pool
        .allocate(
            &id,
            None,
            Some(crate::slot_pool::NewLeaseMemoryReservation {
                memory_reservation_mib: 8320,
            }),
        )
        .await
        .unwrap();
    (service, lease)
}

#[tokio::test]
async fn cancelled_preparation_retains_exclusion_capacity_and_disk_accounting() {
    use crate::lifecycle_limiter::LifecycleMethod;
    for fail in [false, true] {
        let (service, lease) = allocated_service().await;
        let permit = service
            .lifecycle_limiter
            .acquire(LifecycleMethod::PrepareSandbox)
            .await
            .unwrap();
        let ownership = service
            .acquire_preparation_ownership(&lease.sandbox_id, lease.sandbox_uid, permit)
            .await
            .unwrap();
        // Cold startup starts the gateway while preparation still owns the VM
        // filesystem. Sidecars must progress, but destructive work must wait.
        let sidecar = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            service.lock_sandbox_sidecar_cgroup_launch(&lease.sandbox_id),
        )
        .await
        .expect("gateway launch must overlap preparation");
        drop(sidecar);
        let reservation = service.disk_admission.restore_reservation(
            crate::disk_admission::DiskAllocationOperation::PrepareSandbox,
            Some(4096),
        );
        let reserved = service.disk_admission.pending_reservation_bytes();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = service.clone();
        let sandbox_id = lease.sandbox_id.clone();
        let waiter = tokio::spawn(async move {
            let owned = worker.clone();
            worker
                .run_owned_sandbox_work(&sandbox_id, async move {
                    owned
                        .run_owned_preparation(ownership, Some(reservation), move || {
                            entered_tx.send(()).unwrap();
                            release_rx.recv().unwrap();
                            if fail {
                                Err(tonic::Status::internal("partial preparation failed"))
                            } else {
                                Ok(())
                            }
                        })
                        .await
                })
                .await
        });
        entered_rx.await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        let gate = service.sandbox_cgroup_gate_lock(&lease.sandbox_id).await;
        assert!(
            gate.try_write().is_err(),
            "cleanup must wait for host mutation"
        );
        assert_eq!(
            service
                .lifecycle_limiter
                .snapshot(LifecycleMethod::PrepareSandbox)
                .0,
            1
        );
        assert_eq!(service.disk_admission.pending_reservation_bytes(), reserved);
        assert!(service.claim_sandbox_mutation(&lease.sandbox_id).is_err());
        service.device_op_tasks.close();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                service.device_op_tasks.wait()
            )
            .await
            .is_err()
        );
        release_tx.send(()).unwrap();
        service.device_op_tasks.wait().await;
        drop(service.claim_sandbox_mutation(&lease.sandbox_id).unwrap());
        drop(gate.try_write().unwrap());
        assert_eq!(
            service
                .lifecycle_limiter
                .snapshot(LifecycleMethod::PrepareSandbox)
                .0,
            0
        );
        assert_eq!(service.disk_admission.pending_reservation_bytes(), 0);
    }
}

#[tokio::test]
async fn preparation_rechecks_lease_after_waiting_for_cleanup_ownership() {
    let (service, lease) = allocated_service().await;
    let gate = service.lock_sandbox_cgroups(&lease.sandbox_id).await;
    let permit = service
        .lifecycle_limiter
        .acquire(crate::lifecycle_limiter::LifecycleMethod::PrepareSandbox)
        .await
        .unwrap();
    let mut prepare = Box::pin(service.acquire_preparation_ownership(
        &lease.sandbox_id,
        lease.sandbox_uid,
        permit,
    ));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(prepare.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    service.pool.release(&lease.sandbox_id).unwrap();
    drop(gate);
    let error = prepare.await.err().expect("released lease cannot prepare");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[rstest::rstest]
#[case("019e31ec-5633-7111-b27c-1b6ac5cb170a")]
#[case("019e31ec56337111b27c1b6ac5cb170a")]
fn prepared_rootfs_paths_share_the_canonical_sandbox_cache(#[case] sandbox_id: &str) {
    let paths = prepare_sandbox_cache_paths(sandbox_id);
    let expected = "/space/cache/firecracker/sandboxes/019e31ec-5633-7111-b27c-1b6ac5cb170a";

    assert_eq!(paths.sandbox_dir, expected);
    assert_eq!(paths.instance_dir, format!("{expected}/instance"));
    assert_eq!(paths.rootfs, format!("{expected}/instance/rootfs.ext4"));
    assert_eq!(paths.snapshots_dir, format!("{expected}/snapshots"));
}

#[test]
fn prepare_sandbox_owned_paths_excludes_sidecar_run_dir() {
    let run_dir = "/space/run/sandboxes/sandbox-a";
    let paths = prepare_sandbox_owned_paths(
        "/space/cache/sandbox-a",
        "/space/cache/sandbox-a/instance/rootfs.ext4",
        "/space/run/sandboxes/sandbox-a/vm-config.json",
    );

    assert!(
        !paths.contains(&run_dir),
        "PrepareSandbox must not chown the sidecar run dir away from its service group"
    );
}

#[test]
fn runtime_request_keeps_structured_execution_metadata() {
    let execution = ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![]);
    let wire = (&execution).into();
    let (runtime, requested) = parse_runtime_request("firecracker", Some(&wire)).unwrap();

    assert_eq!(runtime, RuntimeKind::Firecracker);
    assert_eq!(requested.unwrap(), execution);
}

#[test]
fn runtime_request_rejects_partial_execution_metadata() {
    let wire = pplx_space_execution_common::proto::ExecutionMetadata {
        runtime: None,
        guest: None,
        state_format: None,
        components: vec![],
        environment: None,
    };
    let error = parse_runtime_request("firecracker", Some(&wire)).unwrap_err();

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}

/// An allocated sandbox with a bound execution, plus a second sandbox whose
/// execution is bound but which holds no lease. Host work is refused, so a
/// request that passes validation ends with `Cancelled` at its first host step.
struct AdmittedSandbox {
    service: super::NodeManagerService,
    lease: crate::net::SlotInfo,
    fence: crate::proto::ExecutionFence,
    unallocated_id: String,
    unallocated_fence: crate::proto::ExecutionFence,
    _runtime: tempfile::TempDir,
}

impl AdmittedSandbox {
    async fn new() -> Self {
        use crate::service::tests::service_tests::{
            allocate_test_lease, bind_test_execution, installed_runtime, refuse_host_work,
            service_for_unit_tests,
        };

        let (runtime, runtime_paths) = installed_runtime();
        let mut service = service_for_unit_tests().await;
        service.runtime_paths = runtime_paths;
        // These tests own request/lease validation, not E0 byte qualification.
        // Inject the synthetic external inventory before refusing host work.
        let inventory =
            pplx_space_node_runtime_core::runtime_inventory::RuntimeInventory::discover(
                [
                    pplx_space_node_runtime_core::runtime_inventory::RuntimeInstallationRoot::new(
                        RuntimeKind::Firecracker,
                        &service.runtime_paths.vendor,
                    ),
                ],
                &service.runtime_paths.assets,
                &service.runtime_paths.kernels,
            )
            .unwrap();
        service
            .runtime_inventory
            .store(Some(std::sync::Arc::new(inventory)));
        refuse_host_work(&mut service).await;
        let id = SandboxId::new().to_string();
        let fence = bind_test_execution(&service, &id);
        let lease = allocate_test_lease(&service, &id);
        let unallocated_id = SandboxId::new().to_string();
        let unallocated_fence = bind_test_execution(&service, &unallocated_id);
        Self {
            service,
            lease,
            fence,
            unallocated_id,
            unallocated_fence,
            _runtime: runtime,
        }
    }

    /// Records what a refused request disturbed: the lease must stay exactly as
    /// allocated and its lifecycle admission must be returned. A leaked permit
    /// breaks the table, since it would starve the remaining rows.
    fn record_disturbance(
        &self,
        case: &str,
        method: crate::lifecycle_limiter::LifecycleMethod,
        mismatches: &mut Vec<String>,
    ) -> std::ops::ControlFlow<()> {
        let current = self.service.pool.get(&self.lease.sandbox_id).unwrap();
        let expected = (
            self.lease.slot,
            self.lease.generation,
            self.lease.sandbox_uid,
            None,
        );
        if current
            .as_ref()
            .map(|lease| (lease.slot, lease.generation, lease.sandbox_uid, lease.pid))
            != Some(expected)
        {
            mismatches.push(format!("{case}: lease changed to {current:?}"));
        }
        if self.service.lifecycle_limiter.snapshot(method) != (0, 0) {
            mismatches.push(format!("{case}: refusal leaked its lifecycle permit"));
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    }
}

/// The signal that a request passed validation and reached its first blocking
/// host step, which `refuse_host_work` refuses.
const HOST_WORK_REFUSED: (tonic::Code, &str) = (tonic::Code::Cancelled, "artifact work cancelled");

// Every invalid case is refused before the jail is inspected or rebuilt. The
// valid request shows where validation ends: the jail-ownership probe.
#[tokio::test]
async fn launch_sandbox_refuses_invalid_requests_before_touching_the_jail() {
    use crate::service::tests::service_tests::{refusal_mismatch, root_request, stale_test_fence};
    use pplx_space_execution_common::proto::ExecutionSelectionMode as Mode;
    use pplx_space_node_runtime_core::runtime_inventory::RuntimeInventoryError;
    use tonic::Code;

    let admitted = AdmittedSandbox::new().await;
    let id = admitted.lease.sandbox_id.clone();
    let uid = admitted.lease.sandbox_uid;
    let valid = crate::proto::LaunchSandboxRequest {
        sandbox_id: id.clone(),
        uid,
        gid: uid,
        runtime: "firecracker".into(),
        execution_selection_mode: Mode::NewColdBoot.into(),
        execution_fence: Some(admitted.fence.clone()),
        ..Default::default()
    };
    let unsupported_runtime = "unsupported-runtime"
        .parse::<RuntimeKind>()
        .unwrap_err()
        .to_string();
    let missing_execution = RuntimeInventoryError::MissingRequestedExecution(
        pplx_space_node_runtime_core::runtime_inventory::ExecutionSelectionMode::ExistingColdBoot,
    )
    .to_string();
    let cases = [
        (
            "rootfs device of another sandbox",
            crate::proto::LaunchSandboxRequest {
                rootfs_block_device_path: Some(format!("/dev/mapper/sbxroot-{}", SandboxId::new())),
                ..valid.clone()
            },
            Code::InvalidArgument,
            "path does not belong to this sandbox",
        ),
        (
            "unsupported runtime",
            crate::proto::LaunchSandboxRequest {
                runtime: "unsupported-runtime".into(),
                ..valid.clone()
            },
            Code::InvalidArgument,
            unsupported_runtime.as_str(),
        ),
        (
            "unspecified selection mode",
            crate::proto::LaunchSandboxRequest {
                execution_selection_mode: Mode::Unspecified.into(),
                ..valid.clone()
            },
            Code::InvalidArgument,
            "execution_selection_mode is required",
        ),
        (
            "restore flag on a cold boot",
            crate::proto::LaunchSandboxRequest {
                snapshot_restore: true,
                ..valid.clone()
            },
            Code::InvalidArgument,
            "snapshot_restore and execution_selection_mode disagree",
        ),
        (
            "restore mode without the restore flag",
            crate::proto::LaunchSandboxRequest {
                execution_selection_mode: Mode::PersistedStateRestore.into(),
                ..valid.clone()
            },
            Code::InvalidArgument,
            "snapshot_restore and execution_selection_mode disagree",
        ),
        (
            "existing cold boot without its recorded execution",
            crate::proto::LaunchSandboxRequest {
                execution_selection_mode: Mode::ExistingColdBoot.into(),
                ..valid.clone()
            },
            Code::FailedPrecondition,
            missing_execution.as_str(),
        ),
        (
            "superseded execution",
            crate::proto::LaunchSandboxRequest {
                execution_fence: Some(stale_test_fence()),
                ..valid.clone()
            },
            Code::FailedPrecondition,
            "sandbox execution identity is missing or superseded",
        ),
        (
            "unallocated sandbox",
            crate::proto::LaunchSandboxRequest {
                sandbox_id: admitted.unallocated_id.clone(),
                execution_fence: Some(admitted.unallocated_fence.clone()),
                ..valid.clone()
            },
            Code::NotFound,
            "sandbox not allocated",
        ),
        (
            "uid of another slot",
            crate::proto::LaunchSandboxRequest {
                uid: uid + 1,
                gid: uid + 1,
                ..valid.clone()
            },
            Code::PermissionDenied,
            "does not match allocated slot UID",
        ),
        (
            "gid differs from uid",
            crate::proto::LaunchSandboxRequest {
                gid: uid + 1,
                ..valid.clone()
            },
            Code::PermissionDenied,
            "must match UID",
        ),
        (
            "valid request",
            valid.clone(),
            HOST_WORK_REFUSED.0,
            HOST_WORK_REFUSED.1,
        ),
    ];
    let mut mismatches = Vec::new();
    for (case, request, code, message) in cases {
        let outcome = admitted.service.launch_sandbox(root_request(request)).await;
        mismatches.extend(refusal_mismatch(case, outcome, code, message));
        if admitted
            .record_disturbance(
                case,
                crate::lifecycle_limiter::LifecycleMethod::LaunchSandbox,
                &mut mismatches,
            )
            .is_break()
        {
            break;
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}

// The deepest case names a template without its resolved directory, which is
// refused before the managed template root or sandbox cache is inspected.
#[tokio::test]
async fn prepare_sandbox_refuses_invalid_requests_before_touching_the_cache() {
    use crate::service::tests::service_tests::{refusal_mismatch, root_request, stale_test_fence};
    use pplx_space_common_core::types::{ContentHash, SandboxArtifactMetadata, TemplateName};
    use pplx_space_execution_common::proto::ExecutionSelectionMode as Mode;
    use pplx_space_node_runtime_core::runtime_inventory::RuntimeInventoryError;
    use std::str::FromStr;
    use tonic::Code;

    let admitted = AdmittedSandbox::new().await;
    let id = admitted.lease.sandbox_id.clone();
    let uid = admitted.lease.sandbox_uid;
    let valid = crate::proto::PrepareSandboxRequest {
        sandbox_id: id.clone(),
        uid,
        runtime: "firecracker".into(),
        execution_selection_mode: Mode::NewColdBoot.into(),
        template: Some("base".into()),
        execution_fence: Some(admitted.fence.clone()),
        ..Default::default()
    };
    let foreign_metadata = serde_json::to_string(&SandboxArtifactMetadata::new(
        SandboxId::new(),
        ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]),
        TemplateName::parse("base").unwrap(),
        None,
    ))
    .unwrap();
    let rootfs_size = |mib| {
        pplx_space_common_core::sandbox_storage::validate_rootfs_mib(mib)
            .unwrap_err()
            .to_string()
    };
    let (empty_rootfs, oversized_rootfs) = (rootfs_size(0), rootfs_size(128 * 1024 + 1));
    let invalid_template = TemplateName::parse("../base").unwrap_err().to_string();
    let invalid_hash = ContentHash::from_str("not-a-digest")
        .unwrap_err()
        .to_string();
    let missing_execution = RuntimeInventoryError::MissingRequestedExecution(
        pplx_space_node_runtime_core::runtime_inventory::ExecutionSelectionMode::ExistingColdBoot,
    )
    .to_string();
    let cases = [
        (
            "empty rootfs",
            crate::proto::PrepareSandboxRequest {
                rootfs_size_mb: Some(0),
                ..valid.clone()
            },
            Code::InvalidArgument,
            empty_rootfs.as_str(),
        ),
        (
            "oversized rootfs",
            crate::proto::PrepareSandboxRequest {
                rootfs_size_mb: Some(128 * 1024 + 1),
                ..valid.clone()
            },
            Code::InvalidArgument,
            oversized_rootfs.as_str(),
        ),
        (
            "unspecified selection mode",
            crate::proto::PrepareSandboxRequest {
                execution_selection_mode: Mode::Unspecified.into(),
                ..valid.clone()
            },
            Code::InvalidArgument,
            "execution_selection_mode is required",
        ),
        (
            "existing cold boot without its recorded execution",
            crate::proto::PrepareSandboxRequest {
                execution_selection_mode: Mode::ExistingColdBoot.into(),
                ..valid.clone()
            },
            Code::FailedPrecondition,
            missing_execution.as_str(),
        ),
        (
            "malformed metadata",
            crate::proto::PrepareSandboxRequest {
                metadata_json: "{".into(),
                ..valid.clone()
            },
            Code::InvalidArgument,
            "invalid sandbox metadata: ",
        ),
        (
            "metadata of another sandbox",
            crate::proto::PrepareSandboxRequest {
                metadata_json: foreign_metadata.clone(),
                ..valid.clone()
            },
            Code::InvalidArgument,
            "sandbox metadata id does not match request",
        ),
        (
            "invalid artifact template",
            crate::proto::PrepareSandboxRequest {
                artifact_template: Some("../base".into()),
                ..valid.clone()
            },
            Code::InvalidArgument,
            invalid_template.as_str(),
        ),
        (
            "invalid content hash",
            crate::proto::PrepareSandboxRequest {
                content_hash: Some("not-a-digest".into()),
                ..valid.clone()
            },
            Code::InvalidArgument,
            invalid_hash.as_str(),
        ),
        (
            "unallocated sandbox",
            crate::proto::PrepareSandboxRequest {
                sandbox_id: admitted.unallocated_id.clone(),
                execution_fence: Some(admitted.unallocated_fence.clone()),
                ..valid.clone()
            },
            Code::NotFound,
            "sandbox not allocated",
        ),
        (
            "uid of another slot",
            crate::proto::PrepareSandboxRequest {
                uid: uid + 1,
                ..valid.clone()
            },
            Code::PermissionDenied,
            "does not match allocated slot UID",
        ),
        (
            "uid outside the sandbox range",
            crate::proto::PrepareSandboxRequest {
                uid: 0,
                ..valid.clone()
            },
            Code::PermissionDenied,
            "must be >=",
        ),
        (
            "superseded execution",
            crate::proto::PrepareSandboxRequest {
                execution_fence: Some(stale_test_fence()),
                ..valid.clone()
            },
            Code::FailedPrecondition,
            "sandbox execution identity is missing or superseded",
        ),
        (
            "template without its resolved directory",
            valid.clone(),
            Code::InvalidArgument,
            "template_dir is required",
        ),
    ];
    let mut mismatches = Vec::new();
    for (case, request, code, message) in cases {
        let outcome = admitted
            .service
            .prepare_sandbox(root_request(request))
            .await;
        mismatches.extend(refusal_mismatch(case, outcome, code, message));
        if admitted
            .record_disturbance(
                case,
                crate::lifecycle_limiter::LifecycleMethod::PrepareSandbox,
                &mut mismatches,
            )
            .is_break()
        {
            break;
        }
        if admitted.service.disk_admission.pending_reservation_bytes() != 0 {
            mismatches.push(format!("{case}: refusal leaked a disk reservation"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}

// Sidecar launch is refused before its identity is resolved from the node's
// run directory and cgroups; the valid request shows that boundary. Only
// net-gateway is used once a lease exists: spaced-host would read the
// registered Firecracker PID from the run directory during validation.
#[tokio::test]
async fn sidecar_launch_refuses_invalid_requests_before_resolving_its_identity() {
    use crate::service::tests::service_tests::{refusal_mismatch, root_request, stale_test_fence};
    use pplx_space_common_core::host::linux::paths;
    use tonic::Code;

    let admitted = AdmittedSandbox::new().await;
    let id = admitted.lease.sandbox_id.clone();
    let uid = admitted.lease.sandbox_uid;
    let gateway_args = |socket: String, sandbox_id: Option<&str>| {
        let mut args = vec!["--socket".to_string(), socket];
        if let Some(sandbox_id) = sandbox_id {
            args.extend(["--sandbox-id".to_string(), sandbox_id.to_string()]);
        }
        args
    };
    let valid = crate::proto::LaunchRequest {
        sandbox_id: id.clone(),
        program: paths::NET_GATEWAY_SVC_BIN.into(),
        args: gateway_args(paths::net_gateway_control_socket(&id), Some(&id)),
        uid,
        gid: uid,
        execution_fence: Some(admitted.fence.clone()),
        ..Default::default()
    };
    let cases = [
        (
            "hex sandbox id that is not a UUID",
            crate::proto::LaunchRequest {
                sandbox_id: "deadbeef".into(),
                ..valid.clone()
            },
            Code::InvalidArgument,
            "sandbox_id",
        ),
        (
            "program outside the allowlist",
            crate::proto::LaunchRequest {
                program: "/bin/sh".into(),
                ..valid.clone()
            },
            Code::PermissionDenied,
            "program '/bin/sh' not in allowed list",
        ),
        (
            "superseded execution",
            crate::proto::LaunchRequest {
                execution_fence: Some(stale_test_fence()),
                ..valid.clone()
            },
            Code::FailedPrecondition,
            "sandbox execution identity is missing or superseded",
        ),
        (
            "unallocated sandbox",
            crate::proto::LaunchRequest {
                sandbox_id: admitted.unallocated_id.clone(),
                execution_fence: Some(admitted.unallocated_fence.clone()),
                ..valid.clone()
            },
            Code::NotFound,
            "sandbox not allocated",
        ),
        (
            "uid of another slot",
            crate::proto::LaunchRequest {
                uid: uid + 1,
                gid: uid + 1,
                ..valid.clone()
            },
            Code::PermissionDenied,
            "does not match allocated slot UID",
        ),
        (
            "gid differs from the allocated uid",
            crate::proto::LaunchRequest {
                gid: uid + 1,
                ..valid.clone()
            },
            Code::PermissionDenied,
            "must match UID",
        ),
        (
            "gateway socket of another sandbox",
            crate::proto::LaunchRequest {
                args: gateway_args(
                    paths::net_gateway_control_socket(&admitted.unallocated_id),
                    Some(&id),
                ),
                ..valid.clone()
            },
            Code::PermissionDenied,
            "launch --socket must be",
        ),
        (
            "gateway without its sandbox id",
            crate::proto::LaunchRequest {
                args: gateway_args(paths::net_gateway_control_socket(&id), None),
                ..valid.clone()
            },
            Code::InvalidArgument,
            "launch requires --sandbox-id <value>",
        ),
        (
            "valid request",
            valid.clone(),
            HOST_WORK_REFUSED.0,
            HOST_WORK_REFUSED.1,
        ),
    ];
    let mut mismatches = Vec::new();
    for (case, request, code, message) in cases {
        let outcome = admitted.service.launch(root_request(request)).await;
        mismatches.extend(refusal_mismatch(case, outcome, code, message));
        if admitted
            .record_disturbance(
                case,
                crate::lifecycle_limiter::LifecycleMethod::Launch,
                &mut mismatches,
            )
            .is_break()
        {
            break;
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}

#[test]
fn instance_rootfs_opens_only_a_single_link_regular_file_without_following_links() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let elsewhere = root.join("elsewhere.ext4");
    std::fs::write(&elsewhere, b"other").unwrap();

    let instance = root.join("instance");
    std::fs::create_dir(&instance).unwrap();
    std::fs::write(instance.join("rootfs.ext4"), b"rootfs").unwrap();
    let file = super::open_instance_rootfs(&instance).unwrap();
    assert_eq!(file.metadata().unwrap().len(), 6);

    let linked_file = root.join("linked-file");
    std::fs::create_dir(&linked_file).unwrap();
    std::os::unix::fs::symlink(&elsewhere, linked_file.join("rootfs.ext4")).unwrap();
    super::open_instance_rootfs(&linked_file).unwrap_err();

    let linked_dir = root.join("linked-dir");
    std::os::unix::fs::symlink(&instance, &linked_dir).unwrap();
    super::open_instance_rootfs(&linked_dir).unwrap_err();

    let hard_linked = root.join("hard-linked");
    std::fs::create_dir(&hard_linked).unwrap();
    std::fs::hard_link(&elsewhere, hard_linked.join("rootfs.ext4")).unwrap();
    super::open_instance_rootfs(&hard_linked).unwrap_err();
}

#[test]
fn retained_instance_repair_grants_traversal_without_write_and_preserves_files() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let root = tempfile::tempdir().unwrap();
    let instance = root.path().join("instance");
    std::fs::create_dir(&instance).unwrap();
    std::fs::write(instance.join("rootfs.ext4"), b"retained disk").unwrap();
    let before = std::fs::metadata(&instance).unwrap();
    std::fs::set_permissions(&instance, std::fs::Permissions::from_mode(0o700)).unwrap();
    // The publisher runs as root; this probe only mutates its own fixture.
    if !pplx_space_test_env::root_or_skip("repair the shared instance group") {
        return;
    }
    super::super::ensure_sandbox_instance_traversal(&instance).unwrap();
    let after = std::fs::metadata(&instance).unwrap();
    assert_eq!(after.mode() & 0o7777, 0o2710);
    assert_eq!(after.uid(), before.uid());
    assert_eq!(after.ino(), before.ino());
    assert_eq!(
        std::fs::read(instance.join("rootfs.ext4")).unwrap(),
        b"retained disk"
    );
    super::super::ensure_sandbox_instance_traversal(root.path().join("missing")).unwrap();
    let alias = root.path().join("alias");
    std::os::unix::fs::symlink(&instance, &alias).unwrap();
    super::super::ensure_sandbox_instance_traversal(alias).unwrap_err();
}

#[test]
fn protected_launch_admission_preserves_binding_and_restore_rules() {
    use super::ExecutionSourceIntegrity::{CatalogSha256, FsVerity, UnverifiedExternal};
    let reference =
        pplx_space_node_runtime_core::catalog::InitialEnvironment::for_architecture(Arch::X86_64)
            .unwrap()
            .definition()
            .unwrap()
            .reference();
    for kind in [RuntimeKind::Firecracker, RuntimeKind::CloudHypervisor] {
        for runtime in [UnverifiedExternal, CatalogSha256, FsVerity] {
            for component in [UnverifiedExternal, CatalogSha256, FsVerity] {
                for restore in [false, true] {
                    let expected = match (kind, runtime, component, restore) {
                        (_, UnverifiedExternal, _, _)
                        | (RuntimeKind::Firecracker, CatalogSha256, _, _) => Ok(None),
                        (RuntimeKind::Firecracker, _, _, true) => Ok(Some(reference)),
                        (_, _, UnverifiedExternal, _)
                        | (RuntimeKind::Firecracker, FsVerity, CatalogSha256, false) => {
                            Err(tonic::Code::FailedPrecondition)
                        }
                        _ => Ok(Some(reference)),
                    };
                    assert_eq!(
                        super::protected_launch_environment(
                            kind,
                            runtime,
                            Some(reference),
                            Some(reference),
                            restore,
                            [component]
                        )
                        .map_err(|e| e.code()),
                        expected,
                        "{kind:?} {runtime:?} {component:?} restore={restore}",
                    );
                }
            }
        }
    }
    for (kind, integrity) in [
        (RuntimeKind::Firecracker, FsVerity),
        (RuntimeKind::CloudHypervisor, FsVerity),
        (RuntimeKind::CloudHypervisor, CatalogSha256),
    ] {
        assert_eq!(
            super::protected_launch_environment(
                kind,
                integrity,
                None,
                Some(reference),
                false,
                [integrity]
            )
            .unwrap_err()
            .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            super::protected_launch_environment(kind, integrity, None, None, false, [integrity])
                .unwrap(),
            None
        );
        assert_eq!(
            super::protected_launch_environment(
                kind,
                integrity,
                Some(reference),
                None,
                false,
                [integrity]
            )
            .unwrap(),
            Some(reference)
        );
    }
}

#[tokio::test]
async fn wrong_launch_acknowledgement_refuses_and_stops_the_execution() {
    use crate::runtime_supervisor_client::tests::{SupervisorScript, scripted_in_process_client};
    use pplx_space_node_runtime_core::proto;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let reference =
        pplx_space_node_runtime_core::catalog::InitialEnvironment::for_architecture(Arch::X86_64)
            .unwrap()
            .definition()
            .unwrap()
            .reference();
    for accepted in [
        reference.to_string(),
        String::new(),
        format!("sha256:{}", "ff".repeat(32)),
    ] {
        let (mut service, lease) = allocated_service().await;
        let fence =
            super::super::tests::service_tests::bind_test_execution(&service, &lease.sandbox_id);
        let stops = Arc::new(AtomicUsize::new(0));
        let stop_calls = stops.clone();
        let sandbox_id = lease.sandbox_id.clone();
        let expected_execution = fence.execution_id.clone();
        let launch_sandbox = sandbox_id.parse().unwrap();
        let matches = accepted == reference.to_string();
        service.runtime_supervisor = scripted_in_process_client(SupervisorScript {
            launch: Some(Arc::new(move |request| {
                assert_eq!(request.sandbox_id, launch_sandbox);
                assert_eq!(request.environment_reference, Some(reference));
                Ok(proto::LaunchRuntimeResponse {
                    pid: 0,
                    accepted_environment_reference: accepted.clone(),
                })
            })),
            stop_runtime: Some(Arc::new(move |request| {
                assert_eq!(request.sandbox_id, sandbox_id);
                assert_eq!(request.execution_id, expected_execution);
                assert!(request.immediate_kill);
                stop_calls.fetch_add(1, Ordering::SeqCst);
                Ok(proto::StopSandboxRuntimeResponse::default())
            })),
            ..Default::default()
        });
        let result = service
            .launch_admitted_runtime(
                &lease.sandbox_id,
                proto::LaunchRuntimeRequest {
                    sandbox_id: lease.sandbox_id.clone(),
                    runtime: pplx_space_execution_common::proto::RuntimeKind::Firecracker as i32,
                    execution_id: fence.execution_id,
                    launcher_binary: Some("/runtime/bin/jailer".into()),
                    runtime_binary: "/runtime/bin/firecracker".into(),
                    workspace_root: "/runtime/jails".into(),
                    network_namespace: "/runtime/netns".into(),
                    verified_runtime: Some(proto::VerifiedRuntimeLaunch {
                        sandbox_id: lease.sandbox_id.clone(),
                        environment_reference: reference.to_string(),
                        integrity_requirement:
                            proto::ArtifactIntegrityRequirement::AuthenticatedImmutableSources
                                .into(),
                    }),
                    ..Default::default()
                },
            )
            .await;
        if matches {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
        }
        assert_eq!(stops.load(Ordering::SeqCst), usize::from(!matches));
        assert!(
            service
                .pool
                .get(&lease.sandbox_id)
                .unwrap()
                .unwrap()
                .pid
                .is_none()
        );
    }
}
