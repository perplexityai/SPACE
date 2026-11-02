use super::cleanup_helpers::*;
use super::network_helpers::*;
use super::validation::*;
use super::*;

#[cfg(test)]
pub(super) mod service_tests {
    // Fault injection at the ownership-walker boundary; filesystem semantics
    // are covered by permission_tree's real descriptor-relative tests.
    macro_rules! mock_ownership {
        () => {
            fn set_readonly(
                &mut self,
                path: &std::path::Path,
                readonly: bool,
            ) -> Result<(), CleanupCommandError> {
                self.run(
                    "btrfs",
                    &[
                        "property",
                        "set",
                        "-ts",
                        &path.to_string_lossy(),
                        "ro",
                        if readonly { "true" } else { "false" },
                    ],
                )
                .map(|_| ())
            }
            fn rewrite_ownership(
                &mut self,
                root: &str,
                prune: bool,
                uid: Option<u32>,
            ) -> Result<(), CleanupCommandError> {
                self.run(
                    "rewrite",
                    &[
                        root,
                        if prune { "prune" } else { "all" },
                        &uid.map_or_else(|| "all".into(), |uid| uid.to_string()),
                    ],
                )
                .map(|_| ())
            }
        };
    }
    use super::*;
    use crate::service::cleanup_inspect::CleanupOperationRegistry;
    use crate::service::runtime::{
        AllocateNewLeaseError, registered_firecracker_pid_with_vm_processes,
    };
    use pplx_space_common_core::types::{LinuxPid, SandboxId};
    use pplx_space_common_svc::PeerIdentity;
    use pplx_space_common_svc::core::host::linux::identities::users;
    use std::str::FromStr;

    #[test]
    fn allocation_storage_failures_are_retryable_at_the_rpc_boundary() {
        for error in [
            crate::slot_pool::SlotStoreError::LockPoisoned,
            crate::slot_pool::SlotStoreError::Database(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
                None,
            )),
        ] {
            assert_eq!(
                allocate_error_to_status(AllocateSlotError::Store(error)).code(),
                tonic::Code::Unavailable
            );
        }
    }

    #[test]
    fn prepared_clone_disposal_retries_filesystem_failure_without_releasing_authority() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        let run = temp.path().join("run");
        std::fs::create_dir(&run).unwrap();
        let req = crate::proto::CleanupPreparedCloneRequest {
            sandbox_id: SandboxId::new().to_string(),
            prepared_destination: None,
        };
        let error = super::super::control::dispose_prepared_clone_resources(&cache, &run, &req)
            .expect_err("a temporarily unavailable cache must not acknowledge disposal");
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert_eq!(
            super::super::control::clone_disposal_status(
                crate::clone_cleanup::CloneDisposalError::BudgetExhausted
            )
            .code(),
            tonic::Code::Unavailable,
        );
        std::fs::create_dir(&cache).unwrap();
        super::super::control::dispose_prepared_clone_resources(&cache, &run, &req).unwrap();
        super::super::control::dispose_prepared_clone_resources(&cache, &run, &req).unwrap();
    }

    #[derive(Default)]
    struct MockCleanupRunner {
        calls: Vec<(String, Vec<String>)>,
        fail_chown: bool,
        delete_cache_on_rewrite: Option<std::path::PathBuf>,
        delete_path_on_rewrite: Option<std::path::PathBuf>,
    }

    impl CleanupCommandRunner for MockCleanupRunner {
        mock_ownership!();
        fn remove_tree(&mut self, path: &std::path::Path) -> Result<(), CleanupCommandError> {
            self.run("artifact-remove", &[&path.to_string_lossy()])
                .map(|_| ())
        }
        fn run(&mut self, program: &str, args: &[&str]) -> Result<String, CleanupCommandError> {
            self.calls.push((
                program.to_string(),
                args.iter().map(|arg| arg.to_string()).collect(),
            ));
            if program == "btrfs" && args.starts_with(&["property", "get", "-ts"]) {
                let path = args.get(3).copied().unwrap_or_default();
                if path.ends_with("readonly") {
                    return Ok("ro=true\n".to_string());
                }
                if path.ends_with("writable") {
                    return Ok("ro=false\n".to_string());
                }
                // A plain-dir "imported" snapshot: `btrfs property get -ts`
                // fails the subvolume getflags ioctl. This must be classified
                // as NotSubvolume (skip the toggle) rather than propagated.
                return Err(CleanupCommandError::NotSubvolume {
                    program: "btrfs".to_string(),
                    args: args.join(" "),
                    status: "exit status: 1".to_string(),
                    detail: "ERROR: Could not get subvolume flags: Invalid argument".to_string(),
                });
            }
            if self.fail_chown && (program == "chown" || program == "rewrite") {
                return Err(CleanupCommandError::FailedWithDetail {
                    program: "chown".to_string(),
                    args: args.join(" "),
                    status: "mock failure".to_string(),
                    detail: "chown failed".to_string(),
                });
            }
            if program == "rewrite"
                && let Some(path) = self.delete_cache_on_rewrite.take()
            {
                let _ = std::fs::remove_dir_all(path);
                return Err(CleanupCommandError::MissingPath {
                    program: "rewrite".to_string(),
                    args: args.join(" "),
                    status: "mock failure".to_string(),
                    detail: "No such file or directory".to_string(),
                });
            }
            if program == "rewrite"
                && let Some(path) = self.delete_path_on_rewrite.take()
            {
                let _ = std::fs::remove_dir_all(path);
                return Err(CleanupCommandError::MissingPath {
                    program: "rewrite".to_string(),
                    args: args.join(" "),
                    status: "mock failure".to_string(),
                    detail: "No such file or directory".to_string(),
                });
            }
            Ok(String::new())
        }
    }

    fn call_names(runner: &MockCleanupRunner) -> Vec<String> {
        fmt_calls(&runner.calls)
    }

    fn fmt_calls(calls: &[(String, Vec<String>)]) -> Vec<String> {
        calls
            .iter()
            .map(|(program, args)| format!("{program} {}", args.join(" ")))
            .collect()
    }

    #[test]
    fn sandbox_uid_gid_validation_accepts_slot_identity() {
        assert_eq!(
            validate_sandbox_uid_gid("sbx", 10_042, 10_042, 10_042).unwrap(),
            10_042
        );
    }

    #[test]
    fn sandbox_uid_gid_validation_rejects_privileged_uid() {
        let err = validate_sandbox_uid_gid("sbx", 0, 0, 10_042).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("UID"));
    }

    #[test]
    fn sandbox_uid_gid_validation_rejects_slot_mismatch() {
        let err = validate_sandbox_uid_gid("sbx", 10_043, 10_043, 10_042).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("allocated slot UID"));
    }

    #[test]
    fn sandbox_uid_gid_validation_rejects_uid_gid_mismatch() {
        let err = validate_sandbox_uid_gid("sbx", 10_042, 10_043, 10_042).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("must match UID"));
    }

    #[test]
    fn sandbox_artifact_dir_shape_rejects_symlink() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        let link = root.path().join("snapshots");
        std::fs::create_dir(&target).unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let err = validate_sandbox_artifact_dir_shape(&link).unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("symlink"));
    }

    #[test]
    fn sandbox_artifact_dir_shape_rejects_file() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("snapshots");
        std::fs::write(&path, b"not a directory").unwrap();

        let err = validate_sandbox_artifact_dir_shape(&path).unwrap_err();

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("not a directory"));
    }

    #[test]
    fn reserved_expose_ports_include_control_plane_ports() {
        assert_eq!(reserved_expose_port_reason(9800), Some("space-node-api"));
        assert_eq!(reserved_expose_port_reason(10250), Some("kubelet-api"));
        assert_eq!(reserved_expose_port_reason(8080), None);
    }

    #[test]
    fn node_ctl_can_read_node_status_only() {
        let authorizer = node_peer_authorizer();
        let node_ctl = PeerIdentity::new(
            users::SPACE_NODECTL.uid,
            users::SPACE_NODECTL.primary_group.gid,
            None,
        );

        authorizer
            .authorize_identity(node_ctl.clone(), "Status", ALLOW_NODE_STATUS)
            .expect("node-ctl needs node status access to publish scheduling snapshots");
        authorizer
            .authorize_identity(node_ctl.clone(), "GetVmState", ALLOW_NODE_STATUS)
            .expect("node-ctl may inspect runtime state without gaining mutation access");
        authorizer
            .authorize_identity(node_ctl.clone(), "GetExecutionInventory", ALLOW_NODE_STATUS)
            .expect("node-ctl needs runtime compatibility evidence");

        let error = authorizer
            .authorize_identity(node_ctl, "SetNetworkPoolDrain", ALLOW_NODE_ADMIN)
            .expect_err("node-ctl must not get broad node-manager admin access");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn generic_vm_routes_authorize_before_validating_inputs() {
        use crate::proto::node_manager_server::NodeManager as Rpc;

        fn request<T: Default>(peer: &PeerIdentity) -> Request<T> {
            let mut request = Request::new(T::default());
            request.extensions_mut().insert(peer.clone());
            request
        }

        let service = service_for_unit_tests().await;
        for (peer, mutation_code) in [
            (PeerIdentity::new(0, 0, None), tonic::Code::InvalidArgument),
            (
                PeerIdentity::new(
                    users::SPACE_NODECTL.uid,
                    users::SPACE_NODECTL.primary_group.gid,
                    None,
                ),
                tonic::Code::PermissionDenied,
            ),
        ] {
            for (method, error, expected) in [
                (
                    "GetVmState",
                    Rpc::get_vm_state(&service, request(&peer))
                        .await
                        .unwrap_err(),
                    tonic::Code::InvalidArgument,
                ),
                (
                    "CaptureVmState",
                    Rpc::capture_vm_state(&service, request(&peer))
                        .await
                        .unwrap_err(),
                    mutation_code,
                ),
                (
                    "RestoreVmState",
                    Rpc::restore_vm_state(&service, request(&peer))
                        .await
                        .unwrap_err(),
                    mutation_code,
                ),
            ] {
                assert_eq!(error.code(), expected, "{method}");
                if expected == tonic::Code::PermissionDenied {
                    assert!(error.message().ends_with(method));
                }
            }
        }
    }

    #[tokio::test]
    async fn sandbox_manager_can_negotiate_and_inspect_send_sources_without_lease_access() {
        let service = service_for_unit_tests().await;
        for (user, allowed) in [
            (users::SPACE_SBXMGR, true),
            (users::SPACE_VOLMGR, true),
            (users::SPACE_NODEAPI, false),
        ] {
            let mut request = Request::new(crate::proto::GetBtrfsSendCapabilitiesRequest {});
            request.extensions_mut().insert(PeerIdentity::new(
                user.uid,
                user.primary_group.gid,
                None,
            ));
            let result = service.get_btrfs_send_capabilities(request).await;
            assert_eq!(result.is_ok(), allowed, "{}", user.name);
        }
        for (user, expected) in [
            (users::SPACE_SBXMGR, tonic::Code::InvalidArgument),
            (users::SPACE_VOLMGR, tonic::Code::PermissionDenied),
            (users::SPACE_NODEAPI, tonic::Code::PermissionDenied),
        ] {
            let mut request = Request::new(crate::proto::BtrfsInspectSendSourceRequest {
                sandbox_id: "invalid".into(),
                artifact_dir: String::new(),
                source_id: String::new(),
            });
            request.extensions_mut().insert(PeerIdentity::new(
                user.uid,
                user.primary_group.gid,
                None,
            ));
            let error = service
                .btrfs_inspect_send_source(request)
                .await
                .unwrap_err();
            assert_eq!(error.code(), expected, "{}", user.name);
        }
        // Negotiation does not grant the lease operations.
        let mut request = Request::new(crate::proto::BtrfsListSendSourcesRequest {});
        request.extensions_mut().insert(PeerIdentity::new(
            users::SPACE_SBXMGR.uid,
            users::SPACE_SBXMGR.primary_group.gid,
            None,
        ));
        assert_eq!(
            service
                .btrfs_list_send_sources(request)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[tokio::test]
    async fn send_source_inspection_has_its_own_lane() {
        let service = service_for_unit_tests().await;
        let id = SandboxId::new();
        let inspect = |service: NodeManagerService| async move {
            let mut request = Request::new(crate::proto::BtrfsInspectSendSourceRequest {
                sandbox_id: id.to_string(),
                artifact_dir: format!(
                    "{}/{id}/snapshot",
                    pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR
                ),
                source_id: uuid::Uuid::now_v7().to_string(),
            });
            request.extensions_mut().insert(PeerIdentity::new(
                users::SPACE_SBXMGR.uid,
                users::SPACE_SBXMGR.primary_group.gid,
                None,
            ));
            service
                .btrfs_inspect_send_source(request)
                .await
                .unwrap_err()
                .code()
        };
        // A saturated diagnostic lane does not block inspection.
        let probes = service
            .blocking_probe_slots
            .clone()
            .try_acquire_many_owned(MAX_BLOCKING_PROBES as u32)
            .unwrap();
        assert_ne!(
            inspect(service.clone()).await,
            tonic::Code::ResourceExhausted
        );
        drop(probes);
        // Stuck inspections exhaust only their own lane.
        let inspections = service
            .send_source_inspection_slots
            .clone()
            .try_acquire_many_owned(MAX_SEND_SOURCE_INSPECTIONS as u32)
            .unwrap();
        assert_eq!(
            inspect(service.clone()).await,
            tonic::Code::ResourceExhausted
        );
        assert_eq!(
            service.blocking_probe_slots.available_permits(),
            MAX_BLOCKING_PROBES
        );
        drop(inspections);
    }

    #[tokio::test]
    async fn volume_manager_can_read_send_sources_without_general_node_authority() {
        let service = service_for_unit_tests().await;
        for (user, expected) in [
            (users::SPACE_VOLMGR, tonic::Code::InvalidArgument),
            (users::SPACE_SBXMGR, tonic::Code::PermissionDenied),
            (users::SPACE_NODEAPI, tonic::Code::PermissionDenied),
        ] {
            let mut request = Request::new(crate::proto::BtrfsSendLease {
                lease_id: "invalid".into(),
            });
            request.extensions_mut().insert(PeerIdentity::new(
                user.uid,
                user.primary_group.gid,
                None,
            ));
            let error = service
                .btrfs_read_send_source(request)
                .await
                .map(|_| ())
                .unwrap_err();
            assert_eq!(error.code(), expected, "{}", user.name);
        }
        let peer = PeerIdentity::new(
            users::SPACE_VOLMGR.uid,
            users::SPACE_VOLMGR.primary_group.gid,
            None,
        );
        let error = node_peer_authorizer()
            .authorize_identity(peer, "SetNetworkPoolDrain", ALLOW_NODE_ADMIN)
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn node_ctl_can_manage_per_sandbox_port_rpcs() {
        // SPACE_NODECTL is permitted to call the per-sandbox port RPCs
        // (ExposePort/UnexposePort DNAT and AllowPort/DenyPort firewall
        // allowlist) so an operator can manage sandbox port state from the
        // CLI without escalating to other admin operations.
        let authorizer = node_peer_authorizer();
        let node_ctl = PeerIdentity::new(
            users::SPACE_NODECTL.uid,
            users::SPACE_NODECTL.primary_group.gid,
            None,
        );

        for operation in ["ExposePort", "UnexposePort", "AllowPort", "DenyPort"] {
            authorizer
                .authorize_identity(node_ctl.clone(), operation, ALLOW_NODE_ADMIN_OR_NODEAPI)
                .unwrap_or_else(|err| panic!("node-ctl must be authorized for {operation}: {err}"));
        }
    }

    #[test]
    fn memory_admission_parses_meminfo_as_whole_mib() {
        let contents = "MemTotal:       67108865 kB\nMemAvailable:    9568769 kB\n";

        assert_eq!(
            crate::memory_admission::parse_meminfo(contents).unwrap(),
            crate::memory_admission::NodeMemorySnapshot {
                total_mib: 65_536,
                available_mib: 9344,
            }
        );
    }

    #[test]
    fn memory_admission_fails_closed_without_memtotal_signal() {
        let error =
            crate::memory_admission::parse_meminfo("MemAvailable: 9568769 kB\n").unwrap_err();

        assert!(matches!(
            error,
            crate::memory_admission::NodeMemoryCapacityError::Missing { field: "MemTotal:" }
        ));
    }

    #[test]
    fn allocation_demand_rejects_zero_and_clamps_to_the_configured_cap() {
        let config = SandboxCgroupConfig {
            vcpu_count: Some(2),
            memory_mib: Some(8320),
        };

        assert_eq!(
            config.allocation_memory_mib(0).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(config.allocation_memory_mib(9000).unwrap(), 8320);
        assert_eq!(config.allocation_memory_mib(2176).unwrap(), 2176);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_new_lease_admission_allows_only_one_reserved_cap() {
        let root = tempfile::tempdir().unwrap();
        let meminfo_path = root.path().join("meminfo");
        std::fs::write(
            &meminfo_path,
            "MemTotal:       18086912 kB\nMemAvailable:   18086912 kB\n",
        )
        .unwrap();
        let cgroup_base = root.path().join("cgroups");
        std::fs::create_dir(&cgroup_base).unwrap();

        let mut service = service_for_unit_tests().await;
        service.pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(2));
        service.pool.set_all_slots_ready_for_test();
        service.memory_admission_config = MemoryAdmissionConfig { reserve_mib: 1024 };
        service.memory_admission_sources = MemoryAdmissionSources::new(meminfo_path, cgroup_base);
        let first_id = SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap();
        let second_id = SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3b4b5").unwrap();

        let (first, second) = tokio::join!(
            service.allocate_new_lease(first_id, None, 8320),
            service.allocate_new_lease(second_id, None, 8320)
        );

        let outcomes = [first, second];
        assert_eq!(
            outcomes.iter().filter(|outcome| outcome.is_ok()).count(),
            1,
            "exactly one allocation should be admitted: {outcomes:?}"
        );
        assert!(outcomes.iter().any(|outcome| matches!(
            outcome,
            Err(AllocateNewLeaseError::MemoryAdmission(
                MemoryAdmissionEvaluationError::Admission(
                    MemoryAdmissionError::InsufficientCapacity {
                        existing_growth_reserve_mib: 8320,
                        ..
                    }
                )
            ))
        )));
        assert_eq!(service.pool.active_memory_reservations().unwrap().len(), 1);
    }

    pub(in crate::service) fn bind_test_execution(
        service: &NodeManagerService,
        sandbox_id: &str,
    ) -> crate::proto::ExecutionFence {
        let fence = pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
            None,
        );
        service.pool.bind_execution(sandbox_id, fence).unwrap();
        fence.into()
    }

    /// An execution fence no sandbox is bound to.
    pub(in crate::service) fn stale_test_fence() -> crate::proto::ExecutionFence {
        pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
            None,
        )
        .into()
    }

    /// Claims a pooled lease without memory admission.
    pub(in crate::service) fn allocate_test_lease(
        service: &NodeManagerService,
        sandbox_id: &str,
    ) -> crate::net::SlotInfo {
        service.pool.set_all_slots_ready_for_test();
        match service
            .pool
            .claim_allocation(
                sandbox_id,
                None,
                Some(crate::slot_pool::NewLeaseMemoryReservation {
                    memory_reservation_mib: 8320,
                }),
            )
            .unwrap()
        {
            crate::slot_pool::ClaimedSlotAllocation::Complete(lease) => lease,
            crate::slot_pool::ClaimedSlotAllocation::Ephemeral(_) => {
                panic!("a ready pooled slot completes its claim synchronously")
            }
        }
    }

    /// Root is authorized for every node-manager RPC without depending on the
    /// authorizer's service-account lookups.
    pub(in crate::service) fn root_request<T>(message: T) -> Request<T> {
        let mut request = Request::new(message);
        request
            .extensions_mut()
            .insert(PeerIdentity::new(0, 0, None));
        request
    }

    /// How a table row's outcome differs from the expected refusal. Tables
    /// collect every row's mismatch, so one regression reports each row it
    /// opens rather than stopping at the first.
    pub(in crate::service) fn refusal_mismatch<T>(
        case: &str,
        outcome: Result<T, Status>,
        code: tonic::Code,
        message: &str,
    ) -> Option<String> {
        match outcome {
            Ok(_) => Some(format!("{case}: accepted")),
            Err(error) if error.code() == code && error.message().contains(message) => None,
            Err(error) => Some(format!("{case}: {error}")),
        }
    }

    /// Makes every blocking host section of `service` refuse before it runs.
    /// `off_runtime` checks the artifact-work owner first, and this owner's
    /// cancellation fires when `run_artifact_mutation` returns. A request that
    /// passes validation therefore stops with `Cancelled` at its first jail,
    /// cache or run-directory step, whatever the test's privileges. The probe
    /// section proves the refusal comes before the section: `off_runtime`
    /// checks the owner again afterwards with the same `Cancelled`, so the
    /// outcome alone cannot show that a host step never ran.
    pub(in crate::service) async fn refuse_host_work(service: &mut NodeManagerService) {
        let owner = service
            .run_artifact_mutation(&SandboxId::new().to_string(), |service| async move {
                Ok(service.artifact_work.clone().expect("artifact work owner"))
            })
            .await
            .unwrap();
        assert_eq!(
            owner.check().unwrap_err().code(),
            tonic::Code::Cancelled,
            "the owner must already refuse"
        );
        service.artifact_work = Some(owner);

        let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let section_ran = std::sync::Arc::clone(&ran);
        let outcome = service
            .off_runtime("refuse_host_work probe", move || {
                section_ran.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            })
            .await;
        assert_eq!(
            outcome.unwrap_err().code(),
            tonic::Code::Cancelled,
            "a host section must refuse"
        );
        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "a refused host section must not run"
        );
    }

    /// A node manager whose process-boundary dependencies belong to the test
    /// and refuse until it installs its own. Runtime discovery and memory
    /// admission read paths under a vacant temporary directory, so both fail
    /// closed instead of reading the node's installation, `/proc/meminfo` or
    /// its cgroups. Every runtime-supervisor RPC fails with `Unimplemented`
    /// without contacting a supervisor. The pool records its node-status marks
    /// instead of notifying node-ctl. Lifecycle limits are the compiled-in
    /// ones, whatever `SPACE_NODEMGR_LIMIT_*` the runner sets.
    pub(in crate::service) async fn service_for_unit_tests() -> NodeManagerService {
        let vacant = vacant_test_root();
        NodeManagerService {
            guest_networks: Default::default(),
            guest_access: Default::default(),
            pool: NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(1)),
            peer_authorizer: node_peer_authorizer(),
            sandbox_cgroup_config: SandboxCgroupConfig::default(),
            disk_io_ceiling: Default::default(),
            memory_admission_config: MemoryAdmissionConfig::default(),
            memory_admission_sources: MemoryAdmissionSources::new(
                vacant.join("meminfo"),
                vacant.join("firecracker-cgroups"),
            ),
            memory_admission_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
            sandbox_cgroup_gate_locks: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            sandbox_vm_cgroup_locks: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            sandbox_sidecar_cgroup_locks: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            sandbox_device_stack_locks: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            sandbox_artifact_locks: Default::default(),
            send_source_locks: Default::default(),
            send_source_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(4)),
            artifact_work: None,
            device_op_tasks: tokio_util::task::TaskTracker::new(),
            blocking_probe_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(
                MAX_BLOCKING_PROBES,
            )),
            send_source_inspection_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(
                MAX_SEND_SOURCE_INSPECTIONS,
            )),
            cache_artifact_delete_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(
                MAX_CACHE_ARTIFACT_DELETES,
            )),
            runtime_supervisor: crate::runtime_supervisor_client::tests::refusing_client(),
            runtime_inventory: std::sync::Arc::new(arc_swap::ArcSwapOption::empty()),
            artifact_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            runtime_inventory_refresh_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
            runtime_paths: crate::config::RuntimePaths {
                vendor: vacant.join("vendor"),
                assets: vacant.join("assets"),
                kernels: vacant.join("kernels"),
                store: crate::config::ArtifactStorePaths {
                    releases: [(
                        pplx_space_common_core::types::RuntimeKind::Firecracker,
                        vacant.join("store/releases"),
                    )]
                    .into(),
                    kernels: vacant.join("store/kernels"),
                    staging: vacant.join("staging"),
                    catalog: vacant.join("catalog"),
                },
            },
            guest_console_logs: false,
            execution_policy: std::sync::Arc::new(
                super::execution_policy::ExecutionPolicyState::load(
                    pplx_space_node_runtime_core::selection::ExecutionPolicy::parse(
                        None,
                        None,
                        &pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(
                            pplx_space_common_core::types::Arch::X86_64,
                        )
                        .unwrap(),
                    )
                    .unwrap(),
                    vacant.join("execution-policy.json"),
                ),
            ),
            lifecycle_limiter: std::sync::Arc::new(
                crate::lifecycle_limiter::tests::default_limiter(),
            ),
            disk_admission: std::sync::Arc::new(
                crate::disk_admission::DiskAdmission::new(
                    pplx_space_common_core::host::disk_admission::CacheDiskAdmissionConfig {
                        enabled: false,
                        ..Default::default()
                    },
                )
                .await,
            ),
            prepared_rootfs_reservations:
                super::prepared_rootfs::PreparedRootfsReservations::default(),
            disk_usage_cache: node_disk_metrics::SandboxDiskUsageCache::default(),
            cleanup_operations: CleanupOperationRegistry::default(),
            orphan_inventory: orphan_inventory::OrphanInventory::default(),
            snapshot_inventory: snapshot_inventory::SnapshotInventory::default(),
        }
    }

    /// A unique path under the system temporary directory that names nothing:
    /// the directory is created only to reserve its name, then removed.
    fn vacant_test_root() -> std::path::PathBuf {
        let root = tempfile::Builder::new()
            .prefix("space-node-mgr-unit-")
            .tempdir()
            .unwrap();
        let path = root.path().to_path_buf();
        root.close().unwrap();
        path
    }

    /// A Firecracker installation and guest kernel under temporary roots, so
    /// runtime discovery never reads the node's fixed installation paths.
    pub(in crate::service) fn installed_runtime() -> (tempfile::TempDir, crate::config::RuntimePaths)
    {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let root = tempfile::tempdir().unwrap();
        let vendor = root.path().join("vendor");
        let assets = root.path().join("assets");
        let kernels = root.path().join("kernels");
        let bin = vendor.join("v1.16.0/bin");
        std::fs::create_dir_all(&bin).unwrap();
        for name in ["firecracker", "jailer"] {
            let path = bin.join(name);
            std::fs::write(&path, "#!/bin/sh\nexit 42\n").unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        symlink("v1.16.0", vendor.join("default")).unwrap();
        std::fs::create_dir_all(kernels.join("vmlinux-6.1.158")).unwrap();
        std::fs::write(kernels.join("vmlinux-6.1.158/vmlinux.bin"), b"kernel").unwrap();
        for marker in ["default", "legacy-default"] {
            symlink("vmlinux-6.1.158", kernels.join(marker)).unwrap();
        }
        std::fs::create_dir(&assets).unwrap();
        let store = crate::config::ArtifactStorePaths {
            releases: [(
                pplx_space_common_core::types::RuntimeKind::Firecracker,
                root.path().join("store/releases"),
            )]
            .into(),
            kernels: root.path().join("store/kernels"),
            staging: root.path().join("staging"),
            catalog: root.path().join("catalog"),
        };
        (
            root,
            crate::config::RuntimePaths {
                vendor,
                assets,
                kernels,
                store,
            },
        )
    }

    #[tokio::test]
    async fn inspect_sandbox_remains_available_when_diagnostics_are_saturated() {
        let service = service_for_unit_tests().await;
        let _diagnostics = service
            .blocking_probe_slots
            .clone()
            .try_acquire_many_owned(MAX_BLOCKING_PROBES as u32)
            .unwrap();
        let id = SandboxId::new();
        assert!(
            service
                .recorded_execution(id.to_string())
                .await
                .expect("lifecycle metadata must not compete with diagnostics")
                .is_none()
        );
        // Stats supplies the identity even when a sandbox has no live cgroup.
        let mut request = Request::new(InspectSandboxRequest {
            sandbox_id: id.to_string(),
            uuid8: id.label().to_string(),
            uid: Some(10_000),
            ..Default::default()
        });
        request.extensions_mut().insert(PeerIdentity::new(
            users::SPACE_SBXMGR.uid,
            users::SPACE_SBXMGR.primary_group.gid,
            None,
        ));
        let inspection = service
            .inspect_sandbox(request)
            .await
            .expect("Stats must not compete with optional diagnostic work")
            .into_inner();
        assert_eq!(inspection.sandbox_id, id.to_string());

        let mut orphan_request = Request::new(ListOrphanCandidatesRequest::default());
        orphan_request
            .extensions_mut()
            .insert(PeerIdentity::new(0, 0, None));
        assert_eq!(
            service
                .list_orphan_candidates(orphan_request)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted,
            "optional diagnostic work must still respect its admission bound",
        );
    }

    #[tokio::test]
    async fn rpc_boundaries_reject_invalid_paths_and_unauthorized_inventory() {
        let service = service_for_unit_tests().await;
        for id in ["/private/tmp", "../outside", "not-a-sandbox-id"] {
            let mut request = Request::new(ListBtrfsSnapshotsRequest {
                sandbox_id: id.into(),
                skip_size_measurement: true,
                ..Default::default()
            });
            request
                .extensions_mut()
                .insert(PeerIdentity::new(0, 0, None));
            assert_eq!(
                service
                    .list_btrfs_snapshots(request)
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument
            );
        }
        let _diagnostics = service
            .blocking_probe_slots
            .clone()
            .try_acquire_many_owned(MAX_BLOCKING_PROBES as u32)
            .unwrap();
        // Both measured and identity-only requests must enumerate under
        // diagnostic pressure so pruning/delete-all/rollback can proceed.
        for skip_size_measurement in [false, true] {
            let mut request = Request::new(ListBtrfsSnapshotsRequest {
                sandbox_id: SandboxId::new().to_string(),
                skip_size_measurement,
                ..Default::default()
            });
            request
                .extensions_mut()
                .insert(PeerIdentity::new(0, 0, None));
            assert!(
                service
                    .list_btrfs_snapshots(request)
                    .await
                    .unwrap()
                    .into_inner()
                    .snapshots
                    .is_empty()
            );
        }
        let mut denied = Request::new(GetExecutionInventoryRequest {});
        denied.extensions_mut().insert(PeerIdentity::new(
            users::SPACE_TMPLMGR.uid,
            users::SPACE_TMPLMGR.primary_group.gid,
            None,
        ));
        assert_eq!(
            service
                .get_execution_inventory(denied)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn get_disk_usage_returns_completed_census_results() {
        let service = service_for_unit_tests().await;
        let measured_id = SandboxId::new();
        let missing_id = SandboxId::new();
        let unknown_id = SandboxId::new();
        service.disk_usage_cache.replace_for_test([
            (
                measured_id,
                node_disk_metrics::SandboxDiskUsageLookup::Usage(42),
            ),
            (
                missing_id,
                node_disk_metrics::SandboxDiskUsageLookup::Missing,
            ),
        ]);
        let mut request = Request::new(GetDiskUsageRequest {
            sandbox_ids: vec![
                measured_id.to_string(),
                missing_id.to_string(),
                unknown_id.to_string(),
            ],
        });
        request
            .extensions_mut()
            .insert(PeerIdentity::new(0, 0, None));

        let response = service.get_disk_usage(request).await.unwrap().into_inner();

        assert_eq!(
            response.usage_bytes.get(&measured_id.to_string()),
            Some(&42)
        );
        assert_eq!(response.missing_ids.len(), 1);
        assert_eq!(response.missing_ids[0].id, missing_id.to_string());
    }

    /// Snapshot-volume mutations run in a tracked task that owns the canonical
    /// per-sandbox lock. Cancelling the RPC waiter cannot let a retry interleave
    /// before the first operation finishes its teardown.
    #[tokio::test]
    async fn cancelled_device_operation_keeps_the_canonical_lock_until_teardown() {
        use std::sync::Arc;
        use std::time::Duration;

        let service = service_for_unit_tests().await;
        let sandbox_id = SandboxId::new();
        let alternate_spelling = sandbox_id.to_string().to_ascii_uppercase();
        let operation_id = alternate_spelling.parse::<SandboxId>().unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let finish = Arc::new(tokio::sync::Notify::new());

        let caller = {
            let service = service.clone();
            let started = Arc::clone(&started);
            let finish = Arc::clone(&finish);
            tokio::spawn(async move {
                service
                    .run_owned_device_operation(
                        &operation_id.to_string(),
                        crate::lifecycle_limiter::LifecycleMethod::PrepareSandbox,
                        async move {
                            started.notify_one();
                            finish.notified().await;
                            Ok(())
                        },
                    )
                    .await
            })
        };
        started.notified().await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());

        // The canonical spelling resolves to the same lock as the accepted
        // uppercase wire spelling and remains held by the tracked task.
        let retry_lock = service.sandbox_device_stack_lock(&sandbox_id).await;
        assert!(
            Arc::clone(&retry_lock).try_lock_owned().is_err(),
            "retry took the device-stack lock while the cancelled operation held it"
        );

        finish.notify_one();
        let retry_guard =
            tokio::time::timeout(Duration::from_secs(1), Arc::clone(&retry_lock).lock_owned())
                .await
                .expect("retry did not proceed after the first operation released the lock");
        drop(retry_guard);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn vm_and_sidecar_cgroup_launches_can_hold_guards_concurrently() {
        let service = service_for_unit_tests().await;
        let sandbox_id = SandboxId::new().to_string();

        let vm_claim = service.claim_sandbox_work(&sandbox_id).unwrap();
        let vm_guard = service.lock_sandbox_vm_cgroup_launch(&sandbox_id).await;
        let sidecar_claim = service.claim_sandbox_work(&sandbox_id).unwrap();
        let sidecar_guard = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            service.lock_sandbox_sidecar_cgroup_launch(&sandbox_id),
        )
        .await
        .expect("independent cgroup family launch must not wait for VM launch");

        let mut duplicate_vm = Box::pin(service.lock_sandbox_vm_cgroup_launch(&sandbox_id));
        assert!(futures_util::poll!(&mut duplicate_vm).is_pending());
        assert!(service.claim_sandbox_mutation(&sandbox_id).is_err());
        drop(sidecar_guard);
        drop(sidecar_claim);
        assert!(service.claim_sandbox_mutation(&sandbox_id).is_err());
        drop(vm_guard);
        drop(duplicate_vm.await);
        drop(vm_claim);
        drop(service.claim_sandbox_mutation(&sandbox_id).unwrap());
    }

    #[tokio::test]
    async fn startup_rpc_admission_shares_work_and_waits_for_cleanup() {
        let service = service_for_unit_tests().await;
        service.pool.set_all_slots_ready_for_test();
        let sandbox_id = SandboxId::new().to_string();
        let execution_fence = bind_test_execution(&service, &sandbox_id);
        service
            .pool
            .allocate(
                &sandbox_id,
                None,
                Some(crate::slot_pool::NewLeaseMemoryReservation {
                    memory_reservation_mib: 8320,
                }),
            )
            .await
            .unwrap();
        for cleanup in [false, true] {
            let claim = if cleanup {
                service.pool.try_claim_sandbox_mutation(&sandbox_id)
            } else {
                service.pool.try_claim_sandbox_work(&sandbox_id)
            }
            .unwrap();
            let mut prepare = Request::new(PrepareSandboxRequest {
                execution_fence: Some(execution_fence.clone()),
                sandbox_id: sandbox_id.clone(),
                runtime: "invalid-runtime".into(),
                ..Default::default()
            });
            prepare
                .extensions_mut()
                .insert(PeerIdentity::new(0, 0, None));
            let mut load = Request::new(crate::proto::RestoreVmStateRequest {
                execution_fence: Some(execution_fence.clone()),
                sandbox_id: sandbox_id.clone(),
                artifacts: crate::snapshot_request::tests::artifacts(
                    "/vmstate".into(),
                    "/memory".into(),
                ),
                rootfs_rate_limit: Some(crate::proto::DriveRateLimit::default()),
                ..Default::default()
            });
            load.extensions_mut().insert(PeerIdentity::new(0, 0, None));
            let mut prepare = Box::pin(service.prepare_sandbox(prepare));
            let mut load = Box::pin(service.restore_vm_state(load));
            if cleanup {
                // A replacement lease can exist while the old slot's reset
                // still owns the sandbox. Neither RPC may enter host work or
                // turn that short exclusion into a failed activation.
                assert!(futures_util::poll!(&mut prepare).is_pending());
                assert!(futures_util::poll!(&mut load).is_pending());
                drop(
                    service
                        .wait_for_sandbox_work(&SandboxId::new().to_string())
                        .await
                        .unwrap(),
                );
                let blocking_owner = claim.clone();
                drop(claim);
                assert!(futures_util::poll!(&mut prepare).is_pending());
                drop(blocking_owner);
            }
            // Invalid payloads stop before Linux work. Reaching validation
            // proves dispatch proceeds after ownership is actually released.
            for error in [prepare.await.unwrap_err(), load.await.unwrap_err()] {
                assert_eq!(error.code(), tonic::Code::InvalidArgument, "{error}");
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn sandbox_work_wait_is_bounded_and_cancellation_does_not_acquire_ownership() {
        for shutdown in [false, true] {
            let service = service_for_unit_tests().await;
            let sandbox_id = SandboxId::new().to_string();
            let owner = service.claim_sandbox_mutation(&sandbox_id).unwrap();
            let mut abandoned = Box::pin(service.wait_for_sandbox_work(&sandbox_id));
            assert!(futures_util::poll!(&mut abandoned).is_pending());
            drop(abandoned);

            let mut wait = Box::pin(service.wait_for_sandbox_work(&sandbox_id));
            assert!(futures_util::poll!(&mut wait).is_pending());
            if shutdown {
                service.pool.shutdown_token.cancel();
            } else {
                tokio::time::advance(Duration::from_secs(31)).await;
            }
            assert_eq!(wait.await.err().unwrap().code(), tonic::Code::Unavailable);
            assert!(service.pool.try_claim_sandbox_work(&sandbox_id).is_none());
            drop(owner);
            assert!(
                service
                    .pool
                    .try_claim_sandbox_mutation(&sandbox_id)
                    .is_some()
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pool_clone_waits_for_cleanup_ownership_before_touching_files() {
        let service = service_for_unit_tests().await;
        let sandbox_id = SandboxId::new().to_string();
        let cleanup_guard = service.lock_sandbox_cgroups(&sandbox_id).await;
        let mut request = Request::new(CloneSandboxRequest {
            donor_sandbox_id: SandboxId::new().to_string(),
            new_sandbox_id: sandbox_id.clone(),
            template: "seed".to_string(),
            prepared_destination: Some(crate::proto::CloneDirectoryIdentity {
                device: 1,
                inode: 1,
            }),
        });
        request
            .extensions_mut()
            .insert(PeerIdentity::new(0, 0, None));
        let clone = service.clone_sandbox_into_prepared_root(request);
        tokio::pin!(clone);
        assert!(futures_util::poll!(&mut clone).is_pending());
        drop(cleanup_guard);
        // Cleanup has left no destination. Even a late RPC must not recreate
        // it after acquiring the gate, regardless of donor validity.
        let error = clone.await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().starts_with("open clone tree:"));
        assert!(!Path::new(CACHE_DIR).join(&sandbox_id).exists());
        tokio::time::timeout(
            Duration::from_millis(50),
            service.lock_sandbox_cgroups(&sandbox_id),
        )
        .await
        .expect("clone failure must release cleanup ownership");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_clone_retains_both_cleanup_gates_until_blocking_work_finishes() {
        let service = service_for_unit_tests().await;
        let donor = SandboxId::new().to_string();
        let target = SandboxId::new().to_string();
        let (started, running) = tokio::sync::oneshot::channel();
        let (finish, release) = std::sync::mpsc::channel();
        let clone_service = service.clone();
        let clone_donor = donor.clone();
        let clone_target = target.clone();
        let caller = tokio::spawn(async move {
            clone_service
                .run_owned_clone(&clone_donor, &clone_target, move || {
                    let _ = started.send(());
                    // A timeout bounds fixture cleanup even if an assertion fails.
                    release.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(())
                })
                .await
        });
        running.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        service.device_op_tasks.close();
        assert!(!service.device_op_tasks.is_empty());
        for id in [&donor, &target] {
            assert!(service.claim_sandbox_mutation(id).is_err());
            let cleanup = service.lock_sandbox_cgroups(id);
            tokio::pin!(cleanup);
            assert!(futures_util::poll!(&mut cleanup).is_pending());
        }
        finish.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), service.device_op_tasks.wait())
            .await
            .unwrap();
        for id in [&donor, &target] {
            drop(service.claim_sandbox_mutation(id).unwrap());
            tokio::time::timeout(Duration::from_secs(1), service.lock_sandbox_cgroups(id))
                .await
                .expect("completion must release cleanup ownership");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_probe_retains_capacity_until_blocking_work_finishes() {
        let (_runtime, runtime_paths) = installed_runtime();
        let mut service = service_for_unit_tests().await;
        service.runtime_paths = runtime_paths;
        service.blocking_probe_slots = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let (started, running) = tokio::sync::oneshot::channel();
        let (finish, release) = std::sync::mpsc::channel();
        let owner = service.clone();
        let caller = tokio::spawn(async move {
            owner
                .off_runtime_probe("probe", move || {
                    let _ = started.send(());
                    release.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(())
                })
                .await
        });
        running.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let error = service
            .off_runtime_probe("overflow", || -> Result<(), Status> {
                panic!("saturated diagnostics must not queue blocking work")
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        // Prepare and Launch need cold discovery even while diagnostics are full.
        let inventory = service.runtime_inventory().await;
        service.device_op_tasks.close();
        assert!(!service.device_op_tasks.is_empty());
        finish.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), service.device_op_tasks.wait())
            .await
            .unwrap();
        service.off_runtime_probe("retry", || Ok(())).await.unwrap();
        inventory.expect("saturated diagnostics must not refuse cold runtime discovery");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn destructive_cgroup_guard_excludes_both_launch_families() {
        let service = service_for_unit_tests().await;
        let sandbox_id = SandboxId::new().to_string();
        let vm_guard = service.lock_sandbox_vm_cgroup_launch(&sandbox_id).await;
        let exclusive_guard = service.lock_sandbox_cgroups(&sandbox_id);
        tokio::pin!(exclusive_guard);

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut exclusive_guard)
                .await
                .is_err(),
            "destructive guard must wait for in-flight VM launch"
        );
        drop(vm_guard);

        let exclusive_guard =
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut exclusive_guard)
                .await
                .expect("destructive guard should acquire after launch releases");
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                service.lock_sandbox_sidecar_cgroup_launch(&sandbox_id),
            )
            .await
            .is_err(),
            "destructive guard must exclude sidecar launch"
        );
        drop(exclusive_guard);
    }

    #[test]
    fn concrete_sveth_names_ignores_wildcard_policy_rules() {
        let rules = r#"
table inet space {
    chain forward {
        type filter hook forward priority filter; policy accept;
        iifname "sveth-*" oifname "sveth-*" drop comment "block-cross-sandbox"
        iifname "sveth-*" accept
        oifname "sveth-*" accept
    }
}
"#;

        assert!(concrete_sveth_names(rules).is_empty());
    }

    #[test]
    fn concrete_sveth_names_extracts_concrete_interface_rules() {
        let rules = r#"
table inet space {
    chain forward {
        iifname "sveth-abc12345" accept
        oifname "sveth-deadbeef" accept
        iifname "sveth-000017" accept
        oifname "sveth-z9" accept
        oifname "sveth-deadbeef" accept
        iifname "sveth-1" accept
        iifname "sveth-123456789" accept
    }
}
"#;

        assert_eq!(
            concrete_sveth_names(rules),
            vec![
                "sveth-000017".to_string(),
                "sveth-abc12345".to_string(),
                "sveth-deadbeef".to_string(),
                "sveth-z9".to_string()
            ]
        );
    }

    #[test]
    fn btrfs_read_only_property_parser_accepts_expected_values() {
        assert_eq!(parse_btrfs_read_only_property("ro=true\n"), Some(true));
        assert_eq!(parse_btrfs_read_only_property("ro=false\n"), Some(false));
        assert_eq!(
            parse_btrfs_read_only_property("label=foo\nro=true\n"),
            Some(true)
        );
        assert_eq!(parse_btrfs_read_only_property("unknown=true\n"), None);
    }

    #[test]
    fn retained_cache_neutralization_temporarily_makes_readonly_snapshots_writable() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        std::fs::create_dir_all(cache_dir.join("snapshots/readonly")).unwrap();
        std::fs::create_dir_all(cache_dir.join("snapshots/writable")).unwrap();
        std::fs::create_dir_all(cache_dir.join("snapshots/imported")).unwrap();
        let mut runner = MockCleanupRunner::default();

        assert!(neutralize_retained_cache_dir_with_runner(&cache_dir, &mut runner).unwrap());

        let calls = call_names(&runner);
        let set_writable = calls
            .iter()
            .position(|call| {
                call.contains("property set -ts") && call.contains("snapshots/readonly ro false")
            })
            .expect("readonly snapshot should be made writable");
        let chown = calls
            .iter()
            .position(|call| call.starts_with("rewrite "))
            .expect("cache should be chowned");
        let restore_readonly = calls
            .iter()
            .position(|call| {
                call.contains("property set -ts") && call.contains("snapshots/readonly ro true")
            })
            .expect("readonly snapshot should be restored");

        assert!(set_writable < chown);
        assert!(chown < restore_readonly);
        assert!(
            !calls
                .iter()
                .any(|call| call.contains("snapshots/writable ro true"))
        );
        assert!(
            !calls
                .iter()
                .any(|call| call.contains("snapshots/imported ro true"))
        );
    }

    #[test]
    fn retained_cache_uid_neutralization_targets_only_old_uid() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        std::fs::create_dir_all(cache_dir.join("snapshots/readonly")).unwrap();
        let mut runner = MockCleanupRunner::default();

        assert!(
            neutralize_retained_cache_dir_owned_by_uid_with_runner(
                &cache_dir,
                10_042,
                &mut runner,
            )
            .unwrap()
        );

        let calls = call_names(&runner);
        let rewrites: Vec<_> = calls
            .iter()
            .filter(|call| call.starts_with("rewrite "))
            .collect();
        assert_eq!(
            rewrites,
            [
                &format!("rewrite {} prune 10042", cache_dir.display()),
                &format!(
                    "rewrite {} all 10042",
                    cache_dir.join("snapshots/readonly").display()
                ),
            ]
        );
    }

    #[test]
    fn retained_cache_uid_neutralization_tolerates_concurrent_cache_delete() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        std::fs::create_dir_all(cache_dir.join("instance")).unwrap();
        let mut runner = MockCleanupRunner {
            delete_cache_on_rewrite: Some(cache_dir.clone()),
            ..Default::default()
        };

        assert!(
            !neutralize_retained_cache_dir_owned_by_uid_with_runner(
                &cache_dir,
                10_042,
                &mut runner,
            )
            .unwrap()
        );
        assert!(!cache_dir.exists());

        let calls = call_names(&runner);
        assert!(calls.iter().any(|call| call.starts_with("rewrite ")));
    }

    #[test]
    fn retained_cache_uid_neutralization_marks_partial_delete_as_cleanup_race() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        std::fs::create_dir_all(cache_dir.join("instance")).unwrap();
        std::fs::write(cache_dir.join("instance/rootfs.ext4"), b"rootfs").unwrap();
        let mut runner = MockCleanupRunner {
            delete_path_on_rewrite: Some(cache_dir.join("instance")),
            ..Default::default()
        };

        let error =
            neutralize_retained_cache_dir_owned_by_uid_with_runner(&cache_dir, 10_042, &mut runner)
                .unwrap_err();

        assert!(cache_dir.exists());
        assert!(matches!(
            error,
            RetainedCacheCleanupError::ConcurrentDelete { detail }
                if detail.ends_with("No such file or directory")
        ));
    }

    #[test]
    fn retained_cache_neutralization_restores_readonly_snapshots_after_chown_failure() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        std::fs::create_dir_all(cache_dir.join("snapshots/readonly")).unwrap();
        let mut runner = MockCleanupRunner {
            fail_chown: true,
            ..Default::default()
        };

        let error = neutralize_retained_cache_dir_with_runner(&cache_dir, &mut runner).unwrap_err();

        let calls = call_names(&runner);
        assert!(error.ends_with(": chown failed"), "{error}");
        assert!(
            calls
                .iter()
                .any(|call| call.contains("snapshots/readonly ro false"))
        );
        assert!(
            calls
                .iter()
                .any(|call| call.contains("snapshots/readonly ro true"))
        );
    }

    fn rewrite_prune_prefix(args: &[String]) -> Option<String> {
        (args.get(1).map(String::as_str) == Some("prune"))
            .then(|| format!("{}/snapshots/", args[0]))
    }

    // Simulates a d-stop snapshot published by a concurrent CreateBtrfsSnapshot
    // *after* the reset enumerated the snapshots dir: it is read-only and was
    // never made writable. A ownership rewrite that descends into it returns EROFS,
    // exactly as the kernel does on a read-only btrfs subvolume — unless the
    // pass pruned the snapshots subtree.
    struct ToctouSnapshotRunner {
        calls: Vec<(String, Vec<String>)>,
        late_snapshot: String,
        injected: bool,
        read_only: std::collections::HashSet<String>,
    }

    impl ToctouSnapshotRunner {
        fn new(late_snapshot: String) -> Self {
            Self {
                calls: Vec::new(),
                late_snapshot,
                injected: false,
                read_only: std::collections::HashSet::new(),
            }
        }
    }

    impl CleanupCommandRunner for ToctouSnapshotRunner {
        mock_ownership!();
        fn remove_tree(&mut self, path: &std::path::Path) -> Result<(), CleanupCommandError> {
            self.run("artifact-remove", &[&path.to_string_lossy()])
                .map(|_| ())
        }
        fn run(&mut self, program: &str, args: &[&str]) -> Result<String, CleanupCommandError> {
            self.calls.push((
                program.to_string(),
                args.iter().map(|arg| arg.to_string()).collect(),
            ));
            if program == "btrfs" && args.starts_with(&["property", "get", "-ts"]) {
                return Err(CleanupCommandError::NotSubvolume {
                    program: "btrfs".to_string(),
                    args: args.join(" "),
                    status: "exit status: 1".to_string(),
                    detail: "ERROR: Could not get subvolume flags: Invalid argument".to_string(),
                });
            }
            if program == "btrfs" && args.starts_with(&["property", "set", "-ts"]) {
                return Ok(String::new());
            }
            if program == "rewrite" {
                // The racing snapshot appears once the sweep has started.
                if !self.injected {
                    self.read_only.insert(self.late_snapshot.clone());
                    self.injected = true;
                }
                {
                    let root = args.first().copied().unwrap_or_default();
                    let owned: Vec<String> = args.iter().map(|arg| arg.to_string()).collect();
                    let prune_prefix = rewrite_prune_prefix(&owned);
                    for read_only_path in &self.read_only {
                        let pruned_out = prune_prefix
                            .as_deref()
                            .is_some_and(|prefix| read_only_path.starts_with(prefix));
                        if read_only_path.starts_with(root) && !pruned_out {
                            return Err(CleanupCommandError::FailedWithDetail {
                                program: "rewrite".to_string(),
                                args: args.join(" "),
                                status: "exit status: 1".to_string(),
                                detail: format!(
                                    "chmod: cannot operate on '{read_only_path}': Read-only file system"
                                ),
                            });
                        }
                    }
                }
            }
            Ok(String::new())
        }
    }

    #[test]
    fn retained_cache_uid_neutralization_prunes_undetected_readonly_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        std::fs::create_dir_all(cache_dir.join("snapshots")).unwrap();
        std::fs::create_dir_all(cache_dir.join("instance")).unwrap();
        let late = cache_dir.join("snapshots").join("d-stop-late");
        let mut runner = ToctouSnapshotRunner::new(late.to_string_lossy().into_owned());

        // Must succeed despite an undetected read-only snapshot racing in during
        // the sweep: the cache-wide passes prune the snapshots subtree, so the
        // recursive chmod/chown never descends into it (no EROFS, no quarantine).
        assert!(
            neutralize_retained_cache_dir_owned_by_uid_with_runner(&cache_dir, 10_042, &mut runner)
                .unwrap()
        );

        let cache_root = cache_dir.to_string_lossy().into_owned();
        let outer_finds = runner
            .calls
            .iter()
            .filter(|(program, args)| {
                program.as_str() == "rewrite"
                    && args.first().map(String::as_str) == Some(cache_root.as_str())
            })
            .collect::<Vec<_>>();
        assert!(!outer_finds.is_empty());
        for (_, args) in outer_finds {
            let joined = args.join(" ");
            assert!(
                args.get(1).map(String::as_str) == Some("prune"),
                "cache-wide pass must prune snapshots: {joined}"
            );
        }
    }

    #[derive(Default)]
    struct ProbeFailRunner {
        calls: Vec<(String, Vec<String>)>,
    }

    impl CleanupCommandRunner for ProbeFailRunner {
        mock_ownership!();
        fn remove_tree(&mut self, path: &std::path::Path) -> Result<(), CleanupCommandError> {
            self.run("artifact-remove", &[&path.to_string_lossy()])
                .map(|_| ())
        }
        fn run(&mut self, program: &str, args: &[&str]) -> Result<String, CleanupCommandError> {
            self.calls.push((
                program.to_string(),
                args.iter().map(|arg| arg.to_string()).collect(),
            ));
            if program == "btrfs" && args.starts_with(&["property", "get", "-ts"]) {
                // A transient/unexpected btrfs failure on a genuine subvolume —
                // NOT a "not a subvolume" error — must propagate, not be skipped.
                return Err(CleanupCommandError::FailedWithDetail {
                    program: "btrfs".to_string(),
                    args: args.join(" "),
                    status: "exit status: 1".to_string(),
                    detail: "ERROR: could not open: Input/output error".to_string(),
                });
            }
            Ok(String::new())
        }
    }

    #[test]
    fn retained_cache_neutralization_propagates_unexpected_probe_error() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        std::fs::create_dir_all(cache_dir.join("snapshots/d-stop-x")).unwrap();
        let mut runner = ProbeFailRunner::default();

        let error =
            neutralize_retained_cache_dir_owned_by_uid_with_runner(&cache_dir, 10_042, &mut runner)
                .unwrap_err();

        assert!(
            matches!(error, RetainedCacheCleanupError::Command(_)),
            "{error:?}"
        );
        assert!(
            error.to_string().contains("failed to probe snapshot"),
            "{error}"
        );
        // Bailed before any destructive ownership pass ran — so a transient
        // probe failure retries the reset instead of EROFS-ing the chmod.
        assert!(
            !runner
                .calls
                .iter()
                .any(|(program, args)| program == "rewrite"
                    && args.iter().any(|arg| arg == "chmod" || arg == "chown")),
            "no ownership rewrite should run after a probe error: {:?}",
            runner.calls
        );
    }

    // Models a real btrfs cache dir with leftover read-only `instance.ro`
    // subvolumes (a crashed/in-flight suspend or restore). A ownership rewrite that
    // descends into one returns EROFS, exactly as the kernel does on a
    // read-only subvolume — unless the pass pruned it out OR the subvolume was
    // force-deleted first. `btrfs subvolume delete -- <path>` removes a path
    // from the read-only set (the fix's force-delete); `property set` clears RO.
    struct InstanceRoEroFsRunner {
        calls: Vec<(String, Vec<String>)>,
        read_only_subvols: std::collections::HashSet<String>,
    }

    impl CleanupCommandRunner for InstanceRoEroFsRunner {
        mock_ownership!();
        fn remove_tree(&mut self, path: &std::path::Path) -> Result<(), CleanupCommandError> {
            self.run("artifact-remove", &[&path.to_string_lossy()])
                .map(|_| ())
        }
        fn run(&mut self, program: &str, args: &[&str]) -> Result<String, CleanupCommandError> {
            self.calls.push((
                program.to_string(),
                args.iter().map(|arg| arg.to_string()).collect(),
            ));
            if program == "btrfs" && args.starts_with(&["property", "get", "-ts"]) {
                return Err(CleanupCommandError::NotSubvolume {
                    program: "btrfs".to_string(),
                    args: args.join(" "),
                    status: "exit status: 1".to_string(),
                    detail: "ERROR: Could not get subvolume flags: Invalid argument".to_string(),
                });
            }
            if program == "btrfs" && args.starts_with(&["property", "set", "-ts"]) {
                return Ok(String::new());
            }
            if program == "artifact-remove" {
                if let Some(target) = args.last() {
                    self.read_only_subvols.remove(*target);
                }
                return Ok(String::new());
            }
            if program == "rewrite" {
                {
                    let root = args.first().copied().unwrap_or_default();
                    let owned: Vec<String> = args.iter().map(|arg| arg.to_string()).collect();
                    let prune_prefix = rewrite_prune_prefix(&owned);
                    for read_only_path in &self.read_only_subvols {
                        let pruned_out = prune_prefix
                            .as_deref()
                            .is_some_and(|prefix| read_only_path.starts_with(prefix));
                        if read_only_path.starts_with(root) && !pruned_out {
                            return Err(CleanupCommandError::FailedWithDetail {
                                program: "rewrite".to_string(),
                                args: args.join(" "),
                                status: "exit status: 1".to_string(),
                                detail: format!(
                                    "chmod: cannot operate on '{read_only_path}': Read-only file system"
                                ),
                            });
                        }
                    }
                }
            }
            Ok(String::new())
        }
    }

    fn seed_instance_ro_runner(cache_dir: &std::path::Path) -> InstanceRoEroFsRunner {
        // Leftover throwaway read-only snapshots from a crashed suspend/restore:
        // the restore-receive landing under the cache dir, and the suspend
        // codec's `btrfs send -p` source under `snapshot/` (singular).
        std::fs::create_dir_all(cache_dir.join("instance")).unwrap();
        std::fs::create_dir_all(cache_dir.join("instance.ro")).unwrap();
        std::fs::create_dir_all(cache_dir.join("snapshot").join("instance.ro")).unwrap();
        let mut read_only_subvols = std::collections::HashSet::new();
        read_only_subvols.insert(cache_dir.join("instance.ro").to_string_lossy().into_owned());
        read_only_subvols.insert(
            cache_dir
                .join("snapshot")
                .join("instance.ro")
                .to_string_lossy()
                .into_owned(),
        );
        InstanceRoEroFsRunner {
            calls: Vec::new(),
            read_only_subvols,
        }
    }

    fn assert_instance_ro_force_deleted(calls: &[String], cache_dir: &std::path::Path) {
        for instance_ro in [
            cache_dir.join("instance.ro"),
            cache_dir.join("snapshot").join("instance.ro"),
        ] {
            let delete = calls
                .iter()
                .position(|call| call == &format!("artifact-remove {}", instance_ro.display()))
                .unwrap_or_else(|| {
                    panic!(
                        "expected force-delete of {}: {calls:?}",
                        instance_ro.display()
                    )
                });
            // The force-delete must precede the ownership rewrite, or the
            // rewrite EROFSes descending into the still-read-only subvolume.
            let first_write_find = calls.iter().position(|call| call.starts_with("rewrite "));
            if let Some(rewrite) = first_write_find {
                assert!(
                    delete < rewrite,
                    "instance.ro must be deleted before the rewrite: {calls:?}"
                );
            }
        }
        // `instance/` is the live writable instance — never deleted by cleanup.
        assert!(
            !calls
                .iter()
                .any(|call| call
                    == &format!("artifact-remove {}", cache_dir.join("instance").display())),
            "instance/ must be preserved: {calls:?}"
        );
    }

    #[test]
    fn retained_cache_uid_neutralization_force_deletes_readonly_instance_ro() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        let mut runner = seed_instance_ro_runner(&cache_dir);

        // Must succeed: the ownership rewrite would EROFS descending into the
        // read-only instance.ro subvolumes unless they are force-deleted first.
        assert!(
            neutralize_retained_cache_dir_owned_by_uid_with_runner(&cache_dir, 10_042, &mut runner)
                .unwrap()
        );

        assert_instance_ro_force_deleted(&fmt_calls(&runner.calls), &cache_dir);
    }

    #[test]
    fn retained_cache_neutralization_force_deletes_readonly_instance_ro() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        let mut runner = seed_instance_ro_runner(&cache_dir);

        assert!(neutralize_retained_cache_dir_with_runner(&cache_dir, &mut runner).unwrap());

        assert_instance_ro_force_deleted(&fmt_calls(&runner.calls), &cache_dir);
    }

    #[derive(Default)]
    struct ProbeMissingRunner {
        calls: Vec<(String, Vec<String>)>,
    }

    impl CleanupCommandRunner for ProbeMissingRunner {
        mock_ownership!();
        fn remove_tree(&mut self, path: &std::path::Path) -> Result<(), CleanupCommandError> {
            self.run("artifact-remove", &[&path.to_string_lossy()])
                .map(|_| ())
        }
        fn run(&mut self, program: &str, args: &[&str]) -> Result<String, CleanupCommandError> {
            self.calls.push((
                program.to_string(),
                args.iter().map(|arg| arg.to_string()).collect(),
            ));
            if program == "btrfs" && args.starts_with(&["property", "get", "-ts"]) {
                // The snapshot was deleted between `read_dir` and this probe.
                return Err(CleanupCommandError::MissingPath {
                    program: "btrfs".to_string(),
                    args: args.join(" "),
                    status: "exit status: 1".to_string(),
                    detail: "ERROR: cannot access subvolume: No such file or directory".to_string(),
                });
            }
            Ok(String::new())
        }
    }

    #[test]
    fn retained_cache_neutralization_skips_snapshot_deleted_during_probe() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        std::fs::create_dir_all(cache_dir.join("snapshots/d-stop-gone")).unwrap();
        let mut runner = ProbeMissingRunner::default();

        // A snapshot vanishing mid-enumeration is a benign race — it must be
        // skipped, not turned into a hard cleanup failure that retries the reset.
        assert!(
            neutralize_retained_cache_dir_owned_by_uid_with_runner(&cache_dir, 10_042, &mut runner)
                .unwrap()
        );

        // The vanished snapshot is skipped: no ownership pass is rooted at it.
        let gone = cache_dir.join("snapshots/d-stop-gone");
        assert!(
            !runner
                .calls
                .iter()
                .any(|(program, args)| program == "rewrite"
                    && args.first().map(String::as_str) == Some(gone.to_string_lossy().as_ref())),
            "no rewrite should target the vanished snapshot: {:?}",
            runner.calls
        );
    }

    #[test]
    fn retained_cache_neutralization_does_not_probe_btrfs_on_symlink_snapshot_entries() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        std::fs::create_dir_all(cache_dir.join("snapshots")).unwrap();
        // A sandbox-planted symlink under snapshots/ pointing at a host subvolume.
        let evil = cache_dir.join("snapshots/evil");
        std::os::unix::fs::symlink("/some/host/subvolume", &evil).unwrap();
        let mut runner = MockCleanupRunner::default();

        assert!(
            neutralize_retained_cache_dir_owned_by_uid_with_runner(&cache_dir, 10_042, &mut runner)
                .unwrap()
        );

        // `btrfs property get/set -ts` must never be invoked on the symlink — its
        // open(2) would resolve the link and could toggle a host subvolume's ro
        // flag outside the cache.
        let probed_symlink = runner.calls.iter().any(|(program, args)| {
            program == "btrfs"
                && args.first().map(String::as_str) == Some("property")
                && args.iter().any(|arg| arg.ends_with("snapshots/evil"))
        });
        assert!(
            !probed_symlink,
            "must not run btrfs property on a symlink entry: {:?}",
            runner.calls
        );

        // It is still re-owned via a per-snapshot pass (the chown -h handles the
        // link itself), so a sandbox-owned symlink can't survive the verify gate.
        let evil_str = evil.to_string_lossy().into_owned();
        let rooted_at_symlink = runner.calls.iter().any(|(program, args)| {
            program == "rewrite" && args.first().map(String::as_str) == Some(evil_str.as_str())
        });
        assert!(
            rooted_at_symlink,
            "symlink entry should still be re-owned via a per-snapshot rewrite: {:?}",
            runner.calls
        );
    }

    #[derive(Default)]
    struct BtrfsMissingRunner {
        calls: Vec<(String, Vec<String>)>,
    }

    impl CleanupCommandRunner for BtrfsMissingRunner {
        mock_ownership!();
        fn remove_tree(&mut self, path: &std::path::Path) -> Result<(), CleanupCommandError> {
            self.run("artifact-remove", &[&path.to_string_lossy()])
                .map(|_| ())
        }
        fn run(&mut self, program: &str, args: &[&str]) -> Result<String, CleanupCommandError> {
            self.calls.push((
                program.to_string(),
                args.iter().map(|arg| arg.to_string()).collect(),
            ));
            if program == "btrfs" {
                // No btrfs-progs on this host: every `btrfs` invocation fails to
                // spawn.
                return Err(CleanupCommandError::Spawn {
                    program: "btrfs".to_string(),
                    detail: "No such file or directory (os error 2)".to_string(),
                });
            }
            Ok(String::new())
        }
    }

    #[test]
    fn retained_cache_neutralization_refuses_symlinked_snapshots_directory() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        std::fs::create_dir_all(&cache_dir).unwrap();
        // The sandbox replaces snapshots/ with a symlink to a host directory.
        let outside = root.path().join("outside");
        std::fs::create_dir_all(outside.join("victim")).unwrap();
        std::os::unix::fs::symlink(&outside, cache_dir.join("snapshots")).unwrap();
        let mut runner = MockCleanupRunner::default();

        assert!(
            neutralize_retained_cache_dir_owned_by_uid_with_runner(&cache_dir, 10_042, &mut runner)
                .unwrap()
        );

        // Enumeration must not follow the link: no btrfs probe runs on its
        // contents, and no per-snapshot rewrite is rooted inside the symlinked dir
        // — so cleanup cannot chmod/chown the host target's files.
        assert!(
            !runner.calls.iter().any(|(program, args)| program == "btrfs"
                && args.first().map(String::as_str) == Some("property")),
            "must not probe entries through a symlinked snapshots dir: {:?}",
            runner.calls
        );
        let snap_prefix = format!("{}/", cache_dir.join("snapshots").to_string_lossy());
        assert!(
            !runner
                .calls
                .iter()
                .any(|(program, args)| program == "rewrite"
                    && args
                        .first()
                        .is_some_and(|first| first.starts_with(&snap_prefix))),
            "no per-snapshot rewrite should run through the symlink: {:?}",
            runner.calls
        );
    }

    #[test]
    fn retained_cache_neutralization_does_not_probe_btrfs_on_special_file_entries() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        std::fs::create_dir_all(cache_dir.join("snapshots")).unwrap();
        // A stray FIFO under snapshots/. `btrfs property get -ts` would open(2)
        // it and block forever; only directories may be probed.
        let fifo = cache_dir.join("snapshots/stray.fifo");
        nix::unistd::mkfifo(&fifo, nix::sys::stat::Mode::from_bits_truncate(0o644))
            .expect("mkfifo");
        let mut runner = MockCleanupRunner::default();

        assert!(
            neutralize_retained_cache_dir_owned_by_uid_with_runner(&cache_dir, 10_042, &mut runner)
                .unwrap()
        );

        // No btrfs probe on the FIFO (it is not a directory, so cannot be a
        // subvolume) — that probe would hang cleanup.
        let probed_fifo = runner.calls.iter().any(|(program, args)| {
            program == "btrfs"
                && args.first().map(String::as_str) == Some("property")
                && args.iter().any(|arg| arg.ends_with("snapshots/stray.fifo"))
        });
        assert!(
            !probed_fifo,
            "must not run btrfs property on a non-directory entry: {:?}",
            runner.calls
        );
        // It is still re-owned via a per-snapshot pass.
        let fifo_str = fifo.to_string_lossy().into_owned();
        assert!(
            runner
                .calls
                .iter()
                .any(|(program, args)| program == "rewrite"
                    && args.first().map(String::as_str) == Some(fifo_str.as_str())),
            "special-file entry should still be re-owned: {:?}",
            runner.calls
        );
    }

    #[test]
    fn retained_cache_neutralization_tolerates_missing_btrfs_progs() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        std::fs::create_dir_all(cache_dir.join("snapshots/imported")).unwrap();
        let mut runner = BtrfsMissingRunner::default();

        // On a non-btrfs / no-btrfs-progs host the probe Spawn-fails; that must
        // be treated as "not a subvolume" (re-own the plain dir, no toggle), not
        // a hard abort that leaves the retained cache un-neutralized.
        assert!(
            neutralize_retained_cache_dir_owned_by_uid_with_runner(&cache_dir, 10_042, &mut runner)
                .unwrap()
        );

        // The plain-dir snapshot is still re-owned via a per-snapshot pass.
        let imported = cache_dir.join("snapshots/imported");
        assert!(
            runner
                .calls
                .iter()
                .any(|(program, args)| program == "rewrite"
                    && args.first().map(String::as_str)
                        == Some(imported.to_string_lossy().as_ref())),
            "plain-dir snapshot should be re-owned: {:?}",
            runner.calls
        );
        // No read-only toggle is attempted when btrfs is unavailable.
        assert!(
            !runner.calls.iter().any(|(program, args)| program == "btrfs"
                && args.first().map(String::as_str) == Some("property")
                && args.iter().any(|arg| arg == "set")),
            "no btrfs property set on a btrfs-unavailable host: {:?}",
            runner.calls
        );
    }

    // Every snapshot probes as a read-only subvolume; toggling `fail_suffix`
    // writable fails, simulating a partial make-writable.
    struct MakeWritableFailRunner {
        calls: Vec<(String, Vec<String>)>,
        fail_suffix: String,
    }

    impl CleanupCommandRunner for MakeWritableFailRunner {
        mock_ownership!();
        fn remove_tree(&mut self, path: &std::path::Path) -> Result<(), CleanupCommandError> {
            self.run("artifact-remove", &[&path.to_string_lossy()])
                .map(|_| ())
        }
        fn run(&mut self, program: &str, args: &[&str]) -> Result<String, CleanupCommandError> {
            self.calls.push((
                program.to_string(),
                args.iter().map(|arg| arg.to_string()).collect(),
            ));
            if program == "btrfs" && args.starts_with(&["property", "get", "-ts"]) {
                return Ok("ro=true\n".to_string());
            }
            if program == "btrfs" && args.starts_with(&["property", "set", "-ts"]) {
                let path = args.get(3).copied().unwrap_or_default();
                let value = args.last().copied().unwrap_or_default();
                if value == "false" && path.ends_with(&self.fail_suffix) {
                    return Err(CleanupCommandError::FailedWithDetail {
                        program: "btrfs".to_string(),
                        args: args.join(" "),
                        status: "exit status: 1".to_string(),
                        detail: "ERROR: failed to set ro flag".to_string(),
                    });
                }
                return Ok(String::new());
            }
            Ok(String::new())
        }
    }

    #[test]
    fn retained_cache_neutralization_restores_partial_make_writable_on_failure() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        std::fs::create_dir_all(cache_dir.join("snapshots/aaa")).unwrap();
        std::fs::create_dir_all(cache_dir.join("snapshots/zzz")).unwrap();
        let mut runner = MakeWritableFailRunner {
            calls: Vec::new(),
            fail_suffix: "zzz".to_string(),
        };

        // Toggling zzz writable fails after aaa was already toggled. Cleanup must
        // still restore aaa's read-only flag before bailing, so a published d-stop
        // snapshot isn't left permanently mutable.
        let error =
            neutralize_retained_cache_dir_owned_by_uid_with_runner(&cache_dir, 10_042, &mut runner)
                .unwrap_err();
        assert!(
            matches!(error, RetainedCacheCleanupError::Command(_)),
            "{error:?}"
        );

        let calls: Vec<String> = runner
            .calls
            .iter()
            .map(|(program, args)| format!("{program} {}", args.join(" ")))
            .collect();
        assert!(
            calls
                .iter()
                .any(|c| c.contains("property set -ts") && c.contains("snapshots/aaa ro true")),
            "aaa must be restored to read-only after a partial toggle failure: {calls:?}"
        );
    }

    // A snapshot dir that probes as writable (ro=false) but whose per-snapshot
    // chmod hits EROFS until it is toggled writable — i.e. a read-only subvolume
    // the up-front toggle missed (misclassified or raced).
    struct PerSnapshotErofsRunner {
        calls: Vec<(String, Vec<String>)>,
        secret_suffix: String,
        secret_writable: bool,
        retry_fails: bool,
        restore_fails: bool,
    }

    impl CleanupCommandRunner for PerSnapshotErofsRunner {
        mock_ownership!();
        fn remove_tree(&mut self, path: &std::path::Path) -> Result<(), CleanupCommandError> {
            self.run("artifact-remove", &[&path.to_string_lossy()])
                .map(|_| ())
        }
        fn run(&mut self, program: &str, args: &[&str]) -> Result<String, CleanupCommandError> {
            self.calls.push((
                program.to_string(),
                args.iter().map(|arg| arg.to_string()).collect(),
            ));
            if program == "btrfs" && args.starts_with(&["property", "get", "-ts"]) {
                return Ok("ro=false\n".to_string());
            }
            if program == "btrfs" && args.starts_with(&["property", "set", "-ts"]) {
                if self.restore_fails && args.last().copied() == Some("true") {
                    return Err(CleanupCommandError::FailedWithDetail {
                        program: "btrfs".to_string(),
                        args: args.join(" "),
                        status: "exit status: 1".to_string(),
                        detail: "ERROR: failed to restore ro flag".to_string(),
                    });
                }
                self.secret_writable = args.last().copied() == Some("false");
                return Ok(String::new());
            }
            if program == "rewrite" {
                let root = args.first().copied().unwrap_or_default();
                if root.ends_with(&self.secret_suffix)
                    && (!self.secret_writable || self.retry_fails)
                {
                    return Err(CleanupCommandError::FailedWithDetail {
                        program: "rewrite".to_string(),
                        args: args.join(" "),
                        status: "exit status: 1".to_string(),
                        detail: "chmod: cannot operate on snapshot: Read-only file system"
                            .to_string(),
                    });
                }
            }
            Ok(String::new())
        }
    }

    #[test]
    fn retained_cache_neutralization_recovers_when_per_snapshot_rewrite_hits_erofs() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        std::fs::create_dir_all(cache_dir.join("snapshots/secret")).unwrap();
        let mut runner = PerSnapshotErofsRunner {
            calls: Vec::new(),
            secret_suffix: "snapshots/secret".to_string(),
            secret_writable: false,
            retry_fails: false,
            restore_fails: false,
        };

        // The snapshot probes writable but is actually read-only, so the
        // per-snapshot chmod hits EROFS. Cleanup must recover (toggle writable,
        // retry, restore) rather than fail the reset into quarantine.
        assert!(
            neutralize_retained_cache_dir_owned_by_uid_with_runner(&cache_dir, 10_042, &mut runner)
                .unwrap()
        );

        let calls: Vec<String> = runner
            .calls
            .iter()
            .map(|(program, args)| format!("{program} {}", args.join(" ")))
            .collect();
        assert!(
            calls
                .iter()
                .any(|c| c.contains("property set -ts") && c.contains("snapshots/secret ro false")),
            "EROFS recovery must toggle the snapshot writable: {calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c.contains("property set -ts") && c.contains("snapshots/secret ro true")),
            "EROFS recovery must restore read-only: {calls:?}"
        );
    }

    #[test]
    fn retained_cache_neutralization_reports_restore_failure_after_erofs_retry_fails() {
        let root = tempfile::tempdir().unwrap();
        let cache_dir = root.path().join("sandbox-a");
        std::fs::create_dir_all(cache_dir.join("snapshots/secret")).unwrap();
        let mut runner = PerSnapshotErofsRunner {
            calls: Vec::new(),
            secret_suffix: "snapshots/secret".to_string(),
            secret_writable: false,
            retry_fails: true,
            restore_fails: true,
        };

        let error =
            neutralize_retained_cache_dir_owned_by_uid_with_runner(&cache_dir, 10_042, &mut runner)
                .unwrap_err();

        assert!(
            matches!(error, RetainedCacheCleanupError::Command(_)),
            "{error:?}"
        );
        assert!(
            error.to_string().contains("failed to restore ro flag"),
            "{error}"
        );

        let calls: Vec<String> = runner
            .calls
            .iter()
            .map(|(program, args)| format!("{program} {}", args.join(" ")))
            .collect();
        assert!(
            calls
                .iter()
                .any(|c| c.contains("property set -ts") && c.contains("snapshots/secret ro true")),
            "EROFS recovery must attempt to restore read-only after the retry: {calls:?}"
        );
    }

    #[test]
    fn template_rootfs_validation_accepts_regular_rootfs_under_template_root() {
        let root = tempfile::tempdir().unwrap();
        let template = root.path().join("seed");
        std::fs::create_dir_all(&template).unwrap();
        std::fs::write(template.join("rootfs.ext4"), b"rootfs").unwrap();

        let rootfs = validate_template_rootfs_under(root.path(), &template).unwrap();
        assert_eq!(
            rootfs,
            std::fs::canonicalize(&template)
                .unwrap()
                .join("rootfs.ext4")
        );
    }

    #[test]
    fn template_rootfs_validation_rejects_template_outside_root() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("rootfs.ext4"), b"rootfs").unwrap();

        let err = validate_template_rootfs_under(root.path(), outside.path()).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn template_rootfs_validation_reports_an_absent_template_root_as_failed_precondition() {
        // The managed template root is a fixed node path; its absence is a node
        // that needs repairing, not a read to retry.
        let root = tempfile::tempdir().unwrap();
        let absent_root = root.path().join("never-created");

        let err =
            validate_template_rootfs_under(&absent_root, &absent_root.join("seed")).unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::FailedPrecondition,
            "unexpected status: {err:?}"
        );
    }

    #[test]
    fn template_rootfs_validation_reports_a_missing_template_dir_as_not_found() {
        let root = tempfile::tempdir().unwrap();

        let err =
            validate_template_rootfs_under(root.path(), &root.path().join("absent")).unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[test]
    fn template_rootfs_validation_reports_an_unusable_template_path_as_failed_precondition() {
        // A template dir whose parent is a regular file cannot resolve, now or
        // on any retry — but it is not a malformed *argument*, so reporting it
        // as InvalidArgument mislabels a node-filesystem problem as a caller
        // bug. It has to be terminal (the activation path propagates
        // FailedPrecondition) without claiming the request was wrong.
        let root = tempfile::tempdir().unwrap();
        let not_a_dir = root.path().join("seed");
        std::fs::write(&not_a_dir, b"regular file").unwrap();

        let err =
            validate_template_rootfs_under(root.path(), &not_a_dir.join("nested")).unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::FailedPrecondition,
            "unexpected status: {err:?}"
        );
    }

    #[test]
    fn template_rootfs_validation_rejects_symlinked_rootfs() {
        let root = tempfile::tempdir().unwrap();
        let template = root.path().join("seed");
        std::fs::create_dir_all(&template).unwrap();
        let target = root.path().join("target-rootfs.ext4");
        std::fs::write(&target, b"rootfs").unwrap();
        std::os::unix::fs::symlink(&target, template.join("rootfs.ext4")).unwrap();

        let err = validate_template_rootfs_under(root.path(), &template).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn sidecar_launch_arg_validation_accepts_expected_spaced_host_paths() {
        let sandbox_id = "sbx-123";
        let runtime_pid = 4_242;
        let sandbox_uid = 10_042;
        let args = vec![
            "--socket".to_string(),
            pplx_space_common_core::host::linux::paths::spaced_host_socket(sandbox_id),
            "--sandbox-id".to_string(),
            sandbox_id.to_string(),
            "--vsock-uds".to_string(),
            format!("{JAIL_DIR}/firecracker/{sandbox_id}/root/v.sock"),
            "--host-control-socket".to_string(),
            pplx_space_common_core::host::linux::paths::SANDBOX_HOST_CONTROL_SOCKET.to_string(),
        ];

        let prepared = prepare_sidecar_launch_args(
            pplx_space_common_core::host::linux::paths::SPACED_HOST_SVC_BIN,
            sandbox_id,
            &args,
            Some(ExpectedSpacedHostPeer::new(
                LinuxPid::try_from_u32(runtime_pid).unwrap(),
                sandbox_uid,
            )),
        )
        .unwrap();
        assert_eq!(
            require_launch_arg(
                &prepared,
                pplx_space_common_core::host::linux::paths::SPACED_HOST_SVC_BIN,
                "--firecracker-pid",
            )
            .unwrap(),
            runtime_pid.to_string()
        );
        assert_eq!(
            require_launch_arg(
                &prepared,
                pplx_space_common_core::host::linux::paths::SPACED_HOST_SVC_BIN,
                "--firecracker-uid",
            )
            .unwrap(),
            sandbox_uid.to_string()
        );
    }

    #[test]
    fn sidecar_launch_arg_validation_rejects_wrong_firecracker_peer() {
        let sandbox_id = "sbx-123";
        let args = vec![
            "--socket".to_string(),
            pplx_space_common_core::host::linux::paths::spaced_host_socket(sandbox_id),
            "--sandbox-id".to_string(),
            sandbox_id.to_string(),
            "--vsock-uds".to_string(),
            format!("{JAIL_DIR}/firecracker/{sandbox_id}/root/v.sock"),
            "--firecracker-pid".to_string(),
            "9999".to_string(),
            "--firecracker-uid".to_string(),
            "10042".to_string(),
            "--host-control-socket".to_string(),
            pplx_space_common_core::host::linux::paths::SANDBOX_HOST_CONTROL_SOCKET.to_string(),
        ];

        let error = prepare_sidecar_launch_args(
            pplx_space_common_core::host::linux::paths::SPACED_HOST_SVC_BIN,
            sandbox_id,
            &args,
            Some(ExpectedSpacedHostPeer::new(
                LinuxPid::try_from_u32(4_242).unwrap(),
                10_042,
            )),
        )
        .expect_err("node-manager must reject a caller-selected Firecracker peer");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn sidecar_launch_arg_validation_rejects_equals_form_override() {
        let sandbox_id = "sbx-123";
        let runtime_pid = 4_242;
        let sandbox_uid = 10_042;
        let args = vec![
            "--socket".to_string(),
            pplx_space_common_core::host::linux::paths::spaced_host_socket(sandbox_id),
            "--sandbox-id".to_string(),
            sandbox_id.to_string(),
            "--vsock-uds".to_string(),
            format!("{JAIL_DIR}/firecracker/{sandbox_id}/root/v.sock"),
            "--firecracker-pid".to_string(),
            runtime_pid.to_string(),
            "--firecracker-pid=9999".to_string(),
            "--firecracker-uid".to_string(),
            sandbox_uid.to_string(),
            "--host-control-socket".to_string(),
            pplx_space_common_core::host::linux::paths::SANDBOX_HOST_CONTROL_SOCKET.to_string(),
        ];

        let error = prepare_sidecar_launch_args(
            pplx_space_common_core::host::linux::paths::SPACED_HOST_SVC_BIN,
            sandbox_id,
            &args,
            Some(ExpectedSpacedHostPeer::new(
                LinuxPid::try_from_u32(runtime_pid).unwrap(),
                sandbox_uid,
            )),
        )
        .expect_err("node-manager must reject an equals-form override");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn spaced_host_peer_uses_the_firecracker_pid_link_not_the_slot_process_pid() {
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_id = "sbx-123";
        let sandbox_run_dir = run_dir.path().join(sandbox_id);
        std::fs::create_dir(&sandbox_run_dir).unwrap();
        std::os::unix::fs::symlink("/proc/4242", sandbox_run_dir.join("fc.pid")).unwrap();

        let pid =
            registered_firecracker_pid_with_vm_processes(run_dir.path(), sandbox_id, &[4_242])
                .unwrap();

        assert_eq!(
            pid.get().get(),
            4_242,
            "a sidecar pid must not replace the runtime pid"
        );
    }

    #[test]
    fn spaced_host_peer_rejects_a_pid_outside_the_sandbox_vm_cgroup() {
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_id = "sbx-123";
        let sandbox_run_dir = run_dir.path().join(sandbox_id);
        std::fs::create_dir(&sandbox_run_dir).unwrap();
        std::os::unix::fs::symlink("/proc/4242", sandbox_run_dir.join("fc.pid")).unwrap();

        let error =
            registered_firecracker_pid_with_vm_processes(run_dir.path(), sandbox_id, &[4_243])
                .expect_err(
                    "a sandbox-writable pid link must not select the authenticated runtime",
                );

        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn sidecar_launch_arg_validation_rejects_cross_sandbox_socket_path() {
        let sandbox_id = "sbx-123";
        let args = vec![
            "--socket".to_string(),
            pplx_space_common_core::host::linux::paths::net_gateway_control_socket("sbx-other"),
            "--sandbox-id".to_string(),
            sandbox_id.to_string(),
        ];

        let err = prepare_sidecar_launch_args(
            pplx_space_common_core::host::linux::paths::NET_GATEWAY_SVC_BIN,
            sandbox_id,
            &args,
            None,
        )
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }
}
