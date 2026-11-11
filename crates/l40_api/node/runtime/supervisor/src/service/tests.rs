use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
async fn retirement_failure_does_not_skip_locked_runtime_cleanup() {
    #[derive(Clone, Copy, Debug)]
    enum Retirement {
        Success,
        IoFailure,
        Panic,
        AdmissionClosed,
    }
    for retirement in [
        Retirement::Success,
        Retirement::IoFailure,
        Retirement::Panic,
        Retirement::AdmissionClosed,
    ] {
        for cleanup_fails in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let service = RuntimeSupervisorService {
                execution_store: crate::execution_store::ExecutionStore::at(
                    temp.path().join("executions"),
                ),
                ..Default::default()
            };
            let sandbox_id = SandboxId::new();
            let execution_id = pplx_space_common_core::types::SandboxActivationAttemptId::new();
            service
                .execution_store
                .write(sandbox_id, execution_id)
                .unwrap();
            let lock = service.runtime_lock(sandbox_id);
            let retirement_lock = lock.clone();
            let stale = service
                .stop_sandbox_runtime_with(
                    StopSandboxRuntimeRequest {
                        sandbox_id: sandbox_id.to_string(),
                        execution_id:
                            pplx_space_common_core::types::SandboxActivationAttemptId::new()
                                .to_string(),
                        ..Default::default()
                    },
                    None,
                    |_| panic!("stale stop must not retire gateway authority"),
                    |_, _| async { panic!("stale stop must not terminate the current runtime") },
                )
                .await;
            assert_eq!(stale.unwrap_err().code(), tonic::Code::FailedPrecondition);
            drop(lock.try_write().unwrap());
            if matches!(retirement, Retirement::AdmissionClosed) {
                service.blocking_slots.close();
            }
            let cleaned = AtomicBool::new(false);
            let retired = Arc::new(AtomicBool::new(false));
            let retirement_started = retired.clone();
            let result = service
                .stop_sandbox_runtime_with(
                    StopSandboxRuntimeRequest {
                        sandbox_id: sandbox_id.to_string(),
                        execution_id: execution_id.to_string(),
                        immediate_kill: true,
                        ..Default::default()
                    },
                    None,
                    move |id| {
                        assert_eq!(id, sandbox_id);
                        retirement_lock.try_read().unwrap_err();
                        retirement_started.store(true, Ordering::SeqCst);
                        match retirement {
                            Retirement::Success => Ok(()),
                            Retirement::IoFailure => {
                                Err(std::io::Error::other("injected retirement failure").into())
                            }
                            Retirement::Panic => panic!("injected retirement panic"),
                            Retirement::AdmissionClosed => {
                                panic!("closed admission ran retirement")
                            }
                        }
                    },
                    |id, immediate_kill| {
                        assert_eq!(id, sandbox_id);
                        assert!(immediate_kill);
                        async {
                            assert!(
                                lock.try_read().is_err(),
                                "replacement must remain excluded during cleanup"
                            );
                            assert_eq!(
                                retired.load(Ordering::SeqCst),
                                !matches!(retirement, Retirement::AdmissionClosed)
                            );
                            cleaned.store(true, Ordering::SeqCst);
                            if cleanup_fails {
                                Err(Status::internal("injected cleanup failure"))
                            } else {
                                Ok(Response::new(StopSandboxRuntimeResponse {
                                    actions: vec!["cleanup completed".into()],
                                }))
                            }
                        }
                    },
                )
                .await;
            assert_eq!(
                cleaned.load(Ordering::SeqCst),
                !matches!(retirement, Retirement::AdmissionClosed),
                "cleanup requires authority validation before retirement ({retirement:?})"
            );
            assert!(
                lock.try_write().is_ok(),
                "completed stop must release exclusion"
            );
            if matches!(retirement, Retirement::AdmissionClosed) {
                assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
                assert!(!retired.load(Ordering::SeqCst));
            } else if cleanup_fails {
                assert_eq!(result.unwrap_err().message(), "injected cleanup failure");
            } else {
                match retirement {
                    Retirement::Success => {
                        assert_eq!(result.unwrap().into_inner().actions, ["cleanup completed"])
                    }
                    Retirement::IoFailure | Retirement::AdmissionClosed => {
                        assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable)
                    }
                    Retirement::Panic => {
                        assert_eq!(result.unwrap_err().code(), tonic::Code::Internal)
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn execution_checks_survive_supervisor_restart_and_reject_superseded_work() {
    use pplx_space_common_core::types::SandboxActivationAttemptId;
    let temp = tempfile::tempdir().unwrap();
    let store = crate::execution_store::ExecutionStore::at(temp.path().join("executions"));
    let sandbox = SandboxId::new();
    let old = SandboxActivationAttemptId::new().to_string();
    let current = SandboxActivationAttemptId::new().to_string();
    store.write(sandbox, current.parse().unwrap()).unwrap();
    let service = RuntimeSupervisorService {
        execution_store: store,
        ..Default::default()
    };
    let unbound = SandboxId::new();
    for allow_absent in [false, true] {
        assert_eq!(
            service
                .check_execution(unbound, "", None, allow_absent)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition,
            "an absent binding cannot authorize an unfenced request"
        );
    }
    for (requested, previous, accepted) in [
        ("", None, false),
        (old.as_str(), None, false),
        (current.as_str(), None, true),
        (old.as_str(), Some(current.as_str()), true),
    ] {
        assert_eq!(
            service
                .check_execution(sandbox, requested, previous, false)
                .await
                .is_ok(),
            accepted
        );
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn execution_binding_cannot_resurrect_work_from_an_exited_daemon() {
    use pplx_space_common_core::types::SandboxActivationAttemptId;
    use pplx_space_node_runtime_core::proto::{BindExecutionRequest, RetireExecutionRequest};
    let temp = tempfile::tempdir().unwrap();
    let service = RuntimeSupervisorService {
        execution_store: crate::execution_store::ExecutionStore::at(temp.path().join("executions")),
        ..Default::default()
    };
    let sandbox = SandboxId::new();
    let execution = SandboxActivationAttemptId::new();
    // Only this test's child represents a peer; never signal another daemon.
    let mut peer = std::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .unwrap();
    let caller = || launch::ProcessHandle::open(peer.id()).unwrap();
    let request = BindExecutionRequest {
        sandbox_id: sandbox.to_string(),
        execution_id: execution.to_string(),
        previous_id: None,
    };
    let guard = service.runtime_lock(sandbox).write_owned().await;
    service
        .bind_execution_owned(request.clone(), caller(), guard)
        .await
        .unwrap();
    let queued_caller = caller();
    service
        .retire_execution_owned(
            RetireExecutionRequest {
                sandbox_id: sandbox.to_string(),
                execution_id: execution.to_string(),
            },
            caller(),
        )
        .await
        .unwrap();
    peer.kill().unwrap();
    peer.wait().unwrap();
    let guard = service.runtime_lock(sandbox).write_owned().await;
    let result = service
        .bind_execution_owned(request, queued_caller, guard)
        .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::Aborted);
    assert_eq!(service.execution_store.read(sandbox).unwrap(), None);
}

#[test]
fn launch_lock_reuses_live_entry() {
    let service = RuntimeSupervisorService::default();
    let key = (SandboxId::new(), "space-netgtw-svc".to_string());
    let lock = service.launch_lock(key.clone());
    let waiter = service.launch_lock(key);

    assert_eq!(service.launch_locks.len(), 1);
    assert!(Arc::ptr_eq(&lock, &waiter));
}

#[test]
fn launch_lock_replaces_expired_entry() {
    let service = RuntimeSupervisorService::default();
    let key = (SandboxId::new(), "space-netgtw-svc".to_string());
    let lock = service.launch_lock(key.clone());
    drop(lock);
    let next = service.launch_lock(key);

    assert_eq!(service.launch_locks.len(), 1);
    assert_eq!(Arc::strong_count(&next), 1);
}

#[test]
fn runtime_lock_replaces_expired_entry() {
    let service = RuntimeSupervisorService::default();
    let sandbox_id = SandboxId::new();
    let lock = service.runtime_lock(sandbox_id);
    drop(lock);
    let next = service.runtime_lock(sandbox_id);

    assert_eq!(service.runtime_locks.len(), 1);
    assert_eq!(Arc::strong_count(&next), 1);
}

#[tokio::test]
async fn update_cgroup_requires_limits_after_authorized_handoff() {
    let service = RuntimeSupervisorService::default();
    let request = UpdateCgroupRequest {
        execution_id: String::new(),
        sandbox_id: SandboxId::new().to_string(),
        cgroup_limits: None,
    };
    assert_eq!(
        service
            .update_cgroup(Request::new(request.clone()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    // Payload validation precedes host work; no execution is admitted here.
    let status = service
        .update_cgroup_owned(request, None)
        .await
        .expect_err("missing cgroup_limits should be rejected");

    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(status.message(), "cgroup_limits is required");
}

#[tokio::test]
async fn sidecar_admission_failure_precedes_cgroup_creation() {
    let service = RuntimeSupervisorService::default();
    service.blocking_slots.close();
    let create_called = Arc::new(AtomicBool::new(false));
    let create_called_by_launch = create_called.clone();
    let sandbox_id = SandboxId::new();

    let result = service
        .spawn_sidecar_launcher(
            sandbox_id,
            launch::SidecarLauncherConfig {
                ns_path: "/proc/self/ns/net".to_string(),
                program: "/bin/true".to_string(),
                args: Vec::new(),
                env: std::collections::HashMap::new(),
                uid: 1,
                gid: 1,
                cgroup_path: cgroup::sidecar_cgroup_path(&sandbox_id),
                pre_exec_sockets: Vec::new(),
                grant_cap_net_admin: false,
            },
            move || {
                create_called_by_launch.store(true, Ordering::Relaxed);
                Ok(cgroup::CgroupCreateResult { created: true })
            },
        )
        .await;

    let status = match result {
        Ok(_) => panic!("closed admission must reject sidecar launch"),
        Err(status) => status,
    };
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(
        !create_called.load(Ordering::Relaxed),
        "admission rejection must happen before cgroup creation"
    );
}

#[tokio::test(start_paused = true)]
async fn stop_backpressure_is_bounded() {
    for shutdown_requested in [false, true] {
        let service = RuntimeSupervisorService::default();
        if shutdown_requested {
            // The transport is shutting down, but may still dispatch an
            // already received request before final operation drain starts.
            service.shutdown.cancel();
        }
        let capacity = service
            .operation_slots
            .clone()
            .acquire_many_owned(MAX_RUNTIME_BLOCKING_OPERATIONS as u32)
            .await
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(6), service.admit_operation()).await;
        assert_eq!(
            result.unwrap().unwrap_err().code(),
            if shutdown_requested {
                tonic::Code::Unavailable
            } else {
                tonic::Code::ResourceExhausted
            }
        );
        drop(capacity);
        service.drain().await;
        assert_eq!(
            service.admit_operation().await.unwrap_err().code(),
            tonic::Code::Unavailable,
            "no caller may start new work after final drain"
        );

        let service = RuntimeSupervisorService::default();
        let sandbox_id = SandboxId::new();
        let lock = service.runtime_lock(sandbox_id);
        let _launching = lock.read().await;
        let stop = service.stop_sandbox_runtime_owned(
            StopSandboxRuntimeRequest {
                previous_execution_id: None,
                sandbox_id: sandbox_id.to_string(),
                ..Default::default()
            },
            None,
        );
        // Keep the lock held: this test never reaches host mutation.
        let result = tokio::time::timeout(Duration::from_secs(6), stop).await;
        assert_eq!(
            result.unwrap().unwrap_err().code(),
            tonic::Code::Unavailable
        );
    }
}

#[tokio::test(start_paused = true)]
async fn cancelled_waiter_retains_runtime_ownership_until_drain_completes() {
    let service = RuntimeSupervisorService::default();
    let reaper_stopped = Arc::new(AtomicBool::new(false));
    let stopped = reaper_stopped.clone();
    let reaper_shutdown = service.reaper_shutdown.clone();
    service.reaper_tasks.spawn(async move {
        reaper_shutdown.cancelled().await;
        stopped.store(true, Ordering::SeqCst);
    });
    let admitted_before_spawn = service.admit_operation().await.unwrap();
    let runtime_lock = service.runtime_lock(SandboxId::new());
    let owned_lock = runtime_lock.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let owner = service.clone();
    let waiter = tokio::spawn(async move {
        owner
            .run_owned_operation(async move {
                let _guard = owned_lock.write().await;
                started_tx.send(()).unwrap();
                finish_rx.await.unwrap();
                Ok(())
            })
            .await
    });
    started_rx.await.unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    runtime_lock.try_write().unwrap_err();
    assert_eq!(
        await_runtime_lock(runtime_lock.write())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert_eq!(
        service.operation_slots.available_permits(),
        MAX_RUNTIME_BLOCKING_OPERATIONS - 2
    );
    let draining = service.drain();
    tokio::pin!(draining);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut draining)
            .await
            .is_err()
    );
    assert_eq!(
        service
            .run_owned_operation(async { Ok(()) })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    finish_tx.send(()).unwrap();
    assert!(
        !service.reaper_shutdown.is_cancelled(),
        "reaper must outlive accepted operations even after admission shutdown"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut draining)
            .await
            .is_err(),
        "drain must cover the admission-to-spawn window too"
    );
    drop(admitted_before_spawn);
    tokio::time::timeout(Duration::from_secs(1), draining)
        .await
        .unwrap();
    drop(runtime_lock.try_write().unwrap());
    assert!(
        reaper_stopped.load(Ordering::SeqCst),
        "drain must join the reaper"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn incomplete_sidecar_launch_is_killed_and_reaped() -> Result<(), Box<dyn std::error::Error>> {
    let child = std::process::Command::new("sleep").arg("30").spawn()?;
    let pid = child.id();
    let tracked = Arc::new(Mutex::new(HashSet::new()));
    let pending_launch = PendingSidecarLaunch::new(pid)?;
    track_child(&tracked, pid);
    assert!(
        tracked
            .lock()
            .map_err(|error| error.to_string())?
            .contains(&pid)
    );

    drop(pending_launch);

    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while std::time::Instant::now() < deadline {
        crate::process::reap_tracked_children(&tracked);
        if tracked
            .lock()
            .map_err(|error| error.to_string())?
            .is_empty()
        {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    panic!("killed sidecar child {pid} was not reaped before the deadline");
}

#[cfg(target_os = "linux")]
#[test]
fn completed_sidecar_launch_disarms_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::MetadataExt;
    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let executable = "sleep";
    let mut child = ChildGuard(std::process::Command::new(executable).arg("30").spawn()?);
    let pid = child.0.id();

    let pending = PendingSidecarLaunch::new(pid)?;
    // `spawn` can return before the child's exec replaces its image, while
    // /proc/<pid>/exe still names this test binary. Production checks the
    // executable only after the launcher reports the exec, so wait for it here.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::fs::read_link(format!("/proc/{pid}/exe"))?.file_name()
        != Some(std::ffi::OsStr::new(executable))
    {
        assert!(
            std::time::Instant::now() < deadline,
            "child {pid} did not exec {executable} before the deadline"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    pending.require_executable(executable)?;
    assert_eq!(
        pending
            .require_executable("/wrong-program")
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    pending.disarm();
    assert!(launch::is_pid_alive(pid));
    let owner = std::fs::metadata("/proc/self")?;
    for (program, uid, expected) in [
        (executable, owner.uid(), true),
        ("/wrong-program", owner.uid(), false),
        (executable, owner.uid() + 1, false),
    ] {
        let result = require_sidecar_identity(pid, program, uid, owner.gid());
        if expected {
            assert!(result?);
        } else {
            assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
        }
    }
    let pinned = PendingSidecarLaunch::new(pid)?;
    child.0.kill()?;
    child.0.wait()?;
    assert_eq!(
        pinned.require_executable(executable).unwrap_err().code(),
        tonic::Code::Unavailable
    );
    assert!(!require_sidecar_identity(
        pid,
        executable,
        owner.uid(),
        owner.gid()
    )?);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn poisoned_tracker_recovers_and_reaps_child() -> Result<(), Box<dyn std::error::Error>> {
    let tracked = Arc::new(Mutex::new(HashSet::new()));
    let poison_target = tracked.clone();
    let _ = std::panic::catch_unwind(move || {
        let _guard = poison_target.lock().expect("tracker lock");
        panic!("poison tracker for test");
    });
    let child = std::process::Command::new("sleep").arg("30").spawn()?;
    let pid = child.id();

    let pending = track_pending_sidecar(&tracked, pid)?;
    assert!(
        tracked
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains(&pid)
    );
    drop(pending);

    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while std::time::Instant::now() < deadline {
        crate::process::reap_tracked_children(&tracked);
        if tracked
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_empty()
        {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    panic!("killed sidecar child {pid} was not reaped before the deadline");
}

#[tokio::test]
async fn cpu_updates_require_execution_launch_evidence() {
    let temp = tempfile::tempdir().unwrap();
    let service = RuntimeSupervisorService {
        execution_store: crate::execution_store::ExecutionStore::at(temp.path().join("executions")),
        ..Default::default()
    };
    let sandbox = SandboxId::new();
    let execution = pplx_space_common_core::types::SandboxActivationAttemptId::new();
    service.execution_store.write(sandbox, execution).unwrap();
    let request = |cpu| UpdateCgroupRequest {
        sandbox_id: sandbox.to_string(),
        execution_id: execution.to_string(),
        cgroup_limits: Some(pplx_space_node_runtime_core::proto::CgroupLimits {
            vcpu_count: cpu,
            memory_mib: Some(512),
            pids_max: Some(64),
        }),
    };
    assert_eq!(
        service
            .update_cgroup_owned(request(Some(1)), None)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        service
            .update_cgroup_owned(request(None), None)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    let count = |value| pplx_space_common_core::types::VcpuCount::try_from_u32(value).unwrap();
    service
        .execution_store
        .record_cpu_budget(sandbox, execution, count(4))
        .unwrap();
    service
        .execution_store
        .record_cpu_budget(sandbox, execution, count(1))
        .unwrap();
    // No real cgroup is present: permitted requests reach I/O, policy failures
    // must reject before any writer is entered.
    for (quota, code) in [
        (0, tonic::Code::InvalidArgument),
        (4, tonic::Code::NotFound),
        (1, tonic::Code::NotFound),
        (4, tonic::Code::NotFound),
        (5, tonic::Code::InvalidArgument),
    ] {
        assert_eq!(
            service
                .update_cgroup_owned(request(Some(quota)), None)
                .await
                .unwrap_err()
                .code(),
            code
        );
    }
    let mut stale = request(Some(1));
    stale.execution_id =
        pplx_space_common_core::types::SandboxActivationAttemptId::new().to_string();
    assert_eq!(
        service
            .update_cgroup_owned(stale, None)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    std::fs::write(
        temp.path()
            .join("executions")
            .join(format!("{sandbox}.cpu")),
        b"invalid budget",
    )
    .unwrap();
    assert_eq!(
        service
            .update_cgroup_owned(request(Some(1)), None)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        service
            .update_cgroup_owned(request(None), None)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
}

#[tokio::test]
async fn authentication_failure_preserves_status_and_existing_cpu_budget() {
    use pplx_space_common_core::types::{Arch, SandboxActivationAttemptId, VcpuCount};
    use pplx_space_node_runtime_core::{
        artifact_store::managed_release_directory, catalog::CompiledEnvironment,
    };
    let temp = tempfile::tempdir().unwrap();
    let service = RuntimeSupervisorService {
        execution_store: crate::execution_store::ExecutionStore::at(temp.path().join("executions")),
        ..Default::default()
    };
    let sandbox = SandboxId::new();
    let execution = SandboxActivationAttemptId::new();
    service.execution_store.write(sandbox, execution).unwrap();
    let environment = CompiledEnvironment::for_architecture(Arch::X86_64)
        .unwrap()
        .remove(0);
    let count = |value| VcpuCount::try_from_u32(value).unwrap();
    for previous in [None, Some(count(1))] {
        if let Some(previous) = previous {
            service
                .execution_store
                .record_cpu_budget(sandbox, execution, previous)
                .unwrap();
        }
        let release = environment.release();
        let root = temp.path().join(managed_release_directory(
            &release.release,
            release.digest(),
        ));
        let launch = crate::verified_launch::VerifiedLaunch::from_environment(
            &environment,
            &root.join("bin/firecracker"),
            &root.join("bin/jailer"),
        )
        .unwrap();
        let result = service
            .prepare_firecracker_launch(sandbox, execution, count(4), Some(launch))
            .await;
        let Err(error) = result else {
            panic!("missing authenticated source was accepted")
        };
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            service
                .execution_store
                .cpu_budget(sandbox, execution)
                .unwrap(),
            previous
        );
    }
}

#[tokio::test]
async fn verified_launch_rejects_ambiguous_and_unsupported_inputs_before_execution() {
    use pplx_space_node_runtime_core::{
        launch_admission::supervisor_launch_status,
        proto::{ArtifactIntegrityRequirement, VerifiedRuntimeLaunch},
    };
    let temp = tempfile::tempdir().unwrap();
    let service = RuntimeSupervisorService {
        execution_store: crate::execution_store::ExecutionStore::at(temp.path().join("executions")),
        ..Default::default()
    };
    let sandbox = SandboxId::new();
    let execution = pplx_space_common_core::types::SandboxActivationAttemptId::new();
    service.execution_store.write(sandbox, execution).unwrap();
    enum Invalid {
        IdentityMismatch,
        MissingReference,
        UnknownRequirement,
        MissingRequirement,
        UnknownEnvironment,
        #[cfg(target_arch = "x86_64")]
        Layout,
    }
    for invalid in [
        Invalid::IdentityMismatch,
        Invalid::MissingReference,
        Invalid::UnknownRequirement,
        Invalid::MissingRequirement,
        Invalid::UnknownEnvironment,
        #[cfg(target_arch = "x86_64")]
        Invalid::Layout,
    ] {
        let mut request = pplx_space_node_runtime_core::proto::LaunchRuntimeRequest {
            sandbox_id: sandbox.to_string(),
            launcher_binary: Some("/synthetic/jailer".into()),
            runtime_binary: "/synthetic/firecracker".into(),
            workspace_root: "/synthetic/jails".into(),
            network_namespace: "/synthetic/netns".into(),
            execution_id: execution.to_string(),
            verified_runtime: Some(VerifiedRuntimeLaunch {
                sandbox_id: sandbox.to_string(),
                environment_reference: format!("sha256:{}", "00".repeat(32)),
                integrity_requirement: ArtifactIntegrityRequirement::AuthenticatedImmutableSources
                    .into(),
            }),
            ..Default::default()
        };
        request.set_runtime(pplx_space_common_core::types::RuntimeKind::Firecracker.into());
        let input = request.verified_runtime.as_mut().unwrap();
        let expected = match invalid {
            Invalid::IdentityMismatch => {
                request.sandbox_id = SandboxId::new().to_string();
                tonic::Code::FailedPrecondition
            }
            Invalid::MissingReference => {
                input.environment_reference.clear();
                tonic::Code::InvalidArgument
            }
            Invalid::UnknownRequirement => {
                input.integrity_requirement = i32::MAX;
                tonic::Code::FailedPrecondition
            }
            Invalid::MissingRequirement => {
                input.integrity_requirement = 0;
                tonic::Code::FailedPrecondition
            }
            Invalid::UnknownEnvironment => tonic::Code::FailedPrecondition,
            #[cfg(target_arch = "x86_64")]
            Invalid::Layout => {
                input.environment_reference =
                    pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(
                        pplx_space_common_core::types::Arch::X86_64,
                    )
                    .unwrap()[0]
                        .definition()
                        .reference()
                        .to_string();
                request.runtime_binary = "/synthetic/invalid-layout".into();
                tonic::Code::InvalidArgument
            }
        };
        let result = match FirecrackerLaunch::try_from(request) {
            Ok(request) => {
                service
                    .launch_firecracker_with(request, None, |_, _| async { Ok(None) })
                    .await
            }
            Err(error) => {
                Err(pplx_space_node_runtime_core::launch_admission::launch_contract_status(error))
            }
        };
        let error = result.unwrap_err();
        assert_eq!(supervisor_launch_status(error).code(), expected);
        assert_eq!(
            service
                .execution_store
                .cpu_budget(sandbox, execution)
                .unwrap(),
            None
        );
        assert_eq!(
            service.execution_store.read(sandbox).unwrap(),
            Some(execution)
        );
    }
}

#[tokio::test]
async fn resident_execution_adoption_does_not_require_current_catalog_or_sources() {
    use pplx_space_common_core::types::SandboxActivationAttemptId;
    for resident in [false, true] {
        for current in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let service = RuntimeSupervisorService {
                execution_store: crate::execution_store::ExecutionStore::at(
                    temp.path().join("executions"),
                ),
                ..Default::default()
            };
            let sandbox = SandboxId::new();
            let execution = SandboxActivationAttemptId::new();
            service.execution_store.write(sandbox, execution).unwrap();
            let reference = format!("sha256:{}", "12".repeat(32));
            let inspected = AtomicBool::new(false);
            let result = service
                .launch_firecracker_with(
                    {
                        let mut wire = pplx_space_node_runtime_core::proto::LaunchRuntimeRequest {
                            sandbox_id: sandbox.to_string(),
                            execution_id: if current { execution } else { SandboxActivationAttemptId::new() }.to_string(),
                            runtime_binary: "/absent/old-cache/firecracker".into(),
                            launcher_binary: Some("/absent/old-cache/jailer".into()),
                            workspace_root: "/synthetic/jails".into(),
                            network_namespace: "/synthetic/netns".into(),
                            verified_runtime: Some(pplx_space_node_runtime_core::proto::VerifiedRuntimeLaunch {
                                sandbox_id: sandbox.to_string(),
                                environment_reference: reference.clone(),
                                integrity_requirement: pplx_space_node_runtime_core::proto::ArtifactIntegrityRequirement::AuthenticatedImmutableSources.into(),
                            }),
                            ..Default::default()
                        };
                        wire.set_runtime(pplx_space_common_core::types::RuntimeKind::Firecracker.into());
                        wire.try_into().unwrap()
                    },
                    None,
                    |id, _| {
                        assert_eq!(id, sandbox);
                        assert!(
                            current,
                            "superseded work must not inspect or adopt a runtime"
                        );
                        inspected.store(true, Ordering::SeqCst);
                        async move { Ok(resident.then_some(123)) }
                    },
                )
                .await;
            assert_eq!(inspected.load(Ordering::SeqCst), current);
            if current && resident {
                let response = result.unwrap().into_inner();
                assert_eq!(response.pid, 123);
                assert_eq!(response.accepted_environment_reference, reference);
            } else {
                assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
            }
            assert_eq!(
                service
                    .execution_store
                    .cpu_budget(sandbox, execution)
                    .unwrap(),
                None
            );
            drop(service.runtime_lock(sandbox).try_write().unwrap());
            drop(
                service
                    .launch_lock((sandbox, FIRECRACKER_LAUNCH_LOCK.to_string()))
                    .try_lock()
                    .unwrap(),
            );
        }
    }
}
