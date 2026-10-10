use super::*;
use crate::runtime_supervisor_client::tests::{
    StubSupervisorServer, SupervisorScript, scripted_client, stop_supervisor_server,
};
use crate::service::tests::service_tests::{bind_test_execution, service_for_unit_tests};
use crate::slot_pool::{NewLeaseMemoryReservation, SlotState};
use pplx_space_common_core::types::SandboxActivationAttemptId;
use pplx_space_node_mgr_client::execution_fence::ExecutionFence;
use std::sync::{Arc, Mutex};
use tonic::Code;

const AMPLE_NODE_MEMORY: &str = "MemTotal: 67108864 kB\nMemAvailable: 67108864 kB\n";

/// A unit-test node manager with one ready slot, a measured node memory file,
/// and a scripted runtime supervisor.
struct Node {
    service: NodeManagerService,
    memory: tempfile::TempDir,
    _supervisor_socket: tempfile::TempDir,
    supervisor: StubSupervisorServer,
}

impl Node {
    async fn start(script: SupervisorScript) -> Self {
        let (supervisor_socket, client, supervisor) = scripted_client(script).await;
        let mut service = service_for_unit_tests().await;
        service.runtime_supervisor = client;
        service.pool.set_all_slots_ready_for_test();
        let memory = tempfile::tempdir().unwrap();
        std::fs::create_dir(memory.path().join("cgroups")).unwrap();
        service.memory_admission_sources = MemoryAdmissionSources::new(
            memory.path().join("meminfo"),
            memory.path().join("cgroups"),
        );
        let node = Self {
            service,
            memory,
            _supervisor_socket: supervisor_socket,
            supervisor,
        };
        node.set_node_memory(Some(AMPLE_NODE_MEMORY));
        node
    }

    fn set_node_memory(&self, meminfo: Option<&str>) {
        let path = self.memory.path().join("meminfo");
        match meminfo {
            Some(contents) => std::fs::write(path, contents).unwrap(),
            None => std::fs::remove_file(path).unwrap(),
        }
    }

    async fn lease(&self, sandbox_id: &str) {
        self.service
            .pool
            .allocate(
                sandbox_id,
                None,
                Some(NewLeaseMemoryReservation {
                    memory_reservation_mib: 1024,
                }),
            )
            .await
            .unwrap();
    }

    fn lease_state(&self) -> Vec<(String, SlotState, u64, Option<u64>)> {
        self.service
            .pool
            .all_records()
            .unwrap()
            .into_iter()
            .map(|record| {
                (
                    record.info.sandbox_id,
                    record.state,
                    record.info.generation,
                    record.memory_reservation_mib,
                )
            })
            .collect()
    }

    async fn stop(self) {
        stop_supervisor_server(self.supervisor).await;
    }
}

fn rpc<T>(message: T, caller: Option<u32>) -> Request<T> {
    let mut request = Request::new(message);
    request
        .extensions_mut()
        .insert(pplx_space_common_svc::PeerIdentity::new(
            0,
            0,
            caller.map(|pid| i32::try_from(pid).unwrap()),
        ));
    request
}

/// The test runner outlives the test, so it stands in for a live lifecycle caller.
fn live_caller() -> Option<u32> {
    Some(std::os::unix::process::parent_id())
}

fn fresh_fence() -> ExecutionFence {
    ExecutionFence::new(SandboxActivationAttemptId::new(), None)
}

/// A shell blocked on stdin. Naming the sandbox in its argv makes the node
/// manager treat it as that sandbox's process.
fn blocked_process(sandbox_id: Option<&str>) -> std::process::Child {
    let child = std::process::Command::new("sh")
        .args(["-c", "read _"])
        .args(sandbox_id)
        .stdin(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    // `spawn` returns once the child is inside exec, before the kernel
    // publishes the new argv that the ownership check reads.
    if let Some(sandbox_id) = sandbox_id {
        let cmdline = format!("/proc/{}/cmdline", child.id());
        let give_up = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !std::fs::read(&cmdline)
            .unwrap_or_default()
            .split(|byte| *byte == 0)
            .any(|arg| arg == sandbox_id.as_bytes())
        {
            assert!(
                std::time::Instant::now() < give_up,
                "{cmdline} never showed the {sandbox_id} marker"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
    child
}

/// Waits until the child exits but leaves it unreaped, so its pid stays pinned.
fn wait_for_exit_unreaped(pid: u32) {
    nix::sys::wait::waitid(
        nix::sys::wait::Id::Pid(nix::unistd::Pid::from_raw(i32::try_from(pid).unwrap())),
        nix::sys::wait::WaitPidFlag::WEXITED | nix::sys::wait::WaitPidFlag::WNOWAIT,
    )
    .unwrap();
}

pub(in crate::service) fn exited_process() -> std::process::Child {
    let child = std::process::Command::new("true").spawn().unwrap();
    wait_for_exit_unreaped(child.id());
    child
}

fn finish(mut child: std::process::Child) {
    let _ = child.kill();
    child.wait().unwrap();
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AllocateRefusal {
    UnpinnedCaller,
    ExitedCaller,
    MissingExecution,
    SupersededExecution,
    SupervisorBindFailure,
    UnnamedPodNetns,
    MissingPodNetns,
    MissingMemoryDemand,
    ClosedLimiter,
    UnreadableNodeMemory,
    InsufficientNodeMemory,
    OverflowingMemoryDemand,
    DrainingPool,
    NoReadySlot,
}

#[rstest::rstest]
#[case::unpinned_caller(
    AllocateRefusal::UnpinnedCaller,
    Code::PermissionDenied,
    "kernel peer process identity"
)]
#[case::exited_caller(
    AllocateRefusal::ExitedCaller,
    Code::Aborted,
    "caller exited before node mutation"
)]
#[case::missing_execution(
    AllocateRefusal::MissingExecution,
    Code::FailedPrecondition,
    "execution identity is required"
)]
#[case::superseded_execution(
    AllocateRefusal::SupersededExecution,
    Code::FailedPrecondition,
    "release the previous execution"
)]
#[case::supervisor_bind_failure(
    AllocateRefusal::SupervisorBindFailure,
    Code::Unavailable,
    "bind execution unavailable"
)]
#[case::unnamed_pod_netns(
    AllocateRefusal::UnnamedPodNetns,
    Code::InvalidArgument,
    "invalid netns_path"
)]
#[case::missing_pod_netns(
    AllocateRefusal::MissingPodNetns,
    Code::NotFound,
    "pod namespace not found"
)]
#[case::missing_memory_demand(
    AllocateRefusal::MissingMemoryDemand,
    Code::InvalidArgument,
    "sandbox_cgroup_memory_mib is required"
)]
#[case::closed_limiter(
    AllocateRefusal::ClosedLimiter,
    Code::Unavailable,
    "lifecycle limiter closed"
)]
#[case::unreadable_node_memory(
    AllocateRefusal::UnreadableNodeMemory,
    Code::Unavailable,
    "cannot observe node memory capacity"
)]
#[case::insufficient_node_memory(
    AllocateRefusal::InsufficientNodeMemory,
    Code::ResourceExhausted,
    "above total memory"
)]
#[case::overflowing_memory_demand(
    AllocateRefusal::OverflowingMemoryDemand,
    Code::ResourceExhausted,
    "overflows"
)]
#[case::draining_pool(
    AllocateRefusal::DrainingPool,
    Code::ResourceExhausted,
    "network slot pool is draining"
)]
#[case::no_ready_slot(
    AllocateRefusal::NoReadySlot,
    Code::ResourceExhausted,
    "no ready network slots"
)]
#[tokio::test]
async fn allocate_refusal_grants_no_lease(
    #[case] refusal: AllocateRefusal,
    #[case] code: Code,
    #[case] message: &str,
) {
    let mut node = Node::start(SupervisorScript {
        bind_execution_failure: (refusal == AllocateRefusal::SupervisorBindFailure)
            .then_some(Code::Unavailable),
        ..Default::default()
    })
    .await;
    let sandbox_id = SandboxId::new().to_string();
    let mut request = AllocateRequest {
        sandbox_id: sandbox_id.clone(),
        netns_path: None,
        sandbox_cgroup_memory_mib: 2048,
        execution_fence: Some(fresh_fence().into()),
    };
    let mut caller = live_caller();
    let mut exited = None;
    match refusal {
        AllocateRefusal::UnpinnedCaller => caller = None,
        AllocateRefusal::ExitedCaller => {
            let child = exited_process();
            caller = Some(child.id());
            exited = Some(child);
        }
        AllocateRefusal::MissingExecution => request.execution_fence = None,
        AllocateRefusal::SupersededExecution => {
            bind_test_execution(&node.service, &sandbox_id);
        }
        AllocateRefusal::SupervisorBindFailure => {}
        // A pod netns sends allocation through slot setup, which rebuilds
        // host networking. A draining pool refuses before that if the netns
        // check regresses.
        AllocateRefusal::UnnamedPodNetns => {
            request.netns_path = Some("/".to_string());
            node.service.pool.set_draining(true).unwrap();
        }
        AllocateRefusal::MissingPodNetns => {
            request.netns_path = Some(format!("/var/run/netns/space-test-{sandbox_id}"));
            node.service.pool.set_draining(true).unwrap();
        }
        AllocateRefusal::MissingMemoryDemand => request.sandbox_cgroup_memory_mib = 0,
        AllocateRefusal::ClosedLimiter => node.service.lifecycle_limiter.close(),
        AllocateRefusal::UnreadableNodeMemory => node.set_node_memory(None),
        AllocateRefusal::InsufficientNodeMemory => {
            node.set_node_memory(Some("MemTotal: 2097152 kB\nMemAvailable: 2097152 kB\n"));
        }
        AllocateRefusal::OverflowingMemoryDemand => {
            node.service.memory_admission_config.reserve_mib = u64::MAX;
        }
        AllocateRefusal::DrainingPool => node.service.pool.set_draining(true).unwrap(),
        AllocateRefusal::NoReadySlot => {
            node.service.pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(1));
        }
    }

    let error = node
        .service
        .allocate(rpc(request, caller))
        .await
        .unwrap_err();

    assert_eq!(error.code(), code, "{error}");
    assert!(error.message().contains(message), "{error}");
    assert!(node.service.pool.get(&sandbox_id).unwrap().is_none());
    assert!(
        node.service
            .pool
            .active_memory_reservations()
            .unwrap()
            .is_empty()
    );
    if let Some(child) = exited {
        finish(child);
    }
    node.stop().await;
}

#[tokio::test]
async fn allocate_grants_a_lease_then_returns_it_without_new_admission() {
    let node = Node::start(SupervisorScript::default()).await;
    let sandbox_id = SandboxId::new().to_string();
    let fence = fresh_fence();
    let request = |sandbox_cgroup_memory_mib| {
        rpc(
            AllocateRequest {
                sandbox_id: sandbox_id.clone(),
                netns_path: None,
                sandbox_cgroup_memory_mib,
                execution_fence: Some(fence.into()),
            },
            live_caller(),
        )
    };

    let granted = node
        .service
        .allocate(request(2048))
        .await
        .unwrap()
        .into_inner();

    let lease = node.service.pool.get_record(&sandbox_id).unwrap().unwrap();
    assert_eq!(lease.state, SlotState::Leased);
    assert_eq!(lease.memory_reservation_mib, Some(2048));
    assert_eq!(
        (
            granted.slot,
            granted.slot_generation,
            granted.sandbox_uid,
            granted.slot_label.clone(),
            granted.namespace.clone(),
            granted.namespace_path.clone(),
        ),
        (
            lease.info.slot,
            lease.info.generation,
            lease.info.sandbox_uid,
            lease.info.slot_label(),
            lease.info.ns_name(),
            lease.info.ns_path(),
        )
    );
    assert_eq!(
        node.service.pool.execution_fence(&sandbox_id).unwrap(),
        Some(fence)
    );

    // A retry for the leased sandbox needs neither demand nor a memory reading.
    node.set_node_memory(None);
    let retried = node
        .service
        .allocate(request(0))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retried, granted);
    node.stop().await;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReleaseRefusal {
    MissingExecution,
    SupersededExecution,
    LiveProcessWithoutForce,
    ClosedLimiter,
}

#[rstest::rstest]
#[case::missing_execution(
    ReleaseRefusal::MissingExecution,
    Code::FailedPrecondition,
    "execution identity is missing or superseded"
)]
#[case::superseded_execution(
    ReleaseRefusal::SupersededExecution,
    Code::FailedPrecondition,
    "execution identity is missing or superseded"
)]
#[case::live_process_without_force(
    ReleaseRefusal::LiveProcessWithoutForce,
    Code::FailedPrecondition,
    "sandbox has a running process"
)]
#[case::closed_limiter(
    ReleaseRefusal::ClosedLimiter,
    Code::Unavailable,
    "lifecycle limiter closed"
)]
#[tokio::test]
async fn release_refusal_keeps_the_lease(
    #[case] refusal: ReleaseRefusal,
    #[case] code: Code,
    #[case] message: &str,
) {
    // The supervisor refuses runtime stop, so a refusal that regresses ends
    // there, before slot release and root-namespace DNAT cleanup.
    let node = Node::start(SupervisorScript::default()).await;
    let sandbox_id = SandboxId::new().to_string();
    let fence = bind_test_execution(&node.service, &sandbox_id);
    node.lease(&sandbox_id).await;
    let process = blocked_process(Some(&sandbox_id));
    node.service
        .pool
        .set_pid(&sandbox_id, process.id())
        .unwrap();
    let mut request = ReleaseRequest {
        sandbox_id: sandbox_id.clone(),
        force: true,
        execution_fence: Some(fence),
    };
    match refusal {
        ReleaseRefusal::MissingExecution => request.execution_fence = None,
        ReleaseRefusal::SupersededExecution => {
            request.execution_fence = Some(fresh_fence().into());
        }
        ReleaseRefusal::LiveProcessWithoutForce => request.force = false,
        ReleaseRefusal::ClosedLimiter => node.service.lifecycle_limiter.close(),
    }
    let before = node.lease_state();

    let error = node.service.release(rpc(request, None)).await.unwrap_err();

    assert_eq!(error.code(), code, "{error}");
    assert!(error.message().contains(message), "{error}");
    if refusal == ReleaseRefusal::LiveProcessWithoutForce {
        assert!(
            error.message().contains(&process.id().to_string()),
            "{error}"
        );
    }
    assert_eq!(node.lease_state(), before);
    finish(process);
    node.stop().await;
}

/// The supervisor would report a signalled process, so a refusal that leaks
/// through to it turns into a successful response.
fn signalling_supervisor() -> SupervisorScript {
    SupervisorScript {
        stop_process: Some(Arc::new(|_| {
            Ok(pplx_space_node_runtime_core::proto::StopProcessResponse { success: true })
        })),
        ..Default::default()
    }
}

fn stop_request(
    sandbox_id: &str,
    fence: crate::proto::ExecutionFence,
    pid: Option<u32>,
) -> StopProcessRequest {
    StopProcessRequest {
        sandbox_id: sandbox_id.to_string(),
        signal: libc::SIGTERM as u32,
        pid,
        execution_fence: Some(fence),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StopRefusal {
    SupersededExecution,
    DisallowedSignal,
    NotAllocated,
    NoLaunchedProcess,
    NodeManagerPid,
    ForeignProcess,
}

#[rstest::rstest]
#[case::superseded_execution(
    StopRefusal::SupersededExecution,
    Code::FailedPrecondition,
    "execution identity is missing or superseded"
)]
#[case::disallowed_signal(
    StopRefusal::DisallowedSignal,
    Code::InvalidArgument,
    "only SIGTERM (15) and SIGKILL (9) are allowed, got 1"
)]
#[case::not_allocated(StopRefusal::NotAllocated, Code::NotFound, "sandbox not allocated")]
#[case::no_launched_process(
    StopRefusal::NoLaunchedProcess,
    Code::FailedPrecondition,
    "sandbox has no launched process"
)]
#[case::node_manager_pid(
    StopRefusal::NodeManagerPid,
    Code::FailedPrecondition,
    "refusing to signal node-manager pid"
)]
#[case::foreign_process(
    StopRefusal::ForeignProcess,
    Code::PermissionDenied,
    "process does not belong to sandbox"
)]
#[tokio::test]
async fn stop_process_refuses_without_signalling(
    #[case] refusal: StopRefusal,
    #[case] code: Code,
    #[case] message: &str,
) {
    let node = Node::start(signalling_supervisor()).await;
    let sandbox_id = SandboxId::new().to_string();
    let fence = bind_test_execution(&node.service, &sandbox_id);
    if refusal != StopRefusal::NotAllocated {
        node.lease(&sandbox_id).await;
    }
    let foreign = blocked_process(None);
    let mut request = stop_request(&sandbox_id, fence, Some(foreign.id()));
    match refusal {
        StopRefusal::SupersededExecution => request.execution_fence = Some(fresh_fence().into()),
        StopRefusal::DisallowedSignal => request.signal = 1,
        StopRefusal::NotAllocated | StopRefusal::ForeignProcess => {}
        StopRefusal::NoLaunchedProcess => request.pid = None,
        StopRefusal::NodeManagerPid => request.pid = Some(std::process::id()),
    }

    let error = node
        .service
        .stop_process(rpc(request, None))
        .await
        .unwrap_err();

    assert_eq!(error.code(), code, "{error}");
    assert!(error.message().contains(message), "{error}");
    finish(foreign);
    node.stop().await;
}

/// Linux never assigns a pid above `PID_MAX_LIMIT` (2^22), so no process can
/// hold this one.
const UNASSIGNABLE_PID: u32 = i32::MAX as u32;

#[rstest::rstest]
#[case::exited_unreaped(false)]
#[case::absent(true)]
#[tokio::test]
async fn stop_process_reports_an_already_gone_process_as_not_stopped(#[case] absent: bool) {
    let node = Node::start(signalling_supervisor()).await;
    let sandbox_id = SandboxId::new().to_string();
    let fence = bind_test_execution(&node.service, &sandbox_id);
    node.lease(&sandbox_id).await;
    let unreaped = (!absent).then(exited_process);
    let pid = unreaped
        .as_ref()
        .map_or(UNASSIGNABLE_PID, std::process::Child::id);

    let response = node
        .service
        .stop_process(rpc(stop_request(&sandbox_id, fence, Some(pid)), None))
        .await
        .unwrap()
        .into_inner();

    assert!(!response.success);
    if let Some(child) = unreaped {
        finish(child);
    }
    node.stop().await;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SupervisorStop {
    Signalled,
    NotSignalled,
    RejectedWhileOwned,
    RejectedAfterExit,
    Unavailable,
}

#[rstest::rstest]
#[case::signalled(SupervisorStop::Signalled, Ok(true))]
#[case::not_signalled(SupervisorStop::NotSignalled, Ok(false))]
#[case::rejected_while_owned(SupervisorStop::RejectedWhileOwned, Err(Code::PermissionDenied))]
#[case::rejected_after_exit(SupervisorStop::RejectedAfterExit, Ok(false))]
#[case::unavailable(SupervisorStop::Unavailable, Err(Code::Unavailable))]
#[tokio::test]
async fn stop_process_forwards_an_owned_process_to_the_supervisor(
    #[case] supervisor: SupervisorStop,
    #[case] expected: Result<bool, Code>,
) {
    let sandbox_id = SandboxId::new().to_string();
    let process = blocked_process(Some(&sandbox_id));
    let pid = process.id();
    let forwarded = Arc::new(Mutex::new(Vec::new()));
    let node = Node::start(SupervisorScript {
        stop_process: Some(Arc::new({
            let forwarded = Arc::clone(&forwarded);
            move |request| {
                forwarded.lock().unwrap().push(request);
                match supervisor {
                    SupervisorStop::Signalled | SupervisorStop::NotSignalled => {
                        Ok(pplx_space_node_runtime_core::proto::StopProcessResponse {
                            success: supervisor == SupervisorStop::Signalled,
                        })
                    }
                    SupervisorStop::RejectedWhileOwned => {
                        Err(Status::permission_denied("supervisor refused"))
                    }
                    SupervisorStop::RejectedAfterExit => {
                        nix::sys::signal::kill(
                            nix::unistd::Pid::from_raw(i32::try_from(pid).unwrap()),
                            nix::sys::signal::Signal::SIGKILL,
                        )
                        .unwrap();
                        wait_for_exit_unreaped(pid);
                        Err(Status::failed_precondition("process vanished"))
                    }
                    SupervisorStop::Unavailable => Err(Status::unavailable("supervisor busy")),
                }
            }
        })),
        ..Default::default()
    })
    .await;
    let fence = bind_test_execution(&node.service, &sandbox_id);
    node.lease(&sandbox_id).await;
    node.service.pool.set_pid(&sandbox_id, pid).unwrap();

    // Without an explicit pid the request addresses the tracked process.
    let result = node
        .service
        .stop_process(rpc(stop_request(&sandbox_id, fence.clone(), None), None))
        .await;

    match (result, expected) {
        (Ok(response), Ok(success)) => assert_eq!(response.into_inner().success, success),
        (Err(error), Err(code)) => {
            assert_eq!(error.code(), code, "{error}");
            assert!(
                error.message().starts_with("stop process failed"),
                "{error}"
            );
        }
        (result, expected) => panic!("expected {expected:?}, got {result:?}"),
    }
    let forwarded = forwarded.lock().unwrap().clone();
    assert_eq!(
        forwarded,
        [pplx_space_node_runtime_core::proto::StopProcessRequest {
            execution_id: fence.execution_id,
            sandbox_id: sandbox_id.clone(),
            pid,
            signal: libc::SIGTERM as u32,
        }]
    );
    finish(process);
    node.stop().await;
}
