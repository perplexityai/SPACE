mod standalone;

use std::collections::HashSet;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use pplx_space_common_core::types::{ProcessSignal, SandboxId};
use pplx_space_common_svc::{UdsPeerAccount, UdsPeerAuthorizer, UdsPeerPrincipal};
use pplx_space_node_runtime_core::cgroup::{self, CgroupLimits};
use pplx_space_node_runtime_core::launch;
use pplx_space_node_runtime_core::launch_admission::runtime_operation_status;
use pplx_space_node_runtime_core::proto::runtime_supervisor_server::RuntimeSupervisor;
use pplx_space_node_runtime_core::proto::{
    LaunchRuntimeResponse, LaunchSidecarRequest, LaunchSidecarResponse, StopProcessRequest,
    StopProcessResponse, StopSandboxRuntimeRequest, StopSandboxRuntimeResponse,
    UpdateCgroupRequest, UpdateCgroupResponse,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use crate::cgroups::{
    apply_firecracker_cgroup_fixups, cgroup_update_error_to_status, terminate_sandbox_cgroups,
};
use crate::inventory::{
    live_firecracker_pidfile, live_runtime_pid, live_sidecar_pid_from_cgroup, sandbox_runtime_pids,
    write_pid_symlink,
};
use crate::process::{
    kill_direct_child_without_pidfd, parse_sandbox_id, sidecar_pid_filename,
    sidecar_program_filename, signal_sandbox_pids, track_child, validate_sidecar_program,
};
use crate::{os, sidecar_launcher};

const SIDECAR_TERM_GRACE: Duration = Duration::from_millis(500);
const SIDECAR_TERM_POLL: Duration = Duration::from_millis(5);
const SIDECAR_EXEC_TIMEOUT: Duration = Duration::from_secs(5);
const ALLOW_NODE_MANAGER_ONLY: &[UdsPeerPrincipal] = &[UdsPeerPrincipal::Root];
const NODE_MANAGER_BINARY: &str = "space-nodemgr-svc";
const FIRECRACKER_LAUNCH_LOCK: &str = "firecracker";
const MAX_RUNTIME_BLOCKING_OPERATIONS: usize = 32;
const RUNTIME_BLOCKING_ADMISSION_TIMEOUT: Duration = Duration::from_secs(5);

// Bound waiting, not mutation: once acquired, ownership survives cancellation
// until the blocking child and the durable publication/cleanup have completed.
async fn await_runtime_lock<T>(lock: impl std::future::Future<Output = T>) -> Result<T, Status> {
    tokio::time::timeout(RUNTIME_BLOCKING_ADMISSION_TIMEOUT, lock)
        .await
        .map_err(|_| Status::unavailable("runtime mutation ownership is busy; retry"))
}

/// Kills a newly spawned sidecar unless launch reaches the point where its PID
/// is durably published. The guard holds a pidfd-backed process handle while
/// the PID is registered with `fork_children`, so every error path (including
/// cancellation while awaiting the ownership signal) targets the original
/// child and leaves it eligible for SIGCHLD reaping.
struct PendingSidecarLaunch {
    process: launch::ProcessHandle,
    armed: bool,
}

impl PendingSidecarLaunch {
    fn new(pid: u32) -> Result<Self, launch::ProcessHandleError> {
        Ok(Self {
            process: launch::ProcessHandle::open(pid)?,
            armed: true,
        })
    }

    fn disarm(mut self) {
        self.armed = false;
    }

    fn require_executable(&self, program: &str) -> Result<(), Status> {
        let pid = self.process.pid();
        match self.process.program_matches(program).map_err(|error| {
            Status::unavailable(format!("inspect sidecar pid {pid} executable: {error}"))
        })? {
            Some(true) => Ok(()),
            Some(false) => Err(Status::failed_precondition(format!(
                "sidecar pid {pid} did not exec {program}"
            ))),
            None => Err(Status::unavailable(format!(
                "sidecar pid {pid} exited before launch completed"
            ))),
        }
    }

    fn kill(&self) {
        if let Err(error) = self.process.signal(libc::SIGKILL) {
            tracing::warn!(
                pid = self.process.pid(),
                %error,
                "failed to kill sidecar after incomplete launch"
            );
        }
    }
}

impl Drop for PendingSidecarLaunch {
    fn drop(&mut self) {
        if self.armed {
            // A pidfd continues to identify the original child after waitpid
            // reaps it, so an early-exit race can never signal a reused PID.
            self.kill();
        }
    }
}

fn track_pending_sidecar(
    fork_children: &Arc<Mutex<HashSet<u32>>>,
    pid: u32,
) -> Result<PendingSidecarLaunch, Status> {
    let pending_launch = match PendingSidecarLaunch::new(pid) {
        Ok(pending_launch) => pending_launch,
        Err(error) => {
            kill_direct_child_without_pidfd(fork_children, pid);
            return Err(Status::internal(format!(
                "open sidecar process handle for pid {pid}: {error}"
            )));
        }
    };
    // Open the pidfd while the direct child is still unreaped, then publish
    // the PID to the SIGCHLD reaper. Reaping first would make the numeric PID
    // reusable before the launch guard acquires its stable identity.
    track_child(fork_children, pid);
    Ok(pending_launch)
}

type LaunchKey = (SandboxId, String);
type LaunchLock = Arc<tokio::sync::Mutex<()>>;
// The maps keep weak entries so cancelled RPCs cannot leave live locks behind.
type LaunchLocks = Arc<DashMap<LaunchKey, Weak<tokio::sync::Mutex<()>>>>;
type RuntimeLock = Arc<tokio::sync::RwLock<()>>;
type RuntimeLocks = Arc<DashMap<SandboxId, Weak<tokio::sync::RwLock<()>>>>;

#[derive(Clone)]
pub struct RuntimeSupervisorService {
    execution_store: crate::execution_store::ExecutionStore,
    fork_children: Arc<Mutex<HashSet<u32>>>,
    launch_locks: LaunchLocks,
    runtime_locks: RuntimeLocks,
    blocking_slots: Arc<Semaphore>,
    operation_slots: Arc<Semaphore>,
    operations: tokio_util::task::TaskTracker,
    reaper_tasks: tokio_util::task::TaskTracker,
    reaper_shutdown: CancellationToken,
    peer_authorizer: UdsPeerAuthorizer,
    shutdown: CancellationToken,
    jailer_start_timeout: Duration,
}

impl Default for RuntimeSupervisorService {
    fn default() -> Self {
        Self::new(CancellationToken::new())
    }
}

impl RuntimeSupervisorService {
    pub fn new(shutdown: CancellationToken) -> Self {
        Self {
            execution_store: crate::execution_store::ExecutionStore::default(),
            fork_children: Arc::default(),
            launch_locks: Arc::default(),
            runtime_locks: Arc::default(),
            blocking_slots: Arc::new(Semaphore::new(MAX_RUNTIME_BLOCKING_OPERATIONS)),
            operation_slots: Arc::new(Semaphore::new(MAX_RUNTIME_BLOCKING_OPERATIONS)),
            operations: tokio_util::task::TaskTracker::new(),
            reaper_tasks: tokio_util::task::TaskTracker::new(),
            // Admission shutdown must not stop reaping accepted launches.
            reaper_shutdown: CancellationToken::new(),
            peer_authorizer: UdsPeerAuthorizer::new(
                "space-runtime-supervisor",
                std::iter::empty::<UdsPeerAccount>(),
            ),
            shutdown,
            jailer_start_timeout: launch::DEFAULT_JAILER_START_TIMEOUT,
        }
    }

    pub fn with_jailer_start_timeout(mut self, timeout: Duration) -> Self {
        self.jailer_start_timeout = timeout;
        self
    }

    pub async fn drain(&self) {
        self.shutdown.cancel();
        self.operations.close();
        self.operations.wait().await;
        self.operation_slots.close();
        self.reaper_shutdown.cancel();
        self.reaper_tasks.close();
        self.reaper_tasks.wait().await;
    }

    async fn run_owned_operation<T: Send + 'static>(
        &self,
        work: impl std::future::Future<Output = Result<T, Status>> + Send + 'static,
    ) -> Result<T, Status> {
        use tracing::Instrument as _;
        let (permit, ownership) = self.admit_operation().await?;
        // The handler awaits the work, so it runs in the request's span and
        // what it logs carries the request's trace, as node-manager's owned
        // operations do.
        self.operations
            .spawn(
                async move {
                    let _ownership = ownership;
                    let _permit = permit;
                    work.await
                }
                .in_current_span(),
            )
            .await
            .map_err(|error| Status::internal(format!("runtime operation task failed: {error}")))?
    }

    async fn admit_operation(
        &self,
    ) -> Result<
        (
            OwnedSemaphorePermit,
            tokio_util::task::task_tracker::TaskTrackerToken,
        ),
        Status,
    > {
        // Register before checking admission: drain must own operations admitted
        // but not yet spawned, and a closed tracker must reject late arrivals.
        let ownership = self.operations.token();
        let permit = tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => return Err(Status::unavailable("runtime supervisor is draining")),
            result = tokio::time::timeout(RUNTIME_BLOCKING_ADMISSION_TIMEOUT, self.operation_slots.clone().acquire_owned()) =>
                result.map_err(|_| Status::resource_exhausted("runtime operation capacity exhausted"))?
                    .map_err(|_| Status::unavailable("runtime operation admission closed"))?,
        };
        Ok((permit, ownership))
    }

    pub fn spawn_tracked_child_reaper(&self) -> std::io::Result<()> {
        os::spawn_tracked_child_reaper(
            self.fork_children.clone(),
            self.reaper_shutdown.clone(),
            &self.reaper_tasks,
        )
    }

    async fn acquire_blocking_slot(&self) -> Result<OwnedSemaphorePermit, Status> {
        tokio::time::timeout(
            RUNTIME_BLOCKING_ADMISSION_TIMEOUT,
            self.blocking_slots.clone().acquire_owned(),
        )
        .await
        .map_err(|_| {
            Status::resource_exhausted("runtime supervisor blocking-work capacity exhausted")
        })?
        .map_err(|_| Status::unavailable("runtime supervisor blocking-work queue closed"))
    }

    async fn spawn_sidecar_launcher<CreateCgroup>(
        &self,
        sandbox_id: SandboxId,
        config: launch::SidecarLauncherConfig,
        create_cgroup: CreateCgroup,
    ) -> Result<sidecar_launcher::SpawnedSidecarLauncher, Status>
    where
        CreateCgroup: FnOnce() -> Result<cgroup::CgroupCreateResult, Status>,
    {
        let launcher_slot = self.acquire_blocking_slot().await?;
        let prepared_launcher = sidecar_launcher::prepare(config, launcher_slot)
            .await
            .map_err(Status::internal)?;
        let cgroup_result = create_cgroup()?;
        match prepared_launcher.spawn() {
            Ok(launcher) => Ok(launcher),
            Err(error) => {
                if cgroup_result.created {
                    // Another sidecar launch may already be using this shared
                    // group. Reclaim an empty group only; never kill its users.
                    if let Err(cleanup_error) = self.reclaim_failed_sidecar_cgroup(sandbox_id).await
                    {
                        tracing::warn!(%sandbox_id, %cleanup_error, "failed to reclaim sidecar cgroup after spawn failure");
                    }
                }
                Err(Status::internal(error))
            }
        }
    }

    async fn reclaim_failed_sidecar_cgroup(&self, sandbox_id: SandboxId) -> Result<(), Status> {
        let permit = self.acquire_blocking_slot().await?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            cgroup::reclaim_unpopulated_cgroup(std::path::Path::new(&cgroup::sidecar_cgroup_path(
                &sandbox_id,
            )))
        })
        .await
        .map_err(|error| Status::internal(format!("sidecar cgroup cleanup task failed: {error}")))?
        .map(|_| ())
        .map_err(Status::internal)
    }

    async fn apply_firecracker_cgroup_fixups(&self, sandbox_id: SandboxId, pid: u32) {
        let blocking_slot = match self.blocking_slots.clone().try_acquire_owned() {
            Ok(blocking_slot) => blocking_slot,
            Err(_) => {
                tracing::warn!(%sandbox_id, pid, "skipping firecracker cgroup fixups under blocking-work saturation");
                return;
            }
        };
        apply_firecracker_cgroup_fixups(sandbox_id, pid, blocking_slot).await;
    }

    fn authorize_peer<T>(
        &self,
        request: &Request<T>,
        operation: &'static str,
    ) -> Result<(), Status> {
        let peer =
            self.peer_authorizer
                .authorize_request(request, operation, ALLOW_NODE_MANAGER_ONLY)?;
        let Some(pid) = peer.identity().pid() else {
            tracing::warn!(
                operation,
                "rejecting runtime-supervisor RPC without peer pid"
            );
            return Err(Status::unauthenticated("UDS peer pid missing"));
        };
        let Some(pid) = u32::try_from(pid).ok() else {
            tracing::warn!(
                operation,
                peer_pid = pid,
                "rejecting runtime-supervisor RPC with invalid peer pid"
            );
            return Err(Status::unauthenticated("UDS peer pid invalid"));
        };
        if !os::process_matches_binary(pid, NODE_MANAGER_BINARY) {
            tracing::warn!(
                operation,
                peer_pid = pid,
                "rejecting runtime-supervisor RPC from non-node-manager process"
            );
            return Err(Status::permission_denied(
                "runtime supervisor only accepts node-manager RPCs",
            ));
        }
        Ok(())
    }

    fn launch_lock(&self, key: LaunchKey) -> LaunchLock {
        self.launch_locks
            .retain(|_, existing| existing.strong_count() > 0);
        match self.launch_locks.entry(key) {
            Entry::Occupied(mut entry) => {
                if let Some(lock) = entry.get().upgrade() {
                    lock
                } else {
                    let lock = Arc::new(tokio::sync::Mutex::new(()));
                    entry.insert(Arc::downgrade(&lock));
                    lock
                }
            }
            Entry::Vacant(entry) => {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                entry.insert(Arc::downgrade(&lock));
                lock
            }
        }
    }

    fn runtime_lock(&self, sandbox_id: SandboxId) -> RuntimeLock {
        self.runtime_locks
            .retain(|_, existing| existing.strong_count() > 0);
        match self.runtime_locks.entry(sandbox_id) {
            Entry::Occupied(mut entry) => {
                if let Some(lock) = entry.get().upgrade() {
                    lock
                } else {
                    let lock = Arc::new(tokio::sync::RwLock::new(()));
                    entry.insert(Arc::downgrade(&lock));
                    lock
                }
            }
            Entry::Vacant(entry) => {
                let lock = Arc::new(tokio::sync::RwLock::new(()));
                entry.insert(Arc::downgrade(&lock));
                lock
            }
        }
    }
}

fn required_update_cgroup_limits(
    req: UpdateCgroupRequest,
) -> Result<pplx_space_node_runtime_core::proto::CgroupLimits, Status> {
    req.cgroup_limits
        .ok_or_else(|| Status::invalid_argument("cgroup_limits is required"))
}

mod execution;
#[cfg(test)]
mod tests;

#[tonic::async_trait]
impl RuntimeSupervisor for RuntimeSupervisorService {
    async fn bind_execution(
        &self,
        request: Request<pplx_space_node_runtime_core::proto::BindExecutionRequest>,
    ) -> Result<Response<pplx_space_node_runtime_core::proto::BindExecutionResponse>, Status> {
        self.authorize_peer(&request, "BindExecution")?;
        let caller = execution::pin_caller(&request)?;
        // Binding can create absent authority. Acquire ownership before
        // detaching work, so a timed-out queued bind cannot follow retirement.
        let sandbox = parse_sandbox_id(&request.get_ref().sandbox_id)?;
        let guard = await_runtime_lock(self.runtime_lock(sandbox).write_owned()).await?;
        let service = self.clone();
        self.run_owned_operation(async move {
            service
                .bind_execution_owned(request.into_inner(), caller, guard)
                .await?;
            Ok(Response::new(
                pplx_space_node_runtime_core::proto::BindExecutionResponse {},
            ))
        })
        .await
    }

    async fn retire_execution(
        &self,
        request: Request<pplx_space_node_runtime_core::proto::RetireExecutionRequest>,
    ) -> Result<Response<pplx_space_node_runtime_core::proto::RetireExecutionResponse>, Status>
    {
        self.authorize_peer(&request, "RetireExecution")?;
        let caller = execution::pin_caller(&request)?;
        let service = self.clone();
        self.run_owned_operation(async move {
            service
                .retire_execution_owned(request.into_inner(), caller)
                .await?;
            Ok(Response::new(
                pplx_space_node_runtime_core::proto::RetireExecutionResponse {},
            ))
        })
        .await
    }

    async fn launch_runtime(
        &self,
        request: Request<pplx_space_node_runtime_core::proto::LaunchRuntimeRequest>,
    ) -> Result<Response<pplx_space_node_runtime_core::proto::LaunchRuntimeResponse>, Status> {
        self.authorize_peer(&request, "LaunchRuntime")?;
        let caller = Some(execution::pin_caller(&request)?);
        let request = request.into_inner();
        if request.runtime
            == pplx_space_execution_common::proto::RuntimeKind::CloudHypervisor as i32
        {
            let service = self.clone();
            return self
                .run_owned_operation(async move {
                    service.launch_standalone_owned(request, caller).await
                })
                .await;
        }
        let request = FirecrackerLaunch::try_from(request)
            .map_err(pplx_space_node_runtime_core::launch_admission::launch_contract_status)?;
        let service = self.clone();
        self.run_owned_operation(async move {
            let response = service
                .launch_firecracker_owned(request, caller)
                .await?
                .into_inner();
            Ok(Response::new(
                pplx_space_node_runtime_core::proto::LaunchRuntimeResponse {
                    pid: response.pid,
                    accepted_environment_reference: response.accepted_environment_reference,
                },
            ))
        })
        .await
        .map_err(runtime_operation_status)
    }

    async fn launch_sidecar(
        &self,
        request: Request<LaunchSidecarRequest>,
    ) -> Result<Response<LaunchSidecarResponse>, Status> {
        self.authorize_peer(&request, "LaunchSidecar")?;
        let caller = Some(execution::pin_caller(&request)?);
        let request = request.into_inner();
        let service = self.clone();
        self.run_owned_operation(async move { service.launch_sidecar_owned(request, caller).await })
            .await
    }

    async fn stop_process(
        &self,
        request: Request<StopProcessRequest>,
    ) -> Result<Response<StopProcessResponse>, Status> {
        self.authorize_peer(&request, "StopProcess")?;
        let caller = Some(execution::pin_caller(&request)?);
        let req = request.into_inner();
        let sandbox_id = parse_sandbox_id(&req.sandbox_id)?;
        let lock = self.runtime_lock(sandbox_id);
        let _guard = await_runtime_lock(lock.read()).await?;
        execution::check_caller(caller.as_ref())?;
        self.check_execution(sandbox_id, &req.execution_id, None, false)
            .await?;
        let signal = i32::try_from(req.signal)
            .ok()
            .and_then(|signal| ProcessSignal::try_from_i32(signal).ok())
            .ok_or_else(|| Status::invalid_argument("signal must be a valid process signal"))?;
        if signal.as_i32() != libc::SIGTERM && signal.as_i32() != libc::SIGKILL {
            return Err(Status::invalid_argument(format!(
                "only SIGTERM ({}) and SIGKILL ({}) are allowed, got {}",
                libc::SIGTERM,
                libc::SIGKILL,
                signal.as_i32(),
            )));
        }
        let process = match launch::ProcessHandle::open(req.pid) {
            Ok(process) => process,
            Err(error) if error.is_not_found() => {
                return Ok(Response::new(StopProcessResponse { success: false }));
            }
            Err(error) => {
                return Err(Status::failed_precondition(format!(
                    "open process handle: {error}"
                )));
            }
        };
        match process
            .sandbox_ownership(&sandbox_id.to_string())
            .map_err(|error| {
                Status::unavailable(format!("process ownership unavailable: {error}"))
            })? {
            launch::ProcessOwnership::Gone => {
                return Ok(Response::new(StopProcessResponse { success: false }));
            }
            launch::ProcessOwnership::Foreign => {
                return Err(Status::permission_denied(format!(
                    "pid {} does not belong to sandbox {}",
                    process.pid(),
                    sandbox_id
                )));
            }
            launch::ProcessOwnership::Owned => {}
        }
        let success = process
            .signal(signal.as_i32())
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(StopProcessResponse { success }))
    }

    async fn stop_sandbox_runtime(
        &self,
        request: Request<StopSandboxRuntimeRequest>,
    ) -> Result<Response<StopSandboxRuntimeResponse>, Status> {
        self.authorize_peer(&request, "StopSandboxRuntime")?;
        let caller = Some(execution::pin_caller(&request)?);
        let request = request.into_inner();
        let service = self.clone();
        self.run_owned_operation(async move {
            service.stop_sandbox_runtime_owned(request, caller).await
        })
        .await
    }

    async fn update_cgroup(
        &self,
        request: Request<UpdateCgroupRequest>,
    ) -> Result<Response<UpdateCgroupResponse>, Status> {
        self.authorize_peer(&request, "UpdateCgroup")?;
        let caller = Some(execution::pin_caller(&request)?);
        let request = request.into_inner();
        let service = self.clone();
        self.run_owned_operation(async move { service.update_cgroup_owned(request, caller).await })
            .await
    }
}

impl RuntimeSupervisorService {
    async fn prepare_firecracker_launch(
        &self,
        sandbox_id: SandboxId,
        execution: pplx_space_common_core::types::SandboxActivationAttemptId,
        budget: pplx_space_common_core::types::VcpuCount,
        sealed_launch: Option<crate::verified_launch::VerifiedLaunch>,
    ) -> Result<Option<crate::verified_launch::OpenedRuntime>, Status> {
        self.execution_io(move |store| {
            // Authentication is a precondition, not an uncertain spawn.
            let opened = sealed_launch
                .as_ref()
                .map(|launch| {
                    launch
                        .open()
                        .map_err(pplx_space_node_runtime_core::launch_admission::admission_refusal)
                })
                .transpose()?;
            let previous = store.cpu_budget(sandbox_id, execution).map_err(|error| {
                Status::failed_precondition(format!("read CPU launch budget: {error}"))
            })?;
            if previous.is_none()
                && !cgroup::firecracker_is_quiescent(&sandbox_id.to_string()).map_err(|error| {
                    Status::unavailable(format!("VM occupancy unavailable: {error}"))
                })?
            {
                return Err(Status::failed_precondition(
                    "cannot establish CPU budget for an unidentified live VM",
                ));
            }
            store
                .record_cpu_budget(sandbox_id, execution, budget)
                .map_err(|error| {
                    Status::failed_precondition(format!("record CPU launch budget: {error}"))
                })?;
            Ok(opened)
        })
        .await
    }

    async fn adopt_firecracker(
        &self,
        sandbox_id: SandboxId,
        chroot_base_dir: &str,
    ) -> Result<Option<u32>, Status> {
        if let Some(pid) = live_runtime_pid(&sandbox_id, "fc.pid")? {
            tracing::info!(
                sandbox_id = %sandbox_id,
                pid,
                source = "run_dir_pid",
                "runtime supervisor found live firecracker"
            );
            self.apply_firecracker_cgroup_fixups(sandbox_id, pid).await;
            return Ok(Some(pid));
        }
        if let Some(pid) = live_firecracker_pidfile(&sandbox_id, chroot_base_dir)? {
            write_pid_symlink(&sandbox_id, "fc.pid", pid)?;
            tracing::info!(
                sandbox_id = %sandbox_id,
                pid,
                source = "firecracker_pidfile",
                "runtime supervisor recovered firecracker pid"
            );
            self.apply_firecracker_cgroup_fixups(sandbox_id, pid).await;
            return Ok(Some(pid));
        }
        Ok(None)
    }

    async fn launch_firecracker_owned(
        &self,
        req: FirecrackerLaunch,
        caller: Option<launch::ProcessHandle>,
    ) -> Result<Response<LaunchRuntimeResponse>, Status> {
        self.launch_firecracker_with(req, caller, |id, root| async move {
            self.adopt_firecracker(id, &root).await
        })
        .await
    }

    async fn launch_firecracker_with<F>(
        &self,
        req: FirecrackerLaunch,
        caller: Option<launch::ProcessHandle>,
        adopt: impl FnOnce(SandboxId, String) -> F,
    ) -> Result<Response<LaunchRuntimeResponse>, Status>
    where
        F: std::future::Future<Output = Result<Option<u32>, Status>>,
    {
        let environment = req.environment_reference;
        let accepted_environment_reference = environment
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let sandbox_id = req.sandbox_id;
        let launch_key = (sandbox_id, FIRECRACKER_LAUNCH_LOCK.to_string());
        let launch_lock = self.launch_lock(launch_key.clone());
        let launch_guard = await_runtime_lock(launch_lock.lock()).await?;
        let runtime_lock = self.runtime_lock(sandbox_id);
        let runtime_guard = await_runtime_lock(runtime_lock.read()).await?;
        execution::check_caller(caller.as_ref())?;
        let execution = self
            .check_parsed_execution(sandbox_id, req.execution_id, None, false)
            .await?;
        let result = async {
            let sandbox_id = launch_key.0;
            if let Some(limits) = req.cgroup_limits.as_ref() {
                cgroup::validate_cgroup_update(limits.vcpu_count, limits.memory_mib, limits.pids_max)
                    .map_err(cgroup_update_error_to_status)?;
            }
            if let Some(pid) = adopt(sandbox_id, req.chroot_base_dir.as_str().to_owned()).await? {
                return Ok(Response::new(LaunchRuntimeResponse {
                    pid,
                    accepted_environment_reference: accepted_environment_reference.clone(),
                }));
            }
            // A resident execution retains its original launch contract across
            // catalog/cache changes. Only a new process needs current sources.
            let sealed_launch = environment
                .map(|reference| crate::verified_launch::VerifiedLaunch::resolve(reference, &req))
                .transpose()
                .map_err(pplx_space_node_runtime_core::launch_admission::admission_refusal)?;
            let launch_sandbox_id = sandbox_id;
            let cgroup_limits = req
                .cgroup_limits
                .as_ref()
                .map(|limits| {
                    CgroupLimits::from_request(
                        limits.vcpu_count,
                        limits.memory_mib,
                        limits.pids_max,
                    )
                })
                .unwrap_or_default();
            let budget = cgroup_limits.cpu_budget().map_err(cgroup_update_error_to_status)?;
            // Existing-PID recovery returns above without inventing launch evidence.
            // Publish before spawn and retain it if launch completion is uncertain.
            let opened = self.prepare_firecracker_launch(
                sandbox_id, execution, budget, sealed_launch,
            ).await?;
            let blocking_slot = self.acquire_blocking_slot().await?;
            let jailer_start_timeout = self.jailer_start_timeout;
            let launch_result = tokio::task::spawn_blocking(move || {
                let _blocking_slot = blocking_slot;
                launch::launch_via_jailer(launch::JailerLaunch {
                    sandbox_id: &launch_sandbox_id.to_string(),
                    jailer: opened.as_ref().map_or(launch::JailerExecutable::LegacyPath(req.jailer_bin.as_str()), |files| launch::JailerExecutable::Sealed(&files.jailer)),
                    firecracker_bin: opened.as_ref().map_or(req.firecracker_bin.as_str(), |files| files.firecracker_path.as_str()),
                    chroot_base_dir: req.chroot_base_dir.as_str(),
                    ns_path: req.ns_path.as_str(),
                    uid: req.uid,
                    gid: req.gid,
                    fc_config_path: req.fc_config_path.as_ref().map_or("", |path| path.as_str()),
                    fc_log_path: req.fc_log_path.as_ref().map_or("", |path| path.as_str()),
                    fc_metrics_path: req.fc_metrics_path.as_ref().map_or("", |path| path.as_str()),
                    fc_console_path: req.fc_console_path.as_ref().map_or("", |path| path.as_str()),
                    cgroup_limits: &cgroup_limits,
                    jailer_start_timeout,
                })
            })
            .await
            .map_err(|error| Status::internal(format!("firecracker launch task failed: {error}")))
            .and_then(|result| result.map_err(Status::internal));
            let pid = match launch_result {
                Ok(pid) => pid,
                Err(error) => {
                    // The owned launch still holds its execution/launch guards;
                    // its blocking permit has been released before drain polling.
                    if let Err(cleanup_error) = terminate_sandbox_cgroups(
                        sandbox_id, Arc::clone(&self.blocking_slots), cgroup::CgroupScope::Firecracker,
                    ).await {
                        tracing::warn!(%sandbox_id, %cleanup_error, "failed to clean up Firecracker launch");
                    }
                    return Err(error);
                }
            };
            if let Err(error) = write_pid_symlink(&sandbox_id, "fc.pid", pid) {
                match launch::ProcessHandle::open(pid) {
                    Ok(handle) => {
                        if let Err(cleanup_error) =
                            handle.signal_if_owned(&sandbox_id.to_string(), libc::SIGKILL)
                        {
                            tracing::warn!(%sandbox_id, pid, %cleanup_error, "failed to kill firecracker after pid publication failure");
                        }
                    }
                    Err(cleanup_error) if cleanup_error.is_not_found() => {}
                    Err(cleanup_error) => {
                        tracing::warn!(%sandbox_id, pid, %cleanup_error, "failed to pin firecracker after pid publication failure");
                    }
                }
                return Err(error);
            }
            self.apply_firecracker_cgroup_fixups(sandbox_id, pid).await;
            tracing::info!(
                sandbox_id = %sandbox_id,
                pid,
                "runtime supervisor launched firecracker"
            );

            Ok(Response::new(LaunchRuntimeResponse {
                pid,
                accepted_environment_reference: accepted_environment_reference.clone(),
            }))
        }
        .await;
        drop(runtime_guard);
        drop(launch_guard);
        result
    }

    async fn launch_sidecar_owned(
        &self,
        mut req: LaunchSidecarRequest,
        caller: Option<launch::ProcessHandle>,
    ) -> Result<Response<LaunchSidecarResponse>, Status> {
        let sandbox_id = parse_sandbox_id(&req.sandbox_id)?;
        validate_sidecar_program(&req.program)?;
        let is_gateway =
            req.program == pplx_space_common_core::host::linux::paths::NET_GATEWAY_SVC_BIN;
        let program_filename = sidecar_program_filename(&req.program)?;
        let pid_filename = sidecar_pid_filename(&req.program)?;
        let launch_key = (sandbox_id, req.program.clone());
        let launch_lock = self.launch_lock(launch_key.clone());
        let launch_guard = Arc::new(await_runtime_lock(launch_lock.lock_owned()).await?);
        let runtime_lock = self.runtime_lock(sandbox_id);
        let runtime_guard = Arc::new(await_runtime_lock(runtime_lock.read_owned()).await?);
        execution::check_caller(caller.as_ref())?;
        self.check_execution(sandbox_id, &req.execution_id, None, false)
            .await?;
        let result = async {
            let sandbox_id = launch_key.0;
            if let Some(pid) = live_runtime_pid(&sandbox_id, &pid_filename)?
                && require_sidecar_identity(pid, &req.program, req.uid, req.gid)?
            {
                if is_gateway {
                    require_recorded_gateway(self, sandbox_id, pid, req.uid).await?;
                }
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    program = %program_filename,
                    pid,
                    source = "run_dir_pid",
                    "runtime supervisor found live sidecar"
                );
                return Ok(Response::new(LaunchSidecarResponse { pid }));
            }
            if let Some(pid) = live_sidecar_pid_from_cgroup(&sandbox_id, &program_filename)?
                && require_sidecar_identity(pid, &req.program, req.uid, req.gid)?
            {
                if is_gateway {
                    require_recorded_gateway(self, sandbox_id, pid, req.uid).await?;
                }
                write_pid_symlink(&sandbox_id, &pid_filename, pid)?;
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    program = %program_filename,
                    pid,
                    source = "cgroup",
                    "runtime supervisor recovered sidecar pid"
                );
                return Ok(Response::new(LaunchSidecarResponse { pid }));
            }
            let capability = is_gateway
                .then(pplx_space_node_runtime_core::gateway_launch::new_capability)
                .transpose()
                .map_err(|_| Status::internal("generate gateway launch capability"))?;
            if let Some(capability) = &capability {
                req.env.insert(
                    pplx_space_node_runtime_core::gateway_launch::CAPABILITY_ENV.to_owned(),
                    capability.to_string(),
                );
            }
            let pre_exec_sockets = req
                .pre_exec_sockets
                .into_iter()
                .map(|socket| {
                    Ok(launch::PreExecSocket {
                        bind_addr: socket.bind_addr.parse().map_err(|error| {
                            Status::invalid_argument(format!(
                                "invalid pre-exec bind address {}: {error}",
                                socket.bind_addr
                            ))
                        })?,
                        tcp: socket.tcp,
                        transparent: socket.transparent,
                        recvorigdstaddr: socket.recvorigdstaddr,
                        fd_arg: socket.fd_arg,
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?;
            let launcher = self
                .spawn_sidecar_launcher(
                    sandbox_id,
                    launch::SidecarLauncherConfig {
                        ns_path: req.ns_path,
                        program: req.program,
                        args: req.args,
                        env: req.env,
                        uid: req.uid,
                        gid: req.gid,
                        cgroup_path: cgroup::sidecar_cgroup_path(&sandbox_id),
                        pre_exec_sockets,
                        grant_cap_net_admin: req.grant_cap_net_admin,
                    },
                    || {
                        cgroup::create_cgroup(&sandbox_id, &CgroupLimits::default()).map_err(
                            |error| Status::internal(format!("create sidecar cgroup: {error}")),
                        )
                    },
                )
                .await?;
            let pid = launcher.pid;
            // Track before awaiting the exec handshake. Caller cancellation
            // does not cancel this owned operation; an incomplete launch still
            // kills the original child and the SIGCHLD task reaps it.
            let pending_launch = match track_pending_sidecar(&self.fork_children, pid) {
                Ok(pending_launch) => pending_launch,
                Err(status) => return Err(status),
            };
            let exec_result = tokio::time::timeout(SIDECAR_EXEC_TIMEOUT, launcher.wait_for_exec())
                .await
                .map_err(|_| {
                    Status::deadline_exceeded(format!(
                        "sidecar pid {pid} did not exec within {}ms",
                        SIDECAR_EXEC_TIMEOUT.as_millis()
                    ))
                })?
                .map_err(Status::failed_precondition);
            if let Err(status) = exec_result {
                drop(pending_launch);
                return Err(status);
            }
            pending_launch.require_executable(&program_filename)?;
            let pending_launch = if let Some(capability) = capability {
                let slot = self.acquire_blocking_slot().await?;
                let evidence_launch_guard = launch_guard.clone();
                let evidence_runtime_guard = runtime_guard.clone();
                tokio::task::spawn_blocking(move || {
                    let _slot = slot;
                    let _launch_guard = evidence_launch_guard;
                    let _runtime_guard = evidence_runtime_guard;
                    pplx_space_node_runtime_core::gateway_launch::record(
                        sandbox_id,
                        &pending_launch.process,
                        req.uid,
                        &capability,
                    )
                    .map_err(|error| {
                        tracing::warn!(%sandbox_id, %error, "gateway launch evidence publication failed");
                        Status::failed_precondition("record gateway launch evidence")
                    })?;
                    Ok::<_, Status>(pending_launch)
                })
                .await
                .map_err(|_| Status::internal("gateway launch evidence task failed"))??
            } else {
                pending_launch
            };
            write_pid_symlink(&sandbox_id, &pid_filename, pid)?;
            pending_launch.disarm();
            tracing::info!(
                sandbox_id = %sandbox_id,
                program = %program_filename,
                pid,
                "runtime supervisor launched sidecar"
            );
            Ok(Response::new(LaunchSidecarResponse { pid }))
        }
        .await;
        drop(runtime_guard);
        drop(launch_guard);
        result
    }

    async fn stop_sandbox_runtime_owned(
        &self,
        req: StopSandboxRuntimeRequest,
        caller: Option<launch::ProcessHandle>,
    ) -> Result<Response<StopSandboxRuntimeResponse>, Status> {
        self.stop_sandbox_runtime_with(
            req,
            caller,
            pplx_space_node_runtime_core::gateway_launch::remove,
            |sandbox_id, immediate_kill| self.stop_runtime_processes(sandbox_id, immediate_kill),
        )
        .await
    }

    async fn stop_sandbox_runtime_with<R, C, F>(
        &self,
        req: StopSandboxRuntimeRequest,
        caller: Option<launch::ProcessHandle>,
        retire: R,
        cleanup: C,
    ) -> Result<Response<StopSandboxRuntimeResponse>, Status>
    where
        R: FnOnce(
                SandboxId,
            )
                -> Result<(), pplx_space_node_runtime_core::gateway_launch::GatewayLaunchError>
            + Send
            + 'static,
        C: FnOnce(SandboxId, bool) -> F,
        F: std::future::Future<Output = Result<Response<StopSandboxRuntimeResponse>, Status>>,
    {
        let sandbox_id = parse_sandbox_id(&req.sandbox_id)?;
        let runtime_lock = self.runtime_lock(sandbox_id);
        let runtime_guard = await_runtime_lock(runtime_lock.write_owned()).await?;
        execution::check_caller(caller.as_ref())?;
        self.check_execution(
            sandbox_id,
            &req.execution_id,
            req.previous_execution_id.as_deref(),
            true,
        )
        .await?;
        // Attempt revocation first even if process cleanup will fail, but a
        // broken evidence store must not prevent termination. Keep exclusion
        // outside the blocking task too: its error or panic must not admit a
        // replacement before process/cgroup cleanup finishes.
        let runtime_guard = Arc::new(runtime_guard);
        let retirement = async {
            let slot = self.acquire_blocking_slot().await?;
            let guard = runtime_guard.clone();
            tokio::task::spawn_blocking(move || {
                let _slot = slot;
                let _guard = guard;
                retire(sandbox_id)
            })
            .await
            .map_err(|_| Status::internal("gateway authority retirement task failed"))?
            .map_err(|error| {
                tracing::warn!(%sandbox_id, %error, "gateway authority retirement failed");
                Status::unavailable("retire gateway launch authority")
            })
        }
        .await;
        if let Err(error) = &retirement {
            tracing::warn!(%sandbox_id, %error, "continuing runtime cleanup after authority retirement failure");
        }
        let result = cleanup(sandbox_id, req.immediate_kill).await;
        drop(runtime_guard);
        // Cleanup failure takes precedence, but a failed retirement remains
        // unacknowledged so the lifecycle owner retries after repair.
        let response = result?;
        retirement?;
        Ok(response)
    }

    async fn stop_runtime_processes(
        &self,
        sandbox_id: SandboxId,
        immediate_kill: bool,
    ) -> Result<Response<StopSandboxRuntimeResponse>, Status> {
        let mut actions = Vec::new();
        let pids = sandbox_runtime_pids(&sandbox_id);
        let term_signal = if immediate_kill {
            libc::SIGKILL
        } else {
            libc::SIGTERM
        };
        signal_sandbox_pids(&sandbox_id, &pids, term_signal, &mut actions);
        if !immediate_kill {
            let started = tokio::time::Instant::now();
            let still_alive = loop {
                let still_alive = sandbox_runtime_pids(&sandbox_id)
                    .into_iter()
                    .filter(|(_, pid)| launch::is_pid_alive(*pid))
                    .collect::<Vec<_>>();
                if still_alive.is_empty() || started.elapsed() >= SIDECAR_TERM_GRACE {
                    break still_alive;
                }
                tokio::time::sleep(SIDECAR_TERM_POLL).await;
            };
            signal_sandbox_pids(&sandbox_id, &still_alive, libc::SIGKILL, &mut actions);
        }
        terminate_sandbox_cgroups(
            sandbox_id,
            Arc::clone(&self.blocking_slots),
            cgroup::CgroupScope::Sandbox,
        )
        .await
        .map(|cgroup_actions| {
            actions.extend(cgroup_actions);
            Response::new(StopSandboxRuntimeResponse { actions })
        })
    }

    async fn update_cgroup_owned(
        &self,
        req: UpdateCgroupRequest,
        caller: Option<launch::ProcessHandle>,
    ) -> Result<Response<UpdateCgroupResponse>, Status> {
        let sandbox_id = parse_sandbox_id(&req.sandbox_id)?;
        let execution_id = req.execution_id.clone();
        let cgroup_limits = required_update_cgroup_limits(req)?;
        let launch_key = (sandbox_id, FIRECRACKER_LAUNCH_LOCK.to_string());
        let launch_lock = self.launch_lock(launch_key.clone());
        let launch_guard = await_runtime_lock(launch_lock.lock()).await?;
        let runtime_lock = self.runtime_lock(sandbox_id);
        let runtime_guard = await_runtime_lock(runtime_lock.read()).await?;
        execution::check_caller(caller.as_ref())?;
        let execution = self
            .check_execution(sandbox_id, &execution_id, None, false)
            .await?;
        cgroup::validate_cgroup_update(
            cgroup_limits.vcpu_count,
            cgroup_limits.memory_mib,
            cgroup_limits.pids_max,
        )
        .map_err(cgroup_update_error_to_status)?;
        if let Some(quota) = cgroup_limits.vcpu_count {
            self.execution_io(move |store| {
                let budget = store
                    .cpu_budget(sandbox_id, execution)
                    .map_err(|error| {
                        Status::failed_precondition(format!("read CPU launch budget: {error}"))
                    })?;
                match budget {
                    Some(budget) if quota > budget.get().get() => {
                        return Err(Status::invalid_argument(
                            "CPU quota exceeds the execution's initial launch budget",
                        ));
                    }
                    Some(_) => {}
                    None => {
                        let nonincreasing = cgroup::cpu_quota_is_nonincreasing(
                            &sandbox_id.to_string(), quota,
                        ).map_err(|error| Status::failed_precondition(format!(
                            "CPU launch budget is absent and current quota cannot be verified: {error}"
                        )))?;
                        if !nonincreasing {
                            return Err(Status::failed_precondition(
                                "CPU launch budget is absent; reactivate before raising CPU quota",
                            ));
                        }
                    }
                }
                Ok(())
            })
            .await?;
        }
        let blocking_slot = self.acquire_blocking_slot().await?;
        let result = tokio::task::spawn_blocking(move || {
            let _blocking_slot = blocking_slot;
            cgroup::update_firecracker_cgroup(
                &sandbox_id.to_string(),
                cgroup_limits.vcpu_count,
                cgroup_limits.memory_mib,
                cgroup_limits.pids_max,
            )
        })
        .await
        .map_err(|error| Status::internal(format!("cgroup update task failed: {error}")))?
        .map_err(cgroup_update_error_to_status);
        drop(runtime_guard);
        drop(launch_guard);
        result?;
        Ok(Response::new(UpdateCgroupResponse {}))
    }
}

/// Refuse adoption when an existing gateway lacks its original launch evidence.
async fn require_recorded_gateway(
    service: &RuntimeSupervisorService,
    sandbox_id: SandboxId,
    pid: u32,
    uid: u32,
) -> Result<(), Status> {
    let slot = service.acquire_blocking_slot().await?;
    tokio::task::spawn_blocking(move || {
        let _slot = slot;
        pplx_space_node_runtime_core::gateway_launch::require_recorded(sandbox_id, pid, uid)
    })
    .await
    .map_err(|error| {
        tracing::warn!(%error, %sandbox_id, pid, uid, "gateway launch evidence task failed");
        Status::internal("gateway launch evidence task failed")
    })?
    .map_err(|error| {
        tracing::warn!(%error, %sandbox_id, pid, uid, "gateway adoption launch evidence rejected");
        Status::failed_precondition("gateway has no valid launch evidence; cycle the gateway")
    })
}

/// Return false only when the pinned process has disappeared.
fn require_sidecar_identity(pid: u32, program: &str, uid: u32, gid: u32) -> Result<bool, Status> {
    let handle = match launch::ProcessHandle::open(pid) {
        Ok(handle) => handle,
        Err(error) if error.is_not_found() => return Ok(false),
        Err(error) => return Err(Status::unavailable(format!("pin live sidecar: {error}"))),
    };
    match handle
        .program_matches(program)
        .map_err(|error| Status::unavailable(format!("inspect live sidecar executable: {error}")))?
    {
        None => return Ok(false),
        Some(false) => {
            return Err(Status::failed_precondition(
                "live sidecar binary differs from requested program",
            ));
        }
        Some(true) => {}
    }
    match handle.credentials_match(uid, gid).map_err(|error| {
        Status::unavailable(format!("inspect live sidecar credentials: {error}"))
    })? {
        None => Ok(false),
        Some(true) => Ok(true),
        Some(false) => Err(Status::failed_precondition(
            "live sidecar credentials differ from requested identity",
        )),
    }
}

use pplx_space_node_runtime_core::runtime_launch::FirecrackerLaunch;
