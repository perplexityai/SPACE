use super::cleanup_helpers::*;
use super::disk::*;
use super::validation::*;
use super::*;
use pplx_space_common_core::host::linux::paths;
use pplx_space_common_core::types::SandboxId;
use pplx_space_events::{
    SandboxEvent, SandboxEventBoolField, SandboxEventDetails, SandboxEventSource, SandboxEventType,
};
use tracing::Instrument as _;

type CleanupOperationResult = Result<CleanupSandboxResponse, Status>;
const CLEANUP_OPERATION_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const CLEANUP_PHASE_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
const CLEANUP_CLAIM_WARNING_INTERVAL: Duration = Duration::from_secs(30);

pub(super) async fn wait_for_cleanup_phase<T>(
    shutdown_token: &tokio_util::sync::CancellationToken,
    phase: impl Future<Output = T>,
    timeout_message: &'static str,
) -> Result<T, Status> {
    tokio::select! {
        biased;
        _ = shutdown_token.cancelled() => {
            Err(Status::unavailable("node manager is shutting down"))
        }
        result = tokio::time::timeout(CLEANUP_PHASE_ACQUIRE_TIMEOUT, phase) => {
            result.map_err(|_| Status::unavailable(timeout_message))
        }
    }
}

#[derive(Clone)]
struct InFlightCleanupOperation {
    request: CleanupSandboxRequest,
    result: tokio::sync::watch::Receiver<Option<CleanupOperationResult>>,
    started_at: Instant,
}

/// Owns node-local cleanup mutations independently of any one gRPC waiter.
/// Identical retries join the in-flight result; a different intent waits for
/// the incumbent mutation to finish before it may claim the sandbox.
#[derive(Clone)]
pub(super) struct CleanupOperationRegistry {
    operations: std::sync::Arc<dashmap::DashMap<SandboxId, InFlightCleanupOperation>>,
    task_tracker: tokio_util::task::TaskTracker,
    admission_open: std::sync::Arc<std::sync::Mutex<bool>>,
    shutdown_token: tokio_util::sync::CancellationToken,
    waiter_timeout: Duration,
}

struct CleanupOperationClaim {
    operations: std::sync::Arc<dashmap::DashMap<SandboxId, InFlightCleanupOperation>>,
    sandbox_id: SandboxId,
    armed: bool,
}

impl CleanupOperationClaim {
    fn release(&mut self) {
        self.armed = false;
        self.operations.remove(&self.sandbox_id);
        record_cleanup_registry_snapshot(&self.operations);
    }

    fn complete(mut self, publish_result: impl FnOnce()) {
        publish_result();
        self.release();
    }
}

#[cfg(test)]
impl Default for CleanupOperationRegistry {
    fn default() -> Self {
        Self::new(tokio_util::task::TaskTracker::new())
    }
}

fn record_cleanup_registry_snapshot(
    operations: &dashmap::DashMap<SandboxId, InFlightCleanupOperation>,
) {
    let now = Instant::now();
    let oldest_age = operations
        .iter()
        .map(|entry| {
            now.saturating_duration_since(entry.started_at)
                .as_secs_f64()
        })
        .fold(0.0, f64::max);
    pplx_space_metrics::node_manager::cleanup_operation_in_flight(operations.len());
    pplx_space_metrics::node_manager::cleanup_operation_oldest_claim_age_seconds(oldest_age);
}

impl Drop for CleanupOperationClaim {
    fn drop(&mut self) {
        if self.armed {
            self.operations.remove(&self.sandbox_id);
            record_cleanup_registry_snapshot(&self.operations);
        }
    }
}

impl CleanupOperationRegistry {
    pub(super) fn new(task_tracker: tokio_util::task::TaskTracker) -> Self {
        Self {
            operations: std::sync::Arc::new(dashmap::DashMap::new()),
            task_tracker,
            admission_open: std::sync::Arc::new(std::sync::Mutex::new(true)),
            shutdown_token: tokio_util::sync::CancellationToken::new(),
            waiter_timeout: CLEANUP_OPERATION_WAIT_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_waiter_timeout(
        task_tracker: tokio_util::task::TaskTracker,
        waiter_timeout: Duration,
    ) -> Self {
        Self {
            waiter_timeout,
            ..Self::new(task_tracker)
        }
    }

    pub(super) fn stop_admitting(&self) {
        let mut admission_open = self.admission_open.lock().expect("mutex poisoned");
        *admission_open = false;
        self.shutdown_token.cancel();
    }

    /// Start or join one cleanup operation. The spawned mutation deliberately
    /// outlives cancellation of the caller so blocking filesystem work cannot
    /// escape the sandbox serialization boundary.
    async fn run<F, Fut>(
        &self,
        sandbox_id: SandboxId,
        request: CleanupSandboxRequest,
        operation: F,
    ) -> Result<CleanupSandboxResponse, Status>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = CleanupOperationResult> + Send + 'static,
    {
        let mut operation = Some(operation);
        loop {
            // Serialize shutdown with the only point that may detach a new
            // mutation. Once admission closes, the service owner can close
            // the task tracker knowing no cleanup task can appear afterward.
            let (mut result, same_request, operation_owner);
            {
                let admission_open = self.admission_open.lock().expect("mutex poisoned");
                if !*admission_open {
                    return Err(Status::unavailable("node manager is shutting down"));
                }
                (result, same_request, operation_owner) = match self.operations.entry(sandbox_id) {
                    dashmap::mapref::entry::Entry::Occupied(entry) => {
                        let in_flight = entry.get();
                        (
                            in_flight.result.clone(),
                            in_flight.request == request,
                            false,
                        )
                    }
                    dashmap::mapref::entry::Entry::Vacant(entry) => {
                        let (result_tx, result_rx) = tokio::sync::watch::channel(None);
                        let started_at = Instant::now();
                        entry.insert(InFlightCleanupOperation {
                            request: request.clone(),
                            result: result_rx.clone(),
                            started_at,
                        });
                        record_cleanup_registry_snapshot(&self.operations);
                        let claim = CleanupOperationClaim {
                            operations: std::sync::Arc::clone(&self.operations),
                            sandbox_id,
                            armed: true,
                        };
                        let operation = operation
                            .take()
                            .expect("cleanup operation closure is consumed only by its own claim");
                        let operations = std::sync::Arc::clone(&self.operations);
                        // The owner's request span follows the operation, so
                        // its cleanup events and calls stay in that request's
                        // trace, as `run_owned_operation`'s work does.
                        self.task_tracker.spawn(async move {
                            let operation = operation();
                            tokio::pin!(operation);
                            let mut warning = tokio::time::interval_at(
                                tokio::time::Instant::now() + CLEANUP_CLAIM_WARNING_INTERVAL,
                                CLEANUP_CLAIM_WARNING_INTERVAL,
                            );
                            let result = loop {
                                tokio::select! {
                                    result = &mut operation => break result,
                                    _ = warning.tick() => {
                                        let age = started_at.elapsed();
                                        tracing::warn!(
                                            %sandbox_id,
                                            claim_age_secs = age.as_secs(),
                                            "sandbox cleanup operation remains in flight"
                                        );
                                        record_cleanup_registry_snapshot(&operations);
                                    }
                                }
                            };
                            let outcome = if result.is_ok() {
                                pplx_space_metrics::node_manager::CleanupOperationOutcome::Succeeded
                            } else {
                                pplx_space_metrics::node_manager::CleanupOperationOutcome::Failed
                            };
                            pplx_space_metrics::node_manager::cleanup_operation_finished(
                                outcome, started_at,
                            );
                            claim.complete(|| {
                                result_tx.send_replace(Some(result));
                            });
                        }.in_current_span());
                        (result_rx, true, true)
                    }
                };
            }

            let wait_started = Instant::now();
            let completed = tokio::select! {
                _ = self.shutdown_token.cancelled() => {
                    return Err(Status::unavailable("node manager is shutting down"));
                }
                completed = async {
                    let wait_for_result = async {
                        loop {
                            if let Some(result) = result.borrow().clone() {
                                break Some(result);
                            }
                            if result.changed().await.is_err() {
                                break None;
                            }
                        }
                    };
                    if operation_owner {
                        Ok(wait_for_result.await)
                    } else {
                        tokio::time::timeout(self.waiter_timeout, wait_for_result)
                            .await
                            .map_err(|_| {
                                pplx_space_metrics::node_manager::cleanup_operation_waiter_timed_out(wait_started);
                                record_cleanup_registry_snapshot(&self.operations);
                                Status::resource_exhausted(
                                    "sandbox cleanup is still in progress; retry later",
                                )
                            })
                    }
                } => completed?,
            };
            if same_request {
                return completed.unwrap_or_else(|| {
                    Err(Status::internal(
                        "sandbox cleanup operation ended without a result",
                    ))
                });
            }
            // A different cleanup intent must wait for the incumbent mutation
            // to finish, then claim its own operation. Its result is unrelated.
        }
    }
}

impl NodeManagerService {
    pub fn begin_cleanup_shutdown(&self) {
        self.pool.shutdown_token.cancel();
        self.lifecycle_limiter.close();
        self.cleanup_operations.stop_admitting();
    }

    pub(super) async fn cleanup_sandbox(
        &self,
        request: Request<CleanupSandboxRequest>,
    ) -> Result<Response<CleanupSandboxResponse>, Status> {
        self.authorize_peer(&request, "CleanupSandbox", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let caller = super::execution_fence::pin_caller(
            &request,
            request.get_ref().execution_fence.is_some(),
        )?;
        let (typed_sandbox_id, req) = parse_cleanup_request(request.into_inner())?;
        let operation_request = req.clone();
        let service = self.clone();
        let result = self
            .cleanup_operations
            .run(typed_sandbox_id, req, move || async move {
                service
                    .run_cleanup_sandbox_operation(typed_sandbox_id, operation_request, caller)
                    .await
            })
            .await;
        self.release_prepared_rootfs_reservation(typed_sandbox_id)
            .await;
        result.map(Response::new)
    }

    async fn run_cleanup_sandbox_operation(
        &self,
        typed_sandbox_id: SandboxId,
        req: CleanupSandboxRequest,
        caller: Option<launch::ProcessHandle>,
    ) -> CleanupOperationResult {
        let cleanup_started = Instant::now();
        let result = self
            .cleanup_sandbox_phases(typed_sandbox_id, req, cleanup_started, caller)
            .await;
        if let Err(error) = &result {
            SandboxEvent::new(
                typed_sandbox_id,
                SandboxEventType::CleanupFailed,
                SandboxEventSource::NodeManager,
            )
            .emit_with(
                SandboxEventDetails::default()
                    .reason("cleanup_incomplete")
                    .elapsed_ms(cleanup_started.elapsed().as_millis().to_string())
                    .error(error.code().to_string()),
            );
        }
        result
    }

    async fn cleanup_sandbox_phases(
        &self,
        typed_sandbox_id: SandboxId,
        req: CleanupSandboxRequest,
        cleanup_started: Instant,
        caller: Option<launch::ProcessHandle>,
    ) -> CleanupOperationResult {
        let sandbox_id = &req.sandbox_id;
        let _lifecycle_permit = wait_for_cleanup_phase(
            &self.cleanup_operations.shutdown_token,
            self.lifecycle_limiter
                .acquire(crate::lifecycle_limiter::LifecycleMethod::CleanupSandbox),
            "timed out waiting for cleanup capacity",
        )
        .await??;
        let _artifact_guard = wait_for_cleanup_phase(
            &self.cleanup_operations.shutdown_token,
            self.lock_sandbox_artifacts(sandbox_id),
            "timed out waiting for artifact transfer ownership",
        )
        .await??;
        // Also excludes recovery/reset when the active lease is already gone.
        let _mutation_claim = self.claim_sandbox_mutation(sandbox_id)?;
        let info = self.pool.get(sandbox_id).map_err(slot_store_status)?;
        let _cgroup_guard = wait_for_cleanup_phase(
            &self.cleanup_operations.shutdown_token,
            self.lock_sandbox_cgroups(sandbox_id),
            "timed out waiting for sandbox cleanup ownership",
        )
        .await?;
        // A timeout is not permission to race a surviving device mutation.
        super::execution_fence::check_caller(caller.as_ref())?;
        let execution = self.check_execution(sandbox_id, req.execution_fence.as_ref(), true)?;
        let unbound_cleanup = req
            .execution_fence
            .as_ref()
            .map(pplx_space_node_mgr_client::execution_fence::ExecutionFence::try_from)
            .transpose()?;
        // The caller's durable cleanup intent retries this Unavailable result.
        let device_stack_lock = self.sandbox_device_stack_lock(&typed_sandbox_id).await;
        let _device_stack_guard = wait_for_cleanup_phase(
            &self.cleanup_operations.shutdown_token,
            device_stack_lock.lock_owned(),
            "timed out waiting for device-stack cleanup ownership",
        )
        .await?;
        let force = req.force;
        let immediate_kill = req.immediate_kill;
        let return_after_slot_release = req.keep_filesystem && req.return_after_slot_release;
        let mut actions = Vec::new();
        let mut cleanup_errors = Vec::new();
        tracing::info!(
            sandbox_id,
            force,
            immediate_kill,
            keep_filesystem = req.keep_filesystem,
            return_after_slot_release,
            "cleanup_sandbox started"
        );
        SandboxEvent::new(
            typed_sandbox_id,
            SandboxEventType::CleanupStarted,
            SandboxEventSource::NodeManager,
        )
        .emit_with(
            SandboxEventDetails::default()
                .bool_field(SandboxEventBoolField::Force, force)
                .bool_field(SandboxEventBoolField::ImmediateKill, immediate_kill)
                .bool_field(SandboxEventBoolField::KeepFilesystem, req.keep_filesystem)
                .bool_field(
                    SandboxEventBoolField::ReturnAfterSlotRelease,
                    return_after_slot_release,
                ),
        );

        // Read PIDs from symlinks (created by launch_jailer/launch)
        let fc_pid = read_pid_from_symlink(&format!("{RUN_DIR}/{sandbox_id}/fc.pid"));
        let spaced_host_pid =
            read_pid_from_symlink(&format!("{RUN_DIR}/{sandbox_id}/space-spaced-host-svc.pid"));
        let netgtw_pid =
            read_pid_from_symlink(&format!("{RUN_DIR}/{sandbox_id}/space-netgtw-svc.pid"));
        let tracked_pids = [
            ("firecracker", fc_pid),
            ("spaced-host", spaced_host_pid),
            ("net-gateway", netgtw_pid),
        ];

        // 1. Kill processes
        let fc_live = fc_pid.is_some_and(|pid| tracked_sandbox_pid_alive(sandbox_id, pid));
        if let (true, false, Some(pid)) = (fc_live, force, fc_pid) {
            return Err(Status::failed_precondition(format!(
                "FC PID {pid} still alive"
            )));
        }
        let live_processes = live_tracked_processes(sandbox_id, &tracked_pids);
        let killed_processes = live_processes.len();
        if let Some(info) = info.as_ref() {
            self.adopt_stopping_lease(sandbox_id, info)?;
        }
        // Process absence is not proof that the supervisor's owned mutation
        // finished. Require an acknowledged stop before touching its artifacts.
        // A lifecycle retry revalidates ownership and serializes another Stop
        // behind any surviving supervisor work via its runtime write lock.
        actions.extend(
            stop_sandbox_runtime_with_supervisor(self, sandbox_id, immediate_kill, unbound_cleanup)
                .await
                .inspect_err(|error| {
                    tracing::warn!(sandbox_id, %error, "runtime stop unconfirmed; preserving cleanup ownership and artifacts");
                    pplx_space_metrics::sandbox::worker::runtime_stop_failed();
                })?,
        );
        tracing::debug!(
            sandbox_id,
            immediate_kill,
            killed_processes,
            "cleanup process kill summary"
        );
        tracing::info!(
            sandbox_id,
            elapsed_ms = cleanup_started.elapsed().as_millis() as u64,
            "cleanup_sandbox process-kill phase complete"
        );

        if return_after_slot_release {
            // The slot must not be reusable while any host task from the old
            // lease can still run. The tracked process leaders can exit before
            // a Firecracker worker thread clears cgroup.events populated, so
            // wait for both observations before releasing the slot.
            cleanup_errors.extend(
                wait_for_cleanup_quiescence(sandbox_id, &tracked_pids, CLEANUP_QUIESCENCE_TIMEOUT)
                    .await,
            );
            if !cleanup_errors.is_empty() {
                tracing::warn!(
                    sandbox_id,
                    elapsed_ms = cleanup_started.elapsed().as_millis() as u64,
                    errors = ?cleanup_errors,
                    "cleanup_sandbox failed while waiting for runtime quiescence"
                );
                return Err(Status::internal(format!(
                    "sandbox cleanup failed before slot release: {}",
                    cleanup_errors.join("; ")
                )));
            }
        }
        tracing::info!(
            sandbox_id,
            elapsed_ms = cleanup_started.elapsed().as_millis() as u64,
            "cleanup_sandbox cgroup/slot-capture phase complete"
        );

        // 3. Capture the active network lease before filesystem cleanup.
        //
        // Do not tear down namespace nftables or links here. A sandbox can be
        // stopped and then deleted immediately, which means CleanupSandbox may
        // run twice while the slot-pool reset worker is already rebuilding the
        // reusable slot. Mutating namespace networking from this RPC can race
        // with that reset and leave a slot quarantined. Network cleanup belongs
        // to NetworkPool::release/reset.
        let dirty_infos = if return_after_slot_release {
            self.pool
                .dirty_infos_for_sandbox(sandbox_id)
                .map_err(slot_store_status)?
        } else {
            Vec::new()
        };
        if let Some(ref info) = info {
            actions.push(format!(
                "Network slot {} in {} will be reset by slot pool",
                info.slot,
                info.ns_name()
            ));
        }

        // 5. Remove per-run runtime artifacts even when the retained cache is kept.
        actions.extend(cleanup_runtime_artifacts_blocking(sandbox_id, &_lifecycle_permit).await?);
        tracing::info!(
            sandbox_id,
            elapsed_ms = cleanup_started.elapsed().as_millis() as u64,
            action_count = actions.len(),
            error_count = cleanup_errors.len(),
            "cleanup_sandbox runtime-artifact phase complete"
        );

        // 6. Remove filesystem
        if !req.keep_filesystem {
            let sandbox_id_owned = sandbox_id.to_string();
            let (fs_errors, fs_actions) = spawn_lifecycle_blocking(
                &_lifecycle_permit,
                move || -> (Vec<String>, Vec<String>) {
                    let mut errors = Vec::new();
                    let mut actions = Vec::new();
                    let cache_dir = format!("{CACHE_DIR}/{sandbox_id_owned}");
                    cleanup_action(
                        super::artifact_fs::remove(std::path::Path::new(&cache_dir))
                            .map_err(|e| e.to_string()),
                        || format!("Removed {cache_dir}"),
                        &mut errors,
                        &mut actions,
                    );
                    (errors, actions)
                },
            )
            .await
            .unwrap_or_else(|e| {
                (
                    vec![format!("filesystem cleanup task failed: {e}")],
                    Vec::new(),
                )
            });
            if fs_errors.is_empty() {
                SandboxEvent::new(
                    typed_sandbox_id,
                    SandboxEventType::FilesystemRemoved,
                    SandboxEventSource::NodeManager,
                )
                .emit_with(
                    SandboxEventDetails::default()
                        .local_path(format!("{CACHE_DIR}/{sandbox_id}"))
                        .result(if fs_actions.is_empty() {
                            "not_found"
                        } else {
                            "removed"
                        })
                        .action_count(fs_actions.len().to_string())
                        .elapsed_ms(cleanup_started.elapsed().as_millis().to_string()),
                );
            }
            cleanup_errors.extend(fs_errors);
            actions.extend(fs_actions);
        } else if return_after_slot_release {
            actions.push(
                "Deferred retained cache ownership cleanup until after slot release".to_string(),
            );
        } else {
            cleanup_action(
                neutralize_retained_cache_blocking(sandbox_id, &_lifecycle_permit).await,
                || format!("Neutralized retained cache ownership for {sandbox_id}"),
                &mut cleanup_errors,
                &mut actions,
            );
        }
        tracing::info!(
            sandbox_id,
            elapsed_ms = cleanup_started.elapsed().as_millis() as u64,
            action_count = actions.len(),
            error_count = cleanup_errors.len(),
            "cleanup_sandbox filesystem/cache phase complete"
        );
        cleanup_errors.extend(live_tracked_processes(sandbox_id, &tracked_pids));
        cleanup_errors.extend(live_cgroup_processes(sandbox_id));

        if req.keep_filesystem {
            if !return_after_slot_release
                && let Some(info) = info.as_ref()
                && let Err(error) = verify_retained_cache_not_owned_by_uid_blocking(
                    sandbox_id,
                    info.sandbox_uid,
                    &_lifecycle_permit,
                )
                .await
            {
                cleanup_errors.push(error);
            }
        } else {
            for path in [
                paths::isolated_net_gateway_run_dir(sandbox_id),
                format!("{RUN_DIR}/{sandbox_id}"),
                format!("{CACHE_DIR}/{sandbox_id}"),
                format!("{JAIL_DIR}/firecracker/{sandbox_id}"),
                format!("{JAIL_DIR}/cloud-hypervisor/{sandbox_id}"),
                format!("/sys/fs/cgroup/space/sbx-{sandbox_id}"),
                format!("/sys/fs/cgroup/space/{sandbox_id}"),
            ] {
                if let Err(error) = verify_path_absent(&path) {
                    cleanup_errors.push(error);
                }
            }
        }
        if !cleanup_errors.is_empty() {
            tracing::warn!(
                sandbox_id,
                elapsed_ms = cleanup_started.elapsed().as_millis() as u64,
                errors = ?cleanup_errors,
                "cleanup_sandbox failed before slot release"
            );
            return Err(Status::internal(format!(
                "sandbox cleanup failed before slot release: {}",
                cleanup_errors.join("; ")
            )));
        }

        self.stop_guest_dhcp(sandbox_id).await?;

        // 6. Release network slot
        if info.is_some() {
            self.pool.release(sandbox_id).map_err(|error| {
                Status::internal(format!("release network slot failed: {error}"))
            })?;
            actions.push("Released network slot".to_string());
        } else if return_after_slot_release {
            for info in dirty_infos {
                self.pool.enqueue_slot_reset(info.slot);
            }
        }

        if let Some(execution) = execution {
            // Reconcile either side of an interrupted Allocate/Bind before
            // acknowledging retention or retiring the receiver's authority.
            self.runtime_supervisor
                .bind_execution(pplx_space_node_runtime_core::proto::BindExecutionRequest {
                    sandbox_id: sandbox_id.to_owned(),
                    execution_id: execution.current().to_string(),
                    previous_id: execution.previous().map(|id| id.to_string()),
                })
                .await?;
            if !req.keep_filesystem {
                self.runtime_supervisor
                    .retire_execution(
                        pplx_space_node_runtime_core::proto::RetireExecutionRequest {
                            sandbox_id: sandbox_id.to_owned(),
                            execution_id: execution.current().to_string(),
                        },
                    )
                    .await?;
                self.pool
                    .retire_execution(sandbox_id, execution.current())
                    .map_err(slot_store_status)?;
            }
        }
        let retained_execution_id = execution
            .filter(|_| req.keep_filesystem)
            .map(|fence| fence.current().to_string());

        tracing::info!(
            sandbox_id,
            elapsed_ms = cleanup_started.elapsed().as_millis() as u64,
            action_count = actions.len(),
            "cleanup_sandbox completed"
        );
        SandboxEvent::new(
            typed_sandbox_id,
            SandboxEventType::CleanupCompleted,
            SandboxEventSource::NodeManager,
        )
        .emit_with(
            SandboxEventDetails::default()
                .elapsed_ms(cleanup_started.elapsed().as_millis().to_string())
                .action_count(actions.len().to_string())
                .error_count(cleanup_errors.len().to_string())
                .bool_field(SandboxEventBoolField::Force, force)
                .bool_field(SandboxEventBoolField::ImmediateKill, immediate_kill)
                .bool_field(SandboxEventBoolField::KeepFilesystem, req.keep_filesystem)
                .bool_field(
                    SandboxEventBoolField::ReturnAfterSlotRelease,
                    return_after_slot_release,
                ),
        );
        Ok(CleanupSandboxResponse {
            actions,
            retained_execution_id,
        })
    }

    pub(super) async fn inspect_sandbox(
        &self,
        request: Request<InspectSandboxRequest>,
    ) -> Result<Response<InspectSandboxResponse>, Status> {
        self.authorize_peer(&request, "InspectSandbox", ALLOW_NODE_STATUS)?;
        validate_sandbox_id(&request.get_ref().sandbox_id)?;
        if request.get_ref().execution_only {
            let service = self.clone();
            let sandbox_id = request.into_inner().sandbox_id;
            return self
                .off_runtime("inspect execution authority", move || {
                    let mut response = Response::new(InspectSandboxResponse {
                        execution_fence: service
                            .pool
                            .execution_fence(&sandbox_id)
                            .map_err(slot_store_status)?
                            .map(Into::into),
                        sandbox_id,
                        ..Default::default()
                    });
                    #[allow(deprecated)]
                    response.metadata_mut().insert(
                        pplx_space_node_mgr_client::execution_fence::EXECUTION_AUTHORITY_CAPABILITY,
                        tonic::metadata::MetadataValue::from_static("1"),
                    );
                    Ok(response)
                })
                .await;
        }
        let service = self.clone();
        // Sandbox-manager's Stats RPC depends on this inspection for cgroup
        // counters. Optional inventory work must not exhaust its admission.
        self.off_runtime("inspect sandbox", move || {
            service.inspect_sandbox_blocking(request)
        })
        .await
    }

    fn inspect_sandbox_blocking(
        &self,
        request: Request<InspectSandboxRequest>,
    ) -> Result<Response<InspectSandboxResponse>, Status> {
        let req = request.get_ref();
        let sandbox_id = &req.sandbox_id;
        validate_sandbox_id(sandbox_id)?;

        let fc_pid = read_pid_from_symlink(&format!("{RUN_DIR}/{sandbox_id}/fc.pid"));
        let spaced_host_pid =
            read_pid_from_symlink(&format!("{RUN_DIR}/{sandbox_id}/space-spaced-host-svc.pid"));
        let netgtw_pid =
            read_pid_from_symlink(&format!("{RUN_DIR}/{sandbox_id}/space-netgtw-svc.pid"));

        // Use request fields, fall back to tracker
        let (uid, slot, template, uuid8, pod_netns) = if !req.uuid8.is_empty() {
            let pod_netns = self
                .pool
                .get(sandbox_id)
                .map_err(slot_store_status)?
                .and_then(|info| info.pod_netns);
            (
                req.uid,
                req.slot,
                req.template.clone(),
                req.uuid8.clone(),
                pod_netns,
            )
        } else if let Some(info) = self.pool.get(sandbox_id).map_err(slot_store_status)? {
            (
                None,
                Some(info.slot),
                None,
                info.slot_label(),
                info.pod_netns.clone(),
            )
        } else {
            return Err(Status::not_found("sandbox not found"));
        };

        let ns = format!("sns-{uuid8}");
        let sveth = format!("sveth-{uuid8}");
        let vpeer = format!("vpeer-{uuid8}");
        let ns_up = net::ns_exists(&ns);

        // Cgroup (check both sbx- prefix and direct jailer name)
        let cgroup_path = format!("/sys/fs/cgroup/space/sbx-{sandbox_id}");
        let jailer_cgroup = format!("/sys/fs/cgroup/space/{sandbox_id}");
        let cgroup_exists = std::path::Path::new(&cgroup_path).exists()
            || std::path::Path::new(&jailer_cgroup).exists();
        let cg_base = if std::path::Path::new(&cgroup_path).exists() {
            &cgroup_path
        } else {
            &jailer_cgroup
        };

        let read_cgroup = |file: &str| -> String {
            std::fs::read_to_string(format!("{cg_base}/{file}"))
                .unwrap_or_default()
                .trim()
                .to_string()
        };

        let run_dir = format!("{RUN_DIR}/{sandbox_id}");
        // Inspection reports filesystem presence, not execution authority.
        // Probe both owned adapter roots even when metadata is damaged.
        let jail_roots = [
            pplx_space_common_core::types::RuntimeKind::Firecracker,
            pplx_space_common_core::types::RuntimeKind::CloudHypervisor,
        ]
        .into_iter()
        .map(|runtime| pplx_space_node_runtime_core::control::isolation_root(runtime, sandbox_id))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| Status::failed_precondition(error.to_string()))?;

        let slot_allocated = {
            self.pool
                .get(sandbox_id)
                .map_err(slot_store_status)?
                .is_some()
        };

        Ok(Response::new(InspectSandboxResponse {
            execution_fence: self
                .pool
                .execution_fence(sandbox_id)
                .map_err(slot_store_status)?
                .map(Into::into),
            sandbox_id: sandbox_id.to_string(),
            slot,
            uid,
            runtime_pid: fc_pid,
            runtime_alive: fc_pid.is_some_and(launch::is_pid_alive),
            spaced_host_pid,
            spaced_host_alive: spaced_host_pid.is_some_and(launch::is_pid_alive),
            netgtw_pid,
            netgtw_alive: netgtw_pid.is_some_and(launch::is_pid_alive),
            namespace_exists: ns_up,
            sveth_exists: net::link_exists_in(&sveth, pod_netns.as_deref()),
            vpeer_exists: ns_up && net::ns_link_exists(&ns, &vpeer),
            tap_exists: ns_up && net::ns_link_exists(&ns, "tap0"),
            host_route_exists: slot.is_some_and(|slot| {
                net::host_route_exists_in(
                    &NetId {
                        label: net::slot_label(slot),
                        slot,
                    },
                    pod_netns.as_deref(),
                )
            }),
            nft_forward_exists: crate::firewall::has_forward_rule_in(
                "sveth-*",
                pod_netns.as_deref(),
            ),
            nft_ns_snat_exists: ns_up && crate::firewall::ns_has_snat(&ns),
            nft_ns_dnat_exists: ns_up && crate::firewall::ns_has_dnat(&ns),
            ns_ip_forward: ns_up && net::ns_sysctl_check(&ns, "net.ipv4.ip_forward", "1"),
            ns_rp_filter_off: ns_up
                && net::ns_sysctl_check(&ns, "net.ipv4.conf.tap0.rp_filter", "0")
                && net::ns_sysctl_check(&ns, "net.ipv4.conf.all.rp_filter", "0"),
            cgroup_exists,
            cgroup_cpu_max: if cgroup_exists {
                read_cgroup("cpu.max")
            } else {
                String::new()
            },
            cgroup_memory_max: if cgroup_exists {
                read_cgroup("memory.max")
            } else {
                String::new()
            },
            cgroup_memory_current: if cgroup_exists {
                read_cgroup("memory.current")
            } else {
                String::new()
            },
            cgroup_pids_max: if cgroup_exists {
                read_cgroup("pids.max")
            } else {
                String::new()
            },
            cgroup_pids_current: if cgroup_exists {
                read_cgroup("pids.current")
            } else {
                String::new()
            },
            cgroup_cpu_stat: if cgroup_exists {
                read_cgroup("cpu.stat")
            } else {
                String::new()
            },
            cache_dir_exists: std::path::Path::new(&format!("{CACHE_DIR}/{sandbox_id}")).exists(),
            run_dir_exists: std::path::Path::new(&run_dir).exists(),
            isolation_root_exists: jail_roots.iter().any(|root| root.exists()),
            vm_config_exists: std::path::Path::new(&format!("{run_dir}/vm-config.json")).exists(),
            runtime_log_exists: jail_roots
                .iter()
                .any(|root| root.join("console.log").exists() || root.join("fc.log").exists())
                || std::path::Path::new(&format!("{run_dir}/fc.log")).exists(),
            vsock_sock_exists: jail_roots.iter().any(|root| root.join("v.sock").exists()),
            spaced_sock_exists: std::path::Path::new(&format!("{run_dir}/spaced-host.sock"))
                .exists(),
            rootfs_exists: std::path::Path::new(&format!(
                "{CACHE_DIR}/{sandbox_id}/instance/rootfs.ext4"
            ))
            .exists(),
            runtime_pid_link_exists: std::fs::symlink_metadata(format!(
                "{RUN_DIR}/{sandbox_id}/fc.pid"
            ))
            .is_ok(),
            spaced_host_pid_symlink_exists: std::fs::symlink_metadata(format!(
                "{RUN_DIR}/{sandbox_id}/space-spaced-host-svc.pid"
            ))
            .is_ok(),
            runtime_pidfile_exists: std::path::Path::new(&format!(
                "{JAIL_DIR}/firecracker/{sandbox_id}/root/firecracker.pid"
            ))
            .exists(),
            netgtw_pid_symlink_exists: std::fs::symlink_metadata(format!(
                "{RUN_DIR}/{sandbox_id}/space-netgtw-svc.pid"
            ))
            .is_ok(),
            nft_ns_redirect_exists: ns_up && crate::firewall::ns_has_tproxy(&ns),
            nft_ns_forward_drop_exists: ns_up && crate::firewall::ns_has_forward_drop(&ns),
            slot_allocated,
            template,
        }))
    }

    pub(super) async fn list_orphan_candidates(
        &self,
        request: Request<ListOrphanCandidatesRequest>,
    ) -> Result<Response<ListOrphanCandidatesResponse>, Status> {
        self.authorize_peer(&request, "ListOrphanCandidates", ALLOW_NODE_STATUS)?;
        let service = self.clone();
        self.off_runtime_probe("find orphan evidence", move || {
            service.list_orphan_candidates_blocking(request)
        })
        .await
    }

    fn list_orphan_candidates_blocking(
        &self,
        request: Request<ListOrphanCandidatesRequest>,
    ) -> Result<Response<ListOrphanCandidatesResponse>, Status> {
        // Re-read ownership on every page. Unavailable SQLite is never an
        // empty owner set, and no cursor is consumed on this failure.
        let records = self.pool.all_records().map_err(slot_store_status)?;
        self.orphan_inventory
            .page(request.into_inner(), &records)
            .map(Response::new)
    }
}

fn parse_cleanup_request(
    mut request: CleanupSandboxRequest,
) -> Result<(SandboxId, CleanupSandboxRequest), Status> {
    validate_sandbox_id(&request.sandbox_id)?;
    let sandbox_id = request.sandbox_id.parse::<SandboxId>().map_err(|error| {
        Status::invalid_argument(format!("invalid sandbox_id after validation: {error}"))
    })?;
    request.sandbox_id = sandbox_id.to_string();
    Ok((sandbox_id, request))
}

#[cfg(test)]
mod tests;
