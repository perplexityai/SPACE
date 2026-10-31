use super::cleanup_helpers::*;
use super::disk::*;
use super::network_helpers::*;
use super::validation::*;
use super::*;
use pplx_space_common_core::host::linux::paths;
use pplx_space_common_core::types::{LinuxPid, SandboxId};

pub(super) fn registered_firecracker_pid(
    run_dir: &Path,
    sandbox_id: &str,
) -> Result<LinuxPid, Status> {
    let vm_processes = crate::cgroup::vm_cgroup_process_ids(sandbox_id).map_err(|error| {
        Status::unavailable(format!(
            "sandbox runtime cgroup membership is unavailable: {error}"
        ))
    })?;
    registered_firecracker_pid_with_vm_processes(run_dir, sandbox_id, &vm_processes)
}

/// Called under the launch/cleanup gate before touching a jail. Missing process
/// registration is not proof of absence when the VM cgroup is still populated.
pub(super) fn firecracker_before_jail_rebuild(
    sandbox_id: &str,
) -> Result<Option<LinuxPid>, Status> {
    let processes = match crate::cgroup::vm_cgroup_process_ids(sandbox_id) {
        Ok(processes) => processes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            return Err(Status::unavailable(format!(
                "observe VM ownership: {error}"
            )));
        }
    };
    if !processes.is_empty() {
        let pid = registered_firecracker_pid_with_vm_processes(
            Path::new(RUN_DIR),
            sandbox_id,
            &processes,
        )
        .map_err(|error| {
            Status::unavailable(format!("live VM ownership is unresolved: {error}"))
        })?;
        // Process ownership, not control-socket readiness, prevents rebuilding
        // a live runtime's filesystem. The adapter checks readiness on adoption.
        return Ok(Some(pid));
    }
    let pid_path = Path::new(RUN_DIR).join(sandbox_id).join("fc.pid");
    match std::fs::read_link(&pid_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(Status::unavailable(format!(
            "observe runtime registration: {error}"
        ))),
        Ok(target) => {
            let pid = target
                .strip_prefix("/proc")
                .ok()
                .and_then(|tail| tail.to_str())
                .and_then(|value| value.parse::<u32>().ok())
                .ok_or_else(|| Status::unavailable("malformed runtime registration"))?;
            match crate::launch::ProcessHandle::open(pid) {
                Err(error) if error.is_not_found() => Ok(None),
                Err(error) => Err(Status::unavailable(format!(
                    "observe registered runtime: {error}"
                ))),
                Ok(_) => Err(Status::unavailable(
                    "registered process remains outside observed VM cgroup; cleanup required",
                )),
            }
        }
    }
}

pub(super) fn registered_firecracker_pid_with_vm_processes(
    run_dir: &Path,
    sandbox_id: &str,
    vm_processes: &[u32],
) -> Result<LinuxPid, Status> {
    let pid_path = run_dir.join(sandbox_id).join("fc.pid");
    let pid = read_pid_from_symlink(&pid_path.to_string_lossy())
        .and_then(|pid| LinuxPid::try_from_u32(pid).ok())
        .ok_or_else(|| Status::unavailable("sandbox runtime pid is not registered"))?;
    if !vm_processes.contains(&pid.get().get()) {
        return Err(Status::permission_denied(
            "registered runtime pid is not in the sandbox VM cgroup",
        ));
    }
    Ok(pid)
}
use pplx_space_events::{
    SandboxEvent, SandboxEventBoolField, SandboxEventDetails, SandboxEventSource, SandboxEventType,
};

fn emit_launch_failed(sandbox_id: SandboxId, program: &str, error: impl Into<String>) {
    SandboxEvent::new(
        sandbox_id,
        SandboxEventType::SandboxProcessLaunchFailed,
        SandboxEventSource::NodeManager,
    )
    .emit_with(
        SandboxEventDetails::default()
            .program(program)
            .error(error.into()),
    );
}

#[derive(Debug, thiserror::Error)]
pub(super) enum AllocateNewLeaseError {
    #[error(transparent)]
    ActiveReservations(#[from] ActiveLeaseMemoryReservationError),
    #[error(transparent)]
    MemoryAdmission(#[from] MemoryAdmissionEvaluationError),
    #[error(transparent)]
    Slot(#[from] AllocateSlotError),
}

impl NodeManagerService {
    pub(super) async fn allocate_new_lease(
        &self,
        sandbox_id: SandboxId,
        pod_netns: Option<&str>,
        sandbox_cgroup_memory_mib: u64,
    ) -> Result<SlotInfo, AllocateNewLeaseError> {
        let observed_active_leases = self.pool.active_memory_reservations()?;
        let observation = self
            .memory_admission_config
            .observe_active_leases(&observed_active_leases, &self.memory_admission_sources)
            .await
            .map_err(MemoryAdmissionEvaluationError::from)?;
        let allocation = {
            let _guard = self.memory_admission_lock.lock().await;
            let active_leases_at_commit = self.pool.active_memory_reservations()?;
            self.memory_admission_config
                .check_new_allocation_at_commit(
                    sandbox_cgroup_memory_mib,
                    &observation,
                    &active_leases_at_commit,
                )
                .map_err(MemoryAdmissionEvaluationError::from)?;
            self.pool.claim_allocation(
                &sandbox_id.to_string(),
                pod_netns,
                Some(NewLeaseMemoryReservation {
                    memory_reservation_mib: sandbox_cgroup_memory_mib,
                }),
            )?
        };
        self.pool
            .finish_allocation(allocation)
            .await
            .map_err(AllocateNewLeaseError::from)
    }

    pub(super) async fn allocate(
        &self,
        request: Request<AllocateRequest>,
    ) -> Result<Response<AllocateResponse>, Status> {
        self.authorize_peer(&request, "Allocate", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        validate_sandbox_id(&request.get_ref().sandbox_id)?;
        // Claim before admission can queue. Cleanup must not retire an
        // execution while an accepted allocation can still bind it later.
        let claim = self
            .wait_for_sandbox_work(&request.get_ref().sandbox_id)
            .await?;
        let service = self.clone();
        self.run_claimed_sandbox_work(claim, async move { service.allocate_inner(request).await })
            .await
    }

    async fn allocate_inner(
        &self,
        request: Request<AllocateRequest>,
    ) -> Result<Response<AllocateResponse>, Status> {
        let caller = super::execution_fence::pin_caller(
            &request,
            request.get_ref().execution_fence.is_some(),
        )?;
        let req = request.get_ref();
        let sandbox_id = &req.sandbox_id;
        validate_sandbox_id(sandbox_id)?;
        let typed_sandbox_id = sandbox_id.parse::<SandboxId>().map_err(|error| {
            Status::invalid_argument(format!("invalid sandbox_id after validation: {error}"))
        })?;
        let _lifecycle_permit = self
            .lifecycle_limiter
            .acquire(crate::lifecycle_limiter::LifecycleMethod::Allocate)
            .await?;

        // Validate the pod namespace if provided (CNI). We bridge to it but
        // always create our own sandbox namespace inside.
        let pod_netns =
            if let Some(netns_path) = req.netns_path.as_deref().filter(|path| !path.is_empty()) {
                let netns_name = std::path::Path::new(netns_path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| {
                        Status::invalid_argument(format!("invalid netns_path: {netns_path}"))
                    })?
                    .to_string();
                if !net::ns_exists(&netns_name) {
                    SandboxEvent::new(
                        typed_sandbox_id,
                        SandboxEventType::NetworkAllocationFailed,
                        SandboxEventSource::NodeManager,
                    )
                    .emit_with(
                        SandboxEventDetails::default()
                            .reason("pod_netns_not_found")
                            .bool_field(SandboxEventBoolField::HasNetnsPath, true)
                            .error(format!(
                                "pod namespace not found: {netns_name} (path: {netns_path})"
                            )),
                    );
                    return Err(Status::not_found(format!(
                        "pod namespace not found: {netns_name} (path: {netns_path})"
                    )));
                }
                Some(netns_name)
            } else {
                None
            };
        // Serialize same-sandbox allocation with launch/cleanup so an
        // idempotent concurrent Allocate observes an existing lease before
        // applying new-VM admission.
        let _cgroup_guard = self.lock_sandbox_cgroups(sandbox_id).await;
        super::execution_fence::check_caller(caller.as_ref())?;
        self.bind_execution_for_allocation(sandbox_id, req.execution_fence.as_ref())
            .await?;
        let allocation = if self
            .pool
            .get(sandbox_id)
            .map_err(slot_store_status)?
            .is_none()
        {
            let sandbox_cgroup_memory_mib = self
                .sandbox_cgroup_config
                .allocation_memory_mib(req.sandbox_cgroup_memory_mib)?;
            self.allocate_new_lease(
                typed_sandbox_id,
                pod_netns.as_deref(),
                sandbox_cgroup_memory_mib,
            )
            .await
        } else {
            self.pool
                .allocate(sandbox_id, pod_netns.as_deref(), None)
                .await
                .map_err(AllocateNewLeaseError::from)
        };
        let info = match allocation {
            Ok(info) => info,
            Err(AllocateNewLeaseError::ActiveReservations(error)) => {
                let status = Status::unavailable(format!(
                    "cannot observe active lease memory reservations for sandbox allocation: {error}"
                ));
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %error,
                    "rejecting new sandbox allocation because active lease memory reservations cannot be observed"
                );
                SandboxEvent::new(
                    typed_sandbox_id,
                    SandboxEventType::MemoryAdmissionFailed,
                    SandboxEventSource::NodeManager,
                )
                .emit_with(
                    SandboxEventDetails::default()
                        .reason("memory_admission_unavailable")
                        .error(status.message().to_string())
                        .logged_by_caller(),
                );
                return Err(status);
            }
            Err(AllocateNewLeaseError::MemoryAdmission(
                MemoryAdmissionEvaluationError::NodeMemory(error),
            )) => {
                let status = Status::unavailable(format!(
                    "cannot observe node memory capacity for sandbox allocation: {error}"
                ));
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %error,
                    "rejecting new sandbox allocation because host memory capacity cannot be observed"
                );
                SandboxEvent::new(
                    typed_sandbox_id,
                    SandboxEventType::MemoryAdmissionFailed,
                    SandboxEventSource::NodeManager,
                )
                .emit_with(
                    SandboxEventDetails::default()
                        .reason("memory_admission_unavailable")
                        .error(status.message().to_string())
                        .logged_by_caller(),
                );
                return Err(status);
            }
            Err(AllocateNewLeaseError::MemoryAdmission(
                MemoryAdmissionEvaluationError::Admission(error),
            )) => {
                match &error {
                    MemoryAdmissionError::InsufficientCapacity {
                        node_memory_total_mib,
                        node_used_mib,
                        existing_growth_reserve_mib,
                        sandbox_cgroup_memory_mib,
                        reserve_mib,
                        required_mib,
                    } => {
                        tracing::info!(
                            sandbox_id = %sandbox_id,
                            node_used_mib,
                            existing_growth_reserve_mib,
                            sandbox_cgroup_memory_mib,
                            reserve_mib,
                            required_mib,
                            node_memory_total_mib,
                            "rejecting new sandbox allocation because measured memory demand exceeds node capacity"
                        );
                    }
                    MemoryAdmissionError::RequirementOverflow {
                        node_used_mib,
                        existing_growth_reserve_mib,
                        sandbox_cgroup_memory_mib,
                        reserve_mib,
                    } => {
                        tracing::info!(
                            sandbox_id = %sandbox_id,
                            node_used_mib,
                            existing_growth_reserve_mib,
                            sandbox_cgroup_memory_mib,
                            reserve_mib,
                            "rejecting new sandbox allocation because measured memory demand overflows"
                        );
                    }
                }
                let details = SandboxEventDetails::default()
                    .reason("memory_admission_resource_exhausted")
                    .error(error.to_string());
                // A capacity refusal is normal operation. An overflow means the
                // reservation state is corrupt or saturated, so it stays a WARN.
                let details = match &error {
                    MemoryAdmissionError::InsufficientCapacity { .. } => details.routine(),
                    MemoryAdmissionError::RequirementOverflow { .. } => details,
                };
                SandboxEvent::new(
                    typed_sandbox_id,
                    SandboxEventType::MemoryAdmissionFailed,
                    SandboxEventSource::NodeManager,
                )
                .emit_with(details);
                return Err(Status::resource_exhausted(error.to_string()));
            }
            Err(AllocateNewLeaseError::Slot(error)) => {
                let status = allocate_error_to_status(error);
                let mut details = SandboxEventDetails::default()
                    .reason(status.code().to_string())
                    .bool_field(
                        SandboxEventBoolField::HasNetnsPath,
                        req.netns_path
                            .as_deref()
                            .is_some_and(|path| !path.is_empty()),
                    )
                    .bool_field(SandboxEventBoolField::HasPodNetns, pod_netns.is_some())
                    .error(status.message().to_string());
                match self.pool.counts() {
                    Ok(counts) => {
                        details = details
                            .network_slots_total(counts.total.to_string())
                            .network_slots_ready(counts.ready.to_string())
                            .network_slots_leased(counts.leased.to_string())
                            .network_slots_dirty(counts.dirty.to_string())
                            .network_slots_resetting(counts.resetting.to_string())
                            .network_slots_quarantined(counts.quarantined.to_string())
                            .network_slots_degraded(counts.degraded.to_string());
                    }
                    Err(error) => {
                        tracing::warn!(%error, "allocation failure diagnostics lack slot counts")
                    }
                }
                SandboxEvent::new(
                    typed_sandbox_id,
                    SandboxEventType::NetworkAllocationFailed,
                    SandboxEventSource::NodeManager,
                )
                .emit_with(details);
                return Err(status);
            }
        };
        let net = NetId::from_slot_info(&info);
        if info.pod_netns.is_none() {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                has_netns_path = req
                    .netns_path
                    .as_deref()
                    .is_some_and(|path| !path.is_empty()),
                has_pod_netns = false,
                slot = info.slot,
                slot_label = %info.slot_label(),
                sandbox_uid = info.sandbox_uid,
                vpeer_ip = %net.vpeer_ip_bare(),
                "allocated network slot without pod netns"
            );
        }
        SandboxEvent::new(
            typed_sandbox_id,
            SandboxEventType::NetworkAllocationCompleted,
            SandboxEventSource::NodeManager,
        )
        .emit_with(
            SandboxEventDetails::default()
                .bool_field(
                    SandboxEventBoolField::HasNetnsPath,
                    req.netns_path
                        .as_deref()
                        .is_some_and(|path| !path.is_empty()),
                )
                .bool_field(SandboxEventBoolField::HasPodNetns, info.pod_netns.is_some())
                .network_slot(info.slot.to_string())
                .network_slot_generation(info.generation.to_string())
                .network_slot_label(info.slot_label())
                .sandbox_uid(info.sandbox_uid.to_string()),
        );

        let ns_name = info.ns_name();

        let ns_path = info.ns_path();

        Ok(Response::new(AllocateResponse {
            slot: info.slot,
            namespace: ns_name,
            namespace_path: ns_path,
            guest_ip: GUEST_IP.to_string(),
            guest_gateway: GUEST_GATEWAY.to_string(),
            guest_mac: net.guest_mac(),
            tap_device: TAP_DEVICE.to_string(),
            slot_generation: info.generation,
            slot_label: info.slot_label(),
            sandbox_uid: info.sandbox_uid,
        }))
    }

    pub(super) async fn launch(
        &self,
        request: Request<LaunchRequest>,
    ) -> Result<Response<LaunchResponse>, Status> {
        self.authorize_peer(&request, "Launch", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let sandbox_id = request.get_ref().sandbox_id.clone();
        validate_sandbox_id(&sandbox_id)?;
        let service = self.clone();
        self.run_owned_sandbox_work(
            &sandbox_id,
            async move { service.launch_inner(request).await },
        )
        .await
    }

    async fn launch_inner(
        &self,
        request: Request<LaunchRequest>,
    ) -> Result<Response<LaunchResponse>, Status> {
        let req = request.get_ref();
        validate_sandbox_id(&req.sandbox_id)?;
        let typed_sandbox_id = req.sandbox_id.parse::<SandboxId>().map_err(|error| {
            Status::invalid_argument(format!("invalid sandbox_id after validation: {error}"))
        })?;
        let _lifecycle_permit = self
            .lifecycle_limiter
            .acquire(crate::lifecycle_limiter::LifecycleMethod::Launch)
            .await?;

        // Security: only allow launching known binaries.
        if !ALLOWED_LAUNCH_PROGRAMS.contains(&req.program.as_str()) {
            emit_launch_failed(
                typed_sandbox_id,
                &req.program,
                format!("program '{}' not in allowed list", req.program),
            );
            return Err(Status::permission_denied(format!(
                "program '{}' not in allowed list",
                req.program
            )));
        }
        let _cgroup_guard = self
            .lock_sandbox_sidecar_cgroup_launch(&req.sandbox_id)
            .await;
        self.check_execution(&req.sandbox_id, req.execution_fence.as_ref(), false)?;
        let lease = self
            .pool
            .get(&req.sandbox_id)
            .map_err(slot_store_status)?
            .ok_or_else(|| Status::not_found("sandbox not allocated"))
            .inspect_err(|error| {
                emit_launch_failed(typed_sandbox_id, &req.program, error.message());
            })?;
        let (ns_path, ns_name, vpeer, sandbox_uid, expected_spaced_host_peer) = {
            let info = &lease;
            let _process_uid = gateway_isolation::sidecar_launch_uid(
                &req.program,
                req.uid,
                req.gid,
                info.sandbox_uid,
                &req.sandbox_id,
            )
            .inspect_err(|error| {
                emit_launch_failed(typed_sandbox_id, &req.program, error.message());
            })?;
            let sandbox_uid = info.sandbox_uid;
            let net_id = NetId::from_slot_info(info);
            let expected_spaced_host_peer = (req.program
                == pplx_space_common_core::host::linux::paths::SPACED_HOST_SVC_BIN)
                .then(|| {
                    registered_firecracker_pid(Path::new(RUN_DIR), &req.sandbox_id)
                        .map(|pid| ExpectedSpacedHostPeer::new(pid, sandbox_uid))
                })
                .transpose()
                .inspect_err(|error| {
                    emit_launch_failed(typed_sandbox_id, &req.program, error.message());
                })?;
            (
                info.ns_path(),
                info.ns_name(),
                net_id.vpeer(),
                sandbox_uid,
                expected_spaced_host_peer,
            )
        };
        let expected_spaced_host_peer = match expected_spaced_host_peer {
            Some(peer) => {
                let runtime = self
                    .recorded_execution(req.sandbox_id.clone())
                    .await?
                    .map_or(
                        pplx_space_common_core::types::RuntimeKind::Firecracker,
                        |e| e.runtime().kind,
                    );
                Some(peer.for_runtime(runtime))
            }
            None => None,
        };
        let mut launch_args = prepare_sidecar_launch_args(
            &req.program,
            &req.sandbox_id,
            &req.args,
            expected_spaced_host_peer,
        )
        .inspect_err(|error| {
            emit_launch_failed(typed_sandbox_id, &req.program, error.message());
        })?;

        let process_uid = req.uid;
        let is_gateway = req.program == paths::NET_GATEWAY_SVC_BIN;
        let identity_sandbox_id = req.sandbox_id.clone();
        let identity_program = req.program.clone();
        let process_uid = self
            .off_runtime("resolve sidecar identity", move || {
                gateway_isolation::resolve_sidecar_launch_uid(
                    &identity_sandbox_id,
                    &identity_program,
                    process_uid,
                    process_uid,
                    sandbox_uid,
                )
            })
            .await
            .inspect_err(|error| {
                emit_launch_failed(typed_sandbox_id, &req.program, error.message());
            })?;
        let isolated_gateway = is_gateway && process_uid != sandbox_uid;
        if isolated_gateway {
            let socket_index = launch_args
                .iter()
                .position(|arg| arg == "--socket")
                .expect("validated gateway socket flag");
            *launch_args
                .get_mut(socket_index + 1)
                .expect("validated gateway socket value") =
                paths::isolated_net_gateway_control_socket(&req.sandbox_id);
        }
        let gateway_sandbox_id = req.sandbox_id.clone();
        let run_dir = format!("{RUN_DIR}/{}", req.sandbox_id);
        self.adopt_validated_lease(&req.sandbox_id, &lease)
            .inspect_err(|error| {
                emit_launch_failed(typed_sandbox_id, &req.program, error.message());
            })?;
        spawn_lifecycle_blocking(&_lifecycle_permit, move || {
            std::fs::create_dir_all(&run_dir)?;
            chown_path_blocking(&run_dir, sandbox_uid, sandbox_uid)?;
            ensure_sidecar_run_dir_permissions(&run_dir)?;
            if is_gateway {
                ensure_sandbox_artifact_dir_permissions(paths::sandbox_cache_dir(
                    &gateway_sandbox_id,
                ))?;
                gateway_isolation::prepare_gateway_socket(
                    &gateway_sandbox_id,
                    process_uid,
                    isolated_gateway,
                )?;
            }
            Ok::<(), Status>(())
        })
        .await
        .map_err(|error| {
            let status = Status::internal(format!("create sidecar run dir task failed: {error}"));
            emit_launch_failed(typed_sandbox_id, &req.program, status.message());
            status
        })?
        .map_err(|error| {
            let status = Status::internal(format!("create sidecar run dir failed: {error}"));
            emit_launch_failed(typed_sandbox_id, &req.program, status.message());
            status
        })?;

        // Set up TPROXY rules if launching net-gateway (needs UID for anti-loop rule).
        if req.program == pplx_space_common_core::host::linux::paths::NET_GATEWAY_SVC_BIN {
            let details = || {
                SandboxEventDetails::default()
                    .operation("tproxy_setup")
                    .program(req.program.clone())
                    .netns(ns_name.clone())
                    .sandbox_uid(sandbox_uid.to_string())
            };
            SandboxEvent::new(
                typed_sandbox_id,
                SandboxEventType::NetworkPolicyEnforcementStarted,
                SandboxEventSource::NodeManager,
            )
            .emit_with(details());
            let dhcp = self.recorded_guest_network(req.sandbox_id.clone()).await?
                == pplx_space_common_core::types::guest_image::GuestNetwork::SlotDhcpV1;
            net::setup_tproxy(&ns_name, process_uid, &vpeer, dhcp).map_err(|e| {
                let status = Status::internal(format!("setup tproxy failed: {e}"));
                SandboxEvent::new(
                    typed_sandbox_id,
                    SandboxEventType::NetworkPolicyEnforcementFailed,
                    SandboxEventSource::NodeManager,
                )
                .emit_with(details().error(status.message().to_string()));
                emit_launch_failed(typed_sandbox_id, &req.program, status.message());
                status
            })?;
            SandboxEvent::new(
                typed_sandbox_id,
                SandboxEventType::NetworkPolicyEnforcementCompleted,
                SandboxEventSource::NodeManager,
            )
            .emit_with(details().result("tproxy_ready"));
        }

        // Build pre-exec sockets for net-gateway (TPROXY needs IP_TRANSPARENT as root)
        let pre_exec_sockets = if req.program
            == pplx_space_common_core::host::linux::paths::NET_GATEWAY_SVC_BIN
        {
            vec![
                launch::PreExecSocket {
                    bind_addr: format!("0.0.0.0:{}", crate::firewall::NETGTW_TCP_PORT)
                        .parse()
                        .map_err(|error| {
                            let status =
                                Status::internal(format!("invalid net-gateway TCP bind: {error}"));
                            emit_launch_failed(typed_sandbox_id, &req.program, status.message());
                            status
                        })?,
                    tcp: true,
                    transparent: true,
                    recvorigdstaddr: false,
                    fd_arg: "--tcp-fd".to_string(),
                },
                launch::PreExecSocket {
                    bind_addr: format!("0.0.0.0:{}", crate::firewall::NETGTW_UDP_PORT)
                        .parse()
                        .map_err(|error| {
                            let status =
                                Status::internal(format!("invalid net-gateway UDP bind: {error}"));
                            emit_launch_failed(typed_sandbox_id, &req.program, status.message());
                            status
                        })?,
                    tcp: false,
                    transparent: true,
                    recvorigdstaddr: true,
                    fd_arg: "--udp-fd".to_string(),
                },
            ]
        } else {
            vec![]
        };

        let grant_cap =
            req.program == pplx_space_common_core::host::linux::paths::NET_GATEWAY_SVC_BIN;
        let pid = self
            .runtime_supervisor
            .launch_sidecar(pplx_space_node_runtime_core::proto::LaunchSidecarRequest {
                execution_id: req
                    .execution_fence
                    .as_ref()
                    .map_or_else(String::new, |fence| fence.execution_id.clone()),
                sandbox_id: req.sandbox_id.clone(),
                ns_path: ns_path.clone(),
                program: req.program.clone(),
                args: launch_args,
                env: req.env.clone(),
                uid: process_uid,
                gid: process_uid,
                pre_exec_sockets: pre_exec_sockets
                    .iter()
                    .map(
                        |socket| pplx_space_node_runtime_core::proto::PreExecSocket {
                            bind_addr: socket.bind_addr.to_string(),
                            tcp: socket.tcp,
                            transparent: socket.transparent,
                            recvorigdstaddr: socket.recvorigdstaddr,
                            fd_arg: socket.fd_arg.clone(),
                        },
                    )
                    .collect(),
                grant_cap_net_admin: grant_cap,
            })
            .await
            .map_err(|error| {
                let status = Status::internal(format!("runtime-supervisor launch failed: {error}"));
                emit_launch_failed(typed_sandbox_id, &req.program, status.message());
                status
            })?
            .into_inner()
            .pid;

        if let Err(error) = self.pool.set_pid(&req.sandbox_id, pid) {
            tracing::error!(
                sandbox_id = %req.sandbox_id,
                pid,
                error = %error,
                "failed to persist launched process pid"
            );
            SandboxEvent::new(
                typed_sandbox_id,
                SandboxEventType::SandboxProcessPidPersistFailed,
                SandboxEventSource::NodeManager,
            )
            .emit_with(
                SandboxEventDetails::default()
                    .pid(pid.to_string())
                    .program(req.program.clone())
                    .error(error.to_string())
                    .logged_by_caller(),
            );
            kill_launched_process_after_pid_persist_failure(self, &req.sandbox_id, pid).await;
            return Err(Status::internal(format!(
                "failed to persist launched process pid: {error}"
            )));
        }

        if req.program == pplx_space_common_core::host::linux::paths::SPACED_HOST_SVC_BIN {
            if let Some(socket_path) = req.args.windows(2).find_map(|args| {
                (args.first()? == "--socket")
                    .then(|| args.get(1).map(String::as_str))
                    .flatten()
            }) {
                wait_for_unix_socket_accepting(socket_path, std::time::Duration::from_secs(1))
                    .await
                    .inspect_err(|error| {
                        emit_launch_failed(typed_sandbox_id, &req.program, error.message());
                    })?;
            } else {
                emit_launch_failed(
                    typed_sandbox_id,
                    &req.program,
                    "space-spaced-host-svc launch requires --socket <path>",
                );
                return Err(Status::invalid_argument(
                    "space-spaced-host-svc launch requires --socket <path>",
                ));
            }
        }

        SandboxEvent::new(
            typed_sandbox_id,
            SandboxEventType::SandboxProcessLaunchCompleted,
            SandboxEventSource::NodeManager,
        )
        .emit_with(
            SandboxEventDetails::default()
                .pid(pid.to_string())
                .program(req.program.clone())
                .sandbox_uid(sandbox_uid.to_string()),
        );

        Ok(Response::new(LaunchResponse { pid }))
    }

    pub(super) async fn release(
        &self,
        request: Request<ReleaseRequest>,
    ) -> Result<Response<ReleaseResponse>, Status> {
        self.authorize_peer(&request, "Release", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        validate_sandbox_id(&request.get_ref().sandbox_id)?;
        let sandbox_id = request.get_ref().sandbox_id.clone();
        let service = self.clone();
        self.run_owned_sandbox_mutation(
            &sandbox_id,
            async move { service.release_inner(request).await },
        )
        .await
    }

    async fn release_inner(
        &self,
        request: Request<ReleaseRequest>,
    ) -> Result<Response<ReleaseResponse>, Status> {
        let req = request.get_ref();
        let sandbox_id = &req.sandbox_id;
        validate_sandbox_id(sandbox_id)?;
        let typed_sandbox_id = sandbox_id.parse::<SandboxId>().map_err(|error| {
            Status::invalid_argument(format!("invalid sandbox_id after validation: {error}"))
        })?;
        let _lifecycle_permit = self
            .lifecycle_limiter
            .acquire(crate::lifecycle_limiter::LifecycleMethod::Release)
            .await?;
        let _cgroup_guard = self.lock_sandbox_cgroups(sandbox_id).await;

        self.check_execution(sandbox_id, req.execution_fence.as_ref(), false)?;

        let current_info = self.pool.get(sandbox_id).map_err(slot_store_status)?;
        if let Some(info) = current_info.as_ref() {
            if let Some(pid) = info.pid
                && launch::is_pid_alive(pid)
                && !req.force
            {
                return Err(Status::failed_precondition(format!(
                    "sandbox has a running process (PID {pid}); stop it first or use --force"
                )));
            }
            self.adopt_stopping_lease(sandbox_id, info)?;
            // Only confirmed completion permits releasing the reservation.
            // Any RPC error can leave either a live runtime or owned stop work.
            stop_sandbox_runtime_with_supervisor(self, sandbox_id, false, None).await?;
        }

        self.stop_guest_dhcp(sandbox_id).await?;

        let info = self.pool.release(sandbox_id).map_err(|error| {
            SandboxEvent::new(
                typed_sandbox_id,
                SandboxEventType::NetworkReleaseFailed,
                SandboxEventSource::NodeManager,
            )
            .emit_with(SandboxEventDetails::default().error(error.to_string()));
            Status::internal(format!("release network slot failed: {error}"))
        })?;

        match info {
            Some(info) => {
                let net = NetId::from_slot_info(&info);
                let vpeer_ip = net.vpeer_ip_bare();
                let dnat_cleanup_details = || {
                    SandboxEventDetails::default()
                        .operation("ingress_dnat_cleanup")
                        .protocol("all")
                        .host_port("all")
                        .netns(info.pod_netns.clone().unwrap_or_else(|| "root".to_string()))
                        .vpeer_ip(vpeer_ip.clone())
                        .bool_field(SandboxEventBoolField::HasPodNetns, info.pod_netns.is_some())
                };
                SandboxEvent::new(
                    typed_sandbox_id,
                    SandboxEventType::PortExposureRemoveStarted,
                    SandboxEventSource::NodeManager,
                )
                .emit_with(dnat_cleanup_details());
                match crate::firewall::remove_ingress_dnat_rules(
                    info.pod_netns.as_deref(),
                    crate::firewall::PortSelector::All,
                    "",
                    &vpeer_ip,
                ) {
                    Ok(_) => {
                        SandboxEvent::new(
                            typed_sandbox_id,
                            SandboxEventType::PortExposureRemoveCompleted,
                            SandboxEventSource::NodeManager,
                        )
                        .emit_with(dnat_cleanup_details().result("removed"));
                    }
                    Err(error) => {
                        tracing::warn!(
                            namespace = info.pod_netns.as_deref().unwrap_or("root"),
                            %vpeer_ip,
                            %error,
                            cleanup_phase = "release",
                            "failed to clean ingress DNAT rules"
                        );
                        SandboxEvent::new(
                            typed_sandbox_id,
                            SandboxEventType::PortExposureRemoveFailed,
                            SandboxEventSource::NodeManager,
                        )
                        .emit_with(
                            dnat_cleanup_details()
                                .error(error.to_string())
                                .logged_by_caller(),
                        );
                    }
                }
                SandboxEvent::new(
                    typed_sandbox_id,
                    SandboxEventType::NetworkReleaseCompleted,
                    SandboxEventSource::NodeManager,
                )
                .emit_with(
                    SandboxEventDetails::default()
                        .bool_field(SandboxEventBoolField::Force, req.force)
                        .bool_field(SandboxEventBoolField::HasPodNetns, info.pod_netns.is_some())
                        .network_slot(info.slot.to_string())
                        .network_slot_generation(info.generation.to_string())
                        .network_slot_label(info.slot_label())
                        .sandbox_uid(info.sandbox_uid.to_string()),
                );
                Ok(Response::new(ReleaseResponse {}))
            }
            None => Err(Status::not_found(format!(
                "no allocation matching '{sandbox_id}'"
            ))),
        }
    }

    pub(super) async fn get_node_resource_status(
        &self,
        request: Request<NodeResourceStatusRequest>,
    ) -> Result<Response<NodeResourceStatusResponse>, Status> {
        self.authorize_peer(&request, "GetNodeResourceStatus", ALLOW_NODE_STATUS)?;
        Ok(Response::new(NodeResourceStatusResponse {
            cache_disk: Some(self.disk_admission.cache_disk_status()),
        }))
    }

    pub(super) async fn get_network_pool_status(
        &self,
        request: Request<NetworkPoolStatusRequest>,
    ) -> Result<Response<NetworkPoolStatusResponse>, Status> {
        self.authorize_peer(&request, "GetNetworkPoolStatus", ALLOW_NODE_STATUS)?;
        let counts = self.pool.counts().map_err(slot_store_status)?;
        let draining = self.pool.is_draining().map_err(slot_store_status)?;
        let config = self.pool.config();
        Ok(Response::new(network_pool_status_response(
            &counts,
            draining,
            &config,
            self.sandbox_cgroup_config,
        )))
    }

    pub(super) async fn list_network_leases(
        &self,
        request: Request<ListNetworkLeasesRequest>,
    ) -> Result<Response<ListNetworkLeasesResponse>, Status> {
        self.authorize_peer(&request, "ListNetworkLeases", ALLOW_NODE_STATUS)?;
        let leases = self
            .pool
            .all_records()
            .map_err(slot_store_status)?
            .iter()
            .filter_map(network_lease_from_record)
            .collect();
        Ok(Response::new(ListNetworkLeasesResponse { leases }))
    }

    pub(super) async fn set_network_pool_drain(
        &self,
        request: Request<SetNetworkPoolDrainRequest>,
    ) -> Result<Response<SetNetworkPoolDrainResponse>, Status> {
        self.authorize_peer(&request, "SetNetworkPoolDrain", ALLOW_NODE_ADMIN)?;
        let draining = request.get_ref().draining;
        self.pool
            .set_draining(draining)
            .map_err(|error| Status::internal(format!("failed to update pool drain: {error}")))?;
        Ok(Response::new(SetNetworkPoolDrainResponse { draining }))
    }

    pub(super) async fn repair_network_pool(
        &self,
        request: Request<RepairNetworkPoolRequest>,
    ) -> Result<Response<RepairNetworkPoolResponse>, Status> {
        self.authorize_peer(&request, "RepairNetworkPool", ALLOW_NODE_ADMIN)?;
        #[cfg(target_os = "linux")]
        {
            if self.pool.config().service_port_ingress_isolation
                == crate::slot_pool::ServicePortIngressIsolation::SharedHost
            {
                let removed =
                    crate::firewall::remove_root_ingress_dnat_rules().map_err(|error| {
                        Status::internal(format!(
                            "remove legacy root namespace service-port DNAT rules failed: {error}"
                        ))
                    })?;
                if removed > 0 {
                    tracing::warn!(
                        removed,
                        "removed legacy root namespace service-port DNAT rules during network pool repair"
                    );
                }
            }
        }
        let result = self
            .pool
            .repair(request.get_ref().rebuild_quarantined)
            .map_err(|error| Status::internal(format!("network pool repair failed: {error}")))?;
        Ok(Response::new(RepairNetworkPoolResponse {
            reset_enqueued: result.reset_enqueued,
            topup_requested: result.topup_requested,
        }))
    }

    /// Raise loop-device capacity to cover a requested slot budget, if the
    /// request raises it at all. No-op on every other update.
    #[cfg(target_os = "linux")]
    async fn ensure_loop_capacity_for_slot_increase(
        &self,
        candidate_max_slots: u32,
    ) -> Result<(), Status> {
        use crate::loop_devices;

        let current = self.pool.config().max_slots;
        let target = loop_devices::target_for_slot_increase(
            loop_devices::override_from_env().as_deref(),
            current,
            Some(candidate_max_slots),
        )
        .map_err(|error| {
            Status::invalid_argument(format!(
                "{} is malformed, so the slot budget cannot be raised: {error}",
                loop_devices::LOOP_DEVICE_TARGET_ENV
            ))
        })?;
        let Some(target) = target else {
            return Ok(());
        };

        // The ioctls are blocking and a large increase is thousands of them.
        let report = self
            .device_op_tasks
            .spawn_blocking(move || loop_devices::ensure_capacity(target))
            .await
            .map_err(|error| {
                Status::internal(format!("loop-device capacity task failed to run: {error}"))
            })?
            .map_err(|error| {
                Status::failed_precondition(format!(
                    "refusing to raise the slot budget to {candidate_max_slots}: the node cannot \
                     be given the {target} loop devices it would need: {error}"
                ))
            })?;
        tracing::info!(
            device_target = %target,
            present = report.present(),
            kernel_devices_added = report.kernel_devices_added,
            device_nodes_created = report.device_nodes_created,
            previous_max_slots = current,
            candidate_max_slots,
            "ensured loop-device capacity for a live slot-budget increase"
        );
        Ok(())
    }

    pub(super) async fn set_network_pool_config(
        &self,
        request: Request<SetNetworkPoolConfigRequest>,
    ) -> Result<Response<SetNetworkPoolConfigResponse>, Status> {
        self.authorize_peer(&request, "SetNetworkPoolConfig", ALLOW_NODE_ADMIN)?;
        let req = request.into_inner();
        let _update = self.pool.config_update_lock.lock().await;

        let update = NetworkPoolConfigUpdate {
            max_slots: req.max_slots,
            target_ready_slots: req.target_ready_slots,
            retained_ready_slots: req.retained_ready_slots,
            min_ready_slots: req.min_ready_slots,
        };

        // Validate before doing anything on the request's behalf. Ensuring
        // capacity creates kernel loop devices and /dev nodes and never removes
        // them, so an update that is going to be rejected must be rejected
        // first — otherwise a malformed admin call leaves durable host state
        // behind, repeatably.
        #[cfg(target_os = "linux")]
        {
            let candidate = self
                .pool
                .candidate_config(&update)
                .map_err(Status::failed_precondition)?;

            // The node's loop-device capacity is sized from its slot budget, so a
            // budget raised at runtime has to raise the capacity too, and it has to
            // happen before the new budget is published: once it is, the scheduler
            // fills the extra slots, and a device-mapper-backed rootfs that cannot
            // get a loop device fails in `losetup` on a node that looks correctly
            // configured.
            //
            // Failing the update is the safe outcome, unlike failing startup — it
            // leaves the node on the budget it can already back rather than taking
            // away capacity it has.
            self.ensure_loop_capacity_for_slot_increase(candidate.max_slots)
                .await?;
        }

        let config = self
            .pool
            .update_config(update)
            .map_err(Status::failed_precondition)?;
        Ok(Response::new(SetNetworkPoolConfigResponse {
            config: Some(pool_config_info(config)),
        }))
    }

    pub(super) async fn stop_process(
        &self,
        request: Request<StopProcessRequest>,
    ) -> Result<Response<StopProcessResponse>, Status> {
        self.authorize_peer(&request, "StopProcess", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        validate_sandbox_id(&request.get_ref().sandbox_id)?;
        let sandbox_id = request.get_ref().sandbox_id.clone();
        let service = self.clone();
        self.run_owned_sandbox_work(&sandbox_id, async move {
            service.stop_process_inner(request).await
        })
        .await
    }

    async fn stop_process_inner(
        &self,
        request: Request<StopProcessRequest>,
    ) -> Result<Response<StopProcessResponse>, Status> {
        let req = request.get_ref();
        let sandbox_id = &req.sandbox_id;
        validate_sandbox_id(sandbox_id)?;
        let signal = req.signal as i32;
        let _gate = self
            .sandbox_cgroup_gate_lock(sandbox_id)
            .await
            .read_owned()
            .await;
        self.check_execution(sandbox_id, req.execution_fence.as_ref(), false)?;

        // Validate signal (only SIGTERM and SIGKILL allowed)
        if signal != libc::SIGTERM && signal != libc::SIGKILL {
            return Err(Status::invalid_argument(format!(
                "only SIGTERM ({}) and SIGKILL ({}) are allowed, got {signal}",
                libc::SIGTERM,
                libc::SIGKILL,
            )));
        }

        // Resolve the target PID: an explicit pid from the caller (needed to
        // address a specific sidecar — the pool only tracks the last-launched
        // one), else the pool's tracked pid. Sandbox ownership of an explicit
        // pid is enforced by the runtime-supervisor via cgroup membership.
        let pid = {
            let info = self
                .pool
                .get(sandbox_id)
                .map_err(slot_store_status)?
                .ok_or_else(|| Status::not_found("sandbox not allocated"))?;
            match req.pid {
                Some(pid) => pid,
                None => info.pid.ok_or_else(|| {
                    Status::failed_precondition("sandbox has no launched process")
                })?,
            }
        };
        launch::validate_process_pid(pid).map_err(Status::failed_precondition)?;
        let process = match launch::ProcessHandle::open(pid) {
            Ok(process) => process,
            Err(error) if error.is_not_found() => {
                return Ok(Response::new(StopProcessResponse { success: false }));
            }
            Err(error) => return Err(Status::unavailable(format!("open process handle: {error}"))),
        };
        match process.sandbox_ownership(sandbox_id).map_err(|error| {
            Status::unavailable(format!("process ownership unavailable: {error}"))
        })? {
            launch::ProcessOwnership::Gone => {
                return Ok(Response::new(StopProcessResponse { success: false }));
            }
            launch::ProcessOwnership::Foreign => {
                return Err(Status::permission_denied(
                    "process does not belong to sandbox",
                ));
            }
            launch::ProcessOwnership::Owned => {}
        }

        // Stopping one process does not take over the sandbox lifecycle. The
        // temporary mutation claim excludes recovery during the RPC, but leaves
        // startup leasing promotion/reclamation owned by its re-probe worker.

        let result = self
            .runtime_supervisor
            .stop_process(pplx_space_node_runtime_core::proto::StopProcessRequest {
                execution_id: req
                    .execution_fence
                    .as_ref()
                    .map_or_else(String::new, |fence| fence.execution_id.clone()),
                sandbox_id: sandbox_id.clone(),
                pid,
                signal: req.signal,
            })
            .await;
        let success = match result {
            Ok(response) => response.into_inner().success,
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::PermissionDenied | tonic::Code::FailedPrecondition
                ) && matches!(
                    process.sandbox_ownership(sandbox_id),
                    Ok(launch::ProcessOwnership::Gone)
                ) =>
            {
                // The original process can exit between the two daemons' pidfd
                // opens, including when an older supervisor conflates missing
                // /proc metadata with foreign ownership. Never use a numeric-PID
                // liveness check here: it may already refer to another process.
                false
            }
            Err(error) => {
                return Err(Status::new(
                    error.code(),
                    format!("stop process failed: {error}"),
                ));
            }
        };
        Ok(Response::new(StopProcessResponse { success }))
    }
}

// Every case pins processes through Linux pidfds or reads /proc.
#[cfg(test)]
#[cfg(target_os = "linux")]
pub(super) mod tests;
