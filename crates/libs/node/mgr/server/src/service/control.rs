use super::cleanup_helpers::*;
use super::network_helpers::*;
use super::validation::*;
use super::*;
use pplx_space_events::{
    SandboxEventBoolField, SandboxEventDetails, SandboxEventSource, SandboxEventType,
    emit_sandbox_event,
};

pub(super) fn dispose_prepared_clone_resources(
    cache: &Path,
    run: &Path,
    req: &crate::proto::CleanupPreparedCloneRequest,
) -> Result<(), Status> {
    crate::clone_cleanup::dispose(
        cache,
        run,
        &req.sandbox_id,
        req.prepared_destination.as_ref(),
    )
    .map_err(clone_disposal_status)
}

pub(super) fn clone_disposal_status(error: crate::clone_cleanup::CloneDisposalError) -> Status {
    use crate::clone_cleanup::CloneDisposalError;
    match error {
        CloneDisposalError::RootIdentity
        | CloneDisposalError::SharedRoot
        | CloneDisposalError::UnexpectedRuntime => Status::failed_precondition(error.to_string()),
        #[cfg(target_os = "linux")]
        CloneDisposalError::FilesystemRoot => Status::failed_precondition(error.to_string()),
        CloneDisposalError::BudgetExhausted => Status::unavailable(error.to_string()),
        CloneDisposalError::Io(source) => {
            tracing::warn!(error = %source, "prepared clone disposal filesystem failure; retry permitted");
            Status::unavailable("prepared clone disposal filesystem operation failed")
        }
    }
}

fn allow_port_request_params(req: &AllowPortRequest) -> String {
    let protocol = NetworkProtocol::try_from(req.protocol)
        .map(|protocol| protocol.as_str_name().to_ascii_lowercase())
        .unwrap_or_else(|_| req.protocol.to_string());
    pplx_space_common_svc::format_grpc_request_params([
        ("sandbox_id", req.sandbox_id.clone()),
        ("port", req.port.to_string()),
        ("protocol", protocol),
    ])
}

impl NodeManagerService {
    pub(super) async fn sync_guest(
        &self,
        request: Request<SyncGuestRequest>,
    ) -> Result<Response<SyncGuestResponse>, Status> {
        self.authorize_peer(&request, "SyncGuest", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let service = self.clone();
        self.run_owned_filesystem_work(async move { service.sync_guest_inner(request).await })
            .await
    }

    async fn sync_guest_inner(
        &self,
        request: Request<SyncGuestRequest>,
    ) -> Result<Response<SyncGuestResponse>, Status> {
        let sandbox_id = &request.get_ref().sandbox_id;
        validate_sandbox_id(sandbox_id)?;

        // With bind-mount, FC writes go through the host kernel page cache to
        // the instance subvolume. This flushes host-side dirty pages after the
        // guest filesystem has already been asked to write back its own cache.
        // `sync --file-system` forces a full btrfs transaction commit and can
        // take tens of seconds under commit contention, so wait on it
        // asynchronously instead of parking a runtime worker.
        let instance_dir = format!("{CACHE_DIR}/{sandbox_id}/instance");
        let status = self
            .off_runtime("sync guest", move || {
                crate::host_command::output(
                    std::process::Command::new("sync").args(["--file-system", &instance_dir]),
                    crate::host_command::MUTATION_BUDGET,
                    &[],
                )
                .map(|output| output.status)
                .map_err(|e| Status::internal(format!("sync failed: {e}")))
            })
            .await?;

        if !status.success() {
            return Err(Status::internal("sync --file-system failed"));
        }

        Ok(Response::new(SyncGuestResponse {}))
    }

    pub(super) async fn update_cgroup(
        &self,
        request: Request<UpdateCgroupRequest>,
    ) -> Result<Response<UpdateCgroupResponse>, Status> {
        self.authorize_peer(&request, "UpdateCgroup", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let req = request.get_ref();
        validate_sandbox_id(&req.sandbox_id)?;
        let _cgroup_guard = self.lock_sandbox_cgroups(&req.sandbox_id).await;
        self.check_execution(&req.sandbox_id, req.execution_fence.as_ref(), false)?;
        self.runtime_supervisor
            .update_cgroup(pplx_space_node_runtime_core::proto::UpdateCgroupRequest {
                execution_id: req
                    .execution_fence
                    .as_ref()
                    .map_or_else(String::new, |fence| fence.execution_id.clone()),
                sandbox_id: req.sandbox_id.clone(),
                cgroup_limits: Some(pplx_space_node_runtime_core::proto::CgroupLimits {
                    vcpu_count: req.vcpu_count,
                    memory_mib: self
                        .sandbox_cgroup_config
                        .update_memory_mib(req.memory_mib.map(u64::from)),
                    pids_max: req.pids_max,
                }),
            })
            .await
            .map_err(|error| {
                Status::new(
                    error.code(),
                    format!(
                        "runtime-supervisor update cgroup failed: {}",
                        error.message()
                    ),
                )
            })?;

        Ok(Response::new(UpdateCgroupResponse {}))
    }

    pub(super) async fn clone_sandbox_into_prepared_root(
        &self,
        request: Request<CloneSandboxRequest>,
    ) -> Result<Response<CloneSandboxResponse>, Status> {
        self.authorize_peer(
            &request,
            "CloneSandboxIntoPreparedRoot",
            ALLOW_NODE_ADMIN_OR_SBXMGR,
        )?;
        let req = request.into_inner();
        validate_sandbox_id(&req.donor_sandbox_id)?;
        validate_sandbox_id(&req.new_sandbox_id)?;
        if req.donor_sandbox_id == req.new_sandbox_id {
            return Err(Status::invalid_argument(
                "clone destination must differ from donor",
            ));
        }
        let expected = req
            .prepared_destination
            .ok_or_else(|| Status::invalid_argument("prepared destination identity is required"))?;
        let donor = req.donor_sandbox_id.clone();
        let target = req.new_sandbox_id.clone();
        self.run_owned_clone(&donor, &target, move || {
            let tree = crate::clone_tree::CloneTree::open(
                Path::new(CACHE_DIR), Path::new(RUN_DIR),
                &req.donor_sandbox_id, &req.new_sandbox_id, &expected,
            ).map_err(|error| Status::failed_precondition(format!("open clone tree: {error}")))?;
            tree.copy(gids::SPACE_CACHE_RW, gids::SPACE_SIDECARCLIENTS)
                .map_err(|error| Status::internal(format!("clone tree: {error}")))?;
            tracing::info!(donor_sandbox_id = %req.donor_sandbox_id, sandbox_id = %req.new_sandbox_id, "cloned sandbox");
            Ok(())
        }).await?;
        Ok(Response::new(CloneSandboxResponse {}))
    }

    pub(super) async fn cleanup_prepared_clone(
        &self,
        request: Request<crate::proto::CleanupPreparedCloneRequest>,
    ) -> Result<Response<CleanupSandboxResponse>, Status> {
        self.authorize_peer(&request, "CleanupPreparedClone", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let req = request.into_inner();
        validate_sandbox_id(&req.sandbox_id)?;
        let permit = self
            .lifecycle_limiter
            .acquire(crate::lifecycle_limiter::LifecycleMethod::CleanupSandbox)
            .await?;
        let gate = tokio::time::timeout(
            Duration::from_secs(10),
            self.lock_sandbox_cgroups(&req.sandbox_id),
        )
        .await
        .map_err(|_| Status::unavailable("clone disposal ownership is busy"))?;
        if self
            .pool
            .get(&req.sandbox_id)
            .map_err(slot_store_status)?
            .is_some()
        {
            return Err(Status::failed_precondition(
                "prepared clone acquired a runtime; disposal requires operator repair",
            ));
        }
        if self
            .pool
            .execution_fence(&req.sandbox_id)
            .map_err(slot_store_status)?
            .is_some()
        {
            return Err(Status::failed_precondition(
                "prepared clone belongs to an execution; use lifecycle cleanup",
            ));
        }
        let task = self.off_runtime("clone disposal", move || {
            let _ownership = (permit, gate);
            dispose_prepared_clone_resources(Path::new(CACHE_DIR), Path::new(RUN_DIR), &req)
        });
        tokio::time::timeout(Duration::from_secs(65), task)
            .await
            .map_err(|_| {
                Status::deadline_exceeded("clone disposal continues under lifecycle ownership")
            })??;
        Ok(Response::new(CleanupSandboxResponse {
            retained_execution_id: None,
            actions: vec!["Disposed prepared clone resources".into()],
        }))
    }

    pub(super) async fn run_owned_clone(
        &self,
        donor: &str,
        target: &str,
        work: impl FnOnce() -> Result<(), Status> + Send + 'static,
    ) -> Result<(), Status> {
        // Share preparation capacity; cloning is disk preparation work. Keep
        // capacity and both ordered gates inside the tracked blocking task,
        // so cancellation cannot let cleanup overtake filesystem mutation.
        let permit = self
            .lifecycle_limiter
            .acquire(crate::lifecycle_limiter::LifecycleMethod::PrepareSandbox)
            .await?;
        let (first, second) = if donor < target {
            (donor, target)
        } else {
            (target, donor)
        };
        let first_claim = self.claim_sandbox_work(first)?;
        let second_claim = self.claim_sandbox_work(second)?;
        let (first_gate, second_gate) = tokio::time::timeout(Duration::from_secs(10), async {
            let first = self.lock_sandbox_cgroups(first).await;
            let second = self.lock_sandbox_cgroups(second).await;
            (first, second)
        })
        .await
        .map_err(|_| Status::unavailable("clone lifecycle ownership is busy"))?;
        let task = self.off_runtime("clone", move || {
            let _ownership = (permit, first_claim, second_claim, first_gate, second_gate);
            work()
        });
        tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .map_err(|_| Status::deadline_exceeded("clone continues under cleanup ownership"))?
    }

    pub(super) async fn expose_port(
        &self,
        request: Request<ExposePortRequest>,
    ) -> Result<Response<ExposePortResponse>, Status> {
        self.authorize_peer(&request, "ExposePort", ALLOW_NODE_ADMIN_OR_NODEAPI)?;
        let req = request.get_ref();
        validate_sandbox_id(&req.sandbox_id)?;
        if req.port == 0 || req.port > 65535 {
            return Err(Status::invalid_argument("port must be 1-65535"));
        }
        let port = req.port as u16;
        if let Some(reason) = reserved_expose_port_reason(port) {
            return Err(Status::permission_denied(format!(
                "refusing to expose reserved host port {port} ({reason})"
            )));
        }
        let protocol = request_protocol(req.protocol)?;

        let _mutation_claim = self.claim_sandbox_work(&req.sandbox_id)?;
        let (vpeer_ip, pod_ns, service_port_ingress_isolation) = {
            let info = self
                .pool
                .get(&req.sandbox_id)
                .map_err(slot_store_status)?
                .ok_or_else(|| Status::not_found("sandbox not allocated"))?;
            let net = NetId::from_slot_info(&info);
            (
                net.vpeer_ip_bare(),
                info.pod_netns.clone(),
                self.pool.config().service_port_ingress_isolation,
            )
        };

        let guest_port = req.guest_port.unwrap_or(req.port);
        if guest_port == 0 || guest_port > 65535 {
            return Err(Status::invalid_argument("guest_port must be 1-65535"));
        }
        let guest_port = guest_port as u16;
        let base_details = || {
            SandboxEventDetails::default()
                .operation("ingress_dnat")
                .protocol(protocol)
                .host_port(port.to_string())
                .guest_port(guest_port.to_string())
                .netns(pod_ns.clone().unwrap_or_else(|| "root".to_string()))
                .vpeer_ip(vpeer_ip.clone())
                .bool_field(SandboxEventBoolField::HasPodNetns, pod_ns.is_some())
        };
        emit_sandbox_event(
            &req.sandbox_id,
            SandboxEventType::PortExposureApplyStarted,
            SandboxEventSource::NodeManager,
            base_details(),
        );

        if let Err(message) = validate_ingress_dnat_namespace(
            pod_ns.as_deref(),
            protocol,
            port,
            service_port_ingress_isolation,
        ) {
            tracing::warn!(
                sandbox_id = %req.sandbox_id,
                %protocol,
                host_port = port,
                guest_port,
                %vpeer_ip,
                service_port_ingress_isolation = %service_port_ingress_isolation,
                "refusing unsafe root namespace ingress DNAT"
            );
            emit_sandbox_event(
                &req.sandbox_id,
                SandboxEventType::PortExposureApplyFailed,
                SandboxEventSource::NodeManager,
                base_details()
                    .reason("root_netns_expose_rejected")
                    .error(message.clone())
                    .logged_by_caller(),
            );
            return Err(Status::failed_precondition(message));
        }

        if let Some((existing_ip, existing_guest_port)) =
            crate::firewall::find_ingress_dnat_target(pod_ns.as_deref(), protocol, port)
        {
            if existing_ip == vpeer_ip && existing_guest_port == guest_port {
                emit_sandbox_event(
                    &req.sandbox_id,
                    SandboxEventType::PortExposureApplyCompleted,
                    SandboxEventSource::NodeManager,
                    base_details().result("idempotent"),
                );
                return Ok(Response::new(ExposePortResponse {}));
            }
            let error = format!(
                "host port {protocol}/{port} is already exposed to {existing_ip}:{existing_guest_port}"
            );
            emit_sandbox_event(
                &req.sandbox_id,
                SandboxEventType::PortExposureApplyFailed,
                SandboxEventSource::NodeManager,
                base_details().error(error.clone()),
            );
            return Err(Status::already_exists(error));
        }

        let ingress_iface =
            crate::net::ingress_iface_for_namespace(pod_ns.as_deref()).map_err(|error| {
                let status = ingress_iface_error_status(&error);
                emit_sandbox_event(
                    &req.sandbox_id,
                    SandboxEventType::PortExposureApplyFailed,
                    SandboxEventSource::NodeManager,
                    base_details()
                        .reason(ingress_iface_error_reason(&error))
                        .error(status.message().to_string()),
                );
                status
            })?;

        crate::firewall::add_ingress_dnat_rule(
            pod_ns.as_deref(),
            port,
            guest_port,
            protocol,
            &vpeer_ip,
            &ingress_iface,
        )
        .map_err(|e| {
            let status = Status::internal(format!("add ingress DNAT failed: {e}"));
            emit_sandbox_event(
                &req.sandbox_id,
                SandboxEventType::PortExposureApplyFailed,
                SandboxEventSource::NodeManager,
                base_details()
                    .ingress_iface(ingress_iface.clone())
                    .error(status.message().to_string()),
            );
            status
        })?;
        tracing::info!(
            sandbox_id = %req.sandbox_id,
            %protocol,
            host_port = port,
            %ingress_iface,
            %vpeer_ip,
            guest_port,
            "installed ingress DNAT",
        );
        emit_sandbox_event(
            &req.sandbox_id,
            SandboxEventType::PortExposureApplyCompleted,
            SandboxEventSource::NodeManager,
            base_details()
                .ingress_iface(ingress_iface)
                .result("installed"),
        );
        Ok(Response::new(ExposePortResponse {}))
    }

    pub(super) async fn unexpose_port(
        &self,
        request: Request<UnexposePortRequest>,
    ) -> Result<Response<UnexposePortResponse>, Status> {
        self.authorize_peer(&request, "UnexposePort", ALLOW_NODE_ADMIN_OR_NODEAPI)?;
        let req = request.get_ref();
        validate_sandbox_id(&req.sandbox_id)?;
        let port = match req.port {
            Some(port) if port == 0 || port > 65535 => {
                return Err(Status::invalid_argument("port must be 1-65535"));
            }
            Some(port) => crate::firewall::PortSelector::Port(port as u16),
            None => crate::firewall::PortSelector::All,
        };
        let protocol = request_protocol(req.protocol)?;

        let _mutation_claim = self.claim_sandbox_work(&req.sandbox_id)?;
        let (vpeer_ip, pod_ns) = {
            let info = self
                .pool
                .get(&req.sandbox_id)
                .map_err(slot_store_status)?
                .ok_or_else(|| Status::not_found("sandbox not allocated"))?;
            let net = NetId::from_slot_info(&info);
            (net.vpeer_ip_bare(), info.pod_netns.clone())
        };
        let base_details = || {
            SandboxEventDetails::default()
                .operation("ingress_dnat")
                .protocol(protocol)
                .host_port(describe_port_selector(port))
                .netns(pod_ns.clone().unwrap_or_else(|| "root".to_string()))
                .vpeer_ip(vpeer_ip.clone())
                .bool_field(SandboxEventBoolField::HasPodNetns, pod_ns.is_some())
        };
        emit_sandbox_event(
            &req.sandbox_id,
            SandboxEventType::PortExposureRemoveStarted,
            SandboxEventSource::NodeManager,
            base_details(),
        );

        crate::firewall::remove_ingress_dnat_rules(pod_ns.as_deref(), port, protocol, &vpeer_ip)
            .map_err(|e| {
                let status = Status::internal(format!("remove ingress DNAT failed: {e}"));
                emit_sandbox_event(
                    &req.sandbox_id,
                    SandboxEventType::PortExposureRemoveFailed,
                    SandboxEventSource::NodeManager,
                    base_details().error(status.message().to_string()),
                );
                status
            })?;
        tracing::info!(
            sandbox_id = %req.sandbox_id,
            %protocol,
            host_port = %describe_port_selector(port),
            %vpeer_ip,
            "removed ingress DNAT"
        );
        emit_sandbox_event(
            &req.sandbox_id,
            SandboxEventType::PortExposureRemoveCompleted,
            SandboxEventSource::NodeManager,
            base_details().result("removed"),
        );
        Ok(Response::new(UnexposePortResponse {}))
    }

    pub(super) async fn allow_port(
        &self,
        request: Request<AllowPortRequest>,
    ) -> Result<Response<AllowPortResponse>, Status> {
        let req = request.get_ref();
        let request_params = allow_port_request_params(req);
        pplx_space_common_svc::record_safe_grpc_request_params(&request_params);

        let result = (|| {
            self.authorize_peer(&request, "AllowPort", ALLOW_NODE_ADMIN_OR_NODEAPI)?;
            validate_sandbox_id(&req.sandbox_id)?;
            if req.port == 0 || req.port > 65535 {
                return Err(Status::invalid_argument("port must be 1-65535"));
            }
            let port = req.port as u16;
            let protocol = request_protocol(req.protocol)?;

            let _mutation_claim = self.claim_sandbox_work(&req.sandbox_id)?;
            let (ns_name, vpeer) = {
                let info = self
                    .pool
                    .get(&req.sandbox_id)
                    .map_err(slot_store_status)?
                    .ok_or_else(|| {
                        Status::not_found(format!("sandbox not allocated: {}", req.sandbox_id))
                    })?;
                let net = NetId::from_slot_info(&info);
                (info.ns_name(), net.vpeer())
            };
            emit_sandbox_event(
                &req.sandbox_id,
                SandboxEventType::PortExposureApplyStarted,
                SandboxEventSource::NodeManager,
                SandboxEventDetails::default()
                    .operation("guest_allow")
                    .protocol(protocol)
                    .guest_port(port.to_string())
                    .netns(ns_name.clone()),
            );

            if crate::firewall::has_guest_port_allowed(&ns_name, port, protocol) {
                emit_sandbox_event(
                    &req.sandbox_id,
                    SandboxEventType::PortExposureApplyCompleted,
                    SandboxEventSource::NodeManager,
                    SandboxEventDetails::default()
                        .operation("guest_allow")
                        .protocol(protocol)
                        .guest_port(port.to_string())
                        .netns(ns_name)
                        .result("idempotent"),
                );
                return Ok(Response::new(AllowPortResponse {}));
            }

            crate::firewall::allow_guest_port(&ns_name, &vpeer, port, protocol).map_err(|e| {
                let status = Status::internal(format!("allow guest port failed: {e}"));
                emit_sandbox_event(
                    &req.sandbox_id,
                    SandboxEventType::PortExposureApplyFailed,
                    SandboxEventSource::NodeManager,
                    SandboxEventDetails::default()
                        .operation("guest_allow")
                        .protocol(protocol)
                        .guest_port(port.to_string())
                        .netns(ns_name.clone())
                        .error(status.message().to_string()),
                );
                status
            })?;
            tracing::info!(
                sandbox_id = %req.sandbox_id,
                %protocol,
                guest_port = port,
                ns = %ns_name,
                "installed guest port allow rule",
            );
            emit_sandbox_event(
                &req.sandbox_id,
                SandboxEventType::PortExposureApplyCompleted,
                SandboxEventSource::NodeManager,
                SandboxEventDetails::default()
                    .operation("guest_allow")
                    .protocol(protocol)
                    .guest_port(port.to_string())
                    .netns(ns_name)
                    .result("installed"),
            );
            Ok(Response::new(AllowPortResponse {}))
        })();
        if let Err(status) = &result {
            pplx_space_common_svc::log_failed_grpc_request_params(status, &request_params);
        }
        result
    }

    pub(super) async fn deny_port(
        &self,
        request: Request<DenyPortRequest>,
    ) -> Result<Response<DenyPortResponse>, Status> {
        self.authorize_peer(&request, "DenyPort", ALLOW_NODE_ADMIN_OR_NODEAPI)?;
        let req = request.get_ref();
        validate_sandbox_id(&req.sandbox_id)?;
        let port = match req.port {
            Some(port) if port == 0 || port > 65535 => {
                return Err(Status::invalid_argument("port must be 1-65535"));
            }
            Some(port) => crate::firewall::PortSelector::Port(port as u16),
            None => crate::firewall::PortSelector::All,
        };
        let protocol = request_protocol(req.protocol)?;

        let _mutation_claim = self.claim_sandbox_work(&req.sandbox_id)?;
        let ns_name = {
            let info = self
                .pool
                .get(&req.sandbox_id)
                .map_err(slot_store_status)?
                .ok_or_else(|| Status::not_found("sandbox not allocated"))?;
            info.ns_name()
        };
        let base_details = || {
            SandboxEventDetails::default()
                .operation("guest_allow")
                .protocol(protocol)
                .guest_port(describe_port_selector(port))
                .netns(ns_name.clone())
        };
        emit_sandbox_event(
            &req.sandbox_id,
            SandboxEventType::PortExposureRemoveStarted,
            SandboxEventSource::NodeManager,
            base_details(),
        );

        crate::firewall::deny_guest_port(&ns_name, port, protocol).map_err(|e| {
            let status = Status::internal(format!("deny guest port failed: {e}"));
            emit_sandbox_event(
                &req.sandbox_id,
                SandboxEventType::PortExposureRemoveFailed,
                SandboxEventSource::NodeManager,
                base_details().error(status.message().to_string()),
            );
            status
        })?;
        tracing::info!(
            sandbox_id = %req.sandbox_id,
            %protocol,
            guest_port = %describe_port_selector(port),
            ns = %ns_name,
            "denied guest port"
        );
        emit_sandbox_event(
            &req.sandbox_id,
            SandboxEventType::PortExposureRemoveCompleted,
            SandboxEventSource::NodeManager,
            base_details().result("removed"),
        );
        Ok(Response::new(DenyPortResponse {}))
    }
}

fn validate_ingress_dnat_namespace(
    pod_ns: Option<&str>,
    protocol: &str,
    port: u16,
    service_port_ingress_isolation: crate::slot_pool::ServicePortIngressIsolation,
) -> Result<(), String> {
    if pod_ns.is_some() || service_port_ingress_isolation.permits_root_ingress() {
        return Ok(());
    }
    Err(format!(
        "refusing to expose {protocol}/{port} in the root network namespace with service-port ingress isolation {service_port_ingress_isolation}"
    ))
}

/// Map an ingress-interface resolution failure to the ExposePort status.
///
/// A missing pod network namespace means the CNI sandbox behind this
/// allocation is gone: the allocation is stale and retrying on this node can
/// never succeed, so return `FailedPrecondition` tagged with the
/// stale-pod-netns terminal-reason metadata (which node-ctl classifies as
/// terminal so the desired-state ref completes while the orphan reconciler
/// repairs durable state) instead of a retryable `Internal`. The metadata key
/// — not the bare `FailedPrecondition` code — is the contract, because the
/// same code is also returned for the root-namespace ingress-safety rejection,
/// which must stay retryable. Everything else — netlink timeouts, resource
/// exhaustion, rtnetlink errors — stays `Internal` and therefore retryable.
fn ingress_iface_error_status(error: &crate::net::DefaultIfaceError) -> Status {
    match error {
        crate::net::DefaultIfaceError::NetnsMissing { .. } => {
            let mut status = Status::failed_precondition(format!(
                "pod network namespace is gone; the sandbox allocation is stale: {error}"
            ));
            status.metadata_mut().insert(
                pplx_space_common_core::port_exposure_signals::EXPOSE_TERMINAL_REASON_METADATA_KEY,
                tonic::metadata::MetadataValue::from_static(
                    pplx_space_common_core::port_exposure_signals::EXPOSE_TERMINAL_REASON_STALE_POD_NETNS,
                ),
            );
            status
        }
        crate::net::DefaultIfaceError::Other(_) => {
            Status::internal(format!("resolve ingress interface failed: {error}"))
        }
    }
}

fn ingress_iface_error_reason(error: &crate::net::DefaultIfaceError) -> &'static str {
    match error {
        crate::net::DefaultIfaceError::NetnsMissing { .. } => "pod_netns_missing",
        crate::net::DefaultIfaceError::Other(_) => "resolve_ingress_iface_failed",
    }
}

#[cfg(test)]
mod tests;
