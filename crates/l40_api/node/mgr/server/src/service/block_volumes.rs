//! Privileged retained-disk preparation. The control plane grants ownership;
//! node-manager pins hardware and retains release authority until cleanup.

mod device;

use pplx_space_node_api_client::proto as cluster;
use tonic::{Request, Response, Status};

use super::{ALLOW_NODE_ADMIN_OR_SBXMGR, NodeManagerService, slot_store_status};
use crate::proto;

fn refused() -> Status {
    Status::failed_precondition("retained block-volume execution or initialization is not current")
}

async fn connect_volumes() -> Result<pplx_space_volmgr_client::ConnectedVolumeManagerClient, Status>
{
    pplx_space_volmgr_client::connect(pplx_space_volmgr_client::VOLMGR_SOCKET)
        .await
        .map_err(|error| {
            Status::unavailable(format!("connect retained block-volume relay: {error}"))
        })
}

impl NodeManagerService {
    pub(super) async fn grow_block_volume(
        &self,
        request: Request<proto::GrowBlockVolumeRequest>,
    ) -> Result<Response<proto::PrepareBlockVolumeResponse>, Status> {
        self.authorize_peer(&request, "GrowBlockVolume", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let sandbox =
            super::trusted_paths::canonical_sandbox_id(&request.get_ref().sandbox_id)?.to_string();
        let service = self.clone();
        self.run_owned_device_operation(
            &sandbox,
            crate::lifecycle_limiter::LifecycleMethod::PrepareSandbox,
            async move {
                let req = request.into_inner();
                let id = req.sandbox_id.as_str();
                let local = service
                    .check_execution(id, req.execution_fence.as_ref(), false)?
                    .ok_or_else(refused)?;
                let record = service
                    .pool
                    .retained_block_binding(id)
                    .map_err(slot_store_status)?
                    .ok_or_else(refused)?;
                let mut binding = record.binding;
                device::validate_binding(&binding)?;
                if record.execution_id != local.current().to_string()
                    || binding.read_only
                    || !binding.initialization_ready
                {
                    return Err(refused());
                }
                let mut volumes = connect_volumes().await?;
                let resolve = cluster::ReconcileBlockVolumeGrowthRequest {
                    fence: binding.fence.clone(),
                    operation_id: req.operation_id,
                    device_size_bytes: 0,
                    filesystem_size_bytes: 0,
                };
                let growth = volumes
                    .reconcile_block_volume_growth(resolve.clone())
                    .await?
                    .into_inner();
                if !growth.backing_ready || growth.size_gib != 256 {
                    return Err(Status::unavailable("cloud disk growth is not ready"));
                }
                let mut grown = binding.clone();
                grown.size_gib = growth.size_gib;
                let verify = grown.clone();
                let _pinned = service
                    .off_runtime("verify grown retained device", move || {
                        device::pin(
                            &verify.aws_volume_id,
                            verify.capacity().map_err(|_| refused())?,
                        )
                    })
                    .await?;
                if !growth.complete {
                    let runtime = service
                        .recorded_execution(id.to_owned())
                        .await?
                        .ok_or_else(refused)?;
                    if runtime.runtime().kind != pplx_space_execution_spec::RuntimeKind::Firecracker
                    {
                        return Err(Status::failed_precondition(
                            "unsupported online growth runtime",
                        ));
                    }
                    let api = crate::fc_api::FcApiClient::new(format!(
                        "{}/firecracker/{id}/root/firecracker.sock",
                        super::JAIL_DIR,
                    ));
                    api.refresh_data_drive_capacity().await.map_err(|error| {
                        Status::unavailable(format!("notify data drive growth: {error}"))
                    })?;
                    let config = pplx_space_spaced_host_client::SpacedHostClientConfig::default()
                        .with_socket_path(pplx_space_host_layout::paths::spaced_host_socket(id));
                    let guest =
                        pplx_space_spaced_host_client::SpacedHostClient::new_with_config(config)
                            .await
                            .map_err(|error| {
                                Status::unavailable(format!("connect growth guest: {error}"))
                            })?;
                    let observed = guest
                        .grow_data_drive(
                            pplx_space_spaced_host_client::proto::GrowDataDriveRequest {
                                topology_version: binding.topology_version,
                                mount: binding.mount.clone(),
                                filesystem_uuid: binding.filesystem_uuid.clone(),
                                size_gib: growth.size_gib,
                            },
                        )
                        .await?;
                    // Persist local geometry before releasing the central
                    // checkpoint barrier. A lost central reply is reconciled
                    // using the same operation, never the old device size.
                    if !service
                        .pool
                        .save_retained_block_binding(id, &local.current().to_string(), &grown)
                        .map_err(slot_store_status)?
                    {
                        return Err(refused());
                    }
                    let complete = volumes
                        .reconcile_block_volume_growth(cluster::ReconcileBlockVolumeGrowthRequest {
                            device_size_bytes: observed.device_size_bytes,
                            filesystem_size_bytes: observed.filesystem_size_bytes,
                            ..resolve
                        })
                        .await?
                        .into_inner();
                    if !complete.complete {
                        return Err(refused());
                    }
                }
                binding.size_gib = growth.size_gib;
                if growth.complete
                    && !service
                        .pool
                        .save_retained_block_binding(id, &local.current().to_string(), &binding)
                        .map_err(slot_store_status)?
                {
                    return Err(refused());
                }
                Ok(Response::new(proto::PrepareBlockVolumeResponse {
                    binding: Some(binding),
                }))
            },
        )
        .await
    }

    /// Resume may follow a failed suspend publication. Withdraw that mutable
    /// checkpoint before making the still-attached VM writable again.
    pub(super) async fn invalidate_retained_checkpoint_before_resume(
        &self,
        sandbox: &str,
    ) -> Result<(), Status> {
        let Some(record) = self
            .pool
            .retained_block_binding(sandbox)
            .map_err(slot_store_status)?
        else {
            return Ok(());
        };
        let execution = record
            .binding
            .fence
            .as_ref()
            .and_then(|fence| fence.execution.as_ref())
            .ok_or_else(refused)?;
        connect_volumes()
            .await?
            .activate_block_volume(cluster::ActivateBlockVolumeRequest {
                fence: record.binding.fence.clone(),
                activation_id: execution.activation_attempt_id.clone(),
                checkpoint_id: None,
            })
            .await?;
        Ok(())
    }

    pub(super) async fn prepare_block_volume(
        &self,
        request: Request<proto::PrepareBlockVolumeRequest>,
    ) -> Result<Response<proto::PrepareBlockVolumeResponse>, Status> {
        self.authorize_peer(&request, "PrepareBlockVolume", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let execution = request.get_ref().execution.as_ref().ok_or_else(refused)?;
        let sandbox =
            super::trusted_paths::canonical_sandbox_id(&execution.sandbox_id)?.to_string();
        let service = self.clone();
        self.run_owned_device_operation(
            &sandbox,
            crate::lifecycle_limiter::LifecycleMethod::PrepareSandbox,
            async move {
                let req = request.into_inner();
                let execution = req.execution.ok_or_else(refused)?;
                let sandbox_id = execution.sandbox_id.clone();
                let sandbox = sandbox_id.as_str();
                let local = service
                    .check_execution(sandbox, req.execution_fence.as_ref(), false)?
                    .ok_or_else(refused)?;
                let mut volumes = connect_volumes().await?;
                // Resolving can reserve/attach centrally before its response is
                // received. Persist uncertainty before crossing that boundary.
                let local_execution = local.current().to_string();
                if !service
                    .pool
                    .save_pending_block_resolution(sandbox, &local_execution)
                    .map_err(slot_store_status)?
                {
                    return Err(refused());
                }
                let Some(mut binding) = volumes
                    .resolve_block_volume_binding(cluster::ResolveBlockVolumeBindingRequest {
                        execution: Some(execution),
                    })
                    .await?
                    .into_inner()
                    .binding
                else {
                    // Never let a missing central grant silently discard local
                    // release authority from an earlier activation.
                    if service
                        .pool
                        .retained_block_binding(sandbox)
                        .map_err(slot_store_status)?
                        .is_some()
                    {
                        return Err(refused());
                    }
                    service
                        .pool
                        .clear_pending_block_resolution(sandbox, &local_execution)
                        .map_err(slot_store_status)?;
                    return Ok(Response::new(proto::PrepareBlockVolumeResponse {
                        binding: None,
                    }));
                };
                device::validate_binding(&binding)?;
                let existing = service
                    .pool
                    .retained_block_binding(sandbox)
                    .map_err(slot_store_status)?;
                if existing.as_ref().is_some_and(|old| {
                    old.execution_id != local.current().to_string()
                        || old.binding.fence != binding.fence
                        || old.binding.aws_volume_id != binding.aws_volume_id
                }) {
                    return Err(refused());
                }
                // Retain release authority even when a previous initialization
                // claim is ambiguous and this attempt must refuse formatting.
                if existing.is_none()
                    && !service
                        .pool
                        .save_retained_block_binding(
                            sandbox,
                            &local.current().to_string(),
                            &binding,
                        )
                        .map_err(slot_store_status)?
                {
                    return Err(refused());
                }
                service
                    .pool
                    .clear_pending_block_resolution(sandbox, &local_execution)
                    .map_err(slot_store_status)?;
                if !binding.initialization_ready {
                    let claim = match binding.initialization_claim.clone() {
                        Some(claim) => {
                            // Only a durable local success receipt may finish an
                            // interrupted initialization. Never infer mkfs success
                            // by parsing a potentially guest-modified filesystem.
                            if !existing.as_ref().is_some_and(|old| {
                                old.binding.initialization_ready
                                    && old.binding.initialization_claim.as_deref()
                                        == Some(claim.as_str())
                            }) {
                                return Err(refused());
                            }
                            claim
                        }
                        None => {
                            let claim = uuid::Uuid::now_v7().to_string();
                            if !volumes
                                .claim_block_volume_initialization(
                                    cluster::ClaimBlockVolumeInitializationRequest {
                                        fence: binding.fence.clone(),
                                        claim_id: claim.clone(),
                                    },
                                )
                                .await?
                                .into_inner()
                                .granted
                            {
                                return Err(refused());
                            }
                            binding.initialization_claim = Some(claim.clone());
                            if !service
                                .pool
                                .save_retained_block_binding(
                                    sandbox,
                                    &local.current().to_string(),
                                    &binding,
                                )
                                .map_err(slot_store_status)?
                            {
                                return Err(refused());
                            }
                            let format = binding.clone();
                            service
                                .off_runtime("initialize retained block device", move || {
                                    device::format_fresh(&format)
                                })
                                .await?;
                            binding.initialization_ready = true;
                            if !service
                                .pool
                                .save_retained_block_binding(
                                    sandbox,
                                    &local.current().to_string(),
                                    &binding,
                                )
                                .map_err(slot_store_status)?
                            {
                                return Err(refused());
                            }
                            claim
                        }
                    };
                    volumes
                        .complete_block_volume_initialization(
                            cluster::CompleteBlockVolumeInitializationRequest {
                                fence: binding.fence.clone(),
                                claim_id: claim,
                                filesystem_uuid: binding.filesystem_uuid.clone(),
                            },
                        )
                        .await?;
                    binding.initialization_ready = true;
                }
                // Pin/serial-check ready disks too, but never run host fsck, mount,
                // blkid or mkfs on bytes previously exposed to the guest.
                let verify = binding.clone();
                service
                    .off_runtime("verify retained block device", move || {
                        let capacity = verify.capacity().map_err(|error| {
                            Status::failed_precondition(format!("invalid disk geometry: {error}"))
                        })?;
                        device::pin(&verify.aws_volume_id, capacity).map(drop)
                    })
                    .await?;
                if !service
                    .pool
                    .save_retained_block_binding(sandbox, &local.current().to_string(), &binding)
                    .map_err(slot_store_status)?
                {
                    return Err(refused());
                }
                Ok(Response::new(proto::PrepareBlockVolumeResponse {
                    binding: Some(binding),
                }))
            },
        )
        .await
    }

    pub(super) async fn activate_block_volume(
        &self,
        request: Request<proto::ActivateBlockVolumeRequest>,
    ) -> Result<Response<proto::PrepareBlockVolumeResponse>, Status> {
        self.authorize_peer(&request, "ActivateBlockVolume", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let sandbox =
            super::trusted_paths::canonical_sandbox_id(&request.get_ref().sandbox_id)?.to_string();
        let service = self.clone();
        self.run_owned_device_operation(
            &sandbox,
            crate::lifecycle_limiter::LifecycleMethod::PrepareSandbox,
            async move {
                let req = request.into_inner();
                let local = service
                    .check_execution(&req.sandbox_id, req.execution_fence.as_ref(), false)?
                    .ok_or_else(refused)?;
                let Some(mut record) = service
                    .pool
                    .retained_block_binding(&req.sandbox_id)
                    .map_err(slot_store_status)?
                else {
                    return Ok(Response::new(proto::PrepareBlockVolumeResponse {
                        binding: None,
                    }));
                };
                if record.execution_id != local.current().to_string() {
                    return Err(refused());
                }
                let attempt = record
                    .binding
                    .fence
                    .as_ref()
                    .and_then(|f| f.execution.as_ref())
                    .ok_or_else(refused)?
                    .activation_attempt_id
                    .clone();
                let result = connect_volumes()
                    .await?
                    .activate_block_volume(cluster::ActivateBlockVolumeRequest {
                        fence: record.binding.fence.clone(),
                        activation_id: attempt,
                        checkpoint_id: req.checkpoint_id,
                    })
                    .await?
                    .into_inner();
                record.binding.disk_generation = result.disk_generation;
                if !service
                    .pool
                    .save_retained_block_binding(
                        &req.sandbox_id,
                        &record.execution_id,
                        &record.binding,
                    )
                    .map_err(slot_store_status)?
                {
                    return Err(refused());
                }
                Ok(Response::new(proto::PrepareBlockVolumeResponse {
                    binding: Some(record.binding),
                }))
            },
        )
        .await
    }

    pub(super) async fn publish_block_volume_checkpoint(
        &self,
        request: Request<proto::PublishBlockVolumeCheckpointRequest>,
    ) -> Result<Response<proto::PrepareBlockVolumeResponse>, Status> {
        self.authorize_peer(
            &request,
            "PublishBlockVolumeCheckpoint",
            ALLOW_NODE_ADMIN_OR_SBXMGR,
        )?;
        let sandbox =
            super::trusted_paths::canonical_sandbox_id(&request.get_ref().sandbox_id)?.to_string();
        let service = self.clone();
        self.run_owned_device_operation(
            &sandbox,
            crate::lifecycle_limiter::LifecycleMethod::PrepareSandbox,
            async move {
                let req = request.into_inner();
                let local = service
                    .check_execution(&req.sandbox_id, req.execution_fence.as_ref(), false)?
                    .ok_or_else(refused)?;
                let Some(record) = service
                    .pool
                    .retained_block_binding(&req.sandbox_id)
                    .map_err(slot_store_status)?
                else {
                    return Ok(Response::new(proto::PrepareBlockVolumeResponse {
                        binding: None,
                    }));
                };
                if record.execution_id != local.current().to_string() {
                    return Err(refused());
                }
                connect_volumes()
                    .await?
                    .publish_block_volume_checkpoint(cluster::PublishBlockVolumeCheckpointRequest {
                        fence: record.binding.fence.clone(),
                        disk_generation: record.binding.disk_generation,
                        checkpoint_id: req.checkpoint_id,
                    })
                    .await?;
                Ok(Response::new(proto::PrepareBlockVolumeResponse {
                    binding: Some(record.binding),
                }))
            },
        )
        .await
    }

    /// Called only after supervisor stop, cgroup quiescence and non-lazy jail
    /// unmounts have succeeded, while cleanup retains its execution claim.
    pub(super) async fn release_retained_block_after_cleanup(
        &self,
        sandbox: &str,
        recover_deleted_owner: bool,
    ) -> Result<(), Status> {
        let pending = self
            .pool
            .pending_block_resolution(sandbox)
            .map_err(slot_store_status)?;
        let Some(record) = self
            .pool
            .retained_block_binding(sandbox)
            .map_err(slot_store_status)?
        else {
            if recover_deleted_owner || pending.is_some() {
                connect_volumes()
                    .await?
                    .release_block_volume_binding(cluster::ReleaseBlockVolumeBindingRequest {
                        fence: None,
                        deleted_owner_cleanup: Some(cluster::DeletedBlockVolumeOwnerCleanup {
                            sandbox_id: sandbox.to_owned(),
                            node_id: String::new(),
                        }),
                    })
                    .await?;
                if let Some(execution) = pending {
                    self.pool
                        .clear_pending_block_resolution(sandbox, &execution)
                        .map_err(slot_store_status)?;
                }
            }
            return Ok(());
        };
        connect_volumes()
            .await?
            .release_block_volume_binding(cluster::ReleaseBlockVolumeBindingRequest {
                fence: record.binding.fence,
                deleted_owner_cleanup: None,
            })
            .await?;
        self.pool
            .clear_pending_block_resolution(sandbox, &record.execution_id)
            .map_err(slot_store_status)?;
        self.pool
            .clear_retained_block_binding(sandbox, &record.execution_id)
            .map_err(slot_store_status)
    }

    pub(super) async fn bind_retained_block_for_launch(
        &self,
        sandbox: &str,
        jail: &str,
    ) -> Result<(), Status> {
        let Some(record) = self
            .pool
            .retained_block_binding(sandbox)
            .map_err(slot_store_status)?
        else {
            return Ok(());
        };
        let local = self
            .pool
            .execution_fence(sandbox)
            .map_err(slot_store_status)?
            .ok_or_else(refused)?;
        if record.execution_id != local.current().to_string()
            || !record.binding.initialization_ready
            || record.binding.disk_generation == 0
        {
            return Err(refused());
        }
        let jail = jail.to_owned();
        self.off_runtime("bind retained block device", move || {
            device::bind(&record.binding, &jail)
        })
        .await
    }
}
