use super::*;
use crate::proto::{CloneCapabilitiesRequest, CloneCapabilitiesResponse};
use pplx_space_node_runtime_core::launch_admission::runtime_operation_status;
use tracing::Instrument as _;

struct RpcWait {
    method: &'static str,
    outcome: &'static str,
    started: Instant,
    span: tracing::Span,
}

impl Drop for RpcWait {
    fn drop(&mut self) {
        self.span.record("outcome", self.outcome);
        pplx_space_metrics::node_manager::rpc_wait_finished(
            self.method,
            self.outcome,
            self.started,
        );
    }
}

async fn observe_rpc<T>(
    method: &'static str,
    sandbox_id: String,
    operation: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    let span = tracing::info_span!(
        "node_manager.rpc_wait",
        method,
        sandbox_id,
        outcome = tracing::field::Empty
    );
    let mut observation = RpcWait {
        method,
        outcome: "cancelled",
        started: Instant::now(),
        span: span.clone(),
    };
    let result = operation.instrument(span).await;
    observation.outcome = if result.is_ok() {
        "succeeded"
    } else {
        "failed"
    };
    result
}

#[tonic::async_trait]
impl NodeManager for NodeManagerService {
    type OpenGuestChannelStream = super::guest_control::ChannelStream;
    async fn request_guest_shutdown(
        &self,
        request: Request<crate::proto::RequestGuestShutdownRequest>,
    ) -> Result<Response<crate::proto::RequestGuestShutdownResponse>, Status> {
        NodeManagerService::request_guest_shutdown(self, request).await
    }
    async fn probe_guest_service(
        &self,
        request: Request<crate::proto::ProbeGuestServiceRequest>,
    ) -> Result<Response<crate::proto::ProbeGuestServiceResponse>, Status> {
        NodeManagerService::probe_guest_service(self, request).await
    }
    async fn open_guest_channel(
        &self,
        request: Request<tonic::Streaming<crate::proto::OpenGuestChannelRequest>>,
    ) -> Result<Response<Self::OpenGuestChannelStream>, Status> {
        NodeManagerService::open_guest_channel(self, request).await
    }

    async fn validate_gateway_caller(
        &self,
        request: Request<crate::proto::ValidateGatewayCallerRequest>,
    ) -> Result<Response<crate::proto::ValidateGatewayCallerResponse>, Status> {
        NodeManagerService::validate_gateway_caller(self, request).await
    }

    async fn get_clone_capabilities(
        &self,
        request: Request<CloneCapabilitiesRequest>,
    ) -> Result<Response<CloneCapabilitiesResponse>, Status> {
        self.authorize_peer(&request, "GetCloneCapabilities", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        Ok(Response::new(CloneCapabilitiesResponse {
            prepared_clone_disposal: true,
        }))
    }

    async fn resolve_execution_environment(
        &self,
        request: Request<crate::proto::ResolveExecutionEnvironmentRequest>,
    ) -> Result<Response<crate::proto::ResolveExecutionEnvironmentResponse>, Status> {
        NodeManagerService::resolve_execution_environment(self, request).await
    }

    async fn apply_execution_policy(
        &self,
        request: Request<crate::proto::ApplyExecutionPolicyRequest>,
    ) -> Result<Response<crate::proto::ApplyExecutionPolicyResponse>, Status> {
        NodeManagerService::apply_execution_policy(self, request).await
    }

    async fn get_execution_inventory(
        &self,
        request: Request<GetExecutionInventoryRequest>,
    ) -> Result<Response<pplx_space_execution_common::proto::NodeExecutionInventory>, Status> {
        NodeManagerService::get_execution_inventory(self, request).await
    }

    async fn allocate(
        &self,
        request: Request<AllocateRequest>,
    ) -> Result<Response<AllocateResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc(
            "Allocate",
            sandbox_id,
            NodeManagerService::allocate(self, request),
        )
        .await
    }

    async fn launch(
        &self,
        request: Request<LaunchRequest>,
    ) -> Result<Response<LaunchResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc(
            "Launch",
            sandbox_id,
            NodeManagerService::launch(self, request),
        )
        .await
    }

    async fn release(
        &self,
        request: Request<ReleaseRequest>,
    ) -> Result<Response<ReleaseResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc(
            "Release",
            sandbox_id,
            NodeManagerService::release(self, request),
        )
        .await
    }

    async fn get_node_resource_status(
        &self,
        request: Request<NodeResourceStatusRequest>,
    ) -> Result<Response<NodeResourceStatusResponse>, Status> {
        NodeManagerService::get_node_resource_status(self, request).await
    }

    async fn get_network_pool_status(
        &self,
        request: Request<NetworkPoolStatusRequest>,
    ) -> Result<Response<NetworkPoolStatusResponse>, Status> {
        NodeManagerService::get_network_pool_status(self, request).await
    }

    async fn list_network_leases(
        &self,
        request: Request<ListNetworkLeasesRequest>,
    ) -> Result<Response<ListNetworkLeasesResponse>, Status> {
        NodeManagerService::list_network_leases(self, request).await
    }

    async fn set_network_pool_drain(
        &self,
        request: Request<SetNetworkPoolDrainRequest>,
    ) -> Result<Response<SetNetworkPoolDrainResponse>, Status> {
        NodeManagerService::set_network_pool_drain(self, request).await
    }

    async fn repair_network_pool(
        &self,
        request: Request<RepairNetworkPoolRequest>,
    ) -> Result<Response<RepairNetworkPoolResponse>, Status> {
        NodeManagerService::repair_network_pool(self, request).await
    }

    async fn set_network_pool_config(
        &self,
        request: Request<SetNetworkPoolConfigRequest>,
    ) -> Result<Response<SetNetworkPoolConfigResponse>, Status> {
        NodeManagerService::set_network_pool_config(self, request).await
    }

    async fn stop_process(
        &self,
        request: Request<StopProcessRequest>,
    ) -> Result<Response<StopProcessResponse>, Status> {
        NodeManagerService::stop_process(self, request).await
    }

    async fn launch_sandbox(
        &self,
        request: Request<LaunchSandboxRequest>,
    ) -> Result<Response<LaunchSandboxResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc(
            "LaunchSandbox",
            sandbox_id,
            NodeManagerService::launch_sandbox(self, request),
        )
        .await
    }

    async fn prepare_sandbox(
        &self,
        request: Request<PrepareSandboxRequest>,
    ) -> Result<Response<PrepareSandboxResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc(
            "PrepareSandbox",
            sandbox_id,
            NodeManagerService::prepare_sandbox(self, request),
        )
        .await
    }

    async fn reserve_prepared_rootfs(
        &self,
        request: Request<ReservePreparedRootfsRequest>,
    ) -> Result<Response<ReservePreparedRootfsResponse>, Status> {
        NodeManagerService::reserve_prepared_rootfs(self, request).await
    }

    async fn complete_prepared_rootfs(
        &self,
        request: Request<CompletePreparedRootfsRequest>,
    ) -> Result<Response<CompletePreparedRootfsResponse>, Status> {
        NodeManagerService::complete_prepared_rootfs(self, request).await
    }

    async fn prepare_sandbox_artifact_dir(
        &self,
        request: Request<PrepareSandboxArtifactDirRequest>,
    ) -> Result<Response<PrepareSandboxArtifactDirResponse>, Status> {
        NodeManagerService::prepare_sandbox_artifact_dir(self, request).await
    }

    async fn cleanup_prepared_clone(
        &self,
        request: Request<crate::proto::CleanupPreparedCloneRequest>,
    ) -> Result<Response<CleanupSandboxResponse>, Status> {
        NodeManagerService::cleanup_prepared_clone(self, request).await
    }

    async fn cleanup_sandbox(
        &self,
        request: Request<CleanupSandboxRequest>,
    ) -> Result<Response<CleanupSandboxResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc(
            "CleanupSandbox",
            sandbox_id,
            NodeManagerService::cleanup_sandbox(self, request),
        )
        .await
    }

    async fn inspect_sandbox(
        &self,
        request: Request<InspectSandboxRequest>,
    ) -> Result<Response<InspectSandboxResponse>, Status> {
        NodeManagerService::inspect_sandbox(self, request).await
    }

    async fn capture_sandbox_endpoint_proof(
        &self,
        request: Request<CaptureSandboxEndpointProofRequest>,
    ) -> Result<Response<SandboxEndpointProof>, Status> {
        NodeManagerService::capture_sandbox_endpoint_proof(self, request).await
    }

    async fn validate_sandbox_endpoint_proof(
        &self,
        request: Request<ValidateSandboxEndpointProofRequest>,
    ) -> Result<Response<ValidateSandboxEndpointProofResponse>, Status> {
        NodeManagerService::validate_sandbox_endpoint_proof(self, request).await
    }

    async fn list_orphan_candidates(
        &self,
        request: Request<ListOrphanCandidatesRequest>,
    ) -> Result<Response<ListOrphanCandidatesResponse>, Status> {
        NodeManagerService::list_orphan_candidates(self, request).await
    }

    async fn pause_sandbox(
        &self,
        request: Request<PauseSandboxRequest>,
    ) -> Result<Response<PauseSandboxResponse>, Status> {
        NodeManagerService::pause_sandbox(self, request).await
    }

    async fn resume_sandbox(
        &self,
        request: Request<ResumeSandboxRequest>,
    ) -> Result<Response<ResumeSandboxResponse>, Status> {
        NodeManagerService::resume_sandbox(self, request).await
    }

    async fn get_vm_state(
        &self,
        request: Request<crate::proto::GetVmStateRequest>,
    ) -> Result<Response<crate::proto::GetVmStateResponse>, Status> {
        NodeManagerService::get_vm_state(self, request)
            .await
            .map_err(runtime_operation_status)
    }

    async fn capture_vm_state(
        &self,
        request: Request<crate::proto::CaptureVmStateRequest>,
    ) -> Result<Response<crate::proto::CaptureVmStateResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc("CaptureVmState", sandbox_id, async {
            self.authorize_peer(&request, "CaptureVmState", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
            let (metadata, extensions, input) = request.into_parts();
            let input = crate::snapshot_request::StateCapture::try_from(input)?;
            NodeManagerService::capture_runtime_state(
                self,
                Request::from_parts(metadata, extensions, input),
            )
            .await
            .map_err(runtime_operation_status)?;
            Ok(Response::new(crate::proto::CaptureVmStateResponse {}))
        })
        .await
    }

    async fn restore_vm_state(
        &self,
        request: Request<crate::proto::RestoreVmStateRequest>,
    ) -> Result<Response<crate::proto::RestoreVmStateResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc("RestoreVmState", sandbox_id, async {
            self.authorize_peer(&request, "RestoreVmState", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
            let (metadata, extensions, input) = request.into_parts();
            let input = crate::snapshot_request::StateRestore::try_from(input)?;
            NodeManagerService::restore_runtime_state(
                self,
                Request::from_parts(metadata, extensions, input),
            )
            .await
            .map_err(runtime_operation_status)?;
            Ok(Response::new(crate::proto::RestoreVmStateResponse {}))
        })
        .await
    }

    async fn create_btrfs_snapshot(
        &self,
        request: Request<CreateBtrfsSnapshotRequest>,
    ) -> Result<Response<CreateBtrfsSnapshotResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc(
            "CreateBtrfsSnapshot",
            sandbox_id,
            NodeManagerService::create_btrfs_snapshot(self, request),
        )
        .await
    }

    async fn delete_btrfs_snapshot(
        &self,
        request: Request<DeleteBtrfsSnapshotRequest>,
    ) -> Result<Response<DeleteBtrfsSnapshotResponse>, Status> {
        NodeManagerService::delete_btrfs_snapshot(self, request).await
    }

    async fn delete_cache_artifact(
        &self,
        request: Request<DeleteCacheArtifactRequest>,
    ) -> Result<Response<DeleteCacheArtifactResponse>, Status> {
        NodeManagerService::delete_cache_artifact(self, request).await
    }

    async fn list_btrfs_snapshots(
        &self,
        request: Request<ListBtrfsSnapshotsRequest>,
    ) -> Result<Response<ListBtrfsSnapshotsResponse>, Status> {
        NodeManagerService::list_btrfs_snapshots(self, request).await
    }

    async fn rollback_btrfs_snapshot(
        &self,
        request: Request<RollbackBtrfsSnapshotRequest>,
    ) -> Result<Response<RollbackBtrfsSnapshotResponse>, Status> {
        NodeManagerService::rollback_btrfs_snapshot(self, request).await
    }

    async fn restore_instance_snapshot(
        &self,
        request: Request<RestoreInstanceSnapshotRequest>,
    ) -> Result<Response<RestoreInstanceSnapshotResponse>, Status> {
        NodeManagerService::restore_instance_snapshot(self, request).await
    }

    async fn get_disk_usage(
        &self,
        request: Request<GetDiskUsageRequest>,
    ) -> Result<Response<GetDiskUsageResponse>, Status> {
        NodeManagerService::get_disk_usage(self, request).await
    }

    async fn sync_guest(
        &self,
        request: Request<SyncGuestRequest>,
    ) -> Result<Response<SyncGuestResponse>, Status> {
        NodeManagerService::sync_guest(self, request).await
    }

    async fn update_cgroup(
        &self,
        request: Request<UpdateCgroupRequest>,
    ) -> Result<Response<UpdateCgroupResponse>, Status> {
        NodeManagerService::update_cgroup(self, request).await
    }

    async fn clone_sandbox_into_prepared_root(
        &self,
        request: Request<CloneSandboxRequest>,
    ) -> Result<Response<CloneSandboxResponse>, Status> {
        NodeManagerService::clone_sandbox_into_prepared_root(self, request).await
    }

    async fn expose_port(
        &self,
        request: Request<ExposePortRequest>,
    ) -> Result<Response<ExposePortResponse>, Status> {
        NodeManagerService::expose_port(self, request).await
    }

    async fn unexpose_port(
        &self,
        request: Request<UnexposePortRequest>,
    ) -> Result<Response<UnexposePortResponse>, Status> {
        NodeManagerService::unexpose_port(self, request).await
    }

    async fn allow_port(
        &self,
        request: Request<AllowPortRequest>,
    ) -> Result<Response<AllowPortResponse>, Status> {
        NodeManagerService::allow_port(self, request).await
    }

    async fn deny_port(
        &self,
        request: Request<DenyPortRequest>,
    ) -> Result<Response<DenyPortResponse>, Status> {
        NodeManagerService::deny_port(self, request).await
    }

    async fn publish_artifact(
        &self,
        request: Request<PublishArtifactRequest>,
    ) -> Result<Response<PublishArtifactResponse>, Status> {
        NodeManagerService::publish_artifact(self, request).await
    }

    async fn remove_artifact(
        &self,
        request: Request<RemoveArtifactRequest>,
    ) -> Result<Response<RemoveArtifactResponse>, Status> {
        NodeManagerService::remove_artifact(self, request).await
    }

    // ─── Btrfs delegation ─────────────────────────────────────────────

    async fn btrfs_create_template_subvolume(
        &self,
        request: Request<BtrfsCreateTemplateSubvolumeRequest>,
    ) -> Result<Response<BtrfsCreateTemplateSubvolumeResponse>, Status> {
        NodeManagerService::btrfs_create_template_subvolume(self, request).await
    }

    async fn btrfs_finalize_template_subvolume(
        &self,
        request: Request<BtrfsFinalizeTemplateSubvolumeRequest>,
    ) -> Result<Response<BtrfsFinalizeTemplateSubvolumeResponse>, Status> {
        NodeManagerService::btrfs_finalize_template_subvolume(self, request).await
    }

    async fn btrfs_delete_template_subvolume(
        &self,
        request: Request<BtrfsDeleteTemplateSubvolumeRequest>,
    ) -> Result<Response<BtrfsDeleteTemplateSubvolumeResponse>, Status> {
        NodeManagerService::btrfs_delete_template_subvolume(self, request).await
    }

    async fn btrfs_encode_rootfs_send(
        &self,
        request: Request<BtrfsEncodeRootfsSendRequest>,
    ) -> Result<Response<BtrfsEncodeRootfsSendResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc(
            "BtrfsEncodeRootfsSend",
            sandbox_id,
            NodeManagerService::btrfs_encode_rootfs_send(self, request),
        )
        .await
    }

    async fn btrfs_encode_snapshot_rootfs_send(
        &self,
        request: Request<BtrfsEncodeSnapshotRootfsSendRequest>,
    ) -> Result<Response<BtrfsEncodeSnapshotRootfsSendResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc(
            "BtrfsEncodeSnapshotRootfsSend",
            sandbox_id,
            NodeManagerService::btrfs_encode_snapshot_rootfs_send(self, request),
        )
        .await
    }

    async fn btrfs_materialize_rootfs_send(
        &self,
        request: Request<crate::proto::BtrfsMaterializeRootfsSendRequest>,
    ) -> Result<Response<BtrfsEncodeRootfsSendResponse>, Status> {
        NodeManagerService::btrfs_materialize_rootfs_send(self, request).await
    }

    async fn btrfs_inspect_send_source(
        &self,
        request: Request<crate::proto::BtrfsInspectSendSourceRequest>,
    ) -> Result<Response<crate::proto::BtrfsInspectSendSourceResponse>, Status> {
        NodeManagerService::btrfs_inspect_send_source(self, request).await
    }

    async fn get_btrfs_send_capabilities(
        &self,
        request: Request<crate::proto::GetBtrfsSendCapabilitiesRequest>,
    ) -> Result<Response<crate::proto::GetBtrfsSendCapabilitiesResponse>, Status> {
        NodeManagerService::get_btrfs_send_capabilities(self, request).await
    }

    type BtrfsReadSendSourceStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<crate::proto::BtrfsSendChunk, Status>> + Send>,
    >;

    async fn btrfs_stage_send_source(
        &self,
        request: Request<crate::proto::BtrfsStageSendSourceRequest>,
    ) -> Result<Response<crate::proto::BtrfsSendLease>, Status> {
        NodeManagerService::btrfs_stage_send_source(self, request).await
    }
    async fn btrfs_read_send_source(
        &self,
        request: Request<crate::proto::BtrfsSendLease>,
    ) -> Result<Response<Self::BtrfsReadSendSourceStream>, Status> {
        NodeManagerService::btrfs_read_send_source(self, request).await
    }
    async fn btrfs_delete_send_source(
        &self,
        request: Request<crate::proto::BtrfsSendLease>,
    ) -> Result<Response<crate::proto::BtrfsDeleteSendSourceResponse>, Status> {
        NodeManagerService::btrfs_delete_send_source(self, request).await
    }
    async fn btrfs_list_send_sources(
        &self,
        request: Request<crate::proto::BtrfsListSendSourcesRequest>,
    ) -> Result<Response<crate::proto::BtrfsListSendSourcesResponse>, Status> {
        NodeManagerService::btrfs_list_send_sources(self, request).await
    }

    async fn btrfs_receive_instance(
        &self,
        request: Request<BtrfsReceiveInstanceRequest>,
    ) -> Result<Response<BtrfsReceiveInstanceResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc(
            "BtrfsReceiveInstance",
            sandbox_id,
            NodeManagerService::btrfs_receive_instance(self, request),
        )
        .await
    }

    async fn prepare_snapshot_rootfs(
        &self,
        request: Request<PrepareSnapshotRootfsRequest>,
    ) -> Result<Response<PrepareSnapshotRootfsResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc(
            "PrepareSnapshotRootfs",
            sandbox_id,
            NodeManagerService::prepare_snapshot_rootfs(self, request),
        )
        .await
    }

    async fn btrfs_delete_sandbox_instance(
        &self,
        request: Request<BtrfsDeleteSandboxInstanceRequest>,
    ) -> Result<Response<BtrfsDeleteSandboxInstanceResponse>, Status> {
        NodeManagerService::btrfs_delete_sandbox_instance(self, request).await
    }

    async fn prepare_encrypted_rootfs(
        &self,
        request: Request<PrepareEncryptedRootfsRequest>,
    ) -> Result<Response<PrepareEncryptedRootfsResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc(
            "PrepareEncryptedRootfs",
            sandbox_id,
            NodeManagerService::prepare_encrypted_rootfs(self, request),
        )
        .await
    }

    async fn prepare_encrypted_volume(
        &self,
        request: Request<PrepareEncryptedVolumeRequest>,
    ) -> Result<Response<PrepareEncryptedVolumeResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc(
            "PrepareEncryptedVolume",
            sandbox_id,
            NodeManagerService::prepare_encrypted_volume(self, request),
        )
        .await
    }

    async fn release_encrypted_volume(
        &self,
        request: Request<ReleaseEncryptedVolumeRequest>,
    ) -> Result<Response<ReleaseEncryptedVolumeResponse>, Status> {
        let sandbox_id = request.get_ref().sandbox_id.clone();
        observe_rpc(
            "ReleaseEncryptedVolume",
            sandbox_id,
            NodeManagerService::release_encrypted_volume(self, request),
        )
        .await
    }
}
