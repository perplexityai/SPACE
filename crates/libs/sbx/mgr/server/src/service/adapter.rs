use super::*;
use crate::proto::{CleanupTargetBatch, ScanCleanupTargetsRequest};

#[tonic::async_trait]
impl SandboxManager for SandboxManagerService {
    async fn get_execution_capabilities(
        &self,
        request: Request<crate::proto::GetExecutionCapabilitiesRequest>,
    ) -> Result<Response<crate::proto::GetExecutionCapabilitiesResponse>, Status> {
        self.authorize_peer(
            &request,
            "GetExecutionCapabilities",
            ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR,
        )?;
        Ok(Response::new(
            crate::proto::GetExecutionCapabilitiesResponse {
                environment_selection: true,
            },
        ))
    }

    type SpacedListStream = BoxedStatusStream<SpacedFileInfo>;
    type SpacedReadStream = BoxedStatusStream<SpacedFileChunk>;
    type SpacedWatchStream = BoxedStatusStream<SpacedFileEvent>;
    type SpacedStartProcessStream = BoxedStatusStream<SpacedProcessEvent>;
    type SpacedConnectProcessStream = BoxedStatusStream<SpacedProcessEvent>;
    type ServicePortIngressStream = BoxedStatusStream<ServicePortIngressFrame>;
    type ExportSnapshotStream = SbxExportSnapshotStream;
    type ExportSandboxStream = SbxExportSandboxStream;
    type ScanCleanupTargetsStream = BoxedStatusStream<CleanupTargetBatch>;

    async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        if let Some(sandbox_id) = request.get_ref().sandbox_id.as_deref() {
            record_entry_sandbox_id(sandbox_id);
        }
        let response = SandboxManagerService::create(self, request).await?;
        record_entry_sandbox_id(&response.get_ref().sandbox_id);
        Ok(response)
    }

    async fn start(
        &self,
        request: Request<StartRequest>,
    ) -> Result<Response<StartResponse>, Status> {
        record_entry_sandbox_id(&request.get_ref().sandbox_id);
        SandboxManagerService::start(self, request).await
    }

    async fn stop(&self, request: Request<StopRequest>) -> Result<Response<StopResponse>, Status> {
        record_entry_sandbox_id(&request.get_ref().sandbox_id);
        SandboxManagerService::stop(self, request).await
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        record_entry_sandbox_id(&request.get_ref().sandbox_id);
        SandboxManagerService::delete(self, request).await
    }

    async fn list_auto_lifecycle_delete_receipts(
        &self,
        request: Request<ListAutoLifecycleDeleteReceiptsRequest>,
    ) -> Result<Response<ListAutoLifecycleDeleteReceiptsResponse>, Status> {
        SandboxManagerService::list_auto_lifecycle_delete_receipts(self, request).await
    }

    async fn acknowledge_auto_lifecycle_delete_receipts(
        &self,
        request: Request<AcknowledgeAutoLifecycleDeleteReceiptsRequest>,
    ) -> Result<Response<AcknowledgeAutoLifecycleDeleteReceiptsResponse>, Status> {
        SandboxManagerService::acknowledge_auto_lifecycle_delete_receipts(self, request).await
    }

    async fn list_pending_local_backups(
        &self,
        request: Request<ListPendingLocalBackupsRequest>,
    ) -> Result<Response<ListPendingLocalBackupsResponse>, Status> {
        SandboxManagerService::list_pending_local_backups(self, request).await
    }

    async fn list(&self, request: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        SandboxManagerService::list(self, request).await
    }

    async fn scan_cleanup_targets(
        &self,
        request: Request<ScanCleanupTargetsRequest>,
    ) -> Result<Response<Self::ScanCleanupTargetsStream>, Status> {
        SandboxManagerService::scan_cleanup_targets(self, request).await
    }

    async fn list_build_referencing_sandboxes(
        &self,
        request: Request<ListBuildReferencingSandboxesRequest>,
    ) -> Result<Response<ListBuildReferencingSandboxesResponse>, Status> {
        SandboxManagerService::list_build_referencing_sandboxes(self, request).await
    }

    async fn get_disk_usage(
        &self,
        request: Request<GetDiskUsageRequest>,
    ) -> Result<Response<GetDiskUsageResponse>, Status> {
        SandboxManagerService::get_disk_usage(self, request).await
    }

    async fn cleanup(
        &self,
        request: Request<CleanupRequest>,
    ) -> Result<Response<CleanupResponse>, Status> {
        SandboxManagerService::cleanup(self, request).await
    }

    async fn list_templates(
        &self,
        request: Request<ListTemplatesRequest>,
    ) -> Result<Response<ListTemplatesResponse>, Status> {
        SandboxManagerService::list_templates(self, request).await
    }

    async fn pause(
        &self,
        request: Request<PauseRequest>,
    ) -> Result<Response<PauseResponse>, Status> {
        record_entry_sandbox_id(&request.get_ref().sandbox_id);
        SandboxManagerService::pause(self, request).await
    }

    async fn resume(
        &self,
        request: Request<ResumeRequest>,
    ) -> Result<Response<ResumeResponse>, Status> {
        record_entry_sandbox_id(&request.get_ref().sandbox_id);
        SandboxManagerService::resume(self, request).await
    }

    async fn wake(&self, request: Request<WakeRequest>) -> Result<Response<WakeResponse>, Status> {
        record_entry_sandbox_id(&request.get_ref().sandbox_id);
        SandboxManagerService::wake(self, request).await
    }

    async fn set_auto_lifecycle_policy(
        &self,
        request: Request<SetAutoLifecyclePolicyRequest>,
    ) -> Result<Response<SetAutoLifecyclePolicyResponse>, Status> {
        SandboxManagerService::set_auto_lifecycle_policy(self, request).await
    }

    async fn touch_auto_lifecycle(
        &self,
        request: Request<TouchAutoLifecycleRequest>,
    ) -> Result<Response<TouchAutoLifecycleResponse>, Status> {
        SandboxManagerService::touch_auto_lifecycle(self, request).await
    }

    async fn get_network_policy(
        &self,
        request: Request<GetNetworkPolicyRequest>,
    ) -> Result<Response<GetNetworkPolicyResponse>, Status> {
        SandboxManagerService::get_network_policy(self, request).await
    }

    async fn set_network_policy(
        &self,
        request: Request<SetNetworkPolicyRequest>,
    ) -> Result<Response<SetNetworkPolicyResponse>, Status> {
        SandboxManagerService::set_network_policy(self, request).await
    }

    async fn template_network_policy_updated(
        &self,
        request: Request<TemplateNetworkPolicyUpdatedRequest>,
    ) -> Result<Response<TemplateNetworkPolicyUpdatedResponse>, Status> {
        SandboxManagerService::template_network_policy_updated(self, request).await
    }

    async fn cycle_net_gateway(
        &self,
        request: Request<crate::proto::CycleNetGatewayRequest>,
    ) -> Result<Response<crate::proto::CycleNetGatewayResponse>, Status> {
        SandboxManagerService::cycle_net_gateway(self, request).await
    }

    async fn suspend(
        &self,
        request: Request<SuspendRequest>,
    ) -> Result<Response<SuspendResponse>, Status> {
        record_entry_sandbox_id(&request.get_ref().sandbox_id);
        SandboxManagerService::suspend(self, request).await
    }

    async fn prepare_teardown_suspend(
        &self,
        request: Request<PrepareTeardownSuspendRequest>,
    ) -> Result<Response<PrepareTeardownSuspendResponse>, Status> {
        SandboxManagerService::prepare_teardown_suspend(self, request).await
    }

    async fn enqueue_artifact_backup(
        &self,
        request: Request<EnqueueArtifactBackupRequest>,
    ) -> Result<Response<EnqueueArtifactBackupResponse>, Status> {
        SandboxManagerService::enqueue_artifact_backup(self, request).await
    }

    async fn evict_cluster_deleted_sandbox(
        &self,
        request: Request<crate::proto::EvictClusterDeletedSandboxRequest>,
    ) -> Result<Response<crate::proto::EvictClusterDeletedSandboxResponse>, Status> {
        SandboxManagerService::evict_cluster_deleted_sandbox(self, request).await
    }

    async fn kill(&self, request: Request<KillRequest>) -> Result<Response<KillResponse>, Status> {
        record_entry_sandbox_id(&request.get_ref().sandbox_id);
        SandboxManagerService::kill(self, request).await
    }

    async fn state(
        &self,
        request: Request<StateRequest>,
    ) -> Result<Response<StateResponse>, Status> {
        SandboxManagerService::state(self, request).await
    }

    async fn get_sandbox_health(
        &self,
        request: Request<GetSandboxHealthRequest>,
    ) -> Result<Response<GetSandboxHealthResponse>, Status> {
        SandboxManagerService::get_sandbox_health(self, request).await
    }

    async fn get_env(
        &self,
        request: Request<GetEnvRequest>,
    ) -> Result<Response<GetEnvResponse>, Status> {
        SandboxManagerService::get_env(self, request).await
    }

    async fn create_snapshot(
        &self,
        request: Request<crate::proto::CreateSnapshotRequest>,
    ) -> Result<Response<crate::proto::CreateSnapshotResponse>, Status> {
        SandboxManagerService::create_snapshot(self, request).await
    }

    async fn wait_template_apps(
        &self,
        request: Request<crate::proto::WaitTemplateAppsRequest>,
    ) -> Result<Response<crate::proto::WaitTemplateAppsResponse>, Status> {
        SandboxManagerService::wait_template_apps(self, request).await
    }

    async fn create_template_base_snapshot(
        &self,
        request: Request<CreateTemplateBaseSnapshotRequest>,
    ) -> Result<Response<CreateTemplateBaseSnapshotResponse>, Status> {
        SandboxManagerService::create_template_base_snapshot(self, request).await
    }

    async fn resolve_sandbox_memory(
        &self,
        request: Request<ResolveSandboxMemoryRequest>,
    ) -> Result<Response<ResolveSandboxMemoryResponse>, Status> {
        SandboxManagerService::resolve_sandbox_memory(self, request).await
    }

    async fn stats(
        &self,
        request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        SandboxManagerService::stats(self, request).await
    }

    async fn update(
        &self,
        request: Request<UpdateRequest>,
    ) -> Result<Response<UpdateResponse>, Status> {
        SandboxManagerService::update(self, request).await
    }

    async fn spaced_ping_guest(
        &self,
        request: Request<SpacedPingGuestRequest>,
    ) -> Result<Response<SpacedPingGuestResponse>, Status> {
        SandboxManagerService::spaced_ping_guest(self, request).await
    }

    async fn spaced_get_status(
        &self,
        request: Request<SpacedGetStatusRequest>,
    ) -> Result<Response<SpacedGetStatusResponse>, Status> {
        SandboxManagerService::spaced_get_status(self, request).await
    }

    async fn spaced_wait_ready(
        &self,
        request: Request<SpacedWaitReadyRequest>,
    ) -> Result<Response<SpacedWaitReadyResponse>, Status> {
        SandboxManagerService::spaced_wait_ready(self, request).await
    }

    async fn spaced_stat(
        &self,
        request: Request<SpacedStatRequest>,
    ) -> Result<Response<SpacedStatResponse>, Status> {
        SandboxManagerService::spaced_stat(self, request).await
    }

    async fn spaced_list(
        &self,
        request: Request<SpacedListRequest>,
    ) -> Result<Response<Self::SpacedListStream>, Status> {
        SandboxManagerService::spaced_list(self, request).await
    }

    async fn spaced_read(
        &self,
        request: Request<SpacedReadRequest>,
    ) -> Result<Response<Self::SpacedReadStream>, Status> {
        SandboxManagerService::spaced_read(self, request).await
    }

    async fn spaced_write(
        &self,
        request: Request<tonic::Streaming<SpacedWriteRequest>>,
    ) -> Result<Response<SpacedWriteResponse>, Status> {
        SandboxManagerService::spaced_write(self, request).await
    }

    async fn spaced_write_batch(
        &self,
        request: Request<SpacedWriteBatchRequest>,
    ) -> Result<Response<SpacedWriteBatchResponse>, Status> {
        SandboxManagerService::spaced_write_batch(self, request).await
    }

    async fn spaced_mkdir(
        &self,
        request: Request<SpacedMkdirRequest>,
    ) -> Result<Response<SpacedMkdirResponse>, Status> {
        SandboxManagerService::spaced_mkdir(self, request).await
    }

    async fn spaced_remove(
        &self,
        request: Request<SpacedRemoveRequest>,
    ) -> Result<Response<SpacedRemoveResponse>, Status> {
        SandboxManagerService::spaced_remove(self, request).await
    }

    async fn spaced_rename(
        &self,
        request: Request<SpacedRenameRequest>,
    ) -> Result<Response<SpacedRenameResponse>, Status> {
        SandboxManagerService::spaced_rename(self, request).await
    }

    async fn spaced_watch(
        &self,
        request: Request<SpacedWatchRequest>,
    ) -> Result<Response<Self::SpacedWatchStream>, Status> {
        SandboxManagerService::spaced_watch(self, request).await
    }

    async fn spaced_create_watcher(
        &self,
        request: Request<SpacedCreateWatcherRequest>,
    ) -> Result<Response<SpacedCreateWatcherResponse>, Status> {
        SandboxManagerService::spaced_create_watcher(self, request).await
    }

    async fn spaced_normalize_watch_root(
        &self,
        request: Request<SpacedNormalizeWatchRootRequest>,
    ) -> Result<Response<SpacedNormalizeWatchRootResponse>, Status> {
        SandboxManagerService::spaced_normalize_watch_root(self, request).await
    }

    async fn spaced_get_watcher_events(
        &self,
        request: Request<SpacedGetWatcherEventsRequest>,
    ) -> Result<Response<SpacedGetWatcherEventsResponse>, Status> {
        SandboxManagerService::spaced_get_watcher_events(self, request).await
    }

    async fn spaced_remove_watcher(
        &self,
        request: Request<SpacedRemoveWatcherRequest>,
    ) -> Result<Response<SpacedRemoveWatcherResponse>, Status> {
        SandboxManagerService::spaced_remove_watcher(self, request).await
    }

    async fn spaced_enable_file_changes(
        &self,
        request: Request<SpacedEnableFileChangesRequest>,
    ) -> Result<Response<SpacedEnableFileChangesResponse>, Status> {
        SandboxManagerService::spaced_enable_file_changes(self, request).await
    }

    async fn spaced_disable_file_changes(
        &self,
        request: Request<SpacedDisableFileChangesRequest>,
    ) -> Result<Response<SpacedDisableFileChangesResponse>, Status> {
        SandboxManagerService::spaced_disable_file_changes(self, request).await
    }

    async fn spaced_get_modified_files(
        &self,
        request: Request<SpacedGetModifiedFilesRequest>,
    ) -> Result<Response<SpacedGetModifiedFilesResponse>, Status> {
        SandboxManagerService::spaced_get_modified_files(self, request).await
    }

    async fn spaced_start_process(
        &self,
        request: Request<SpacedStartProcessRequest>,
    ) -> Result<Response<Self::SpacedStartProcessStream>, Status> {
        SandboxManagerService::spaced_start_process(self, request).await
    }

    async fn spaced_connect_process(
        &self,
        request: Request<SpacedConnectProcessRequest>,
    ) -> Result<Response<Self::SpacedConnectProcessStream>, Status> {
        SandboxManagerService::spaced_connect_process(self, request).await
    }

    async fn spaced_list_processes(
        &self,
        request: Request<SpacedListProcessesRequest>,
    ) -> Result<Response<SpacedListProcessesResponse>, Status> {
        SandboxManagerService::spaced_list_processes(self, request).await
    }

    async fn spaced_send_signal(
        &self,
        request: Request<SpacedSendSignalRequest>,
    ) -> Result<Response<SpacedSendSignalResponse>, Status> {
        SandboxManagerService::spaced_send_signal(self, request).await
    }

    async fn spaced_send_input(
        &self,
        request: Request<SpacedSendInputRequest>,
    ) -> Result<Response<SpacedSendInputResponse>, Status> {
        SandboxManagerService::spaced_send_input(self, request).await
    }

    async fn spaced_close_stdin(
        &self,
        request: Request<SpacedCloseStdinRequest>,
    ) -> Result<Response<SpacedCloseStdinResponse>, Status> {
        SandboxManagerService::spaced_close_stdin(self, request).await
    }

    async fn spaced_resize_pty(
        &self,
        request: Request<SpacedResizePtyRequest>,
    ) -> Result<Response<SpacedResizePtyResponse>, Status> {
        SandboxManagerService::spaced_resize_pty(self, request).await
    }

    async fn spaced_kill_process(
        &self,
        request: Request<SpacedKillProcessRequest>,
    ) -> Result<Response<SpacedKillProcessResponse>, Status> {
        SandboxManagerService::spaced_kill_process(self, request).await
    }

    async fn spaced_open_loopback_bridge(
        &self,
        request: Request<SpacedOpenLoopbackBridgeRequest>,
    ) -> Result<Response<SpacedOpenLoopbackBridgeResponse>, Status> {
        SandboxManagerService::spaced_open_loopback_bridge(self, request).await
    }

    async fn spaced_close_loopback_bridge(
        &self,
        request: Request<SpacedCloseLoopbackBridgeRequest>,
    ) -> Result<Response<SpacedCloseLoopbackBridgeResponse>, Status> {
        SandboxManagerService::spaced_close_loopback_bridge(self, request).await
    }

    async fn service_port_ingress(
        &self,
        request: Request<tonic::Streaming<ServicePortIngressFrame>>,
    ) -> Result<Response<Self::ServicePortIngressStream>, Status> {
        SandboxManagerService::service_port_ingress(self, request).await
    }

    async fn enable_snapshots(
        &self,
        request: Request<EnableSnapshotsRequest>,
    ) -> Result<Response<EnableSnapshotsResponse>, Status> {
        SandboxManagerService::enable_snapshots(self, request).await
    }

    async fn disable_snapshots(
        &self,
        request: Request<DisableSnapshotsRequest>,
    ) -> Result<Response<DisableSnapshotsResponse>, Status> {
        SandboxManagerService::disable_snapshots(self, request).await
    }

    async fn list_snapshots(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        SandboxManagerService::list_snapshots(self, request).await
    }

    async fn delete_snapshots(
        &self,
        request: Request<DeleteSnapshotsRequest>,
    ) -> Result<Response<DeleteSnapshotsResponse>, Status> {
        SandboxManagerService::delete_snapshots(self, request).await
    }

    async fn rollback(
        &self,
        request: Request<RollbackRequest>,
    ) -> Result<Response<RollbackResponse>, Status> {
        record_entry_sandbox_id(&request.get_ref().sandbox_id);
        SandboxManagerService::rollback(self, request).await
    }

    async fn adopt_pool_sandbox(
        &self,
        request: Request<AdoptPoolSandboxRequest>,
    ) -> Result<Response<AdoptPoolSandboxResponse>, Status> {
        let response = SandboxManagerService::adopt_pool_sandbox(self, request).await?;
        record_entry_sandbox_id(&response.get_ref().sandbox_id);
        Ok(response)
    }

    async fn wait(&self, request: Request<WaitRequest>) -> Result<Response<WaitResponse>, Status> {
        SandboxManagerService::wait(self, request).await
    }

    async fn clone_from_donor(
        &self,
        request: Request<CloneFromDonorRequest>,
    ) -> Result<Response<CloneFromDonorResponse>, Status> {
        SandboxManagerService::clone_from_donor(self, request).await
    }

    async fn export_snapshot(
        &self,
        request: Request<ExportSnapshotRequest>,
    ) -> Result<Response<Self::ExportSnapshotStream>, Status> {
        SandboxManagerService::export_snapshot(self, request).await
    }

    async fn import_snapshot(
        &self,
        request: Request<tonic::Streaming<ImportSnapshotRequest>>,
    ) -> Result<Response<ImportSnapshotResponse>, Status> {
        SandboxManagerService::import_snapshot(self, request).await
    }

    async fn export_sandbox(
        &self,
        request: Request<ExportSandboxRequest>,
    ) -> Result<Response<Self::ExportSandboxStream>, Status> {
        SandboxManagerService::export_sandbox(self, request).await
    }

    async fn import_sandbox(
        &self,
        request: Request<tonic::Streaming<ImportSandboxRequest>>,
    ) -> Result<Response<ImportSandboxResponse>, Status> {
        SandboxManagerService::import_sandbox(self, request).await
    }
}

/// Record the sandbox a lifecycle call targets on the call's own `grpc.server`
/// span, the current span while a handler runs, so a search by `sandbox_id`
/// finds the request and not only its children.
fn record_entry_sandbox_id(sandbox_id: &str) {
    pplx_space_common_svc::record_sandbox_id(&tracing::Span::current(), sandbox_id);
}

#[cfg(test)]
mod tests;
