//! Implements `space.cluster.api.v1.ClusterApi` by forwarding to the
//! `space.node.api.v1.ClusterApi` implementation via proto wire re-encoding.
//!
//! Request metadata and extensions (including the remote address used for
//! node-bound authentication) are preserved so the underlying handler sees
//! the correct peer IP.

use prost::Message;
use tonic::{Request, Response, Status};

use pplx_space_cluster_api_client::proto as new_proto;
use pplx_space_cluster_api_client::proto::cluster_api_server::ClusterApi as NewClusterApi;
use pplx_space_node_api_client::proto::cluster_api_server::ClusterApi as OldClusterApi;

use super::ClusterApiService;

fn forward_request<F, T>(request: Request<F>) -> Request<T>
where
    F: Message,
    T: Message + Default,
{
    let (metadata, extensions, msg) = request.into_parts();
    Request::from_parts(metadata, extensions, re_encode(msg))
}

/// Keeps the handler's response metadata and extensions along with the
/// re-encoded message.
fn forward_response<F, T>(response: Response<F>) -> Response<T>
where
    F: Message,
    T: Message + Default,
{
    let (metadata, msg, extensions) = response.into_parts();
    Response::from_parts(metadata, re_encode(msg), extensions)
}

fn re_encode<F: Message, T: Message + Default>(from: F) -> T {
    T::decode(from.encode_to_vec().as_slice())
        .expect("same-schema proto types always re-encode successfully")
}

macro_rules! impl_new_cluster_api_forwarding {
    ( $( $method:ident ( $req:ident ) -> $resp:ident ; )* ) => {
        #[tonic::async_trait]
        impl NewClusterApi for ClusterApiService {
            $(
                async fn $method(
                    &self,
                    request: Request<new_proto::$req>,
                ) -> Result<Response<new_proto::$resp>, Status> {
                    let old_req = forward_request::<_, crate::proto::$req>(request);
                    let old_resp = OldClusterApi::$method(self, old_req).await?;
                    Ok(forward_response(old_resp))
                }
            )*
        }
    };
}

impl_new_cluster_api_forwarding! {
    list_nodes(ListNodesRequest) -> ListNodesResponse;
    create_system_template_publication_grant(CreateSystemTemplatePublicationGrantRequest) -> CreateSystemTemplatePublicationGrantResponse;
    register_template(RegisterTemplateRequest) -> RegisterTemplateResponse;
    register_template_release(RegisterTemplateReleaseRequest) -> RegisterTemplateReleaseResponse;
    find_template_release(FindTemplateReleaseRequest) -> FindTemplateReleaseResponse;
    find_template_release_by_hash(FindTemplateReleaseByHashRequest) -> FindTemplateReleaseResponse;
    list_latest_template_releases(ListLatestTemplateReleasesRequest) -> ListLatestTemplateReleasesResponse;
    list_warm_pool_template_releases(ListWarmPoolTemplateReleasesRequest) -> ListWarmPoolTemplateReleasesResponse;
    get_node_network_policy_state(GetNodeNetworkPolicyStateRequest) -> GetNodeNetworkPolicyStateResponse;
    get_node_template_base_snapshot_policy_state(GetNodeTemplateBaseSnapshotPolicyStateRequest) -> GetNodeTemplateBaseSnapshotPolicyStateResponse;
    get_node_template_desired_state(GetNodeTemplateDesiredStateRequest) -> GetNodeTemplateDesiredStateResponse;
    get_node_sandbox_desired_state(GetNodeSandboxDesiredStateRequest) -> GetNodeSandboxDesiredStateResponse;
    get_node_runtime_desired_state(GetNodeRuntimeDesiredStateRequest) -> GetNodeRuntimeDesiredStateResponse;
    resolve_data_plane_session_route(ResolveDataPlaneSessionRouteRequest) -> ResolveDataPlaneSessionRouteResponse;
    bind_data_plane_session_process(BindDataPlaneSessionProcessRequest) -> BindDataPlaneSessionProcessResponse;
    record_data_plane_session_reconnect(RecordDataPlaneSessionReconnectRequest) -> RecordDataPlaneSessionReconnectResponse;
    get_data_plane_session_revocation_state(GetDataPlaneSessionRevocationStateRequest) -> GetDataPlaneSessionRevocationStateResponse;
    get_data_plane_org_revocation_epoch(GetDataPlaneOrgRevocationEpochRequest) -> GetDataPlaneOrgRevocationEpochResponse;
    get_sandbox_runtime_access(GetSandboxRuntimeAccessRequest) -> GetSandboxRuntimeAccessResponse;
    unstable_resolve_sandbox_service_route(UnstableResolveSandboxServiceRouteRequest) -> UnstableResolveSandboxServiceRouteResponse;
    unstable_reset_sandbox_service_route(UnstableResetSandboxServiceRouteRequest) -> UnstableResetSandboxServiceRouteResponse;
    claim_node_reconcile_refs(ClaimNodeReconcileRefsRequest) -> ClaimNodeReconcileRefsResponse;
    complete_node_reconcile_refs(CompleteNodeReconcileRefsRequest) -> CompleteNodeReconcileRefsResponse;
    publish_sandbox_port_exposure_states(PublishSandboxPortExposureStatesRequest) -> PublishSandboxPortExposureStatesResponse;
    publish_node_state(PublishNodeStateRequest) -> PublishNodeStateResponse;
    begin_artifact_backup(BeginArtifactBackupRequest) -> BeginArtifactBackupResponse;
    finalize_artifact_backup(FinalizeArtifactBackupRequest) -> FinalizeArtifactBackupResponse;
    touch_artifact_backup(TouchArtifactBackupRequest) -> TouchArtifactBackupResponse;
    get_artifact_backup_location(GetArtifactBackupLocationRequest) -> GetArtifactBackupLocationResponse;
    get_quarantine_recovery_plan(GetQuarantineRecoveryPlanRequest) -> GetQuarantineRecoveryPlanResponse;
    get_sandbox_snapshot_storage_uri(GetSandboxSnapshotStorageUriRequest) -> GetSandboxSnapshotStorageUriResponse;
    get_suspend_snapshot_location(GetSuspendSnapshotLocationRequest) -> GetSuspendSnapshotLocationResponse;
    get_execution_currency(GetExecutionCurrencyRequest) -> GetExecutionCurrencyResponse;
}

#[cfg(test)]
mod tests;
