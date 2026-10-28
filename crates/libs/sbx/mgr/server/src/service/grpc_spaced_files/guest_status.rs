//! Guest readiness and supported execution capabilities.

use super::*;

pub(super) fn supported_process_execution_info(
    info: Option<pplx_space_spaced_host_client::proto::ProcessExecutionInfo>,
) -> Option<pplx_space_spaced_host_client::proto::ProcessExecutionInfo> {
    info.map(|mut info| {
        // Advertise only guarantees preserved by this manager's request adapters.
        info.capabilities.retain(|capability| {
            use pplx_space_common_grpc::GuestCapability;
            matches!(
                capability.parse::<GuestCapability>(),
                Ok(GuestCapability::ProcessOutputGapResume
                    | GuestCapability::ProcessRecoveryStart
                    | GuestCapability::ProcessRecoveryHandles
                    | GuestCapability::ProcessMutationReceipts)
            )
        });
        info
    })
}

pub(super) fn spaced_status_response(
    status: pplx_space_spaced_host_client::proto::GetStatusResponse,
) -> SpacedGetStatusResponse {
    SpacedGetStatusResponse {
        process_execution: supported_process_execution_info(status.process_execution),
        ready: status.ready,
        guest_uptime: status.guest_uptime,
        host_version: spaced_version_info(status.host_version),
        guest_version: spaced_version_info(status.guest_version),
    }
}

pub(super) fn spaced_ready_response(
    response: pplx_space_spaced_host_client::proto::WaitReadyResponse,
) -> SpacedWaitReadyResponse {
    SpacedWaitReadyResponse {
        process_execution: supported_process_execution_info(response.process_execution),
        guest_uptime: response.guest_uptime,
    }
}

impl SandboxManagerService {
    pub(in crate::service) async fn spaced_ping_guest(
        &self,
        request: Request<SpacedPingGuestRequest>,
    ) -> Result<Response<SpacedPingGuestResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::PingGuest.as_str(),
            ALLOW_SANDBOX_OBSERVER,
        )?;
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::PingGuest)?;
        let sandbox = self.lookup_started_sandbox(&req.sandbox_id)?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        client.ping_guest().await?;
        Ok(Response::new(SpacedPingGuestResponse {}))
    }

    pub(in crate::service) async fn spaced_get_status(
        &self,
        request: Request<SpacedGetStatusRequest>,
    ) -> Result<Response<SpacedGetStatusResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::GetStatus.as_str(),
            ALLOW_SANDBOX_OBSERVER,
        )?;
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::GetStatus)?;
        let sandbox = self.lookup_started_sandbox(&req.sandbox_id)?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        let status = client.get_status().await?;
        Ok(Response::new(spaced_status_response(status)))
    }

    pub(in crate::service) async fn spaced_wait_ready(
        &self,
        request: Request<SpacedWaitReadyRequest>,
    ) -> Result<Response<SpacedWaitReadyResponse>, Status> {
        let peer = self.authorize_peer(
            &request,
            SpacedRpc::WaitReady.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let req = request.get_ref();
        let route_auth = self.prepare_classified_space_api_bridge(
            &request,
            &req.sandbox_id,
            SpacedRpc::WaitReady,
            &peer,
        )?;
        let sandbox = self.lookup_started_sandbox(&req.sandbox_id)?;
        if let Some(route_auth) = &route_auth {
            route_auth.authorize_sandbox(&sandbox)?;
        }
        let endpoint_proof_policy = if route_auth.is_none() {
            SpacedEndpointProofPolicy::AllowIdentityFreePoolDonor
        } else {
            SpacedEndpointProofPolicy::Required
        };
        let timeout = proto_duration_to_std(req.timeout)
            .unwrap_or_else(|| Duration::from_secs(DEFAULT_BLOCKING_READY_TIMEOUT_SECS as u64));
        let response = self
            .legacy_recovery_ready(
                &sandbox,
                route_auth.as_ref(),
                endpoint_proof_policy,
                timeout,
            )
            .await?;
        Ok(Response::new(spaced_ready_response(response)))
    }
}
