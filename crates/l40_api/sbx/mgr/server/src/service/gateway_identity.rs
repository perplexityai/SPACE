//! Launch-time gateway isolation. Existing processes retain their identity
//! until the normal cycle/cleanup owner stops them.

use pplx_space_common_core::host::linux::gateway;
use pplx_space_common_core::types::LinuxUid;
use tonic::Status;

impl super::SandboxManagerService {
    /// Validate both the launch witness and the expected execution. A stale
    /// connected socket cannot authenticate solely by a recycled SO_PEERCRED PID.
    pub(super) async fn authorize_gateway_launch<T>(
        &self,
        request: &tonic::Request<T>,
        sandbox: &crate::db::SandboxRow,
        capability: &[u8],
        execution_generation: i64,
    ) -> Result<(), Status> {
        let peer = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>()
            .and_then(|info| info.peer_cred)
            .ok_or_else(|| Status::unauthenticated("gateway peer credentials missing"))?;
        let pid = peer
            .pid()
            .and_then(|pid| u32::try_from(pid).ok())
            .ok_or_else(|| Status::unauthenticated("gateway peer PID missing"))?;
        if capability.len() != 64
            || !capability.iter().all(u8::is_ascii_hexdigit)
            || sandbox
                .space_api_route_identity
                .execution_generation
                .is_none_or(|current| current.get() != execution_generation)
        {
            return Err(Status::permission_denied(
                "gateway launch or execution authority mismatch",
            ));
        }
        let mut client = self.gateway_caller_client.clone();
        let mut attestation = tonic::Request::new(
            pplx_space_node_mgr_client::proto::ValidateGatewayCallerRequest {
                sandbox_id: sandbox.id.clone(),
                pid,
                uid: peer.uid(),
                launch_capability: capability.to_vec(),
            },
        );
        attestation.set_timeout(std::time::Duration::from_secs(2));
        client
            .validate_gateway_caller(attestation)
            .await
            .map_err(|status| {
                if status.code() == tonic::Code::FailedPrecondition {
                    // Exec precedes publication of the launch witness. A reused PID
                    // can match an old DB registration during that interval, so a
                    // missing/mismatched witness is not proof of epoch revocation.
                    // No authority is returned; hydration retries with backoff.
                    Status::unavailable("gateway launch evidence is pending or changed")
                } else {
                    status
                }
            })?;
        Ok(())
    }
}

pub(crate) fn launch_uid(owner: u32) -> Result<u32, Status> {
    gateway::gateway_uid(LinuxUid::new(owner))
        .map(|uid| uid.get())
        .ok_or_else(|| {
            Status::failed_precondition(
                "gateway isolation requires a slot-derived sandbox identity",
            )
        })
}

#[cfg(test)]
pub(super) mod tests;
