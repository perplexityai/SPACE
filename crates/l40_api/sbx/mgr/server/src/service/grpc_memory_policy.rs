use super::*;

impl SandboxManagerService {
    pub(super) async fn resolve_sandbox_memory(
        &self,
        request: Request<ResolveSandboxMemoryRequest>,
    ) -> Result<Response<ResolveSandboxMemoryResponse>, Status> {
        self.authorize_peer(
            &request,
            "ResolveSandboxMemory",
            ALLOW_SANDBOX_OBSERVER_OR_TMPLMGR,
        )?;
        let requested_memory_mib = request.get_ref().requested_memory_mib;
        if requested_memory_mib == 0 {
            return Err(Status::invalid_argument(
                "requested_memory_mib must be greater than 0",
            ));
        }

        let resolved = self
            .resolve_sandbox_memory_policy_for_request(requested_memory_mib, request.metadata())
            .await?;
        Ok(Response::new(ResolveSandboxMemoryResponse {
            requested_memory_mib: resolved.requested_memory_mib,
            effective_memory_mib: resolved.effective_memory_mib,
            cgroup_memory_mib: resolved.cgroup_memory_mib,
            guest_memory_max_mib: resolved.guest_memory_max_mib,
            cgroup_memory_max_mib: resolved.cgroup_memory_max_mib,
            memory_headroom_mib: resolved.memory_headroom_mib,
        }))
    }
}
