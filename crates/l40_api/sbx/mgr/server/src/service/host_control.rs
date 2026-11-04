use super::*;
use pplx_space_common_grpc::{
    DEFAULT_GRPC_CONNECT_RETRY_POLICY, DEFAULT_LOCAL_GRPC_RETRY_POLICY, GrpcRetryContext,
};
use pplx_space_credential_injection_core::proto::ResolvedNetworkCredential;
use pplx_space_credmgr_client::{
    NETWORK_CREDENTIAL_RESOLUTION_TIMEOUT, proto::ResolveNetworkCredentialsRequest,
};
use pplx_space_net_common::EffectiveNetworkPolicyRef;

use super::network_policy::EffectiveNetworkPolicy;

// Add local authorization and transport headroom to the owning downstream
// budgets. Shorter caller deadlines win.
const CREDENTIAL_INJECTION_TIMEOUT: Duration = DEFAULT_GRPC_CONNECT_RETRY_POLICY
    .max_elapsed
    .saturating_add(NETWORK_CREDENTIAL_RESOLUTION_TIMEOUT)
    .saturating_add(Duration::from_secs(5));

// Credential Manager owns a 15-second provider-resolution budget. The local
// call allows response-forwarding headroom while remaining inside the complete
// GetNetworkPolicy budget above. DEFAULT_LOCAL_GRPC_RETRY_POLICY still bounds
// retries of fast transport failures; it must not bound one in-flight RPC.
const NETWORK_CREDENTIAL_PULL_TIMEOUT: Duration =
    NETWORK_CREDENTIAL_RESOLUTION_TIMEOUT.saturating_add(Duration::from_secs(5));

fn credential_resolution_deadline<T>(request: &Request<T>) -> (tokio::time::Instant, bool) {
    let started = tokio::time::Instant::now();
    let caller_timeout = request
        .metadata()
        .get("grpc-timeout")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_grpc_timeout)
        .filter(|timeout| *timeout <= CREDENTIAL_INJECTION_TIMEOUT);
    (
        started + caller_timeout.unwrap_or(CREDENTIAL_INJECTION_TIMEOUT),
        caller_timeout.is_some(),
    )
}

struct ResolvedGatewayNetworkPolicy {
    sandbox: crate::db::SandboxRow,
    effective_policy: EffectiveNetworkPolicy,
    policy_ref: EffectiveNetworkPolicyRef,
    network_credentials: Vec<ResolvedNetworkCredential>,
}

impl ResolvedGatewayNetworkPolicy {
    fn into_response(self) -> HostControlGetNetworkPolicyResponse {
        HostControlGetNetworkPolicyResponse {
            effective_policy: Some(super::network_policy::network_policy_to_proto(
                &self.effective_policy.policy,
            )),
            inherited: self.effective_policy.inherited,
            network_tx_bytes: self.sandbox.network_tx_bytes,
            network_rx_bytes: self.sandbox.network_rx_bytes,
            effective_policy_ref: self.policy_ref.to_string(),
            network_credentials: self.network_credentials,
        }
    }
}

impl SandboxManagerService {
    async fn authenticate_gateway_policy_request(
        &self,
        request: Request<HostControlGetNetworkPolicyRequest>,
    ) -> Result<crate::db::SandboxRow, Status> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.authenticated_net_gateway_host_control_sandbox(&request)
        })
        .await
        .map_err(|error| Status::internal(format!("gateway authorization task failed: {error}")))?
    }

    /// Pull credential material within Credential Manager's provider budget.
    /// The caller owns the larger end-to-end deadline, including connection
    /// establishment and policy lookup.
    async fn fetch_injection_rules(
        &self,
        sandbox: &crate::db::SandboxRow,
        policy_ref: &EffectiveNetworkPolicyRef,
    ) -> Result<Vec<ResolvedNetworkCredential>, Status> {
        if !self.network_credentials_enabled {
            return Ok(Vec::new());
        }

        let pull = pplx_space_metrics::reconcile::network_credential_pull();
        let execution_generation = sandbox
            .space_api_route_identity
            .execution_generation
            .ok_or_else(|| Status::failed_precondition("sandbox has no current execution"))?;
        let sandbox_id: SandboxId = sandbox
            .id
            .parse()
            .map_err(|_| Status::internal("stored sandbox ID is malformed"))?;
        let client = self
            .credential_manager_client
            .get_or_try_init(|| self.credential_manager_endpoint.connect())
            .await
            .map_err(|_| Status::unavailable("node credential agent is unavailable"))?
            .clone();
        let request = ResolveNetworkCredentialsRequest {
            sandbox_id: sandbox_id.to_string(),
            execution_generation: execution_generation.get(),
            effective_network_policy_ref: policy_ref.to_string(),
        };
        let credentials = pplx_space_common_grpc::retry_grpc_status_with_context(
            "credential-manager",
            "ResolveNetworkCredentials",
            DEFAULT_LOCAL_GRPC_RETRY_POLICY,
            GrpcRetryContext::with_timeout(NETWORK_CREDENTIAL_PULL_TIMEOUT),
            || {
                let mut client = client.clone();
                let request = request.clone();
                async move {
                    client
                        .resolve_network_credentials(request)
                        .await
                        .map(Response::into_inner)
                        .map(|mut response| std::mem::take(&mut response.credentials))
                }
            },
        )
        .await?;
        pull.finish(pplx_space_metrics::reconcile::NetworkCredentialPullOutcome::Success);
        Ok(credentials)
    }

    async fn resolve_gateway_network_policy(
        &self,
        request: Request<HostControlGetNetworkPolicyRequest>,
    ) -> Result<ResolvedGatewayNetworkPolicy, Status> {
        let sandbox = self.authenticate_gateway_policy_request(request).await?;
        if sandbox.pool_template.is_some() {
            return Err(Status::failed_precondition(
                "pool sandbox has no assigned network identity",
            ));
        }

        let effective_policy = self.effective_network_policy(&sandbox).await?;
        let policy_ref = effective_network_policy_ref(&sandbox, &effective_policy)?;
        let network_credentials = if effective_policy.policy.network_credential_rules.is_empty() {
            Vec::new()
        } else {
            match self.fetch_injection_rules(&sandbox, &policy_ref).await {
                Ok(resolved) => resolved,
                Err(error) => {
                    tracing::warn!(
                        sandbox_id = %sandbox.id,
                        code = ?error.code(),
                        "network credential resolution failed; returning no material"
                    );
                    Vec::new()
                }
            }
        };

        Ok(ResolvedGatewayNetworkPolicy {
            sandbox,
            effective_policy,
            policy_ref,
            network_credentials,
        })
    }
}

#[tonic::async_trait]
impl HostControl for SandboxManagerService {
    async fn issue_gateway_intermediate_certificate(
        &self,
        request: Request<IssueGatewayIntermediateCertificateRequest>,
    ) -> Result<Response<IssueGatewayIntermediateCertificateResponse>, Status> {
        self.issue_gateway_authority(request)
            .await
            .map(Response::new)
    }

    async fn set_activity_state(
        &self,
        request: Request<HostControlSetActivityStateRequest>,
    ) -> Result<Response<HostControlSetActivityStateResponse>, Status> {
        let sandbox = self.authenticated_host_control_sandbox(&request)?;
        let _ = match HostControlActivityState::try_from(request.into_inner().state) {
            Ok(HostControlActivityState::Idle) => {
                self.set_guest_activity_state(&sandbox.id, true).await?
            }
            Ok(HostControlActivityState::Active) => {
                self.set_guest_activity_state(&sandbox.id, false).await?
            }
            Ok(HostControlActivityState::Unspecified) => {
                return Err(Status::invalid_argument("activity_state is required"));
            }
            Err(_) => return Err(Status::invalid_argument("unknown activity_state")),
        };
        Ok(Response::new(HostControlSetActivityStateResponse {
            outcome: None,
        }))
    }

    async fn request_active_for(
        &self,
        request: Request<HostControlRequestActiveForRequest>,
    ) -> Result<Response<HostControlRequestActiveForResponse>, Status> {
        let sandbox = self.authenticated_host_control_sandbox(&request)?;
        let hold_for = validate_duration("hold_for", request.into_inner().hold_for)?;
        let outcome = self.request_guest_active_for(&sandbox.id, hold_for).await?;
        Ok(Response::new(HostControlRequestActiveForResponse {
            outcome: Some(outcome),
        }))
    }

    async fn request_shutdown(
        &self,
        request: Request<HostControlShutdownRequest>,
    ) -> Result<Response<HostControlShutdownResponse>, Status> {
        let _ = self.authenticated_host_control_sandbox(&request)?;
        let _ = validate_optional_reason(request.into_inner().reason)?;
        Ok(Response::new(HostControlShutdownResponse {
            outcome: Some(HostControlShutdownOutcome::Denied(HostControlDenied {
                reason: "guest-requested shutdown is disabled".to_string(),
            })),
        }))
    }

    async fn get_network_policy(
        &self,
        request: Request<HostControlGetNetworkPolicyRequest>,
    ) -> Result<Response<HostControlGetNetworkPolicyResponse>, Status> {
        let (credential_deadline, has_caller_deadline) = credential_resolution_deadline(&request);
        let resolved = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(credential_deadline) => {
                return Err(if has_caller_deadline {
                    Status::cancelled("network credential caller deadline expired")
                } else {
                    Status::deadline_exceeded("network credential resolution timed out")
                });
            }
            result = self.resolve_gateway_network_policy(request) => result?,
        };
        // Do not hold the lifecycle/state-writer lease across the credential
        // manager or provider call above. It is needed only for this local ack.
        let state_writer = self.state_writer_lease_if_accepting("GetNetworkPolicyAck");
        // A draining process may still hydrate an existing gateway, but only
        // the active owner may persist its applied-policy acknowledgement.
        if state_writer.is_some() {
            self.mark_network_policy_applied(
                &resolved.sandbox.id,
                resolved.effective_policy.inherited,
                resolved.effective_policy.generation,
            )?;
        }
        Ok(Response::new(resolved.into_response()))
    }
}

#[cfg(test)]
mod tests;
