//! gRPC boundary for network-credential resolution.
//!
//! Transport authentication has already produced a [`ResolvedCaller`] before
//! this adapter runs. It converts that identity and the wire request into
//! domain types, applies the prototype's total concurrency and latency bounds,
//! delegates to Credential Authority, and maps domain failures to gRPC status.

use std::sync::Arc;

use pplx_space_common_svc::core::types::NodeRole;
use pplx_space_credmgr_client::NETWORK_CREDENTIAL_RESOLUTION_TIMEOUT;
use tokio::sync::Semaphore;
use tonic::Status;

use crate::auth::{ClientService, ResolvedCaller};
use crate::credential_injection_authority::{
    CredentialInjectionAuthority, NetworkCredentialResolutionError,
    NetworkCredentialResolutionRequest, ResolutionCaller,
};
use crate::credential_provider::ResolutionError as CredentialResolutionError;
use crate::network_credential_materializer::NetworkCredentialMaterializationError;
use crate::proto::{ResolveNetworkCredentialsRequest, ResolveNetworkCredentialsResponse};

// The PoC admits 32 concurrent resolutions per authority process; production
// tuning remains follow-up work.
const MAX_CONCURRENT_RESOLUTIONS: usize = 32;

#[derive(Clone)]
pub(crate) struct CredentialInjectionRpc {
    authority: CredentialInjectionAuthority,
    permits: Arc<Semaphore>,
}

impl CredentialInjectionRpc {
    pub(crate) fn new(authority: CredentialInjectionAuthority) -> Self {
        Self {
            authority,
            permits: Arc::new(Semaphore::new(MAX_CONCURRENT_RESOLUTIONS)),
        }
    }

    pub(crate) async fn resolve_network_credentials(
        &self,
        caller: &ResolvedCaller,
        request: ResolveNetworkCredentialsRequest,
    ) -> Result<ResolveNetworkCredentialsResponse, Status> {
        let caller = resolution_caller(caller)?;
        let domain_request = NetworkCredentialResolutionRequest::parse(
            &request.sandbox_id,
            request.execution_generation,
            &request.effective_network_policy_ref,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let _permit = self
            .permits
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("credential resolution is at capacity"))?;
        let resolved = tokio::time::timeout(
            NETWORK_CREDENTIAL_RESOLUTION_TIMEOUT,
            self.authority
                .resolve_network_credentials(&caller, domain_request),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("credential resolution timed out"))?
        .map_err(network_credential_resolution_error_to_status)?;

        Ok(ResolveNetworkCredentialsResponse {
            effective_network_policy_ref: resolved.effective_network_policy_ref,
            credentials: resolved.credentials,
        })
    }
}

fn resolution_caller(caller: &ResolvedCaller) -> Result<ResolutionCaller, Status> {
    match caller {
        ResolvedCaller::K8sJwt(identity)
            if identity.service() == ClientService::NodeCredentialAgent =>
        {
            let node_identity = identity.node();
            // Sandbox executions run only on worker nodes. TokenReview
            // retains the Kubernetes node identity for agents on every node
            // role; this RPC supplies the worker role needed by the stable
            // SPACE NodeId derivation.
            let node_id = pplx_space_common_svc::types::k8s::space_k8s_node_id(
                node_identity.node_name().as_str(),
                NodeRole::Worker.as_str(),
            );
            Ok(ResolutionCaller::new(
                node_id,
                node_identity.node_uid().clone(),
            ))
        }
        ResolvedCaller::K8sJwt(_) | ResolvedCaller::UdsPeer(_) | ResolvedCaller::Plaintext => {
            Err(Status::permission_denied(
                "credential injection resolution requires an authenticated node credential agent",
            ))
        }
    }
}

fn network_credential_resolution_error_to_status(
    error: NetworkCredentialResolutionError,
) -> Status {
    match error {
        NetworkCredentialResolutionError::CandidateUnavailableOrUnauthorized => {
            Status::permission_denied(error.to_string())
        }
        NetworkCredentialResolutionError::StaleCandidate => Status::aborted(error.to_string()),
        NetworkCredentialResolutionError::StateUnavailable(state_error) => {
            tracing::warn!(error = %state_error, "credential-injection resolution state unavailable");
            Status::unavailable("credential-injection resolution state unavailable")
        }
        NetworkCredentialResolutionError::InvalidState(state_error) => {
            tracing::error!(error = %state_error, "credential-injection resolution state is invalid");
            Status::internal("credential-injection resolution state is invalid")
        }
        NetworkCredentialResolutionError::Materialize(
            NetworkCredentialMaterializationError::InvalidRules(error),
        ) => {
            tracing::error!(error, "stored credential-injection rules are invalid");
            Status::internal("stored credential-injection rules are invalid")
        }
        NetworkCredentialResolutionError::Materialize(
            NetworkCredentialMaterializationError::InvalidResolvedCredential(error),
        ) => {
            tracing::warn!(error, "resolved credential cannot be injected");
            Status::failed_precondition("resolved credential cannot be injected")
        }
        NetworkCredentialResolutionError::Materialize(
            NetworkCredentialMaterializationError::Provider(error),
        ) => provider_error_to_status(error),
    }
}

fn provider_error_to_status(error: CredentialResolutionError) -> Status {
    match error {
        CredentialResolutionError::ProviderNotConfigured
        | CredentialResolutionError::MalformedReference
        | CredentialResolutionError::CredentialUnavailable
        | CredentialResolutionError::MalformedCredential => {
            Status::failed_precondition(error.to_string())
        }
        CredentialResolutionError::ReferenceUnauthorized => {
            Status::permission_denied(error.to_string())
        }
        CredentialResolutionError::ProviderUnavailable => Status::unavailable(error.to_string()),
    }
}

#[cfg(test)]
mod tests;
