//! Bounded, authenticated delegation to the existing lifecycle API.
use crate::proto::{
    PrepareIngressRequest, ingress_activation_api_client::IngressActivationApiClient,
};
use pplx_space_common_core::types::SandboxId;
use pplx_space_common_grpc::internal_transport::{ClientTransport, Service};
use pplx_space_common_svc::{
    bearer_token::{SpaceBearerToken, insert_space_bearer_token},
    bootstrap::{GrpcClientTrace, wrap_grpc_client_channel},
};
use std::time::Duration;
use tonic::{
    Request, Status,
    transport::{Channel, Endpoint},
};

#[tonic::async_trait]
pub trait IngressActivation: Send + Sync {
    /// The timeout bounds observation, not accepted activation ownership.
    async fn prepare(&self, sandbox_id: SandboxId, timeout: Duration) -> Result<(), Status>;
}

#[derive(Clone)]
pub struct RemoteIngressActivation {
    client: Option<IngressActivationApiClient<GrpcClientTrace<Channel>>>,
}
impl RemoteIngressActivation {
    /// Construct one reusable channel; startup never instantiates a substrate.
    pub async fn from_env() -> Result<Self, Status> {
        let address = match std::env::var("SPACE_INGRESS_ACTIVATION_ENDPOINT") {
            Ok(address) if !address.trim().is_empty() => address,
            Ok(_) | Err(std::env::VarError::NotPresent) => {
                tracing::warn!(
                    "ingress activation endpoint is unconfigured; healthy routes remain available, delegated wakes will be unavailable"
                );
                return Ok(Self { client: None });
            }
            Err(error) => return Err(Status::invalid_argument(error.to_string())),
        };
        let endpoint = Endpoint::from_shared(address)
            .map_err(|error| Status::invalid_argument(error.to_string()))?
            .connect_timeout(Duration::from_secs(3));
        let transport = ClientTransport::from_env(Service::IngressActivation, endpoint.uri())
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let channel = transport
            .endpoint(endpoint)
            .map_err(|error| Status::failed_precondition(error.to_string()))?
            .connect_lazy();
        Ok(Self {
            client: Some(IngressActivationApiClient::new(wrap_grpc_client_channel(
                channel,
            ))),
        })
    }
}
#[tonic::async_trait]
impl IngressActivation for RemoteIngressActivation {
    async fn prepare(&self, sandbox_id: SandboxId, budget: Duration) -> Result<(), Status> {
        if budget.is_zero() {
            return Err(Status::deadline_exceeded(
                "ingress preparation deadline exceeded",
            ));
        }
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Status::unavailable("ingress activation endpoint is not configured"))?;
        let mut request = Request::new(PrepareIngressRequest {
            sandbox_id: sandbox_id.to_string(),
        });
        request.set_timeout(budget);
        // The listener uses the cluster control-plane authentication surface;
        // its deployment allowlist must contain only trusted route resolvers.
        insert_space_bearer_token(request.metadata_mut(), SpaceBearerToken::ClusterApi)
            .map_err(|error| Status::unauthenticated(error.to_string()))?;
        tokio::time::timeout(budget, client.clone().prepare_ingress(request))
            .await
            .map_err(|_| Status::deadline_exceeded("ingress preparation deadline exceeded"))??;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
