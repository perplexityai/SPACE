//! Route authorization precedes this capability; substrate controllers retain
//! activation ownership and admission. The host binary supplies the owner.
use pplx_space_common_core::types::SandboxId;
use pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority;
use std::{sync::Arc, time::Duration};
use tonic::Status;

#[tonic::async_trait]
pub trait IngressActivation: Send + Sync {
    /// Bounds the caller's wait, not accepted activation ownership. Dropping this
    /// future leaves accepted work under the substrate's existing lease/budget.
    async fn prepare(&self, sandbox_id: SandboxId, timeout: Duration) -> Result<(), Status>;
}

#[tonic::async_trait]
pub trait IngressActivationFactory: Send + Sync {
    async fn build(
        &self,
        pool: sqlx::PgPool,
        authority: Option<Arc<SpaceApiRouteEd25519Authority>>,
        tasks: tokio_util::task::TaskTracker,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<Arc<dyn IngressActivation>, Status>;
}
