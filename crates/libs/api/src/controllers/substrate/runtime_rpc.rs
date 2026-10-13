use std::future::Future;

use pplx_space_common_core::types::SandboxId;
use pplx_space_common_grpc::is_client_connect_failure;
use tokio::time::Instant;

use super::{ControllerError, RuntimeClient, Substrate, SubstrateAdapter};

#[derive(Debug, thiserror::Error)]
pub(crate) enum RuntimeRpcError {
    #[error(transparent)]
    Client(#[from] ControllerError),
    #[error(transparent)]
    Rpc(#[from] tonic::Status),
}

impl From<RuntimeRpcError> for ControllerError {
    fn from(error: RuntimeRpcError) -> Self {
        match error {
            RuntimeRpcError::Client(error) => error,
            RuntimeRpcError::Rpc(status) => status.into(),
        }
    }
}

impl RuntimeClient {
    /// Retry once only if the local transport proves the RPC was never sent.
    /// Resolution and both attempts share `deadline`. A fresh probe may change
    /// the endpoint, so mutations must retain their execution-generation fence.
    /// Ambiguous failures remain the caller's responsibility to reconcile.
    pub(crate) async fn call<Req, T, F, Fut>(
        &mut self,
        substrate: &Substrate,
        sandbox_id: SandboxId,
        deadline: Instant,
        request: tonic::Request<Req>,
        mut operation: F,
    ) -> Result<T, RuntimeRpcError>
    where
        Req: Clone,
        F: FnMut(Self, tonic::Request<Req>) -> Fut,
        Fut: Future<Output = Result<T, tonic::Status>>,
    {
        let expired = || {
            RuntimeRpcError::Client(ControllerError::service_unavailable(
                "node-api request deadline expired",
            ))
        };
        if Instant::now() >= deadline {
            return Err(expired());
        }
        let (metadata, extensions, message) = request.into_parts();
        let mut attempt = |client| {
            let mut request =
                tonic::Request::from_parts(metadata.clone(), extensions.clone(), message.clone());
            request.set_timeout(deadline.saturating_duration_since(Instant::now()));
            operation(client, request)
        };
        tokio::time::timeout_at(deadline, async {
            let status = match attempt(self.clone()).await {
                Err(status) if is_client_connect_failure(&status) => status,
                result => return result.map_err(Into::into),
            };
            if Instant::now() >= deadline {
                return Err(expired());
            }
            tracing::debug!(
                %sandbox_id,
                error = %status,
                "runtime RPC was not transmitted; retrying on a fresh worker connection"
            );
            let Some(client) = substrate.probe_runtime_client(sandbox_id).await? else {
                return Err(status.into());
            };
            *self = client;
            if Instant::now() >= deadline {
                return Err(expired());
            }
            attempt(self.clone()).await.map_err(Into::into)
        })
        .await
        .unwrap_or_else(|_| Err(expired()))
    }
}

#[cfg(test)]
mod tests;
