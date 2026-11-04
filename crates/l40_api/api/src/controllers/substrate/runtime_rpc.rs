use std::future::Future;

use pplx_space_common_core::types::SandboxId;
use pplx_space_common_grpc::client_connect_failure_cause;
use pplx_space_metrics::grpc::ClientReplayOutcome;
use tokio::time::Instant;

use super::{ControllerError, RuntimeClient, Substrate, SubstrateAdapter};

/// Tags `grpc.client.replay`, as the worker dials tag their connect spans.
const SERVICE: &str = "worker-node-api";

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
    /// Each replay decision counts once in `grpc.client.replay`, with the
    /// proof the first failure carried and what became of the replay; a call
    /// its caller abandons midway counts nothing.
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
        // Set once the first attempt proves it was never sent, then once the
        // replay's fate is known; a replay the deadline cuts short leaves the
        // outcome unset.
        let mut replay_cause = None;
        let mut replay_outcome = None;
        let result = tokio::time::timeout_at(deadline, async {
            let status = match attempt(self.clone()).await {
                Err(status) => match client_connect_failure_cause(&status) {
                    Some(cause) => {
                        replay_cause = Some(cause);
                        status
                    }
                    None => return Err(status.into()),
                },
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
            let client = match substrate.probe_runtime_client(sandbox_id).await {
                Ok(Some(client)) => client,
                Ok(None) => {
                    replay_outcome = Some(ClientReplayOutcome::NoFreshClient);
                    return Err(status.into());
                }
                Err(error) => {
                    replay_outcome = Some(ClientReplayOutcome::NoFreshClient);
                    return Err(error.into());
                }
            };
            *self = client;
            if Instant::now() >= deadline {
                return Err(expired());
            }
            let replayed = attempt(self.clone()).await;
            replay_outcome = Some(if replayed.is_ok() {
                ClientReplayOutcome::ReplayedOk
            } else {
                ClientReplayOutcome::ReplayedError
            });
            replayed.map_err(Into::into)
        })
        .await
        .unwrap_or_else(|_| Err(expired()));
        if let Some(cause) = replay_cause {
            pplx_space_metrics::grpc::client_replay(
                SERVICE,
                cause.as_str(),
                replay_outcome.unwrap_or(ClientReplayOutcome::DeadlineExpired),
            );
        }
        result
    }
}

#[cfg(test)]
mod tests;
