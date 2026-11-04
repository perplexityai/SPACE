use std::future::Future;
use std::time::Duration;

use pplx_space_common_core::types::SandboxId;
use pplx_space_common_grpc::GrpcRetryPolicy;
use pplx_space_node_mgr_client::{ConnectError, NodeManagerGrpcClient};
use tokio_util::sync::CancellationToken;
use tonic::Status;

const RETRY_POLICY: GrpcRetryPolicy = GrpcRetryPolicy {
    max_retries: usize::MAX,
    initial_delay: Duration::from_millis(100),
    backoff_multiplier: 2.0,
    max_delay: Duration::from_secs(1),
    max_elapsed: Duration::MAX,
    jitter_factor: 0.2,
    max_server_retry_after: Duration::ZERO,
};
const DIAL_TIMEOUT: Duration = Duration::from_secs(2);
const INSPECT_TIMEOUT: Duration = Duration::from_secs(10);
const WARNING_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug)]
pub(super) enum StartupPhase {
    /// No service ownership yet; a predecessor may or may not still serve.
    BeforeTakeover,
    /// Service ownership acquired; recovery and production serving have not begun.
    BeforeRecovery,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum NodeManagerStartupError {
    #[error("shutdown requested while waiting for node-manager")]
    Cancelled,
    #[error(transparent)]
    Status(#[from] Status),
}

enum AttemptError {
    Retry(Status),
    Fatal(Status),
}

/// Wait for the read-only execution endpoint until it answers, shutdown is
/// requested, or a non-transient failure is observed. Each dial and RPC is
/// bounded; the overall wait is independent of any caller's replay deadline.
/// Call before acquiring the recovery work gate: dependency startup may need it.
pub(super) async fn wait_for_node_manager<C, Fut>(
    phase: StartupPhase,
    shutdown: &CancellationToken,
    mut connect: C,
) -> Result<(), NodeManagerStartupError>
where
    C: FnMut() -> Fut,
    Fut: Future<Output = Result<NodeManagerGrpcClient, ConnectError>>,
{
    let started = tokio::time::Instant::now();
    let retry = async {
        let mut attempts = 0u64;
        let mut next_warning = started + WARNING_INTERVAL;
        loop {
            attempts = attempts.saturating_add(1);
            let attempt = async {
                // Fresh channels avoid remaining attached to a draining peer.
                // connect_once leaves this loop as the only retry owner.
                let mut client = tokio::time::timeout(DIAL_TIMEOUT, connect())
                    .await
                    .map_err(|_| {
                        AttemptError::Retry(Status::deadline_exceeded(
                            "node-manager dial timed out",
                        ))
                    })?
                    .map_err(classify_connect_error)?;
                tokio::time::timeout(
                    INSPECT_TIMEOUT,
                    pplx_space_node_mgr_client::execution_fence::inspect_execution(
                        &mut client,
                        SandboxId::new(),
                    ),
                )
                .await
                .map_err(|_| {
                    AttemptError::Retry(Status::deadline_exceeded("node-manager inspect timed out"))
                })?
                .map(drop)
                .map_err(classify_inspect_status)
            };
            let status = match attempt.await {
                Ok(()) => {
                    if attempts > 1 {
                        tracing::info!(
                            ?phase,
                            attempts,
                            wait_secs = started.elapsed().as_secs(),
                            "node-manager became available for startup"
                        );
                    }
                    return Ok(());
                }
                Err(AttemptError::Fatal(status)) => return Err(status),
                Err(AttemptError::Retry(status)) => status,
            };
            let now = tokio::time::Instant::now();
            if attempts == 1 {
                tracing::info!(?phase, attempts, wait_secs = started.elapsed().as_secs(), error = %status,
                    "sandbox-manager startup waiting for node-manager");
            }
            if now >= next_warning {
                tracing::warn!(?phase, attempts, wait_secs = started.elapsed().as_secs(), error = %status,
                    "sandbox-manager startup still waiting for node-manager");
                next_warning = now + WARNING_INTERVAL;
            }
            // The exponential reaches its cap at index four. Saturating the
            // index also keeps arbitrarily long dependency waits overflow-safe.
            let delay = RETRY_POLICY
                .backoff_delay(attempts.saturating_sub(1).min(4) as usize)
                .unwrap_or(RETRY_POLICY.max_delay);
            tokio::time::sleep(delay).await;
        }
    };
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => Err(NodeManagerStartupError::Cancelled),
        result = retry => result.map_err(Into::into),
    }
}

fn classify_connect_error(error: ConnectError) -> AttemptError {
    let transient = match &error {
        ConnectError::Connect(pplx_space_common_grpc::UdsConnectError::Transport {
            source,
            ..
        }) => std::iter::successors(
            Some(source as &(dyn std::error::Error + 'static)),
            |error| error.source(),
        )
        .filter_map(|error| error.downcast_ref::<std::io::Error>())
        .any(|error| {
            matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            )
        }),
        _ => false,
    };
    let status = Status::unavailable(format!("connect for node-manager startup: {error}"));
    if transient {
        AttemptError::Retry(status)
    } else {
        AttemptError::Fatal(status)
    }
}

fn classify_inspect_status(status: Status) -> AttemptError {
    // This read has no side effects, including when a timed-out request ran.
    // Admission can reject it while predecessor traffic shares the UDS budget.
    match status.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::ResourceExhausted => AttemptError::Retry(status),
        _ => AttemptError::Fatal(status),
    }
}

#[cfg(test)]
mod tests;
