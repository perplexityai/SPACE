//! Process shutdown: SIGINT and SIGTERM handling and the cancellation tokens services drain on.

use super::*;

#[derive(Debug, thiserror::Error)]
pub enum WaitForSignalError {
    #[error("failed to install SIGINT handler: {0}")]
    Sigint(#[source] std::io::Error),
    #[error("failed to install SIGTERM handler: {0}")]
    Sigterm(#[source] std::io::Error),
    #[cfg(not(unix))]
    #[error("failed to install Ctrl-C handler: {0}")]
    CtrlC(#[source] std::io::Error),
}

pub async fn wait_for_sigterm() -> Result<(), WaitForSignalError> {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(WaitForSignalError::Sigterm)?;
    sigterm.recv().await;
    tracing::info!("received SIGTERM, shutting down");
    Ok(())
}

pub async fn sigterm_shutdown() {
    if let Err(error) = wait_for_sigterm().await {
        tracing::error!(error = %error, "failed to wait for SIGTERM");
    }
}

pub fn shutdown_token() -> CancellationToken {
    shutdown_token_with_drain_signal(None)
}

pub fn shutdown_token_with_drain(drain_signal: crate::lifecycle::DrainSignal) -> CancellationToken {
    shutdown_token_with_drain_signal(Some(drain_signal))
}

pub(super) fn shutdown_token_with_drain_signal(
    drain_signal: Option<crate::lifecycle::DrainSignal>,
) -> CancellationToken {
    let token = CancellationToken::new();
    // Install the handlers before returning, not when the task below is first
    // polled. Callers report readiness right after this, and a stop signal
    // that arrives before its handler exists takes the default action and
    // kills the process without draining.
    let shutdown_signal = install_shutdown_signal_handlers();

    tokio::spawn({
        let token = token.clone();
        async move {
            match shutdown_signal {
                Ok(shutdown_signal) => shutdown_signal.await,
                Err(error) => {
                    tracing::error!(error = %error, "failed to wait for shutdown signal");
                }
            }
            if let Some(drain_signal) = drain_signal {
                drain_signal.begin();
            }
            token.cancel();
        }
    });

    token
}

/// Registers the stop-signal handlers and returns a future that resolves on
/// the first stop signal. A signal delivered after this returns is recorded
/// even if the future has not been polled yet.
#[cfg(unix)]
pub(super) fn install_shutdown_signal_handlers()
-> Result<impl Future<Output = ()>, WaitForSignalError> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut interrupt = signal(SignalKind::interrupt()).map_err(WaitForSignalError::Sigint)?;
    let mut terminate = signal(SignalKind::terminate()).map_err(WaitForSignalError::Sigterm)?;

    Ok(async move {
        tokio::select! {
            _ = interrupt.recv() => {
                tracing::info!("received SIGINT, shutting down");
            }
            _ = terminate.recv() => {
                tracing::info!("received SIGTERM, shutting down");
            }
        }
    })
}

#[cfg(not(unix))]
pub(super) fn install_shutdown_signal_handlers()
-> Result<impl Future<Output = ()>, WaitForSignalError> {
    let mut ctrl_c = tokio::signal::windows::ctrl_c().map_err(WaitForSignalError::CtrlC)?;

    Ok(async move {
        ctrl_c.recv().await;
        tracing::info!("received Ctrl-C, shutting down");
    })
}
