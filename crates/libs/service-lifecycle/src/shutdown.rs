//! Process shutdown: SIGINT and SIGTERM handling and the cancellation tokens services drain on.

use tokio_util::sync::CancellationToken;

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

pub fn shutdown_token_with_drain(drain_signal: crate::DrainSignal) -> CancellationToken {
    shutdown_token_with_drain_signal(Some(drain_signal))
}

/// Stop-signal tokens for a service that drains after a stop signal:
/// `shutdown` is cancelled by the first stop signal (SIGINT or SIGTERM; Ctrl-C
/// off Unix), `forced` by the next one, which ends the drain early. The same
/// handlers see both, so a second signal that arrives while the first is
/// still being handled is kept rather than lost.
pub struct StopTokens {
    pub shutdown: CancellationToken,
    pub forced: CancellationToken,
}

pub fn stop_tokens() -> StopTokens {
    let shutdown = CancellationToken::new();
    let forced = CancellationToken::new();
    spawn_stop_signal_task(None, shutdown.clone(), Some(forced.clone()));
    StopTokens { shutdown, forced }
}

pub(super) fn shutdown_token_with_drain_signal(
    drain_signal: Option<crate::DrainSignal>,
) -> CancellationToken {
    let token = CancellationToken::new();
    spawn_stop_signal_task(drain_signal, token.clone(), None);
    token
}

fn spawn_stop_signal_task(
    drain_signal: Option<crate::DrainSignal>,
    token: CancellationToken,
    forced: Option<CancellationToken>,
) {
    // Install the handlers before returning, not when the task below is first
    // polled. Callers report readiness right after this, and a stop signal
    // that arrives before its handler exists takes the default action and
    // kills the process without draining.
    let signals = StopSignals::install();

    tokio::spawn(async move {
        let mut signals = match signals {
            Ok(mut signals) => {
                signals.recv("shutting down").await;
                Some(signals)
            }
            Err(error) => {
                tracing::error!(error = %error, "failed to wait for shutdown signal");
                None
            }
        };
        if let Some(drain_signal) = drain_signal {
            drain_signal.begin();
        }
        token.cancel();
        if let (Some(forced), Some(signals)) = (forced, signals.as_mut()) {
            signals.recv("ending the drain").await;
            forced.cancel();
        }
    });
}

/// The process's stop-signal handlers. A signal delivered after they're
/// installed is kept for the next [`StopSignals::recv`], even if nothing is
/// polling yet; signals delivered between two calls count once.
pub(super) struct StopSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(not(unix))]
    ctrl_c: tokio::signal::windows::CtrlC,
}

impl StopSignals {
    #[cfg(unix)]
    pub(super) fn install() -> Result<Self, WaitForSignalError> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt()).map_err(WaitForSignalError::Sigint)?,
            terminate: signal(SignalKind::terminate()).map_err(WaitForSignalError::Sigterm)?,
        })
    }

    #[cfg(not(unix))]
    pub(super) fn install() -> Result<Self, WaitForSignalError> {
        Ok(Self {
            ctrl_c: tokio::signal::windows::ctrl_c().map_err(WaitForSignalError::CtrlC)?,
        })
    }

    /// Resolves on the next stop signal and logs it with what happens next.
    #[cfg(unix)]
    pub(super) async fn recv(&mut self, action: &str) {
        tokio::select! {
            _ = self.interrupt.recv() => {
                tracing::info!("received SIGINT, {action}");
            }
            _ = self.terminate.recv() => {
                tracing::info!("received SIGTERM, {action}");
            }
        }
    }

    #[cfg(not(unix))]
    pub(super) async fn recv(&mut self, action: &str) {
        self.ctrl_c.recv().await;
        tracing::info!("received Ctrl-C, {action}");
    }
}

#[cfg(test)]
mod tests;
