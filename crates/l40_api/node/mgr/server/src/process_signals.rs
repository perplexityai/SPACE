//! Route process-directed stop signals away from non-repeatable blocking work.
use std::io;
use std::marker::PhantomData;
use std::sync::OnceLock;
use std::thread::JoinHandle;

use nix::sys::signal::{SigSet, SigmaskHow, Signal};
use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error)]
pub enum InstallError {
    #[error("failed to block shutdown signals on the startup thread")]
    Mask(#[source] nix::errno::Errno),
    #[error("process shutdown signal routing is already installed")]
    AlreadyInstalled,
    #[error("failed to start the shutdown signal thread")]
    Spawn(#[source] io::Error),
}

struct StartupMask {
    previous: SigSet,
    // Restoring another thread's mask would leave the startup thread blocked.
    _thread: PhantomData<*mut ()>,
}

impl Drop for StartupMask {
    fn drop(&mut self) {
        if let Err(error) = self.previous.thread_set_mask() {
            tracing::error!(%error, "failed to restore startup thread signal mask");
        }
    }
}

/// Owns the process's SIGINT/SIGTERM receiver and the existing service stop token.
///
/// Install on the startup thread before creating the runtime, telemetry, or any
/// other threads. Keep it alive until those threads have stopped. Inherited
/// blocking keeps process-directed stop signals off every worker, including
/// lazily created blocking-pool threads. This does not route thread-directed or
/// profiler signals; non-repeatable sends still need their local signal guard.
/// Child commands must unblock these signals before exec.
pub struct ProcessShutdownSignals {
    shutdown: CancellationToken,
    stop_receiver: CancellationToken,
    receiver: Option<JoinHandle<()>>,
    _mask: StartupMask,
}

impl ProcessShutdownSignals {
    pub fn install() -> Result<Self, InstallError> {
        let signals = shutdown_signals();
        let mask = StartupMask {
            previous: signals
                .thread_swap_mask(SigmaskHow::SIG_BLOCK)
                .map_err(InstallError::Mask)?,
            _thread: PhantomData,
        };
        let mut child_mask = mask.previous;
        child_mask.remove(Signal::SIGINT);
        child_mask.remove(Signal::SIGTERM);
        CHILD_MASK
            .set(child_mask)
            .map_err(|_| InstallError::AlreadyInstalled)?;
        let shutdown = CancellationToken::new();
        let stop_receiver = CancellationToken::new();
        let receiver = std::thread::Builder::new()
            .name("space-stop-signals".into())
            .spawn({
                let shutdown = shutdown.clone();
                let stop_receiver = stop_receiver.clone();
                move || {
                    // A receiver panic must trigger drain, not leave a service
                    // alive with no thread able to consume its stop signals.
                    let _cancel_on_exit = shutdown.clone().drop_guard();
                    receive(signals, shutdown, stop_receiver);
                }
            })
            .map_err(InstallError::Spawn)?;
        Ok(Self {
            shutdown,
            stop_receiver,
            receiver: Some(receiver),
            _mask: mask,
        })
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }
}

impl Drop for ProcessShutdownSignals {
    fn drop(&mut self) {
        self.stop_receiver.cancel();
        if let Some(receiver) = self.receiver.take()
            && receiver.join().is_err()
        {
            tracing::error!("shutdown signal thread panicked");
        }
    }
}

fn shutdown_signals() -> SigSet {
    let mut signals = SigSet::empty();
    signals.add(Signal::SIGINT);
    signals.add(Signal::SIGTERM);
    signals
}

fn receive(signals: SigSet, shutdown: CancellationToken, stop: CancellationToken) {
    // Bound join time even on startup failure or ordinary return without a stop
    // signal. No synthetic signal is needed to wake or terminate the receiver.
    let timeout = libc::timespec {
        tv_sec: 0,
        tv_nsec: 100_000_000,
    };
    while !stop.is_cancelled() {
        // SAFETY: the set and timeout remain valid; no siginfo output is requested.
        let result =
            unsafe { libc::sigtimedwait(signals.as_ref(), std::ptr::null_mut(), &timeout) };
        if result >= 0 {
            tracing::info!(signal = result, "received shutdown signal, shutting down");
            shutdown.cancel();
        } else {
            let error = nix::errno::Errno::last();
            if !matches!(error, nix::errno::Errno::EAGAIN | nix::errno::Errno::EINTR) {
                tracing::error!(%error, "shutdown signal receiver failed; shutting down");
                shutdown.cancel();
                return;
            }
        }
    }
}

// Captured before worker creation; never inherit a send guard's temporary mask.
static CHILD_MASK: OnceLock<SigSet> = OnceLock::new();

pub(crate) fn child_mask() -> io::Result<SigSet> {
    let mut mask = match CHILD_MASK.get() {
        Some(mask) => *mask,
        // Library callers that do not install process routing retain their
        // current mask, with stop signals unblocked for foreground tools.
        None => SigSet::thread_get_mask()?,
    };
    mask.remove(Signal::SIGINT);
    mask.remove(Signal::SIGTERM);
    Ok(mask)
}

#[cfg(test)]
mod tests;
