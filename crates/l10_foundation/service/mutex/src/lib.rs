//! File-based service mutex via `flock(2)`.
//!
//! Active/passive selection for a daemon that can briefly coexist with a
//! successor instance on the same host (e.g. during a rolling restart). The
//! successor blocks on [`ServiceMutex::acquire`] until the predecessor
//! releases the mutex — cooperatively on `SIGTERM` drain, or via kernel-level
//! release on process death.

use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// Polling interval used by [`ServiceMutex::acquire_with_timeout`]. The
/// non-blocking flock is retried every interval until the predecessor
/// releases the mutex or the timeout/cancellation token fires. Small enough
/// that handoff latency is dominated by active-side startup work; large
/// enough that the poll itself is invisible in profiles.
pub const SERVICE_MUTEX_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Outcome of [`ServiceMutex::acquire_with_timeout`].
#[derive(Debug)]
pub enum AcquireOutcome {
    /// The mutex was acquired; the caller should proceed with active-startup.
    Acquired(ServiceMutex),
    /// `shutdown_token` fired before the mutex could be acquired. The caller
    /// should clean up any passive-mode resources and exit cleanly with
    /// status 0 — the process was asked to terminate while still waiting to
    /// take over.
    PassiveExit,
}

/// RAII handle to an exclusively-held `flock` that gates a service's active
/// state. Dropping the handle releases the kernel-level `flock` and closes
/// the underlying file. Only the holder runs timers, mutates shared on-host
/// state, and binds the production socket.
#[derive(Debug)]
pub struct ServiceMutex {
    _lock: Flock<std::fs::File>,
    path: PathBuf,
}

impl ServiceMutex {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Terminates the process without releasing this ownership guard through
    /// normal stack unwinding.
    ///
    /// Mutex-owning daemons must use this when they cannot prove that every
    /// task touching the owned state has stopped. `process::exit` skips
    /// destructors, so the kernel releases the flock only at the process
    /// boundary, after the runtime and its detached tasks are gone.
    pub fn hard_exit(self, exit_code: i32, reason: &'static str) -> ! {
        tracing::error!(
            service_mutex = %self.path.display(),
            reason,
            exit_code,
            "exiting at a hard process boundary before releasing service ownership"
        );
        std::process::exit(exit_code)
    }

    /// Acquires the mutex on `path`, retrying until the underlying `flock` is
    /// available, `shutdown_token` fires, or `timeout` elapses (if `Some`).
    ///
    /// Uses non-blocking `flock` with a `poll_interval` between attempts so
    /// cancellation is always promptly observed. `Cancelled` means the
    /// process was asked to shut down while still passive — exit cleanly.
    /// `Timeout` means the predecessor never released the mutex within
    /// budget — exit non-zero so the supervisor restarts the process and a
    /// stuck predecessor surfaces as a visible failure instead of an
    /// indefinitely-passive process whose health endpoint still reports OK.
    ///
    /// Creates new mutexes owner-only, without changing the process umask.
    /// Opening an existing mutex preserves its inode, contents and permissions.
    pub async fn acquire(
        path: impl AsRef<Path>,
        shutdown_token: CancellationToken,
        poll_interval: Duration,
        timeout: Option<Duration>,
    ) -> Result<Self, AcquireError> {
        let path = path.as_ref().to_path_buf();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .mode(0o600)
            .open(&path)
            .map_err(|source| AcquireError::Open {
                path: path.clone(),
                source,
            })?;

        let deadline = timeout.map(|t| tokio::time::Instant::now() + t);
        loop {
            if shutdown_token.is_cancelled() {
                return Err(AcquireError::Cancelled);
            }
            if let Some(deadline) = deadline
                && tokio::time::Instant::now() >= deadline
            {
                return Err(AcquireError::Timeout {
                    path,
                    waited: timeout.expect("deadline implies timeout"),
                });
            }
            match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
                Ok(lock) => return Ok(Self { _lock: lock, path }),
                Err((returned, Errno::EWOULDBLOCK)) => {
                    file = returned;
                    let sleep = tokio::time::sleep(poll_interval);
                    tokio::pin!(sleep);
                    if let Some(deadline) = deadline {
                        tokio::select! {
                            _ = shutdown_token.cancelled() => {
                                return Err(AcquireError::Cancelled);
                            }
                            _ = tokio::time::sleep_until(deadline) => {
                                return Err(AcquireError::Timeout {
                                    path,
                                    waited: timeout.expect("deadline implies timeout"),
                                });
                            }
                            _ = &mut sleep => {}
                        }
                    } else {
                        tokio::select! {
                            _ = shutdown_token.cancelled() => {
                                return Err(AcquireError::Cancelled);
                            }
                            _ = &mut sleep => {}
                        }
                    }
                }
                Err((_, errno)) => {
                    return Err(AcquireError::Flock {
                        path,
                        source: std::io::Error::from_raw_os_error(errno as i32),
                    });
                }
            }
        }
    }

    /// SPACE-default acquire: [`SERVICE_MUTEX_POLL_INTERVAL`] between
    /// non-blocking attempts, bounded by `timeout`. Folds the
    /// `Cancelled`-while-passive case into [`AcquireOutcome::PassiveExit`]
    /// so daemons can match on the outcome without re-implementing the
    /// clean-exit branch each time. `Timeout` and other errors stay as
    /// [`AcquireError`] so the process exits non-zero on a stuck
    /// predecessor.
    pub async fn acquire_with_timeout(
        path: impl AsRef<Path>,
        shutdown_token: &CancellationToken,
        timeout: Duration,
    ) -> Result<AcquireOutcome, AcquireError> {
        match Self::acquire(
            path,
            shutdown_token.clone(),
            SERVICE_MUTEX_POLL_INTERVAL,
            Some(timeout),
        )
        .await
        {
            Ok(mutex) => Ok(AcquireOutcome::Acquired(mutex)),
            Err(AcquireError::Cancelled) => Ok(AcquireOutcome::PassiveExit),
            Err(other) => Err(other),
        }
    }
}

#[derive(Debug, Error)]
pub enum AcquireError {
    #[error("failed to open service mutex file {}: {source}", path.display())]
    Open {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to flock service mutex {}: {source}", path.display())]
    Flock {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("shutdown requested before service mutex was acquired")]
    Cancelled,
    #[error(
        "timed out after {waited:?} waiting for service mutex on {}",
        path.display()
    )]
    Timeout { path: PathBuf, waited: Duration },
}

#[cfg(test)]
mod tests;
