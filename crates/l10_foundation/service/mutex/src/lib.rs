//! File-based service mutex via `flock(2)`.
//!
//! Active/passive selection for a daemon that can briefly coexist with a
//! successor instance on the same host (e.g. during a rolling restart). The
//! successor blocks on [`ServiceMutex::acquire`] until the predecessor
//! releases the mutex — cooperatively on `SIGTERM` drain, or via kernel-level
//! release on process death.
//!
//! A passive instance that can tell its active startup would fail runs
//! [`gate_takeover`] before reporting itself ready to take over, so a
//! surge rollout keeps the predecessor serving instead of replacing it with
//! an instance that would refuse to start.

use std::fmt::Display;
use std::future::Future;
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

/// How often [`gate_takeover`] runs its check again while the check blocks
/// takeover and another instance holds the mutex.
pub const TAKEOVER_RECHECK_INTERVAL: Duration = Duration::from_secs(5);

/// How often [`gate_takeover`] repeats an unchanged block at warn, so an
/// operator reading only recent logs still finds it.
pub const TAKEOVER_BLOCK_REMINDER_INTERVAL: Duration = Duration::from_secs(300);

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
        let mut file = open_mutex_file(&path)?;

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

    /// Takes the mutex on `path` if no other instance holds it, without
    /// waiting. Opens the file as [`acquire`](Self::acquire) does.
    pub fn try_acquire(path: impl AsRef<Path>) -> Result<Option<Self>, AcquireError> {
        let path = path.as_ref().to_path_buf();
        let file = open_mutex_file(&path)?;
        match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(lock) => Ok(Some(Self { _lock: lock, path })),
            Err((_, Errno::EWOULDBLOCK)) => Ok(None),
            Err((_, errno)) => Err(AcquireError::Flock {
                path,
                source: std::io::Error::from_raw_os_error(errno as i32),
            }),
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

fn open_mutex_file(path: &Path) -> Result<std::fs::File, AcquireError> {
    std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .read(true)
        .mode(0o600)
        .open(path)
        .map_err(|source| AcquireError::Open {
            path: path.to_path_buf(),
            source,
        })
}

/// What a passive instance does once [`gate_takeover`] returns.
#[derive(Debug)]
pub enum TakeoverGate {
    /// The check allows takeover. Report takeover-ready, then acquire the
    /// mutex as usual; any acquire budget starts now.
    Open,
    /// The check still blocks, but no other instance held the mutex, so this
    /// one took it. Waiting protects nothing then: start as usual, and the
    /// startup step the check predicts decides, failing with its own error
    /// where the check foresaw that.
    Acquired(ServiceMutex),
    /// Shutdown was requested while gated: exit cleanly, as from a passive
    /// wait.
    PassiveExit,
}

#[derive(Debug, Error)]
pub enum TakeoverGateError<E> {
    #[error("takeover check failed: {0}")]
    Check(#[source] E),
    #[error(transparent)]
    Mutex(AcquireError),
}

/// Holds a passive instance short of takeover while `check` says its active
/// startup would fail and another instance holds the mutex on `path`.
///
/// `check` answers `Ok(None)` when nothing blocks takeover and `Ok(Some(_))`
/// with what does: a predicted startup failure, or why the check couldn't
/// tell this time, which blocks too, since only a check that passes opens the
/// gate. An error ends the gate. While blocked and the mutex is held, the
/// gate logs the block at warn when first found, when it changes and every
/// [`TAKEOVER_BLOCK_REMINDER_INTERVAL`], and checks again every
/// [`TAKEOVER_RECHECK_INTERVAL`]. The caller keeps readiness withheld until
/// [`TakeoverGate::Open`], so a surge rollout stalls with the predecessor
/// serving. A block with the mutex free has no predecessor to protect, so the
/// gate takes the mutex and lets startup fail loudly rather than wait.
pub async fn gate_takeover<C, Fut, B, E>(
    path: &Path,
    shutdown_token: &CancellationToken,
    mut check: C,
) -> Result<TakeoverGate, TakeoverGateError<E>>
where
    C: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<B>, E>>,
    B: Display + PartialEq,
{
    let mut reported: Option<(B, tokio::time::Instant)> = None;
    loop {
        if shutdown_token.is_cancelled() {
            return Ok(TakeoverGate::PassiveExit);
        }
        let Some(block) = check().await.map_err(TakeoverGateError::Check)? else {
            return Ok(TakeoverGate::Open);
        };
        if let Some(mutex) = ServiceMutex::try_acquire(path).map_err(TakeoverGateError::Mutex)? {
            tracing::warn!(
                service_mutex = %path.display(),
                "no other instance holds the service mutex, so taking it and starting even \
                 though the takeover check blocks: {block}"
            );
            return Ok(TakeoverGate::Acquired(mutex));
        }
        let now = tokio::time::Instant::now();
        let notice = match &reported {
            None => Some("detected"),
            Some((previous, _)) if *previous != block => Some("changed"),
            Some((_, at)) if now.duration_since(*at) >= TAKEOVER_BLOCK_REMINDER_INTERVAL => {
                Some("reminder")
            }
            Some(_) => None,
        };
        if let Some(notice) = notice {
            tracing::warn!(
                service_mutex = %path.display(),
                notice,
                "withholding takeover readiness while another instance holds the service \
                 mutex: {block}"
            );
            reported = Some((block, now));
        }
        tokio::select! {
            _ = shutdown_token.cancelled() => return Ok(TakeoverGate::PassiveExit),
            _ = tokio::time::sleep(TAKEOVER_RECHECK_INTERVAL) => {}
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
