//! Per-port accept loop that fronts a loopback listener on the VM's external
//! interface. Spawned by [`super::port_forwarder`] with an already-bound
//! listener (the bind itself — including retry/classification — is the
//! driver's job); each accepted connection lazily dials `127.0.0.1:port`.
//! The loop runs until the per-port `CancellationToken` fires (caller
//! invoked `LoopbackBridgeManager::close`, or service shutdown cascaded
//! from the parent token).

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

/// A forwarding listener that releases its port when dropped, including when
/// the per-port task is aborted. A child forked by a concurrent process spawn
/// holds a copy of the listening socket until it execs; closing only this
/// descriptor would keep the socket listening, so a reopen of the port would
/// fail with `EADDRINUSE` and report `Direct`. Shutting the socket down takes
/// every copy out of the listening state, and the port can be bound again.
struct ReleasingListener(TcpListener);

impl Drop for ReleasingListener {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        if let Err(error) =
            nix::sys::socket::shutdown(self.0.as_raw_fd(), nix::sys::socket::Shutdown::Both)
        {
            tracing::debug!(%error, "failed to shut down a released port-forward listener");
        }
    }
}

const LOOPBACK_DIAL_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const ACCEPT_FAILURE_WARNING_INTERVAL: Duration = Duration::from_secs(60);
static ACCEPT_FAILURE_DIAGNOSTICS: LazyLock<AcceptFailureDiagnostics> =
    LazyLock::new(AcceptFailureDiagnostics::default);

#[derive(Default)]
struct AcceptFailureDiagnostics {
    count: AtomicU64,
    last_warning: Mutex<Option<Instant>>,
}

impl AcceptFailureDiagnostics {
    fn record(&self, bind_addr: SocketAddr, error: &std::io::Error) {
        self.count.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let mut last_warning = self
            .last_warning
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if last_warning
            .is_some_and(|last| now.duration_since(last) < ACCEPT_FAILURE_WARNING_INTERVAL)
        {
            return;
        }
        *last_warning = Some(now);
        let failures_since_last_warning = self.count.swap(0, Ordering::Relaxed);
        tracing::warn!(
            %bind_addr,
            %error,
            failures_since_last_warning,
            "port-forward: accept failed (further warnings throttled)"
        );
    }
}

pub(super) async fn run(
    listener: TcpListener,
    bind_addr: SocketAddr,
    dial_addr: SocketAddr,
    active_connections: std::sync::Arc<Semaphore>,
    idle_timeout: Duration,
    cancel: CancellationToken,
) {
    tracing::info!(%bind_addr, %dial_addr, "port-forward: listening");
    let listener = ReleasingListener(listener);
    let listener = &listener.0;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!(%bind_addr, "port-forward: cancelled");
                return;
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((client, peer)) => {
                        // Hold at most one accepted socket per listener while
                        // waiting for shared capacity. This applies bounded
                        // backpressure through the kernel backlog instead of
                        // completing the TCP handshake and immediately
                        // resetting clients during ordinary saturation.
                        let connection_permit = tokio::select! {
                            biased;
                            _ = cancel.cancelled() => return,
                            permit = active_connections.clone().acquire_owned() => {
                                match permit {
                                    Ok(permit) => permit,
                                    Err(_) => return,
                                }
                            }
                        };
                        let cancel = cancel.clone();
                        tokio::spawn(async move {
                            if let Err(error) = handle_connection(
                                client,
                                peer,
                                dial_addr,
                                idle_timeout,
                                connection_permit,
                                cancel,
                            ).await {
                                tracing::debug!(
                                    %bind_addr, %peer, %dial_addr, %error,
                                    "port-forward: connection ended with error"
                                );
                            }
                        });
                    }
                    Err(error) => {
                        ACCEPT_FAILURE_DIAGNOSTICS.record(bind_addr, &error);
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        }
    }
}

async fn handle_connection(
    client: TcpStream,
    peer: SocketAddr,
    dial_addr: SocketAddr,
    idle_timeout: Duration,
    _connection_permit: OwnedSemaphorePermit,
    cancel: CancellationToken,
) -> std::io::Result<()> {
    if let Err(error) = client.set_nodelay(true) {
        tracing::debug!(%peer, %error, "port-forward: failed to set client TCP_NODELAY");
    }

    let upstream = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Ok(()),
        result = tokio::time::timeout(LOOPBACK_DIAL_TIMEOUT, TcpStream::connect(dial_addr)) => {
            match result {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => {
                    tracing::debug!(%peer, %dial_addr, %error, "port-forward: loopback dial refused");
                    return Err(error);
                }
                Err(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "loopback dial timed out",
                    ));
                }
            }
        }
    };
    if let Err(error) = upstream.set_nodelay(true) {
        tracing::debug!(%peer, %dial_addr, %error, "port-forward: failed to set upstream TCP_NODELAY");
    }

    relay_with_idle_timeout(client, upstream, idle_timeout, cancel).await
}

/// Reader wrapper that records progress in one shared atomic timestamp. The
/// watchdog samples it periodically instead of re-arming a timer for every
/// copied chunk.
struct ActivityReader<R> {
    inner: R,
    activity: ActivityClock,
}

#[derive(Clone)]
struct ActivityClock {
    origin: tokio::time::Instant,
    last_activity_nanos: Arc<AtomicU64>,
}

impl ActivityClock {
    fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
            last_activity_nanos: Arc::new(AtomicU64::new(0)),
        }
    }

    fn elapsed_nanos(&self) -> u64 {
        self.origin.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
    }

    fn record(&self) {
        self.last_activity_nanos
            .store(self.elapsed_nanos(), Ordering::Relaxed);
    }

    fn idle_for(&self) -> Duration {
        Duration::from_nanos(
            self.elapsed_nanos()
                .saturating_sub(self.last_activity_nanos.load(Ordering::Relaxed)),
        )
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ActivityReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            self.activity.record();
        }
        result
    }
}

async fn relay_with_idle_timeout(
    client: TcpStream,
    upstream: TcpStream,
    idle_timeout: Duration,
    cancel: CancellationToken,
) -> std::io::Result<()> {
    let (client_read, mut client_write) = client.into_split();
    let (upstream_read, mut upstream_write) = upstream.into_split();
    let activity = ActivityClock::new();
    let mut client_read = ActivityReader {
        inner: client_read,
        activity: activity.clone(),
    };
    let mut upstream_read = ActivityReader {
        inner: upstream_read,
        activity: activity.clone(),
    };
    let client_to_upstream = async {
        tokio::io::copy(&mut client_read, &mut upstream_write).await?;
        upstream_write.shutdown().await
    };
    let upstream_to_client = async {
        tokio::io::copy(&mut upstream_read, &mut client_write).await?;
        client_write.shutdown().await
    };
    let relay = async { tokio::try_join!(client_to_upstream, upstream_to_client).map(|_| ()) };
    tokio::pin!(relay);
    let check_interval = (idle_timeout / 4)
        .max(Duration::from_millis(1))
        .min(MAX_IDLE_CHECK_INTERVAL);
    let mut idle_check = tokio::time::interval(check_interval);
    idle_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            result = &mut relay => return result,
            _ = idle_check.tick() => {
                if activity.idle_for() >= idle_timeout {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "forwarded connection idle timeout",
                    ));
                }
            }
        }
    }
}
