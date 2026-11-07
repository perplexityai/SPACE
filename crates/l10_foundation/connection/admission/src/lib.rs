//! Bounded accepted-connection admission for local service transports.
//!
//! Tonic applies service layers after a transport has been accepted. Those
//! layers cannot bound idle HTTP/2 connections or protect the listener's file
//! descriptor reserve. This module acquires capacity before polling `accept`,
//! holds it for the physical connection lifetime, and owns retry/backoff and
//! terminal listener-error policy outside tonic.

use std::future::Future;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tonic::transport::server::Connected;

const INITIAL_ACCEPT_BACKOFF: Duration = Duration::from_millis(100);
const MAX_ACCEPT_BACKOFF: Duration = Duration::from_secs(1);
const ACCEPT_ERROR_LOG_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionAdmissionConfig {
    listener_name: &'static str,
    max_connections: usize,
}

impl ConnectionAdmissionConfig {
    pub const fn new(listener_name: &'static str, max_connections: usize) -> Self {
        assert!(max_connections > 0, "connection limit must be nonzero");
        Self {
            listener_name,
            max_connections,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptErrorReason {
    Emfile,
    Enfile,
    Interrupted,
    Other,
}

impl AcceptErrorReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Emfile => "emfile",
            Self::Enfile => "enfile",
            Self::Interrupted => "interrupted",
            Self::Other => "other",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcceptErrorDisposition {
    RetryImmediately,
    RetryWithBackoff,
    Fatal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionAdmissionEvent {
    CapacityExhausted,
    AcceptError(AcceptErrorReason),
    AcceptBackoff(AcceptErrorReason),
}

impl ConnectionAdmissionEvent {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CapacityExhausted => "capacity_exhausted",
            Self::AcceptError(_) => "accept_error",
            Self::AcceptBackoff(_) => "accept_backoff",
        }
    }

    pub const fn reason(self) -> Option<AcceptErrorReason> {
        match self {
            Self::CapacityExhausted => None,
            Self::AcceptError(reason) | Self::AcceptBackoff(reason) => Some(reason),
        }
    }
}

/// One physical-listener admission event and whether it belongs to the
/// rate-bounded diagnostic sample.
///
/// Exact counters should consume every observation. Sinks where each metric
/// is also a log record should consume only observations for which
/// [`Self::is_diagnostic_sample`] is true.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionAdmissionObservation {
    event: ConnectionAdmissionEvent,
    diagnostic_sample: bool,
}

impl ConnectionAdmissionObservation {
    const fn new(event: ConnectionAdmissionEvent, diagnostic_sample: bool) -> Self {
        Self {
            event,
            diagnostic_sample,
        }
    }

    pub const fn event(self) -> ConnectionAdmissionEvent {
        self.event
    }

    pub const fn is_diagnostic_sample(self) -> bool {
        self.diagnostic_sample
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AcceptErrorAction {
    pub reason: AcceptErrorReason,
    pub pause: Duration,
    /// Whether the listener owner can safely retry this accept failure.
    /// Fatal errors must terminate the owning service rather than leave a
    /// live process whose listener can never make progress.
    pub retry: bool,
    pub log: bool,
    pub suppressed: u64,
}

#[derive(Clone, Copy, Default)]
struct AcceptErrorDiagnosticState {
    last_logged_at: Option<Instant>,
    suppressed: u64,
}

#[derive(Default)]
struct AcceptErrorDiagnostics {
    emfile: AcceptErrorDiagnosticState,
    enfile: AcceptErrorDiagnosticState,
    interrupted: AcceptErrorDiagnosticState,
    other: AcceptErrorDiagnosticState,
}

impl AcceptErrorDiagnostics {
    fn for_reason_mut(&mut self, reason: AcceptErrorReason) -> &mut AcceptErrorDiagnosticState {
        match reason {
            AcceptErrorReason::Emfile => &mut self.emfile,
            AcceptErrorReason::Enfile => &mut self.enfile,
            AcceptErrorReason::Interrupted => &mut self.interrupted,
            AcceptErrorReason::Other => &mut self.other,
        }
    }
}

/// State for one listener's consecutive `accept()` failures.
#[derive(Default)]
pub struct AcceptErrors {
    backoff: Duration,
    // Immediate per-connection failures must not suppress the first diagnostic
    // for resource pressure, even when both have the coarse `other` reason.
    // Fatal failures bypass sampling altogether.
    immediate_diagnostics: AcceptErrorDiagnostics,
    backoff_diagnostics: AcceptErrorDiagnostics,
}

impl AcceptErrors {
    pub fn reset(&mut self) {
        self.backoff = Duration::ZERO;
    }

    pub fn record(&mut self, error: &io::Error) -> AcceptErrorAction {
        let reason = classify_accept_error(error);
        let disposition = classify_accept_error_disposition(error);
        let retry = disposition != AcceptErrorDisposition::Fatal;
        let pause = if disposition == AcceptErrorDisposition::RetryWithBackoff {
            next_backoff(self.backoff)
        } else {
            Duration::ZERO
        };
        let (log, suppressed) = match disposition {
            AcceptErrorDisposition::RetryImmediately => {
                record_diagnostic_sample(self.immediate_diagnostics.for_reason_mut(reason))
            }
            AcceptErrorDisposition::RetryWithBackoff => {
                record_diagnostic_sample(self.backoff_diagnostics.for_reason_mut(reason))
            }
            AcceptErrorDisposition::Fatal => (true, self.take_suppressed(reason)),
        };
        self.backoff = pause;
        AcceptErrorAction {
            reason,
            pause,
            retry,
            log,
            suppressed,
        }
    }

    fn take_suppressed(&mut self, reason: AcceptErrorReason) -> u64 {
        let immediate = self.immediate_diagnostics.for_reason_mut(reason);
        let immediate_suppressed = immediate.suppressed;
        immediate.suppressed = 0;

        let backoff = self.backoff_diagnostics.for_reason_mut(reason);
        let backoff_suppressed = backoff.suppressed;
        backoff.suppressed = 0;

        immediate_suppressed.saturating_add(backoff_suppressed)
    }
}

fn record_diagnostic_sample(diagnostics: &mut AcceptErrorDiagnosticState) -> (bool, u64) {
    let now = Instant::now();
    let log = diagnostics
        .last_logged_at
        .is_none_or(|last| now.duration_since(last) >= ACCEPT_ERROR_LOG_INTERVAL);
    let suppressed = if log { diagnostics.suppressed } else { 0 };
    if log {
        diagnostics.last_logged_at = Some(now);
        diagnostics.suppressed = 0;
    } else {
        diagnostics.suppressed = diagnostics.suppressed.saturating_add(1);
    }
    (log, suppressed)
}

#[derive(Debug, thiserror::Error)]
#[error("{listener_name} listener failed: {source}")]
pub struct ConnectionAdmissionFailure {
    listener_name: &'static str,
    #[source]
    source: io::Error,
}

impl ConnectionAdmissionFailure {
    fn new(listener_name: &'static str, source: io::Error) -> Self {
        Self {
            listener_name,
            source,
        }
    }
}

struct ConnectionAdmissionStatus {
    failure: Arc<Mutex<Option<ConnectionAdmissionFailure>>>,
    failure_notify: Arc<tokio::sync::Notify>,
}

impl ConnectionAdmissionStatus {
    /// Returns a terminal failure already recorded before the admitted IO
    /// stream ended. Production servers should use [`Self::wait_for_failure`]
    /// so tonic cannot turn listener failure into a graceful, potentially
    /// unbounded connection drain.
    #[cfg(test)]
    fn take_failure(self) -> Option<ConnectionAdmissionFailure> {
        self.failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// Waits for a terminal listener failure. Graceful shutdown and consumer
    /// cancellation deliberately leave this pending so the server future owns
    /// its normal drain behavior.
    async fn wait_for_failure(self) -> ConnectionAdmissionFailure {
        loop {
            if let Some(failure) = self
                .failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
            {
                return failure;
            }
            // `notify_one` retains a permit when no waiter is registered, so a
            // failure recorded between the check and this await is not lost.
            self.failure_notify.notified().await;
        }
    }
}

#[must_use = "serve the admitted stream and check its terminal status"]
pub struct AdmittedConnections<IO> {
    incoming: ReceiverStream<Result<AdmittedConnection<IO>, io::Error>>,
    status: ConnectionAdmissionStatus,
}

impl<IO> AdmittedConnections<IO> {
    /// Runs the admitted stream through its server and gives terminal listener
    /// failure priority over the server result.
    ///
    /// Tonic consumes incoming-stream errors and may then drain existing
    /// long-lived connections indefinitely. Owning the race here makes it
    /// impossible for a server caller to omit the listener-failure path.
    pub async fn supervise<BuildServer, Server, Output>(
        self,
        build_server: BuildServer,
    ) -> Result<Output, ConnectionAdmissionFailure>
    where
        BuildServer: FnOnce(ReceiverStream<Result<AdmittedConnection<IO>, io::Error>>) -> Server,
        Server: Future<Output = Output>,
    {
        let Self { incoming, status } = self;
        let server = build_server(incoming);
        let admission_failure = status.wait_for_failure();
        tokio::pin!(server);
        tokio::pin!(admission_failure);
        tokio::select! {
            biased;
            failure = &mut admission_failure => Err(failure),
            result = &mut server => Ok(result),
        }
    }

    #[cfg(test)]
    fn into_parts(
        self,
    ) -> (
        ReceiverStream<Result<AdmittedConnection<IO>, io::Error>>,
        ConnectionAdmissionStatus,
    ) {
        (self.incoming, self.status)
    }
}

struct AdmissionTaskGuard {
    listener_name: &'static str,
    failure: Arc<Mutex<Option<ConnectionAdmissionFailure>>>,
    failure_notify: Arc<tokio::sync::Notify>,
    armed: bool,
}

impl AdmissionTaskGuard {
    fn new(
        listener_name: &'static str,
        failure: Arc<Mutex<Option<ConnectionAdmissionFailure>>>,
        failure_notify: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            listener_name,
            failure,
            failure_notify,
            armed: true,
        }
    }

    fn fail(&mut self, source: io::Error) {
        self.store_failure(source);
        self.armed = false;
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    fn store_failure(&self, source: io::Error) {
        let mut failure = self.failure.lock().unwrap_or_else(PoisonError::into_inner);
        if failure.is_none() {
            *failure = Some(ConnectionAdmissionFailure::new(self.listener_name, source));
            self.failure_notify.notify_one();
        }
    }
}

impl Drop for AdmissionTaskGuard {
    fn drop(&mut self) {
        if self.armed {
            self.store_failure(io::Error::other(
                "connection admission task stopped unexpectedly",
            ));
        }
    }
}

#[derive(Debug)]
pub struct AdmittedConnection<IO> {
    inner: IO,
    _admission: OwnedSemaphorePermit,
}

impl<IO: AsyncRead + Unpin> AsyncRead for AdmittedConnection<IO> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buffer)
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for AdmittedConnection<IO> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buffer)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

impl<IO: Connected> Connected for AdmittedConnection<IO> {
    type ConnectInfo = IO::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}

/// Wraps an accepted-IO stream with a stable physical-connection limit.
///
/// Capacity is acquired before the source stream is polled, so connections
/// beyond the limit remain in the kernel listener backlog. Recoverable accept
/// errors are retried here so resource pressure receives bounded backoff
/// instead of being consumed inside tonic's accept path. The returned value
/// exposes only [`AdmittedConnections::supervise`], which prevents tonic from
/// hiding a terminal listener failure behind an unbounded graceful drain.
///
/// `observe` receives every admission event with the listener name. Each
/// observation also identifies the rate-bounded diagnostic sample: a
/// continuously failing or saturated listener samples each class at most once
/// per [`ACCEPT_ERROR_LOG_INTERVAL`], and terminal errors are always sampled.
pub fn admit_connections<S, IO, Observe>(
    mut incoming: S,
    shutdown: CancellationToken,
    config: ConnectionAdmissionConfig,
    observe: Observe,
) -> AdmittedConnections<IO>
where
    S: Stream<Item = Result<IO, io::Error>> + Send + Unpin + 'static,
    IO: Send + 'static,
    Observe: Fn(&'static str, ConnectionAdmissionObservation) + Send + 'static,
{
    let slots = Arc::new(tokio::sync::Semaphore::new(config.max_connections));
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let failure = Arc::new(Mutex::new(None));
    let failure_notify = Arc::new(tokio::sync::Notify::new());
    let status = ConnectionAdmissionStatus {
        failure: Arc::clone(&failure),
        failure_notify: Arc::clone(&failure_notify),
    };
    tokio::spawn(async move {
        let mut task_guard = AdmissionTaskGuard::new(config.listener_name, failure, failure_notify);
        let mut accept_errors = AcceptErrors::default();
        let mut last_capacity_log = None;
        loop {
            if slots.available_permits() == 0 {
                let now = Instant::now();
                let diagnostic_sample = last_capacity_log
                    .is_none_or(|last| now.duration_since(last) >= ACCEPT_ERROR_LOG_INTERVAL);
                observe(
                    config.listener_name,
                    ConnectionAdmissionObservation::new(
                        ConnectionAdmissionEvent::CapacityExhausted,
                        diagnostic_sample,
                    ),
                );
                if diagnostic_sample {
                    tracing::warn!(
                        listener = config.listener_name,
                        max_connections = config.max_connections,
                        "service listener connection capacity exhausted"
                    );
                    last_capacity_log = Some(now);
                }
            }
            let admission = tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    task_guard.disarm();
                    return;
                },
                _ = tx.closed() => {
                    task_guard.disarm();
                    return;
                },
                result = Arc::clone(&slots).acquire_owned() => match result {
                    Ok(admission) => admission,
                    Err(_) => {
                        task_guard.fail(io::Error::other(
                            "connection admission semaphore closed unexpectedly",
                        ));
                        return;
                    },
                },
            };
            let accepted = tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    task_guard.disarm();
                    return;
                },
                _ = tx.closed() => {
                    task_guard.disarm();
                    return;
                },
                result = incoming.next() => match result {
                    Some(result) => result,
                    None => {
                        task_guard.fail(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incoming listener stream ended",
                        ));
                        return;
                    },
                },
            };
            match accepted {
                Ok(inner) => {
                    accept_errors.reset();
                    let admitted = Ok(AdmittedConnection {
                        inner,
                        _admission: admission,
                    });
                    if send_or_shutdown(&tx, admitted, &shutdown).await.is_err() {
                        task_guard.disarm();
                        return;
                    }
                }
                Err(error) => {
                    drop(admission);
                    let action = accept_errors.record(&error);
                    observe(
                        config.listener_name,
                        ConnectionAdmissionObservation::new(
                            ConnectionAdmissionEvent::AcceptError(action.reason),
                            action.log,
                        ),
                    );
                    if action.log {
                        tracing::warn!(
                            listener = config.listener_name,
                            %error,
                            reason = action.reason.as_str(),
                            suppressed = action.suppressed,
                            backoff_ms = u64::try_from(action.pause.as_millis())
                                .unwrap_or(u64::MAX),
                            retry = action.retry,
                            "service listener accept error"
                        );
                    }
                    if !action.retry {
                        task_guard.fail(error);
                        return;
                    }
                    if !action.pause.is_zero() {
                        observe(
                            config.listener_name,
                            ConnectionAdmissionObservation::new(
                                ConnectionAdmissionEvent::AcceptBackoff(action.reason),
                                action.log,
                            ),
                        );
                        tokio::select! {
                            biased;
                            _ = shutdown.cancelled() => {
                                task_guard.disarm();
                                return;
                            },
                            _ = tx.closed() => {
                                task_guard.disarm();
                                return;
                            },
                            _ = tokio::time::sleep(action.pause) => {}
                        }
                    }
                }
            }
        }
    });
    AdmittedConnections {
        incoming: ReceiverStream::new(rx),
        status,
    }
}

async fn send_or_shutdown<T>(
    tx: &tokio::sync::mpsc::Sender<T>,
    value: T,
    shutdown: &CancellationToken,
) -> Result<(), ()> {
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => Err(()),
        result = tx.send(value) => result.map_err(|_| ()),
    }
}

fn classify_accept_error(error: &io::Error) -> AcceptErrorReason {
    match error.raw_os_error() {
        Some(libc::EMFILE) => AcceptErrorReason::Emfile,
        Some(libc::ENFILE) => AcceptErrorReason::Enfile,
        _ if error.kind() == io::ErrorKind::Interrupted => AcceptErrorReason::Interrupted,
        _ => AcceptErrorReason::Other,
    }
}

fn classify_accept_error_disposition(error: &io::Error) -> AcceptErrorDisposition {
    if is_permanent_listener_error(error) {
        return AcceptErrorDisposition::Fatal;
    }
    if is_immediate_retry_accept_error(error) {
        return AcceptErrorDisposition::RetryImmediately;
    }
    // Resource-pressure and unknown accept errors do not prove that the
    // listening socket is unusable. Preserve the data plane and avoid a hot
    // loop while the condition clears or supervision observes repeated
    // failures.
    AcceptErrorDisposition::RetryWithBackoff
}

fn is_immediate_retry_accept_error(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::TimedOut
    ) {
        return true;
    }
    linux_pending_connection_error(error.raw_os_error())
}

#[cfg(target_os = "linux")]
fn linux_pending_connection_error(raw: Option<i32>) -> bool {
    matches!(
        raw,
        Some(
            libc::ENETDOWN
                | libc::EPROTO
                | libc::ENOPROTOOPT
                | libc::EHOSTDOWN
                | libc::ENONET
                | libc::EHOSTUNREACH
                | libc::EOPNOTSUPP
                | libc::ENETUNREACH
                | libc::EPERM
                | libc::ENOSR
                | libc::ESOCKTNOSUPPORT
                | libc::EPROTONOSUPPORT
        )
    )
}

#[cfg(not(target_os = "linux"))]
fn linux_pending_connection_error(_raw: Option<i32>) -> bool {
    false
}

#[cfg(unix)]
fn is_permanent_listener_error(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EBADF | libc::ENOTSOCK | libc::EINVAL | libc::EFAULT)
    )
}

#[cfg(not(unix))]
fn is_permanent_listener_error(_error: &io::Error) -> bool {
    false
}

fn next_backoff(current: Duration) -> Duration {
    if current.is_zero() {
        return INITIAL_ACCEPT_BACKOFF;
    }
    current.saturating_mul(2).min(MAX_ACCEPT_BACKOFF)
}

#[cfg(test)]
mod tests;
