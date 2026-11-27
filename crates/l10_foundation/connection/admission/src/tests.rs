use std::io::IoSlice;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::Connected;

use super::{
    AcceptErrorReason, AcceptErrors, AdmittedConnection, ConnectionAdmissionConfig,
    ConnectionAdmissionEvent, INITIAL_ACCEPT_BACKOFF, MAX_ACCEPT_BACKOFF, admit_connections,
};

const TEST_CONFIG: ConnectionAdmissionConfig = ConnectionAdmissionConfig::new("test-listener", 2);

struct PendingSource {
    dropped: Arc<AtomicUsize>,
}

impl tokio_stream::Stream for PendingSource {
    type Item = Result<UnixStream, std::io::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl Drop for PendingSource {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

struct VectoredWriter {
    calls: Arc<AtomicUsize>,
}

impl AsyncWrite for VectoredWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Err(std::io::Error::other(
            "scalar write must not replace a supported vectored write",
        )))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(Ok(buffers.iter().map(|buffer| buffer.len()).sum()))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn admitted_connection_preserves_vectored_writes() {
    let calls = Arc::new(AtomicUsize::new(0));
    let permit = Arc::new(tokio::sync::Semaphore::new(1))
        .acquire_owned()
        .await
        .expect("open test semaphore");
    let mut admitted = AdmittedConnection {
        inner: VectoredWriter {
            calls: Arc::clone(&calls),
        },
        _admission: permit,
    };

    assert!(admitted.is_write_vectored());
    let buffers = [IoSlice::new(b"first"), IoSlice::new(b"second")];
    let written = admitted
        .write_vectored(&buffers)
        .await
        .expect("vectored write");

    assert_eq!(written, 11);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn permit_is_held_for_connection_lifetime() {
    let (first, _first_peer) = UnixStream::pair().expect("first stream pair");
    let (second, _second_peer) = UnixStream::pair().expect("second stream pair");
    let (third, _third_peer) = UnixStream::pair().expect("third stream pair");
    let source = tokio_stream::iter([Ok(first), Ok(second), Ok(third)]);
    let shutdown = CancellationToken::new();
    let capacity_waits = Arc::new(AtomicUsize::new(0));
    let observed_capacity_waits = Arc::clone(&capacity_waits);
    let admission = admit_connections(
        source,
        shutdown,
        TEST_CONFIG,
        move |_listener, observation| {
            if observation.event() == ConnectionAdmissionEvent::CapacityExhausted {
                observed_capacity_waits.fetch_add(1, Ordering::Relaxed);
            }
        },
    );
    let (mut incoming, _status) = admission.into_parts();

    let first = incoming
        .next()
        .await
        .expect("first item")
        .expect("first IO");
    let _second = incoming
        .next()
        .await
        .expect("second item")
        .expect("second IO");
    assert!(
        tokio::time::timeout(Duration::from_millis(20), incoming.next())
            .await
            .is_err(),
        "capacity must remain charged while the accepted IO is alive"
    );
    assert_eq!(capacity_waits.load(Ordering::Relaxed), 1);

    drop(first);
    tokio::time::timeout(Duration::from_secs(1), incoming.next())
        .await
        .expect("released capacity must unblock admission")
        .expect("third item")
        .expect("third IO");
}

#[tokio::test(start_paused = true)]
async fn capacity_observations_are_exact_while_diagnostic_samples_are_bounded() {
    let (first, _first_peer) = UnixStream::pair().expect("first stream pair");
    let (second, _second_peer) = UnixStream::pair().expect("second stream pair");
    let source = tokio_stream::iter([Ok(first), Ok(second)]).chain(tokio_stream::pending());
    let observed = Arc::new(Mutex::new(Vec::new()));
    let observed_capacity = Arc::clone(&observed);
    let admission = admit_connections(
        source,
        CancellationToken::new(),
        ConnectionAdmissionConfig::new("test-listener", 1),
        move |_listener, observation| {
            if observation.event() == ConnectionAdmissionEvent::CapacityExhausted {
                observed_capacity
                    .lock()
                    .expect("capacity capture lock")
                    .push(observation.is_diagnostic_sample());
            }
        },
    );
    let (mut incoming, _status) = admission.into_parts();

    let first = incoming
        .next()
        .await
        .expect("first item")
        .expect("first IO");
    tokio::task::yield_now().await;
    drop(first);
    let _second = incoming
        .next()
        .await
        .expect("second item")
        .expect("second IO");
    tokio::task::yield_now().await;

    assert_eq!(
        *observed.lock().expect("capacity capture lock"),
        [true, false],
        "each capacity wait must be observable while diagnostics stay sampled"
    );
}

#[tokio::test]
async fn admitted_connection_preserves_peer_credentials() {
    let (accepted, _peer) = UnixStream::pair().expect("stream pair");
    let expected = accepted
        .connect_info()
        .peer_cred
        .expect("source peer credentials");
    let source = tokio_stream::iter([Ok(accepted)]);
    let admission = admit_connections(source, CancellationToken::new(), TEST_CONFIG, |_, _| {});
    let (mut incoming, _status) = admission.into_parts();

    let admitted = incoming.next().await.expect("item").expect("admitted IO");
    let actual = admitted
        .connect_info()
        .peer_cred
        .expect("admitted peer credentials");
    assert_eq!(actual.pid(), expected.pid());
    assert_eq!(actual.uid(), expected.uid());
    assert_eq!(actual.gid(), expected.gid());
}

#[tokio::test]
async fn descriptor_exhaustion_retries_without_reaching_consumer() {
    let (accepted, _peer) = UnixStream::pair().expect("stream pair");
    let source = tokio_stream::iter([
        Err(std::io::Error::from_raw_os_error(libc::EMFILE)),
        Ok(accepted),
    ]);
    let backoffs = Arc::new(AtomicUsize::new(0));
    let observed_backoffs = Arc::clone(&backoffs);
    let admission = admit_connections(
        source,
        CancellationToken::new(),
        TEST_CONFIG,
        move |_listener, observation| {
            if observation.event()
                == ConnectionAdmissionEvent::AcceptBackoff(AcceptErrorReason::Emfile)
            {
                observed_backoffs.fetch_add(1, Ordering::Relaxed);
            }
        },
    );
    let (mut incoming, _status) = admission.into_parts();

    tokio::time::timeout(Duration::from_secs(1), incoming.next())
        .await
        .expect("recoverable failure must retry")
        .expect("later source item")
        .expect("recoverable failure must remain internal");
    assert_eq!(backoffs.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn diagnostic_events_are_named_and_rate_bounded() {
    let (accepted, _peer) = UnixStream::pair().expect("stream pair");
    let source = tokio_stream::iter([
        Err(std::io::Error::from_raw_os_error(libc::EMFILE)),
        Err(std::io::Error::from_raw_os_error(libc::EMFILE)),
        Ok(accepted),
    ]);
    let observed = Arc::new(Mutex::new(Vec::new()));
    let observed_events = Arc::clone(&observed);
    let admission = admit_connections(
        source,
        CancellationToken::new(),
        TEST_CONFIG,
        move |listener, observation| {
            observed_events.lock().expect("event capture lock").push((
                listener,
                observation.event(),
                observation.is_diagnostic_sample(),
            ));
        },
    );
    let (mut incoming, _status) = admission.into_parts();

    incoming
        .next()
        .await
        .expect("later source item")
        .expect("recoverable failures must remain internal");

    assert_eq!(
        *observed.lock().expect("event capture lock"),
        [
            (
                "test-listener",
                ConnectionAdmissionEvent::AcceptError(AcceptErrorReason::Emfile),
                true,
            ),
            (
                "test-listener",
                ConnectionAdmissionEvent::AcceptBackoff(AcceptErrorReason::Emfile),
                true,
            ),
            (
                "test-listener",
                ConnectionAdmissionEvent::AcceptError(AcceptErrorReason::Emfile),
                false,
            ),
            (
                "test-listener",
                ConnectionAdmissionEvent::AcceptBackoff(AcceptErrorReason::Emfile),
                false,
            ),
        ],
        "every event must be observable while only the first failure is sampled"
    );
}

#[tokio::test]
async fn fatal_accept_error_is_retained_for_supervision() {
    let source = tokio_stream::iter([Err::<UnixStream, _>(std::io::Error::from_raw_os_error(
        libc::EBADF,
    ))]);
    let admission = admit_connections(source, CancellationToken::new(), TEST_CONFIG, |_, _| {});
    let (mut incoming, status) = admission.into_parts();

    assert!(incoming.next().await.is_none());
    let failure = status
        .take_failure()
        .expect("fatal listener error must be retained outside tonic");
    assert!(
        failure
            .to_string()
            .starts_with("test-listener listener failed:")
    );
    let source = std::error::Error::source(&failure)
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .expect("admission failure must retain its io::Error source");
    assert_eq!(source.raw_os_error(), Some(libc::EBADF));
}

#[tokio::test]
async fn shutdown_interrupts_capacity_wait() {
    let (first, _first_peer) = UnixStream::pair().expect("first stream pair");
    let (second, _second_peer) = UnixStream::pair().expect("second stream pair");
    let source = tokio_stream::iter([Ok(first), Ok(second)]);
    let shutdown = CancellationToken::new();
    let admission = admit_connections(
        source,
        shutdown.clone(),
        ConnectionAdmissionConfig::new("test-listener", 1),
        |_, _| {},
    );
    let (mut incoming, status) = admission.into_parts();
    let _first = incoming
        .next()
        .await
        .expect("first item")
        .expect("first IO");

    shutdown.cancel();
    let next = tokio::time::timeout(Duration::from_secs(1), incoming.next())
        .await
        .expect("shutdown must stop admission");
    assert!(next.is_none());
    assert!(status.take_failure().is_none());
}

#[tokio::test]
async fn dropping_consumer_stops_admission_task_and_releases_source() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let admission = admit_connections(
        PendingSource {
            dropped: Arc::clone(&dropped),
        },
        CancellationToken::new(),
        TEST_CONFIG,
        |_, _| {},
    );
    let (incoming, status) = admission.into_parts();

    drop(incoming);
    tokio::time::timeout(Duration::from_secs(1), async {
        while dropped.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping the consumer must stop the detached admission task");
    assert!(status.take_failure().is_none());
}

#[test]
fn fatal_and_transient_errors_do_not_back_off_and_success_resets_backoff() {
    let mut errors = AcceptErrors::default();

    let fatal = errors.record(&std::io::Error::from_raw_os_error(libc::EBADF));
    assert!(fatal.pause.is_zero());
    assert!(!fatal.retry);
    for kind in [
        std::io::ErrorKind::ConnectionAborted,
        std::io::ErrorKind::ConnectionReset,
        std::io::ErrorKind::BrokenPipe,
        std::io::ErrorKind::Interrupted,
        std::io::ErrorKind::WouldBlock,
        std::io::ErrorKind::TimedOut,
    ] {
        let transient = errors.record(&std::io::Error::from(kind));
        assert!(transient.pause.is_zero(), "{kind:?} must retry immediately");
        assert!(transient.retry, "{kind:?} must remain retryable");
    }

    let exhausted = std::io::Error::from_raw_os_error(libc::EMFILE);
    let first_exhausted = errors.record(&exhausted);
    assert_eq!(first_exhausted.pause, INITIAL_ACCEPT_BACKOFF);
    assert!(first_exhausted.retry);
    assert_eq!(errors.record(&exhausted).pause, INITIAL_ACCEPT_BACKOFF * 2);
    for _ in 0..10 {
        errors.record(&exhausted);
    }
    assert_eq!(errors.record(&exhausted).pause, MAX_ACCEPT_BACKOFF);

    errors.reset();
    assert_eq!(errors.record(&exhausted).pause, INITIAL_ACCEPT_BACKOFF);
}

#[test]
fn immediate_retry_does_not_suppress_resource_pressure_diagnostics() {
    let mut errors = AcceptErrors::default();

    let immediate = errors.record(&std::io::Error::from(std::io::ErrorKind::ConnectionAborted));
    assert!(immediate.log);
    assert!(immediate.pause.is_zero());

    let descriptor_pressure = errors.record(&std::io::Error::from_raw_os_error(libc::EMFILE));
    assert!(descriptor_pressure.log);
    assert_eq!(descriptor_pressure.pause, INITIAL_ACCEPT_BACKOFF);

    errors.reset();
    let socket_memory_pressure = errors.record(&std::io::Error::from_raw_os_error(libc::ENOBUFS));
    assert!(
        socket_memory_pressure.log,
        "backoff diagnostics must be independent of immediate-retry diagnostics with the same coarse reason"
    );
    assert_eq!(socket_memory_pressure.pause, INITIAL_ACCEPT_BACKOFF);
}

#[test]
fn resource_pressure_and_unknown_errors_back_off_without_killing_listener() {
    let mut errors = AcceptErrors::default();

    for error in [
        std::io::Error::from_raw_os_error(libc::ENOBUFS),
        std::io::Error::from_raw_os_error(libc::ENOMEM),
        std::io::Error::other("unknown accept failure"),
    ] {
        errors.reset();
        let action = errors.record(&error);
        assert!(action.retry);
        assert_eq!(action.pause, INITIAL_ACCEPT_BACKOFF);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn linux_pending_connection_errors_retry_immediately() {
    let mut errors = AcceptErrors::default();

    for errno in [
        libc::ENETDOWN,
        libc::EPROTO,
        libc::ENOPROTOOPT,
        libc::EHOSTDOWN,
        libc::ENONET,
        libc::EHOSTUNREACH,
        libc::EOPNOTSUPP,
        libc::ENETUNREACH,
        libc::EPERM,
        libc::ENOSR,
        libc::ESOCKTNOSUPPORT,
        libc::EPROTONOSUPPORT,
    ] {
        let action = errors.record(&std::io::Error::from_raw_os_error(errno));
        assert!(
            action.retry,
            "errno {errno} must not terminate the listener"
        );
        assert!(
            action.pause.is_zero(),
            "errno {errno} belongs to one pending connection"
        );
    }
}

#[tokio::test]
async fn unexpected_source_end_is_a_terminal_listener_failure() {
    let admission = admit_connections(
        tokio_stream::empty::<Result<UnixStream, std::io::Error>>(),
        CancellationToken::new(),
        TEST_CONFIG,
        |_, _| {},
    );
    let (mut incoming, status) = admission.into_parts();

    assert!(incoming.next().await.is_none());
    let failure = status
        .take_failure()
        .expect("a production listener stream cannot end normally");
    assert_eq!(
        failure.to_string(),
        "test-listener listener failed: incoming listener stream ended"
    );
}

#[tokio::test]
async fn terminal_failure_survives_tonic_incoming_error_handling() {
    let (server_stream, _client_stream) = UnixStream::pair().expect("stream pair");
    let source = tokio_stream::iter([
        Ok(server_stream),
        Err(std::io::Error::from_raw_os_error(libc::EBADF)),
    ]);
    let admission = admit_connections(source, CancellationToken::new(), TEST_CONFIG, |_, _| {});
    let (_reporter, health) = tonic_health::server::health_reporter();

    let failure = tokio::time::timeout(
        Duration::from_secs(1),
        admission.supervise(move |incoming| {
            tonic::transport::Server::builder()
                .add_service(health)
                .serve_with_incoming_shutdown(incoming, std::future::pending::<()>())
        }),
    )
    .await
    .expect("listener failure must not wait for a long-lived connection to close")
    .expect_err("fatal listener failure must preempt tonic");
    let source = std::error::Error::source(&failure)
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .expect("admission failure must retain its io::Error source");
    assert_eq!(source.raw_os_error(), Some(libc::EBADF));
}

#[test]
fn fatal_error_bypasses_retry_log_throttle() {
    let mut errors = AcceptErrors::default();
    let retryable = std::io::Error::other("unknown retryable accept error");

    assert!(errors.record(&retryable).log);
    assert!(!errors.record(&retryable).log);

    let fatal = errors.record(&std::io::Error::from_raw_os_error(libc::EBADF));
    assert!(!fatal.retry);
    assert!(fatal.log);
    assert_eq!(fatal.suppressed, 1);
}

#[test]
fn repeated_accept_errors_are_log_throttled_across_success() {
    let mut errors = AcceptErrors::default();
    let error = std::io::Error::from_raw_os_error(libc::EMFILE);

    assert!(errors.record(&error).log);
    assert_eq!(errors.record(&error).pause, INITIAL_ACCEPT_BACKOFF * 2);
    errors.reset();

    let after_success = errors.record(&error);
    assert_eq!(after_success.pause, INITIAL_ACCEPT_BACKOFF);
    assert!(!after_success.log);
}
