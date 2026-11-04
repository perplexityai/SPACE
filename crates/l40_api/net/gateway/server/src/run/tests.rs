use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_stream::StreamExt;

#[tokio::test]
async fn control_connection_admission_holds_until_connection_drop() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let socket_path = temp_dir.path().join("control.sock");
    let listener = UnixListener::bind(&socket_path).expect("control listener");
    let shutdown = CancellationToken::new();
    let admission = bounded_control_connections(listener, shutdown.clone());
    admission
        .supervise(move |mut incoming| async move {
            let mut clients = Vec::new();
            let mut admitted = Vec::new();

            for _ in 0..MAX_CONTROL_CONNECTIONS {
                clients.push(
                    UnixStream::connect(&socket_path)
                        .await
                        .expect("client connection"),
                );
                admitted.push(
                    incoming
                        .next()
                        .await
                        .expect("incoming stream")
                        .expect("accepted connection"),
                );
            }

            clients.push(
                UnixStream::connect(&socket_path)
                    .await
                    .expect("queued client connection"),
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(20), incoming.next())
                    .await
                    .is_err(),
                "the listener must not accept beyond its descriptor budget"
            );

            drop(admitted.pop());
            tokio::time::timeout(Duration::from_secs(1), incoming.next())
                .await
                .expect("released capacity should unblock accept")
                .expect("incoming stream")
                .expect("accepted queued connection");
            shutdown.cancel();
        })
        .await
        .expect("listener admission must remain healthy");
}

#[tokio::test]
async fn ingress_partition_leaves_control_connection_headroom() {
    use crate::resource_budget::MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS;

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let socket_path = temp_dir.path().join("control.sock");
    let listener = UnixListener::bind(&socket_path).expect("control listener");
    let shutdown = CancellationToken::new();
    let admission = bounded_control_connections(listener, shutdown.clone());
    admission
        .supervise(move |mut incoming| async move {
            let mut clients = Vec::new();
            let mut admitted = Vec::new();

            for _ in 0..MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS {
                clients.push(
                    UnixStream::connect(&socket_path)
                        .await
                        .expect("ingress client"),
                );
                admitted.push(
                    incoming
                        .next()
                        .await
                        .expect("incoming stream")
                        .expect("admitted ingress transport"),
                );
            }

            clients.push(
                UnixStream::connect(&socket_path)
                    .await
                    .expect("administrative client"),
            );
            admitted.push(
                tokio::time::timeout(Duration::from_secs(1), incoming.next())
                    .await
                    .expect("ingress partition must leave control headroom")
                    .expect("incoming stream")
                    .expect("admitted administrative transport"),
            );
            shutdown.cancel();
        })
        .await
        .expect("listener admission must remain healthy");
}

#[tokio::test]
async fn fd_exhaustion_backs_off_without_reaching_tonic() {
    let (accepted, _peer) = UnixStream::pair().expect("unix stream pair");
    let source = tokio_stream::iter([
        Err(std::io::Error::from_raw_os_error(libc::EMFILE)),
        Err(std::io::Error::from_raw_os_error(libc::EMFILE)),
        Ok(accepted),
    ])
    .chain(tokio_stream::pending());
    let shutdown = CancellationToken::new();
    let backoffs = Arc::new(AtomicUsize::new(0));
    let observed_backoffs = Arc::clone(&backoffs);
    let admission = bounded_control_connections_from_stream(source, shutdown, move |reason| {
        assert_eq!(reason, "emfile");
        observed_backoffs.fetch_add(1, Ordering::Relaxed);
    });
    admission
        .supervise(|mut incoming| async move {
            tokio::time::timeout(Duration::from_secs(1), incoming.next())
                .await
                .expect("recoverable accept error must retry")
                .expect("source must produce the later connection")
                .expect("recoverable accept error must not reach tonic");
        })
        .await
        .expect("recoverable error must not fail supervision");
    assert_eq!(
        backoffs.load(Ordering::Relaxed),
        2,
        "the legacy gateway counter must observe every backoff"
    );
}

#[tokio::test]
async fn fatal_accept_error_reaches_supervision_without_backoff() {
    let source = tokio_stream::iter([Err(std::io::Error::from_raw_os_error(libc::EBADF))]);
    let backoffs = Arc::new(AtomicUsize::new(0));
    let observed_backoffs = Arc::clone(&backoffs);
    let admission =
        bounded_control_connections_from_stream(source, CancellationToken::new(), move |_| {
            observed_backoffs.fetch_add(1, Ordering::Relaxed);
        });
    let failure = tokio::time::timeout(
        Duration::from_secs(1),
        admission.supervise(|incoming| async move {
            std::future::pending::<()>().await;
            drop(incoming);
        }),
    )
    .await
    .expect("fatal accept error must reach supervision promptly")
    .expect_err("fatal error must bypass tonic incoming-error handling");
    assert!(failure.to_string().contains("Bad file descriptor"));
    assert_eq!(backoffs.load(Ordering::Relaxed), 0);
}

#[test]
fn clean_control_server_return_is_a_service_failure() {
    let error = unexpected_control_exit(Ok(())).expect_err("clean return is unexpected");

    assert_eq!(
        error.to_string(),
        "net-gateway control server exited unexpectedly"
    );
}

#[tokio::test]
async fn clean_proxy_return_is_a_service_failure() {
    let task = tokio::spawn(async { Ok(()) });

    let error = unexpected_proxy_exit("TCP", task.await);

    assert_eq!(error.to_string(), "TCP proxy exited unexpectedly");
}

#[tokio::test]
async fn proxy_io_error_is_a_service_failure() {
    let task = tokio::spawn(async {
        Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "listener failed",
        ))
    });

    let error = unexpected_proxy_exit("UDP", task.await);

    assert_eq!(error.to_string(), "UDP proxy failed: listener failed");
}

async fn panic_proxy() -> std::io::Result<()> {
    panic!("proxy panic");
}

#[tokio::test]
async fn proxy_panic_is_a_service_failure() {
    let task = tokio::spawn(panic_proxy());

    let error = unexpected_proxy_exit("TCP", task.await);

    assert!(error.to_string().starts_with("TCP proxy task failed:"));
}
