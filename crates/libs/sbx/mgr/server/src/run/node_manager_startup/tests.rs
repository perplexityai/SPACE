use pplx_space_node_mgr_client::proto::InspectSandboxResponse;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::*;
use crate::run::test_receiver::{Receiver, Reply, always, dial};

#[tokio::test]
async fn waits_through_a_node_manager_restart_gap() {
    #[derive(Clone, Copy, Debug)]
    enum Gap {
        SocketMissing,
        ConnectionRefused,
    }
    for gap in [Gap::SocketMissing, Gap::ConnectionRefused] {
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("node.sock");
        if matches!(gap, Gap::ConnectionRefused) {
            // A predecessor that exited without unlinking leaves a socket
            // file nothing accepts on.
            drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
        }
        // The successor serves before its store is ready, then succeeds.
        let reply: Reply = Arc::new(|call| match call {
            0 => Err(Status::unavailable("receiver starting")),
            1 => Err(Status::deadline_exceeded("receiver busy")),
            2 => Err(Status::cancelled("receiver shutting down")),
            3 => Err(Status::resource_exhausted("receiver admission busy")),
            _ => Ok(InspectSandboxResponse::default()),
        });
        let mut receiver = None;
        let mut dials = 0;
        let result = wait_for_node_manager(
            StartupPhase::BeforeRecovery,
            &CancellationToken::new(),
            || {
                dials += 1;
                if dials == 2 {
                    if matches!(gap, Gap::ConnectionRefused) {
                        std::fs::remove_file(&socket).unwrap();
                    }
                    receiver = Some(Receiver::bind(&socket, reply.clone()));
                }
                dial(socket.clone())
            },
        )
        .await;
        result.unwrap_or_else(|error| panic!("{gap:?}: {error}"));
        assert_eq!(dials, 6, "{gap:?}");
        assert_eq!(receiver.unwrap().calls(), 5, "{gap:?}");
    }
}

#[tokio::test]
async fn non_transient_failures_end_startup_without_retrying() {
    for code in [
        tonic::Code::FailedPrecondition,
        tonic::Code::InvalidArgument,
        tonic::Code::Unimplemented,
        tonic::Code::PermissionDenied,
    ] {
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("node.sock");
        let receiver = Receiver::bind(
            &socket,
            Arc::new(move |_| Err(Status::new(code, "receiver rejected"))),
        );
        let error = wait_for_node_manager(
            StartupPhase::BeforeRecovery,
            &CancellationToken::new(),
            || dial(socket.clone()),
        )
        .await
        .unwrap_err();
        let NodeManagerStartupError::Status(status) = error else {
            panic!("{code:?}: unexpected {error:?}");
        };
        assert_eq!(status.code(), code);
        assert_eq!(receiver.calls(), 1, "{code:?}");
    }

    let mut dials = 0;
    let error = wait_for_node_manager(
        StartupPhase::BeforeRecovery,
        &CancellationToken::new(),
        || {
            dials += 1;
            async {
                Err(ConnectError::InvalidUri(
                    "not a URI".parse::<http::Uri>().unwrap_err(),
                ))
            }
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(error, NodeManagerStartupError::Status(_)));
    assert_eq!(dials, 1);
}

#[tokio::test(start_paused = true)]
async fn stalled_dial_is_bounded_and_retried_until_shutdown() {
    let shutdown = CancellationToken::new();
    let mut dials = 0;
    let started = tokio::time::Instant::now();
    let result = wait_for_node_manager(StartupPhase::BeforeRecovery, &shutdown, || {
        dials += 1;
        if dials == 2 {
            shutdown.cancel();
        }
        std::future::pending()
    })
    .await;
    assert!(matches!(result, Err(NodeManagerStartupError::Cancelled)));
    assert_eq!(dials, 2);
    assert!(started.elapsed() >= DIAL_TIMEOUT);
}

#[tokio::test]
async fn stalled_inspect_is_bounded_and_cancellable() {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("node.sock");
    let receiver = Receiver::bind_async(&socket, |_| std::future::pending());
    let shutdown = CancellationToken::new();
    let _guard = shutdown.clone().drop_guard();
    let wait = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            wait_for_node_manager(StartupPhase::BeforeRecovery, &shutdown, || {
                dial(socket.clone())
            })
            .await
        }
    });
    receiver.entered.notified().await;
    tokio::time::pause();
    tokio::time::advance(INSPECT_TIMEOUT + Duration::from_secs(1)).await;
    tokio::time::resume();
    tokio::time::timeout(Duration::from_secs(5), receiver.entered.notified())
        .await
        .expect("a stalled read must surrender its attempt and retry");
    shutdown.cancel();
    let result = tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(NodeManagerStartupError::Cancelled)));
}

#[tokio::test]
async fn recovers_after_a_prolonged_dependency_outage() {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("node.sock");
    let ready = Arc::new(AtomicBool::new(false));
    let receiver = Receiver::bind(
        &socket,
        Arc::new({
            let ready = ready.clone();
            move |_| {
                if ready.load(Ordering::SeqCst) {
                    Ok(InspectSandboxResponse::default())
                } else {
                    Err(Status::unavailable("receiver restarting"))
                }
            }
        }),
    );
    let shutdown = CancellationToken::new();
    let _guard = shutdown.clone().drop_guard();
    let wait = tokio::spawn({
        let shutdown = shutdown.clone();
        let socket = socket.clone();
        async move {
            wait_for_node_manager(StartupPhase::BeforeRecovery, &shutdown, || {
                dial(socket.clone())
            })
            .await
        }
    });
    receiver.entered.notified().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(60)).await;
    ready.store(true, Ordering::SeqCst);
    // Resume real I/O after advancing the outage; socket readiness must not
    // compete with paused-clock auto-advancement of per-attempt deadlines.
    tokio::time::resume();
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(receiver.calls() >= 2);
}

#[tokio::test]
async fn shutdown_ends_the_wait() {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("node.sock");
    let receiver = Receiver::bind(
        &socket,
        always(|| Status::unavailable("receiver restarting")),
    );
    let shutdown = CancellationToken::new();
    let wait = tokio::spawn({
        let shutdown = shutdown.clone();
        let socket = socket.clone();
        async move {
            wait_for_node_manager(StartupPhase::BeforeRecovery, &shutdown, || {
                dial(socket.clone())
            })
            .await
        }
    });
    receiver.entered.notified().await;
    shutdown.cancel();
    let result = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("shutdown must end the wait promptly")
        .unwrap();
    assert!(matches!(result, Err(NodeManagerStartupError::Cancelled)));
}
