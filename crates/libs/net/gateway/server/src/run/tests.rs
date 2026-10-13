use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_stream::StreamExt;

#[test]
fn network_policy_loader_accepts_v1_and_fails_closed_without_it() {
    let v1 = NetworkPolicyV1::deny_all();
    let v1_proto = NetworkPolicyProto::from(&v1);

    assert_eq!(
        network_policy_from_host_control_proto(Some(&v1_proto)).unwrap(),
        v1.clone(),
        "the V1 host-control field hydrates the gateway"
    );

    let _error = network_policy_from_host_control_proto(None)
        .expect_err("a response without a V1 policy must fail closed");
}

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

/// sandbox-manager registers a gateway only after its launch returns, so the
/// first pulls are refused. Within the registration window that refusal is
/// pending, not a failure: no failure event, retries that back off rather than
/// storm host-control on one reused connection, and a pull that completes as
/// soon as the owner's first control call, which follows registration, arrives.
/// A gateway that is never registered fails once, within bounded attempts.
#[tokio::test(start_paused = true)]
async fn initial_hydration_treats_refusal_before_registration_as_pending() {
    use pplx_space_observability::test_capture::{CapturedEventsLayer, field_value};
    use tracing_subscriber::layer::SubscriberExt as _;

    let sandbox_id = pplx_space_common_core::types::SandboxId::new().to_string();
    for registered_after in [Some(Duration::from_millis(300)), None] {
        let layer = CapturedEventsLayer::new();
        let events = layer.events();
        let _subscriber =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        let launched_at = tokio::time::Instant::now();
        let (owner_contact, owner_contacted) = tokio::sync::watch::channel(false);
        if let Some(after) = registered_after {
            tokio::spawn(async move {
                tokio::time::sleep(after).await;
                owner_contact.send_replace(true);
            });
        }
        let connects = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let connect = {
            let connects = Arc::clone(&connects);
            async move {
                connects.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };
        let pull = {
            let attempts = Arc::clone(&attempts);
            let sandbox_id = sandbox_id.clone();
            move |()| {
                let elapsed = launched_at.elapsed();
                attempts.lock().unwrap().push(elapsed);
                let registered = registered_after.is_some_and(|after| elapsed >= after);
                let sandbox_id = sandbox_id.clone();
                async move {
                    if registered {
                        Ok(InitialNetworkState {
                            policy: NetworkPolicyV1::allow_all(),
                            network_credentials: Default::default(),
                            policy_ref: String::new(),
                            network_tx_bytes: 0,
                            network_rx_bytes: 0,
                        })
                    } else {
                        Err(HydrateNetworkStateError::GetNetworkPolicy {
                            sandbox_id,
                            source: tonic::Status::permission_denied("unknown caller"),
                        })
                    }
                }
            }
        };
        // Every wait its full bound, so the attempt count is exact.
        let wait = RegistrationWait {
            launched_at,
            owner_contacted,
            jitter: |bound| bound,
        };

        let result = hydrate_network_state(&sandbox_id, wait, connect, pull).await;
        let finished = launched_at.elapsed();
        let attempts = attempts.lock().unwrap().clone();
        let failed = events
            .events()
            .iter()
            .filter(|event| {
                field_value(event, "event_type") == Some("network_policy_enforcement_failed")
            })
            .count();
        assert_eq!(
            attempts.first(),
            Some(&Duration::ZERO),
            "the first pull is immediate"
        );
        assert_eq!(
            connects.load(Ordering::SeqCst),
            1,
            "one connection for every pull"
        );
        match registered_after {
            Some(after) => {
                assert!(result.is_ok());
                assert_eq!(failed, 0, "a refusal before registration is not a failure");
                assert_eq!(finished, after, "the owner's call ends the wait at once");
                // Waits of 10, 20, 40, 80 and 160 ms: pulls at 0, 10, 30, 70
                // and 150 ms, then the owner's call ends the next wait.
                assert_eq!(attempts.len(), 6, "{attempts:?}");
            }
            None => {
                assert!(result.is_err());
                assert_eq!(failed, 1);
                assert!(attempts.len() <= usize::try_from(MAX_PENDING_ATTEMPTS).unwrap());
                assert!(finished <= REGISTRATION_WINDOW + PENDING_RETRY_CAP);
                // Past the doubling, retries are a full cap apart, not a storm.
                assert!(
                    attempts
                        .windows(2)
                        .skip(6)
                        .all(|pair| pair[1] - pair[0] == PENDING_RETRY_CAP),
                    "{attempts:?}"
                );
            }
        }
    }
}
