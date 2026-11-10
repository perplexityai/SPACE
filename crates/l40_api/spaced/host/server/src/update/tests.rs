use super::*;
use futures_util::StreamExt;
use uuid::Uuid;

#[tokio::test]
async fn transport_disconnect_does_not_claim_a_guest_restart() {
    for end in [
        "manager_disconnect",
        "manager_backpressure",
        "guest_disconnect",
    ] {
        let update = HostUpdate::new();
        update.admission.lock().unwrap().incarnation = Some("configured".into());
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let event = wire::Status {
            incarnation: "configured".into(),
            ..Default::default()
        };
        if end == "manager_disconnect" {
            receiver.close();
        } else if end == "manager_backpressure" {
            sender.try_send(Ok(event.clone())).unwrap();
        }
        let stream = tokio_stream::iter([Ok(event)]).chain(tokio_stream::pending());
        let shutdown = CancellationToken::new();
        if end == "guest_disconnect" {
            update
                .forward_observation(tokio_stream::empty(), sender, &shutdown)
                .await;
        } else if end == "manager_backpressure" {
            let forwarding = update.forward_observation(stream, sender, &shutdown);
            tokio::pin!(forwarding);
            assert!(
                futures_util::poll!(&mut forwarding).is_pending(),
                "slow readers must not disconnect observation"
            );
            receiver.recv().await.unwrap().unwrap();
            assert!(futures_util::poll!(&mut forwarding).is_pending());
            receiver.recv().await.unwrap().unwrap();
            drop(receiver);
            forwarding.await;
        } else {
            update.forward_observation(stream, sender, &shutdown).await;
        }
        update.admit().expect(end);
    }
}

#[tokio::test(start_paused = true)]
async fn unsuccessful_capture_barrier_restores_admission() {
    for cancel in [false, true] {
        let update = HostUpdate::new();
        let _subscriber = update.subscriber.acquire().await.unwrap();
        if cancel {
            let pending = update.pause_observation();
            tokio::pin!(pending);
            tokio::select! {
                biased;
                _ = &mut pending => panic!("subscriber has not quiesced"),
                _ = tokio::task::yield_now() => {},
            }
        } else {
            assert_eq!(
                update.pause_observation().await.err().unwrap().code(),
                tonic::Code::DeadlineExceeded
            );
        }
        assert!(
            !update.admission.lock().unwrap().paused,
            "an abandoned capture must resume metadata observation"
        );
        update
            .admit()
            .expect("an abandoned capture must not strand customer admission");
    }
}

#[tokio::test]
async fn capture_barrier_quiesces_metadata_without_stranding_customer_dispatch() {
    let update = Arc::new(HostUpdate::new());
    let permit = update.subscriber.clone().acquire_owned().await.unwrap();
    let child = update.clone();
    let observing = tokio::spawn(async move {
        let _permit = permit;
        child.changed.notified().await;
        assert!(child.admission.lock().unwrap().paused);
    });
    tokio::time::timeout(Duration::from_secs(1), update.pause_observation())
        .await
        .unwrap()
        .unwrap()
        .keep_paused();
    observing.await.unwrap();
    // No NotifyResume follows when VM pause fails after PrepareSuspend. This
    // must also work for legacy/pool guests without a configuration observer.
    update
        .admit()
        .expect("metadata pause must not own customer admission");
    assert!(!update.admission.lock().unwrap().customer_admission_held());
    update.admission.lock().unwrap().configuring = true;
    assert!(update.admission.lock().unwrap().customer_admission_held());
    assert!(
        update.admit().is_err(),
        "real maintenance holds remain closed"
    );
    update.admission.lock().unwrap().configuring = false;
    update.resume_observation();
    update.admit().unwrap();
}

#[tokio::test]
async fn paused_subscription_does_not_connect_to_the_guest() {
    let root = tempfile::tempdir().unwrap();
    let service = SpacedHostService::new_with_config(
        CancellationToken::new(),
        SpacedHostServiceConfig {
            socket_path: root.path().join("host.sock"),
            bridge_host_socket_path: root.path().join("bridge.sock"),
            bridge_guest_connector: BridgeGuestConnector::Uds(
                root.path().join("missing-guest.sock"),
            ),
            host_control_socket_path: root.path().join("control.sock"),
            ..Default::default()
        },
    );
    service
        .inner
        .update
        .pause_observation()
        .await
        .unwrap()
        .keep_paused();
    let result = GuestUpdate::subscribe(&service, Request::new(wire::StatusRequest {})).await;
    let Err(error) = result else {
        panic!("paused metadata connection admitted");
    };
    assert_eq!(
        error.message(),
        "metadata observation is paused for lifecycle capture"
    );
}

#[tokio::test]
async fn maintenance_ownership_and_active_dispatch_survive_guest_disconnect() {
    let root = tempfile::tempdir().unwrap();
    let service = SpacedHostService::new_with_config(
        CancellationToken::new(),
        SpacedHostServiceConfig {
            socket_path: root.path().join("host.sock"),
            bridge_host_socket_path: root.path().join("bridge.sock"),
            bridge_guest_connector: BridgeGuestConnector::Uds(root.path().join("missing.sock")),
            host_control_socket_path: root.path().join("control.sock"),
            ..Default::default()
        },
    );
    let update = &service.inner.update;
    let execution = Uuid::now_v7();
    let operation = Uuid::now_v7();
    let dispatch = update.admit().unwrap();
    update.admission.lock().unwrap().owner = Some(Owner {
        operation: execution,
        execution,
    });
    let reserved = wire::Attempt {
        operation_id: execution.to_string(),
        execution_id: execution.to_string(),
        ..Default::default()
    };
    assert_eq!(
        service.update_owner(Some(&reserved)).unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    let restart = GuestUpdate::restart(
        &service,
        Request::new(wire::RestartRequest {
            operation_id: execution.to_string(),
            execution_id: execution.to_string(),
            force: true,
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(restart.unwrap_err().code(), tonic::Code::InvalidArgument);
    // Maintenance can take over ordinary reconfiguration of this execution.
    // A failed guest probe must retain the accepted hold for reconciliation.
    let result = GuestUpdate::hold(
        &service,
        Request::new(wire::HoldRequest {
            operation_id: operation.to_string(),
            execution_id: execution.to_string(),
        }),
    )
    .await;
    let Err(_) = result else {
        panic!("disconnected guest returned status");
    };
    update
        .require_owner(&Owner {
            operation,
            execution,
        })
        .unwrap();
    let result = GuestUpdate::hold(
        &service,
        Request::new(wire::HoldRequest {
            operation_id: Uuid::now_v7().to_string(),
            execution_id: execution.to_string(),
        }),
    )
    .await;
    let Err(error) = result else {
        panic!("another operation stole admission");
    };
    assert_eq!(error.message(), "another operation owns admission");
    let result = GuestUpdate::restart(
        &service,
        Request::new(wire::RestartRequest {
            operation_id: operation.to_string(),
            execution_id: execution.to_string(),
            ..Default::default()
        }),
    )
    .await;
    let Err(error) = result else {
        panic!("active dispatch admitted normal restart");
    };
    assert_eq!(error.message(), "host dispatches remain in flight");
    drop(dispatch);
    assert_eq!(update.admission.lock().unwrap().active, 0);
}

#[test]
fn host_adopts_surviving_forwarding_without_resurrecting_closed_ports() {
    let mut ports = Forwarding::default();
    ports.remember(9000, true);
    ports.remember(8080, false);
    assert_eq!(ports.observe(&[8080, 8081]), vec![8081, 9000]);
    // A subsequent guest crash reports no ports; replay host intent.
    assert_eq!(ports.observe(&[]), vec![8081, 9000]);
    ports.remember(9000, false);
    assert_eq!(ports.observe(&[8080, 8081, 9000]), vec![8081]);
}

#[tokio::test]
async fn only_explicit_upstream_eof_finishes_the_guest_upload() {
    for clean_eof in [false, true] {
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let message = wire::StageRequest {
            part: Some(wire::stage_request::Part::Chunk(wire::Chunk {
                binary: wire::Binary::Service as i32,
                offset: 0,
                data: b"payload".to_vec(),
            })),
        };
        sender
            .send(StageForward::Message(message.clone()))
            .await
            .unwrap();
        if clean_eof {
            sender.send(StageForward::Finish).await.unwrap();
        }
        drop(sender);
        let stream = stage_forward_stream(receiver);
        tokio::pin!(stream);
        assert_eq!(stream.next().await, Some(message));
        if !clean_eof {
            assert_eq!(
                stream.next().await,
                Some(wire::StageRequest { part: None }),
                "cancel/error/timeout must poison an otherwise complete upload"
            );
        }
        assert!(stream.next().await.is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn capture_interrupts_pending_subscription_setup() {
    let update = Arc::new(HostUpdate::new());
    let permit = update.subscriber.clone().acquire_owned().await.unwrap();
    let setup_owner = update.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let setup = tokio::spawn(async move {
        let _permit = permit;
        setup_owner
            .until_observation_pauses(
                async {
                    entered.send(()).unwrap();
                    std::future::pending::<()>().await
                },
                &CancellationToken::new(),
            )
            .await
    });
    started.await.unwrap();
    let pause = update
        .pause_observation()
        .await
        .expect("capture must cancel setup before its quiescence timeout");
    assert_eq!(
        setup.await.unwrap().unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    drop(pause);
    assert!(!update.admission.lock().unwrap().paused);
}

#[tokio::test]
async fn stage_relay_preserves_early_guest_rejection_and_upstream_errors() {
    for chunk in [false, true] {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        drop(receiver);
        let input = tokio_stream::iter(chunk.then_some(Ok(wire::StageRequest { part: None })));
        let result = tokio::try_join!(
            biased;
            forward_stage(input, sender),
            async { Err::<(), _>(Status::resource_exhausted("guest disk full")) },
        );
        assert_eq!(result.unwrap_err().message(), "guest disk full");
    }
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let input = tokio_stream::iter([Err(Status::cancelled("upstream aborted"))]);
    assert_eq!(
        forward_stage(input, sender).await.unwrap_err().message(),
        "upstream aborted"
    );
    let stream = stage_forward_stream(receiver);
    tokio::pin!(stream);
    assert_eq!(stream.next().await, Some(wire::StageRequest { part: None }));
}

#[tokio::test]
async fn observation_adopts_first_generation_and_gates_only_a_replacement() {
    for (previous, observed, held) in [
        (None, "first", false),
        (Some("first"), "first", false),
        (Some("first"), "replacement", true),
    ] {
        let update = HostUpdate::new();
        update.admission.lock().unwrap().incarnation = previous.map(str::to_owned);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let shutdown = CancellationToken::new();
        let stream = tokio_stream::iter([Ok(wire::Status {
            incarnation: observed.into(),
            ..Default::default()
        })])
        .chain(tokio_stream::pending());
        let work = update.forward_observation(stream, sender, &shutdown);
        tokio::pin!(work);
        assert!(futures_util::poll!(&mut work).is_pending());
        assert_eq!(
            receiver
                .recv()
                .await
                .unwrap()
                .unwrap()
                .customer_admission_held,
            held
        );
        assert_eq!(update.admit().is_err(), held);
        shutdown.cancel();
        work.await;
    }
}

#[test]
fn capture_status_does_not_latch_a_configuration_hold() {
    for observe in [false, true] {
        let mut state = Admission {
            incarnation: Some("old".into()),
            active: 1,
            ..Default::default()
        };
        let mut status = wire::Status {
            incarnation: "new".into(),
            ..Default::default()
        };
        state.annotate_status(&mut status, observe);
        assert_eq!(status.customer_admission_held, observe);
        assert_eq!(state.configuring, observe);
        assert_eq!(
            status.defer_reasons,
            vec![wire::DeferReason::Operations as i32]
        );
    }
}

#[tokio::test(start_paused = true)]
async fn update_wait_observes_release_deadline_shutdown_and_capture_without_dispatch() {
    enum End {
        Release,
        CallerDeadline,
        HoldDeadline,
        Shutdown,
        Capture,
        Replaced,
    }
    for end in [
        End::Release,
        End::CallerDeadline,
        End::HoldDeadline,
        End::Shutdown,
        End::Capture,
        End::Replaced,
    ] {
        let update = HostUpdate::new();
        let owner = Owner {
            operation: Uuid::now_v7(),
            execution: Uuid::now_v7(),
        };
        update
            .admission
            .lock()
            .unwrap()
            .hold(owner.clone())
            .unwrap();
        let shutdown = CancellationToken::new();
        let caller_deadline =
            matches!(end, End::CallerDeadline).then(|| Deadline::now() + Duration::from_secs(1));
        let waiting = update.admit_when_ready(caller_deadline, &shutdown);
        tokio::pin!(waiting);
        assert!(futures_util::poll!(&mut waiting).is_pending());
        assert_eq!(update.admission.lock().unwrap().active, 0);
        match end {
            End::Release => update.admission.lock().unwrap().owner = None,
            End::CallerDeadline => tokio::time::advance(Duration::from_secs(1)).await,
            End::HoldDeadline => {
                tokio::time::advance(ADMISSION_WAIT_BUDGET - Duration::from_secs(1)).await;
                update.admission.lock().unwrap().hold(owner).unwrap();
                tokio::time::advance(Duration::from_secs(1)).await;
            }
            End::Shutdown => shutdown.cancel(),
            End::Capture => update.pause_observation().await.unwrap().keep_paused(),
            End::Replaced => {
                update.admission.lock().unwrap().owner = Some(Owner {
                    operation: Uuid::now_v7(),
                    execution: Uuid::now_v7(),
                })
            }
        }
        update.admission_changed.send_replace(());
        let result = waiting.await;
        match end {
            End::Release => {
                let guard = result.expect("settled generation admits");
                assert_eq!(update.admission.lock().unwrap().active, 1);
                drop(guard);
            }
            End::CallerDeadline => {
                assert_eq!(result.err().unwrap().code(), tonic::Code::DeadlineExceeded)
            }
            End::Shutdown => assert_eq!(result.err().unwrap().code(), tonic::Code::Cancelled),
            End::HoldDeadline | End::Capture | End::Replaced => {
                assert_eq!(result.err().unwrap().code(), tonic::Code::Unavailable)
            }
        }
        assert_eq!(update.admission.lock().unwrap().active, 0);
    }
}

#[tokio::test(start_paused = true)]
async fn mounted_wait_is_bounded_by_existing_limits_and_preserves_control_capacity() {
    use crate::admission::HostAdmissionLayer;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tonic::codegen::http;
    use tower::{Layer, Service};

    let directory = tempfile::tempdir().unwrap();
    let host = SpacedHostService::new_with_config(
        CancellationToken::new(),
        SpacedHostServiceConfig {
            socket_path: directory.path().join("host.sock"),
            bridge_host_socket_path: directory.path().join("bridge.sock"),
            bridge_guest_connector: BridgeGuestConnector::Uds(directory.path().join("guest.sock")),
            host_control_socket_path: directory.path().join("control.sock"),
            ..Default::default()
        },
    );
    host.inner
        .update
        .admission
        .lock()
        .unwrap()
        .hold(Owner {
            operation: Uuid::now_v7(),
            execution: Uuid::now_v7(),
        })
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let handler = tower::service_fn(move |_: http::Request<()>| {
        observed.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok::<_, std::convert::Infallible>(http::Response::new(
            tonic::body::Body::empty(),
        )))
    });
    let mut service = crate::bridge::host_request_limiter()
        .layer(HostAdmissionLayer(host.clone()).layer(handler));
    let request = |method: &'static str| {
        let mut request = http::Request::builder().uri(method).body(()).unwrap();
        request
            .extensions_mut()
            .insert(pplx_space_uds_peer_auth::PeerIdentity::new(
                1000,
                1000,
                Some(42),
            ));
        request
    };
    let mut waiting = Vec::new();
    // Fill the existing per-peer networking/control-request bucket. The
    // maintenance RPC must still be callable to release these waiting requests.
    for _ in 0..128 {
        let mut future = service.call(request(
            "/space.spaced.host.v1.Networking/OpenLoopbackBridge",
        ));
        assert!(futures_util::poll!(&mut future).is_pending());
        waiting.push(future);
    }
    let error = service
        .call(request(
            "/space.spaced.host.v1.Networking/OpenLoopbackBridge",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(
        error.downcast::<Status>().unwrap().code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(
        service
            .call(request("/space.spaced.host.v1.GuestUpdate/Release"))
            .await
            .unwrap(),
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // Cancelling one request frees limiter capacity without executing it later.
    drop(waiting.pop());
    let mut replacement = service.call(request(
        "/space.spaced.host.v1.Networking/OpenLoopbackBridge",
    ));
    assert!(futures_util::poll!(&mut replacement).is_pending());
    host.inner.update.admission.lock().unwrap().owner = None;
    host.inner.update.admission_changed.send_replace(());
    drop(replacement.await.unwrap());
    for future in waiting {
        drop(future.await.unwrap());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 129);
    assert_eq!(host.inner.update.admission.lock().unwrap().active, 0);
}

#[tokio::test(start_paused = true)]
async fn maintenance_wait_keeps_the_caller_deadline_across_customer_routes() {
    use crate::admission::HostAdmissionLayer;
    use tonic::codegen::http;
    use tower::{Layer, Service};
    for route in [
        "Process/Start",
        "Filesystem/Write",
        "Networking/OpenLoopbackBridge",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let host = SpacedHostService::new_with_config(
            CancellationToken::new(),
            SpacedHostServiceConfig {
                socket_path: directory.path().join("host.sock"),
                bridge_host_socket_path: directory.path().join("bridge.sock"),
                bridge_guest_connector: BridgeGuestConnector::Uds(
                    directory.path().join("guest.sock"),
                ),
                host_control_socket_path: directory.path().join("control.sock"),
                ..Default::default()
            },
        );
        host.inner
            .update
            .admission
            .lock()
            .unwrap()
            .hold(Owner {
                operation: Uuid::now_v7(),
                execution: Uuid::now_v7(),
            })
            .unwrap();
        let (sender, mut observed) = tokio::sync::mpsc::unbounded_channel();
        let handler = tower::service_fn(move |request: http::Request<()>| {
            sender
                .send(request.headers()["grpc-timeout"].clone())
                .unwrap();
            std::future::ready(Ok::<_, std::convert::Infallible>(http::Response::new(
                tonic::body::Body::empty(),
            )))
        });
        let mut service = HostAdmissionLayer(host.clone()).layer(handler);
        let request = || {
            http::Request::builder()
                .uri(format!("/space.spaced.host.v1.{route}"))
                .header("grpc-timeout", "3S")
                .body(())
                .unwrap()
        };
        let mut cancelled = service.call(request());
        assert!(futures_util::poll!(&mut cancelled).is_pending());
        drop(cancelled);
        let mut pending = service.call(request());
        assert!(futures_util::poll!(&mut pending).is_pending());
        observed.try_recv().unwrap_err();
        tokio::time::advance(Duration::from_secs(1)).await;
        host.inner.update.admission.lock().unwrap().owner = None;
        host.inner.update.admission_changed.send_replace(());
        drop(pending.await.unwrap());
        let mut headers = http::HeaderMap::new();
        headers.insert("grpc-timeout", observed.try_recv().unwrap());
        let remaining = pplx_space_common_grpc::GrpcRetryContext::none()
            .with_grpc_timeout_metadata(&tonic::metadata::MetadataMap::from_headers(headers))
            .remaining()
            .unwrap();
        assert_eq!(remaining, Duration::from_secs(2));
        assert!(
            observed.try_recv().is_err(),
            "cancelled request must never dispatch"
        );
    }
}
