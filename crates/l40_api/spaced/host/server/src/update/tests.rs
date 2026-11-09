use super::*;
use futures_util::StreamExt;

#[tokio::test]
async fn only_guest_generation_loss_closes_admission_after_subscription_ends() {
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
        assert_eq!(update.admit().is_err(), end == "guest_disconnect", "{end}");
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
    update.admission.lock().unwrap().configuring = true;
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
