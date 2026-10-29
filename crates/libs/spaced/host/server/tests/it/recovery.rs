use std::{path::Path, time::Duration};

use pplx_space_common_core::types::{GuestUserName, ProcessRecoveryToken};
use pplx_space_spaced_common::{proto::host as proto, recovery::RecoveryReason};
use pplx_space_spaced_guest_server::{
    FilesystemExecutionMode, HostConnectorType, SpacedGuestService, SpacedGuestServiceConfig,
};
use pplx_space_spaced_host_client::{SpacedHostClient, SpacedHostClientConfig};
use pplx_space_spaced_host_server::{
    BridgeGuestConnector, SpacedHostPeerAuthorization, SpacedHostService, SpacedHostServiceConfig,
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

async fn host(
    directory: &Path,
    guest_socket: &Path,
    epoch: ProcessRecoveryToken,
    shutdown: CancellationToken,
) -> (SpacedHostClient, AbortOnDropHandle<()>) {
    std::fs::create_dir_all(directory).unwrap();
    let socket = directory.join("host.sock");
    let host = SpacedHostService::new_with_config(
        shutdown,
        SpacedHostServiceConfig {
            process_recovery_activation: Some(epoch),
            socket_path: socket.clone(),
            bridge_host_socket_path: directory.join("bridge.sock"),
            host_control_socket_path: directory.join("control.sock"),
            bridge_guest_connector: BridgeGuestConnector::Uds(guest_socket.into()),
            host_peer_authorization: SpacedHostPeerAuthorization::Disabled,
        },
    );
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        host.run().await.unwrap();
    }));
    let client =
        SpacedHostClient::new_with_config(SpacedHostClientConfig::new().with_socket_path(socket))
            .await
            .unwrap();
    (client, task)
}

async fn activate(client: &SpacedHostClient) -> proto::ProcessExecutionInfo {
    let ready = client.wait_ready(Duration::from_secs(2)).await.unwrap();
    let barrier = ready.process_recovery_barrier_id.unwrap().parse().unwrap();
    client
        .resume_process_recovery(barrier, std::time::Duration::from_secs(20))
        .await
        .unwrap();
    let ready = client.wait_ready(Duration::from_secs(2)).await.unwrap();
    assert!(ready.process_recovery_barrier_id.is_none());
    let info = ready.process_execution.unwrap();
    for capability in [
        "process_recovery_start",
        "process_recovery_handles",
        "process_mutation_receipts",
    ] {
        assert!(info.capabilities.iter().any(|value| value == capability));
    }
    info
}

async fn started(client: &SpacedHostClient, request: proto::StartRequest) -> proto::ProcessEvent {
    let mut stream = client.recovery_start(request).await.unwrap();
    let event = stream.message().await.unwrap().unwrap();
    assert!(matches!(
        event.event,
        Some(proto::process_event::Event::Started(_))
    ));
    event
}

fn handle(event: &proto::ProcessEvent, activation: &str) -> proto::RecoveryHandle {
    let Some(proto::process_event::Event::Started(started)) = &event.event else {
        panic!("started");
    };
    proto::RecoveryHandle {
        contract_version: 1,
        expected_instance_id: started.instance_id.clone().unwrap(),
        expected_activation_id: activation.into(),
        process_id: event.process_id.clone(),
        process_incarnation: started.process_incarnation.clone().unwrap(),
        sequence: None,
    }
}
fn sequenced(handle: &proto::RecoveryHandle, sequence: u64) -> proto::RecoveryHandle {
    proto::RecoveryHandle {
        contract_version: 2,
        sequence: Some(sequence),
        ..handle.clone()
    }
}
async fn output(client: &SpacedHostClient, handle: proto::RecoveryHandle) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = client
            .recovery_connect_process(proto::ConnectProcessRequest {
                recovery: Some(handle),
                output_cursor: Some(0),
                resume_after_output_gap: Some(true),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut bytes = Vec::new();
        let mut exited = false;
        while let Some(event) = stream.message().await.unwrap() {
            match event.event.unwrap() {
                proto::process_event::Event::Stdout(value)
                | proto::process_event::Event::Stderr(value) => bytes.extend(value.data),
                proto::process_event::Event::Exit(_) => exited = true,
                _ => {}
            }
        }
        assert!(exited);
        bytes
    })
    .await
    .expect("process exits within the test deadline")
}

#[tokio::test]
async fn recovery_envelopes_survive_real_host_and_guest_transports() {
    let directory = tempfile::tempdir().unwrap();
    let guest_socket = directory.path().join("guest.sock");
    let shutdown = CancellationToken::new();
    // POSIX account lookup keeps the fixture usable without privileges on both
    // Linux and macOS, without assuming the test user's name or numeric ID.
    let account = std::process::Command::new("id")
        .arg("-un")
        .output()
        .unwrap();
    assert!(account.status.success());
    let user = String::from_utf8(account.stdout).unwrap().trim().to_owned();
    let guest = SpacedGuestService::with_config(
        shutdown.clone(),
        SpacedGuestServiceConfig {
            default_operation_user: GuestUserName::parse(user).unwrap(),
            filesystem_execution: FilesystemExecutionMode::Direct,
            host_connector_type: HostConnectorType::Uds(directory.path().join("first/bridge.sock")),
            local_socket_path: None,
            bridge_socket_path: Some(guest_socket.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let serving = guest.clone();
    let guest_task = AbortOnDropHandle::new(tokio::spawn(async move {
        serving.run().await.unwrap();
    }));
    assert!(guest.wait_until_listening().await);
    let epoch = ProcessRecoveryToken::new();
    let (client, first_host) = host(
        &directory.path().join("first"),
        &guest_socket,
        epoch,
        shutdown.child_token(),
    )
    .await;
    let info = activate(&client).await;
    let request = proto::StartRequest {
        recovery: Some(proto::RecoveryStart {
            contract_version: 1,
            expected_instance_id: info.instance_id,
            expected_activation_id: epoch.to_string(),
            command: vec!["/bin/cat".into()],
        }),
        process_id: Some("recovery-cat".into()),
        idempotency_key: Some("recovery-cat".into()),
        stdin: true,
        background: true,
        ..Default::default()
    };
    let event = started(&client, request.clone()).await;
    assert_eq!(started(&client, request.clone()).await, event);
    let handle = handle(&event, &epoch.to_string());
    let mut conflict = request.clone();
    conflict.recovery.as_mut().unwrap().command = vec!["/bin/false".into()];
    assert_eq!(
        client.recovery_start(conflict).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    for reason in [
        RecoveryReason::EngineRestarted,
        RecoveryReason::ActivationChanged,
        RecoveryReason::ProcessReplaced,
    ] {
        let mut stale = handle.clone();
        let replacement = ProcessRecoveryToken::new().to_string();
        match reason {
            RecoveryReason::EngineRestarted => stale.expected_instance_id = replacement,
            RecoveryReason::ActivationChanged => stale.expected_activation_id = replacement,
            RecoveryReason::ProcessReplaced => stale.process_incarnation = replacement,
            _ => unreachable!(),
        }
        let error = client
            .recovery_send_input(proto::SendInputRequest {
                recovery: Some(sequenced(&stale, 1)),
                data: b"must not write".to_vec(),
                process_id: String::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(RecoveryReason::from_status(&error), Some(reason));
    }
    let input = proto::SendInputRequest {
        recovery: Some(sequenced(&handle, 1)),
        data: b"once\n".to_vec(),
        process_id: String::new(),
    };
    client.recovery_send_input(input.clone()).await.unwrap();
    client.recovery_send_input(input.clone()).await.unwrap();
    let close = proto::CloseStdinRequest {
        recovery: Some(sequenced(&handle, 2)),
        ..Default::default()
    };
    client.recovery_close_stdin(close.clone()).await.unwrap();
    client.recovery_close_stdin(close).await.unwrap();
    assert_eq!(output(&client, handle.clone()).await, b"once\n");

    let mut pty_request = request.clone();
    pty_request.process_id = Some("recovery-pty".into());
    pty_request.idempotency_key = Some("recovery-pty".into());
    pty_request.pty = Some(proto::PtyConfig {
        size: Some(proto::PtySize { rows: 24, cols: 80 }),
    });
    let pty_event = started(&client, pty_request).await;
    let pty = self::handle(&pty_event, &epoch.to_string());
    client
        .recovery_resize_pty(proto::ResizePtyRequest {
            recovery: Some(pty.clone()),
            size: Some(proto::PtySize {
                rows: 30,
                cols: 100,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    let kill = proto::SendSignalRequest {
        recovery: Some(sequenced(&pty, 1)),
        signal: 9,
        ..Default::default()
    };
    client.recovery_send_signal(kill.clone()).await.unwrap();
    client.recovery_send_signal(kill).await.unwrap();
    output(&client, pty).await;

    #[cfg(target_os = "linux")]
    {
        // One byte proves that the write reached the child; the remaining input
        // exceeds the pipe capacity while the child stops reading.
        let mut blocked = request.clone();
        blocked.process_id = Some("blocked-input".into());
        blocked.idempotency_key = Some("blocked-input".into());
        blocked.recovery.as_mut().unwrap().command = vec![
            "/bin/sh".into(),
            "-c".into(),
            "dd bs=1 count=1 of=/dev/null 2>/dev/null; printf blocked; sleep 60".into(),
        ];
        let blocked = self::handle(&started(&client, blocked).await, &epoch.to_string());
        let pending_input = proto::SendInputRequest {
            recovery: Some(sequenced(&blocked, 1)),
            data: vec![b'x'; 2 * 1024 * 1024],
            process_id: String::new(),
        };
        let writer = client.clone();
        let retry = pending_input.clone();
        let writing = AbortOnDropHandle::new(tokio::spawn(async move {
            writer.recovery_send_input(pending_input).await
        }));
        let mut events = client
            .recovery_connect_process(proto::ConnectProcessRequest {
                recovery: Some(blocked.clone()),
                output_cursor: Some(0),
                ..Default::default()
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut bytes = Vec::new();
            while !bytes.ends_with(b"blocked") {
                let event = events.message().await.unwrap().expect("child remains live");
                if let Some(proto::process_event::Event::Stdout(output)) = event.event {
                    bytes.extend(output.data);
                }
            }
        })
        .await
        .expect("child consumed the first byte");
        drop(events);
        assert!(!writing.is_finished());
        client
            .prepare_suspend(Duration::from_secs(5))
            .await
            .unwrap();
        let interrupted = tokio::time::timeout(Duration::from_secs(5), writing)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(
            RecoveryReason::from_status(&interrupted),
            Some(RecoveryReason::InputOutcomeUncertain)
        );
        activate(&client).await;
        let replay = client.recovery_send_input(retry).await.unwrap_err();
        assert_eq!(replay.code(), interrupted.code());
        assert_eq!(
            RecoveryReason::from_status(&replay),
            RecoveryReason::from_status(&interrupted)
        );
        assert!(
            interrupted
                .metadata()
                .get("space-process-input-written")
                .is_some()
        );
        assert_eq!(
            replay.metadata().get("space-process-input-written"),
            interrupted.metadata().get("space-process-input-written")
        );
        // A version-one fenced kill is the escape hatch for an ordered stream
        // whose next sequence cannot pass its pending owner.
        client
            .recovery_send_signal(proto::SendSignalRequest {
                recovery: Some(blocked.clone()),
                signal: 9,
                ..Default::default()
            })
            .await
            .unwrap();
        output(&client, blocked).await;

        // The real preparation RPC quiesces without deleting process receipts.
        client
            .prepare_suspend(Duration::from_secs(5))
            .await
            .unwrap();
        activate(&client).await;
        client.recovery_send_input(input).await.unwrap();
        client
            .prepare_suspend(Duration::from_secs(5))
            .await
            .unwrap();
        // Model the routing side of restore: guest state survives, but its new
        // host is bound to a different externally supplied activation.
        let fresh = ProcessRecoveryToken::new();
        let (replacement, second_host) = host(
            &directory.path().join("second"),
            &guest_socket,
            fresh,
            shutdown.child_token(),
        )
        .await;
        let restored = activate(&replacement).await;
        assert_eq!(restored.instance_id, handle.expected_instance_id);
        let error = replacement.recovery_start(request).await.unwrap_err();
        assert_eq!(
            RecoveryReason::from_status(&error),
            Some(RecoveryReason::ActivationChanged)
        );
        shutdown.cancel();
        second_host.await.unwrap();
    }
    shutdown.cancel();
    first_host.await.unwrap();
    guest_task.await.unwrap();
}
