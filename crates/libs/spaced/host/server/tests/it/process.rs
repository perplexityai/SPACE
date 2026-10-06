use crate::fixture::HostFixture;
use crate::mock_bridge_guest::{
    MockBridgeGuest, start_mock_bridge_guest, start_mock_bridge_guest_with_service,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn interactive_effects_are_single_attempt_after_lost_reply() {
    for resize in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("guest.sock");
        let accepted = Arc::new(AtomicUsize::new(0));
        let bridge = start_mock_bridge_guest_with_service(
            socket.clone(),
            MockBridgeGuest {
                interactive_process_control_calls: Some(accepted.clone()),
                input_reply_error: Some(tonic::Code::Unavailable),
                resize_fail_once: resize,
                ..Default::default()
            },
        )
        .await;
        let fixture = HostFixture::start(Some(socket)).await;
        let client = fixture.client().await;
        client
            .wait_ready(std::time::Duration::from_secs(1))
            .await
            .expect("ready");
        let result = if resize {
            client
                .resize_pty(
                    "proc-1".into(),
                    pplx_space_spaced_host_client::proto::PtySize { rows: 24, cols: 80 },
                )
                .await
                .map(|_| ())
        } else {
            client
                .send_input("proc-1".into(), b"once".to_vec())
                .await
                .map(|_| ())
        };
        assert_eq!(
            result.expect_err("ambiguous outcome").code(),
            tonic::Code::Unavailable
        );
        assert_eq!(accepted.load(Ordering::SeqCst), 1);
        fixture.stop().await;
        bridge.abort();
    }
}
#[tokio::test]
async fn process_validation_is_terminal_before_guest_connection() {
    use pplx_space_spaced_host_client::proto::{
        ConnectProcessRequest, StartRequest, process_client::ProcessClient,
    };
    let fixture = HostFixture::start(None).await;
    fixture.client().await;
    let channel = pplx_space_common_grpc::connect_uds_channel("test host", &fixture.socket_path)
        .await
        .unwrap();
    let mut client = ProcessClient::new(channel);
    let invalid_start = client.start(StartRequest::default()).await.unwrap_err();
    let invalid_connect = client
        .connect_process(ConnectProcessRequest::default())
        .await
        .unwrap_err();
    for error in [invalid_start, invalid_connect] {
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(pplx_space_common_grpc::is_terminal_guest_process_error(
            &error
        ));
    }
    fixture.stop().await;
}

#[tokio::test]
async fn process_start_relays_to_guest_bridge() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket = temp_dir.path().join("spaced-bridge-guest.sock");
    let bridge_task = start_mock_bridge_guest(bridge_guest_socket.clone()).await;
    let fixture = HostFixture::start(Some(bridge_guest_socket)).await;
    let client = fixture.client().await;
    client
        .wait_ready(std::time::Duration::from_secs(1))
        .await
        .expect("wait ready response");

    let mut stream = client
        .start_process_stream_with_options(pplx_space_spaced_host_client::StartProcessOptions {
            idempotency_key: Some(
                pplx_space_common_core::types::IdempotencyKey::parse("start-key").unwrap(),
            ),
            ..pplx_space_spaced_host_client::StartProcessOptions::new(
                pplx_space_common_core::types::ProcessCommand::new(vec![
                    "echo".into(),
                    "hello".into(),
                ])
                .unwrap(),
            )
        })
        .await
        .expect("start response");
    let mut events = Vec::new();
    while let Some(event) = stream.message().await.expect("event") {
        events.push(event);
    }
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].process_id, "proc-1");

    fixture.stop().await;
    bridge_task.abort();
}

#[tokio::test]
async fn process_start_forwards_assigned_process_id_to_guest_bridge() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket = temp_dir.path().join("spaced-bridge-guest.sock");
    let bridge_task = start_mock_bridge_guest(bridge_guest_socket.clone()).await;
    let fixture = HostFixture::start(Some(bridge_guest_socket)).await;
    let client = fixture.client().await;
    client
        .wait_ready(std::time::Duration::from_secs(1))
        .await
        .expect("wait ready response");

    let assigned = pplx_space_common_core::types::ProcessId::parse("assigned-proc").unwrap();
    let mut stream = client
        .start_process_stream_with_options(pplx_space_spaced_host_client::StartProcessOptions {
            idempotency_key: Some(
                pplx_space_common_core::types::IdempotencyKey::parse("start-key").unwrap(),
            ),
            process_id: Some(assigned.clone()),
            ..pplx_space_spaced_host_client::StartProcessOptions::new(
                pplx_space_common_core::types::ProcessCommand::new(vec![
                    "echo".into(),
                    "hello".into(),
                ])
                .unwrap(),
            )
        })
        .await
        .expect("start response");
    let mut events = Vec::new();
    while let Some(event) = stream.message().await.expect("event") {
        events.push(event);
    }
    // The mock echoes what it received, so this fails if the assigned id is
    // dropped anywhere in the host client's request build.
    assert!(!events.is_empty());
    for event in &events {
        assert_eq!(event.process_id, assigned.as_str());
    }

    fixture.stop().await;
    bridge_task.abort();
}

#[tokio::test]
// SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_1_2: retire with the host capability gate.
async fn process_interactive_control_rejects_legacy_guest_version_without_forwarding() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket = temp_dir.path().join("spaced-bridge-guest.sock");
    let interactive_calls = Arc::new(AtomicUsize::new(0));
    let bridge_task = start_mock_bridge_guest_with_service(
        bridge_guest_socket.clone(),
        MockBridgeGuest {
            version: "0.1.1",
            interactive_process_control_calls: Some(interactive_calls.clone()),
            ..MockBridgeGuest::default()
        },
    )
    .await;
    let fixture = HostFixture::start(Some(bridge_guest_socket)).await;
    let client = fixture.client().await;
    client
        .wait_ready(std::time::Duration::from_secs(1))
        .await
        .expect("wait ready response");

    for (operation, error) in [
        (
            "SendInput",
            client
                .send_input("proc-1".into(), b"hello stdin\n".to_vec())
                .await
                .expect_err("legacy guest must reject send input"),
        ),
        (
            "CloseStdin",
            client
                .close_stdin("proc-1".into())
                .await
                .expect_err("legacy guest must reject close stdin"),
        ),
        (
            "ResizePty",
            client
                .resize_pty(
                    "proc-1".into(),
                    pplx_space_spaced_host_client::proto::PtySize { rows: 24, cols: 80 },
                )
                .await
                .expect_err("legacy guest must reject resize pty"),
        ),
    ] {
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains(operation));
        assert!(error.message().contains("0.1.1"));
        assert!(error.message().contains("0.1.2 or newer"));
        assert_eq!(
            pplx_space_common_grpc::GuestCapabilityUnsupported::from_status(&error)
                .map(|unsupported| unsupported.capability),
            Some(pplx_space_common_grpc::GuestCapability::InteractiveProcessControl)
        );
    }

    assert_eq!(interactive_calls.load(Ordering::SeqCst), 0);

    fixture.stop().await;
    bridge_task.abort();
}

#[tokio::test]
async fn process_connect_and_kill_relay_to_guest_bridge() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket = temp_dir.path().join("spaced-bridge-guest.sock");
    let bridge_task = start_mock_bridge_guest(bridge_guest_socket.clone()).await;
    let fixture = HostFixture::start(Some(bridge_guest_socket)).await;
    let client = fixture.client().await;

    let mut events = client
        .connect_process_stream_with_cursor("proc-1".into(), None, None)
        .await
        .expect("connect response");
    assert_eq!(
        events.message().await.unwrap().unwrap().process_id,
        "proc-1"
    );
    assert!(events.message().await.unwrap().is_none());

    client
        .send_input("proc-1".into(), b"hello stdin\n".to_vec())
        .await
        .expect("send input response");
    client
        .close_stdin("proc-1".into())
        .await
        .expect("close stdin response");
    client
        .kill_process("proc-1".into())
        .await
        .expect("kill response");

    fixture.stop().await;
    bridge_task.abort();
}
