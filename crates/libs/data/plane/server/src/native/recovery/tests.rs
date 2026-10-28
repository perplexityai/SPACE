use super::*;

fn start() -> RecoveryStart {
    RecoveryStart {
        contract_version: 1,
        expected_instance_id: uuid::Uuid::now_v7().to_string(),
        expected_activation_id: uuid::Uuid::now_v7().to_string(),
        command: vec!["true".into()],
    }
}

#[test]
fn recovery_handle_cannot_escape_its_command_session() {
    let session = DataPlaneSessionId::Process(pplx_space_public_api::ProcessSessionId::new());
    let start = start();
    let handle = Handle {
        instance_id: start.expected_instance_id.clone(),
        activation_id: start.expected_activation_id.clone(),
        process_id: process_id_for_session(session).to_string(),
        process_incarnation: uuid::Uuid::now_v7().to_string(),
    };
    let mut equivalent = handle.clone();
    equivalent.instance_id = equivalent.instance_id.to_uppercase();
    equivalent.activation_id = equivalent.activation_id.to_uppercase();
    handle_for_session(equivalent, &start, session, Some(1)).unwrap();
    let wire = handle_for_session(handle.clone(), &start, session, Some(1)).unwrap();
    assert_eq!(wire.contract_version, 2);
    assert_eq!(wire.sequence, Some(1));
    for field in 0..3 {
        let mut other = handle.clone();
        match field {
            0 => other.instance_id = uuid::Uuid::now_v7().to_string(),
            1 => other.activation_id = uuid::Uuid::now_v7().to_string(),
            _ => other.process_id = "another-process".into(),
        }
        assert_eq!(
            handle_for_session(other, &start, session, Some(1))
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
    assert_eq!(
        handle_for_session(handle, &start, session, Some(0))
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn recovery_relay_requires_ack_and_preserves_counted_gaps() {
    let start = start();
    let mut cursor = 5;
    let event = |event| ProcessEvent {
        process_id: "test".into(),
        pid: Some(42),
        event: Some(event),
    };
    let gap = || {
        event(spaced_process_event::Event::OutputGap(
            ops::ProcessOutputGap {
                skipped_events: 3,
                output_cursor: 8,
                output_bytes_before: 120,
            },
        ))
    };
    assert_eq!(
        event_frame(gap(), &start, &mut cursor, true, true, true)
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    for confirmed in [false, true] {
        let ack = event(spaced_process_event::Event::Connected(
            ops::ProcessConnected {
                output_cursor: Some(5),
                resume_after_output_gap: confirmed,
            },
        ));
        assert_eq!(
            event_frame(ack, &start, &mut cursor, true, true, true).is_ok(),
            confirmed
        );
    }
    assert_eq!(
        event_frame(gap(), &start, &mut cursor, false, true, true).unwrap(),
        Server::RecoveryOutputGap {
            skipped_events: 3,
            output_cursor: 8,
            output_bytes_before: 120,
        }
    );
    let stdout = event(spaced_process_event::Event::Stdout(ops::ProcessOutput {
        data: b"hello".to_vec(),
    }));
    assert_eq!(
        event_frame(stdout, &start, &mut cursor, false, true, true).unwrap(),
        Server::RecoveryStdout {
            output_cursor: 9,
            data_base64: BASE64_STANDARD.encode("hello")
        }
    );
    assert_eq!(
        event_frame(gap(), &start, &mut cursor, false, true, false)
            .unwrap_err()
            .code(),
        tonic::Code::DataLoss
    );
}

#[tokio::test]
async fn connect_eof_without_an_exit_is_not_success() {
    let connected = ProcessEvent {
        process_id: "test".into(),
        pid: Some(42),
        event: Some(spaced_process_event::Event::Connected(
            ops::ProcessConnected {
                output_cursor: Some(0),
                resume_after_output_gap: true,
            },
        )),
    };
    let exit = ProcessEvent {
        process_id: "test".into(),
        pid: Some(42),
        event: Some(spaced_process_event::Event::Exit(ops::ProcessExit {
            outcome: Some(spaced_process_exit::Outcome::Exited(ops::ProcessExited {
                exit_code: 0,
            })),
        })),
    };
    for events in [vec![], vec![connected.clone()], vec![connected, exit]] {
        let complete = events.len() == 2;
        let result = relay(
            &mut futures_util::sink::drain(),
            futures_util::stream::iter(events.into_iter().map(Ok)),
            &start(),
            Some(0),
            true,
        )
        .await;
        if complete {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().code(), tonic::Code::DataLoss);
        }
    }
}

#[test]
fn public_errors_preserve_categories_without_transport_diagnostics() {
    for (status, expected) in [
        (
            Status::not_found("endpoint diagnostic"),
            "process or sandbox is no longer available",
        ),
        (
            Status::unavailable("endpoint diagnostic"),
            "process service is temporarily unavailable",
        ),
        (
            RecoveryReason::TemporarilyUnavailable.into_status(),
            "process recovery is temporarily unavailable",
        ),
    ] {
        let code = status.code() as i32;
        let Server::RecoveryError {
            code: actual,
            message,
            ..
        } = error_frame(status)
        else {
            panic!("error frame")
        };
        assert_eq!(actual, code);
        assert_eq!(message, expected);
        assert!(!message.contains("endpoint diagnostic"));
    }
}

#[tokio::test]
async fn recovery_protocol_is_acknowledged_by_the_serving_data_plane() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return;
    }
    use pplx_space_public_api::recovery::RECOVERY_WEBSOCKET_PROTOCOL;
    let state = crate::tests::test_data_plane_state().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serving = state.clone();
    let server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(
            listener,
            router(serving).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    }));
    let session = DataPlaneSessionId::Process(pplx_space_public_api::ProcessSessionId::new());
    for offered in [
        None,
        Some("unsupported-protocol"),
        Some(RECOVERY_WEBSOCKET_PROTOCOL),
    ] {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
        let protocol = offered
            .map(|value| format!("Sec-WebSocket-Protocol: {value}\r\n"))
            .unwrap_or_default();
        socket.write_all(format!(
            "GET /v1/sessions/{session}/attach HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {}\r\n{protocol}\r\n",
            BASE64_STANDARD.encode(b"recovery-fixture!"),
        ).as_bytes()).await.unwrap();
        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\n") {
            response.push(socket.read_u8().await.unwrap());
            assert!(response.len() < 4096);
        }
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 101"), "{response}");
        let acknowledged = response.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("sec-websocket-protocol")
                .then(|| value.trim())
        });
        assert_eq!(
            acknowledged,
            offered.filter(|value| *value == RECOVERY_WEBSOCKET_PROTOCOL)
        );
    }
    state.drain_started.cancel();
    state.upgrade_tasks.close();
    tokio::time::timeout(Duration::from_secs(2), state.upgrade_tasks.wait())
        .await
        .unwrap();
    drop(server);
}
