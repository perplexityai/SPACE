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
