use std::convert::Infallible;

use futures_util::stream;
use pplx_space_sbx_mgr_client::proto::{
    SpacedProcessConnected, SpacedProcessEvent, SpacedProcessExit, SpacedProcessOutput,
    SpacedProcessSignaled, SpacedProcessStarted,
};
use serde::Deserialize;
use serde_json::Value;

use super::*;

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct TestMessage {
    value: String,
}

fn connect_value(bytes: Bytes) -> Value {
    decode_connect_json_bytes::<Value>(&bytes).expect("connect JSON value")
}

#[test]
fn process_events_encode_connect_wire_shape() {
    let started = process_event_to_connect_message(
        SpacedProcessEvent {
            process_id: "proc-1".to_string(),
            pid: Some(7),
            event: Some(spaced_process_event::Event::Started(SpacedProcessStarted {
                start_time: None,
            })),
        },
        false,
        EcwProcessStdoutStream::Stdout,
    )
    .expect("start message");
    assert_eq!(
        connect_value(started),
        serde_json::json!({ "event": { "start": { "pid": 7 } } })
    );

    let connected = process_event_to_connect_message(
        SpacedProcessEvent {
            process_id: "proc-1".to_string(),
            pid: None,
            event: Some(spaced_process_event::Event::Connected(
                SpacedProcessConnected {
                    output_cursor: None,
                },
            )),
        },
        true,
        EcwProcessStdoutStream::Stdout,
    )
    .expect("connected-as-start message");
    assert_eq!(
        connect_value(connected),
        serde_json::json!({ "event": { "start": { "pid": null } } })
    );

    let stdout = process_event_to_connect_message(
        SpacedProcessEvent {
            process_id: "proc-1".to_string(),
            pid: Some(7),
            event: Some(spaced_process_event::Event::Stdout(SpacedProcessOutput {
                data: b"hello".to_vec(),
            })),
        },
        false,
        EcwProcessStdoutStream::Stdout,
    )
    .expect("stdout message");
    assert_eq!(
        connect_value(stdout),
        serde_json::json!({ "event": { "data": { "stdout": "aGVsbG8=" } } })
    );

    let signaled = process_event_to_connect_message(
        SpacedProcessEvent {
            process_id: "proc-1".to_string(),
            pid: Some(7),
            event: Some(spaced_process_event::Event::Exit(SpacedProcessExit {
                outcome: Some(spaced_process_exit::Outcome::Signaled(
                    SpacedProcessSignaled { signal: Some(9) },
                )),
            })),
        },
        false,
        EcwProcessStdoutStream::Stdout,
    )
    .expect("exit message");
    assert_eq!(
        connect_value(signaled),
        serde_json::json!({
            "event": {
                "end": {
                    "exitCode": 137,
                    "exited": false,
                    "status": "signaled",
                    "error": "process terminated by signal 9",
                }
            }
        })
    );
}

#[test]
fn pty_process_events_encode_connect_pty_output() {
    let pty = process_event_to_connect_message(
        SpacedProcessEvent {
            process_id: "proc-1".to_string(),
            pid: Some(7),
            event: Some(spaced_process_event::Event::Stdout(SpacedProcessOutput {
                data: b"hello pty".to_vec(),
            })),
        },
        false,
        EcwProcessStdoutStream::Pty,
    )
    .expect("pty message");
    assert_eq!(
        connect_value(pty),
        serde_json::json!({ "event": { "data": { "pty": "aGVsbG8gcHR5" } } })
    );
}

#[tokio::test]
async fn incremental_connect_stream_decoder_handles_split_frames() {
    let first = connect_json_message(serde_json::json!({ "value": "one" }), false);
    let second = connect_json_message(serde_json::json!({ "value": "two" }), false);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&first);
    bytes.extend_from_slice(&second);
    let split_at = 7;
    let chunks = vec![
        Ok::<_, Infallible>(Bytes::copy_from_slice(&bytes[..split_at])),
        Ok(Bytes::copy_from_slice(&bytes[split_at..])),
    ];
    let mut body = Body::from_stream(stream::iter(chunks));
    let mut buffer = Vec::new();

    assert_eq!(
        decode_next_connect_json_stream_message::<TestMessage>(&mut body, &mut buffer)
            .await
            .unwrap(),
        Some(TestMessage {
            value: "one".to_string()
        })
    );
    assert_eq!(
        decode_next_connect_json_stream_message::<TestMessage>(&mut body, &mut buffer)
            .await
            .unwrap(),
        Some(TestMessage {
            value: "two".to_string()
        })
    );
    assert!(
        decode_next_connect_json_stream_message::<TestMessage>(&mut body, &mut buffer)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn incremental_connect_stream_decoder_rejects_large_messages() {
    let mut bytes = Vec::new();
    bytes.push(0);
    bytes.extend_from_slice(&((E2B_CONNECT_JSON_BODY_LIMIT_BYTES + 1) as u32).to_be_bytes());
    let mut body = Body::from(bytes);
    let mut buffer = Vec::new();

    let error = decode_next_connect_json_stream_message::<TestMessage>(&mut body, &mut buffer)
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
}
