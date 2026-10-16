use super::*;
use crate::ECW_ENVD_PORT;
use crate::state::{EcwRouteOrigin, EcwSandboxHost};
use axum::http::{HeaderValue, StatusCode};
use pplx_space_common_core::types::{IdempotencyKey, NodeId, OrgId, SandboxId};
use pplx_space_sbx_mgr_client::restart_retry_on_successor_status;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

fn test_ecw_route(route_generation: u64) -> EcwResolvedRoute {
    let sandbox_id = SandboxId::new();
    EcwResolvedRoute {
        cache_epoch: route_generation,
        origin: EcwRouteOrigin {
            target: EcwSandboxHost {
                port: ECW_ENVD_PORT,
                sandbox_id,
            },
            transport: crate::runtime_route::EcwRouteTransport::Direct,
            envd_access_token_sha256: None,
            traffic_access_token_sha256: None,
            api_key_sha256: None,
        },
        sandbox_id,
        org_id: OrgId::new(),
        template_id: None,
        route_generation,
        current_node_id: NodeId::new(),
        backend_endpoint: "http://127.0.0.1:9800"
            .parse()
            .expect("valid backend endpoint"),
        space_api_route_token: None,
        service_port_ingress_token: None,
    }
}

fn started_process_event(process_id: &ProcessId) -> SpacedProcessEvent {
    SpacedProcessEvent {
        process_id: process_id.to_string(),
        pid: Some(123),
        event: Some(spaced_ops::process_event::Event::Started(
            spaced_ops::ProcessStarted { start_time: None },
        )),
    }
}

fn stdout_process_event(process_id: &ProcessId, data: &[u8]) -> SpacedProcessEvent {
    SpacedProcessEvent {
        process_id: process_id.to_string(),
        pid: Some(123),
        event: Some(spaced_ops::process_event::Event::Stdout(
            spaced_ops::ProcessOutput {
                data: data.to_vec(),
            },
        )),
    }
}

fn stderr_process_event(process_id: &ProcessId, data: &[u8]) -> SpacedProcessEvent {
    SpacedProcessEvent {
        process_id: process_id.to_string(),
        pid: Some(123),
        event: Some(spaced_ops::process_event::Event::Stderr(
            spaced_ops::ProcessOutput {
                data: data.to_vec(),
            },
        )),
    }
}

fn connected_process_event(process_id: &ProcessId) -> SpacedProcessEvent {
    SpacedProcessEvent {
        process_id: process_id.to_string(),
        pid: Some(123),
        event: Some(spaced_ops::process_event::Event::Connected(
            spaced_ops::ProcessConnected {},
        )),
    }
}

fn exited_process_event(process_id: &ProcessId) -> SpacedProcessEvent {
    SpacedProcessEvent {
        process_id: process_id.to_string(),
        pid: Some(123),
        event: Some(spaced_ops::process_event::Event::Exit(
            spaced_ops::ProcessExit {
                outcome: Some(spaced_ops::process_exit::Outcome::Exited(
                    spaced_ops::ProcessExited { exit_code: 0 },
                )),
            },
        )),
    }
}

fn stdout_bytes(events: Vec<Result<SpacedProcessEvent, tonic::Status>>) -> Vec<u8> {
    events
        .into_iter()
        .filter_map(|event| match event.expect("process event").event {
            Some(spaced_ops::process_event::Event::Stdout(output)) => Some(output.data),
            _ => None,
        })
        .flatten()
        .collect()
}

fn output_records(
    events: &[Result<SpacedProcessEvent, tonic::Status>],
) -> Vec<(&'static str, Vec<u8>)> {
    events
        .iter()
        .filter_map(
            |event| match event.as_ref().expect("process event").event.as_ref() {
                Some(spaced_ops::process_event::Event::Stdout(output)) => {
                    Some(("stdout", output.data.clone()))
                }
                Some(spaced_ops::process_event::Event::Stderr(output)) => {
                    Some(("stderr", output.data.clone()))
                }
                _ => None,
            },
        )
        .collect()
}

#[test]
fn process_start_respects_explicit_stdin_flag() {
    let route = test_ecw_route(0);
    let request = ProcessStartConnectRequestE2b {
        process: ProcessConfigRequestE2b {
            cmd: "cat".to_string(),
            args: Vec::new(),
            envs: StringMap::new(),
            cwd: None,
        },
        pty: None,
        tag: Some("server".to_string()),
        stdin: Some(false),
    };

    let native = build_process_start_request(&HeaderMap::new(), &route, request).unwrap();
    let operation = native.operation.expect("start operation");

    assert_eq!(operation.command, ["cat"]);
    assert!(!operation.stdin);
    IdempotencyKey::parse(operation.idempotency_key.expect("idempotency key"))
        .expect("valid idempotency key");
}

#[tokio::test]
async fn process_start_open_retry_preserves_execution_identity() {
    let route = test_ecw_route(0);
    let request = ProcessStartConnectRequestE2b {
        process: ProcessConfigRequestE2b {
            cmd: "true".to_string(),
            args: Vec::new(),
            envs: StringMap::new(),
            cwd: None,
        },
        pty: None,
        tag: None,
        stdin: None,
    };
    let native = build_process_start_request(&HeaderMap::new(), &route, request).unwrap();
    let expected_key = native
        .operation
        .as_ref()
        .and_then(|operation| operation.idempotency_key.clone())
        .expect("idempotency key");
    let observed_keys = Arc::new(Mutex::new(Vec::new()));
    let keys_for_attempt = Arc::clone(&observed_keys);
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_call = Arc::clone(&attempts);

    let (result, attempt_count) = retry_process_start_open(
        native,
        ProcessStartRecoveryMode::RetryStart,
        Duration::from_secs(2),
        move |request, _| {
            let key = request
                .operation
                .and_then(|operation| operation.idempotency_key)
                .expect("idempotency key");
            keys_for_attempt
                .lock()
                .expect("observed keys lock")
                .push(key);
            let attempts = Arc::clone(&attempts_for_call);
            async move {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(tonic::Status::cancelled("operation was canceled"))
                } else {
                    Ok("opened")
                }
            }
        },
    )
    .await
    .unwrap();

    assert_eq!(result, "opened");
    assert_eq!(attempt_count, 2);
    assert_eq!(
        *observed_keys.lock().expect("observed keys lock"),
        [expected_key.clone(), expected_key]
    );
}

#[test]
fn process_start_resume_preserves_original_timeout() {
    let route = test_ecw_route(0);
    let timeout = prost_types::Duration {
        seconds: 17,
        nanos: 250_000_000,
    };
    let start_request = SpacedStartProcessRequest {
        sandbox_id: route.sandbox_id.to_string(),
        operation: Some(spaced_ops::StartRequest {
            timeout: Some(timeout),
            ..Default::default()
        }),
    };
    let process_id = ProcessId::new();

    let resume_request = build_process_start_resume_request(&route, &start_request, &process_id, 3);

    assert_eq!(
        resume_request.operation.expect("connect operation").timeout,
        Some(timeout)
    );
}

#[test]
fn process_start_accepts_pty_size() {
    let route = test_ecw_route(0);
    let request = ProcessStartConnectRequestE2b {
        process: ProcessConfigRequestE2b {
            cmd: "cat".to_string(),
            args: Vec::new(),
            envs: StringMap::new(),
            cwd: None,
        },
        pty: Some(ProcessPtyRequestE2b {
            size: Some(ProcessPtySizeRequestE2b { rows: 24, cols: 80 }),
        }),
        tag: None,
        stdin: Some(true),
    };

    let native = build_process_start_request(&HeaderMap::new(), &route, request).unwrap();
    let operation = native.operation.expect("start operation");
    let size = operation.pty.expect("pty config").size.expect("pty size");
    assert_eq!(size.rows, 24);
    assert_eq!(size.cols, 80);
}

#[test]
fn process_signal_translation_uses_closed_vocabulary() {
    assert_eq!(
        process_signal_to_native(ProcessSignalValueE2b::Name("SIGNAL_SIGKILL".to_string()))
            .unwrap(),
        9
    );
    assert_eq!(
        process_signal_to_native(ProcessSignalValueE2b::Name("SIGNAL_SIGTERM".to_string()))
            .unwrap(),
        15
    );
    assert_eq!(
        process_signal_to_native(ProcessSignalValueE2b::Number(9)).unwrap(),
        9
    );
    process_signal_to_native(ProcessSignalValueE2b::Number(1)).unwrap_err();
    process_signal_to_native(ProcessSignalValueE2b::Name("SIGNAL_SIGHUP".to_string())).unwrap_err();
}

#[test]
fn process_start_accepts_tag_metadata() {
    let route = test_ecw_route(0);
    let request = ProcessStartConnectRequestE2b {
        process: ProcessConfigRequestE2b {
            cmd: "sleep".to_string(),
            args: vec!["60".to_string()],
            envs: StringMap::new(),
            cwd: None,
        },
        pty: None,
        tag: Some("server".to_string()),
        stdin: None,
    };

    let native = build_process_start_request(&HeaderMap::new(), &route, request).unwrap();
    let operation = native.operation.expect("start operation");

    assert_eq!(operation.command, ["sleep", "60"]);
    assert_eq!(operation.tag.as_deref(), Some("server"));
    assert!(operation.stdin);
}

#[test]
fn process_update_translates_pty_resize() {
    let request = ProcessUpdateRequestE2b {
        process: ProcessSelectorRequestE2b {
            pid: Some(123),
            tag: None,
        },
        pty: Some(ProcessPtyRequestE2b {
            size: Some(ProcessPtySizeRequestE2b { rows: 24, cols: 80 }),
        }),
    };

    let size = translate_process_pty_resize(request.pty.as_ref())
        .expect("translated resize")
        .expect("resize size");
    assert_eq!(size.rows, 24);
    assert_eq!(size.cols, 80);
}

#[test]
fn process_update_translation_accepts_empty_update() {
    let request = ProcessUpdateRequestE2b {
        process: ProcessSelectorRequestE2b {
            pid: Some(123),
            tag: None,
        },
        pty: None,
    };

    assert!(
        translate_process_pty_resize(request.pty.as_ref())
            .expect("accepted empty update")
            .is_none()
    );
}

#[test]
fn process_input_decodes_stdin_base64() {
    let input = ProcessInputRequestE2b {
        stdin: Some(BASE64_STANDARD.encode(b"hello stdin\n")),
        pty: None,
    };

    let bytes = process_input_bytes(input).expect("decoded input");
    assert_eq!(bytes, b"hello stdin\n");
}

#[test]
fn process_input_decodes_pty_base64() {
    let input = ProcessInputRequestE2b {
        stdin: None,
        pty: Some(BASE64_STANDARD.encode(b"pty input")),
    };

    let bytes = process_input_bytes(input).expect("decoded pty input");
    assert_eq!(bytes, b"pty input");
}

#[test]
fn process_input_rejects_ambiguous_input_fields() {
    let input = ProcessInputRequestE2b {
        stdin: Some(BASE64_STANDARD.encode(b"hello stdin\n")),
        pty: Some(BASE64_STANDARD.encode(b"pty input")),
    };

    let error = process_input_bytes(input).unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
}

#[test]
fn process_stream_input_decodes_connect_envelopes() {
    let mut body = Vec::new();
    body.extend(connect_json_envelope(serde_json::json!({
        "start": {
            "process": {
                "pid": 123
            }
        }
    })));
    body.extend(connect_json_envelope(serde_json::json!({
        "data": {
            "input": {
                "stdin": BASE64_STANDARD.encode(b"hello stdin\n")
            }
        }
    })));
    body.extend(connect_json_envelope(serde_json::json!({
        "keepalive": {}
    })));

    let mut messages =
        ecw_connect::decode_connect_json_stream_bytes::<ProcessStreamInputRequestE2b>(&body)
            .expect("decoded stream");
    assert_eq!(messages.len(), 3);

    match messages.remove(0).into_event().expect("start event") {
        ProcessStreamInputEventE2b::Start(start) => {
            assert_eq!(start.process.pid, Some(123));
        }
        _ => panic!("expected start event"),
    }
    match messages.remove(0).into_event().expect("data event") {
        ProcessStreamInputEventE2b::Data(data) => {
            let bytes = process_input_bytes(data.input).expect("stdin bytes");
            assert_eq!(bytes, b"hello stdin\n");
        }
        _ => panic!("expected data event"),
    }
    match messages.remove(0).into_event().expect("keepalive event") {
        ProcessStreamInputEventE2b::Keepalive => {}
        _ => panic!("expected keepalive event"),
    }
}

#[test]
fn process_stream_input_detects_connect_json_stream_requests() {
    let mut headers = HeaderMap::new();
    assert!(!is_connect_json_stream_request(&headers));

    headers.insert(
        header::CONTENT_TYPE,
        "application/connect+json; charset=utf-8".parse().unwrap(),
    );
    assert!(is_connect_json_stream_request(&headers));

    headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
    assert!(!is_connect_json_stream_request(&headers));
}

fn connect_json_envelope(value: Value) -> Vec<u8> {
    let payload = serde_json::to_vec(&value).expect("json payload");
    let mut envelope = Vec::with_capacity(5 + payload.len());
    envelope.push(0);
    envelope.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    envelope.extend_from_slice(&payload);
    envelope
}

#[test]
fn process_info_to_e2b_splits_command_and_preserves_config() {
    let info = spaced_ops::ProcessInfo {
        process_id: "proc_ignored".to_string(),
        pid: 123,
        command: vec![
            "python".to_string(),
            "-m".to_string(),
            "http.server".to_string(),
        ],
        cwd: Some("/workspace".to_string()),
        env: BTreeMap::from([("PORT".to_string(), "8000".to_string())])
            .into_iter()
            .collect(),
        user: Some("user".to_string()),
        tag: Some("server".to_string()),
        pty: None,
    };

    let response = process_info_to_e2b(info);

    assert_eq!(response.pid, 123);
    assert_eq!(response.config.cmd, "python");
    assert_eq!(response.config.args, ["-m", "http.server"]);
    assert_eq!(response.config.cwd.as_deref(), Some("/workspace"));
    assert_eq!(
        response.config.envs.get("PORT").map(String::as_str),
        Some("8000")
    );
    assert_eq!(response.tag.as_deref(), Some("server"));
}

#[test]
fn process_stdout_stream_uses_pty_metadata_when_present() {
    let mut process = spaced_ops::ProcessInfo {
        process_id: "proc-1".to_string(),
        pid: 123,
        command: vec!["bash".to_string()],
        cwd: None,
        env: BTreeMap::new().into_iter().collect(),
        user: None,
        tag: None,
        pty: None,
    };

    assert_eq!(
        process_stdout_stream(&process),
        ecw_connect::EcwProcessStdoutStream::Stdout
    );

    process.pty = Some(false);
    assert_eq!(
        process_stdout_stream(&process),
        ecw_connect::EcwProcessStdoutStream::Stdout
    );

    process.pty = Some(true);
    assert_eq!(
        process_stdout_stream(&process),
        ecw_connect::EcwProcessStdoutStream::Pty
    );
}

#[test]
fn process_start_idempotency_key_binds_caller_key_to_its_start() {
    let route = test_ecw_route(0);
    // Enough entries that two independently built env maps are unlikely to
    // iterate in the same order.
    let envs: StringMap = (0..32)
        .map(|index| (format!("VAR_{index}"), format!("value-{index}")))
        .collect();
    let backend_key = |client_key: Option<&str>, cmd: &str| {
        let mut headers = HeaderMap::new();
        if let Some(client_key) = client_key {
            headers.insert("idempotency-key", client_key.parse().unwrap());
        }
        let request = ProcessStartConnectRequestE2b {
            process: ProcessConfigRequestE2b {
                cmd: cmd.to_string(),
                args: vec!["-c".to_string(), "true".to_string()],
                envs: envs.clone(),
                cwd: None,
            },
            pty: None,
            tag: None,
            stdin: None,
        };
        build_process_start_request(&headers, &route, request)
            .unwrap()
            .operation
            .and_then(|operation| operation.idempotency_key)
            .expect("idempotency key")
    };

    assert_eq!(
        backend_key(Some("start-1"), "bash"),
        backend_key(Some("start-1"), "bash"),
        "a retried start must reach spaced with the original key",
    );
    assert_ne!(
        backend_key(Some("start-1"), "bash"),
        backend_key(Some("start-1"), "sh"),
        "a reused key must not replay a different command",
    );
    assert_ne!(
        backend_key(Some("start-1"), "bash"),
        backend_key(Some("start-2"), "bash"),
    );
    assert_ne!(
        backend_key(None, "bash"),
        backend_key(None, "bash"),
        "starts without a caller key stay distinct",
    );
}

#[test]
fn process_start_rejects_unusable_idempotency_keys() {
    let route = test_ecw_route(0);
    for value in [
        HeaderValue::from_static(" "),
        HeaderValue::from_str(&"k".repeat(MAX_PROCESS_START_IDEMPOTENCY_KEY_BYTES + 1)).unwrap(),
        HeaderValue::from_bytes(b"key-\xff").unwrap(),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert("idempotency-key", value);
        let request = ProcessStartConnectRequestE2b {
            process: ProcessConfigRequestE2b {
                cmd: "true".to_string(),
                args: Vec::new(),
                envs: StringMap::new(),
                cwd: None,
            },
            pty: None,
            tag: None,
            stdin: None,
        };

        let error = build_process_start_request(&headers, &route, request).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }
}

#[test]
fn connect_timeout_parses_milliseconds() {
    let mut headers = HeaderMap::new();
    headers.insert("connect-timeout-ms", "1500".parse().unwrap());

    let timeout = connect_timeout(&headers).unwrap().unwrap();
    assert_eq!(timeout.seconds, 1);
    assert_eq!(timeout.nanos, 500_000_000);
}

#[test]
fn process_stream_open_timeout_defaults_to_cap() {
    assert_eq!(
        process_stream_open_timeout(None),
        NodeApiOperationBudget::StreamOpen.timeout()
    );
}

#[test]
fn process_stream_open_timeout_caps_long_command_timeout() {
    let timeout = prost_types::Duration {
        seconds: 30,
        nanos: 0,
    };

    assert_eq!(
        process_stream_open_timeout(Some(&timeout)),
        NodeApiOperationBudget::StreamOpen.timeout()
    );
}

#[test]
fn process_stream_open_timeout_preserves_short_command_timeout() {
    let timeout = prost_types::Duration {
        seconds: 2,
        nanos: 500_000_000,
    };

    assert_eq!(
        process_stream_open_timeout(Some(&timeout)),
        Duration::from_millis(2_500)
    );
}

#[test]
fn process_stream_open_timeout_uses_cap_for_unbounded_command_timeout() {
    let timeout = prost_types::Duration {
        seconds: 0,
        nanos: 0,
    };

    assert_eq!(
        process_stream_open_timeout(Some(&timeout)),
        NodeApiOperationBudget::StreamOpen.timeout()
    );
}

#[test]
fn process_stream_stall_after_uses_command_timeout_plus_grace() {
    let timeout = prost_types::Duration {
        seconds: 1,
        nanos: 500_000_000,
    };

    assert_eq!(
        process_stream_stall_after(Some(&timeout)),
        Some(Duration::from_millis(1_500) + PROCESS_STREAM_STALL_GRACE)
    );
}

#[tokio::test]
async fn process_start_stream_reconnects_after_sandbox_manager_handoff() {
    let process_id = ProcessId::new();
    let started = started_process_event(&process_id);
    let before_cut = stdout_process_event(&process_id, b"before");
    let connected = connected_process_event(&process_id);
    let after_cut = stdout_process_event(&process_id, b"after");
    let exit = exited_process_event(&process_id);
    let initial = tokio_stream::iter(vec![
        Ok(started),
        Ok(before_cut),
        Err(restart_retry_on_successor_status("sandbox-manager handoff")),
    ]);
    let reopen_calls = Arc::new(Mutex::new(Vec::new()));
    let observed_reopen_calls = Arc::clone(&reopen_calls);
    let reopen = move |resume: ProcessStartResume| {
        observed_reopen_calls
            .lock()
            .expect("reopen calls lock")
            .push(resume);
        let after_cut = after_cut.clone();
        let connected = connected.clone();
        let exit = exit.clone();
        async move {
            let stream: BoxedSpacedProcessStream = Box::pin(tokio_stream::iter(vec![
                Ok(connected),
                Ok(after_cut),
                Ok(exit),
            ]));
            Ok(stream)
        }
    };

    let events = Box::pin(resumable_process_start_stream(initial, false, reopen))
        .collect::<Vec<_>>()
        .await;

    assert!(events.iter().all(Result::is_ok));
    assert_eq!(stdout_bytes(events), b"beforeafter");
    assert_eq!(
        *reopen_calls.lock().expect("reopen calls lock"),
        [ProcessStartResume::Connect {
            process_id,
            output_cursor: 1,
        }]
    );
}

#[tokio::test]
async fn process_start_stream_reconnects_after_node_api_cancellation() {
    let process_id = ProcessId::new();
    let connected = connected_process_event(&process_id);
    let exit = exited_process_event(&process_id);
    let initial = tokio_stream::iter(vec![
        Ok(started_process_event(&process_id)),
        Ok(stdout_process_event(&process_id, b"01")),
        Ok(stderr_process_event(&process_id, b"02")),
        Ok(stdout_process_event(&process_id, b"03")),
        Err(tonic::Status::cancelled("operation was canceled")),
    ]);
    let reopen_calls = Arc::new(Mutex::new(Vec::new()));
    let observed_reopen_calls = Arc::clone(&reopen_calls);
    let reopened_process_id = process_id.clone();
    let reopen = move |resume: ProcessStartResume| {
        observed_reopen_calls
            .lock()
            .expect("reopen calls lock")
            .push(resume);
        let connected = connected.clone();
        let exit = exit.clone();
        let process_id = reopened_process_id.clone();
        async move {
            let stream: BoxedSpacedProcessStream = Box::pin(tokio_stream::iter(vec![
                Ok(connected),
                Ok(stdout_process_event(&process_id, b"04")),
                Ok(stderr_process_event(&process_id, b"05")),
                Ok(exit),
            ]));
            Ok(stream)
        }
    };

    let events = Box::pin(resumable_process_start_stream(initial, false, reopen))
        .collect::<Vec<_>>()
        .await;

    assert!(events.iter().all(Result::is_ok));
    assert_eq!(
        output_records(&events),
        [
            ("stdout", b"01".to_vec()),
            ("stderr", b"02".to_vec()),
            ("stdout", b"03".to_vec()),
            ("stdout", b"04".to_vec()),
            ("stderr", b"05".to_vec()),
        ]
    );
    assert!(matches!(
        events.last(),
        Some(Ok(SpacedProcessEvent {
            event: Some(spaced_ops::process_event::Event::Exit(
                spaced_ops::ProcessExit {
                    outcome: Some(spaced_ops::process_exit::Outcome::Exited(
                        spaced_ops::ProcessExited { exit_code: 0 }
                    )),
                }
            )),
            ..
        }))
    ));
    assert_eq!(
        *reopen_calls.lock().expect("reopen calls lock"),
        [ProcessStartResume::Connect {
            process_id,
            output_cursor: 3,
        }]
    );
}

#[tokio::test]
async fn process_start_stream_bounds_connected_reopens_without_output() {
    let process_id = ProcessId::new();
    let initial = tokio_stream::iter(vec![Ok(started_process_event(&process_id))]);
    let reopen_count = Arc::new(AtomicUsize::new(0));
    let observed_reopen_count = Arc::clone(&reopen_count);
    let reopened_process_id = process_id.clone();
    let reopen = move |_| {
        let attempt = observed_reopen_count.fetch_add(1, Ordering::SeqCst);
        let connected = connected_process_event(&reopened_process_id);
        async move {
            if attempt >= 2 {
                return Err(tonic::Status::invalid_argument(
                    "unbounded connected reopen",
                ));
            }
            Ok(Box::pin(tokio_stream::iter(vec![Ok(connected)])) as BoxedSpacedProcessStream)
        }
    };

    let events = Box::pin(resumable_process_start_stream(initial, false, reopen))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(reopen_count.load(Ordering::SeqCst), 2);
    assert_eq!(
        events.last().unwrap().as_ref().unwrap_err().code(),
        tonic::Code::Unavailable
    );
}

#[tokio::test]
async fn process_start_stream_bounds_recoverable_errors_without_output() {
    let process_id = ProcessId::new();
    let initial = tokio_stream::iter(vec![
        Ok(started_process_event(&process_id)),
        Err(tonic::Status::cancelled("node-api rollout")),
    ]);
    let reopen_count = Arc::new(AtomicUsize::new(0));
    let observed_reopen_count = Arc::clone(&reopen_count);
    let reopened_process_id = process_id.clone();
    let reopen = move |_| {
        let attempt = observed_reopen_count.fetch_add(1, Ordering::SeqCst);
        let connected = connected_process_event(&reopened_process_id);
        async move {
            if attempt >= 2 {
                return Err(tonic::Status::invalid_argument(
                    "unbounded recoverable reopen",
                ));
            }
            Ok(Box::pin(tokio_stream::iter(vec![
                Ok(connected),
                Err(tonic::Status::cancelled("node-api rollout")),
            ])) as BoxedSpacedProcessStream)
        }
    };

    let events = Box::pin(resumable_process_start_stream(initial, false, reopen))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(reopen_count.load(Ordering::SeqCst), 2);
    assert_eq!(
        events.last().unwrap().as_ref().unwrap_err().code(),
        tonic::Code::Unavailable
    );
}

#[tokio::test]
async fn process_start_stream_output_resets_reopen_budget() {
    let process_id = ProcessId::new();
    let initial = tokio_stream::iter(vec![
        Ok(started_process_event(&process_id)),
        Err(tonic::Status::cancelled("node-api rollout")),
    ]);
    let reopen_count = Arc::new(AtomicUsize::new(0));
    let observed_reopen_count = Arc::clone(&reopen_count);
    let reopened_process_id = process_id.clone();
    let reopen = move |_| {
        let attempt = observed_reopen_count.fetch_add(1, Ordering::SeqCst);
        let connected = connected_process_event(&reopened_process_id);
        let output = stdout_process_event(&reopened_process_id, b"progress");
        let exit = exited_process_event(&reopened_process_id);
        async move {
            let events = match attempt {
                0 | 2 => vec![
                    Ok(connected),
                    Err(tonic::Status::cancelled("node-api rollout")),
                ],
                1 => vec![
                    Ok(connected),
                    Ok(output),
                    Err(tonic::Status::cancelled("node-api rollout")),
                ],
                3 => vec![Ok(connected), Ok(exit)],
                _ => panic!("unexpected reopen attempt"),
            };
            Ok(Box::pin(tokio_stream::iter(events)) as BoxedSpacedProcessStream)
        }
    };

    let events = Box::pin(resumable_process_start_stream(initial, false, reopen))
        .collect::<Vec<_>>()
        .await;

    assert!(events.iter().all(Result::is_ok));
    assert_eq!(stdout_bytes(events), b"progress");
    assert_eq!(reopen_count.load(Ordering::SeqCst), 4);
}

#[test]
fn process_start_reopen_backoff_is_bounded() {
    assert_eq!(
        [
            process_start_reopen_delay(0),
            process_start_reopen_delay(1),
            process_start_reopen_delay(2),
        ],
        [
            Some(Duration::from_millis(100)),
            Some(Duration::from_millis(200)),
            None,
        ]
    );
}

#[tokio::test]
async fn process_start_stream_rejects_output_from_a_different_process() {
    let process_id = ProcessId::new();
    let changed_process_id = ProcessId::new();
    let initial = tokio_stream::iter(vec![
        Ok(started_process_event(&process_id)),
        Ok(stdout_process_event(&changed_process_id, b"wrong process")),
        Ok(exited_process_event(&process_id)),
    ]);

    let events = Box::pin(resumable_process_start_stream(initial, false, |_| async {
        unreachable!("the stream should fail before reopening")
    }))
    .collect::<Vec<_>>()
    .await;

    assert_eq!(events.len(), 2);
    events[0].as_ref().expect("first event should succeed");
    assert_eq!(
        events[1].as_ref().unwrap_err().code(),
        tonic::Code::DataLoss
    );
}

#[tokio::test]
async fn process_start_stream_does_not_reopen_after_terminal_result() {
    let process_id = ProcessId::new();
    let initial = tokio_stream::iter(vec![
        Ok(started_process_event(&process_id)),
        Ok(exited_process_event(&process_id)),
        Err(tonic::Status::cancelled("connection closed after trailers")),
    ]);
    let reopen_count = Arc::new(AtomicUsize::new(0));
    let observed_reopen_count = Arc::clone(&reopen_count);
    let reopened_process_id = process_id.clone();

    let events = Box::pin(resumable_process_start_stream(initial, false, move |_| {
        observed_reopen_count.fetch_add(1, Ordering::SeqCst);
        let connected = connected_process_event(&reopened_process_id);
        let exit = exited_process_event(&reopened_process_id);
        async move {
            Ok(Box::pin(tokio_stream::iter(vec![Ok(connected), Ok(exit)]))
                as BoxedSpacedProcessStream)
        }
    }))
    .collect::<Vec<_>>()
    .await;

    let terminal_count = events
        .iter()
        .filter(|event| {
            matches!(
                event,
                Ok(SpacedProcessEvent {
                    event: Some(spaced_ops::process_event::Event::Exit(_)),
                    ..
                })
            )
        })
        .count();
    assert_eq!(terminal_count, 1);
    assert_eq!(reopen_count.load(Ordering::SeqCst), 0);
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn dropping_process_start_stream_cancels_pending_reopen() {
    let initial = tokio_stream::iter(vec![Err(tonic::Status::cancelled(
        "operation was canceled",
    ))]);
    let reopen_started = Arc::new(tokio::sync::Notify::new());
    let observed_reopen_started = Arc::clone(&reopen_started);
    let reopen_dropped = Arc::new(AtomicBool::new(false));
    let observed_reopen_dropped = Arc::clone(&reopen_dropped);
    let reopen_count = Arc::new(AtomicUsize::new(0));
    let observed_reopen_count = Arc::clone(&reopen_count);
    let mut stream = Box::pin(resumable_process_start_stream(initial, false, move |_| {
        observed_reopen_count.fetch_add(1, Ordering::SeqCst);
        let reopen_started = Arc::clone(&observed_reopen_started);
        let reopen_dropped = Arc::clone(&observed_reopen_dropped);
        async move {
            let _drop_flag = DropFlag(reopen_dropped);
            reopen_started.notify_one();
            std::future::pending::<Result<BoxedSpacedProcessStream, tonic::Status>>().await
        }
    }));
    let poller = tokio::spawn(async move { stream.next().await });
    reopen_started.notified().await;

    poller.abort();
    let error = poller.await.unwrap_err();

    assert!(error.is_cancelled());
    assert!(reopen_dropped.load(Ordering::SeqCst));
    assert_eq!(reopen_count.load(Ordering::SeqCst), 1);
}

#[test]
fn process_start_recovery_accepts_backend_cancellation() {
    assert!(is_recoverable_process_start_interruption(
        &tonic::Status::cancelled("operation was canceled")
    ));
    assert!(!is_recoverable_process_start_interruption(
        &tonic::Status::deadline_exceeded("command deadline elapsed")
    ));
    assert!(!is_recoverable_process_start_interruption(
        &tonic::Status::invalid_argument("invalid process")
    ));
}

#[test]
fn process_start_recovery_telemetry_classifies_bounded_statuses() {
    let statuses = [
        tonic::Status::cancelled("cancelled"),
        tonic::Status::internal("connection reset"),
        tonic::Status::unavailable("unavailable"),
        tonic::Status::unknown("transport error"),
    ];

    let interruptions = statuses.map(|status| {
        process_start_recovery_interruption(&status)
            .expect("recoverable interruption")
            .to_string()
    });

    assert_eq!(
        interruptions,
        ["cancelled", "internal", "unavailable", "unknown"]
    );
    assert_eq!(
        ProcessStartResume::Start.recovery_mode().to_string(),
        "retry_start"
    );
    assert_eq!(
        ProcessStartResume::Connect {
            process_id: ProcessId::new(),
            output_cursor: 7,
        }
        .recovery_mode()
        .to_string(),
        "reattach"
    );
    assert!(
        process_start_recovery_interruption(&tonic::Status::deadline_exceeded("deadline"))
            .is_none()
    );
}

#[tokio::test]
async fn process_start_stream_retries_start_after_handoff_before_process_is_bound() {
    let process_id = ProcessId::new();
    let started = started_process_event(&process_id);
    let exit = exited_process_event(&process_id);
    let connected = connected_process_event(&process_id);
    let replayed_output = stdout_process_event(&process_id, b"replayed");
    let initial = tokio_stream::iter(vec![Err(restart_retry_on_successor_status(
        "sandbox-manager handoff",
    ))]);
    let reopen_calls = Arc::new(Mutex::new(Vec::new()));
    let observed_reopen_calls = Arc::clone(&reopen_calls);
    let reopen = move |resume: ProcessStartResume| {
        observed_reopen_calls
            .lock()
            .expect("reopen calls lock")
            .push(resume.clone());
        let started = started.clone();
        let exit = exit.clone();
        let connected = connected.clone();
        let replayed_output = replayed_output.clone();
        async move {
            let stream: BoxedSpacedProcessStream = match resume {
                ProcessStartResume::Start => Box::pin(tokio_stream::iter(vec![Ok(started)])),
                ProcessStartResume::Connect { .. } => Box::pin(tokio_stream::iter(vec![
                    Ok(connected),
                    Ok(replayed_output),
                    Ok(exit),
                ])),
            };
            Ok(stream)
        }
    };

    let events = Box::pin(resumable_process_start_stream(initial, false, reopen))
        .collect::<Vec<_>>()
        .await;

    assert!(events.iter().all(Result::is_ok));
    assert_eq!(stdout_bytes(events), b"replayed");
    assert_eq!(
        *reopen_calls.lock().expect("reopen calls lock"),
        [
            ProcessStartResume::Start,
            ProcessStartResume::Connect {
                process_id,
                output_cursor: 0,
            }
        ]
    );
}

#[tokio::test]
async fn process_start_stream_replays_output_after_initial_open_retry() {
    let process_id = ProcessId::new();
    let initial = tokio_stream::iter(vec![Ok(started_process_event(&process_id))]);
    let output = stdout_process_event(&process_id, b"replayed");
    let connected = connected_process_event(&process_id);
    let exit = exited_process_event(&process_id);
    let reopen_calls = Arc::new(Mutex::new(Vec::new()));
    let observed_reopen_calls = Arc::clone(&reopen_calls);
    let reopen = move |resume: ProcessStartResume| {
        observed_reopen_calls
            .lock()
            .expect("reopen calls lock")
            .push(resume);
        let output = output.clone();
        let connected = connected.clone();
        let exit = exit.clone();
        async move {
            let stream: BoxedSpacedProcessStream = Box::pin(tokio_stream::iter(vec![
                Ok(connected),
                Ok(output),
                Ok(exit),
            ]));
            Ok(stream)
        }
    };

    let events = Box::pin(resumable_process_start_stream(initial, true, reopen))
        .collect::<Vec<_>>()
        .await;

    assert!(events.iter().all(Result::is_ok));
    assert_eq!(stdout_bytes(events), b"replayed");
    assert_eq!(
        *reopen_calls.lock().expect("reopen calls lock"),
        [ProcessStartResume::Connect {
            process_id,
            output_cursor: 0,
        }]
    );
}

#[test]
fn process_replay_miss_statuses_are_bounded() {
    let replay_miss_codes = [
        tonic::Code::NotFound,
        tonic::Code::FailedPrecondition,
        tonic::Code::ResourceExhausted,
    ];

    let replay_misses = replay_miss_codes
        .map(|code| is_process_replay_miss(&tonic::Status::new(code, "replay failed")));

    assert_eq!(replay_misses, [true, true, true]);
    assert!(!is_process_replay_miss(&tonic::Status::unavailable(
        "transport failed"
    )));
    let backpressure = pplx_space_common_grpc::uds_per_sandbox_limit_exceeded_status(
        "SpacedConnectProcess UDS per-sandbox limiter exceeded",
    );
    assert!(!is_process_replay_miss(&backpressure));
    assert_eq!(
        process_start_delivery_failure(&backpressure),
        Some(ProcessStartDeliveryOutcome::Backpressure)
    );
    assert_eq!(
        process_start_delivery_failure(&tonic::Status::not_found("cursor pruned")),
        Some(ProcessStartDeliveryOutcome::ReplayMiss)
    );
}

#[test]
fn process_start_reopen_preserves_backend_status_classes() {
    let resume = ProcessStartResume::Connect {
        process_id: ProcessId::new(),
        output_cursor: 7,
    };
    let not_found = process_start_reopen_status(
        ecw_connect::tonic_status_to_ecw_error(tonic::Status::not_found("cursor pruned")),
        &resume,
    );
    assert_eq!(not_found.code(), tonic::Code::NotFound);

    let backpressure = process_start_reopen_status(
        ecw_connect::tonic_status_to_ecw_error(
            pplx_space_common_grpc::uds_per_sandbox_limit_exceeded_status("busy"),
        ),
        &resume,
    );
    assert_eq!(backpressure.code(), tonic::Code::ResourceExhausted);
    assert!(UdsPerSandboxLimitExceeded::from_status(&backpressure).is_some());
}

#[test]
fn process_status_classification_does_not_expose_status_messages() {
    let first = tonic::Status::unauthenticated("Basic user@example.com");
    let second = tonic::Status::unauthenticated("different secret");

    assert_eq!(
        process_start_status_class(&first),
        ProcessStartStatusClass::BackendStatus
    );
    assert_eq!(
        process_start_status_class(&first),
        process_start_status_class(&second)
    );
    assert_eq!(
        process_start_status_class(
            &pplx_space_common_grpc::uds_per_sandbox_limit_exceeded_status("backpressure")
        ),
        ProcessStartStatusClass::UdsBackpressure
    );
}

#[test]
fn basic_auth_username_is_forwarded() {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        format!(
            "Basic {}",
            BASE64_STANDARD.encode(format!("{DEFAULT_GUEST_USER}:password"))
        )
        .parse()
        .unwrap(),
    );

    assert_eq!(
        envd_basic_username(&headers),
        Some(DEFAULT_GUEST_USER.to_string()),
    );
}

#[test]
fn default_workdir_uses_configured_templates() {
    let mut env = pplx_space_test_env::TestEnv::default();
    assert_eq!(
        ecw_default_workdir_from_environment(Some("code-runtime"), &env),
        Some(format!("/home/{DEFAULT_GUEST_USER}"))
    );
    assert_eq!(
        ecw_default_workdir_from_environment(Some("custom-template"), &env),
        None
    );
    env.set(
        "SPACE_E2B_HOME_WORKDIR_TEMPLATES",
        "custom-template, other-template",
    );

    assert_eq!(
        ecw_default_workdir_from_environment(Some("custom-template"), &env),
        Some(format!("/home/{DEFAULT_GUEST_USER}"))
    );
    assert_eq!(
        ecw_default_workdir_from_environment(Some("other-template"), &env),
        Some(format!("/home/{DEFAULT_GUEST_USER}"))
    );
    assert_eq!(
        ecw_default_workdir_from_environment(Some("code-runtime"), &env),
        None
    );
    assert_eq!(
        ecw_default_workdir_from_environment(Some("custom-template-extra"), &env),
        None
    );
    assert_eq!(ecw_default_workdir_from_environment(None, &env), None);
    env.set("SPACE_E2B_HOME_WORKDIR_TEMPLATES", "");
    assert_eq!(ecw_default_workdir_from_environment(Some(""), &env), None);
    assert_eq!(
        ecw_default_workdir_from_environment(Some("custom-template"), &env),
        None
    );
}
