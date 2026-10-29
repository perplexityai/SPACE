use super::*;
use pplx_space_spaced_guest_protocol::{
    proto::operations::{PtyConfig, RecoveryHandle},
    recovery::RecoveryReason,
};

pub(super) async fn running(service: &ExecutionService, pty: bool) -> RecoveryHandle {
    running_command(service, pty, vec!["/bin/cat".into()]).await
}

pub(super) async fn running_command(
    service: &ExecutionService,
    pty: bool,
    command: Vec<String>,
) -> RecoveryHandle {
    let mut request = recovery_request(service, "controlled").await;
    request
        .recovery
        .as_mut()
        .expect("recovery envelope")
        .command = command;
    request.stdin = true;
    request.pty = pty.then_some(PtyConfig { size: None });
    let activation = request
        .recovery
        .as_ref()
        .expect("envelope")
        .expected_activation_id
        .clone();
    let mut events = Process::start(service, Request::new(request))
        .await
        .expect("start cat")
        .into_inner();
    let event = events
        .next()
        .await
        .expect("started event")
        .expect("started");
    let started = event
        .event
        .and_then(|event| match event {
            process_event::Event::Started(started) => Some(started),
            _ => None,
        })
        .expect("start prefix");
    RecoveryHandle {
        contract_version: 1,
        sequence: None,
        expected_instance_id: started.instance_id.expect("instance"),
        expected_activation_id: activation,
        process_id: event.process_id,
        process_incarnation: started.process_incarnation.expect("incarnation"),
    }
}

async fn connect(
    service: &ExecutionService,
    handle: RecoveryHandle,
) -> Result<Response<<ExecutionService as Process>::ConnectProcessStream>, Status> {
    Process::connect_process(
        service,
        Request::new(ConnectProcessRequest {
            recovery: Some(handle),
            output_cursor: Some(0),
            ..Default::default()
        }),
    )
    .await
}

pub(super) async fn output(service: &ExecutionService, handle: RecoveryHandle) -> Vec<u8> {
    let mut events = connect(service, handle)
        .await
        .expect("connect")
        .into_inner();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut output = Vec::new();
        while let Some(event) = events.next().await {
            if let Some(process_event::Event::Stdout(chunk)) = event.expect("process event").event {
                output.extend(chunk.data);
            }
        }
        output
    })
    .await
    .expect("process completes")
}

#[tokio::test]
async fn fenced_pipe_controls_and_completed_reconnect_use_the_original_record() {
    let service = cancellation_service();
    let handle = running(&service, false).await;
    Process::send_input(
        &service,
        Request::new(SendInputRequest {
            recovery: Some(handle.clone()),
            data: b"one copy\n".to_vec(),
            process_id: String::new(),
        }),
    )
    .await
    .unwrap();
    Process::close_stdin(
        &service,
        Request::new(CloseStdinRequest {
            recovery: Some(handle.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(output(&service, handle.clone()).await, b"one copy\n");
    assert_eq!(output(&service, handle.clone()).await, b"one copy\n");
    let id = ProcessId::parse(&handle.process_id).unwrap();
    let original = service.inner.processes.get_record(&id).unwrap();
    // Reusing an ID never turns an old handle into control of its replacement.
    service
        .inner
        .processes
        .records
        .remove(&ProcessId::parse(&handle.process_id).unwrap());
    let replacement = StartRequest {
        process_id: Some(handle.process_id.clone()),
        idempotency_key: Some("replacement".into()),
        recovery: Some(RecoveryStart {
            contract_version: 1,
            expected_instance_id: handle.expected_instance_id.clone(),
            expected_activation_id: handle.expected_activation_id.clone(),
            command: vec!["/bin/true".into()],
        }),
        ..Default::default()
    };
    events(&service, replacement).await;
    service.inner.processes.remove_if_current(&id, &original);
    assert!(
        service.inner.processes.get_record(&id).is_some(),
        "stale cleanup must preserve a replacement"
    );
    let error = connect(&service, handle).await.err().unwrap();
    assert_eq!(
        RecoveryReason::from_status(&error),
        Some(RecoveryReason::ProcessReplaced)
    );
    service.cancel();
    service.drain().await.unwrap();
}

#[tokio::test]
async fn fenced_pty_resize_and_signal_reach_the_selected_process() {
    let service = cancellation_service();
    let handle = running(&service, true).await;
    Process::resize_pty(
        &service,
        Request::new(ResizePtyRequest {
            recovery: Some(handle.clone()),
            size: Some(PtySize {
                rows: 24,
                cols: 100,
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    Process::send_signal(
        &service,
        Request::new(SendSignalRequest {
            recovery: Some(handle.clone()),
            signal: libc::SIGKILL,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let mut stream = connect(&service, handle).await.unwrap().into_inner();
    let events = tokio::time::timeout(Duration::from_secs(5), async {
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            events.push(event.unwrap());
        }
        events
    })
    .await
    .unwrap();
    assert!(
        events
            .iter()
            .any(|event| matches!(&event.event, Some(process_event::Event::Exit(_))))
    );
    service.cancel();
    service.drain().await.unwrap();
}

#[tokio::test]
async fn every_fenced_entrypoint_rejects_stale_or_ambiguous_handles_before_effects() {
    let service = cancellation_service();
    let valid = running(&service, false).await;
    for case in 0..7 {
        let mut handle = valid.clone();
        let mut legacy = String::new();
        let (code, reason) = match case {
            0 => {
                handle.expected_instance_id = ProcessRecoveryToken::new().to_string();
                (
                    tonic::Code::FailedPrecondition,
                    Some(RecoveryReason::EngineRestarted),
                )
            }
            1 => {
                handle.expected_activation_id = ProcessRecoveryToken::new().to_string();
                (
                    tonic::Code::FailedPrecondition,
                    Some(RecoveryReason::ActivationChanged),
                )
            }
            2 => {
                handle.process_incarnation = ProcessRecoveryToken::new().to_string();
                (
                    tonic::Code::FailedPrecondition,
                    Some(RecoveryReason::ProcessReplaced),
                )
            }
            3 => {
                handle.process_id = "missing".into();
                (tonic::Code::NotFound, None)
            }
            4 => {
                handle.contract_version = 3;
                (
                    tonic::Code::FailedPrecondition,
                    Some(RecoveryReason::UnsupportedContract),
                )
            }
            5 => {
                legacy = valid.process_id.clone();
                (tonic::Code::InvalidArgument, None)
            }
            _ => {
                let activation = ProcessRecoveryToken::new();
                service
                    .configure_process_recovery(Some(activation))
                    .await
                    .unwrap();
                // Learning the new epoch cannot renew a handle from an old activation.
                handle.expected_activation_id = activation.to_string();
                (
                    tonic::Code::FailedPrecondition,
                    Some(RecoveryReason::ActivationChanged),
                )
            }
        };
        let statuses = [
            Process::connect_process(
                &service,
                Request::new(ConnectProcessRequest {
                    process_id: legacy.clone(),
                    recovery: Some(handle.clone()),
                    ..Default::default()
                }),
            )
            .await
            .err()
            .unwrap(),
            Process::send_input(
                &service,
                Request::new(SendInputRequest {
                    process_id: legacy.clone(),
                    recovery: Some(handle.clone()),
                    data: b"must not write".to_vec(),
                }),
            )
            .await
            .unwrap_err(),
            Process::close_stdin(
                &service,
                Request::new(CloseStdinRequest {
                    process_id: legacy.clone(),
                    recovery: Some(handle.clone()),
                }),
            )
            .await
            .unwrap_err(),
            Process::send_signal(
                &service,
                Request::new(SendSignalRequest {
                    process_id: legacy.clone(),
                    recovery: Some(handle.clone()),
                    signal: libc::SIGKILL,
                }),
            )
            .await
            .unwrap_err(),
            Process::resize_pty(
                &service,
                Request::new(ResizePtyRequest {
                    process_id: legacy,
                    recovery: Some(handle),
                    size: Some(PtySize { rows: 24, cols: 80 }),
                }),
            )
            .await
            .unwrap_err(),
        ];
        for status in statuses {
            assert_eq!(status.code(), code, "case {case}");
            assert_eq!(RecoveryReason::from_status(&status), reason, "case {case}");
        }
    }
    service
        .configure_process_recovery(Some(valid.expected_activation_id.parse().unwrap()))
        .await
        .unwrap();
    Process::send_input(
        &service,
        Request::new(SendInputRequest {
            recovery: Some(valid.clone()),
            data: b"still alive\n".to_vec(),
            process_id: String::new(),
        }),
    )
    .await
    .unwrap();
    Process::close_stdin(
        &service,
        Request::new(CloseStdinRequest {
            recovery: Some(valid.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(output(&service, valid).await, b"still alive\n");
    service.cancel();
    service.drain().await.unwrap();
}

#[tokio::test]
async fn quiescing_recovery_waits_for_an_admitted_control_and_cancellation_releases_it() {
    let service = cancellation_service();
    let handle = running(&service, false).await;
    let record = service
        .inner
        .processes
        .get_running(&ProcessId::parse(&handle.process_id).unwrap())
        .unwrap();
    let ProcessInput::Pipe(input) = &record.input else {
        panic!("pipe")
    };
    let lock = input.descriptor.lock().await;
    let mut writing = Box::pin(Process::send_input(
        &service,
        Request::new(SendInputRequest {
            recovery: Some(handle),
            data: b"pending".to_vec(),
            process_id: String::new(),
        }),
    ));
    assert!(futures_util::poll!(&mut writing).is_pending());
    let mut disabling = Box::pin(service.configure_process_recovery(None));
    assert!(futures_util::poll!(&mut disabling).is_pending());
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(writing, disabling)
    })
    .await;
    drop(lock);
    let (writing, disabling) = result.expect("quiescing interrupts the admitted writer");
    writing.unwrap_err();
    disabling.unwrap();
    assert!(service.process_execution_info().activation_id.is_none());
    service.cancel();
    service.drain().await.unwrap();
}
