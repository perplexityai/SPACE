use super::handles::{output, running, running_command};
use super::*;
use pplx_space_spaced_guest_protocol::{
    proto::operations::RecoveryHandle, recovery::RecoveryReason,
};

fn sequenced(handle: &RecoveryHandle, sequence: u64) -> RecoveryHandle {
    RecoveryHandle {
        contract_version: 2,
        sequence: Some(sequence),
        ..handle.clone()
    }
}
async fn input(
    service: &ExecutionService,
    handle: &RecoveryHandle,
    sequence: u64,
    data: &[u8],
) -> Result<(), Status> {
    Process::send_input(
        service,
        Request::new(SendInputRequest {
            process_id: String::new(),
            recovery: Some(if sequence == 0 {
                handle.clone()
            } else {
                sequenced(handle, sequence)
            }),
            data: data.to_vec(),
        }),
    )
    .await
    .map(|_| ())
}
async fn close(
    service: &ExecutionService,
    handle: &RecoveryHandle,
    sequence: u64,
) -> Result<(), Status> {
    Process::close_stdin(
        service,
        Request::new(CloseStdinRequest {
            process_id: String::new(),
            recovery: Some(if sequence == 0 {
                handle.clone()
            } else {
                sequenced(handle, sequence)
            }),
        }),
    )
    .await
    .map(|_| ())
}

#[tokio::test]
async fn mutation_receipts_replay_after_lost_replies_and_process_exit() {
    let service = cancellation_service();
    let handle = running(&service, false).await;
    input(&service, &handle, 1, b"only once\n").await.unwrap();
    input(&service, &handle, 1, b"only once\n").await.unwrap();
    assert_eq!(
        input(&service, &handle, 1, b"different")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        close(&service, &handle, 1).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        RecoveryReason::from_status(&close(&service, &handle, 3).await.unwrap_err()),
        Some(RecoveryReason::MutationOutOfOrder)
    );
    close(&service, &handle, 2).await.unwrap();
    assert_eq!(output(&service, handle.clone()).await, b"only once\n");
    // Replay precedes the running-process check for all terminal receipts.
    input(&service, &handle, 1, b"only once\n").await.unwrap();
    close(&service, &handle, 2).await.unwrap();
    assert_eq!(
        close(&service, &handle, 3).await.unwrap_err().code(),
        tonic::Code::NotFound
    );
    service.cancel();
    service.drain().await.unwrap();
}

#[tokio::test]
async fn quiescing_or_shutdown_settles_blocked_pipe_and_pty_input() {
    for (pty, sequenced_input, shutdown) in [
        (false, false, false),
        (false, true, false),
        (true, false, false),
        (true, true, false),
        (false, false, true),
        (false, true, true),
        (true, false, true),
        (true, true, true),
    ] {
        let service = cancellation_service();
        let command = if pty {
            "stty raw -echo; printf ready; sleep 60"
        } else {
            "printf ready; sleep 60"
        };
        let handle = running_command(
            &service,
            pty,
            vec!["/bin/sh".into(), "-c".into(), command.into()],
        )
        .await;
        let mut ready = Process::connect_process(
            &service,
            Request::new(ConnectProcessRequest {
                recovery: Some(handle.clone()),
                output_cursor: Some(0),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut output = Vec::new();
            while let Some(event) = ready.next().await {
                if let Some(process_event::Event::Stdout(chunk)) = event.unwrap().event {
                    output.extend(chunk.data);
                    if output == b"ready" {
                        return;
                    }
                }
            }
            panic!("child closed before ready");
        })
        .await
        .unwrap();
        drop(ready);
        let record = service
            .inner
            .processes
            .get_running(&ProcessId::parse(&handle.process_id).unwrap())
            .unwrap();
        let held = || match &record.input {
            ProcessInput::Pipe(input) => input.descriptor.try_lock().is_err(),
            ProcessInput::Pty(input) => input.writer.descriptor.try_lock().is_err(),
            ProcessInput::Disabled => panic!("input enabled"),
        };
        let slots = service.inner.process_payload_slots.available_permits();
        let data = vec![b'x'; 1024 * 1024];
        let sequence = u64::from(sequenced_input);
        let mut first = Box::pin(input(&service, &handle, sequence, &data));
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                assert!(futures_util::poll!(&mut first).is_pending());
                if held() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let response = if sequenced_input {
            let mut duplicate = Box::pin(input(&service, &handle, sequence, &data));
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    assert!(futures_util::poll!(&mut duplicate).is_pending());
                    if record.recovery.as_ref().unwrap().mutations.has_waiter() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            drop(first);
            duplicate
        } else {
            first
        };
        let (quiesced, response) = tokio::join!(
            async {
                let result = if shutdown {
                    service.cancel();
                    Ok(Ok(()))
                } else {
                    tokio::time::timeout(
                        Duration::from_secs(2),
                        service.configure_process_recovery(None),
                    )
                    .await
                };
                if result.is_err() {
                    service.cancel();
                }
                result
            },
            tokio::time::timeout(Duration::from_secs(2), response)
        );
        let error = response.unwrap().unwrap_err();
        let written = error
            .metadata()
            .get("space-process-input-written")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok());
        let replay = if sequenced_input && !shutdown && quiesced.is_ok() {
            service
                .configure_process_recovery(Some(handle.expected_activation_id.parse().unwrap()))
                .await
                .unwrap();
            Some(input(&service, &handle, sequence, &data).await.unwrap_err())
        } else {
            None
        };
        service.cancel();
        service.drain().await.unwrap();
        quiesced
            .expect("blocked input must not hold quiescing")
            .unwrap();
        assert_eq!(
            RecoveryReason::from_status(&error),
            Some(RecoveryReason::InputOutcomeUncertain)
        );
        assert!(
            written.is_some_and(|n| n > 0 && n < data.len()),
            "pty={pty}, sequenced={sequenced_input}: {error:?}"
        );
        assert!(!record.recovery.as_ref().unwrap().mutations.has_pending());
        assert_eq!(
            service.inner.process_payload_slots.available_permits(),
            slots
        );
        if let Some(replay) = replay {
            assert_eq!(replay.code(), error.code());
            assert_eq!(
                replay.metadata().clone().into_headers(),
                error.metadata().clone().into_headers()
            );
        }
    }
}

#[tokio::test]
async fn sequenced_pty_close_error_and_signal_are_replayable() {
    let service = cancellation_service();
    let handle = running(&service, true).await;
    for _ in 0..2 {
        assert_eq!(
            close(&service, &handle, 1).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
    }
    for _ in 0..2 {
        Process::send_signal(
            &service,
            Request::new(SendSignalRequest {
                process_id: String::new(),
                recovery: Some(sequenced(&handle, 2)),
                signal: libc::SIGKILL,
            }),
        )
        .await
        .unwrap();
    }
    output(&service, handle.clone()).await;
    assert_eq!(
        close(&service, &handle, 1).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    service.cancel();
    service.drain().await.unwrap();
}
