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
            recovery: Some(sequenced(handle, sequence)),
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
            recovery: Some(sequenced(handle, sequence)),
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
async fn quiescing_or_shutdown_settles_a_blocked_sequenced_input() {
    for shutdown in [false, true] {
        let service = cancellation_service();
        let handle = running_command(
            &service,
            false,
            vec!["/bin/sh".into(), "-c".into(), "sleep 60".into()],
        )
        .await;
        let record = service
            .inner
            .processes
            .get_running(&ProcessId::parse(&handle.process_id).unwrap())
            .unwrap();
        let ProcessInput::Pipe(stdin) = &record.input else {
            panic!("pipe")
        };
        let slots = service.inner.process_payload_slots.available_permits();
        let data = vec![b'x'; 1024 * 1024];
        let mut first = Box::pin(input(&service, &handle, 1, &data));
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                assert!(futures_util::poll!(&mut first).is_pending());
                if record.recovery.as_ref().unwrap().mutations.has_pending()
                    && stdin.descriptor.try_lock().is_err()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut duplicate = Box::pin(input(&service, &handle, 1, &data));
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
        let quiesced = if shutdown {
            service.cancel();
            Ok(Ok(()))
        } else {
            tokio::time::timeout(
                Duration::from_secs(2),
                service.configure_process_recovery(None),
            )
            .await
        };
        // Even a failing negative control must release its real child and writer.
        if quiesced.is_err() {
            service.cancel();
        }
        let error = tokio::time::timeout(Duration::from_secs(2), duplicate)
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(
            RecoveryReason::from_status(&error),
            Some(RecoveryReason::InputOutcomeUncertain)
        );
        assert!(!record.recovery.as_ref().unwrap().mutations.has_pending());
        assert_eq!(
            service.inner.process_payload_slots.available_permits(),
            slots
        );
        if !shutdown && quiesced.is_ok() {
            service
                .configure_process_recovery(Some(handle.expected_activation_id.parse().unwrap()))
                .await
                .unwrap();
            let replay = input(&service, &handle, 1, &data).await.unwrap_err();
            assert_eq!(replay.code(), error.code());
            assert_eq!(
                replay.metadata().clone().into_headers(),
                error.metadata().clone().into_headers()
            );
        }
        service.cancel();
        service.drain().await.unwrap();
        quiesced
            .expect("a blocked stdin must not block quiescing")
            .unwrap();
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
