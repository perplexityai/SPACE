use super::handles::{output, running};
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
async fn cancellation_keeps_the_mutation_owner_and_its_activation_barrier() {
    let service = cancellation_service();
    let handle = running(&service, false).await;
    let record = service
        .inner
        .processes
        .get_running(&ProcessId::parse(&handle.process_id).unwrap())
        .unwrap();
    let ProcessInput::Pipe(stdin) = &record.input else {
        panic!("pipe")
    };
    let slots = service.inner.process_payload_slots.available_permits();
    let locked = stdin.descriptor.lock().await;
    let mut first = Box::pin(input(&service, &handle, 1, b"owned\n"));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            assert!(futures_util::poll!(&mut first).is_pending());
            if record.recovery.as_ref().unwrap().mutations.has_pending() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut duplicate = Box::pin(input(&service, &handle, 1, b"owned\n"));
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
    assert_eq!(
        service.inner.process_payload_slots.available_permits(),
        slots - 2
    );
    assert_eq!(
        input(&service, &handle, 1, b"conflict")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    drop(first);
    assert_eq!(
        service.inner.process_payload_slots.available_permits(),
        slots - 2
    );
    // A duplicate is a waiter, not a second effect owner.
    drop(duplicate);
    let mut disabling = Box::pin(service.configure_process_recovery(None));
    assert!(futures_util::poll!(&mut disabling).is_pending());
    drop(locked);
    tokio::time::timeout(Duration::from_secs(2), &mut disabling)
        .await
        .unwrap()
        .unwrap();
    // No memory restore took place: re-enable this same activation to retry the receipt.
    service
        .configure_process_recovery(Some(handle.expected_activation_id.parse().unwrap()))
        .await
        .unwrap();
    input(&service, &handle, 1, b"owned\n").await.unwrap();
    close(&service, &handle, 2).await.unwrap();
    assert_eq!(output(&service, handle).await, b"owned\n");
    service.cancel();
    service.drain().await.unwrap();
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
