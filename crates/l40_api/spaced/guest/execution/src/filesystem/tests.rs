use super::*;
use std::time::Duration;

// A burst larger than the slot budget waits for a slot instead of failing at
// once; the queue's bounds are `QueuedSlots`'.
#[tokio::test(start_paused = true)]
async fn a_file_operation_burst_queues_until_a_slot_frees() {
    let service = FilesystemWorkerService::new(
        GuestUserName::parse("user".to_string()).expect("user name"),
        CancellationToken::new(),
    );
    let held = service
        .operation_admission
        .acquire(MAX_CONCURRENT_FILE_OPERATIONS as u32)
        .await
        .unwrap();
    let waiter = tokio::spawn({
        let service = service.clone();
        async move { service.acquire_file_operation_slot().await }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!waiter.is_finished());
    drop(held);
    let _admitted = waiter
        .await
        .unwrap()
        .expect("a queued operation is admitted once a slot frees");
}
