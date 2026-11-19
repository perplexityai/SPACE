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

// Removing a directory that still has entries without `recursive` is refused
// for the path's state, not reported as a guest fault, and removes nothing.
#[tokio::test]
async fn a_non_recursive_remove_of_a_non_empty_directory_fails_its_precondition() {
    let user = nix::unistd::User::from_uid(nix::unistd::Uid::current())
        .expect("user lookup")
        .expect("current user");
    let service = FilesystemWorkerService::new(
        GuestUserName::parse(user.name).expect("user name"),
        CancellationToken::new(),
    );
    let directory = tempfile::tempdir().expect("directory");
    let target = directory.path().join("full");
    std::fs::create_dir(&target).expect("target directory");
    std::fs::write(target.join("entry"), b"kept").expect("entry");

    let error = service
        .remove(Request::new(RemoveRequest {
            path: target.display().to_string(),
            recursive: false,
            missing_ok: true,
            user: None,
        }))
        .await
        .expect_err("a non-empty directory is not removed without recursive");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(std::fs::read(target.join("entry")).expect("entry"), b"kept");
}
