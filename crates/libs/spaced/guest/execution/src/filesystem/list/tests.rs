use super::*;
use std::sync::Arc;

use pplx_space_common_core::types::GuestPath;
use tokio::sync::Semaphore;

#[test]
fn directory_budget_counts_cumulative_traversal_not_stack_width() {
    let mut pending = Vec::new();
    let mut directories_enqueued = MAX_LIST_DIRECTORIES;

    let error = push_pending_directory(
        &mut pending,
        &mut directories_enqueued,
        PathBuf::from("one-too-many"),
        2,
    )
    .expect_err("cumulative directory budget should reject another directory");

    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    // The request's own size, not load: retrying gives the same answer.
    assert!(!pplx_space_common_grpc::is_refused_before_start(&error));
    assert!(pending.is_empty());
    assert_eq!(directories_enqueued, MAX_LIST_DIRECTORIES);
}

#[tokio::test]
async fn list_stream_holds_its_shared_slot_until_the_stream_is_dropped() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    for index in 0..40 {
        std::fs::write(temp_dir.path().join(format!("entry-{index}")), b"x")
            .expect("create list entry");
    }
    let slots = Arc::new(Semaphore::new(1));
    let permit = Arc::clone(&slots)
        .try_acquire_owned()
        .expect("first stream slot");
    let path = GuestPath::parse(temp_dir.path().display().to_string()).expect("guest path");

    let stream = list_stream(path, None, permit).await.expect("list stream");
    assert!(
        Arc::clone(&slots).try_acquire_owned().is_err(),
        "a live response stream must retain its slot"
    );

    drop(stream);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if slots.available_permits() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping the response stream must release its slot");
}
