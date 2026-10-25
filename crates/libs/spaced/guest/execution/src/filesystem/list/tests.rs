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

    let stream = list_stream(
        path,
        None,
        permit,
        tokio_util::task::TaskTracker::new(),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("list stream");
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

#[tokio::test]
async fn cancelled_list_reports_failure_after_buffered_entries_without_blocking_drain() {
    use futures_util::StreamExt;
    let directory = tempfile::tempdir().unwrap();
    for index in 0..64 {
        std::fs::write(directory.path().join(format!("entry-{index}")), b"x").unwrap();
    }
    let slots = Arc::new(Semaphore::new(1));
    let tasks = tokio_util::task::TaskTracker::new();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let mut stream = list_stream(
        GuestPath::parse(directory.path().display().to_string()).unwrap(),
        None,
        slots.clone().try_acquire_owned().unwrap(),
        tasks.clone(),
        shutdown.clone(),
    )
    .await
    .unwrap();
    stream.next().await.unwrap().unwrap();
    shutdown.cancel();
    tasks.close();
    tokio::time::timeout(std::time::Duration::from_secs(5), tasks.wait())
        .await
        .expect("cleanup does not wait for the reader");
    assert_eq!(slots.available_permits(), 1);
    let mut entries = 1;
    loop {
        match stream
            .next()
            .await
            .expect("interrupted listing must not end successfully")
        {
            Ok(_) => entries += 1,
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::Unavailable);
                assert!(!pplx_space_common_grpc::is_refused_before_start(&status));
                break;
            }
        }
    }
    assert!(entries < 64);
    assert!(stream.next().await.is_none());
}
