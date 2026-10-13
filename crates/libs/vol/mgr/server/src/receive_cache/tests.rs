use super::*;

#[tokio::test]
async fn failed_attempt_releases_budget_after_surviving_writer_stops() {
    let cache = ReceiveCache::new(2 * UNIT_BYTES, UNIT_BYTES);
    let first = cache.try_reserve().unwrap();
    let second = cache.try_reserve().unwrap();
    assert!(cache.try_reserve().is_none());
    let writer = first.0.clone();
    writer.attach(&tempfile::tempfile().unwrap());
    drop(first); // Cancellation/failure before successful publication.
    tokio::task::yield_now().await;
    assert!(
        cache.try_reserve().is_none(),
        "surviving writer still owns its prefix"
    );
    drop(writer);
    let released = tokio::time::timeout(Duration::from_secs(5), cache.0.budget.acquire())
        .await
        .unwrap()
        .unwrap();
    drop(released);
    let retry = cache
        .try_reserve()
        .expect("failed attempt's bytes are reusable");
    drop((second, retry));
    assert!(cache.shutdown(Duration::from_secs(5)).await);
    assert!(cache.try_reserve().is_none());
}

#[tokio::test(start_paused = true)]
async fn successful_retention_expires_and_reuses_its_budget() {
    let cache = ReceiveCache::new(UNIT_BYTES, UNIT_BYTES);
    let attempt = cache.try_reserve().unwrap();
    attempt.0.attach(&tempfile::tempfile().unwrap());
    attempt.0.mark_success();
    drop(attempt);
    tokio::task::yield_now().await;
    tokio::time::advance(RETENTION_TTL - Duration::from_secs(1)).await;
    assert!(cache.try_reserve().is_none());
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::time::resume();
    let released = cache.0.budget.acquire().await.unwrap();
    drop(released);
    assert!(cache.try_reserve().is_some());
    assert!(cache.shutdown(Duration::from_secs(5)).await);
}

#[tokio::test]
async fn retention_pins_the_attempt_inode_across_destination_replacement() {
    use std::os::unix::fs::MetadataExt;

    let cache = ReceiveCache::new(UNIT_BYTES, UNIT_BYTES);
    let attempt = cache.try_reserve().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("stream");
    let old = directory.path().join("previous");
    std::fs::write(&path, b"original stream").unwrap();
    attempt.0.attach(&File::open(&path).unwrap());
    std::fs::rename(&path, &old).unwrap();
    std::fs::write(&path, b"replacement stream").unwrap();
    let inode = attempt.0.file.get().unwrap().metadata().unwrap().ino();
    assert_eq!(inode, std::fs::metadata(&old).unwrap().ino());
    assert_ne!(inode, std::fs::metadata(&path).unwrap().ino());
    finish_output(&path, Some(attempt.0.clone())).await;
    assert!(cache.shutdown(Duration::from_secs(5)).await);
    assert_eq!(attempt.0.retained_bytes(), 0);
}

#[tokio::test]
async fn disabled_or_oversized_prefix_falls_back_without_waiting() {
    for (node, pull) in [
        (0, UNIT_BYTES),
        (UNIT_BYTES, 0),
        (UNIT_BYTES, 2 * UNIT_BYTES),
    ] {
        let cache = ReceiveCache::new(node, pull);
        assert!(cache.try_reserve().is_none());
        assert!(cache.shutdown(Duration::from_secs(5)).await);
    }
}

#[tokio::test]
async fn last_controller_drop_expires_a_surviving_attempt() {
    let cache = ReceiveCache::new(UNIT_BYTES, UNIT_BYTES);
    let attempt = cache.try_reserve().unwrap();
    attempt.0.attach(&tempfile::tempfile().unwrap());
    let tasks = cache.0.tasks.clone();
    drop(cache);
    tasks.close();
    tokio::time::timeout(Duration::from_secs(5), tasks.wait())
        .await
        .unwrap();
    assert_eq!(attempt.0.retained_bytes(), 0);
}
