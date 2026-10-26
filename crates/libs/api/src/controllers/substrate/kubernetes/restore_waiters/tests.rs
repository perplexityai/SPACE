use sqlx::postgres::PgPoolOptions;

use super::*;

fn waiters_with_capacity(capacity: usize) -> RestoreWaiters {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused@localhost/unused")
        .expect("lazy pool builds without connecting");
    RestoreWaiters::with_capacity(
        K8sStore::new(
            pool,
            "space".to_string(),
            crate::controllers::substrate::kubernetes::store::MaterializationMode::Fenced,
        ),
        CancellationToken::new(),
        TaskTracker::new(),
        capacity,
    )
}

#[tokio::test]
async fn subscribers_share_one_sandbox_entry_and_distinct_entries_are_capped() {
    let waiters = waiters_with_capacity(1);
    let sandbox_id = SandboxId::new();

    let first = waiters.register(sandbox_id).expect("first subscriber fits");
    let second = waiters
        .register(sandbox_id)
        .expect("same sandbox reuses its channel");
    let error = waiters
        .register(SandboxId::new())
        .expect_err("a distinct sandbox exceeds the cap");

    let senders = waiters.inner.senders.lock().unwrap();
    assert_eq!(senders.len(), 1);
    assert_eq!(senders.get(&sandbox_id).unwrap().receiver_count(), 2);
    drop(senders);
    drop((first, second));
    assert!(matches!(error, SubstrateError::RestoreInFlight { .. }));
}

#[tokio::test]
async fn terminal_result_reaches_every_subscriber() {
    let waiters = waiters_with_capacity(1);
    let sandbox_id = SandboxId::new();
    let mut first = waiters.register(sandbox_id).expect("first subscriber fits");
    let mut second = waiters
        .register(sandbox_id)
        .expect("same sandbox reuses its channel");
    let expected = UserRestoreObservation::Claimable;

    publish_poll_results(&waiters.inner, &[sandbox_id], |_| {
        RestorePollStatus::Observed(expected.clone())
    });

    assert_eq!(wait_for_restore_update(&mut first).await.unwrap(), expected);
    assert_eq!(
        wait_for_restore_update(&mut second).await.unwrap(),
        expected
    );
    assert!(waiters.inner.senders.lock().unwrap().is_empty());
}

#[tokio::test]
async fn poller_is_tracked_and_stops_on_shutdown() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused@localhost/unused")
        .expect("lazy pool builds without connecting");
    let shutdown_token = CancellationToken::new();
    let task_tracker = TaskTracker::new();
    let waiters = RestoreWaiters::with_capacity(
        K8sStore::new(
            pool,
            "space".to_string(),
            crate::controllers::substrate::kubernetes::store::MaterializationMode::Fenced,
        ),
        shutdown_token.clone(),
        task_tracker.clone(),
        1,
    );
    let mut receiver = waiters
        .register(SandboxId::new())
        .expect("one restore follower fits");

    waiters.ensure_poller();
    assert_eq!(task_tracker.len(), 1);

    shutdown_token.cancel();
    task_tracker.close();
    tokio::time::timeout(Duration::from_secs(1), task_tracker.wait())
        .await
        .expect("restore poller must stop inside the shutdown budget");
    assert_eq!(task_tracker.len(), 0);
    let error = wait_for_restore_update(&mut receiver)
        .await
        .expect_err("shutdown must release restore followers");
    assert!(
        matches!(error, SubstrateError::Failed { .. }),
        "shutdown should report a stopped polling loop, got {error:?}"
    );
}
