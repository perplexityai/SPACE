use crate::fixture_job_id;
use std::time::Duration;

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn completion_before_polling_the_receiver_is_retained() {
    // Arrange
    let waiters = CompletionWaiters::default();
    let mut waiter = waiters.register(&fixture_job_id("job-1")).unwrap();

    // Act
    waiters.notify(&fixture_job_id("job-1"));
    let notified_at = tokio::time::timeout(Duration::from_millis(10), waiter.notified()).await;

    // Assert
    assert!(notified_at.unwrap().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn completion_is_broadcast_to_every_registered_waiter() {
    // Arrange
    let waiters = CompletionWaiters::default();
    let mut first = waiters.register(&fixture_job_id("job-1")).unwrap();
    let mut second = waiters.register(&fixture_job_id("job-1")).unwrap();

    // Act
    waiters.notify(&fixture_job_id("job-1"));
    let (first, second) = tokio::join!(first.notified(), second.notified());

    // Assert
    assert!(first.is_some());
    assert!(second.is_some());
}
