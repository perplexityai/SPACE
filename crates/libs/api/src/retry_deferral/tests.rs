use super::*;

#[tokio::test]
async fn deferral_is_scoped_to_the_wrapped_future() {
    assert!(current().is_none());
    let deferral = RetryDeferral::new();
    assert!(scope(Some(deferral.clone()), async { current().is_some() }).await);
    assert!(current().is_none());
    assert!(!scope(None, async { current().is_some() }).await);
    assert!(!deferral.deferred());
}

#[tokio::test]
async fn a_deferred_wait_is_recorded_on_the_callers_handle() {
    assert!(!defer_retry_wait("test"));

    let deferral = RetryDeferral::new();
    let recorded = scope(Some(deferral.clone()), async {
        // A detached task re-enters the caller's deferral and records on
        // the same handle.
        let inherited = current();
        tokio::spawn(scope(inherited, async { defer_retry_wait("test") }))
            .await
            .expect("task completes")
    })
    .await;

    assert!(recorded);
    assert!(deferral.deferred());
}
