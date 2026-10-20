use super::*;

#[tokio::test]
async fn deferral_is_scoped_to_the_wrapped_future() {
    assert!(!retry_waits_deferred());
    assert!(defer_retry_waits(async { retry_waits_deferred() }).await);
    assert!(!retry_waits_deferred());
    assert!(scope(true, async { retry_waits_deferred() }).await);
    assert!(!scope(false, async { retry_waits_deferred() }).await);
}
