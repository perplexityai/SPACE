use super::*;

#[test]
fn only_failures_delay_the_same_digest_and_restart_allows_retry() {
    let now = Instant::now();
    let mut backoff = RetryBackoff::default();
    for _ in 0..3 {
        assert!(backoff.allows("template", "digest", now));
    }
    backoff.failed("template", "digest", now);
    assert!(!backoff.allows("template", "digest", now + Duration::from_secs(299)));
    assert!(backoff.allows("template", "new-digest", now));
    assert!(backoff.allows("other-template", "digest", now));
    assert!(RetryBackoff::default().allows("template", "digest", now));
    assert!(backoff.allows("template", "digest", now + RETRY_DELAY));
    assert!(backoff.failures.is_empty());
}

#[test]
fn failure_cache_is_bounded_and_prunes_expired_entries() {
    let now = Instant::now();
    let mut backoff = RetryBackoff::default();
    for i in 0..MAX_FAILURES + 1 {
        backoff.failed(
            "template",
            &i.to_string(),
            now + Duration::from_millis(i as u64),
        );
    }
    assert_eq!(backoff.failures.len(), MAX_FAILURES);
    assert!(backoff.allows("template", "0", now));
    assert!(!backoff.allows("template", &MAX_FAILURES.to_string(), now));
    assert!(backoff.allows(
        "template",
        "any",
        now + RETRY_DELAY + Duration::from_secs(2)
    ));
    assert!(backoff.failures.is_empty());
}
