use super::*;

#[test]
fn jittered_delay_stays_within_bounds() {
    let base = Duration::from_millis(100);
    for _ in 0..100 {
        let delay = jittered_delay(base, 0.2);
        assert!(delay >= Duration::from_millis(80));
        assert!(delay <= Duration::from_millis(120));
    }
}

#[test]
fn default_connect_policy_has_bounded_rollout_budget() {
    assert_eq!(
        DEFAULT_GRPC_CONNECT_RETRY_POLICY.max_elapsed,
        Duration::from_secs(10)
    );
    assert_eq!(
        DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
        DEFAULT_GRPC_CONNECT_RETRY_POLICY
    );
    assert!(DEFAULT_GRPC_CONNECT_RETRY_POLICY.max_delay <= Duration::from_secs(1));
}
