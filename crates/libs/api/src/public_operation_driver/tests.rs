use super::*;

// This crate's tests build with `test-hooks` on, so the staleness and deadline
// overrides read here as they do in a Kind build.
#[test]
fn execution_config_reads_the_dispatchers_settings() {
    let config =
        |env: &pplx_space_test_env::TestEnv| OperationExecutionConfig::from_environment(env);
    let mut env = pplx_space_test_env::TestEnv::default();
    assert_eq!(config(&env), OperationExecutionConfig::default());

    env.set(CLAIM_STALE_SECS_ENV, "9");
    env.set(M1_MAX_AGE_ENV, "45");
    env.set(RETRY_DEFERRAL_ENABLED_ENV, "false");
    assert_eq!(
        config(&env),
        OperationExecutionConfig {
            claim_stale_after: Duration::from_secs(9),
            m1_max_age: Some(Duration::from_secs(45)),
            defer_retry_waits: false,
            ..OperationExecutionConfig::default()
        }
    );

    // Zero turns the deadline off; it never makes a claim stale at once.
    env.set(CLAIM_STALE_SECS_ENV, "0");
    env.set(M1_MAX_AGE_ENV, "0");
    assert_eq!(config(&env).claim_stale_after, DEFAULT_CLAIM_STALE_AFTER);
    assert_eq!(config(&env).m1_max_age, None);

    for malformed in ["", "soon", "-1"] {
        env.set(CLAIM_STALE_SECS_ENV, malformed);
        env.set(M1_MAX_AGE_ENV, malformed);
        env.set(RETRY_DEFERRAL_ENABLED_ENV, malformed);
        assert_eq!(
            config(&env),
            OperationExecutionConfig::default(),
            "{malformed:?}"
        );
    }
}
