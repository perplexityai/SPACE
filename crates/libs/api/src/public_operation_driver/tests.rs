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

/// Each outcome is read from the report of the recording that produces it;
/// the match fails the build until a new outcome has its report here.
#[test]
fn an_execution_outcome_is_read_from_the_report_that_records_it() {
    let outcomes = [
        ExecutionOutcome::Succeeded,
        ExecutionOutcome::Failed,
        ExecutionOutcome::Requeued,
        ExecutionOutcome::Canceled,
        ExecutionOutcome::Lost,
        ExecutionOutcome::Error,
    ];
    for outcome in outcomes {
        let report: Result<TickReport, ()> = match outcome {
            ExecutionOutcome::Succeeded => Ok(TickReport {
                succeeded: 1,
                ..TickReport::default()
            }),
            ExecutionOutcome::Failed => Ok(TickReport {
                failed: 1,
                ..TickReport::default()
            }),
            ExecutionOutcome::Requeued => Ok(TickReport {
                retried: 1,
                ..TickReport::default()
            }),
            ExecutionOutcome::Canceled => Ok(TickReport {
                canceled: 1,
                ..TickReport::default()
            }),
            // The result was not recorded: the claim was taken meanwhile.
            ExecutionOutcome::Lost => Ok(TickReport {
                stale_claims: 1,
                ..TickReport::default()
            }),
            ExecutionOutcome::Error => Err(()),
        };
        assert_eq!(execution_outcome(&report), outcome, "{report:?}");
    }
    // A renewal that finds the claim taken returns before recording anything.
    assert_eq!(
        execution_outcome::<()>(&Ok(TickReport::default())),
        ExecutionOutcome::Lost
    );
}
