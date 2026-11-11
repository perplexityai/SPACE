use pplx_space_common_core::types::OrgId;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, TestDbResult, pg_test_db};
use pplx_space_public_api::OperationTargetType;

use super::*;
use crate::public_operations::{
    ClaimLimits, ClaimPublicOperations, CreatePublicOperation, CreatePublicOperationResult,
};

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

/// A claimed operation of `kind`, on a sandbox no other operation targets.
async fn claimed(
    store: &PublicOperationStore,
    kind: OperationKind,
    sandbox: &str,
) -> TestDbResult<(String, ClaimedPublicOperation)> {
    let created = store
        .create_operation(operation_input(kind, sandbox))
        .await?;
    let CreatePublicOperationResult::Created(operation_id) = created else {
        return Err(format!("{kind:?} was not admitted: {created:?}").into());
    };
    let mut claim = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(1),
            claim_stale_after: DEFAULT_CLAIM_STALE_AFTER,
            claimable_kinds: &[kind],
        })
        .await?;
    let operation = claim.operations.pop().ok_or("the operation is claimable")?;
    assert_eq!(operation.id, operation_id);
    Ok((claim.claim_token, operation))
}

fn operation_input(kind: OperationKind, sandbox: &str) -> CreatePublicOperation<'_> {
    CreatePublicOperation {
        org_id: OrgId::from_uuid(SEED_ORG_ID),
        actor_user_id: None,
        api_product: ApiProduct::Native,
        kind,
        target_type: Some(OperationTargetType::Sandbox),
        target_id: Some(sandbox),
        payload_json: serde_json::json!({}),
        idempotency: None,
        traceparent: None,
    }
}

fn executed(state: OperationState) -> std::future::Ready<OperationExecutionResult> {
    std::future::ready(OperationExecutionResult {
        state,
        progress_message: String::new(),
        result_json: None,
        terminal_error_code: None,
        terminal_error_json: None,
        retry_after: None,
        requeue_reason: PublicOperationRequeueReason::TransientFailure,
    })
}

/// Each exit of an execution, run against the store, returns the outcome it
/// recorded beside its report. The match fails the build until a new outcome
/// has its exits here.
#[tokio::test]
async fn each_exit_of_an_execution_returns_how_it_ended() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let config = OperationExecutionConfig::default();
    let run = |store: &PublicOperationStore,
               (claim_token, operation): (String, ClaimedPublicOperation),
               config: OperationExecutionConfig,
               execution: std::future::Ready<OperationExecutionResult>| {
        execute_and_record(
            store.clone(),
            claim_token,
            config,
            operation,
            OperationExecutor::Dispatcher,
            execution,
        )
    };
    let wake = OperationKind::WakeSandbox;
    let outcomes = [
        ExecutionOutcome::Succeeded,
        ExecutionOutcome::Failed,
        ExecutionOutcome::Requeued,
        ExecutionOutcome::Canceled,
        ExecutionOutcome::Lost,
        ExecutionOutcome::Error,
    ];
    for outcome in outcomes {
        let exits = match outcome {
            ExecutionOutcome::Succeeded => {
                let claim = claimed(&store, wake, "sbx_succeeds").await?;
                let ended = run(&store, claim, config, executed(OperationState::Succeeded));
                vec![(
                    "completed",
                    ended.await?,
                    TickReport {
                        succeeded: 1,
                        ..TickReport::default()
                    },
                )]
            }
            ExecutionOutcome::Failed => {
                let failed = TickReport {
                    failed: 1,
                    ..TickReport::default()
                };
                let claim = claimed(&store, wake, "sbx_fails").await?;
                let reported = run(&store, claim, config, executed(OperationState::Failed));
                let claim = claimed(&store, wake, "sbx_exhausts_its_attempts").await?;
                let exhausted = OperationExecutionConfig {
                    max_retry_attempts: 1,
                    ..config
                };
                let exhausted = run(&store, claim, exhausted, executed(OperationState::Queued));
                let claim = claimed(&store, wake, "sbx_passes_its_deadline").await?;
                let overdue = OperationExecutionConfig {
                    m1_max_age: Some(Duration::ZERO),
                    ..config
                };
                let overdue = run(&store, claim, overdue, executed(OperationState::Queued));
                vec![
                    ("reported", reported.await?, failed.clone()),
                    ("retry budget exhausted", exhausted.await?, failed.clone()),
                    ("deadline exceeded", overdue.await?, failed),
                ]
            }
            ExecutionOutcome::Requeued => {
                let claim = claimed(&store, wake, "sbx_retries").await?;
                let ended = run(&store, claim, config, executed(OperationState::Queued));
                vec![(
                    "released",
                    ended.await?,
                    TickReport {
                        retried: 1,
                        ..TickReport::default()
                    },
                )]
            }
            ExecutionOutcome::Canceled => {
                let sandbox = "sbx_deleted_while_created";
                let claim = claimed(&store, OperationKind::CreateSandbox, sandbox).await?;
                store
                    .create_operation(operation_input(OperationKind::DeleteSandbox, sandbox))
                    .await?;
                let ended = run(&store, claim, config, executed(OperationState::Queued));
                vec![(
                    "superseded by a delete",
                    ended.await?,
                    TickReport {
                        canceled: 1,
                        ..TickReport::default()
                    },
                )]
            }
            ExecutionOutcome::Lost => {
                let mut exits = Vec::new();
                // Another claimant holds the operation when its result is
                // recorded, whichever result it is.
                for state in [
                    OperationState::Succeeded,
                    OperationState::Failed,
                    OperationState::Queued,
                ] {
                    let (_, operation) =
                        claimed(&store, wake, &format!("sbx_lost_{state}")).await?;
                    let taken = ("taken".to_owned(), operation);
                    exits.push((
                        "result not recorded",
                        run(&store, taken, config, executed(state)).await?,
                        TickReport {
                            stale_claims: 1,
                            ..TickReport::default()
                        },
                    ));
                }
                // Or when the claim is renewed, before the execution ends.
                let (_, operation) = claimed(&store, wake, "sbx_lost_on_renewal").await?;
                let ended = execute_and_record(
                    store.clone(),
                    "taken".to_owned(),
                    OperationExecutionConfig {
                        claim_stale_after: Duration::from_millis(30),
                        ..config
                    },
                    operation,
                    OperationExecutor::Dispatcher,
                    std::future::pending(),
                );
                exits.push(("renewal refused", ended.await?, TickReport::default()));
                exits
            }
            ExecutionOutcome::Error => {
                // Nothing listens on port 1, so recording the result fails.
                let unreachable = PublicOperationStore::new(
                    sqlx::postgres::PgPoolOptions::new()
                        .acquire_timeout(Duration::from_millis(200))
                        .connect_lazy("postgres://space@127.0.0.1:1/space")?,
                );
                let claim = claimed(&store, wake, "sbx_unrecorded").await?;
                let ended = run(
                    &unreachable,
                    claim,
                    config,
                    executed(OperationState::Succeeded),
                );
                let ended = ended.await;
                assert!(ended.is_err(), "the store error ends it: {ended:?}");
                Vec::new()
            }
        };
        for (exit, (report, ended), expected) in exits {
            assert_eq!(ended, outcome, "{exit}");
            assert_eq!(report, expected, "{exit}");
        }
    }
    Ok(())
}
