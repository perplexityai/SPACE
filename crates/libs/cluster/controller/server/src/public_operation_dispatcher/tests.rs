use pplx_space_common_core::types::OrgId;
use pplx_space_common_core::types::UserId;

use super::*;
use pplx_space_api::public_sandbox_operations::CreateSandboxOperationPayload;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{RuntimeKind, TemplateId, TemplateReference};
use pplx_space_common_svc::space_api_route_ed25519::{
    SPACE_API_ROUTE_ED25519_ISSUER_ENV, SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV,
    SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV, SpaceApiRouteEd25519Authority,
};
use pplx_space_public_api::{
    OperationId, OperationTargetType, PublicOperationEventType, PublicSandboxId, SandboxResources,
};
use pplx_space_test_env::TestResult;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use uuid::Uuid;

/// Tight retry/claim limits (claim, retry, and execute exactly one
/// operation at a time) used by the executor-outcome tests below, which
/// each assert on a single claimed operation's terminal state.
fn single_claim_config() -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        claim_limit: 1,
        execution_concurrency: 1,
        max_retry_attempts: 1,
        ..ReconcilerConfig::default()
    }
}

#[test]
fn config_defaults_enabled() {
    let config = ReconcilerConfig::default();
    assert!(config.enabled);
    assert_eq!(config.tick, DEFAULT_TICK);
    assert_eq!(config.claim_limit, DEFAULT_CLAIM_LIMIT);
    assert_eq!(config.execution_concurrency, DEFAULT_EXECUTION_CONCURRENCY);
    assert_eq!(config.claim_stale_after, DEFAULT_CLAIM_STALE_AFTER);
    assert_eq!(config.retry_after, DEFAULT_RETRY_AFTER);
    assert_eq!(config.max_retry_attempts, DEFAULT_MAX_RETRY_ATTEMPTS);
    assert_eq!(config.m1_max_age, Some(DEFAULT_M1_MAX_AGE));
}

#[test]
fn tick_defaults_to_25ms_and_the_test_override_takes_whole_seconds() {
    const TICK: &str = "SPACE_PUBLIC_OPERATION_DISPATCHER_TICK_SECS";
    let tick = |env: &pplx_space_test_env::TestEnv| ReconcilerConfig::from_environment(env).tick;
    let mut env = pplx_space_test_env::TestEnv::default();
    assert_eq!(tick(&env), Duration::from_millis(25));
    env.set(TICK, "2");
    assert_eq!(tick(&env), Duration::from_secs(2));
    for invalid in ["0", "abc", ""] {
        env.set(TICK, invalid);
        assert_eq!(tick(&env), DEFAULT_TICK, "{invalid:?}");
    }
}

#[test]
fn m1_max_age_env_defaults_and_zero_disables() {
    let mut env = pplx_space_test_env::TestEnv::default();
    assert_eq!(
        ReconcilerConfig::from_environment(&env).m1_max_age,
        Some(Duration::from_secs(1_800))
    );
    env.set(M1_MAX_AGE_ENV, "0");
    assert_eq!(ReconcilerConfig::from_environment(&env).m1_max_age, None);
}

// This crate's tests build with `test-hooks` on (a dev-dependency feature), so
// they can't tell a real environment read from a test-hook read. The source
// guard `scripts/source_guards/env_test_hook_boundary.py` fails CI if this knob
// is read through a `test_*` reader again.
#[test]
fn execution_concurrency_parses_and_clamps_from_the_environment() {
    const CONCURRENCY: &str = "SPACE_PUBLIC_OPERATION_DISPATCHER_EXECUTION_CONCURRENCY";
    let concurrency = |env: &pplx_space_test_env::TestEnv| {
        ReconcilerConfig::from_environment(env).execution_concurrency
    };
    let mut env = pplx_space_test_env::TestEnv::default();
    assert_eq!(concurrency(&env), DEFAULT_EXECUTION_CONCURRENCY);

    env.set(CONCURRENCY, "40");
    assert_eq!(concurrency(&env), 40);

    // An over-ceiling value clamps rather than falling back to the default,
    // including one too large for `usize`.
    for over in ["500", "99999999999999999999"] {
        env.set(CONCURRENCY, over);
        assert_eq!(concurrency(&env), MAX_EXECUTION_CONCURRENCY, "{over:?}");
    }

    for invalid in ["0", "-5", "sixteen", "", "99999999999999999999x"] {
        env.set(CONCURRENCY, invalid);
        assert_eq!(
            concurrency(&env),
            DEFAULT_EXECUTION_CONCURRENCY,
            "{invalid:?}"
        );
    }
}

#[test]
fn tick_report_merge_sums_outcomes_without_claim_double_counting() {
    let mut report = TickReport {
        claimed: 3,
        ..TickReport::default()
    };
    report.merge(TickReport {
        claimed: 0,
        succeeded: 1,
        failed: 2,
        retried: 3,
        stale_claims: 4,
    });

    assert_eq!(
        report,
        TickReport {
            claimed: 3,
            succeeded: 1,
            failed: 2,
            retried: 3,
            stale_claims: 4,
        }
    );
}

#[test]
fn native_executor_claims_create_sandbox() {
    assert!(NATIVE_PUBLIC_OPERATION_KINDS.contains(&OperationKind::CreateSandbox));
    assert!(NATIVE_PUBLIC_OPERATION_KINDS.contains(&OperationKind::UpdateSandbox));
    assert!(NATIVE_PUBLIC_OPERATION_KINDS.contains(&OperationKind::PauseSandbox));
    assert!(NATIVE_PUBLIC_OPERATION_KINDS.contains(&OperationKind::DeleteSandbox));
    assert!(NATIVE_PUBLIC_OPERATION_KINDS.contains(&OperationKind::KillSandbox));
    assert!(!NATIVE_PUBLIC_OPERATION_KINDS.contains(&OperationKind::CreateSession));
}

#[tokio::test]
async fn tick_fails_operation_after_retry_budget() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let operation_id = match store
        .create_operation(pplx_space_api::public_operations::CreatePublicOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: None,
            api_product: pplx_space_api::public_operations::ApiProduct::Native,
            kind: OperationKind::WakeSandbox,
            target_type: Some(pplx_space_public_api::OperationTargetType::Sandbox),
            target_id: Some("sbx_retry_budget"),
            payload_json: serde_json::json!({}),
            idempotency: None,
            traceparent: None,
        })
        .await?
    {
        pplx_space_api::public_operations::CreatePublicOperationResult::Created(id) => id,
        other => panic!("expected created operation, got {other:?}"),
    };
    let config = single_claim_config();

    let report = tick(&config, &store, &AlwaysRetryExecutor).await?;

    assert_eq!(
        report,
        TickReport {
            claimed: 1,
            failed: 1,
            ..TickReport::default()
        }
    );
    let operation = store
        .fetch_operation(OrgId::from_uuid(org_id), operation_id)
        .await?
        .expect("operation remains visible");
    assert_eq!(operation.state, OperationState::Failed);
    assert_eq!(
        operation.terminal_error_code,
        Some(PublicErrorCode::Internal)
    );
    Ok(())
}

#[tokio::test]
async fn tick_does_not_exhaust_backpressure_retries() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let operation_id = match store
        .create_operation(pplx_space_api::public_operations::CreatePublicOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: None,
            api_product: pplx_space_api::public_operations::ApiProduct::Native,
            kind: OperationKind::WakeSandbox,
            target_type: Some(pplx_space_public_api::OperationTargetType::Sandbox),
            target_id: Some("sbx_backpressure"),
            payload_json: serde_json::json!({}),
            idempotency: None,
            traceparent: None,
        })
        .await?
    {
        pplx_space_api::public_operations::CreatePublicOperationResult::Created(id) => id,
        other => panic!("expected created operation, got {other:?}"),
    };
    // Retry budget of 1 would fail a normal retry, but a backpressure
    // re-queue (cluster admission 429) must wait behind the limiter rather
    // than terminally fail.
    let config = single_claim_config();

    let report = tick(&config, &store, &AlwaysBackpressureExecutor).await?;

    assert_eq!(
        report,
        TickReport {
            claimed: 1,
            retried: 1,
            ..TickReport::default()
        }
    );
    let operation = store
        .fetch_operation(OrgId::from_uuid(org_id), operation_id)
        .await?
        .expect("operation remains visible");
    assert_eq!(operation.state, OperationState::Queued);
    let attempts: i32 = sqlx::query_scalar(sql!(
        "SELECT attempts FROM public_operations WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(attempts, 0);
    Ok(())
}

#[tokio::test]
async fn delete_execution_renews_claim_while_a_phase_is_blocked() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let operation_id = create_queued_operation(
        &store,
        OrgId::from_uuid(org_id),
        ApiProduct::Native,
        OperationKind::DeleteSandbox,
        "sbx_slow_lease",
    )
    .await?;
    let executor = BlockingExecutor::default();
    let started = Arc::clone(&executor.started);
    let release = Arc::clone(&executor.release);
    let config = ReconcilerConfig {
        claim_stale_after: Duration::from_secs(1),
        ..single_claim_config()
    };
    let tick_task = tokio::spawn({
        let store = store.clone();
        let executor = executor.clone();
        async move { tick(&config, &store, &executor).await }
    });
    started.notified().await;
    sqlx::query(sql!(
        "UPDATE public_operations
            SET claimed_at = NOW() - INTERVAL '2 seconds'
          WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .execute(&db.pool)
    .await?;

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let fresh: bool = sqlx::query_scalar(sql!(
                "SELECT claimed_at > NOW() - INTERVAL '1 second'
                   FROM public_operations
                  WHERE operation_id = $1"
            ))
            .bind(operation_id.as_uuid())
            .fetch_one(&db.pool)
            .await
            .expect("claim freshness query succeeds");
            if fresh {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;

    let competing = store
        .claim_operations(ClaimPublicOperations {
            limit: 1,
            claim_stale_after: Duration::from_secs(1),
            claimable_kinds: &[OperationKind::DeleteSandbox],
        })
        .await?;
    assert!(competing.operations.is_empty());
    release.notify_one();
    let report = tick_task.await??;
    assert_eq!(report.succeeded, 1);
    let finalized: String = sqlx::query_scalar(sql!(
        "SELECT state FROM public_operations WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(finalized, OperationState::Succeeded.as_str());
    Ok(())
}

fn fast_loop_config(execution_concurrency: usize) -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        tick: Duration::from_millis(20),
        execution_concurrency,
        ..ReconcilerConfig::default()
    }
}

async fn operation_state(pool: &PgPool, operation_id: OperationId) -> Result<String, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT state FROM public_operations WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(pool)
    .await
}

async fn wait_for_state(
    pool: &PgPool,
    operation_id: OperationId,
    expected: OperationState,
) -> TestResult {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if operation_state(pool, operation_id).await? == expected.as_str() {
                return Ok::<(), sqlx::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn run_slow_execution_does_not_block_newly_queued_operation() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let slow = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::DeleteSandbox,
        TargetBlockingExecutor::BLOCKED_TARGET,
    )
    .await?;
    let executor = TargetBlockingExecutor::default();
    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(run(
        fast_loop_config(2),
        store.clone(),
        executor.clone(),
        shutdown.clone(),
    ));
    executor.started.notified().await;

    let fast = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::DeleteSandbox,
        "sbx_fast",
    )
    .await?;
    wait_for_state(&db.pool, fast, OperationState::Succeeded).await?;
    assert_eq!(
        operation_state(&db.pool, slow).await?,
        OperationState::Running.as_str()
    );

    executor.release.notify_one();
    wait_for_state(&db.pool, slow, OperationState::Succeeded).await?;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    Ok(())
}

#[tokio::test]
async fn run_never_claims_beyond_execution_concurrency() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let slow = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::DeleteSandbox,
        TargetBlockingExecutor::BLOCKED_TARGET,
    )
    .await?;
    let executor = TargetBlockingExecutor::default();
    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(run(
        fast_loop_config(1),
        store.clone(),
        executor.clone(),
        shutdown.clone(),
    ));
    executor.started.notified().await;

    let waiting = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::DeleteSandbox,
        "sbx_waiting",
    )
    .await?;
    // Many ticks elapse while the only slot is busy; the waiting operation
    // must stay unclaimed rather than being claimed and left to go stale.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (state, attempts): (String, i32) = sqlx::query_as(sql!(
        "SELECT state, attempts FROM public_operations WHERE operation_id = $1"
    ))
    .bind(waiting.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(state, OperationState::Queued.as_str());
    assert_eq!(attempts, 0);

    executor.release.notify_one();
    wait_for_state(&db.pool, slow, OperationState::Succeeded).await?;
    wait_for_state(&db.pool, waiting, OperationState::Succeeded).await?;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    Ok(())
}

#[tokio::test]
async fn run_shutdown_drains_in_flight_executions() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let slow = create_queued_operation(
        &store,
        org_id,
        ApiProduct::Native,
        OperationKind::DeleteSandbox,
        TargetBlockingExecutor::BLOCKED_TARGET,
    )
    .await?;
    let executor = TargetBlockingExecutor::default();
    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(run(
        fast_loop_config(1),
        store.clone(),
        executor.clone(),
        shutdown.clone(),
    ));
    executor.started.notified().await;

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !dispatcher.is_finished(),
        "shutdown must wait for the in-flight execution"
    );

    executor.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    assert_eq!(
        operation_state(&db.pool, slow).await?,
        OperationState::Succeeded.as_str()
    );
    Ok(())
}

#[tokio::test]
async fn tick_does_not_exhaust_workflow_progress_requeues() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let operation_id = match store
        .create_operation(pplx_space_api::public_operations::CreatePublicOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: None,
            api_product: pplx_space_api::public_operations::ApiProduct::Native,
            kind: OperationKind::ForkSandbox,
            target_type: Some(pplx_space_public_api::OperationTargetType::Sandbox),
            target_id: Some("sbx_fork_progress"),
            payload_json: serde_json::json!({}),
            idempotency: None,
            traceparent: None,
        })
        .await?
    {
        pplx_space_api::public_operations::CreatePublicOperationResult::Created(id) => id,
        other => panic!("expected created operation, got {other:?}"),
    };
    let config = single_claim_config();

    let report = tick(&config, &store, &AlwaysWorkflowProgressExecutor).await?;

    assert_eq!(
        report,
        TickReport {
            claimed: 1,
            retried: 1,
            ..TickReport::default()
        }
    );
    let operation = store
        .fetch_operation(OrgId::from_uuid(org_id), operation_id)
        .await?
        .expect("operation remains visible");
    assert_eq!(operation.state, OperationState::Queued);
    let attempts: i32 = sqlx::query_scalar(sql!(
        "SELECT attempts FROM public_operations WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(attempts, 0);
    Ok(())
}

#[tokio::test]
async fn expired_native_m1_requeues_fail_with_evidence() -> TestResult {
    for (reason, message, m1_max_age, expected_duration) in [
        (
            PublicOperationRequeueReason::WorkflowProgress,
            "sandbox workflow is still converging",
            Duration::from_secs(900),
            "15m",
        ),
        (
            PublicOperationRequeueReason::ActivationBackpressure,
            "cluster admission rate limited",
            DEFAULT_M1_MAX_AGE,
            "30m",
        ),
        (
            PublicOperationRequeueReason::TransientFailure,
            "node call temporarily unavailable",
            DEFAULT_M1_MAX_AGE,
            "30m",
        ),
    ] {
        assert_deadline_failure(
            FixedExecutor::requeue(&[OperationKind::WakeSandbox], reason, message),
            m1_max_age,
            expected_duration,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn backdated_fork_and_admin_operations_remain_outside_m1_deadline() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let fork_id = create_queued_operation(
        &store,
        OrgId::from_uuid(org_id),
        ApiProduct::Native,
        OperationKind::ForkSandbox,
        "sbx_old_fork",
    )
    .await?;
    let admin_id = create_queued_operation(
        &store,
        OrgId::from_uuid(org_id),
        ApiProduct::Admin,
        OperationKind::WakeSandbox,
        "sbx_old_admin_wake",
    )
    .await?;
    backdate_operations(&db.pool, &[fork_id, admin_id]).await?;
    let config = ReconcilerConfig {
        claim_limit: 2,
        execution_concurrency: 2,
        max_retry_attempts: 10,
        ..single_claim_config()
    };
    let executor = FixedExecutor::requeue(
        &[OperationKind::ForkSandbox, OperationKind::WakeSandbox],
        PublicOperationRequeueReason::WorkflowProgress,
        "workflow remains in progress",
    );

    let report = tick(&config, &store, &executor).await?;

    assert_eq!(report.claimed, 2);
    assert_eq!(report.retried, 2);
    assert_eq!(report.failed, 0);
    for operation_id in [fork_id, admin_id] {
        let operation = store
            .fetch_operation(OrgId::from_uuid(org_id), operation_id)
            .await?
            .expect("operation remains visible");
        assert_eq!(operation.state, OperationState::Queued);
    }
    Ok(())
}

#[tokio::test]
async fn success_after_native_m1_age_threshold_still_wins() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let operation_id = create_queued_operation(
        &store,
        OrgId::from_uuid(org_id),
        ApiProduct::Native,
        OperationKind::WakeSandbox,
        "sbx_late_success",
    )
    .await?;
    backdate_operations(&db.pool, &[operation_id]).await?;

    let report = tick(&single_claim_config(), &store, &AlwaysSuccessExecutor).await?;

    assert_eq!(report.succeeded, 1);
    let state: String = sqlx::query_scalar(sql!(
        "SELECT state FROM public_operations WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(state, OperationState::Succeeded.as_str());
    Ok(())
}

#[tokio::test]
async fn native_executor_requeues_create_when_cluster_admission_rejects() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let user_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    seed_template_release(&db.pool).await?;
    // No start-eligible node is seeded, so the cluster admission gate
    // rejects with the empty-cluster reason before any placement work.
    // That rejection is store-independent, so it exercises the requeue
    // plumbing without needing Redis; the token-bucket rate-limit path is
    // covered by the pplx-space-api cluster_admission tests.

    let sandbox_id = PublicSandboxId::new();
    let operation = ClaimedPublicOperation {
        id: OperationId::new(),
        org_id: OrgId::from_uuid(org_id),
        actor_user_id: Some(UserId::from_uuid(user_id)),
        api_product: pplx_space_api::public_operations::ApiProduct::Native,
        kind: OperationKind::CreateSandbox,
        target_type: Some(OperationTargetType::Sandbox),
        target_id: Some(sandbox_id.to_string()),
        payload: CreateSandboxOperationPayload {
            federated_create: None,
            pool_claim: None,
            sandbox_id,
            request: public_create_request(),
            node_pool: None,
        }
        .into_json(),
        attempts: 1,
        claim_generation: 1,
        traceparent: None,
    };
    // Direct is only a placeholder: admission sees zero eligible nodes, as it would
    // from a synced, empty Kubernetes node cache, and rejects before any create.
    let substrate = Substrate::Direct(Box::new(
        pplx_space_api::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone()),
    ));
    let executor = NativePublicOperationExecutor::with_cluster_admission_config(
        db.pool.clone(),
        substrate,
        ClusterSandboxAdmissionConfig {
            enabled: true,
            refill_rps: 6.0,
            burst: 60.0,
            redis_url: None,
        },
        FeatureFlagClient::disabled(),
        test_route_ed25519_authority(),
        pplx_space_common_svc::federated_identity::FederatedIdentityPolicy::default()
            .grant_audience,
        None,
    );

    let result = executor.execute("claim-token", operation).await;

    assert_eq!(result.state, OperationState::Queued);
    // Bounded, not exact: the hint is jittered so a shed population does not
    // re-phase onto one second, and `cluster_admission` owns the window. This
    // rejection carries no refill signal, so it draws from the floor upward.
    //
    // The literals are repeated rather than imported because that module's
    // window constants are private; there is no shared symbol to reference. And
    // this range is *not* a tripwire on the window — one draw inside `1..=5`
    // passes if the window narrows, or if the jitter is removed entirely. Both
    // of those are owned by
    // `cluster_admission::tests::retry_after_hint_spreads_over_every_whole_second_from_one_to_five`
    // and `::empty_cluster_rejects_activation`, which assert exact sets. What
    // this asserts is only that the dispatcher receives a paced hint in the
    // shape it expects. Note the one direction it does catch — a window widened
    // past 5s — arrives as a flake rather than a signal, since a single draw
    // exceeds the range only sometimes.
    let retry_after = result
        .retry_after
        .expect("an empty-cluster requeue is paced");
    assert!(
        (Duration::from_secs(1)..=Duration::from_secs(5)).contains(&retry_after),
        "empty-cluster requeue delay {retry_after:?} escaped the jitter window"
    );
    assert!(
        result.progress_message.contains("no eligible"),
        "unexpected requeue message: {}",
        result.progress_message
    );
    assert!(
        result.requeue_reason == PublicOperationRequeueReason::TransientFailure,
        "an empty-cluster rejection must be a bounded retry that counts against the \
         transient-error budget, not indefinite backpressure — a durable capacity outage \
         (all workers gone/cordoned) must eventually terminate, unlike a full token bucket"
    );
    Ok(())
}

#[tokio::test]
async fn native_executor_persists_exemptions_on_policy_update_and_template_reset() -> TestResult {
    use pplx_space_api::public_sandbox_operations::UpdateSandboxOperationPayload;
    use pplx_space_public_api::{SandboxNetworkPolicyPatch, UpdateSandboxRequest};

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let user_id = UserId::from_uuid(seed_user(&db.pool, org_id).await?);
    seed_template_release(&db.pool).await?;
    let template_policy = serde_json::json!({
        "version": "v1",
        "egress": {"default_action": "allow", "allow": [], "deny": []},
        "tls_interception": "on"
    });
    let override_policy = serde_json::json!({
        "version": "v1",
        "egress": {
            "default_action": "deny",
            "allow": [{"domain": "allowed.example.test"}],
            "deny": [{"domain": "blocked.example.test"}]
        },
        "tls_interception": "on"
    });
    sqlx::query(sql!(
        "UPDATE templates SET network_policy_json = $1 WHERE name = 'seed'"
    ))
    .bind(&template_policy)
    .execute(&db.pool)
    .await?;
    let exemptions =
        TlsInterceptionExemptions::from_config(r#"[{"wildcard_domain":"*.relay.example.test"}]"#)?;

    for configured in [false, true] {
        let sandbox_id = PublicSandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 template_id, network_policy_generation
             ) VALUES ($1, $2, $3, 'k8s', 'started', 'unplaced', 'seed', 1)"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(user_id)
        .bind(org_id)
        .execute(&db.pool)
        .await?;
        // Direct is only a placeholder here: this test never reaches the substrate.
        let substrate = Substrate::Direct(Box::new(
            pplx_space_api::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone()),
        ));
        let executor = NativePublicOperationExecutor::with_cluster_admission_config(
            db.pool.clone(),
            substrate,
            ClusterSandboxAdmissionConfig {
                enabled: false,
                refill_rps: 6.0,
                burst: 60.0,
                redis_url: None,
            },
            FeatureFlagClient::disabled(),
            test_route_ed25519_authority(),
            pplx_space_common_svc::federated_identity::FederatedIdentityPolicy::default()
                .grant_audience,
            configured.then(|| exemptions.clone()),
        );

        for reset in [false, true] {
            let request = UpdateSandboxRequest {
                metadata: None,
                resources: None,
                network_policy: Some(if reset {
                    SandboxNetworkPolicyPatch::InheritTemplateDefault
                } else {
                    SandboxNetworkPolicyPatch::Set {
                        policy: serde_json::from_value(override_policy.clone())?,
                    }
                }),
                auto_lifecycle: None,
            };
            let operation = ClaimedPublicOperation {
                id: OperationId::new(),
                org_id,
                actor_user_id: Some(user_id),
                api_product: ApiProduct::Native,
                kind: OperationKind::UpdateSandbox,
                target_type: Some(OperationTargetType::Sandbox),
                target_id: Some(sandbox_id.to_string()),
                payload: UpdateSandboxOperationPayload {
                    sandbox_id,
                    request,
                }
                .into_json(),
                attempts: 1,
                claim_generation: 1,
                traceparent: None,
            };
            let mut first_generation = None;
            for _ in 0..2 {
                let result = executor.execute("claim-token", operation.clone()).await;
                assert_eq!(result.state, OperationState::Succeeded, "{result:?}");
                let (persisted, generation): (Option<serde_json::Value>, i64) =
                    sqlx::query_as(sql!(
                        "SELECT network_policy_override_json, network_policy_generation
                     FROM sandboxes WHERE id = $1"
                    ))
                    .bind(sandbox_id.as_uuid())
                    .fetch_one(&db.pool)
                    .await?;
                assert_eq!(persisted.is_some(), configured || !reset);
                let effective = persisted.as_ref().unwrap_or(&template_policy);
                let mut expected = if reset {
                    template_policy.clone()
                } else {
                    override_policy.clone()
                };
                if configured {
                    expected["tls_interception_exemptions"] = serde_json::json!([
                        {"wildcard_domain": "*.relay.example.test"}
                    ]);
                }
                assert_eq!(effective, &expected);
                if let Some(first_generation) = first_generation {
                    assert_eq!(generation, first_generation, "replay must be idempotent");
                } else {
                    first_generation = Some(generation);
                }
            }
        }
    }
    Ok(())
}

async fn assert_deadline_failure(
    executor: FixedExecutor,
    m1_max_age: Duration,
    expected_duration: &str,
) -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let operation_id = create_queued_operation(
        &store,
        OrgId::from_uuid(org_id),
        ApiProduct::Native,
        OperationKind::WakeSandbox,
        "sbx_expired_m1",
    )
    .await?;
    backdate_operations(&db.pool, &[operation_id]).await?;
    let config = ReconcilerConfig {
        max_retry_attempts: 10,
        m1_max_age: Some(m1_max_age),
        ..single_claim_config()
    };

    let metrics = pplx_space_metrics::test_capture::capture_counters_guard();
    let report = tick(&config, &store, &executor).await?;
    let deadline_metric = metrics
        .finish()
        .into_iter()
        .find(|metric| metric.name == "operations.deadline_exceeded")
        .expect("deadline metric is emitted");
    assert_eq!(
        deadline_metric.tags,
        [
            "api_product:native".to_string(),
            "kind:wake_sandbox".to_string(),
            format!("last_requeue_reason:{}", executor.requeue_reason),
        ]
    );
    let stale_claim_token = executor
        .claim_token
        .lock()
        .expect("claim token lock is available")
        .clone()
        .expect("executor observed the claim token");
    assert!(
        !store
            .complete_claimed_operation(CompleteClaimedPublicOperation {
                claim_token: stale_claim_token,
                operation_id,
                progress_message: "late completion".to_string(),
                result_json: serde_json::json!({}),
            })
            .await?
    );

    assert_eq!(
        report,
        TickReport {
            claimed: 1,
            failed: 1,
            ..TickReport::default()
        }
    );
    let operation = store
        .fetch_operation(OrgId::from_uuid(org_id), operation_id)
        .await?
        .expect("operation remains visible");
    assert_eq!(operation.state, OperationState::Failed);
    assert_eq!(
        operation.terminal_error_code,
        Some(PublicErrorCode::OperationFailed)
    );
    assert_eq!(
        operation.progress_message,
        format!(
            "Operation did not reach a terminal result within {expected_duration}; \
             inspect the target state before retrying."
        )
    );
    assert!(operation.completed_at.is_some());
    let expected_error = serde_json::json!({
        "operation_id": operation_id.to_string(),
        "kind": OperationKind::WakeSandbox.as_str(),
        "reason": "operation_deadline_exceeded",
        "max_age_seconds": m1_max_age.as_secs(),
        "last_requeue_reason": executor.requeue_reason.as_str(),
        "last_progress_message": executor.progress_message,
        "attempts": 1,
    });
    assert_eq!(operation.terminal_error, Some(expected_error.clone()));
    let events = store
        .fetch_operation_events(OrgId::from_uuid(org_id), operation_id, None, 10)
        .await?
        .expect("operation events remain visible");
    let event = events.last().expect("terminal failure event is recorded");
    assert_eq!(event.event_type, PublicOperationEventType::Failed);
    assert_eq!(
        event.event["code"],
        PublicErrorCode::OperationFailed.as_str()
    );
    assert_eq!(event.event["error"], expected_error);
    Ok(())
}

async fn create_queued_operation(
    store: &PublicOperationStore,
    org_id: OrgId,
    api_product: ApiProduct,
    kind: OperationKind,
    target_id: &str,
) -> Result<OperationId, pplx_space_api::public_operations::PublicOperationStoreError> {
    match store
        .create_operation(pplx_space_api::public_operations::CreatePublicOperation {
            org_id,
            actor_user_id: None,
            api_product,
            kind,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(target_id),
            payload_json: serde_json::json!({}),
            idempotency: None,
            traceparent: None,
        })
        .await?
    {
        pplx_space_api::public_operations::CreatePublicOperationResult::Created(id) => Ok(id),
        other => panic!("expected created operation, got {other:?}"),
    }
}

async fn backdate_operations(pool: &PgPool, operation_ids: &[OperationId]) -> TestResult {
    let operation_ids = operation_ids
        .iter()
        .map(|operation_id| operation_id.as_uuid())
        .collect::<Vec<_>>();
    sqlx::query(sql!(
        "UPDATE public_operations
         SET created_at = NOW() - INTERVAL '1801 seconds'
         WHERE operation_id = ANY($1)"
    ))
    .bind(&operation_ids)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Clone)]
struct FixedExecutor {
    supported_kinds: &'static [OperationKind],
    state: OperationState,
    requeue_reason: PublicOperationRequeueReason,
    progress_message: &'static str,
    claim_token: Arc<Mutex<Option<String>>>,
}

impl FixedExecutor {
    fn requeue(
        supported_kinds: &'static [OperationKind],
        requeue_reason: PublicOperationRequeueReason,
        progress_message: &'static str,
    ) -> Self {
        Self {
            supported_kinds,
            state: OperationState::Queued,
            requeue_reason,
            progress_message,
            claim_token: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait::async_trait]
impl PublicOperationExecutor for FixedExecutor {
    fn supported_kinds(&self) -> &'static [OperationKind] {
        self.supported_kinds
    }

    async fn execute(
        &self,
        claim_token: &str,
        _operation: ClaimedPublicOperation,
    ) -> OperationExecutionResult {
        *self
            .claim_token
            .lock()
            .expect("claim token lock is available") = Some(claim_token.to_string());
        OperationExecutionResult {
            state: self.state.clone(),
            progress_message: self.progress_message.to_string(),
            result_json: None,
            terminal_error_code: None,
            terminal_error_json: None,
            retry_after: Some(Duration::from_secs(1)),
            requeue_reason: self.requeue_reason,
        }
    }
}

#[derive(Clone)]
struct AlwaysSuccessExecutor;

#[derive(Clone, Default)]
struct BlockingExecutor {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait::async_trait]
impl PublicOperationExecutor for BlockingExecutor {
    fn supported_kinds(&self) -> &'static [OperationKind] {
        &[OperationKind::DeleteSandbox]
    }

    async fn execute(
        &self,
        _claim_token: &str,
        _operation: ClaimedPublicOperation,
    ) -> OperationExecutionResult {
        self.started.notify_one();
        self.release.notified().await;
        OperationExecutionResult {
            state: OperationState::Succeeded,
            progress_message: "completed after delayed phase".to_string(),
            result_json: Some(serde_json::json!({})),
            terminal_error_code: None,
            terminal_error_json: None,
            retry_after: None,
            requeue_reason: PublicOperationRequeueReason::TransientFailure,
        }
    }
}

/// Blocks only the operation targeting [`Self::BLOCKED_TARGET`] until
/// released; every other operation succeeds immediately.
#[derive(Clone, Default)]
struct TargetBlockingExecutor {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl TargetBlockingExecutor {
    const BLOCKED_TARGET: &'static str = "sbx_slow";
}

#[async_trait::async_trait]
impl PublicOperationExecutor for TargetBlockingExecutor {
    fn supported_kinds(&self) -> &'static [OperationKind] {
        &[OperationKind::DeleteSandbox]
    }

    async fn execute(
        &self,
        _claim_token: &str,
        operation: ClaimedPublicOperation,
    ) -> OperationExecutionResult {
        if operation.target_id.as_deref() == Some(Self::BLOCKED_TARGET) {
            self.started.notify_one();
            self.release.notified().await;
        }
        OperationExecutionResult {
            state: OperationState::Succeeded,
            progress_message: "completed".to_string(),
            result_json: Some(serde_json::json!({})),
            terminal_error_code: None,
            terminal_error_json: None,
            retry_after: None,
            requeue_reason: PublicOperationRequeueReason::TransientFailure,
        }
    }
}

#[async_trait::async_trait]
impl PublicOperationExecutor for AlwaysSuccessExecutor {
    fn supported_kinds(&self) -> &'static [OperationKind] {
        &[OperationKind::WakeSandbox]
    }

    async fn execute(
        &self,
        _claim_token: &str,
        _operation: ClaimedPublicOperation,
    ) -> OperationExecutionResult {
        OperationExecutionResult {
            state: OperationState::Succeeded,
            progress_message: "sandbox is running".to_string(),
            result_json: Some(serde_json::json!({})),
            terminal_error_code: None,
            terminal_error_json: None,
            retry_after: None,
            requeue_reason: PublicOperationRequeueReason::TransientFailure,
        }
    }
}

#[derive(Clone)]
struct AlwaysRetryExecutor;

#[async_trait::async_trait]
impl PublicOperationExecutor for AlwaysRetryExecutor {
    fn supported_kinds(&self) -> &'static [OperationKind] {
        &[OperationKind::WakeSandbox]
    }

    async fn execute(
        &self,
        _claim_token: &str,
        _operation: ClaimedPublicOperation,
    ) -> OperationExecutionResult {
        OperationExecutionResult {
            state: OperationState::Queued,
            progress_message: "retry requested".to_string(),
            result_json: None,
            terminal_error_code: None,
            terminal_error_json: None,
            retry_after: Some(Duration::from_secs(1)),
            requeue_reason: PublicOperationRequeueReason::TransientFailure,
        }
    }
}

#[derive(Clone)]
struct AlwaysBackpressureExecutor;

#[async_trait::async_trait]
impl PublicOperationExecutor for AlwaysBackpressureExecutor {
    fn supported_kinds(&self) -> &'static [OperationKind] {
        &[OperationKind::WakeSandbox]
    }

    async fn execute(
        &self,
        _claim_token: &str,
        _operation: ClaimedPublicOperation,
    ) -> OperationExecutionResult {
        OperationExecutionResult {
            state: OperationState::Queued,
            progress_message: "cluster admission rate limited".to_string(),
            result_json: None,
            terminal_error_code: None,
            terminal_error_json: None,
            retry_after: Some(Duration::from_secs(1)),
            requeue_reason: PublicOperationRequeueReason::ActivationBackpressure,
        }
    }
}

#[derive(Clone)]
struct AlwaysWorkflowProgressExecutor;

#[async_trait::async_trait]
impl PublicOperationExecutor for AlwaysWorkflowProgressExecutor {
    fn supported_kinds(&self) -> &'static [OperationKind] {
        &[OperationKind::ForkSandbox]
    }

    async fn execute(
        &self,
        _claim_token: &str,
        _operation: ClaimedPublicOperation,
    ) -> OperationExecutionResult {
        OperationExecutionResult {
            state: OperationState::Queued,
            progress_message: "fork artifact upload is in progress".to_string(),
            result_json: None,
            terminal_error_code: None,
            terminal_error_json: None,
            retry_after: Some(Duration::from_secs(1)),
            requeue_reason: PublicOperationRequeueReason::WorkflowProgress,
        }
    }
}

async fn seed_org(pool: &PgPool) -> Result<uuid::Uuid, sqlx::Error> {
    let id = uuid::Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO organizations (
             id,
             name,
             slug,
             byok_enabled
         )
         VALUES ($1, $2, $3, false)"
    ))
    .bind(id)
    .bind(format!("Org {id}"))
    .bind(format!("org-{}", id.simple()))
    .execute(pool)
    .await?;
    Ok(id)
}

async fn seed_user(pool: &PgPool, org_id: OrgId) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO users (
             id,
             email,
             password_hash,
             display_name,
             org_id,
             role
         )
         VALUES ($1, $2, 'hash', 'Test User', $3, 'member')"
    ))
    .bind(id)
    .bind(format!("{id}@example.test"))
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn seed_template_release(pool: &PgPool) -> Result<(), sqlx::Error> {
    let release_id = Uuid::now_v7();
    sqlx::query(
        sql!("INSERT INTO templates (id, name, description, org_id, visibility, network_policy_generation)
         VALUES ($1, 'seed', NULL, NULL, 'generic', 1)
         ON CONFLICT (name) DO NOTHING"),
    )
    .bind(TemplateId::new().as_uuid())
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash, org_id)
         VALUES ($1, 'seed', 'fc', $2, NULL)"
    ))
    .bind(release_id)
    .bind("sha256:cluster-admission-test")
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, 'seed', 'latest')"
    ))
    .bind(release_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn public_create_request() -> pplx_space_public_api::CreateSandboxRequest {
    pplx_space_public_api::CreateSandboxRequest {
        template: "seed:latest".parse::<TemplateReference>().unwrap(),
        runtime: Some(RuntimeKind::Firecracker),
        name: Some("native-create".to_string()),
        env: BTreeMap::new(),
        metadata: BTreeMap::new(),
        resources: Some(SandboxResources {
            vcpu: Some(2),
            memory_mib: Some(1024),
            disk_mib: None,
        }),
        network_policy: None,
        auto_lifecycle: None,
        idle_timeout: None,
        idle_action: None,
    }
}

fn test_route_ed25519_authority() -> Arc<SpaceApiRouteEd25519Authority> {
    {
        let mut env = pplx_space_test_env::TestEnv::default();
        env.set("SPACE_CLUSTER_ID", "testing");
        env.set(
            SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV,
            pplx_space_common_svc::space_api_route_ed25519::test_signing_key_path()
                .to_str()
                .unwrap(),
        );
        env.set(
            SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV,
            "cluster-controller-test-key",
        );
        env.set(SPACE_API_ROUTE_ED25519_ISSUER_ENV, "cluster-api");
        Arc::new(SpaceApiRouteEd25519Authority::load_required_from_environment(&env).unwrap())
    }
}
