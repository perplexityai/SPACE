//! `set_access_idle_timeout` against real Postgres and, where a live execution
//! matters, a simulated worker. Concurrent writers are real transactions held
//! open on another connection, so every interleaving here is one Postgres can
//! produce, not a mock.

use std::time::Duration;

use super::*;
use crate::e2b::{auto_lifecycle_policy, lifecycle_config_from_policy};
use crate::routes::AppState;
use axum::http::StatusCode;
use pplx_space_common_core::types::{AutoLifecyclePolicy, NodeId};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, pg_test_db, pg_test_db_with_max_connections,
};
use pplx_space_node_api_sim::SimWorkerHarness;
use pplx_space_test_env::TestResult;
use serde_json::{Value, json};
use time::OffsetDateTime;

const GENERATION: i64 = 7;

#[derive(Debug, PartialEq)]
struct StoredLifecycle {
    lifecycle: String,
    policy: Value,
    state: Value,
    next_auto_event_at: Option<i64>,
}

async fn stored(pool: &PgPool, id: SandboxId) -> TestResult<StoredLifecycle> {
    let (lifecycle, policy, state, next_auto_event_at) =
        sqlx::query_as::<_, (String, Value, Value, Option<i64>)>(sql!(
            "SELECT lifecycle, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at
             FROM sandboxes WHERE id = $1"
        ))
        .bind(id)
        .fetch_one(pool)
        .await?;
    Ok(StoredLifecycle {
        lifecycle,
        policy,
        state,
        next_auto_event_at,
    })
}

/// Seeds published scheduling state so a write is visible: the policy write
/// clears both `auto_lifecycle_state_json` and `next_auto_event_at`.
async fn seed(
    pool: &PgPool,
    lifecycle: &str,
    policy: Value,
    node: Option<NodeId>,
) -> TestResult<Sandbox> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, placement, active_node_id,
             auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, template_id,
             network_policy_generation)
         VALUES ($1, $2, $3, $4, $5, $6, $7, '{\"seeded\": true}'::jsonb,
                 extract(epoch FROM now())::bigint + 300, 'seed', 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(lifecycle)
    .bind(if node.is_some() {
        "on_node"
    } else {
        "unplaced"
    })
    .bind(node)
    .bind(policy)
    .execute(pool)
    .await?;
    Ok(fetch_sandbox(pool, id).await?.expect("seeded sandbox"))
}

fn policy_json(timeout_secs: u32, auto_pause: bool, auto_resume: bool) -> Value {
    serde_json::to_value(auto_lifecycle_policy(timeout_secs, auto_pause, auto_resume))
        .expect("policy serializes")
}

async fn register_node(pool: &PgPool, endpoint: Option<&str>) -> TestResult<NodeId> {
    let node = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered')"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    if let Some(endpoint) = endpoint {
        sqlx::query(sql!(
            "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
        ))
        .bind(node)
        .bind(endpoint)
        .execute(pool)
        .await?;
    }
    Ok(node)
}

async fn sim_node(pool: &PgPool) -> TestResult<(SimWorkerHarness, NodeId)> {
    let node = NodeId::new();
    let worker = SimWorkerHarness::builder(node.to_string())
        .with_internal_api()
        .start()
        .await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered')"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
    ))
    .bind(node)
    .bind(worker.endpoint())
    .execute(pool)
    .await?;
    Ok((worker, node))
}

/// Runs the sandbox on the worker with an access-deadline floor that only a
/// `ResetPolicy` clears, so the floor's absence proves the node was reset.
async fn run_on_worker(worker: &SimWorkerHarness, id: SandboxId) -> TestResult<i64> {
    let key = id.to_string();
    worker.state().ensure_default_running(&key).await?;
    worker
        .state()
        .set_route_identity_provenance(&key, Some(GENERATION as u64), Some(GENERATION), None)
        .await?;
    let floor = OffsetDateTime::now_utc().unix_timestamp() + 900;
    worker
        .state()
        .apply_access_deadline(&key, Some(floor), None)
        .await?;
    Ok(floor)
}

async fn publish_route(
    executor: impl sqlx::PgExecutor<'_>,
    id: SandboxId,
    node: NodeId,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities
         (sandbox_id, route_generation, current_node_id, substrate_kind,
          execution_generation, activation_attempt_id)
         VALUES ($1, $2, $3, 'space', $2, $4)"
    ))
    .bind(id)
    .bind(GENERATION)
    .bind(node)
    .bind(uuid::Uuid::now_v7())
    .execute(executor)
    .await?;
    Ok(())
}

async fn node_policy(
    worker: &SimWorkerHarness,
    id: SandboxId,
) -> TestResult<(AutoLifecyclePolicy, Option<i64>)> {
    use prost::Message;
    let record = worker.state().get(&id.to_string()).await?;
    let policy = crate::controllers::auto_lifecycle::policy_from_proto(
        &pplx_space_sbx_mgr_client::proto::AutoLifecyclePolicy::decode(
            record.access_idle_policy.as_slice(),
        )?,
    )?;
    Ok((policy, record.access_extended_until))
}

async fn reconcile_rows(pool: &PgPool, node: NodeId) -> TestResult<i64> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM node_reconcile_queue WHERE node_id = $1"
    ))
    .bind(node)
    .fetch_one(pool)
    .await?)
}

async fn set_timeout(
    controller: &SandboxController,
    pool: &PgPool,
    sandbox: &Sandbox,
    timeout_secs: u32,
) -> Result<AccessIdleTimeoutOutcome, ControllerError> {
    controller
        .set_access_idle_timeout(
            pool,
            sandbox,
            timeout_secs,
            prost_types::Timestamp {
                seconds: OffsetDateTime::now_utc().unix_timestamp(),
                nanos: 0,
            },
            tokio::time::Instant::now() + Duration::from_secs(10),
            None,
        )
        .await
}

/// Waits until a policy write is queued behind another transaction's lock on
/// the sandbox row, i.e. it read the row and reached its compare-and-set. The
/// pool needs a connection beyond the lock holder's and the writer's.
async fn wait_for_blocked_policy_write(pool: &PgPool) -> TestResult {
    for _ in 0..500 {
        let blocked: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                 WHERE datname = current_database() AND wait_event_type = 'Lock'
                   AND query LIKE '%UPDATE sandboxes SET auto_lifecycle_json%')"
        ))
        .fetch_one(pool)
        .await?;
        if blocked {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the timeout never reached its policy write");
}

fn direct_controller(pool: &PgPool) -> SandboxController {
    SandboxController::new(crate::controllers::substrate::Substrate::from_direct(
        crate::controllers::substrate::direct::DirectSubstrate::new(pool.clone()),
    ))
}

/// The keep-alive path: the policy is unchanged, so nothing is written and the
/// row lock another transaction holds does not delay it, yet the node is still
/// reset, which is what restarts its idle clock.
#[tokio::test(flavor = "current_thread")]
async fn keep_alive_ignores_the_row_lock_and_still_resets_the_node() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let (worker, node) = sim_node(&db.pool).await?;
    let state = AppState::from_pool(db.pool.clone()).await?;
    let sandbox = seed(
        &db.pool,
        "started",
        policy_json(90, true, false),
        Some(node),
    )
    .await?;
    publish_route(&db.pool, sandbox.id, node).await?;
    let floor = run_on_worker(&worker, sandbox.id).await?;
    assert_eq!(node_policy(&worker, sandbox.id).await?.1, Some(floor));
    let before = stored(&db.pool, sandbox.id).await?;

    let mut holder = db.pool.begin().await?;
    sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox.id)
        .execute(&mut *holder)
        .await?;
    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        set_timeout(&state.sandbox_controller, &db.pool, &sandbox, 90),
    )
    .await
    .expect("a keep-alive must not wait for the sandbox row lock")?;
    holder.rollback().await?;

    assert_eq!(outcome, AccessIdleTimeoutOutcome::Applied);
    let (applied, extended) = node_policy(&worker, sandbox.id).await?;
    assert_eq!(extended, None, "ResetPolicy clears the access floor");
    assert_eq!(applied.stages[0].at, 90);
    assert_eq!(stored(&db.pool, sandbox.id).await?, before);
    assert_eq!(reconcile_rows(&db.pool, node).await?, 0);
    Ok(())
}

/// A dormant sandbox with an unchanged policy needs neither a write nor a node
/// call. The held row lock proves no write: one would block past the bound.
/// No worker exists, so any node call would fail the request.
#[tokio::test(flavor = "current_thread")]
async fn dormant_keep_alive_is_stored_only_without_a_write() -> TestResult {
    let db = pg_test_db().await?;
    let controller = direct_controller(&db.pool);
    for lifecycle in ["created", "paused", "suspended"] {
        let sandbox = seed(&db.pool, lifecycle, policy_json(90, true, true), None).await?;
        let before = stored(&db.pool, sandbox.id).await?;
        let mut holder = db.pool.begin().await?;
        sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
            .bind(sandbox.id)
            .execute(&mut *holder)
            .await?;
        let outcome = tokio::time::timeout(
            Duration::from_secs(3),
            set_timeout(&controller, &db.pool, &sandbox, 90),
        )
        .await
        .unwrap_or_else(|_| panic!("{lifecycle}: an unchanged policy must not write"))?;
        holder.rollback().await?;
        assert_eq!(outcome, AccessIdleTimeoutOutcome::StoredOnly, "{lifecycle}");
        assert_eq!(stored(&db.pool, sandbox.id).await?, before, "{lifecycle}");
    }
    Ok(())
}

/// A changed timeout persists the policy, clears published scheduling state,
/// enqueues reconciliation in the same transaction, and resets the node with
/// the new policy.
#[tokio::test(flavor = "current_thread")]
async fn timeout_change_writes_enqueues_and_resets_the_node() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let (worker, node) = sim_node(&db.pool).await?;
    let state = AppState::from_pool(db.pool.clone()).await?;
    let sandbox = seed(
        &db.pool,
        "started",
        policy_json(90, true, false),
        Some(node),
    )
    .await?;
    publish_route(&db.pool, sandbox.id, node).await?;
    run_on_worker(&worker, sandbox.id).await?;

    let outcome = set_timeout(&state.sandbox_controller, &db.pool, &sandbox, 120).await?;

    assert_eq!(outcome, AccessIdleTimeoutOutcome::Applied);
    let after = stored(&db.pool, sandbox.id).await?;
    assert_eq!(after.policy, policy_json(120, true, false));
    assert_eq!(after.state, json!({}));
    assert_eq!(after.next_auto_event_at, None);
    assert_eq!(reconcile_rows(&db.pool, node).await?, 1);
    let (applied, extended) = node_policy(&worker, sandbox.id).await?;
    assert_eq!(
        applied,
        auto_lifecycle_policy(120, true, false).normalized()
    );
    assert_eq!(extended, None);
    Ok(())
}

/// A policy write that commits between the timeout's read and its
/// compare-and-set wins; the timeout re-reads and applies its timeout on top,
/// keeping the concurrent writer's auto-resume.
#[tokio::test(flavor = "current_thread")]
async fn timeout_change_retries_on_top_of_a_concurrent_policy_write() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let (worker, node) = sim_node(&db.pool).await?;
    let state = AppState::from_pool(db.pool.clone()).await?;
    let sandbox = seed(
        &db.pool,
        "started",
        policy_json(90, true, false),
        Some(node),
    )
    .await?;
    publish_route(&db.pool, sandbox.id, node).await?;
    run_on_worker(&worker, sandbox.id).await?;

    let mut patch = db.pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET auto_lifecycle_json = $2 WHERE id = $1"
    ))
    .bind(sandbox.id)
    .bind(policy_json(90, true, true))
    .execute(&mut *patch)
    .await?;
    let controller = &state.sandbox_controller;
    let timeout = set_timeout(controller, &db.pool, &sandbox, 120);
    tokio::pin!(timeout);
    tokio::select! {
        result = &mut timeout => panic!("finished before the concurrent write: {result:?}"),
        waited = wait_for_blocked_policy_write(&db.pool) => waited?,
    }
    patch.commit().await?;
    let outcome = timeout.await?;

    assert_eq!(outcome, AccessIdleTimeoutOutcome::Applied);
    assert_eq!(
        stored(&db.pool, sandbox.id).await?.policy,
        policy_json(120, true, true)
    );
    let (applied, _) = node_policy(&worker, sandbox.id).await?;
    assert_eq!(applied, auto_lifecycle_policy(120, true, true).normalized());
    Ok(())
}

/// A sandbox that turns terminal between the timeout's read and its write is
/// not found, and none of its lifecycle columns change: clearing
/// `next_auto_event_at` could cancel the event that finishes its teardown.
#[tokio::test(flavor = "current_thread")]
async fn timeout_change_never_writes_a_sandbox_that_became_terminal() -> TestResult {
    let db = pg_test_db_with_max_connections(4).await?;
    let controller = direct_controller(&db.pool);
    for terminal in ["stopping", "deleting"] {
        let sandbox = seed(&db.pool, "suspended", policy_json(90, true, false), None).await?;
        let mut stop = db.pool.begin().await?;
        sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
            .bind(sandbox.id)
            .bind(terminal)
            .execute(&mut *stop)
            .await?;
        let timeout = set_timeout(&controller, &db.pool, &sandbox, 120);
        tokio::pin!(timeout);
        tokio::select! {
            result = &mut timeout => panic!("{terminal}: finished before the stop: {result:?}"),
            waited = wait_for_blocked_policy_write(&db.pool) => waited?,
        }
        stop.commit().await?;
        let before = stored(&db.pool, sandbox.id).await?;

        let error = timeout.await.expect_err("a terminal sandbox is not found");

        assert_eq!(error.status(), StatusCode::NOT_FOUND, "{terminal}");
        assert_eq!(before.lifecycle, terminal);
        assert_eq!(stored(&db.pool, sandbox.id).await?, before, "{terminal}");
    }
    Ok(())
}

/// The compare-and-set compares the stored document, not a re-serialization of
/// its parsed policy, so a stored shape the policy type would not produce
/// still changes on the first attempt instead of exhausting its retries.
#[tokio::test(flavor = "current_thread")]
async fn timeout_change_matches_a_non_canonical_stored_policy() -> TestResult {
    let db = pg_test_db().await?;
    let controller = direct_controller(&db.pool);
    let mut legacy = policy_json(90, true, true);
    legacy["legacy_field"] = json!(1);
    legacy["unused"] = Value::Null;
    legacy["wake"]["on"] = json!(["access", "access"]);
    let sandbox = seed(&db.pool, "suspended", legacy.clone(), None).await?;
    let parsed: AutoLifecyclePolicy = serde_json::from_value(legacy)?;
    assert_ne!(
        serde_json::to_value(parsed.normalized())?,
        stored(&db.pool, sandbox.id).await?.policy,
        "the seed must not round-trip"
    );

    let outcome = set_timeout(&controller, &db.pool, &sandbox, 120).await?;

    assert_eq!(outcome, AccessIdleTimeoutOutcome::StoredOnly);
    assert_eq!(
        stored(&db.pool, sandbox.id).await?.policy,
        policy_json(120, true, true)
    );
    Ok(())
}

/// Write, then read. An activation that commits its execution claim before the
/// timeout's write is seen by the read after it, so the new execution gets the
/// new policy; removing or hoisting that read returns `StoredOnly` here. Once
/// the timeout's write commits, the activation's own policy read returns it.
#[tokio::test(flavor = "current_thread")]
async fn timeout_change_and_activation_claim_see_each_other() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let (worker, node) = sim_node(&db.pool).await?;
    let state = AppState::from_pool(db.pool.clone()).await?;

    let sandbox = seed(&db.pool, "suspended", policy_json(90, true, true), None).await?;
    run_on_worker(&worker, sandbox.id).await?;
    let mut claim = db.pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started', placement = 'on_node', active_node_id = $2
         WHERE id = $1"
    ))
    .bind(sandbox.id)
    .bind(node)
    .execute(&mut *claim)
    .await?;
    publish_route(&mut *claim, sandbox.id, node).await?;
    let timeout = set_timeout(&state.sandbox_controller, &db.pool, &sandbox, 120);
    tokio::pin!(timeout);
    tokio::select! {
        result = &mut timeout => panic!("finished before the claim: {result:?}"),
        waited = wait_for_blocked_policy_write(&db.pool) => waited?,
    }
    claim.commit().await?;
    assert_eq!(timeout.await?, AccessIdleTimeoutOutcome::Applied);
    let (applied, extended) = node_policy(&worker, sandbox.id).await?;
    assert_eq!(applied, auto_lifecycle_policy(120, true, true).normalized());
    assert_eq!(extended, None);

    let dormant = seed(&db.pool, "suspended", policy_json(90, true, true), None).await?;
    assert_eq!(
        set_timeout(&state.sandbox_controller, &db.pool, &dormant, 120).await?,
        AccessIdleTimeoutOutcome::StoredOnly
    );
    let activation_policy = sandbox_store::auto_lifecycle_policy(&db.pool, dormant.id)
        .await?
        .expect("sandbox exists");
    assert_eq!(
        lifecycle_config_from_policy(&activation_policy).map(|config| config.timeout_secs),
        Some(120)
    );
    Ok(())
}

/// Every compare-and-set losing ends in the retryable policy-changed error
/// after a bounded number of attempts, with nothing written or enqueued. The
/// trigger skips the row's update, which is what a lost compare-and-set sees;
/// a sequence counts attempts because each lost attempt rolls back.
#[tokio::test(flavor = "current_thread")]
async fn exhausted_timeout_writes_fail_retryably_without_writing() -> TestResult {
    let db = pg_test_db().await?;
    let controller = direct_controller(&db.pool);
    let node = register_node(&db.pool, None).await?;
    let sandbox = seed(
        &db.pool,
        "suspended",
        policy_json(90, true, false),
        Some(node),
    )
    .await?;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE SEQUENCE timeout_write_attempts;
         CREATE FUNCTION lose_timeout_write() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.auto_lifecycle_json IS DISTINCT FROM OLD.auto_lifecycle_json THEN
                 PERFORM nextval('timeout_write_attempts');
                 RETURN NULL;
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER lose_timeout_write BEFORE UPDATE ON sandboxes
             FOR EACH ROW WHEN (OLD.id = '{}') EXECUTE FUNCTION lose_timeout_write();",
        sandbox.id
    )))
    .execute(&db.pool)
    .await?;
    let before = stored(&db.pool, sandbox.id).await?;

    let error = set_timeout(&controller, &db.pool, &sandbox, 120)
        .await
        .expect_err("every write lost");

    assert_eq!(error.status(), StatusCode::PRECONDITION_FAILED);
    let attempts: i64 = sqlx::query_scalar(sql!(
        "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM timeout_write_attempts"
    ))
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(attempts, TIMEOUT_POLICY_WRITE_ATTEMPTS as i64);
    assert_eq!(stored(&db.pool, sandbox.id).await?, before);
    assert_eq!(reconcile_rows(&db.pool, node).await?, 0);
    Ok(())
}

/// The write's SQL lifecycle guard and the controller's terminal
/// classification are two spellings of one rule; this pins them together.
#[tokio::test(flavor = "current_thread")]
async fn timeout_write_guard_matches_the_terminal_classification() -> TestResult {
    let db = pg_test_db().await?;
    let next = auto_lifecycle_policy(120, true, false);
    for lifecycle in SandboxLifecycle::all() {
        let sandbox = seed(
            &db.pool,
            lifecycle.as_str(),
            policy_json(90, true, false),
            None,
        )
        .await?;
        let execution = sandbox_store::auto_lifecycle_execution(&db.pool, sandbox.id)
            .await?
            .expect("sandbox exists");
        let written = sandbox_store::replace_auto_lifecycle_policy(
            &db.pool,
            sandbox.id,
            &execution.policy_raw,
            &next,
        )
        .await?;
        assert_eq!(
            written,
            timeout_needs_runtime(&execution).is_ok(),
            "{lifecycle:?}"
        );
    }
    Ok(())
}
