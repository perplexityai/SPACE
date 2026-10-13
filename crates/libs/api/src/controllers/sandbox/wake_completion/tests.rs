use pplx_space_common_core::types::{NodeId, SandboxId};
use uuid::Uuid;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pplx_space_db_test_fixtures::try_pg_test_db;

use super::*;

use pplx_space_test_env::TestResult;

#[test]
fn guard_defuse_disarms_and_double_defuse_is_safe() {
    let mut guard = WakeCompletionGuard::unarmed(SandboxId::new(), WakeCompletionAction::Wake);
    assert!(!guard.is_armed(), "fresh guard must be unarmed");
    guard.arm(Box::pin(async {}));
    assert!(guard.is_armed());
    guard.defuse();
    assert!(!guard.is_armed());
    guard.defuse();
    assert!(!guard.is_armed());
    // Dropping a defused guard is a no-op (no metric, no spawn).
    drop(guard);
}

/// Drop must never panic outside a Tokio runtime: an armed guard dropped
/// from non-async teardown sheds (sweeper backstop) instead of crashing.
#[test]
fn dropping_an_armed_guard_without_a_runtime_sheds_instead_of_panicking() {
    let mut guard = WakeCompletionGuard::with_slots(
        SandboxId::new(),
        WakeCompletionAction::Wake,
        Arc::new(Semaphore::new(1)),
    );
    guard.arm(Box::pin(async {}));
    drop(guard);
}

/// The regression the guard exists for: an armed guard whose owner is
/// dropped (the handler future cancelled) must still run the completion.
#[tokio::test(flavor = "current_thread")]
async fn dropping_an_armed_guard_spawns_the_completion() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut guard = WakeCompletionGuard::with_slots(
        SandboxId::new(),
        WakeCompletionAction::Wake,
        Arc::new(Semaphore::new(1)),
    );
    guard.arm(Box::pin(async move {
        let _ = tx.send(());
    }));
    drop(guard);
    tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("completion must be spawned when an armed guard drops")
        .expect("completion future must run to completion");
}

/// The cancel-storm brake: with every redrive slot taken, a dropped
/// armed guard must shed (no task spawned) instead of amplifying the
/// storm; once slots free up, the next cancellation redrives again.
#[tokio::test(flavor = "current_thread")]
async fn dropping_an_armed_guard_sheds_when_the_redrive_cap_is_full() {
    // A private two-slot pool: exhausting it cannot starve unrelated
    // tests sharing the process-global semaphore.
    let slots = Arc::new(Semaphore::new(2));
    let all_slots = Arc::clone(&slots)
        .acquire_many_owned(2)
        .await
        .expect("semaphore is never closed");

    let (tx, mut rx) = tokio::sync::oneshot::channel();
    let mut guard = WakeCompletionGuard::with_slots(
        SandboxId::new(),
        WakeCompletionAction::Wake,
        Arc::clone(&slots),
    );
    guard.arm(Box::pin(async move {
        let _ = tx.send(());
    }));
    drop(guard);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        rx.try_recv().is_err(),
        "with the cap full the completion must be shed, not spawned"
    );

    drop(all_slots);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut guard = WakeCompletionGuard::with_slots(
        SandboxId::new(),
        WakeCompletionAction::Wake,
        Arc::clone(&slots),
    );
    guard.arm(Box::pin(async move {
        let _ = tx.send(());
    }));
    drop(guard);
    tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("with slots free the completion must be spawned again")
        .expect("completion future must run");
}

async fn insert_k8s_sandbox(pool: &PgPool, sandbox_id: SandboxId, lifecycle: &str) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         VALUES ($1,
                 '00000000-0000-0000-0000-000000000010',
                 '00000000-0000-0000-0000-000000000001',
                 'k8s',
                 $2,
                 1)"
    ))
    .bind(sandbox_id)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    state: &str,
    pod_ip: Option<&str>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, state, active, pod_ip)
         VALUES ($1, 1, $2, 'sandboxes', $3, TRUE, $4::INET)"
    ))
    .bind(sandbox_id)
    .bind(format!("sandbox-{sandbox_id}"))
    .bind(state)
    .bind(pod_ip)
    .execute(pool)
    .await?;
    Ok(())
}

/// Register a node and place the sandbox on it — the sandbox-scoped
/// reconcile enqueue resolves its target from `sandboxes.active_node_id`
/// and the queue rows carry a foreign key to `nodes`.
async fn place_sandbox_on_node(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.1'::INET, 'worker', 'k8s', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(node_id)
}

async fn lifecycle_of(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<String> {
    Ok(
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

/// End-to-end redrive over live Postgres: the execution starts
/// unroutable (the cancelled handler never hydrated it), the stubbed
/// hydration "repairs" it on the second poll, and the fenced CAS then
/// converges the row.
#[tokio::test(flavor = "current_thread")]
async fn redrive_waits_for_routability_then_completes() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_k8s_sandbox(pool, sandbox_id, "restoring").await?;
    insert_execution(pool, sandbox_id, "creating", None).await?;

    let attempts = Arc::new(AtomicUsize::new(0));
    let hydrate = {
        let attempts = Arc::clone(&attempts);
        let pool = pool.clone();
        move || {
            let attempts = Arc::clone(&attempts);
            let pool = pool.clone();
            async move {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    // First poll: the pod is not routable yet.
                    return Ok(false);
                }
                // Second poll: "hydration" lands the pod metadata, the
                // way hydrate_restored_execution would.
                sqlx::query(sql!(
                    "UPDATE sandbox_k8s_executions
                     SET state = 'running', pod_ip = '10.90.0.20'::INET
                     WHERE sandbox_id = $1 AND active"
                ))
                .bind(sandbox_id)
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;
                Ok(true)
            }
        }
    };

    let outcome = redrive_wake_completion(
        pool,
        sandbox_id,
        WakeCompletionAction::Wake,
        Duration::from_secs(10),
        Duration::from_millis(20),
        REDRIVE_HYDRATE_BUDGET,
        hydrate,
    )
    .await;
    assert_eq!(outcome, WakeCompletionRedriveOutcome::Completed);
    assert_eq!(lifecycle_of(pool, sandbox_id).await?, "started");
    Ok(())
}

/// The fence: hydration claims routable but the execution was replaced
/// underneath (unroutable at flip time) — the CAS must claim nothing and
/// the redrive must eventually abandon, leaving `restoring` for the
/// sweeper rather than manufacturing started-without-routability.
#[tokio::test(flavor = "current_thread")]
async fn redrive_cas_refuses_unroutable_execution_at_flip_time() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_k8s_sandbox(pool, sandbox_id, "restoring").await?;
    insert_execution(pool, sandbox_id, "creating", None).await?;

    // A hydration stub that lies (stale answer): claims routable while
    // the DB execution stays creating/no-IP.
    let outcome = redrive_wake_completion(
        pool,
        sandbox_id,
        WakeCompletionAction::Wake,
        Duration::from_millis(200),
        Duration::from_millis(20),
        REDRIVE_HYDRATE_BUDGET,
        || async { Ok(true) },
    )
    .await;
    assert_eq!(outcome, WakeCompletionRedriveOutcome::Abandoned);
    assert_eq!(
        lifecycle_of(pool, sandbox_id).await?,
        "restoring",
        "the fenced CAS must never assert started without a routable execution"
    );
    Ok(())
}

/// The CAS re-checks `restoring` at flip time, not only the loop's read: a
/// row another actor moved to `deleting` while hydration was in flight must
/// keep that lifecycle even though hydration answered routable, and the
/// redrive then observes the move and resolves.
#[tokio::test(flavor = "current_thread")]
async fn redrive_cas_refuses_a_row_that_left_restoring_during_hydration() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_k8s_sandbox(pool, sandbox_id, "restoring").await?;
    insert_execution(pool, sandbox_id, "running", Some("10.90.0.23")).await?;

    let hydrate = {
        let pool = pool.clone();
        move || {
            let pool = pool.clone();
            async move {
                sqlx::query(sql!(
                    "UPDATE sandboxes SET lifecycle = 'deleting' WHERE id = $1"
                ))
                .bind(sandbox_id)
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;
                Ok(true)
            }
        }
    };

    let outcome = redrive_wake_completion(
        pool,
        sandbox_id,
        WakeCompletionAction::Wake,
        Duration::from_secs(30),
        Duration::from_millis(20),
        REDRIVE_HYDRATE_BUDGET,
        hydrate,
    )
    .await;
    assert_eq!(outcome, WakeCompletionRedriveOutcome::AlreadyResolved);
    assert_eq!(lifecycle_of(pool, sandbox_id).await?, "deleting");
    Ok(())
}

/// Lease parity with the other `restoring -> started` writers: an
/// unexpired restore lease means a restore attempt (most likely a client
/// retry that fired after our caller hung up) is actively driving the
/// row and will finalize it itself. The redrive must not converge under
/// it — same invariant the sweeper's converge CAS and
/// `begin_restore_lifecycle` enforce — and must converge once the lease
/// expires with nothing driving the row.
#[tokio::test(flavor = "current_thread")]
async fn redrive_cas_respects_unexpired_restore_lease() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_k8s_sandbox(pool, sandbox_id, "restoring").await?;
    insert_execution(pool, sandbox_id, "running", Some("10.90.0.22")).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET restore_lease_expires_at = NOW() + INTERVAL '1 hour',
             restore_lease_token = $2
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;

    let outcome = redrive_wake_completion(
        pool,
        sandbox_id,
        WakeCompletionAction::Wake,
        Duration::from_millis(200),
        Duration::from_millis(20),
        REDRIVE_HYDRATE_BUDGET,
        || async { Ok(true) },
    )
    .await;
    assert_eq!(
        outcome,
        WakeCompletionRedriveOutcome::Abandoned,
        "an unexpired lease means another attempt owns the row; the redrive must not converge under it"
    );
    assert_eq!(lifecycle_of(pool, sandbox_id).await?, "restoring");

    // Expire the lease: nothing is driving the row any more, so the
    // redrive converges.
    sqlx::query(sql!(
        "UPDATE sandboxes SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    let outcome = redrive_wake_completion(
        pool,
        sandbox_id,
        WakeCompletionAction::Wake,
        Duration::from_secs(5),
        Duration::from_millis(20),
        REDRIVE_HYDRATE_BUDGET,
        || async { Ok(true) },
    )
    .await;
    assert_eq!(outcome, WakeCompletionRedriveOutcome::Completed);
    assert_eq!(lifecycle_of(pool, sandbox_id).await?, "started");
    Ok(())
}

/// A row another actor already finished (or moved) resolves immediately
/// without any write.
#[tokio::test(flavor = "current_thread")]
async fn redrive_backs_off_when_row_already_resolved() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_k8s_sandbox(pool, sandbox_id, "started").await?;
    insert_execution(pool, sandbox_id, "running", Some("10.90.0.21")).await?;

    let outcome = redrive_wake_completion(
        pool,
        sandbox_id,
        WakeCompletionAction::Wake,
        Duration::from_secs(5),
        Duration::from_millis(20),
        REDRIVE_HYDRATE_BUDGET,
        || async { panic!("hydration must not run for a non-restoring row") },
    )
    .await;
    assert_eq!(outcome, WakeCompletionRedriveOutcome::AlreadyResolved);
    assert_eq!(lifecycle_of(pool, sandbox_id).await?, "started");
    Ok(())
}

/// The budget must be a hard bound on the whole redrive future, not just
/// a check between awaits: a hydration (or query) that never resolves —
/// a blocked row lock, a wedged apiserver connection — must not hold a
/// redrive slot indefinitely and starve every later cancellation into
/// shedding.
///
/// The hydration grace is scaled down with the budget and the poll
/// interval: what this pins is that the bound exists and is derived from
/// the three timing inputs, not the wall-clock size of production's
/// per-attempt probe budget. The outer `timeout` is the fallback that
/// turns a missing bound into a failure rather than a hung test.
#[tokio::test(flavor = "current_thread")]
async fn redrive_hard_bounds_a_never_resolving_hydration() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_k8s_sandbox(pool, sandbox_id, "restoring").await?;
    insert_execution(pool, sandbox_id, "creating", None).await?;

    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        redrive_wake_completion(
            pool,
            sandbox_id,
            WakeCompletionAction::Wake,
            Duration::from_millis(200),
            Duration::from_millis(20),
            Duration::from_millis(200),
            || async {
                // A hydration that never resolves: the deadline check
                // between awaits can never run.
                std::future::pending::<Result<bool, String>>().await
            },
        ),
    )
    .await
    .expect("the redrive must return within its hard bound, not hang forever");
    assert_eq!(outcome, WakeCompletionRedriveOutcome::Abandoned);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "hard bound must fire near budget + one-await grace (took {:?})",
        started.elapsed()
    );
    Ok(())
}

/// Suspension resets durable open port exposures to `apply_state =
/// pending`; the normal wake completion re-enqueues their reconciliation
/// after the lifecycle CAS. The redrive must do the same when it
/// completes the row itself — otherwise a cancelled restore becomes
/// `started` with its durable ports unapplied.
#[tokio::test(flavor = "current_thread")]
async fn redrive_completion_enqueues_pending_open_port_exposures() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_k8s_sandbox(pool, sandbox_id, "restoring").await?;
    insert_execution(pool, sandbox_id, "running", Some("10.90.0.30")).await?;
    place_sandbox_on_node(pool, sandbox_id).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_port_exposures
            (sandbox_id, protocol, guest_port, visibility, source, desired_state, apply_state)
         VALUES ($1, 'tcp', 8080, 'public', 'user', 'open', 'pending')"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;

    let outcome = redrive_wake_completion(
        pool,
        sandbox_id,
        WakeCompletionAction::Wake,
        Duration::from_secs(5),
        Duration::from_millis(20),
        REDRIVE_HYDRATE_BUDGET,
        || async { Ok(true) },
    )
    .await;
    assert_eq!(outcome, WakeCompletionRedriveOutcome::Completed);
    let enqueued: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1 FROM node_reconcile_queue
             WHERE element = $1::TEXT AND facet = $2::TEXT
         )"
    ))
    .bind(sandbox_id.to_string())
    .bind(pplx_space_common_svc::types::ReconcileFacet::SandboxDesiredState.as_str())
    .fetch_one(pool)
    .await?;
    assert!(
        enqueued,
        "a completed redrive must enqueue the sandbox desired-state \
         reconciliation so pending open exposures are re-applied"
    );
    Ok(())
}

/// Same requirement when another actor (published-state convergence, a
/// client retry) lands the started flip and the redrive merely observes
/// it — the devbox-validated path.
#[tokio::test(flavor = "current_thread")]
async fn redrive_observed_started_enqueues_pending_open_port_exposures() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_k8s_sandbox(pool, sandbox_id, "started").await?;
    insert_execution(pool, sandbox_id, "running", Some("10.90.0.31")).await?;
    place_sandbox_on_node(pool, sandbox_id).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_port_exposures
            (sandbox_id, protocol, guest_port, visibility, source, desired_state, apply_state)
         VALUES ($1, 'tcp', 8080, 'public', 'user', 'open', 'pending')"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;

    let outcome = redrive_wake_completion(
        pool,
        sandbox_id,
        WakeCompletionAction::Wake,
        Duration::from_secs(5),
        Duration::from_millis(20),
        REDRIVE_HYDRATE_BUDGET,
        || async { panic!("hydration must not run for a non-restoring row") },
    )
    .await;
    assert_eq!(outcome, WakeCompletionRedriveOutcome::AlreadyResolved);
    let enqueued: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1 FROM node_reconcile_queue
             WHERE element = $1::TEXT AND facet = $2::TEXT
         )"
    ))
    .bind(sandbox_id.to_string())
    .bind(pplx_space_common_svc::types::ReconcileFacet::SandboxDesiredState.as_str())
    .fetch_one(pool)
    .await?;
    assert!(
        enqueued,
        "observing a started row must also enqueue exposure reconciliation — \
         the converging actor (e.g. published-state) does not"
    );
    Ok(())
}

/// A deleted sandbox row must terminate the redrive immediately — not
/// hold a redrive slot polling until the budget elapses. A concurrent
/// delete is another actor resolving the row, exactly like a concurrent
/// completion.
#[tokio::test(flavor = "current_thread")]
async fn redrive_terminates_immediately_when_row_is_deleted() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    // Never inserted: the row is gone (deleted mid-redrive in prod terms).
    let sandbox_id = SandboxId::new();

    let started = std::time::Instant::now();
    let outcome = redrive_wake_completion(
        pool,
        sandbox_id,
        WakeCompletionAction::Wake,
        Duration::from_secs(30),
        Duration::from_millis(20),
        REDRIVE_HYDRATE_BUDGET,
        || async { panic!("hydration must not run for a deleted row") },
    )
    .await;
    assert_eq!(
        outcome,
        WakeCompletionRedriveOutcome::AlreadyResolved,
        "a missing row is terminal — nothing left to converge"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the redrive must finish immediately on a deleted row, not sit \
         out its budget holding a slot (took {:?})",
        started.elapsed()
    );
    Ok(())
}
