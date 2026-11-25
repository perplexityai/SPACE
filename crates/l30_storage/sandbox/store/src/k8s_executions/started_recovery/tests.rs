//! Each started-recovery write against the row it takes and the rows its
//! guard refuses, which it leaves unchanged, and the recovering-execution
//! lock's mode and scope: the execution row only, until the transaction ends.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{RecoveringActivationAttempt, claim, complete, fail, hand_off_to_successor};

const LEASE_TTL_SECS: i64 = 60;

async fn insert_sandbox(pool: &PgPool, lifecycle: &str) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, $4, $5, 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Kubernetes)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(id)
}

/// An execution last updated `age_secs` ago, holding `activation_attempt_id`
/// when one is given.
async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    state: &str,
    active: bool,
    activation_attempt_id: Option<Uuid>,
    age_secs: i64,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active,
             activation_attempt_id, activation_attempt_expires_at,
             activation_attempt_target_state, updated_at)
         VALUES ($1, $2, $3, 'sandboxes', 'uid-' || $2::TEXT, $4, $5,
                 $6, CASE WHEN $6::UUID IS NULL THEN NULL
                          ELSE NOW() + INTERVAL '1 hour' END,
                 CASE WHEN $6::UUID IS NULL THEN NULL ELSE 'started' END,
                 NOW() - make_interval(secs => $7))"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(format!("sbx-recovery-{}", Uuid::now_v7()))
    .bind(state)
    .bind(active)
    .bind(activation_attempt_id)
    .bind(age_secs)
    .execute(pool)
    .await?;
    Ok(())
}

/// Every column the writes here set, read back raw.
#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct ExecutionRow {
    state: String,
    active: bool,
    activation_attempt_id: Option<Uuid>,
    route_activation_id: Option<Uuid>,
    activation_attempt_expires_at: Option<time::OffsetDateTime>,
    activation_attempt_target_state: Option<String>,
    updated_at: time::OffsetDateTime,
}

async fn execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
) -> TestResult<ExecutionRow> {
    Ok(sqlx::query_as(sql!(
        "SELECT state, active, activation_attempt_id, route_activation_id,
                activation_attempt_expires_at, activation_attempt_target_state, updated_at
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(pool)
    .await?)
}

/// Whether another transaction's `FOR KEY SHARE NOWAIT` on the execution
/// row fails with `lock_not_available`. Only `FOR UPDATE` refuses it, so
/// this pins the lock's mode.
async fn execution_key_share_blocked(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
) -> TestResult<bool> {
    let mut probe = pool.begin().await?;
    let result = sqlx::query(sql!(
        "SELECT 1 FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2
          FOR KEY SHARE NOWAIT"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_optional(&mut *probe)
    .await;
    probe.rollback().await?;
    lock_not_available(result)
}

/// Whether another transaction's `FOR UPDATE NOWAIT` on the `sandboxes` row
/// fails with `lock_not_available`. Every row-lock mode refuses it.
async fn sandbox_row_locked(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<bool> {
    let mut probe = pool.begin().await?;
    let result = sqlx::query(sql!(
        "SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE NOWAIT"
    ))
    .bind(sandbox_id)
    .fetch_optional(&mut *probe)
    .await;
    probe.rollback().await?;
    lock_not_available(result)
}

fn lock_not_available(
    result: Result<Option<sqlx::postgres::PgRow>, sqlx::Error>,
) -> TestResult<bool> {
    match result {
        Ok(_) => Ok(false),
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03") => Ok(true),
        Err(error) => Err(error.into()),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn claim_takes_a_claimable_execution_and_leaves_the_rest() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    // A claim that created the execution takes it while it is `creating`.
    let created = insert_sandbox(pool, "started").await?;
    insert_execution(pool, created, 1, "creating", true, None, 0).await?;
    assert_eq!(
        claim(pool, created, 1, true, LEASE_TTL_SECS).await?,
        Some(1)
    );
    assert_eq!(execution(pool, created, 1).await?.state, "recovering");

    // One that found it takes a `running` execution, and a `recovering`
    // one whose lease has run out by the database clock.
    let found = insert_sandbox(pool, "started").await?;
    insert_execution(pool, found, 1, "running", true, None, 0).await?;
    assert_eq!(claim(pool, found, 1, false, LEASE_TTL_SECS).await?, Some(1));
    let stale = insert_sandbox(pool, "started").await?;
    insert_execution(pool, stale, 2, "recovering", true, None, 2 * LEASE_TTL_SECS).await?;
    assert_eq!(claim(pool, stale, 2, false, LEASE_TTL_SECS).await?, Some(2));

    // Losing cases: a recovery still inside its lease, a found execution
    // claimed as created, the wrong generation, a retired execution and a
    // stale one in a state neither branch takes.
    let fresh = insert_sandbox(pool, "started").await?;
    insert_execution(pool, fresh, 1, "recovering", true, None, 0).await?;
    let mismatched = insert_sandbox(pool, "started").await?;
    insert_execution(pool, mismatched, 1, "running", true, None, 0).await?;
    let retired = insert_sandbox(pool, "started").await?;
    insert_execution(pool, retired, 1, "running", false, None, 0).await?;
    let stale_suspended = insert_sandbox(pool, "started").await?;
    insert_execution(
        pool,
        stale_suspended,
        1,
        "suspended",
        true,
        None,
        2 * LEASE_TTL_SECS,
    )
    .await?;
    for (sandbox_id, generation, created) in [
        (fresh, 1, false),
        (mismatched, 1, true),
        (mismatched, 2, false),
        (retired, 1, false),
        (stale_suspended, 1, false),
    ] {
        let before = execution(pool, sandbox_id, 1).await?;
        assert_eq!(
            claim(pool, sandbox_id, generation, created, LEASE_TTL_SECS).await?,
            None
        );
        assert_eq!(execution(pool, sandbox_id, 1).await?, before);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn lock_recovering_execution_locks_only_the_execution_until_the_transaction_ends()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let attempt = Uuid::now_v7();
    let sandbox_id = insert_sandbox(pool, "started").await?;
    insert_execution(pool, sandbox_id, 3, "recovering", true, Some(attempt), 0).await?;

    for commit in [true, false] {
        let mut tx = pool.begin().await?;
        assert_eq!(
            super::lock_recovering_execution(&mut tx, sandbox_id, 3).await?,
            Some(RecoveringActivationAttempt {
                activation_attempt_id: Some(attempt),
                activation_attempt_target_state: Some("started".to_string()),
            })
        );
        // `FOR UPDATE OF e`: the execution row refuses `FOR KEY SHARE`, and
        // the joined `sandboxes` row stays unlocked.
        assert!(execution_key_share_blocked(pool, sandbox_id, 3).await?);
        assert!(!sandbox_row_locked(pool, sandbox_id).await?);
        if commit {
            tx.commit().await?;
        } else {
            tx.rollback().await?;
        }
        assert!(!execution_key_share_blocked(pool, sandbox_id, 3).await?);
        assert!(!sandbox_row_locked(pool, sandbox_id).await?);
    }

    // No row: another generation, an execution that isn't `recovering`, a
    // sandbox that is no longer `started`, and an inactive execution.
    let mut tx = pool.begin().await?;
    assert_eq!(
        super::lock_recovering_execution(&mut tx, sandbox_id, 2).await?,
        None
    );
    tx.rollback().await?;
    let running = insert_sandbox(pool, "started").await?;
    insert_execution(pool, running, 1, "running", true, None, 0).await?;
    let suspending = insert_sandbox(pool, "suspending").await?;
    insert_execution(pool, suspending, 1, "recovering", true, None, 0).await?;
    let inactive = insert_sandbox(pool, "started").await?;
    insert_execution(pool, inactive, 1, "recovering", false, None, 0).await?;
    for other in [running, suspending, inactive] {
        let mut tx = pool.begin().await?;
        assert_eq!(
            super::lock_recovering_execution(&mut tx, other, 1).await?,
            None
        );
        tx.rollback().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn hand_off_to_successor_moves_only_a_creating_successor() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let attempt = Uuid::now_v7();

    let sandbox_id = insert_sandbox(pool, "started").await?;
    insert_execution(pool, sandbox_id, 2, "creating", true, None, 0).await?;
    assert_eq!(
        hand_off_to_successor(pool, sandbox_id, 2, Some(attempt), Some("started"), 60).await?,
        1
    );
    let successor = execution(pool, sandbox_id, 2).await?;
    assert_eq!(successor.state, "recovering");
    assert_eq!(successor.activation_attempt_id, Some(attempt));
    assert_eq!(successor.route_activation_id, Some(attempt));
    assert!(successor.activation_attempt_expires_at.is_some());
    assert_eq!(
        successor.activation_attempt_target_state.as_deref(),
        Some("started")
    );

    // Without an attempt the successor takes no lease.
    let unattempted = insert_sandbox(pool, "started").await?;
    insert_execution(pool, unattempted, 1, "creating", true, None, 0).await?;
    assert_eq!(
        hand_off_to_successor(pool, unattempted, 1, None, None, 60).await?,
        1
    );
    let successor = execution(pool, unattempted, 1).await?;
    assert_eq!(successor.state, "recovering");
    assert_eq!(successor.activation_attempt_expires_at, None);

    // Losing cases: a successor that is no longer `creating`, the wrong
    // generation (against a successor still `creating` as well) and a
    // retired one.
    let retired = insert_sandbox(pool, "started").await?;
    insert_execution(pool, retired, 1, "creating", false, None, 0).await?;
    let wrong_generation = insert_sandbox(pool, "started").await?;
    insert_execution(pool, wrong_generation, 2, "creating", true, None, 0).await?;
    for (sandbox_id, generation) in [
        (sandbox_id, 2),
        (sandbox_id, 3),
        (retired, 1),
        (wrong_generation, 3),
    ] {
        let stored = if generation == 3 { 2 } else { generation };
        let before = execution(pool, sandbox_id, stored).await?;
        assert_eq!(
            hand_off_to_successor(pool, sandbox_id, generation, Some(Uuid::now_v7()), None, 60)
                .await?,
            0
        );
        assert_eq!(execution(pool, sandbox_id, stored).await?, before);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn complete_moves_only_a_recovering_execution_at_or_past_the_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let sandbox_id = insert_sandbox(pool, "started").await?;
    insert_execution(pool, sandbox_id, 4, "recovering", true, None, 0).await?;
    assert_eq!(
        complete(pool, sandbox_id, 3, "running", "recovering").await?,
        1
    );
    assert_eq!(execution(pool, sandbox_id, 4).await?.state, "running");
    let equal = insert_sandbox(pool, "started").await?;
    insert_execution(pool, equal, 4, "recovering", true, None, 0).await?;
    assert_eq!(complete(pool, equal, 4, "running", "recovering").await?, 1);

    // Losing cases: an execution no longer recovering, one older than the
    // generation and a retired one.
    let older = insert_sandbox(pool, "started").await?;
    insert_execution(pool, older, 4, "recovering", true, None, 0).await?;
    let retired = insert_sandbox(pool, "started").await?;
    insert_execution(pool, retired, 4, "recovering", false, None, 0).await?;
    for (sandbox_id, generation) in [(sandbox_id, 4), (older, 5), (retired, 4)] {
        let before = execution(pool, sandbox_id, 4).await?;
        assert_eq!(
            complete(pool, sandbox_id, generation, "running", "recovering").await?,
            0
        );
        assert_eq!(execution(pool, sandbox_id, 4).await?, before);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn fail_matches_the_attempt_or_the_unattempted_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let attempt = Uuid::now_v7();

    // With an attempt: the execution at or past the generation holding it.
    let attempted = insert_sandbox(pool, "started").await?;
    insert_execution(pool, attempted, 5, "recovering", true, Some(attempt), 0).await?;
    assert_eq!(
        fail(
            pool,
            attempted,
            4,
            "recovery_failed",
            "recovering",
            Some(SandboxActivationAttemptId::from_uuid(attempt)),
        )
        .await?,
        Some(5)
    );
    let failed = execution(pool, attempted, 5).await?;
    assert_eq!(failed.state, "recovery_failed");
    assert_eq!(failed.activation_attempt_id, None);
    assert_eq!(failed.activation_attempt_expires_at, None);
    assert_eq!(failed.activation_attempt_target_state, None);

    // Without one: exactly the generation, holding no attempt.
    let unattempted = insert_sandbox(pool, "started").await?;
    insert_execution(pool, unattempted, 2, "recovering", true, None, 0).await?;
    assert_eq!(
        fail(pool, unattempted, 2, "recovery_failed", "recovering", None).await?,
        Some(2)
    );

    // Losing cases: another attempt, no attempt against an execution that
    // holds one, no attempt at a newer generation than asked, an attempt at
    // an older generation than asked, an execution no longer recovering
    // (with and without an attempt), and an inactive execution holding the
    // attempt.
    let held = insert_sandbox(pool, "started").await?;
    insert_execution(pool, held, 3, "recovering", true, Some(attempt), 0).await?;
    let newer = insert_sandbox(pool, "started").await?;
    insert_execution(pool, newer, 3, "recovering", true, None, 0).await?;
    let inactive = insert_sandbox(pool, "started").await?;
    insert_execution(pool, inactive, 3, "recovering", false, Some(attempt), 0).await?;
    let other_attempt = Some(SandboxActivationAttemptId::from_uuid(Uuid::now_v7()));
    let held_attempt = Some(SandboxActivationAttemptId::from_uuid(attempt));
    for (sandbox_id, generation, activation_attempt_id) in [
        (held, 3, other_attempt),
        (held, 3, None),
        (newer, 2, None),
        (held, 4, held_attempt),
        (attempted, 5, held_attempt),
        (attempted, 5, None),
        (inactive, 3, held_attempt),
    ] {
        let stored = if sandbox_id == attempted { 5 } else { 3 };
        let before = execution(pool, sandbox_id, stored).await?;
        assert_eq!(
            fail(
                pool,
                sandbox_id,
                generation,
                "recovery_failed",
                "recovering",
                activation_attempt_id,
            )
            .await?,
            None
        );
        assert_eq!(execution(pool, sandbox_id, stored).await?, before);
    }
    Ok(())
}
