//! The execution state lock holds `FOR UPDATE` until the caller's
//! transaction ends and sees a retirement it waited on, and the state
//! compare-and-set writes only from the expected state.

use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, try_pg_test_db, try_pg_test_db_with_max_connections,
};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{compare_and_set_active_state, lock_active_state};
use crate::k8s_executions::retire;

async fn insert_sandbox(pool: &PgPool) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', 'started', 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    state: &str,
    active: bool,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, state, active)
         VALUES ($1, $2, $3, 'space-sandboxes', $4, $5)"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(format!(
        "pod-{}-g{generation}",
        sandbox_id.as_uuid().simple()
    ))
    .bind(state)
    .bind(active)
    .execute(pool)
    .await?;
    Ok(())
}

/// Whether another transaction's `FOR KEY SHARE NOWAIT` on the execution
/// fails with `lock_not_available`. Only `FOR UPDATE` refuses it, so this
/// pins the lock's mode.
async fn key_share_blocked(
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
    match result {
        Ok(_) => Ok(false),
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03") => Ok(true),
        Err(error) => Err(error.into()),
    }
}

/// Every execution row of the sandbox, oldest generation first: generation,
/// state and active.
async fn rows(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Vec<(i64, String, bool)>> {
    Ok(sqlx::query_as::<_, (i64, String, bool)>(sql!(
        "SELECT generation, state, active
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation"
    ))
    .bind(sandbox_id)
    .fetch_all(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn active_state_lock_holds_for_update_until_the_transaction_ends() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;
    insert_execution(pool, sandbox, 1, "replaced", false).await?;
    insert_execution(pool, sandbox, 2, "running", true).await?;

    let mut tx = pool.begin().await?;
    assert_eq!(
        lock_active_state(&mut tx, sandbox, 2).await?.as_deref(),
        Some("running")
    );
    assert!(key_share_blocked(pool, sandbox, 2).await?);
    tx.commit().await?;
    assert!(!key_share_blocked(pool, sandbox, 2).await?);

    let mut tx = pool.begin().await?;
    assert!(lock_active_state(&mut tx, sandbox, 2).await?.is_some());
    assert!(key_share_blocked(pool, sandbox, 2).await?);
    tx.rollback().await?;
    assert!(!key_share_blocked(pool, sandbox, 2).await?);

    // A retired generation, or one the sandbox never had, reads as `None`
    // and locks nothing.
    let mut tx = pool.begin().await?;
    assert_eq!(lock_active_state(&mut tx, sandbox, 1).await?, None);
    assert_eq!(lock_active_state(&mut tx, sandbox, 3).await?, None);
    assert!(!key_share_blocked(pool, sandbox, 1).await?);
    assert!(!key_share_blocked(pool, sandbox, 2).await?);
    tx.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn active_state_lock_that_waits_on_a_retirement_reads_none() -> TestResult {
    // The retiring transaction, the waiting lock and the wait probe.
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;
    insert_execution(pool, sandbox, 2, "running", true).await?;

    let mut retiring = pool.begin().await?;
    let retiring_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *retiring)
        .await?;
    assert!(retire::finish_generation(&mut *retiring, sandbox, 2, "replaced").await?);

    let waiter_pool = pool.clone();
    let waiter = tokio::spawn(async move {
        let mut tx = waiter_pool.begin().await?;
        let state = lock_active_state(&mut tx, sandbox, 2).await?;
        tx.rollback().await?;
        Ok::<_, sqlx::Error>(state)
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let waiting: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                  WHERE datname = current_database()
                    AND $1 = ANY(pg_blocking_pids(pid))
             )"
        ))
        .bind(retiring_pid)
        .fetch_one(pool)
        .await?;
        if waiting {
            break;
        }
        assert!(!waiter.is_finished(), "the lock never waited");
        assert!(
            std::time::Instant::now() < deadline,
            "the lock never waited"
        );
        tokio::task::yield_now().await;
    }
    retiring.commit().await?;
    assert_eq!(waiter.await??, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn state_compare_and_set_writes_only_from_the_expected_state() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;
    insert_execution(pool, sandbox, 1, "creating", false).await?;
    insert_execution(pool, sandbox, 2, "created", true).await?;
    let before = rows(pool, sandbox).await?;

    // Another state, a retired generation, or one the sandbox never had.
    for (generation, expected) in [(2, "creating"), (1, "creating"), (3, "created")] {
        assert_eq!(
            compare_and_set_active_state(pool, sandbox, generation, "recovering", expected).await?,
            0
        );
        assert_eq!(rows(pool, sandbox).await?, before);
    }

    assert_eq!(
        compare_and_set_active_state(pool, sandbox, 2, "recovering", "created").await?,
        1
    );
    assert_eq!(
        rows(pool, sandbox).await?,
        vec![
            (1, "creating".to_string(), false),
            (2, "recovering".to_string(), true),
        ]
    );
    Ok(())
}
