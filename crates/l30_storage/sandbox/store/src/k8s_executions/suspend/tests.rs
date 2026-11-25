//! Each suspend-commit write against the row it takes and the rows its guard
//! refuses, which it leaves unchanged, and the suspended-execution lock's
//! mode until the transaction ends.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{lock_suspended_execution, mark_committed, mark_forgotten_committed};

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

/// An execution tracking pod `uid-<generation>`, last updated a minute ago.
async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    state: &str,
    active: bool,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active,
             updated_at)
         VALUES ($1, $2, $3, 'sandboxes', 'uid-' || $2::TEXT, $4, $5,
                 NOW() - INTERVAL '1 minute')"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(format!("sbx-suspend-{}", Uuid::now_v7()))
    .bind(state)
    .bind(active)
    .execute(pool)
    .await?;
    Ok(())
}

/// `(state, active, updated_at)`.
type ExecutionRow = (String, bool, time::OffsetDateTime);

async fn execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
) -> TestResult<ExecutionRow> {
    Ok(sqlx::query_as(sql!(
        "SELECT state, active, updated_at
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
    match result {
        Ok(_) => Ok(false),
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03") => Ok(true),
        Err(error) => Err(error.into()),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn lock_suspended_execution_holds_the_execution_until_the_transaction_ends() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let active = insert_sandbox(pool, "suspending").await?;
    insert_execution(pool, active, 2, "suspended", true).await?;
    let retired = insert_sandbox(pool, "suspended").await?;
    insert_execution(pool, retired, 2, "suspended", false).await?;

    for (sandbox_id, still_active, commit) in [
        (active, true, true),
        (active, true, false),
        (retired, false, true),
    ] {
        let mut tx = pool.begin().await?;
        assert_eq!(
            lock_suspended_execution(&mut tx, sandbox_id, 2, "uid-2", "suspended").await?,
            Some(still_active)
        );
        assert!(execution_key_share_blocked(pool, sandbox_id, 2).await?);
        if commit {
            tx.commit().await?;
        } else {
            tx.rollback().await?;
        }
        assert!(!execution_key_share_blocked(pool, sandbox_id, 2).await?);
    }

    // No row: another generation, another pod, another state.
    let mut tx = pool.begin().await?;
    for (generation, pod_uid, state) in [
        (3, "uid-2", "suspended"),
        (2, "uid-3", "suspended"),
        (2, "uid-2", "running"),
    ] {
        assert_eq!(
            lock_suspended_execution(&mut tx, active, generation, pod_uid, state).await?,
            None
        );
    }
    tx.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn mark_committed_moves_only_the_active_pods_running_or_suspended_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let running = insert_sandbox(pool, "suspending").await?;
    insert_execution(pool, running, 1, "running", true).await?;
    assert_eq!(
        mark_committed(pool, running, 1, "uid-1", "suspended", "running").await?,
        1
    );
    assert_eq!(execution(pool, running, 1).await?.0, "suspended");
    // A replay finds it already `suspended` and touches it.
    let replay = insert_sandbox(pool, "suspending").await?;
    insert_execution(pool, replay, 1, "suspended", true).await?;
    let before = execution(pool, replay, 1).await?;
    assert_eq!(
        mark_committed(pool, replay, 1, "uid-1", "suspended", "running").await?,
        1
    );
    let after = execution(pool, replay, 1).await?;
    assert_eq!(after.0, "suspended");
    assert!(after.2 > before.2);

    // Losing cases: another pod, another generation, a retired execution
    // and one in neither state.
    let retired = insert_sandbox(pool, "suspending").await?;
    insert_execution(pool, retired, 1, "running", false).await?;
    let creating = insert_sandbox(pool, "suspending").await?;
    insert_execution(pool, creating, 1, "creating", true).await?;
    for (sandbox_id, generation, pod_uid) in [
        (running, 1, "uid-2"),
        (running, 2, "uid-1"),
        (retired, 1, "uid-1"),
        (creating, 1, "uid-1"),
    ] {
        let before = execution(pool, sandbox_id, 1).await?;
        assert_eq!(
            mark_committed(
                pool,
                sandbox_id,
                generation,
                pod_uid,
                "suspended",
                "running"
            )
            .await?,
            0
        );
        assert_eq!(execution(pool, sandbox_id, 1).await?, before);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn mark_forgotten_committed_needs_a_suspending_sandbox_and_no_successor() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    // Another sandbox's successor doesn't stand in the way.
    let succeeded = insert_sandbox(pool, "suspending").await?;
    insert_execution(pool, succeeded, 1, "forgotten", false).await?;
    insert_execution(pool, succeeded, 2, "creating", true).await?;
    let forgotten = insert_sandbox(pool, "suspending").await?;
    insert_execution(pool, forgotten, 1, "forgotten", false).await?;
    assert_eq!(
        mark_forgotten_committed(pool, forgotten, 1, "uid-1", "suspended").await?,
        1
    );
    let (state, active, _) = execution(pool, forgotten, 1).await?;
    assert_eq!((state.as_str(), active), ("suspended", false));

    // Losing cases: a successor exists, the sandbox is no longer
    // `suspending`, the execution is still active, it isn't `forgotten`,
    // another pod, and the wrong generation.
    let started = insert_sandbox(pool, "started").await?;
    insert_execution(pool, started, 1, "forgotten", false).await?;
    let still_active = insert_sandbox(pool, "suspending").await?;
    insert_execution(pool, still_active, 1, "forgotten", true).await?;
    let replaced = insert_sandbox(pool, "suspending").await?;
    insert_execution(pool, replaced, 1, "replaced", false).await?;
    let other_pod = insert_sandbox(pool, "suspending").await?;
    insert_execution(pool, other_pod, 1, "forgotten", false).await?;
    let wrong_generation = insert_sandbox(pool, "suspending").await?;
    insert_execution(pool, wrong_generation, 1, "forgotten", false).await?;
    for (sandbox_id, generation, pod_uid) in [
        (succeeded, 1, "uid-1"),
        (started, 1, "uid-1"),
        (still_active, 1, "uid-1"),
        (replaced, 1, "uid-1"),
        (other_pod, 1, "uid-2"),
        (wrong_generation, 2, "uid-1"),
    ] {
        let before = execution(pool, sandbox_id, 1).await?;
        assert_eq!(
            mark_forgotten_committed(pool, sandbox_id, generation, pod_uid, "suspended").await?,
            0
        );
        assert_eq!(execution(pool, sandbox_id, 1).await?, before);
    }
    Ok(())
}
