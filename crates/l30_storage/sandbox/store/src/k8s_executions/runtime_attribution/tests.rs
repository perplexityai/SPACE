//! [`lock_tracked_pod_uid`] reads what its caller decides on, and holds a
//! `FOR UPDATE` lock on the active execution row until the caller's
//! transaction ends.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::lock_tracked_pod_uid;

async fn insert_sandbox(pool: &PgPool) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'started', 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Kubernetes)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    active: bool,
    pod_uid: Option<&str>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
         VALUES ($1, $2, 'pod-' || $1::TEXT || '-' || $2::TEXT, 'sandboxes', $3,
                 'running', $4)"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(pod_uid)
    .bind(active)
    .execute(pool)
    .await?;
    Ok(())
}

/// Whether another transaction's `FOR KEY SHARE NOWAIT` on the execution
/// row fails with `lock_not_available`. Only `FOR UPDATE` refuses it, so this
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

#[tokio::test(flavor = "current_thread")]
async fn lock_tracked_pod_uid_reads_the_active_generations_uid() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let tracked = insert_sandbox(pool).await?;
    insert_execution(pool, tracked, 1, false, Some("uid-old")).await?;
    insert_execution(pool, tracked, 2, true, Some("uid-a")).await?;
    let blank = insert_sandbox(pool).await?;
    insert_execution(pool, blank, 1, true, Some("  ")).await?;

    let mut tx = pool.begin().await?;
    assert_eq!(
        lock_tracked_pod_uid(&mut tx, tracked, 2).await?,
        Some(Some("uid-a".to_string()))
    );
    assert_eq!(lock_tracked_pod_uid(&mut tx, blank, 1).await?, Some(None));
    // A retired generation, one never minted, and a missing sandbox.
    assert_eq!(lock_tracked_pod_uid(&mut tx, tracked, 1).await?, None);
    assert_eq!(lock_tracked_pod_uid(&mut tx, tracked, 3).await?, None);
    assert_eq!(
        lock_tracked_pod_uid(&mut tx, SandboxId::new(), 1).await?,
        None
    );
    tx.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn lock_tracked_pod_uid_holds_for_update_until_the_transaction_ends() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;
    insert_execution(pool, sandbox, 1, false, Some("uid-old")).await?;
    insert_execution(pool, sandbox, 2, true, Some("uid-a")).await?;
    assert!(!key_share_blocked(pool, sandbox, 2).await?);

    // Held until commit.
    let mut tx = pool.begin().await?;
    assert!(lock_tracked_pod_uid(&mut tx, sandbox, 2).await?.is_some());
    assert!(key_share_blocked(pool, sandbox, 2).await?);
    tx.commit().await?;
    assert!(!key_share_blocked(pool, sandbox, 2).await?);

    // Held until rollback.
    let mut tx = pool.begin().await?;
    assert!(lock_tracked_pod_uid(&mut tx, sandbox, 2).await?.is_some());
    assert!(key_share_blocked(pool, sandbox, 2).await?);
    tx.rollback().await?;
    assert!(!key_share_blocked(pool, sandbox, 2).await?);

    // A retired generation matches no row and locks nothing.
    let mut tx = pool.begin().await?;
    assert_eq!(lock_tracked_pod_uid(&mut tx, sandbox, 1).await?, None);
    assert!(!key_share_blocked(pool, sandbox, 1).await?);
    assert!(!key_share_blocked(pool, sandbox, 2).await?);
    tx.rollback().await?;
    Ok(())
}
