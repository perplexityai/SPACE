use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::claim;

async fn insert_sandbox(pool: &PgPool) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn insert_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    state: &str,
) -> TestResult<SnapshotId> {
    let id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, org_id)
         VALUES ($1, $2, 'full', $3, $4::UUID)"
    ))
    .bind(id)
    .bind(sandbox_id)
    .bind(state)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn snapshot_state(pool: &PgPool, id: SnapshotId) -> TestResult<Option<String>> {
    Ok(
        sqlx::query_scalar(sql!("SELECT state FROM snapshots WHERE id = $1"))
            .bind(id)
            .fetch_optional(pool)
            .await?,
    )
}

/// A `ready` snapshot is claimed once. The losing cases: a claimed row, a row
/// in any other state and a missing row match nothing and change nothing.
#[tokio::test(flavor = "current_thread")]
async fn claim_moves_only_a_ready_snapshot_to_deleting() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;
    let ready = insert_snapshot(pool, sandbox, "ready").await?;
    let creating = insert_snapshot(pool, sandbox, "creating").await?;
    let failed = insert_snapshot(pool, sandbox, "failed").await?;

    let mut tx = pool.begin().await?;
    assert!(claim(&mut *tx, ready).await?);
    assert!(!claim(&mut *tx, ready).await?);
    assert!(!claim(&mut *tx, creating).await?);
    assert!(!claim(&mut *tx, failed).await?);
    assert!(!claim(&mut *tx, SnapshotId::new()).await?);
    tx.commit().await?;

    assert_eq!(
        snapshot_state(pool, ready).await?.as_deref(),
        Some("deleting")
    );
    assert_eq!(
        snapshot_state(pool, creating).await?.as_deref(),
        Some("creating")
    );
    assert_eq!(
        snapshot_state(pool, failed).await?.as_deref(),
        Some("failed")
    );
    Ok(())
}
