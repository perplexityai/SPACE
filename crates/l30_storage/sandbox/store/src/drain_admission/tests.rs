//! The v2 drain start's read of the sandbox rows against Postgres.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_node_model::SubstrateKind;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;

async fn insert_sandbox(pool: &PgPool, lifecycle: &str) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, $4, $5, 'unplaced', 1)"
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

/// Of the ids asked about, exactly the `stopped` rows come back, by id: not
/// a row in another lifecycle, an id with no row, or a stopped row nobody
/// asked about.
#[tokio::test(flavor = "current_thread")]
async fn only_the_asked_stopped_rows_come_back() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let mut stopped = vec![
        insert_sandbox(pool, "stopped").await?,
        insert_sandbox(pool, "stopped").await?,
    ];
    stopped.sort();
    let started = insert_sandbox(pool, "started").await?;
    let suspended = insert_sandbox(pool, "suspended").await?;
    let unasked = insert_sandbox(pool, "stopped").await?;
    let asked = [stopped[1], started, SandboxId::new(), suspended, stopped[0]];
    assert_eq!(stopped_among(pool, &asked).await?, stopped);
    assert_eq!(stopped_among(pool, &[started, suspended]).await?, []);
    assert_eq!(stopped_among(pool, &[]).await?, []);
    assert_eq!(stopped_among(pool, &[unasked]).await?, [unasked]);
    Ok(())
}
