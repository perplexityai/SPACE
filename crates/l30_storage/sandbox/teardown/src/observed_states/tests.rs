use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::clear_failed_observation;

async fn insert_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.10', 'worker', 'k8s', 'ready')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(node_id)
}

#[tokio::test(flavor = "current_thread")]
async fn clear_failed_observation_keeps_a_live_record() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = insert_node(&db.pool).await?;
    let observed = |status: &'static str, pid: Option<i32>| {
        let pool = db.pool.clone();
        async move {
            let sandbox_id = SandboxId::new();
            sqlx::query(sql!(
                "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, pid)
                 VALUES ($1, $2, $3, $4)"
            ))
            .bind(sandbox_id)
            .bind(node_id)
            .bind(status)
            .bind(pid)
            .execute(&pool)
            .await?;
            TestResult::<SandboxId>::Ok(sandbox_id)
        }
    };
    let exists = |sandbox_id: SandboxId| {
        let pool = db.pool.clone();
        async move {
            sqlx::query_scalar::<_, bool>(sql!(
                "SELECT EXISTS (SELECT 1 FROM sandbox_observed_states WHERE sandbox_id = $1)"
            ))
            .bind(sandbox_id)
            .fetch_one(&pool)
            .await
        }
    };
    let fossil = observed("failed", None).await?;
    let running = observed("running", None).await?;
    let failed_with_pid = observed("failed", Some(42)).await?;

    let mut conn = db.pool.acquire().await?;
    for sandbox_id in [fossil, running, failed_with_pid] {
        clear_failed_observation(&mut *conn, sandbox_id).await?;
    }
    drop(conn);

    assert!(
        !exists(fossil).await?,
        "a failed, pid-less record is purged"
    );
    assert!(exists(running).await?, "a non-failed record survives");
    assert!(
        exists(failed_with_pid).await?,
        "a record with a pid survives"
    );
    Ok(())
}

/// Only a publish whose status reads `stopped` (trimmed, any case) and whose
/// execution metadata names the environment counts; another status, another
/// environment, no metadata or no publish at all doesn't.
#[tokio::test(flavor = "current_thread")]
async fn published_stopped_under_matches_only_a_stopped_publish_under_the_environment() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = insert_node(&db.pool).await?;
    const E1: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    const E2: &str = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
    for (status, environment, published) in [
        (" Stopped", Some(E1), true),
        ("running", Some(E1), false),
        ("stopped", Some(E2), false),
        ("stopped", None, false),
    ] {
        let sandbox_id = SandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, execution_metadata)
             VALUES ($1, $2, $3,
                     CASE WHEN $4::TEXT IS NULL THEN NULL
                          ELSE jsonb_build_object('environment', $4::TEXT) END)"
        ))
        .bind(sandbox_id)
        .bind(node_id)
        .bind(status)
        .bind(environment)
        .execute(&db.pool)
        .await?;
        assert_eq!(
            super::published_stopped_under(&db.pool, sandbox_id, E1).await?,
            published,
            "{status:?} under {environment:?}"
        );
    }
    assert!(!super::published_stopped_under(&db.pool, SandboxId::new(), E1).await?);
    Ok(())
}
