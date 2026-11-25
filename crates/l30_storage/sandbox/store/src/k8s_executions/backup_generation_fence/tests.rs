use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_sandbox_model::ExecutionGeneration;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;

use super::super::ownership_test_fixture::{
    execution_no_key_update_blocked, execution_share_blocked, insert_execution, insert_sandbox,
};
use super::{FenceRow, share_lock_from_generation};

#[tokio::test(flavor = "current_thread")]
async fn share_lock_from_generation_share_locks_the_executions_from_the_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = insert_sandbox(pool, "started").await?;
    let older = insert_execution(pool, sandbox_id, 1, "suspended", false, None).await?;
    let captured = insert_execution(pool, sandbox_id, 2, "replaced", false, Some(" ")).await?;
    let newer = insert_execution(pool, sandbox_id, 3, "creating", true, None).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_materialized_at = NOW() WHERE id = $1"
    ))
    .bind(newer.id)
    .execute(pool)
    .await?;

    let mut tx = pool.begin().await?;
    let mut rows =
        share_lock_from_generation(&mut tx, sandbox_id, ExecutionGeneration::new(2)?).await?;
    rows.sort_by_key(|row| row.generation);
    assert_eq!(
        rows,
        [
            FenceRow {
                generation: 2,
                active: false,
                replaced: true,
                materialized: false,
            },
            FenceRow {
                generation: 3,
                active: true,
                replaced: false,
                materialized: true,
            },
        ]
    );
    // `FOR SHARE`, not `FOR KEY SHARE`: a `FOR NO KEY UPDATE` waits. And not
    // `FOR NO KEY UPDATE` or `FOR UPDATE`: another `FOR SHARE` doesn't.
    for id in [captured.id, newer.id] {
        assert!(execution_no_key_update_blocked(pool, id).await?);
        assert!(!execution_share_blocked(pool, id).await?);
    }
    // Below the generation, nothing is locked.
    assert!(!execution_no_key_update_blocked(pool, older.id).await?);
    tx.commit().await?;
    for id in [captured.id, newer.id] {
        assert!(!execution_no_key_update_blocked(pool, id).await?);
    }
    Ok(())
}
