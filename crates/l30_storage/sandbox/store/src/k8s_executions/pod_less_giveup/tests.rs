use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;

use super::super::ownership_test_fixture::{
    execution_key_share_blocked, execution_state, execution_version, insert_execution,
    insert_sandbox,
};
use super::{lock_executions_in_generation_order, retire_active_executions};

#[tokio::test(flavor = "current_thread")]
async fn lock_executions_in_generation_order_locks_every_execution_for_update() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = insert_sandbox(pool, "started").await?;
    let retired = insert_execution(pool, sandbox_id, 1, "replaced", false, None).await?;
    let active = insert_execution(pool, sandbox_id, 2, "creating", true, None).await?;
    let other_sandbox = insert_sandbox(pool, "started").await?;
    let other = insert_execution(pool, other_sandbox, 1, "creating", true, None).await?;

    let mut tx = pool.begin().await?;
    lock_executions_in_generation_order(&mut *tx, sandbox_id).await?;
    // `FOR UPDATE` on each, inactive included: even a key-share lock waits.
    assert!(execution_key_share_blocked(pool, retired.id).await?);
    assert!(execution_key_share_blocked(pool, active.id).await?);
    assert!(!execution_key_share_blocked(pool, other.id).await?);
    tx.commit().await?;
    assert!(!execution_key_share_blocked(pool, retired.id).await?);
    assert!(!execution_key_share_blocked(pool, active.id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retire_active_executions_retires_only_the_active_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = insert_sandbox(pool, "started").await?;
    let retired = insert_execution(pool, sandbox_id, 1, "replaced", false, None).await?;
    let retired_before = execution_version(pool, retired.id).await?;
    let other_sandbox = insert_sandbox(pool, "started").await?;
    let other = insert_execution(pool, other_sandbox, 1, "creating", true, None).await?;
    let other_before = execution_version(pool, other.id).await?;

    // The losing case: no active execution, and nothing changes.
    assert_eq!(retire_active_executions(pool, sandbox_id).await?, 0);
    assert_eq!(execution_version(pool, retired.id).await?, retired_before);

    let active = insert_execution(pool, sandbox_id, 2, "creating", true, None).await?;
    assert_eq!(retire_active_executions(pool, sandbox_id).await?, 1);
    assert_eq!(
        execution_state(pool, active.id).await?,
        (false, "suspended".to_string())
    );
    assert_eq!(execution_version(pool, retired.id).await?, retired_before);
    assert_eq!(
        execution_state(pool, retired.id).await?,
        (false, "replaced".to_string())
    );
    // Another sandbox's active execution is not retired.
    assert_eq!(execution_version(pool, other.id).await?, other_before);
    Ok(())
}
