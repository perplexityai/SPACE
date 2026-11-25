use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use uuid::Uuid;

use super::super::ownership_test_fixture::{
    execution_key_share_blocked, execution_state, execution_version, insert_execution,
    insert_sandbox,
};
use super::{lock_executions, retire_recovered_execution};

#[tokio::test(flavor = "current_thread")]
async fn lock_executions_locks_every_execution_of_the_sandbox_for_update() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = insert_sandbox(pool, "quarantined").await?;
    let retired = insert_execution(pool, sandbox_id, 1, "replaced", false, None).await?;
    let active = insert_execution(pool, sandbox_id, 2, "running", true, None).await?;
    let other_sandbox = insert_sandbox(pool, "quarantined").await?;
    let other = insert_execution(pool, other_sandbox, 1, "running", true, None).await?;

    let mut tx = pool.begin().await?;
    lock_executions(&mut tx, sandbox_id).await?;
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
async fn retire_recovered_execution_retires_only_the_named_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = insert_sandbox(pool, "quarantined").await?;
    let named = insert_execution(pool, sandbox_id, 1, "running", true, None).await?;
    let before = execution_version(pool, named.id).await?;

    // The losing case: no execution has that id, and nothing changes.
    assert!(!retire_recovered_execution(pool, Uuid::now_v7()).await?);
    assert_eq!(execution_version(pool, named.id).await?, before);

    assert!(retire_recovered_execution(pool, named.id).await?);
    assert_eq!(
        execution_state(pool, named.id).await?,
        (false, "suspended".to_string())
    );
    Ok(())
}
