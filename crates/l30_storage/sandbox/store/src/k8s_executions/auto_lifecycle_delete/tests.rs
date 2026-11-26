use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;

use super::super::ownership_test_fixture::{
    execution_key_share_blocked, hold_execution, insert_execution, insert_sandbox,
    is_lock_not_available,
};
use super::lock_latest_execution_evidence;

#[tokio::test(flavor = "current_thread")]
async fn lock_latest_execution_evidence_locks_only_the_latest_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = insert_sandbox(pool, "suspended").await?;
    let older = insert_execution(pool, sandbox_id, 1, "replaced", false, None).await?;
    let latest = insert_execution(pool, sandbox_id, 2, "suspended", false, None).await?;

    let mut tx = pool.begin().await?;
    let evidence = lock_latest_execution_evidence(&mut *tx, sandbox_id, NodeId::new())
        .await?
        .expect("the sandbox has executions");
    assert_eq!(evidence.latest_execution_generation.get(), 2);
    assert_eq!(evidence.route_activation_id, None);
    assert!(!evidence.publisher_owns_execution);
    assert!(!evidence.creating_snapshot_exists);
    // `FOR UPDATE OF execution`, the statement's one `FROM` relation: the
    // latest execution only, so that even a key-share lock waits on it.
    assert!(execution_key_share_blocked(pool, latest.id).await?);
    assert!(!execution_key_share_blocked(pool, older.id).await?);
    tx.commit().await?;
    assert!(!execution_key_share_blocked(pool, latest.id).await?);

    // `NOWAIT`: a held latest execution fails the read at once with
    // `lock_not_available`.
    let holder = hold_execution(pool, latest.id).await?;
    let mut tx = pool.begin().await?;
    let error = sqlx::Error::from(
        lock_latest_execution_evidence(&mut *tx, sandbox_id, NodeId::new())
            .await
            .expect_err("a held execution fails the lock"),
    );
    assert!(is_lock_not_available(&error), "{error}");
    tx.rollback().await?;
    holder.rollback().await?;

    let bare = insert_sandbox(pool, "suspended").await?;
    let mut tx = pool.begin().await?;
    assert!(
        lock_latest_execution_evidence(&mut *tx, bare, NodeId::new())
            .await?
            .is_none()
    );
    tx.rollback().await?;
    Ok(())
}
