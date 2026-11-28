//! [`Tx::savepoint`]: what a nested [`Tx`] keeps or undoes in the transaction
//! around it, read back after the outer transaction commits.

use pplx_space_db_test_fixtures::{TestDb, try_raw_pg_test_db_with_max_connections};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;

use crate::{Db, DbError, DbErrorKind, Executor};

/// A test database with one empty `items` table, and a [`Db`] on its pool.
async fn db_with_items() -> TestResult<Option<(TestDb, Db)>> {
    let Some(test_db) = try_raw_pg_test_db_with_max_connections(4).await? else {
        return Ok(None);
    };
    sqlx::query(sql!("CREATE TABLE items (id BIGINT PRIMARY KEY)"))
        .execute(&test_db.pool)
        .await?;
    let db = Db::from(test_db.pool.clone());
    Ok(Some((test_db, db)))
}

async fn insert(executor: impl Executor<'_>, id: i64) -> Result<(), DbError> {
    sqlx::query(sql!("INSERT INTO items (id) VALUES ($1)"))
        .bind(id)
        .execute(executor.into_sqlx())
        .await?;
    Ok(())
}

async fn ids(db: &Db) -> Result<Vec<i64>, DbError> {
    let ids = sqlx::query_scalar(sql!("SELECT id FROM items ORDER BY id"))
        .fetch_all(db.into_sqlx())
        .await?;
    Ok(ids)
}

#[tokio::test]
async fn a_committed_savepoint_keeps_its_writes_in_the_transaction() -> TestResult {
    let Some((_test_db, db)) = db_with_items().await? else {
        return Ok(());
    };
    let mut tx = db.begin().await?;
    insert(&mut tx, 1).await?;
    let mut savepoint = tx.savepoint().await?;
    insert(&mut savepoint, 2).await?;
    let mut inner = savepoint.savepoint().await?;
    insert(&mut inner, 3).await?;
    inner.commit().await?;
    savepoint.commit().await?;
    // Released, not committed: nothing is visible outside the transaction.
    assert_eq!(ids(&db).await?, Vec::<i64>::new());
    tx.commit().await?;
    assert_eq!(ids(&db).await?, [1, 2, 3]);
    Ok(())
}

#[tokio::test]
async fn a_rolled_back_or_dropped_savepoint_undoes_only_its_writes() -> TestResult {
    let Some((_test_db, db)) = db_with_items().await? else {
        return Ok(());
    };
    let mut tx = db.begin().await?;
    insert(&mut tx, 1).await?;
    let mut savepoint = tx.savepoint().await?;
    insert(&mut savepoint, 2).await?;
    savepoint.rollback().await?;
    {
        let mut savepoint = tx.savepoint().await?;
        insert(&mut savepoint, 3).await?;
    }
    insert(&mut tx, 4).await?;
    tx.commit().await?;
    assert_eq!(ids(&db).await?, [1, 4]);
    Ok(())
}

/// A failed statement aborts only the savepoint: rolled back, the outer
/// transaction runs more statements and commits.
#[tokio::test]
async fn a_failure_in_a_savepoint_leaves_the_transaction_usable() -> TestResult {
    let Some((_test_db, db)) = db_with_items().await? else {
        return Ok(());
    };
    let mut tx = db.begin().await?;
    insert(&mut tx, 1).await?;
    let mut savepoint = tx.savepoint().await?;
    let error = insert(&mut savepoint, 1)
        .await
        .expect_err("a duplicate key");
    assert_eq!(error.kind(), DbErrorKind::UniqueViolation, "{error:?}");
    savepoint.rollback().await?;
    insert(&mut tx, 2).await?;
    tx.commit().await?;
    assert_eq!(ids(&db).await?, [1, 2]);
    Ok(())
}
