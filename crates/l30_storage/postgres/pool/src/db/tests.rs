//! A verb written against [`Executor`] runs on the pool and inside a
//! transaction, and composes with another store's verb in one transaction.

use pplx_space_db_test_fixtures::try_raw_pg_test_db_with_max_connections;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;

use crate::{Db, DbError, Executor};

/// Two stand-in stores, each writing only its own table, as real stores do.
mod widget_store {
    use super::*;

    pub async fn insert(executor: impl Executor<'_>, id: i64) -> Result<(), DbError> {
        sqlx::query(sql!("INSERT INTO widgets (id) VALUES ($1)"))
            .bind(id)
            .execute(executor.into_sqlx())
            .await?;
        Ok(())
    }

    pub async fn count(executor: impl Executor<'_>) -> Result<i64, DbError> {
        let count = sqlx::query_scalar::<_, i64>(sql!("SELECT COUNT(*)::BIGINT FROM widgets"))
            .fetch_one(executor.into_sqlx())
            .await?;
        Ok(count)
    }
}

mod gadget_store {
    use super::*;

    pub async fn insert(executor: impl Executor<'_>, id: i64) -> Result<(), DbError> {
        sqlx::query(sql!("INSERT INTO gadgets (id) VALUES ($1)"))
            .bind(id)
            .execute(executor.into_sqlx())
            .await?;
        Ok(())
    }

    pub async fn count(executor: impl Executor<'_>) -> Result<i64, DbError> {
        let count = sqlx::query_scalar::<_, i64>(sql!("SELECT COUNT(*)::BIGINT FROM gadgets"))
            .fetch_one(executor.into_sqlx())
            .await?;
        Ok(count)
    }
}

async fn db_with_tables() -> TestResult<Option<(pplx_space_db_test_fixtures::TestDb, Db)>> {
    let Some(test_db) = try_raw_pg_test_db_with_max_connections(4).await? else {
        return Ok(None);
    };
    sqlx::query(sql!("CREATE TABLE widgets (id BIGINT PRIMARY KEY)"))
        .execute(&test_db.pool)
        .await?;
    sqlx::query(sql!("CREATE TABLE gadgets (id BIGINT PRIMARY KEY)"))
        .execute(&test_db.pool)
        .await?;
    let db = Db::from(test_db.pool.clone());
    Ok(Some((test_db, db)))
}

#[tokio::test]
async fn a_verb_runs_on_the_pool() -> TestResult {
    let Some((_test_db, db)) = db_with_tables().await? else {
        return Ok(());
    };
    widget_store::insert(&db, 1).await?;
    assert_eq!(widget_store::count(&db).await?, 1);
    Ok(())
}

#[tokio::test]
async fn verbs_of_two_stores_commit_together_in_one_tx() -> TestResult {
    let Some((_test_db, db)) = db_with_tables().await? else {
        return Ok(());
    };
    let mut tx = db.begin().await?;
    widget_store::insert(&mut tx, 1).await?;
    gadget_store::insert(&mut tx, 1).await?;
    // The transaction sees its own writes; the pool does not yet.
    assert_eq!(widget_store::count(&mut tx).await?, 1);
    assert_eq!(widget_store::count(&db).await?, 0);
    tx.commit().await?;

    assert_eq!(widget_store::count(&db).await?, 1);
    assert_eq!(gadget_store::count(&db).await?, 1);
    Ok(())
}

#[tokio::test]
async fn rollback_and_drop_discard_both_stores_writes() -> TestResult {
    let Some((_test_db, db)) = db_with_tables().await? else {
        return Ok(());
    };
    let mut tx = db.begin().await?;
    widget_store::insert(&mut tx, 1).await?;
    gadget_store::insert(&mut tx, 1).await?;
    tx.rollback().await?;

    {
        let mut tx = db.begin().await?;
        widget_store::insert(&mut tx, 2).await?;
        gadget_store::insert(&mut tx, 2).await?;
    }

    assert_eq!(widget_store::count(&db).await?, 0);
    assert_eq!(gadget_store::count(&db).await?, 0);
    Ok(())
}

/// A function holding `&mut Tx` passes it on with `&mut *tx`, once per verb.
#[tokio::test]
async fn a_borrowed_tx_is_passed_on_per_verb() -> TestResult {
    async fn insert_both(tx: &mut crate::Tx<'_>, id: i64) -> Result<(), DbError> {
        widget_store::insert(&mut *tx, id).await?;
        gadget_store::insert(&mut *tx, id).await
    }

    let Some((_test_db, db)) = db_with_tables().await? else {
        return Ok(());
    };
    let mut tx = db.begin().await?;
    insert_both(&mut tx, 1).await?;
    insert_both(&mut tx, 2).await?;
    tx.commit().await?;
    assert_eq!(gadget_store::count(&db).await?, 2);
    Ok(())
}

/// The transition implementations: a caller still holding the driver's pool,
/// or an open driver transaction, calls the same verb.
#[tokio::test]
async fn a_verb_accepts_the_drivers_pool_and_connection() -> TestResult {
    let Some((test_db, _db)) = db_with_tables().await? else {
        return Ok(());
    };
    widget_store::insert(&test_db.pool, 1).await?;
    let mut tx = test_db.pool.begin().await?;
    widget_store::insert(&mut *tx, 2).await?;
    gadget_store::insert(&mut *tx, 2).await?;
    tx.commit().await?;
    assert_eq!(widget_store::count(&test_db.pool).await?, 2);
    assert_eq!(gadget_store::count(&test_db.pool).await?, 1);
    Ok(())
}

/// What `tokio::spawn` requires of a task: a future that borrows a `Db` or a
/// `Tx` across awaits, through nested async functions, is `Send`. This is a
/// compile-time check: the test fails to build, not to run, if a verb's
/// future stops being `Send` (see `SqlxExecutor` for why it can).
#[test]
fn futures_using_the_pool_and_a_tx_are_send() {
    // Takes the task's function, never a future: the check is on the type,
    // so it needs no pool and no runtime.
    fn assert_send<F: std::future::Future + Send>(_task: fn(Db) -> F) {}

    async fn on_the_pool(db: &Db) -> Result<i64, DbError> {
        widget_store::insert(db, 1).await?;
        widget_store::count(db).await
    }

    async fn in_a_tx(tx: &mut crate::Tx<'_>) -> Result<i64, DbError> {
        widget_store::insert(&mut *tx, 1).await?;
        gadget_store::insert(&mut *tx, 1).await?;
        widget_store::count(&mut *tx).await
    }

    async fn task(db: Db) -> Result<(), DbError> {
        on_the_pool(&db).await?;
        let mut tx = db.begin().await?;
        in_a_tx(&mut tx).await?;
        tx.commit().await
    }

    assert_send(task);
}

mod savepoint;
mod settings;
