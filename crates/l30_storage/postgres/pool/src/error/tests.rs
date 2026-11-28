//! One test per [`DbErrorKind`], each raised by the real condition rather
//! than an injected SQLSTATE, so the classification is checked against what
//! Postgres and the pool actually return.

use std::time::Duration;

use pplx_space_db_test_fixtures::{TestDb, try_raw_pg_test_db_with_max_connections};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions as _, Row as _};

use crate::{Db, DbError, DbErrorKind};

async fn test_db() -> TestResult<Option<(TestDb, Db)>> {
    let Some(test_db) = try_raw_pg_test_db_with_max_connections(4).await? else {
        return Ok(None);
    };
    sqlx::query(sql!(
        "CREATE TABLE parents (
             id BIGINT CONSTRAINT parents_pkey PRIMARY KEY,
             n BIGINT NOT NULL DEFAULT 1 CONSTRAINT parents_n_positive CHECK (n > 0)
         )"
    ))
    .execute(&test_db.pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TABLE children (
             id BIGINT PRIMARY KEY,
             parent_id BIGINT NOT NULL
                 CONSTRAINT children_parent_id_fkey REFERENCES parents (id)
         )"
    ))
    .execute(&test_db.pool)
    .await?;
    sqlx::query(sql!("INSERT INTO parents (id) VALUES (1), (2)"))
        .execute(&test_db.pool)
        .await?;
    let db = Db::from(test_db.pool.clone());
    Ok(Some((test_db, db)))
}

async fn classify(result: Result<impl Sized, sqlx::Error>) -> DbError {
    match result {
        Ok(_) => panic!("the statement must fail"),
        Err(error) => DbError::from(error),
    }
}

#[tokio::test]
async fn a_missing_row_is_not_found() -> TestResult {
    let Some((test_db, _db)) = test_db().await? else {
        return Ok(());
    };
    let error = classify(
        sqlx::query_scalar::<_, i64>(sql!("SELECT id FROM parents WHERE id = 0"))
            .fetch_one(&test_db.pool)
            .await,
    )
    .await;
    assert_eq!(error.kind(), DbErrorKind::NotFound, "{error:?}");
    Ok(())
}

#[tokio::test]
async fn a_duplicate_key_is_a_unique_violation_naming_its_constraint() -> TestResult {
    let Some((test_db, _db)) = test_db().await? else {
        return Ok(());
    };
    let error = classify(
        sqlx::query(sql!("INSERT INTO parents (id) VALUES (1)"))
            .execute(&test_db.pool)
            .await,
    )
    .await;
    assert_eq!(error.kind(), DbErrorKind::UniqueViolation, "{error:?}");
    assert_eq!(error.constraint(), Some("parents_pkey"));
    // A real conflict, not a lost race: retrying reproduces it.
    assert!(!error.is_lock_contention());
    Ok(())
}

#[tokio::test]
async fn a_missing_parent_is_a_foreign_key_violation_naming_its_constraint() -> TestResult {
    let Some((test_db, _db)) = test_db().await? else {
        return Ok(());
    };
    let error = classify(
        sqlx::query(sql!("INSERT INTO children (id, parent_id) VALUES (1, 99)"))
            .execute(&test_db.pool)
            .await,
    )
    .await;
    assert_eq!(error.kind(), DbErrorKind::ForeignKeyViolation, "{error:?}");
    assert_eq!(error.constraint(), Some("children_parent_id_fkey"));
    Ok(())
}

#[tokio::test]
async fn a_failed_check_is_a_check_violation_naming_its_constraint() -> TestResult {
    let Some((test_db, _db)) = test_db().await? else {
        return Ok(());
    };
    let error = classify(
        sqlx::query(sql!("INSERT INTO parents (id, n) VALUES (3, 0)"))
            .execute(&test_db.pool)
            .await,
    )
    .await;
    assert_eq!(error.kind(), DbErrorKind::CheckViolation, "{error:?}");
    assert_eq!(error.constraint(), Some("parents_n_positive"));
    Ok(())
}

/// A `REPEATABLE READ` transaction updating a row a concurrent transaction
/// changed after its snapshot.
#[tokio::test]
async fn a_concurrent_update_under_repeatable_read_is_a_serialization_failure() -> TestResult {
    let Some((test_db, _db)) = test_db().await? else {
        return Ok(());
    };
    let mut tx = test_db.pool.begin().await?;
    sqlx::query(sql!("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        .execute(&mut *tx)
        .await?;
    sqlx::query(sql!("SELECT n FROM parents WHERE id = 1"))
        .execute(&mut *tx)
        .await?;
    sqlx::query(sql!("UPDATE parents SET n = n + 1 WHERE id = 1"))
        .execute(&test_db.pool)
        .await?;
    let error = classify(
        sqlx::query(sql!("UPDATE parents SET n = n + 1 WHERE id = 1"))
            .execute(&mut *tx)
            .await,
    )
    .await;
    assert_eq!(error.kind(), DbErrorKind::SerializationFailure, "{error:?}");
    assert!(error.is_lock_contention());
    Ok(())
}

/// Two transactions each holding the row the other then waits for.
#[tokio::test]
async fn a_lock_cycle_is_a_deadlock() -> TestResult {
    let Some((test_db, _db)) = test_db().await? else {
        return Ok(());
    };
    let mut first = test_db.pool.begin().await?;
    let mut second = test_db.pool.begin().await?;
    sqlx::query(sql!("UPDATE parents SET n = n + 1 WHERE id = 1"))
        .execute(&mut *first)
        .await?;
    sqlx::query(sql!("UPDATE parents SET n = n + 1 WHERE id = 2"))
        .execute(&mut *second)
        .await?;
    let (first_result, second_result) = tokio::join!(
        sqlx::query(sql!("UPDATE parents SET n = n + 1 WHERE id = 2")).execute(&mut *first),
        sqlx::query(sql!("UPDATE parents SET n = n + 1 WHERE id = 1")).execute(&mut *second),
    );
    // Postgres aborts one of the two to break the cycle; the other proceeds.
    let errors = [first_result.err(), second_result.err()]
        .into_iter()
        .flatten()
        .map(DbError::from)
        .collect::<Vec<_>>();
    let [error] = errors.as_slice() else {
        panic!("exactly one of the two must be aborted: {errors:?}");
    };
    assert_eq!(error.kind(), DbErrorKind::Deadlock, "{error:?}");
    assert!(error.is_lock_contention());
    Ok(())
}

#[tokio::test]
async fn a_held_lock_refused_with_nowait_is_lock_not_available() -> TestResult {
    let Some((test_db, _db)) = test_db().await? else {
        return Ok(());
    };
    let mut holder = test_db.pool.begin().await?;
    sqlx::query(sql!("SELECT id FROM parents WHERE id = 1 FOR UPDATE"))
        .execute(&mut *holder)
        .await?;
    let error = classify(
        sqlx::query(sql!(
            "SELECT id FROM parents WHERE id = 1 FOR UPDATE NOWAIT"
        ))
        .execute(&test_db.pool)
        .await,
    )
    .await;
    assert_eq!(error.kind(), DbErrorKind::LockNotAvailable, "{error:?}");
    assert!(error.is_lock_contention());
    Ok(())
}

#[tokio::test]
async fn a_statement_timeout_is_query_canceled() -> TestResult {
    let Some((test_db, _db)) = test_db().await? else {
        return Ok(());
    };
    let mut tx = test_db.pool.begin().await?;
    sqlx::query(sql!("SET LOCAL statement_timeout = '10ms'"))
        .execute(&mut *tx)
        .await?;
    let error = classify(
        sqlx::query(sql!("SELECT pg_sleep(5)"))
            .execute(&mut *tx)
            .await,
    )
    .await;
    assert_eq!(error.kind(), DbErrorKind::QueryCanceled, "{error:?}");
    Ok(())
}

/// A pool whose one connection is checked out: [`Db::begin`] waits out the
/// acquire timeout.
#[tokio::test]
async fn an_exhausted_pool_is_pool_timed_out() -> TestResult {
    let Some((test_db, _db)) = test_db().await? else {
        return Ok(());
    };
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(100))
        .connect(&test_db.database_url)
        .await?;
    let _held = pool.acquire().await?;
    let error = Db::from(pool.clone())
        .begin()
        .await
        .expect_err("no connection is free");
    assert_eq!(error.kind(), DbErrorKind::PoolTimedOut, "{error:?}");
    Ok(())
}

/// A connection to a port bound but not listening: the socket is refused
/// before any server could answer. Holding the bound socket through the
/// attempt keeps another listener from taking the port. Needs no database.
#[tokio::test]
async fn a_refused_connection_is_io() -> TestResult {
    let reserved = tokio::net::TcpSocket::new_v4()?;
    reserved.bind(([127, 0, 0, 1], 0).into())?;
    let port = reserved.local_addr()?.port();
    let error = classify(
        PgConnectOptions::new()
            .host("127.0.0.1")
            .port(port)
            .connect()
            .await,
    )
    .await;
    drop(reserved);
    assert_eq!(error.kind(), DbErrorKind::Io, "{error:?}");
    assert_eq!(error.sqlstate(), None);
    Ok(())
}

/// A row read as the caller asked when it cannot be: a value of the wrong
/// type, and a column missing by name or by index.
#[tokio::test]
async fn reading_a_row_wrongly_is_decode() -> TestResult {
    let Some((test_db, _db)) = test_db().await? else {
        return Ok(());
    };
    let row = sqlx::query(sql!("SELECT 'x'::text AS name"))
        .fetch_one(&test_db.pool)
        .await?;
    for error in [
        row.try_get::<i64, _>("name"),
        row.try_get::<i64, _>("missing"),
        row.try_get::<i64, _>(1),
    ] {
        let error = classify(error).await;
        assert_eq!(error.kind(), DbErrorKind::Decode, "{error:?}");
        assert_eq!(error.sqlstate(), None);
    }
    // A value failing to decode outside a row, which no statement here
    // reaches, is classified the same way.
    let error = DbError::from(sqlx::Error::Decode("not a value".into()));
    assert_eq!(error.kind(), DbErrorKind::Decode, "{error:?}");
    Ok(())
}

/// The server's code for a failure it reported, kind or not, and none for
/// one it did not.
#[tokio::test]
async fn sqlstate_is_the_servers_code() -> TestResult {
    let Some((test_db, _db)) = test_db().await? else {
        return Ok(());
    };
    let error = classify(
        sqlx::query(sql!("SELECT 'x'::int"))
            .execute(&test_db.pool)
            .await,
    )
    .await;
    assert_eq!(error.kind(), DbErrorKind::Other, "{error:?}");
    assert_eq!(error.sqlstate(), Some("22P02"));
    let error = classify(
        sqlx::query(sql!("INSERT INTO parents (id) VALUES (1)"))
            .execute(&test_db.pool)
            .await,
    )
    .await;
    assert_eq!(error.sqlstate(), Some("23505"));
    assert_eq!(DbError::from(sqlx::Error::RowNotFound).sqlstate(), None);
    Ok(())
}

#[tokio::test]
async fn any_other_failure_is_other() -> TestResult {
    let Some((test_db, _db)) = test_db().await? else {
        return Ok(());
    };
    let error = classify(
        sqlx::query(sql!("SELECT missing_column FROM parents"))
            .execute(&test_db.pool)
            .await,
    )
    .await;
    assert_eq!(error.kind(), DbErrorKind::Other, "{error:?}");
    Ok(())
}

/// Logs read the same, and converting back yields the driver's error intact.
#[tokio::test]
async fn display_and_the_conversion_back_are_the_drivers() -> TestResult {
    let Some((test_db, _db)) = test_db().await? else {
        return Ok(());
    };
    let insert = || sqlx::query(sql!("INSERT INTO parents (id) VALUES (1)"));
    let driver_error = insert()
        .execute(&test_db.pool)
        .await
        .expect_err("duplicate key");
    let error = DbError::from(
        insert()
            .execute(&test_db.pool)
            .await
            .expect_err("duplicate key"),
    );
    assert_eq!(error.to_string(), driver_error.to_string());
    // Its source is the driver error's own source, so the chain repeats no
    // line.
    assert_eq!(
        std::error::Error::source(&error).map(ToString::to_string),
        std::error::Error::source(&driver_error).map(ToString::to_string),
    );

    let back = sqlx::Error::from(error);
    let database = back.as_database_error().expect("a database error");
    assert_eq!(database.code().as_deref(), Some("23505"));
    assert_eq!(database.constraint(), Some("parents_pkey"));
    assert!(matches!(
        sqlx::Error::from(DbError::from(sqlx::Error::RowNotFound)),
        sqlx::Error::RowNotFound
    ));
    Ok(())
}
