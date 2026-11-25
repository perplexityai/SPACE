//! [`Db::begin_snapshot`] and the transaction-local settings, read back from
//! Postgres inside the transaction and on its connection after it ends.

use std::time::{Duration, Instant};

use pplx_space_db_test_fixtures::{TestDb, try_raw_pg_test_db_with_max_connections};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::{Db, DbError, DbErrorKind, Executor, Tx, TxSettings};

/// A test database with one `items` table, and a [`Db`] on its pool.
async fn db_with_items() -> TestResult<Option<(TestDb, Db)>> {
    let Some(test_db) = try_raw_pg_test_db_with_max_connections(4).await? else {
        return Ok(None);
    };
    sqlx::query(sql!("CREATE TABLE items (id BIGINT PRIMARY KEY)"))
        .execute(&test_db.pool)
        .await?;
    sqlx::query(sql!("INSERT INTO items (id) VALUES (1)"))
        .execute(&test_db.pool)
        .await?;
    let db = Db::from(test_db.pool.clone());
    Ok(Some((test_db, db)))
}

/// A pool of one connection, so a statement after a transaction runs on the
/// transaction's connection and reads its session values.
async fn one_connection_pool(test_db: &TestDb) -> TestResult<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&test_db.database_url)
        .await?;
    Ok(pool)
}

async fn setting(executor: impl Executor<'_>, name: &str) -> Result<String, DbError> {
    let value = sqlx::query_scalar(sql!("SELECT current_setting($1)"))
        .bind(name)
        .fetch_one(executor.into_sqlx())
        .await?;
    Ok(value)
}

async fn set_session(db: &Db, name: &str, value: &str) -> Result<(), DbError> {
    sqlx::query(sql!("SELECT set_config($1, $2, FALSE)"))
        .bind(name)
        .bind(value)
        .execute(db.into_sqlx())
        .await?;
    Ok(())
}

async fn set_local(tx: &mut Tx<'_>, name: &str, value: &str) -> Result<(), DbError> {
    sqlx::query(sql!("SELECT set_config($1, $2, TRUE)"))
        .bind(name)
        .bind(value)
        .execute(tx.into_sqlx())
        .await?;
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum Timeout {
    Lock,
    Statement,
    IdleInTransaction,
}

impl Timeout {
    const ALL: [Self; 3] = [Self::Lock, Self::Statement, Self::IdleInTransaction];

    fn setting(self) -> &'static str {
        match self {
            Self::Lock => "lock_timeout",
            Self::Statement => "statement_timeout",
            Self::IdleInTransaction => "idle_in_transaction_session_timeout",
        }
    }

    async fn set(self, tx: &mut Tx<'_>, d: Duration) -> Result<(), DbError> {
        match self {
            Self::Lock => tx.set_local_lock_timeout(d).await,
            Self::Statement => tx.set_local_statement_timeout(d).await,
            Self::IdleInTransaction => tx.set_local_idle_in_transaction_timeout(d).await,
        }
    }
}

/// Each setter changes its setting for the transaction only: once it commits
/// or rolls back, its connection reads the session value again.
#[tokio::test]
async fn each_setter_lasts_until_the_transaction_ends() -> TestResult {
    let Some((test_db, _db)) = db_with_items().await? else {
        return Ok(());
    };
    let db = Db::from(one_connection_pool(&test_db).await?);
    for timeout in Timeout::ALL {
        let name = timeout.setting();
        set_session(&db, name, "43210ms").await?;
        for commit in [true, false] {
            let mut tx = db.begin().await?;
            timeout.set(&mut tx, Duration::from_millis(98_765)).await?;
            assert_eq!(setting(&mut tx, name).await?, "98765ms", "{timeout:?}");
            if commit {
                tx.commit().await?;
            } else {
                tx.rollback().await?;
            }
            assert_eq!(
                setting(&db, name).await?,
                "43210ms",
                "{timeout:?} after commit: {commit}"
            );
        }
    }
    Ok(())
}

/// `lower_local_lock_timeout` lowers no bound or a longer one, keeps a shorter
/// one from the session or the transaction, and
/// `restore_local_lock_timeout` puts back whatever it found.
#[tokio::test]
async fn lowering_the_lock_timeout_never_raises_it_and_restores_it() -> TestResult {
    struct Case {
        name: &'static str,
        session: &'static str,
        local: Option<&'static str>,
        lower_to: Duration,
        lowered: &'static str,
        restored: &'static str,
    }
    let cases = [
        Case {
            name: "no bound",
            session: "0",
            local: None,
            lower_to: Duration::from_millis(250),
            lowered: "250ms",
            restored: "0",
        },
        Case {
            name: "a longer bound",
            session: "0",
            local: Some("5s"),
            lower_to: Duration::from_millis(250),
            lowered: "250ms",
            restored: "5s",
        },
        Case {
            name: "a shorter session bound",
            session: "500ms",
            local: None,
            lower_to: Duration::from_millis(2500),
            lowered: "500ms",
            restored: "500ms",
        },
        Case {
            name: "a shorter transaction bound",
            session: "0",
            local: Some("500ms"),
            lower_to: Duration::from_millis(2500),
            lowered: "500ms",
            restored: "500ms",
        },
    ];

    let Some((test_db, _db)) = db_with_items().await? else {
        return Ok(());
    };
    let db = Db::from(one_connection_pool(&test_db).await?);
    for case in cases {
        set_session(&db, "lock_timeout", case.session).await?;
        let mut tx = db.begin().await?;
        if let Some(local) = case.local {
            set_local(&mut tx, "lock_timeout", local).await?;
        }
        let saved = tx.lower_local_lock_timeout(case.lower_to).await?;
        assert_eq!(
            setting(&mut tx, "lock_timeout").await?,
            case.lowered,
            "from {}",
            case.name
        );
        tx.restore_local_lock_timeout(saved).await?;
        assert_eq!(
            setting(&mut tx, "lock_timeout").await?,
            case.restored,
            "from {}",
            case.name
        );
        tx.rollback().await?;
    }
    Ok(())
}

/// A wait for a row another connection holds ends with `55P03` at the bound.
#[tokio::test]
async fn the_lock_timeout_bounds_a_lock_wait() -> TestResult {
    let Some((_test_db, db)) = db_with_items().await? else {
        return Ok(());
    };
    let mut holder = db.begin().await?;
    sqlx::query(sql!("SELECT id FROM items WHERE id = 1 FOR UPDATE"))
        .execute(holder.into_sqlx())
        .await?;

    let bound = Duration::from_millis(200);
    let mut tx = db.begin().await?;
    tx.set_local_lock_timeout(bound).await?;
    let started = Instant::now();
    // Far past the bound: without it the wait would last until `holder` ends.
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        sqlx::query(sql!("SELECT id FROM items WHERE id = 1 FOR UPDATE"))
            .execute((&mut tx).into_sqlx()),
    )
    .await
    .expect("lock_timeout must end the wait");
    let waited = started.elapsed();

    let error = DbError::from(result.expect_err("the row is locked"));
    assert_eq!(error.kind(), DbErrorKind::LockNotAvailable, "{error:?}");
    assert!(
        waited >= bound,
        "gave up after {waited:?}, before {bound:?}"
    );
    holder.rollback().await?;
    Ok(())
}

/// A snapshot transaction is `REPEATABLE READ` and read-only, and a row
/// committed elsewhere between two of its reads is invisible to the second.
#[tokio::test]
async fn a_snapshot_reads_one_snapshot() -> TestResult {
    async fn count(executor: impl Executor<'_>) -> Result<i64, DbError> {
        let count = sqlx::query_scalar(sql!("SELECT COUNT(*)::BIGINT FROM items"))
            .fetch_one(executor.into_sqlx())
            .await?;
        Ok(count)
    }

    let Some((_test_db, db)) = db_with_items().await? else {
        return Ok(());
    };
    let mut tx = db.begin_snapshot().await?;
    assert_eq!(
        setting(&mut tx, "transaction_isolation").await?,
        "repeatable read"
    );
    assert_eq!(setting(&mut tx, "transaction_read_only").await?, "on");
    assert_eq!(count(&mut tx).await?, 1);
    sqlx::query(sql!("INSERT INTO items (id) VALUES (2)"))
        .execute(db.into_sqlx())
        .await?;
    assert_eq!(count(&mut tx).await?, 1);
    tx.commit().await?;
    assert_eq!(count(&db).await?, 2);
    Ok(())
}

#[tokio::test]
async fn a_snapshot_refuses_writes() -> TestResult {
    let Some((_test_db, db)) = db_with_items().await? else {
        return Ok(());
    };
    let mut tx = db.begin_snapshot().await?;
    let error = sqlx::query(sql!("INSERT INTO items (id) VALUES (2)"))
        .execute(tx.into_sqlx())
        .await
        .expect_err("a read-only transaction refuses writes");
    let code = error.as_database_error().and_then(|error| error.code());
    assert_eq!(code.as_deref(), Some("25006"), "{error:?}");
    Ok(())
}

/// The transition implementation: a caller holding the driver's transaction
/// calls the same setter on it.
#[tokio::test]
async fn a_setter_runs_on_the_drivers_transaction() -> TestResult {
    let Some((test_db, _db)) = db_with_items().await? else {
        return Ok(());
    };
    let pool = one_connection_pool(&test_db).await?;
    let db = Db::from(pool.clone());
    set_session(&db, "statement_timeout", "43210ms").await?;
    let mut tx = pool.begin().await?;
    tx.set_local_statement_timeout(Duration::from_millis(98_765))
        .await?;
    assert_eq!(setting(&mut *tx, "statement_timeout").await?, "98765ms");
    tx.commit().await?;
    assert_eq!(setting(&db, "statement_timeout").await?, "43210ms");
    Ok(())
}

/// On a connection with no transaction open, a setter refuses rather than set
/// a value that would last for its own statement only.
#[tokio::test]
async fn a_setter_refuses_a_connection_outside_a_transaction() -> TestResult {
    let Some((test_db, _db)) = db_with_items().await? else {
        return Ok(());
    };
    let mut connection = test_db.pool.acquire().await?;
    let result = connection
        .set_local_lock_timeout(Duration::from_millis(250))
        .await;
    assert!(result.is_err(), "{result:?}");
    Ok(())
}
