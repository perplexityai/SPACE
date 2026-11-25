//! Store-owned handles on the control-plane database: the pool ([`Db`]), an
//! open transaction ([`Tx`]), and the [`Executor`] a store verb runs on, which
//! is either of them.
//!
//! A crate above the storage layer holds a [`Db`], opens a [`Tx`] when it
//! needs several statements to commit together, and passes either to store
//! verbs. It never names the driver: none of these types exposes it, so a
//! service or a coordinator that calls store verbs needs no `sqlx`
//! dependency.
//!
//! A store verb takes `impl Executor<'_>` and hands the driver's executor to
//! its statement through [`Executor::into_sqlx`]:
//!
//! ```no_run
//! use pplx_space_postgres_pool::{Db, DbError, Executor};
//!
//! // In a store, the one place `sqlx` is named.
//! async fn ready_node_count(executor: impl Executor<'_>) -> Result<i64, DbError> {
//!     let count = sqlx::query_scalar::<_, i64>(
//!         "SELECT COUNT(*)::BIGINT FROM nodes WHERE state = 'ready'",
//!     )
//!     .fetch_one(executor.into_sqlx())
//!     .await?;
//!     Ok(count)
//! }
//!
//! // In a service: the same verb on the pool or in a transaction.
//! async fn caller(db: &Db) -> Result<(), DbError> {
//!     ready_node_count(db).await?;
//!     let mut tx = db.begin().await?;
//!     ready_node_count(&mut tx).await?;
//!     ready_node_count(&mut tx).await?;
//!     tx.commit().await
//! }
//! ```
//!
//! # Configuring a transaction
//!
//! [`Db::begin_snapshot`] opens a read-only `REPEATABLE READ` transaction:
//! every statement in it reads one snapshot, and any write to a
//! non-temporary table fails.
//!
//! Timeouts are set on the open transaction, never on the pool, and last
//! until it commits or rolls back, as `SET LOCAL` does; the connection goes
//! back to the pool with its session values:
//!
//! - [`Tx::set_local_lock_timeout`], [`Tx::set_local_statement_timeout`] and
//!   [`Tx::set_local_idle_in_transaction_timeout`] set `lock_timeout`,
//!   `statement_timeout` and `idle_in_transaction_session_timeout`.
//! - [`Tx::lower_local_lock_timeout`] bounds the lock waits that follow by at
//!   most a duration without raising a shorter bound already in force, and
//!   [`Tx::restore_local_lock_timeout`] puts back the value it replaced, so a
//!   tight bound can cover a few statements rather than the whole
//!   transaction.
//!
//! The setters and the lowering take a [`Duration`] and send it in whole
//! milliseconds, rounded down. Debug builds panic on a duration under 1 ms,
//! and release builds send it as `1ms`, since `0ms` would mean no bound. A
//! duration must be at most 2147483647 ms (about 24.8 days); Postgres can
//! refuse a longer one, which aborts the transaction.
//!
//! A caller still holding the driver's transaction reaches the same methods
//! through [`TxSettings`].
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use pplx_space_postgres_pool::{Db, DbError};
//!
//! async fn caller(db: &Db) -> Result<(), DbError> {
//!     let mut tx = db.begin().await?;
//!     tx.set_local_statement_timeout(Duration::from_secs(5)).await?;
//!     let saved = tx.lower_local_lock_timeout(Duration::from_millis(250)).await?;
//!     // ... statements whose lock waits must stay short ...
//!     tx.restore_local_lock_timeout(saved).await?;
//!     tx.commit().await
//! }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures_core::Stream;
use pplx_space_sql_locator::sql;
use sqlx::postgres::{PgQueryResult, PgRow, PgStatement, PgTypeInfo};
use sqlx::{Describe, Either, Execute, PgConnection, PgPool, Postgres, SqlStr};

use crate::DbError;

/// The control-plane database pool.
///
/// Cheap to clone: clones share one pool. `From<PgPool>` wraps the caller's
/// pool; a pool from this crate's connect functions keeps their pool
/// configuration, per-connection setup, metrics and database identity.
#[derive(Clone, Debug)]
pub struct Db {
    pool: PgPool,
}

/// For the transition while services still hold the pool the connect
/// functions return: wrap it, sharing the pool rather than opening another.
impl From<PgPool> for Db {
    fn from(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl Db {
    /// Begin a transaction on a connection from the pool.
    pub async fn begin(&self) -> Result<Tx<'static>, DbError> {
        Ok(Tx {
            inner: self.pool.begin().await?,
        })
    }

    /// Begin a transaction that reads one snapshot and writes nothing:
    /// `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`. Every statement in
    /// it sees the database as of its first statement, and a write to a
    /// non-temporary table fails with `25006` (`read_only_sql_transaction`).
    pub async fn begin_snapshot(&self) -> Result<Tx<'static>, DbError> {
        Ok(Tx {
            inner: self
                .pool
                .begin_with(sql!("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"))
                .await?,
        })
    }
}

/// An open transaction from [`Db::begin`] or [`Db::begin_snapshot`].
///
/// Pass `&mut tx` to store verbs to run them inside it; a function holding
/// `tx: &mut Tx` passes `&mut *tx`, so each verb borrows it afresh. Dropping
/// it without [`Tx::commit`] rolls it back.
#[derive(Debug)]
pub struct Tx<'c> {
    inner: sqlx::Transaction<'c, Postgres>,
}

impl Tx<'_> {
    /// Commit the transaction.
    pub async fn commit(self) -> Result<(), DbError> {
        Ok(self.inner.commit().await?)
    }

    /// Roll the transaction back.
    pub async fn rollback(self) -> Result<(), DbError> {
        Ok(self.inner.rollback().await?)
    }

    /// Bound each later lock wait in this transaction by `d`, until it ends:
    /// `lock_timeout`, set as `SET LOCAL` does. A wait past it fails with
    /// `55P03` (`lock_not_available`).
    ///
    /// `d` is sent in whole milliseconds, rounded down. Debug builds panic on
    /// a duration under 1 ms, and release builds send it as `1ms`, since
    /// `0ms` would mean no bound. `d` must be at most 2147483647 ms (about
    /// 24.8 days); Postgres can refuse a longer one, which aborts the
    /// transaction.
    pub async fn set_local_lock_timeout(&mut self, d: Duration) -> Result<(), DbError> {
        set_local_lock_timeout(&mut self.inner, d).await
    }

    /// Bound each later statement in this transaction by `d`, until it ends:
    /// `statement_timeout`, set as `SET LOCAL` does. A statement past it is
    /// cancelled with `57014` (`query_canceled`).
    ///
    /// `d` is sent in whole milliseconds, rounded down. Debug builds panic on
    /// a duration under 1 ms, and release builds send it as `1ms`, since
    /// `0ms` would mean no bound. `d` must be at most 2147483647 ms (about
    /// 24.8 days); Postgres can refuse a longer one, which aborts the
    /// transaction.
    pub async fn set_local_statement_timeout(&mut self, d: Duration) -> Result<(), DbError> {
        set_local_statement_timeout(&mut self.inner, d).await
    }

    /// Bound how long this transaction may sit idle between statements by
    /// `d`, until it ends: `idle_in_transaction_session_timeout`, set as
    /// `SET LOCAL` does. Past it the server ends the session.
    ///
    /// `d` is sent in whole milliseconds, rounded down. Debug builds panic on
    /// a duration under 1 ms, and release builds send it as `1ms`, since
    /// `0ms` would mean no bound. `d` must be at most 2147483647 ms (about
    /// 24.8 days); Postgres can refuse a longer one, which aborts the
    /// transaction.
    pub async fn set_local_idle_in_transaction_timeout(
        &mut self,
        d: Duration,
    ) -> Result<(), DbError> {
        set_local_idle_in_transaction_timeout(&mut self.inner, d).await
    }

    /// Bound each later lock wait in this transaction by at most `d`, never
    /// raising a shorter bound already in force for the transaction or the
    /// session. Returns the `lock_timeout` it found, for
    /// [`Tx::restore_local_lock_timeout`]; without that, the bound lasts
    /// until the transaction ends.
    ///
    /// `d` is sent in whole milliseconds, rounded down. Debug builds panic on
    /// a duration under 1 ms, and release builds send it as `1ms`, since
    /// `0ms` would mean no bound. `d` must be at most 2147483647 ms (about
    /// 24.8 days); Postgres can refuse a longer one, which aborts the
    /// transaction.
    pub async fn lower_local_lock_timeout(
        &mut self,
        d: Duration,
    ) -> Result<SavedLockTimeout, DbError> {
        lower_local_lock_timeout(&mut self.inner, d).await
    }

    /// Put back, for the rest of this transaction, the `lock_timeout` that
    /// [`Tx::lower_local_lock_timeout`] replaced. Like any statement, it fails
    /// with `25P02` once a failed statement has aborted the transaction.
    pub async fn restore_local_lock_timeout(
        &mut self,
        saved: SavedLockTimeout,
    ) -> Result<(), DbError> {
        restore_local_lock_timeout(&mut self.inner, saved).await
    }
}

/// The `lock_timeout` [`Tx::lower_local_lock_timeout`] replaced, to hand to
/// [`Tx::restore_local_lock_timeout`].
#[derive(Debug)]
#[must_use = "pass it to `restore_local_lock_timeout`, or the lowered bound lasts until the transaction ends"]
pub struct SavedLockTimeout(String);

/// [`Tx`]'s settings methods, for the transition: a caller still holding an
/// open `sqlx` transaction calls them on it, as `tx.set_local_lock_timeout(d)`
/// on a `sqlx::Transaction` or on the `&mut PgConnection` it derefs to.
///
/// Sealed: only this crate implements it, for [`Tx`] and the driver's
/// connection, so a helper during the transition can take either. The
/// connection implementation, and with it this trait, go when the last such
/// caller converts to [`Tx`], whose own methods stay.
///
/// On a connection with no transaction open, each method returns an error
/// and sets nothing. On a nested `sqlx` transaction (a savepoint), a value
/// set inside it is undone if that savepoint, or one enclosing it, rolls
/// back, and otherwise lasts until the top-level transaction ends.
pub trait TxSettings: sealed::Sealed {
    /// [`Tx::set_local_lock_timeout`].
    fn set_local_lock_timeout(
        &mut self,
        d: Duration,
    ) -> impl Future<Output = Result<(), DbError>> + Send;

    /// [`Tx::set_local_statement_timeout`].
    fn set_local_statement_timeout(
        &mut self,
        d: Duration,
    ) -> impl Future<Output = Result<(), DbError>> + Send;

    /// [`Tx::set_local_idle_in_transaction_timeout`].
    fn set_local_idle_in_transaction_timeout(
        &mut self,
        d: Duration,
    ) -> impl Future<Output = Result<(), DbError>> + Send;

    /// [`Tx::lower_local_lock_timeout`].
    fn lower_local_lock_timeout(
        &mut self,
        d: Duration,
    ) -> impl Future<Output = Result<SavedLockTimeout, DbError>> + Send;

    /// [`Tx::restore_local_lock_timeout`].
    fn restore_local_lock_timeout(
        &mut self,
        saved: SavedLockTimeout,
    ) -> impl Future<Output = Result<(), DbError>> + Send;
}

impl TxSettings for Tx<'_> {
    fn set_local_lock_timeout(
        &mut self,
        d: Duration,
    ) -> impl Future<Output = Result<(), DbError>> + Send {
        set_local_lock_timeout(&mut self.inner, d)
    }

    fn set_local_statement_timeout(
        &mut self,
        d: Duration,
    ) -> impl Future<Output = Result<(), DbError>> + Send {
        set_local_statement_timeout(&mut self.inner, d)
    }

    fn set_local_idle_in_transaction_timeout(
        &mut self,
        d: Duration,
    ) -> impl Future<Output = Result<(), DbError>> + Send {
        set_local_idle_in_transaction_timeout(&mut self.inner, d)
    }

    fn lower_local_lock_timeout(
        &mut self,
        d: Duration,
    ) -> impl Future<Output = Result<SavedLockTimeout, DbError>> + Send {
        lower_local_lock_timeout(&mut self.inner, d)
    }

    fn restore_local_lock_timeout(
        &mut self,
        saved: SavedLockTimeout,
    ) -> impl Future<Output = Result<(), DbError>> + Send {
        restore_local_lock_timeout(&mut self.inner, saved)
    }
}

impl TxSettings for PgConnection {
    fn set_local_lock_timeout(
        &mut self,
        d: Duration,
    ) -> impl Future<Output = Result<(), DbError>> + Send {
        set_local_lock_timeout(self, d)
    }

    fn set_local_statement_timeout(
        &mut self,
        d: Duration,
    ) -> impl Future<Output = Result<(), DbError>> + Send {
        set_local_statement_timeout(self, d)
    }

    fn set_local_idle_in_transaction_timeout(
        &mut self,
        d: Duration,
    ) -> impl Future<Output = Result<(), DbError>> + Send {
        set_local_idle_in_transaction_timeout(self, d)
    }

    fn lower_local_lock_timeout(
        &mut self,
        d: Duration,
    ) -> impl Future<Output = Result<SavedLockTimeout, DbError>> + Send {
        lower_local_lock_timeout(self, d)
    }

    fn restore_local_lock_timeout(
        &mut self,
        saved: SavedLockTimeout,
    ) -> impl Future<Output = Result<(), DbError>> + Send {
        restore_local_lock_timeout(self, saved)
    }
}

/// `d` as a setting value, in whole milliseconds and at least `1ms`: `0ms`
/// would mean no bound.
fn setting_millis(d: Duration) -> String {
    debug_assert!(
        d.as_millis() != 0,
        "a timeout under 1 ms would render as `0ms`, which Postgres reads as no bound: {d:?}"
    );
    let millis = d.as_millis().max(1);
    format!("{millis}ms")
}

/// Outside a transaction, a transaction-local setting would last for its
/// own statement only.
fn require_transaction(connection: &PgConnection) -> Result<(), DbError> {
    if sqlx::Connection::is_in_transaction(connection) {
        Ok(())
    } else {
        Err(DbError::from(sqlx::Error::InvalidArgument(
            "a transaction-local setting needs an open transaction".to_owned(),
        )))
    }
}

async fn set_local_lock_timeout(connection: &mut PgConnection, d: Duration) -> Result<(), DbError> {
    require_transaction(connection)?;
    sqlx::query(sql!("SELECT set_config('lock_timeout', $1, TRUE)"))
        .bind(setting_millis(d))
        .execute(connection)
        .await?;
    Ok(())
}

async fn set_local_statement_timeout(
    connection: &mut PgConnection,
    d: Duration,
) -> Result<(), DbError> {
    require_transaction(connection)?;
    sqlx::query(sql!("SELECT set_config('statement_timeout', $1, TRUE)"))
        .bind(setting_millis(d))
        .execute(connection)
        .await?;
    Ok(())
}

async fn set_local_idle_in_transaction_timeout(
    connection: &mut PgConnection,
    d: Duration,
) -> Result<(), DbError> {
    require_transaction(connection)?;
    sqlx::query(sql!(
        "SELECT set_config('idle_in_transaction_session_timeout', $1, TRUE)"
    ))
    .bind(setting_millis(d))
    .execute(connection)
    .await?;
    Ok(())
}

async fn lower_local_lock_timeout(
    connection: &mut PgConnection,
    d: Duration,
) -> Result<SavedLockTimeout, DbError> {
    require_transaction(connection)?;
    let millis = setting_millis(d);
    let saved: String = sqlx::query_scalar(sql!("SELECT current_setting('lock_timeout')"))
        .fetch_one(&mut *connection)
        .await?;
    // Sets nothing when a bound at least as short is in force; `0` is none.
    sqlx::query(sql!(
        "SELECT set_config('lock_timeout', $1::text, TRUE)
          WHERE current_setting('lock_timeout')::interval = interval '0'
             OR current_setting('lock_timeout')::interval > $1::text::interval"
    ))
    .bind(millis)
    .execute(connection)
    .await?;
    Ok(SavedLockTimeout(saved))
}

async fn restore_local_lock_timeout(
    connection: &mut PgConnection,
    saved: SavedLockTimeout,
) -> Result<(), DbError> {
    require_transaction(connection)?;
    sqlx::query(sql!("SELECT set_config('lock_timeout', $1, TRUE)"))
        .bind(saved.0)
        .execute(connection)
        .await?;
    Ok(())
}

mod sealed {
    pub trait Sealed {}

    impl Sealed for &super::Db {}
    impl Sealed for &mut super::Tx<'_> {}
    impl Sealed for &sqlx::PgPool {}
    impl Sealed for &mut sqlx::PgConnection {}
    impl Sealed for super::Tx<'_> {}
    impl Sealed for sqlx::PgConnection {}
}

/// Where a store verb runs its statements: the pool (`&Db`) or an open
/// transaction (`&mut Tx`).
///
/// Sealed: only this crate implements it. Besides [`Db`] and [`Tx`], it is
/// implemented for the driver's pool and connection for the transition, so a
/// store verb can switch to `impl Executor<'_>` while some of its callers
/// still hold a `sqlx` pool or pass an open `sqlx` transaction as
/// `&mut *tx`. Those two implementations go once no caller needs them.
pub trait Executor<'c>: sealed::Sealed + Send {
    /// The driver's executor, for a store verb's statement. Only stores
    /// call it: [`SqlxExecutor`] is for passing to `sqlx`.
    fn into_sqlx(self) -> SqlxExecutor<'c>;
}

impl<'c> Executor<'c> for &'c Db {
    fn into_sqlx(self) -> SqlxExecutor<'c> {
        SqlxExecutor(SqlxExecutorInner::Pool(&self.pool))
    }
}

impl<'c> Executor<'c> for &'c mut Tx<'_> {
    fn into_sqlx(self) -> SqlxExecutor<'c> {
        SqlxExecutor(SqlxExecutorInner::Connection(&mut self.inner))
    }
}

impl<'c> Executor<'c> for &'c PgPool {
    fn into_sqlx(self) -> SqlxExecutor<'c> {
        SqlxExecutor(SqlxExecutorInner::Pool(self))
    }
}

impl<'c> Executor<'c> for &'c mut PgConnection {
    fn into_sqlx(self) -> SqlxExecutor<'c> {
        SqlxExecutor(SqlxExecutorInner::Connection(self))
    }
}

/// The driver's executor behind an [`Executor`]: the pool or one connection.
///
/// One concrete type rather than a type per [`Executor`] implementation (an
/// associated type). A store verb's future holds it across an await, and the
/// compiler proves such a future `Send`, as `tokio::spawn` requires, for every
/// lifetime its borrows could have. It can do that for a concrete type, as for
/// `&mut PgConnection` itself, but not for an associated type of an
/// implementation that holds only for related lifetimes, as the one for
/// `&'c mut Tx` must: a verb run on a borrowed `Tx` inside a spawned task
/// would then not compile.
#[derive(Debug)]
pub struct SqlxExecutor<'c>(SqlxExecutorInner<'c>);

#[derive(Debug)]
enum SqlxExecutorInner<'c> {
    Pool(&'c PgPool),
    Connection(&'c mut PgConnection),
}

type PgRowStream<'e, T> = Pin<Box<dyn Stream<Item = Result<T, sqlx::Error>> + Send + 'e>>;
type PgFuture<'e, T> = Pin<Box<dyn Future<Output = Result<T, sqlx::Error>> + Send + 'e>>;

// Each method is one the pool and the connection implement themselves; the
// rest are the trait's defaults built on these, as they are for both.
impl<'c> sqlx::Executor<'c> for SqlxExecutor<'c> {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(self, query: E) -> PgRowStream<'e, Either<PgQueryResult, PgRow>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Postgres>,
    {
        match self.0 {
            SqlxExecutorInner::Pool(pool) => pool.fetch_many(query),
            SqlxExecutorInner::Connection(connection) => connection.fetch_many(query),
        }
    }

    fn fetch_optional<'e, 'q: 'e, E>(self, query: E) -> PgFuture<'e, Option<PgRow>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Postgres>,
    {
        match self.0 {
            SqlxExecutorInner::Pool(pool) => pool.fetch_optional(query),
            SqlxExecutorInner::Connection(connection) => connection.fetch_optional(query),
        }
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [PgTypeInfo],
    ) -> PgFuture<'e, PgStatement>
    where
        'c: 'e,
    {
        match self.0 {
            SqlxExecutorInner::Pool(pool) => pool.prepare_with(sql, parameters),
            SqlxExecutorInner::Connection(connection) => connection.prepare_with(sql, parameters),
        }
    }

    fn describe<'e>(self, sql: SqlStr) -> PgFuture<'e, Describe<Postgres>>
    where
        'c: 'e,
    {
        match self.0 {
            SqlxExecutorInner::Pool(pool) => pool.describe(sql),
            SqlxExecutorInner::Connection(connection) => connection.describe(sql),
        }
    }
}

#[cfg(test)]
mod tests;
