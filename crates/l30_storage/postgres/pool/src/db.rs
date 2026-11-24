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

use std::future::Future;
use std::pin::Pin;

use futures_core::Stream;
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
}

/// An open transaction from [`Db::begin`].
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
}

mod sealed {
    pub trait Sealed {}

    impl Sealed for &super::Db {}
    impl Sealed for &mut super::Tx<'_> {}
    impl Sealed for &sqlx::PgPool {}
    impl Sealed for &mut sqlx::PgConnection {}
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
