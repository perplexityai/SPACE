//! Managed role-backed Postgres pools for long-running SPACE services.
//!
//! The application tier connects only as the managed logins: `space_user`
//! for ordinary service queries and `space_creds` for the credential-fenced
//! lane. `DATABASE_URL` supplies the endpoint (host, port, database, TLS);
//! the managed login replaces its username/password. There is no fallback to
//! the connecting admin — a missing password projection fails startup.

use std::num::NonZeroU32;
use std::time::Duration;

use pplx_space_db_identity::DatabaseIdentity;
use sqlx::PgPool;

use super::{
    ConnectPostgresAsError, DatabaseConnectOptionsError, DbOptions, connect_postgres_as,
    connect_postgres_lazy_as, password_env_for_identity,
    spawn_postgres_pool_metrics_reporter_for_identity,
};

/// Read the projected password for a managed identity, failing when absent.
pub fn managed_database_password(
    identity: DatabaseIdentity,
) -> Result<String, DatabaseConnectOptionsError> {
    let env_var = password_env_for_identity(identity);
    managed_password(identity, std::env::var(env_var).ok())
}

/// Empty values count as absent so a blank env projection cannot silently
/// route a managed login's traffic through a password Postgres will refuse.
fn managed_password(
    identity: DatabaseIdentity,
    password: Option<String>,
) -> Result<String, DatabaseConnectOptionsError> {
    password.filter(|password| !password.is_empty()).ok_or(
        DatabaseConnectOptionsError::MissingPassword {
            identity,
            env_var: password_env_for_identity(identity),
        },
    )
}

/// Connect a service's general-purpose pool as `space_user`.
///
/// Reads `SPACE_DB_USER_PASSWORD`, verifies the connection eagerly so a
/// misconfigured deployment fails at startup rather than on the first
/// request, and spawns the identity-tagged pool metrics reporter.
pub async fn connect_space_user_pool(
    database_url: &str,
    application_name: &str,
) -> Result<PgPool, ConnectPostgresAsError> {
    let identity = DatabaseIdentity::SpaceUser;
    let password = managed_database_password(identity)?;
    let pool = connect_postgres_as(database_url, application_name, identity, &password).await?;
    spawn_postgres_pool_metrics_reporter_for_identity(pool.clone(), identity);
    Ok(pool)
}

/// Prepare a service's general-purpose `space_user` pool without connecting;
/// the first acquire opens the connection.
///
/// Reads `SPACE_DB_USER_PASSWORD` (missing still fails eagerly) and spawns
/// the identity-tagged pool metrics reporter.
pub fn connect_space_user_pool_lazy(
    database_url: &str,
    application_name: &str,
) -> Result<PgPool, DatabaseConnectOptionsError> {
    let identity = DatabaseIdentity::SpaceUser;
    let password = managed_database_password(identity)?;
    let pool = connect_postgres_lazy_as(
        database_url,
        application_name,
        identity,
        &password,
        DbOptions::default(),
    )?;
    spawn_postgres_pool_metrics_reporter_for_identity(pool.clone(), identity);
    Ok(pool)
}

/// The settings of a *sideline* pool: one that shares a database with a
/// service's load-bearing pool but must never be able to starve it. A
/// sideline sharing the general pool can do exactly that without ever failing
/// itself — if its statements block (a relation lock taken by DDL is the usual
/// way), each one holds a connection until the role's `statement_timeout`
/// fires, and once the general pool is drained the *load-bearing* queries
/// start failing their acquire even though nothing is wrong with them. A
/// separate small pool turns that cross-workload failure into a
/// self-contained one: the sideline's own acquires fail, its caller degrades,
/// and the general pool is untouched.
///
/// [`DbOptions`] with the cap required: a sideline without one is not a
/// sideline.
#[derive(Clone, Debug)]
pub struct SidelineOptions {
    /// Connection ceiling, regardless of `POSTGRES_POOL_MAX_CONNECTIONS`.
    ///
    /// Size it to the sideline's steady-state concurrency, not its peak — the
    /// point is the ceiling. `NonZeroU32` because a zero cap is the one value
    /// with no useful meaning and a silent failure mode: sqlx validates
    /// nothing, so the pool would start, never yield a connection, and burn the
    /// full acquire timeout on every request while the caller degraded forever.
    /// A cap computed at a call site (`replicas / n`, a parsed env default) can
    /// reach zero, so the type refuses it rather than a runtime check nobody
    /// writes.
    pub max_connections: NonZeroU32,
    /// See [`DbOptions::statement_timeout`]. Set it unless the sideline
    /// genuinely may run long. The connection cap bounds how much database
    /// capacity a blocked sideline can hold, but not how long its caller
    /// waits — that is the role's `statement_timeout`, 10s for `space_user`.
    /// The two settings answer different halves of the problem and a
    /// request-path sideline wants both.
    pub statement_timeout: Option<Duration>,
}

impl From<SidelineOptions> for DbOptions {
    fn from(options: SidelineOptions) -> Self {
        let SidelineOptions {
            max_connections,
            statement_timeout,
        } = options;
        Self {
            max_connections: Some(max_connections),
            statement_timeout,
        }
    }
}

/// Prepare a small `space_user` pool for a sideline workload (see
/// [`SidelineOptions`]), capped at `options.max_connections` regardless of
/// `POSTGRES_POOL_MAX_CONNECTIONS`.
///
/// Deliberately does **not** spawn the pool metrics reporter, unlike
/// [`connect_space_user_pool_lazy`]. Those metrics are tagged by
/// `database_identity` alone, and a sideline pool is the same `space_user`
/// identity as the general pool — so it would emit into the same series.
/// `db.pool.connections_{in_use,idle}` would merely blend, but
/// `db.pool.utilization` is a ratio against each pool's *own*
/// `max_connections`: a sideline at 2-of-3 reports 0.67 while the general pool
/// sits idle. Aggregating that ratio by service without distinguishing the
/// pools can manufacture saturation alerts for an intentionally small sideline.
///
/// Observe a sideline through its caller's own degradation signal instead: an
/// exhausted sideline pool fails its acquire, which the caller sees as an
/// ordinary query error and reports as a degraded outcome. Give it a
/// distinguishing metric of its own before adding one here.
pub fn connect_space_user_sideline_pool_lazy(
    database_url: &str,
    application_name: &str,
    options: SidelineOptions,
) -> Result<PgPool, DatabaseConnectOptionsError> {
    let identity = DatabaseIdentity::SpaceUser;
    let password = managed_database_password(identity)?;
    connect_postgres_lazy_as(
        database_url,
        application_name,
        identity,
        &password,
        options.into(),
    )
}

/// Pool pinned to `space_creds`, the login holding grants on the
/// credential-fenced tables.
///
/// The credential tables (`user_credentials`, `org_credentials`,
/// `org_master_keys`, `snapshot_storage_location_encrypted_fields`) are
/// deliberately absent from `space_user`'s grants, so statements against
/// them must never run on a service's general pool. This newtype keeps the
/// two lanes distinct in signatures: a callsite that needs the fenced lane
/// asks for a [`CredentialsPool`], and passing the general pool there does
/// not compile.
///
/// Transactions that interleave fenced and unfenced statements (for example
/// the org-row lock serializing master-key writers) must `begin()` on this
/// pool so every statement runs on one connection under one login;
/// `space_creds` holds read/write DML on all tables, so such transactions
/// are grant-complete here.
#[derive(Clone, Debug)]
pub struct CredentialsPool {
    pool: PgPool,
}

impl CredentialsPool {
    /// Connect the fenced lane as `space_creds`.
    ///
    /// Reads `SPACE_DB_CREDS_PASSWORD`, verifies the connection eagerly, and
    /// spawns the identity-tagged pool metrics reporter.
    pub async fn connect(
        database_url: &str,
        application_name: &str,
    ) -> Result<Self, ConnectPostgresAsError> {
        let identity = DatabaseIdentity::SpaceCreds;
        let password = managed_database_password(identity)?;
        let pool = connect_postgres_as(database_url, application_name, identity, &password).await?;
        spawn_postgres_pool_metrics_reporter_for_identity(pool.clone(), identity);
        Ok(Self { pool })
    }

    /// Prepare the fenced lane as `space_creds` without connecting; the
    /// first acquire opens the connection.
    ///
    /// Reads `SPACE_DB_CREDS_PASSWORD` (missing still fails eagerly) and
    /// spawns the identity-tagged pool metrics reporter.
    pub fn connect_lazy(
        database_url: &str,
        application_name: &str,
    ) -> Result<Self, DatabaseConnectOptionsError> {
        let identity = DatabaseIdentity::SpaceCreds;
        let password = managed_database_password(identity)?;
        let pool = connect_postgres_lazy_as(
            database_url,
            application_name,
            identity,
            &password,
            DbOptions::default(),
        )?;
        spawn_postgres_pool_metrics_reporter_for_identity(pool.clone(), identity);
        Ok(Self { pool })
    }

    /// Fenced lane over a caller-owned pool whose login already holds the
    /// credential-table grants.
    ///
    /// For single-pool callers, especially tests running against a local
    /// Postgres superuser; production startup uses [`Self::connect`].
    #[must_use]
    pub const fn shared(pool: PgPool) -> Self {
        Self { pool }
    }

    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[cfg(test)]
mod tests;
