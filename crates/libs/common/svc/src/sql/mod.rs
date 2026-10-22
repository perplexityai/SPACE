//! Shared SQL helpers for service startup.

use pplx_space_common_core::sql;
use std::num::NonZeroU32;
use std::time::Duration;

use log::LevelFilter;
use pplx_space_common_core::types::DatabaseIdentity;
use sqlx::ConnectOptions as _;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

pub use pplx_space_federated_grant_store as federated_service_grants;
pub use pplx_space_postgres_contention as contention;
pub mod identity;
#[cfg(feature = "migration-checksums")]
pub use pplx_space_migration_checksums as migration_checksums;
pub use pplx_space_node_registration_store as node_registration;
pub use pplx_space_sandbox_quarantine_store as sandbox_quarantine;
pub mod sandbox_residency;
pub use pplx_space_sandbox_routability as sandbox_routability;

const POOL_METRICS_INTERVAL: Duration = Duration::from_secs(10);

const DEFAULT_MAX_CONNECTIONS: u32 = 25;
const DEFAULT_MIN_CONNECTIONS: u32 = 1;
const DEFAULT_ACQUIRE_TIMEOUT_SECS: u64 = 5;
const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 300;
const DEFAULT_MAX_LIFETIME_SECS: u64 = 30 * 60;
const DEFAULT_MAX_LIFETIME_JITTER_FRACTION: f64 = 1.0 / 6.0;

/// Password projections for managed application database identities.
pub const SPACE_DB_USER_PASSWORD_ENV: &str = "SPACE_DB_USER_PASSWORD";
pub const SPACE_DB_CREDS_PASSWORD_ENV: &str = "SPACE_DB_CREDS_PASSWORD";

pub use identity::{
    CredentialsPool, connect_space_user_pool, connect_space_user_pool_lazy,
    connect_space_user_sideline_pool_lazy, managed_database_password,
};

/// Building connection options for a managed database identity failed.
#[derive(Debug, thiserror::Error)]
pub enum DatabaseConnectOptionsError {
    #[error("{env_var} must be set to initialize the {identity} pool")]
    MissingPassword {
        identity: DatabaseIdentity,
        env_var: &'static str,
    },
    #[error("failed to parse DATABASE_URL for the {identity} pool")]
    InvalidDatabaseUrl {
        identity: DatabaseIdentity,
        #[source]
        source: sqlx::Error,
    },
}

/// Build connection options for a managed Postgres identity.
///
/// `DATABASE_URL` contributes only the endpoint: the managed identity
/// replaces the username/password while host, port, database, TLS, and other
/// connection options are preserved. The connecting admin embedded in the
/// URL is never used as a pool login.
///
/// `application_name` (the caller's service name) is stamped on every
/// connection so `pg_stat_activity` and server logs attribute connections to
/// the owning service when debugging pool or server-side saturation.
pub fn database_connect_options(
    database_url: &str,
    application_name: &str,
    identity: DatabaseIdentity,
    password: &str,
) -> Result<PgConnectOptions, DatabaseConnectOptionsError> {
    if password.is_empty() {
        return Err(DatabaseConnectOptionsError::MissingPassword {
            identity,
            env_var: password_env_for_identity(identity),
        });
    }
    let options = database_url
        .parse::<PgConnectOptions>()
        .map_err(|source| DatabaseConnectOptionsError::InvalidDatabaseUrl { identity, source })?
        .application_name(application_name)
        .username(identity.as_str())
        .password(password);
    Ok(options)
}

/// Connect a managed Postgres identity with the shared pool configuration.
///
/// One generic pool knob per setting, set per-Deployment in Helm (env is
/// per-container, so per-service env names add nothing):
/// `POSTGRES_POOL_MAX_CONNECTIONS` (default 25),
/// `POSTGRES_POOL_MIN_CONNECTIONS` (default 1),
/// `POSTGRES_POOL_ACQUIRE_TIMEOUT_SECS` (default 5),
/// `POSTGRES_POOL_IDLE_TIMEOUT_SECS` (default 300),
/// `POSTGRES_POOL_MAX_LIFETIME_SECS` (default 1800),
/// `POSTGRES_POOL_MAX_LIFETIME_JITTER_FRACTION` (default 1/6), and
/// `SPACE_SQLX_QUERY_LOG_LEVEL` (default off).
pub async fn connect_postgres_as(
    database_url: &str,
    application_name: &str,
    identity: DatabaseIdentity,
    password: &str,
) -> Result<PgPool, ConnectPostgresAsError> {
    let options = database_connect_options(database_url, application_name, identity, password)?
        .log_statements(sqlx_query_log_level_from_env());
    postgres_pool_options()
        .connect_with(options)
        .await
        .map_err(ConnectPostgresAsError::Connect)
}

/// Connecting a managed Postgres identity failed.
#[derive(Debug, thiserror::Error)]
pub enum ConnectPostgresAsError {
    #[error(transparent)]
    Options(#[from] DatabaseConnectOptionsError),
    #[error("failed to connect Postgres pool")]
    Connect(#[source] sqlx::Error),
}

/// Prepare a managed Postgres identity pool whose connection ceiling is capped
/// at `max_connections`, ignoring `POSTGRES_POOL_MAX_CONNECTIONS`.
///
/// For a *sideline* workload: one that shares a database with a service's
/// load-bearing pool but must never be able to starve it. A sideline sharing
/// the general pool can do exactly that without ever failing itself — if its
/// statements block (a relation lock taken by DDL is the usual way), each one
/// holds a connection until the role's `statement_timeout` fires, and once the
/// general pool is drained the *load-bearing* queries start failing their
/// acquire even though nothing is wrong with them. A separate small pool turns
/// that cross-workload failure into a self-contained one: the sideline's own
/// acquires fail, its caller degrades, and the general pool is untouched.
///
/// Size it to the sideline's steady-state concurrency, not its peak — the
/// point is the ceiling. `NonZeroU32` because a zero cap is the one value with
/// no useful meaning and a silent failure mode: sqlx validates nothing, so the
/// pool would start, never yield a connection, and burn the full acquire
/// timeout on every request while the caller degraded forever. A cap computed
/// at a call site (`replicas / n`, a parsed env default) can reach zero, so the
/// type refuses it rather than a runtime check nobody writes. Every other knob matches [`connect_postgres_as`], and
/// `min_connections` is clamped to the cap so a large
/// `POSTGRES_POOL_MIN_CONNECTIONS` cannot exceed it.
///
/// `statement_timeout`, when set, is applied per connection as a session
/// setting for this pool alone. That is the piece a connection cap cannot
/// provide: the cap bounds how much of the database's capacity a blocked
/// sideline can hold, but each blocked statement still runs to the *role's*
/// `statement_timeout` (10s for `space_user`), and its caller waits that long.
/// A pool-scoped setting cancels server-side — no abandoned statement, no
/// dropped future, and no effect on any other pool using the same role.
pub fn connect_postgres_lazy_as_capped(
    database_url: &str,
    application_name: &str,
    identity: DatabaseIdentity,
    password: &str,
    max_connections: NonZeroU32,
    statement_timeout: Option<Duration>,
) -> Result<PgPool, DatabaseConnectOptionsError> {
    let max_connections = max_connections.get();
    let options = database_connect_options(database_url, application_name, identity, password)?
        .log_statements(sqlx_query_log_level_from_env());
    let pool_options = postgres_pool_options()
        .max_connections(max_connections)
        .min_connections(min_connections_from_env().min(max_connections));
    let pool_options = match statement_timeout {
        // Replaces the base `after_connect`, which is a single-slot callback in
        // sqlx, so the type-cache warm has to be re-run here rather than added
        // to. Both are per-connection setup, so the cost is unchanged.
        Some(statement_timeout) => {
            let timeout_setting = format!("{}ms", statement_timeout.as_millis());
            pool_options.after_connect(move |conn, _meta| {
                let timeout_setting = timeout_setting.clone();
                Box::pin(async move {
                    // Every base setup step, then the pool-local addition.
                    // `after_connect` is single-slot, so this override has to
                    // re-run the shared setup rather than add to it — going
                    // through `per_connection_setup` is what keeps a future
                    // step from being silently skipped by capped pools.
                    per_connection_setup(conn).await?;
                    // `set_config(..., false)` is session-scoped, which is the
                    // right lifetime for `after_connect`: it holds for every
                    // statement this connection serves. Bound as a parameter
                    // rather than interpolated so the statement stays static.
                    sqlx::query(sql!("SELECT set_config('statement_timeout', $1, false)"))
                        .bind(timeout_setting)
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            })
        }
        None => pool_options,
    };
    Ok(pool_options.connect_lazy_with(options))
}

/// Prepare a managed Postgres identity pool without waiting for a connection.
///
/// Same options and pool knobs as [`connect_postgres_as`]. The minimum pool
/// floor is established in the background, while configuration errors still
/// fail eagerly.
pub fn connect_postgres_lazy_as(
    database_url: &str,
    application_name: &str,
    identity: DatabaseIdentity,
    password: &str,
) -> Result<PgPool, DatabaseConnectOptionsError> {
    let options = database_connect_options(database_url, application_name, identity, password)?
        .log_statements(sqlx_query_log_level_from_env());
    Ok(postgres_pool_options().connect_lazy_with(options))
}

fn postgres_pool_options() -> PgPoolOptions {
    let max_connections = max_connections_from_env();
    let max_lifetime = max_lifetime_from_env();
    let max_lifetime_jitter_fraction = max_lifetime_jitter_fraction_from_env();

    PgPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(min_connections_from_env().min(max_connections))
        .acquire_timeout(acquire_timeout_from_env())
        .idle_timeout(Some(idle_timeout_from_env()))
        // SQLx enforces max_lifetime while acquiring, which can make a cohort
        // of same-aged connections expire on the request path. Retire them
        // after release with jitter instead.
        .max_lifetime(None)
        .after_release(move |_conn, meta| {
            Box::pin(async move {
                Ok(meta.age
                    < jittered_lifetime(max_lifetime, max_lifetime_jitter_fraction, rand::random()))
            })
        })
        .acquire_time_level(LevelFilter::Info)
        .after_connect(|conn, _meta| Box::pin(per_connection_setup(conn)))
}

const fn password_env_for_identity(identity: DatabaseIdentity) -> &'static str {
    match identity {
        DatabaseIdentity::SpaceUser => SPACE_DB_USER_PASSWORD_ENV,
        DatabaseIdentity::SpaceCreds => SPACE_DB_CREDS_PASSWORD_ENV,
    }
}

/// Run a no-op cast query against each custom Postgres enum so sqlx caches
/// its OID + variant table at connection time. Without this, the first hot-path
/// request that touches a custom enum pays ~7ms for three pg_catalog
/// introspection queries (`regtype::oid`, `pg_type`, `pg_enum`) mid-request.
/// Per-connection setup shared by every pool this module builds.
///
/// The single home for anything that must run on a fresh connection. sqlx's
/// `after_connect` holds one callback, so a pool that needs its own step (see
/// [`connect_postgres_lazy_as_capped`]) replaces this hook rather than
/// extending it — and must call this first. Adding a step here reaches every
/// pool; inlining one into a caller's own hook does not.
async fn per_connection_setup(conn: &mut sqlx::PgConnection) -> Result<(), sqlx::Error> {
    warm_type_cache(conn).await
}

async fn warm_type_cache(conn: &mut sqlx::PgConnection) -> Result<(), sqlx::Error> {
    sqlx::query_scalar::<_, Vec<crate::types::ReconcileReason>>(sql!(
        "SELECT ARRAY['sandbox_network_policy_updated'::state_reconciliation_reason]"
    ))
    .fetch_one(conn)
    .await?;
    Ok(())
}

fn sqlx_query_log_level_from_env() -> LevelFilter {
    match std::env::var("SPACE_SQLX_QUERY_LOG_LEVEL") {
        Ok(raw) => parse_level_filter(&raw).unwrap_or_else(|| {
            tracing::warn!(
                raw = %raw,
                default = "off",
                "invalid SPACE_SQLX_QUERY_LOG_LEVEL; using default"
            );
            LevelFilter::Off
        }),
        Err(_) => LevelFilter::Off,
    }
}

fn parse_level_filter(raw: &str) -> Option<LevelFilter> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "off" => Some(LevelFilter::Off),
        "error" => Some(LevelFilter::Error),
        "warn" | "warning" => Some(LevelFilter::Warn),
        "info" => Some(LevelFilter::Info),
        "debug" => Some(LevelFilter::Debug),
        "trace" => Some(LevelFilter::Trace),
        _ => None,
    }
}

fn max_connections_from_env() -> u32 {
    u32::try_from(parse_positive_env_u64(
        "POSTGRES_POOL_MAX_CONNECTIONS",
        u64::from(DEFAULT_MAX_CONNECTIONS),
    ))
    .unwrap_or(DEFAULT_MAX_CONNECTIONS)
}

fn min_connections_from_env() -> u32 {
    u32::try_from(parse_positive_env_u64(
        "POSTGRES_POOL_MIN_CONNECTIONS",
        u64::from(DEFAULT_MIN_CONNECTIONS),
    ))
    .unwrap_or(DEFAULT_MIN_CONNECTIONS)
}

fn acquire_timeout_from_env() -> Duration {
    Duration::from_secs(parse_positive_env_u64(
        "POSTGRES_POOL_ACQUIRE_TIMEOUT_SECS",
        DEFAULT_ACQUIRE_TIMEOUT_SECS,
    ))
}

fn idle_timeout_from_env() -> Duration {
    Duration::from_secs(parse_positive_env_u64(
        "POSTGRES_POOL_IDLE_TIMEOUT_SECS",
        DEFAULT_IDLE_TIMEOUT_SECS,
    ))
}

fn max_lifetime_from_env() -> Duration {
    Duration::from_secs(parse_positive_env_u64(
        "POSTGRES_POOL_MAX_LIFETIME_SECS",
        DEFAULT_MAX_LIFETIME_SECS,
    ))
}

fn max_lifetime_jitter_fraction_from_env() -> f64 {
    const VAR: &str = "POSTGRES_POOL_MAX_LIFETIME_JITTER_FRACTION";

    match std::env::var(VAR) {
        Ok(raw) => match raw.trim().parse::<f64>() {
            Ok(fraction) if (0.0..1.0).contains(&fraction) => fraction,
            _ => {
                tracing::warn!(
                    var = VAR,
                    raw = %raw,
                    default = DEFAULT_MAX_LIFETIME_JITTER_FRACTION,
                    "env var is not a number from 0 (inclusive) to 1 (exclusive); using default"
                );
                DEFAULT_MAX_LIFETIME_JITTER_FRACTION
            }
        },
        Err(_) => DEFAULT_MAX_LIFETIME_JITTER_FRACTION,
    }
}

fn jittered_lifetime(lifetime: Duration, jitter_fraction: f64, sample: f64) -> Duration {
    let factor = 1.0 + jitter_fraction * (sample * 2.0 - 1.0);
    lifetime.mul_f64(factor)
}

fn parse_positive_env_u64(var: &str, default: u64) -> u64 {
    match std::env::var(var) {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(n) if n > 0 => n,
            _ => {
                tracing::warn!(
                    var,
                    raw = %raw,
                    default,
                    "env var is not a positive integer; using default"
                );
                default
            }
        },
        Err(_) => default,
    }
}

/// Spawn a background task that emits Postgres pool-occupancy metrics every
/// 10s, tagged with the pool's managed database identity.
///
/// All metrics are DogStatsD distributions: they aggregate server-side across
/// every replica, so `max:`/percentile queries surface the worst pod without
/// per-pod tags (gauges from replicas sharing a node agent would collapse into
/// one last-write-wins series).
///
/// Successful real-acquire timing comes from SQLx's `sqlx::pool::acquire`
/// events; this reporter reads state only and never consumes pool capacity.
pub fn spawn_postgres_pool_metrics_reporter_for_identity(pool: PgPool, identity: DatabaseIdentity) {
    let max = pool.options().get_max_connections();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(POOL_METRICS_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            interval.tick().await;
            let size = pool.size();
            let idle = pool.num_idle();
            let in_use = size.saturating_sub(idle as u32);
            pplx_space_metrics::db::pool::connections_in_use_for_identity(in_use, identity);
            pplx_space_metrics::db::pool::connections_idle_for_identity(idle, identity);
            if let Some(ratio) = pool_utilization(in_use, max) {
                pplx_space_metrics::db::pool::utilization_for_identity(ratio, identity);
            }
        }
    });
}

/// In-use fraction of the pool cap, or `None` when the cap is zero (a state
/// sqlx does not normally allow, but a 0/0 sample must not emit NaN).
fn pool_utilization(in_use: u32, max: u32) -> Option<f64> {
    (max > 0).then(|| f64::from(in_use) / f64::from(max))
}

#[cfg(test)]
mod tests;
