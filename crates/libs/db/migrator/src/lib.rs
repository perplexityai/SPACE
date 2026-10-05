//! The SPACE schema migrator: the one-shot job that applies every bundled
//! migration set — the control-plane Postgres schema and each node-local
//! `SQLite` store — and converges the control-plane Postgres roles.
//!
//! This crate is the whole job. The `space-db-migrator` binary is a thin
//! `main` that parses flags, initializes logging, and calls [`run`], so a
//! deployment that needs private wiring can supply its own `main` against
//! this library without copying any of the job.
//!
//! It is its own crate rather than a module of a service library because
//! `sqlx::migrate!` embeds every bundled `db/*` directory as static data in
//! whatever crate holds the macro: only the job that applies them should
//! carry that weight, and no long-running service links this.

use pplx_space_common_svc::sql;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};
use rusqlite::Connection;
use sqlx::migrate::{MigrateError, MigrationType, Migrator};
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{PgConnection, PgPool, SqlitePool};

use pplx_space_common_svc::sql::sqlite::{self, SqliteError};

mod baseline_cutover;
pub mod bootstrap_roles;
pub mod migration_retry;
#[cfg(test)]
mod pg_test_support;
pub mod user_password;

static POSTGRES_MIGRATOR: Migrator = sqlx::migrate!("../../../../db/postgres");
use pplx_space_sbxmgr_migrations::SANDBOX_MANAGER_MIGRATOR;
static TEMPLATE_MANAGER_MIGRATOR: Migrator = sqlx::migrate!("../../../../db/template-manager");
static NODE_MANAGER_MIGRATOR: Migrator = sqlx::migrate!("../../../../db/node-manager");
static NODE_CTL_MIGRATOR: Migrator = sqlx::migrate!("../../../../db/node-ctl");
static RUNTIME_MANAGER_MIGRATOR: Migrator = sqlx::migrate!("../../../../db/runtime-manager");
static VOLUME_MANAGER_MIGRATOR: Migrator = sqlx::migrate!("../../../../db/volume-manager");
static BUILD_MANAGER_MIGRATOR: Migrator = sqlx::migrate!("../../../../db/build-manager");
static COMMON_SCHEMA_MIGRATOR: Migrator = sqlx::migrate!("../../../../db/common");

/// The name the migrator logs its startup banner under, and the name of the
/// binary that hosts it.
pub const SERVICE_NAME: &str = "space-db-migrator";

/// Which database the job acts on and what it does to it: the binary's
/// subcommand tree, so a private `main` reuses the whole command surface
/// rather than restating it.
#[derive(Subcommand)]
pub enum Database {
    /// Run control-plane Postgres migrations.
    #[command(alias = "postgres")]
    ControlPlane(ControlPlaneArgs),
    /// Converge control-plane Postgres roles, grants, and object ownership.
    #[command(name = "bootstrap-roles")]
    BootstrapRoles(bootstrap_roles::BootstrapRolesArgs),
    /// Set an existing console user's password and revoke their sessions.
    SetUserPassword(user_password::SetUserPasswordArgs),
    /// Run sandbox-manager `SQLite` migrations.
    #[command(name = "sandbox-manager", alias = "node-local", alias = "sqlite")]
    SandboxManager(SqliteArgs),
    /// Run template-manager `SQLite` migrations.
    #[command(name = "template-manager")]
    TemplateManager(SqliteArgs),
    /// Run node-manager `SQLite` migrations.
    #[command(name = "node-manager")]
    NodeManager(SqliteArgs),
    /// Run node-ctl `SQLite` migrations.
    #[command(name = "node-ctl")]
    NodeCtl(SqliteArgs),
    /// Run runtime-manager `SQLite` migrations.
    #[command(name = "runtime-manager")]
    RuntimeManager(SqliteArgs),
    /// Run volume-manager `SQLite` migrations.
    #[command(name = "volume-manager")]
    VolumeManager(SqliteArgs),
    /// Run build-manager `SQLite` migrations.
    #[command(name = "build-manager")]
    BuildManager(SqliteArgs),
}

#[derive(Parser)]
pub struct ControlPlaneArgs {
    /// Control-plane Postgres connection URI.
    #[arg(long)]
    pub database_url: String,

    /// Confirm destructive reset when command is `reset`.
    #[arg(long)]
    pub confirm_data_loss: bool,

    /// `activeDeadlineSeconds` of the Job running this command, which the
    /// lock-retry window is sized against. The Job's chart passes its own
    /// value in; Kubernetes does not expose the deadline to the container.
    #[arg(
        long,
        env = "SPACE_DB_MIGRATE_JOB_DEADLINE_SECONDS",
        default_value_t = migration_retry::MIGRATION_JOB_ACTIVE_DEADLINE.as_secs()
    )]
    pub job_deadline_seconds: u64,

    /// Migration command to run.
    #[arg(value_enum, default_value_t = Command::Up)]
    pub command: Command,
}

#[derive(Parser)]
pub struct SqliteArgs {
    /// Node-local `SQLite` database path.
    #[arg(long)]
    pub database_path: Option<PathBuf>,

    /// Confirm destructive reset when command is `reset`.
    #[arg(long)]
    pub confirm_data_loss: bool,

    /// Migration command to run.
    #[arg(value_enum, default_value_t = Command::Up)]
    pub command: Command,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum Command {
    Up,
    Down,
    Status,
    Reset,
}

type SqliteDowngradeAdmission =
    fn(&Connection) -> Result<(), pplx_space_sqlite_migrations::MigrationCompatibilityError>;

#[derive(Clone, Copy)]
struct SqliteDatabase {
    name: &'static str,
    default_path: &'static str,
    migrator: &'static Migrator,
    common_schema: bool,
    compatibility_hook: Option<sqlite::MigrationCompatibilityHook>,
    downgrade_admission: Option<SqliteDowngradeAdmission>,
}

/// Everything [`run`] needs from the process that hosts it: the binary's
/// subcommand, already parsed.
///
/// Deliberately not `Debug`: the control-plane subcommands carry a Postgres
/// URI with a password in it.
pub struct DbMigrateRunConfig {
    /// Which database to act on, and what to do to it.
    pub database: Database,
}

impl From<Database> for DbMigrateRunConfig {
    fn from(database: Database) -> Self {
        Self { database }
    }
}

/// Why [`run`] returned without finishing the requested command.
///
/// Variants name the step that failed rather than the kind of failure: a
/// migration job that fails is read by whoever is unblocking a deploy, and
/// which step died is the whole diagnosis. Every SQL variant keeps the
/// driver error as its source.
#[derive(Debug, thiserror::Error)]
pub enum RunDbMigrateError {
    #[error(transparent)]
    BaselineCutover(#[from] baseline_cutover::Error),
    #[error("reset requires --confirm-data-loss")]
    ResetNotConfirmed,
    #[error("migration {version} has no matching .down.sql file")]
    MissingDownMigration { version: i64 },
    #[error(transparent)]
    BootstrapRoles(#[from] bootstrap_roles::BootstrapRolesError),
    #[error(transparent)]
    UserPassword(#[from] user_password::SetUserPasswordError),
    #[error(transparent)]
    Sqlite(#[from] SqliteError),
    #[error(transparent)]
    DowngradeAdmission(#[from] pplx_space_sqlite_migrations::MigrationCompatibilityError),
    #[error("failed to connect to control-plane Postgres: {0}")]
    ConnectPostgres(#[source] sqlx::Error),
    #[error("failed to reconnect to control-plane Postgres after reset: {0}")]
    ReconnectPostgres(#[source] sqlx::Error),
    #[error("failed to apply control-plane Postgres migrations: {0}")]
    ApplyMigrations(#[source] migration_retry::MigrationRunFailed),
    #[error("failed to apply control-plane Postgres migrations after reset: {0}")]
    ApplyMigrationsAfterReset(#[source] migration_retry::MigrationRunFailed),
    #[error("failed to connect to SQLite at {path}: {source}")]
    ConnectSqlite {
        path: PathBuf,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to open SQLite at {path}: {source}")]
    OpenSqlite {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },
    #[error("failed to acquire Postgres connection: {0}")]
    AcquirePostgresConnection(#[source] sqlx::Error),
    #[error("failed to drop Postgres public schema: {0}")]
    DropPublicSchema(#[source] sqlx::Error),
    #[error("failed to recreate Postgres public schema: {0}")]
    CreatePublicSchema(#[source] sqlx::Error),
    #[error("failed to restore Postgres public schema grants: {0}")]
    GrantPublicSchema(#[source] sqlx::Error),
    #[error("failed to create control-plane Postgres _sqlx_migrations table: {0}")]
    CreatePostgresMigrationsTable(#[source] sqlx::Error),
    #[error("failed to create SQLite _sqlx_migrations table: {0}")]
    CreateSqliteMigrationsTable(#[source] sqlx::Error),
    #[error("failed to load control-plane Postgres SQLx migration history: {0}")]
    LoadPostgresHistory(#[source] sqlx::Error),
    #[error("failed to load SQLite SQLx migration history: {0}")]
    LoadSqliteHistory(#[source] sqlx::Error),
    #[error("failed to roll back control-plane Postgres migration: {0}")]
    RollbackPostgres(#[source] MigrateError),
    #[error("failed to roll back SQLite migration: {0}")]
    RollbackSqlite(#[source] MigrateError),
}

/// Run the requested migration command to completion.
///
/// One-shot: there is no shutdown token, because the job either finishes the
/// command or fails. Kubernetes bounds it with `activeDeadlineSeconds`, which
/// the control-plane subcommand is told about so its lock-retry window can be
/// sized against it (see
/// [`migration_retry::MIGRATION_JOB_ACTIVE_DEADLINE`]).
pub async fn run(config: DbMigrateRunConfig) -> Result<(), RunDbMigrateError> {
    match config.database {
        Database::ControlPlane(args) => run_control_plane(args).await,
        Database::BootstrapRoles(args) => Ok(bootstrap_roles::run(args).await?),
        Database::SetUserPassword(args) => Ok(user_password::run(args).await?),
        Database::SandboxManager(args) => run_sqlite(sqlite_sandbox_manager(), args).await,
        Database::TemplateManager(args) => run_sqlite(sqlite_template_manager(), args).await,
        Database::NodeManager(args) => run_sqlite(sqlite_node_manager(), args).await,
        Database::NodeCtl(args) => run_sqlite(sqlite_node_ctl(), args).await,
        Database::RuntimeManager(args) => run_sqlite(sqlite_runtime_manager(), args).await,
        Database::VolumeManager(args) => run_sqlite(sqlite_volume_manager(), args).await,
        Database::BuildManager(args) => run_sqlite(sqlite_build_manager(), args).await,
    }
}

fn sqlite_sandbox_manager() -> SqliteDatabase {
    SqliteDatabase {
        name: "sandbox-manager",
        default_path: pplx_space_common_svc::core::host::linux::paths::SANDBOX_MANAGER_DB,
        migrator: &SANDBOX_MANAGER_MIGRATOR,
        common_schema: true,
        compatibility_hook: Some(pplx_space_sbxmgr_migrations::prepare_migrations),
        downgrade_admission: Some(pplx_space_sbxmgr_migrations::ensure_downgrade_allowed),
    }
}

fn sqlite_template_manager() -> SqliteDatabase {
    SqliteDatabase {
        name: "template-manager",
        default_path: pplx_space_common_svc::core::host::linux::paths::TEMPLATE_MANAGER_DB,
        migrator: &TEMPLATE_MANAGER_MIGRATOR,
        common_schema: true,
        compatibility_hook: None,
        downgrade_admission: None,
    }
}

fn sqlite_node_manager() -> SqliteDatabase {
    SqliteDatabase {
        name: "node-manager",
        default_path: pplx_space_common_svc::core::host::linux::paths::NODE_MANAGER_DB,
        migrator: &NODE_MANAGER_MIGRATOR,
        common_schema: false,
        compatibility_hook: None,
        downgrade_admission: None,
    }
}

fn sqlite_node_ctl() -> SqliteDatabase {
    SqliteDatabase {
        name: "node-ctl",
        default_path: pplx_space_common_svc::core::host::linux::paths::NODE_CTL_DB,
        migrator: &NODE_CTL_MIGRATOR,
        common_schema: false,
        compatibility_hook: None,
        downgrade_admission: None,
    }
}

fn sqlite_runtime_manager() -> SqliteDatabase {
    SqliteDatabase {
        name: "runtime-manager",
        default_path: pplx_space_common_svc::core::host::linux::paths::RUNTIME_MANAGER_DB,
        migrator: &RUNTIME_MANAGER_MIGRATOR,
        common_schema: true,
        compatibility_hook: None,
        downgrade_admission: None,
    }
}

fn sqlite_volume_manager() -> SqliteDatabase {
    SqliteDatabase {
        name: "volume-manager",
        default_path: pplx_space_common_svc::core::host::linux::paths::VOLUME_MANAGER_DB,
        migrator: &VOLUME_MANAGER_MIGRATOR,
        common_schema: true,
        compatibility_hook: None,
        downgrade_admission: None,
    }
}

fn sqlite_build_manager() -> SqliteDatabase {
    SqliteDatabase {
        name: "build-manager",
        default_path: pplx_space_common_svc::core::host::linux::paths::BUILD_MANAGER_DB,
        migrator: &BUILD_MANAGER_MIGRATOR,
        common_schema: true,
        compatibility_hook: None,
        downgrade_admission: None,
    }
}

async fn run_control_plane(args: ControlPlaneArgs) -> Result<(), RunDbMigrateError> {
    ensure_down_migration_pairs(&POSTGRES_MIGRATOR)?;
    let mut pool_options = PgPoolOptions::new().max_connections(1);
    // Up/Down run as space_migrator (assumed via SET ROLE when the
    // connection authenticates as an admin) so migration-created objects get
    // migrator ownership and its default privileges. Reset keeps the
    // connecting identity: it drops the public schema, which the migrator
    // does not own.
    if matches!(args.command, Command::Up | Command::Down) {
        pool_options = pool_options
            .after_connect(|conn, _metadata| Box::pin(bootstrap_roles::assume_migrator_role(conn)));
    }
    let pool = pool_options
        .connect(&args.database_url)
        .await
        .map_err(RunDbMigrateError::ConnectPostgres)?;

    let retry_policy = migration_retry::LockRetryPolicy::for_job_deadline(Duration::from_secs(
        args.job_deadline_seconds,
    ));
    match args.command {
        Command::Up => {
            baseline_cutover::prepare(&pool, &POSTGRES_MIGRATOR).await?;
            apply_control_plane_migrations(&pool, retry_policy, RunDbMigrateError::ApplyMigrations)
                .await?;
        }
        // Deliberately unretried, unlike Up: `undo` takes the same table
        // locks, and down migrations bound them the same way, but a rollback
        // is a supervised operator action with a human present to re-run it.
        Command::Down => rollback_last_postgres(&pool, &POSTGRES_MIGRATOR).await?,
        Command::Status => print_status_postgres(&pool, &POSTGRES_MIGRATOR).await?,
        Command::Reset => {
            if !args.confirm_data_loss {
                return Err(RunDbMigrateError::ResetNotConfirmed);
            }
            reset_postgres_schema(&pool).await?;
            // Re-apply migrations under the same identity rules as Up: the
            // schema drop/create above needs the connecting admin (the
            // migrator does not own the public schema), but the rebuilt
            // objects must be migrator-owned or the reset database lands
            // outside the role model until an extra bootstrap pass. The
            // reset grant of ALL on the schema to PUBLIC lets the migrator
            // create before bootstrap-roles reconverges the tighter grants.
            let migrate_pool = PgPoolOptions::new()
                .max_connections(1)
                .after_connect(|conn, _metadata| {
                    Box::pin(bootstrap_roles::assume_migrator_role(conn))
                })
                .connect(&args.database_url)
                .await
                .map_err(RunDbMigrateError::ReconnectPostgres)?;
            apply_control_plane_migrations(
                &migrate_pool,
                retry_policy,
                RunDbMigrateError::ApplyMigrationsAfterReset,
            )
            .await?;
        }
    }

    Ok(())
}

/// Apply every pending control-plane migration, retrying a lock timeout.
///
/// `failed` names the pass in the reported error, because the reset path runs
/// this twice and "which of the two" is the first thing an operator asks.
async fn apply_control_plane_migrations(
    pool: &PgPool,
    retry_policy: migration_retry::LockRetryPolicy,
    failed: fn(migration_retry::MigrationRunFailed) -> RunDbMigrateError,
) -> Result<(), RunDbMigrateError> {
    migration_retry::run_up(pool, &POSTGRES_MIGRATOR, retry_policy)
        .await
        .map_err(failed)?;
    Ok(())
}

async fn run_sqlite(database: SqliteDatabase, args: SqliteArgs) -> Result<(), RunDbMigrateError> {
    ensure_down_migration_pairs(database.migrator)?;
    if database.common_schema {
        ensure_down_migration_pairs(&COMMON_SCHEMA_MIGRATOR)?;
    }
    let database_path = args
        .database_path
        .unwrap_or_else(|| PathBuf::from(database.default_path));

    match args.command {
        Command::Up => {
            migrate_sqlite_up(database, &database_path).await?;
        }
        Command::Reset => {
            if !args.confirm_data_loss {
                return Err(RunDbMigrateError::ResetNotConfirmed);
            }
            sqlite::remove_database_files(&database_path)?;
            migrate_sqlite_up(database, &database_path).await?;
        }
        Command::Down => {
            rollback_last_sqlite_database(database, &database_path, &COMMON_SCHEMA_MIGRATOR)
                .await?;
        }
        Command::Status => {
            print_status_sqlite_database(database, &database_path).await?;
        }
    }

    Ok(())
}

async fn migrate_sqlite_up(
    database: SqliteDatabase,
    database_path: &Path,
) -> Result<(), RunDbMigrateError> {
    Ok(sqlite::migrate_embedded(
        database.name,
        database_path,
        sqlite::EmbeddedMigrators {
            service: database.migrator,
            common: database.common_schema.then_some(&COMMON_SCHEMA_MIGRATOR),
        },
        database.compatibility_hook,
    )
    .await?)
}

async fn sqlite_pool(database_path: &Path) -> Result<SqlitePool, RunDbMigrateError> {
    let connect_options = SqliteConnectOptions::new()
        .filename(database_path)
        .create_if_missing(true);
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(connect_options)
        .await
        .map_err(|source| RunDbMigrateError::ConnectSqlite {
            path: database_path.to_path_buf(),
            source,
        })
}

async fn reset_postgres_schema(pool: &PgPool) -> Result<(), RunDbMigrateError> {
    sqlx::query(sql!("DROP SCHEMA IF EXISTS public CASCADE"))
        .execute(pool)
        .await
        .map_err(RunDbMigrateError::DropPublicSchema)?;
    sqlx::query(sql!("CREATE SCHEMA public"))
        .execute(pool)
        .await
        .map_err(RunDbMigrateError::CreatePublicSchema)?;
    sqlx::query(sql!("GRANT ALL ON SCHEMA public TO public"))
        .execute(pool)
        .await
        .map_err(RunDbMigrateError::GrantPublicSchema)?;
    Ok(())
}

async fn ensure_sqlx_migrations_table_postgres(pool: &PgPool) -> Result<(), RunDbMigrateError> {
    let mut conn = pool
        .acquire()
        .await
        .map_err(RunDbMigrateError::AcquirePostgresConnection)?;
    ensure_sqlx_migrations_table_postgres_conn(&mut conn).await
}

async fn ensure_sqlx_migrations_table_postgres_conn(
    conn: &mut PgConnection,
) -> Result<(), RunDbMigrateError> {
    sqlx::query(sql!(
        r"
        CREATE TABLE IF NOT EXISTS _sqlx_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
            success BOOLEAN NOT NULL,
            checksum BYTEA NOT NULL,
            execution_time BIGINT NOT NULL
        )
        "
    ))
    .execute(conn)
    .await
    .map_err(RunDbMigrateError::CreatePostgresMigrationsTable)?;
    Ok(())
}

async fn ensure_sqlx_migrations_table_sqlite(pool: &SqlitePool) -> Result<(), RunDbMigrateError> {
    sqlx::query(sql!(
        r"
        CREATE TABLE IF NOT EXISTS _sqlx_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            success BOOLEAN NOT NULL,
            checksum BLOB NOT NULL,
            execution_time BIGINT NOT NULL
        )
        "
    ))
    .execute(pool)
    .await
    .map_err(RunDbMigrateError::CreateSqliteMigrationsTable)?;
    Ok(())
}

async fn applied_versions_postgres(pool: &PgPool) -> Result<Vec<i64>, RunDbMigrateError> {
    sqlx::query_scalar(sql!(
        "SELECT version FROM _sqlx_migrations WHERE success = TRUE ORDER BY version"
    ))
    .fetch_all(pool)
    .await
    .map_err(RunDbMigrateError::LoadPostgresHistory)
}

async fn applied_versions_sqlite(pool: &SqlitePool) -> Result<Vec<i64>, RunDbMigrateError> {
    sqlx::query_scalar(sql!(
        "SELECT version FROM _sqlx_migrations WHERE success = TRUE ORDER BY version"
    ))
    .fetch_all(pool)
    .await
    .map_err(RunDbMigrateError::LoadSqliteHistory)
}

async fn rollback_last_postgres(
    pool: &PgPool,
    migrator: &Migrator,
) -> Result<(), RunDbMigrateError> {
    ensure_sqlx_migrations_table_postgres(pool).await?;
    let applied_versions = applied_versions_postgres(pool).await?;
    rollback_last(migrator, &applied_versions, |target| async move {
        migrator
            .undo(pool, target)
            .await
            .map_err(RunDbMigrateError::RollbackPostgres)
    })
    .await
}

async fn rollback_last_sqlite(
    pool: &SqlitePool,
    migrator: &Migrator,
) -> Result<(), RunDbMigrateError> {
    ensure_sqlx_migrations_table_sqlite(pool).await?;
    let applied_versions = applied_versions_sqlite(pool).await?;
    rollback_last(migrator, &applied_versions, |target| async move {
        migrator
            .undo(pool, target)
            .await
            .map_err(RunDbMigrateError::RollbackSqlite)
    })
    .await
}

async fn rollback_last_sqlite_database(
    database: SqliteDatabase,
    database_path: &Path,
    common_migrator: &Migrator,
) -> Result<(), RunDbMigrateError> {
    if let Some(admit) = database.downgrade_admission {
        let conn = open_sqlite_connection(database_path)?;
        admit(&conn)?;
    }
    let pool = sqlite_pool(database_path).await?;
    ensure_sqlx_migrations_table_sqlite(&pool).await?;

    let baseline = pplx_space_common_svc::sql::baselines::PUBLIC_SQLITE_BASELINE;
    if !database.common_schema {
        return rollback_last_sqlite(&pool, database.migrator).await;
    }

    let service_version = applied_versions_sqlite(&pool).await?.last().copied();
    let mut conn = open_sqlite_connection(database_path)?;
    sqlite::ensure_common_schema_migrations_table(&conn)?;
    if sqlite::applied_common_schema_migrations(&conn)?
        .keys()
        .copied()
        .max()
        .is_some_and(|version| version > baseline && Some(version) > service_version)
    {
        return rollback_last_common_schema(&mut conn, common_migrator);
    }

    rollback_last_sqlite(&pool, database.migrator).await
}

fn open_sqlite_connection(database_path: &Path) -> Result<Connection, RunDbMigrateError> {
    Connection::open(database_path).map_err(|source| RunDbMigrateError::OpenSqlite {
        path: database_path.to_path_buf(),
        source,
    })
}

fn rollback_last_common_schema(
    conn: &mut Connection,
    migrator: &Migrator,
) -> Result<(), RunDbMigrateError> {
    sqlite::ensure_common_schema_migrations_table(conn)?;
    let applied = sqlite::applied_common_schema_migrations(conn)?;
    let Some(current_version) = applied.keys().copied().max() else {
        println!("no applied migrations to roll back");
        return Ok(());
    };

    sqlite::rollback_common_schema_migration(conn, migrator, current_version)?;
    let target_version = applied
        .keys()
        .copied()
        .filter(|version| *version < current_version)
        .max()
        .unwrap_or(0);
    println!("rolled back common schema migration {current_version} to {target_version}");
    Ok(())
}

async fn rollback_last<F, Fut>(
    migrator: &Migrator,
    applied_versions: &[i64],
    undo: F,
) -> Result<(), RunDbMigrateError>
where
    F: FnOnce(i64) -> Fut,
    Fut: std::future::Future<Output = Result<(), RunDbMigrateError>>,
{
    ensure_down_migration_pairs(migrator)?;

    let Some(current_version) = applied_versions.iter().copied().max() else {
        println!("no applied migrations to roll back");
        return Ok(());
    };
    let target_version = applied_versions
        .iter()
        .copied()
        .filter(|version| *version < current_version)
        .max()
        .unwrap_or(0);

    undo(target_version).await?;
    println!("rolled back migration {current_version} to {target_version}");
    Ok(())
}

fn ensure_down_migration_pairs(migrator: &Migrator) -> Result<(), RunDbMigrateError> {
    let up_versions: BTreeSet<i64> = migrator
        .iter()
        .filter(|migration| migration.migration_type == MigrationType::ReversibleUp)
        .map(|migration| migration.version)
        .collect();
    let down_versions: BTreeSet<i64> = migrator
        .iter()
        .filter(|migration| migration.migration_type == MigrationType::ReversibleDown)
        .map(|migration| migration.version)
        .collect();

    if let Some(version) = up_versions.difference(&down_versions).next() {
        return Err(RunDbMigrateError::MissingDownMigration { version: *version });
    }

    Ok(())
}

async fn print_status_postgres(
    pool: &PgPool,
    migrator: &Migrator,
) -> Result<(), RunDbMigrateError> {
    ensure_sqlx_migrations_table_postgres(pool).await?;
    print_status("service", migrator, applied_versions_postgres(pool).await?);
    Ok(())
}

async fn print_status_sqlite_database(
    database: SqliteDatabase,
    database_path: &Path,
) -> Result<(), RunDbMigrateError> {
    let pool = sqlite_pool(database_path).await?;
    ensure_sqlx_migrations_table_sqlite(&pool).await?;

    if database.common_schema {
        let conn = open_sqlite_connection(database_path)?;
        sqlite::ensure_common_schema_migrations_table(&conn)?;
        let applied = sqlite::applied_common_schema_migrations(&conn)?
            .keys()
            .copied()
            .collect::<Vec<_>>();
        print_status("common", &COMMON_SCHEMA_MIGRATOR, applied);
    }

    print_status(
        "service",
        database.migrator,
        applied_versions_sqlite(&pool).await?,
    );
    Ok(())
}

fn print_status(scope: &str, migrator: &Migrator, applied_versions: Vec<i64>) {
    let applied_versions: BTreeSet<i64> = applied_versions.into_iter().collect();
    for migration in migrator
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        let status = if applied_versions.contains(&migration.version) {
            "applied"
        } else {
            "pending"
        };
        println!(
            "{scope}\t{status}\t{}\t{}",
            migration.version,
            migration.description.as_ref()
        );
    }
}

/// SQLSTATE of a server-side failure, or `None` when the error never
/// reached Postgres.
///
/// Borrowed rather than owned: every caller compares it against literals
/// through `as_deref`, so there is nothing to allocate.
pub(crate) fn sqlstate(error: &sqlx::Error) -> Option<Cow<'_, str>> {
    error.as_database_error().and_then(|db| db.code())
}

#[cfg(test)]
mod tests;
