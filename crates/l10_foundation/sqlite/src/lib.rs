//! Shared helpers that open, migrate and maintain a service-owned SQLite
//! database by path, for the node services and the migrator that own one.
//!
//! They live in their own crate so that a crate that must not touch a
//! service's database can leave them out of its dependencies. `cargo arch`'s `layers.storage_access` lets only
//! stores, storage helpers and the crates its `[layers.storage_access]`
//! baseline lists have a normal or build dependency on this crate, and that
//! baseline only shrinks.

pub mod baselines;
pub mod runtime_manager;
mod template_manager;

use pplx_space_migration_checksums::{AcceptedChecksums, ForwardCompatible};
use pplx_space_sql_locator::sql;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::Connection;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
#[cfg(test)]
use sqlx::SqlitePool;
use sqlx::migrate::{Migration, Migrator};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tracing::Instrument as _;

pub type SqliteResult<T> = Result<T, SqliteError>;

/// An error while opening, migrating, or maintaining a service-owned SQLite store.
///
/// The concrete database and filesystem errors remain available through the
/// standard error source chain without making this shared helper's public API
/// depend on every backend error type.
#[derive(Debug, thiserror::Error)]
#[error("{context}{source}")]
pub struct SqliteError {
    context: String,
    #[source]
    source: Box<dyn StdError + Send + Sync + 'static>,
}

impl SqliteError {
    fn context(context: impl Into<String>, source: impl StdError + Send + Sync + 'static) -> Self {
        Self {
            context: format!("{}: ", context.into()),
            source: Box::new(source),
        }
    }

    fn message(message: impl Into<String>) -> Self {
        Self {
            context: String::new(),
            source: Box::new(SqliteErrorMessage(message.into())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct SqliteErrorMessage(String);

trait SqliteContext<T> {
    fn context(self, context: impl Display) -> SqliteResult<T>;
    fn with_context(self, context: impl FnOnce() -> String) -> SqliteResult<T>;
}

impl<T, E> SqliteContext<T> for Result<T, E>
where
    E: StdError + Send + Sync + 'static,
{
    fn context(self, context: impl Display) -> SqliteResult<T> {
        self.map_err(|source| SqliteError::context(context.to_string(), source))
    }

    fn with_context(self, context: impl FnOnce() -> String) -> SqliteResult<T> {
        self.map_err(|source| SqliteError::context(context(), source))
    }
}

impl<T> SqliteContext<T> for Option<T> {
    fn context(self, context: impl Display) -> SqliteResult<T> {
        self.ok_or_else(|| SqliteError::message(context.to_string()))
    }

    fn with_context(self, context: impl FnOnce() -> String) -> SqliteResult<T> {
        self.ok_or_else(|| SqliteError::message(context()))
    }
}

#[derive(Debug, Clone)]
pub struct RusqliteOpenOptions {
    pub busy_timeout: Duration,
    pub foreign_keys: bool,
    pub synchronous_normal: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct SqliteU64(pub u64);

impl rusqlite::ToSql for SqliteU64 {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        let value = i64::try_from(self.0)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        Ok(rusqlite::types::ToSqlOutput::Owned(
            rusqlite::types::Value::Integer(value),
        ))
    }
}

impl rusqlite::types::FromSql for SqliteU64 {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let value = <i64 as rusqlite::types::FromSql>::column_result(value)?;
        let value = u64::try_from(value)
            .map_err(|error| rusqlite::types::FromSqlError::Other(Box::new(error)))?;
        Ok(Self(value))
    }
}

impl Default for RusqliteOpenOptions {
    fn default() -> Self {
        Self {
            busy_timeout: Duration::from_secs(5),
            foreign_keys: false,
            synchronous_normal: true,
        }
    }
}

/// Embedded migration sets for a service-owned SQLite store and any shared
/// common schema it consumes.
#[derive(Clone, Copy)]
pub struct EmbeddedMigrators<'a> {
    pub service: &'a Migrator,
    pub common: Option<&'a Migrator>,
}

/// Apply shared schema and service migrations after validating both histories.
/// A recorded migration neither catalog bundles is refused unless
/// `db/forward-compatible-migrations.toml` lists it with that checksum; a
/// listed one stays as it is.
///
/// Daemons call this after acquiring their database ownership mutex.
pub async fn migrate_embedded(
    db_name: &str,
    db_path: &Path,
    migrators: EmbeddedMigrators<'_>,
) -> SqliteResult<()> {
    let checksums = AcceptedChecksums::embedded().context("invalid migration checksum manifest")?;
    let forward =
        ForwardCompatible::embedded().context("invalid forward-compatible migration manifest")?;
    migrate_once(db_name, db_path, migrators, &checksums, &forward)
        .await
        .with_context(|| {
            format!(
                "failed to migrate {db_name} SQLite at {}",
                db_path.display()
            )
        })
}

/// Use the existing shared history without moving or rewriting its rows.
pub fn common_migrator(migrator: &Migrator) -> Migrator {
    Migrator {
        migrations: migrator.migrations.clone(),
        table_name: "_space_common_schema_migrations".into(),
        ..Migrator::DEFAULT
    }
}

/// What one catalog's history table records.
#[derive(Clone, Debug, Default)]
struct RecordedHistory {
    /// At least one migration.
    applied: bool,
    /// Listed forward-compatible migrations the catalog doesn't bundle, as
    /// `(version, recorded checksum)`.
    forward: Vec<(i64, Vec<u8>)>,
}

/// Fails if SQLx ever applies it: a placeholder stands only for a history row
/// already recorded with its checksum, which SQLx compares and leaves.
const FORWARD_PLACEHOLDER_SQL: &str =
    "SELECT forward_compatible_migration_recorded_by_a_newer_release_is_never_applied_here;";

/// `migrator` plus a placeholder for each accepted forward-compatible row,
/// carrying the recorded checksum. SQLx then leaves those rows alone and still
/// refuses any other recorded version it doesn't bundle, such as one a
/// concurrent runner recorded after validation.
fn with_forward_placeholders(migrator: &Migrator, forward: &[(i64, Vec<u8>)]) -> Migrator {
    use sqlx::SqlSafeStr as _;
    use sqlx::migrate::MigrationType;

    let mut migrations = migrator.migrations.to_vec();
    for (version, checksum) in forward {
        let mut placeholder = Migration::new(
            *version,
            "forward-compatible migration recorded by a newer release".into(),
            MigrationType::Simple,
            FORWARD_PLACEHOLDER_SQL.into_sql_str(),
            false,
        );
        placeholder.checksum = checksum.clone().into();
        migrations.push(placeholder);
    }
    migrations.sort_by_key(|migration| migration.version);
    Migrator {
        migrations: migrations.into(),
        ignore_missing: migrator.ignore_missing,
        locking: migrator.locking,
        no_tx: migrator.no_tx,
        table_name: migrator.table_name.clone(),
        create_schemas: migrator.create_schemas.clone(),
    }
}

fn validate_public_history(
    store: &str,
    conn: &Connection,
    migrator: &Migrator,
    common: bool,
    checksums: &AcceptedChecksums,
    forward: &ForwardCompatible,
) -> SqliteResult<RecordedHistory> {
    let table = if common {
        "_space_common_schema_migrations"
    } else {
        "_sqlx_migrations"
    };
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = ?1)",
            [table],
            |row| row.get(0),
        )
        .context("failed to inspect public migration history")?;
    if !exists {
        return Ok(RecordedHistory::default());
    }
    let query = if common {
        "SELECT version, success, checksum FROM _space_common_schema_migrations"
    } else {
        "SELECT version, success, checksum FROM _sqlx_migrations"
    };
    let mut statement = conn
        .prepare(query)
        .context("failed to read public migration history")?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, bool>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })
        .context("failed to query public migration history")?;
    let bundled = migrations_by_version(migrator);
    let mut history = RecordedHistory::default();
    for row in rows {
        let (version, success, checksum) = row.context("invalid public migration history")?;
        let compatible = success
            && match bundled.get(&version) {
                Some(migration) => {
                    checksums.accepts(store, version, &migration.checksum, &checksum)
                }
                None => {
                    let listed = forward.accepts(store, version, &checksum);
                    if listed {
                        history.forward.push((version, checksum));
                    }
                    listed
                }
            };
        if !compatible {
            return Err(SqliteError::message(format!(
                "{table} has failed, modified, or unbundled migration {version}; see db/README.md"
            )));
        }
        history.applied = true;
    }
    Ok(history)
}

/// Each catalog's accepted forward-compatible rows, as `(version, checksum)`.
#[derive(Clone, Debug, Default)]
struct ForwardRecorded {
    service: Vec<(i64, Vec<u8>)>,
    common: Vec<(i64, Vec<u8>)>,
}

async fn validate_schema(
    db_name: &str,
    db_path: &Path,
    migrators: EmbeddedMigrators<'_>,
    checksums: &AcceptedChecksums,
    forward: &ForwardCompatible,
) -> SqliteResult<ForwardRecorded> {
    let conn = Connection::open(db_path).context("failed to open SQLite database")?;
    let service =
        validate_public_history(db_name, &conn, migrators.service, false, checksums, forward)?;
    let common = match migrators.common {
        Some(common) => validate_public_history("common", &conn, common, true, checksums, forward)?,
        None => RecordedHistory::default(),
    };
    let service_applied = service.applied;
    let common_applied = common.applied;
    if service_applied && migrators.common.is_some() && !common_applied {
        return Err(SqliteError::message(
            "SQLite service history is present but shared history is missing; explicit repair is required",
        ));
    }
    if !service_applied {
        let expected = if let Some(common) = migrators.common.filter(|_| common_applied) {
            let reference = SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .context("failed to open common schema reference")?;
            let result = async {
                common_migrator(common).run(&reference).await
                    .context("failed to migrate common schema reference")?;
                sqlx::query_as::<_, (String, String)>(sql!(
                    "SELECT name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'
                     AND name NOT IN ('_sqlx_migrations', '_space_common_schema_migrations') ORDER BY name"
                ))
                .fetch_all(&reference).await.context("failed to read common schema reference")
            }.await;
            reference.close().await;
            result?
        } else {
            Vec::new()
        };
        if public_application_schema(&conn)? != expected {
            return Err(SqliteError::message(
                "SQLite has untracked application schema; see db/README.md",
            ));
        }
    }
    Ok(ForwardRecorded {
        service: service.forward,
        common: common.forward,
    })
}

fn public_application_schema(conn: &Connection) -> SqliteResult<Vec<(String, String)>> {
    conn.prepare(
        "SELECT name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'
         AND name NOT IN ('_sqlx_migrations', '_space_common_schema_migrations') ORDER BY name",
    )
    .context("failed to inspect untracked SQLite schema")?
    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
    .context("failed to query untracked SQLite schema")?
    .collect::<Result<_, _>>()
    .context("failed to read untracked SQLite schema")
}

pub async fn migrate(db_name: &str, db_path: &Path, migrator: &Migrator) -> SqliteResult<()> {
    migrate_embedded(
        db_name,
        db_path,
        EmbeddedMigrators {
            service: migrator,
            common: None,
        },
    )
    .await
}

pub fn open_rusqlite(db_path: &Path, options: RusqliteOpenOptions) -> SqliteResult<Connection> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create SQLite directory {}", parent.display()))?;
    }

    let mut conn = Connection::open(db_path)
        .with_context(|| format!("failed to open SQLite database {}", db_path.display()))?;
    conn.busy_timeout(options.busy_timeout)
        .with_context(|| format!("failed to set busy timeout for {}", db_path.display()))?;

    let mut pragmas = String::from("PRAGMA journal_mode=WAL;");
    if options.synchronous_normal {
        pragmas.push_str("PRAGMA synchronous=NORMAL;");
    }
    if options.foreign_keys {
        pragmas.push_str("PRAGMA foreign_keys=ON;");
    }
    conn.execute_batch(&pragmas)
        .with_context(|| format!("failed to configure SQLite database {}", db_path.display()))?;
    conn.trace_v2(
        TraceEventCodes::SQLITE_TRACE_PROFILE,
        Some(profile_rusqlite_trace_event),
    )
    .with_context(|| {
        format!(
            "failed to configure SQLite tracing for {}",
            db_path.display()
        )
    })?;
    restrict_database_permissions(db_path)?;
    Ok(conn)
}

pub fn remove_database_files(db_path: &Path) -> SqliteResult<()> {
    for path in database_files(db_path) {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to remove SQLite database file {}", path.display())
                });
            }
        }
    }
    Ok(())
}

pub fn restrict_database_permissions(db_path: &Path) -> SqliteResult<()> {
    for path in database_files(db_path) {
        restrict_file_permissions(&path)?;
    }
    Ok(())
}

async fn migrate_once(
    db_name: &str,
    db_path: &Path,
    migrators: EmbeddedMigrators<'_>,
    checksums: &AcceptedChecksums,
    forward: &ForwardCompatible,
) -> SqliteResult<()> {
    checksums
        .validate(db_name, migrators.service)
        .context("unlisted migration checksum")?;
    if let Some(common) = migrators.common {
        checksums
            .validate("common", common)
            .context("unlisted common migration checksum")?;
    }
    let span = tracing::info_span!(
        "sqlite.migrate",
        db.system = "sqlite",
        db.name = db_name,
        db.path = %db_path.display(),
    );
    async {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create SQLite directory {}", parent.display())
            })?;
        }

        let connect_options = SqliteConnectOptions::new()
            .filename(db_path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .with_context(|| {
                format!(
                    "failed to connect to {db_name} SQLite at {}",
                    db_path.display()
                )
            })?;

        let result = async {
            if db_name == template_manager::STORE {
                template_manager::reverse_unbundled(db_path, migrators.service)?;
            }
            let recorded = validate_schema(db_name, db_path, migrators, checksums, forward).await?;
            let mut conn = pool
                .acquire()
                .await
                .context("failed to acquire SQLite migration connection")?;
            if let Some(common) = migrators.common {
                let common = with_forward_placeholders(&common_migrator(common), &recorded.common);
                checksums
                    .run("common", &common, &mut *conn)
                    .await
                    .context("failed to apply common SQLite migrations")?;
            }
            let service = with_forward_placeholders(migrators.service, &recorded.service);
            checksums
                .run(db_name, &service, &mut *conn)
                .await
                .with_context(|| format!("failed to apply {db_name} SQLite migrations"))?;
            Ok(())
        }
        .await;

        pool.close().await;
        if result.is_ok() {
            restrict_database_permissions(db_path)?;
        }
        result
    }
    .instrument(span)
    .await
}

fn profile_rusqlite_query(sql: &str, elapsed: Duration) {
    tracing::info!(
        target: "rusqlite::query",
        summary = sqlite_query_summary(sql),
        db.system = "sqlite",
        db.statement = sql,
        rows_affected = 0_i64,
        rows_returned = 0_i64,
        elapsed_secs = elapsed.as_secs_f64(),
    );
}

fn profile_rusqlite_trace_event(event: TraceEvent<'_>) {
    if let TraceEvent::Profile(statement, elapsed) = event {
        let sql = statement.sql();
        profile_rusqlite_query(sql.as_ref(), elapsed);
    }
}

fn sqlite_query_summary(sql: &str) -> &'static str {
    // Skip a leading `/*module_path:line*/` locator comment (the `sql!`
    // macro) so classification still sees the statement verb.
    let sql = match sql.trim_start().strip_prefix("/*") {
        Some(rest) => rest.split_once("*/").map(|(_, sql)| sql).unwrap_or(sql),
        None => sql,
    };
    match sql
        .split_whitespace()
        .next()
        .map(str::trim)
        .map(|word| word.trim_end_matches(';'))
        .unwrap_or_default()
        .to_ascii_uppercase()
        .as_str()
    {
        "SELECT" => "sqlite SELECT",
        "INSERT" => "sqlite INSERT",
        "UPDATE" => "sqlite UPDATE",
        "DELETE" => "sqlite DELETE",
        "CREATE" => "sqlite CREATE",
        "DROP" => "sqlite DROP",
        "ALTER" => "sqlite ALTER",
        "BEGIN" => "sqlite BEGIN",
        "COMMIT" => "sqlite COMMIT",
        "ROLLBACK" => "sqlite ROLLBACK",
        "PRAGMA" => "sqlite PRAGMA",
        _ => "sqlite query",
    }
}

#[cfg(test)]
async fn table_exists(pool: &SqlitePool, table_name: &str) -> SqliteResult<bool> {
    let exists: Option<i64> = sqlx::query_scalar(sql!(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1"
    ))
    .bind(table_name)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to check for SQLite table {table_name}"))?;
    Ok(exists.is_some())
}

fn migrations_by_version(migrator: &Migrator) -> HashMap<i64, &Migration> {
    migrator
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| (migration.version, migration))
        .collect()
}

fn database_files(db_path: &Path) -> [PathBuf; 3] {
    [
        db_path.to_path_buf(),
        PathBuf::from(format!("{}-wal", db_path.display())),
        PathBuf::from(format!("{}-shm", db_path.display())),
    ]
}

#[cfg(unix)]
fn restrict_file_permissions(path: &Path) -> SqliteResult<()> {
    use std::os::unix::fs::PermissionsExt as _;

    match std::fs::metadata(path) {
        Ok(_) => std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to chmod 0600 {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("failed to stat SQLite file {}", path.display()))
        }
    }
}

#[cfg(not(unix))]
fn restrict_file_permissions(_path: &Path) -> SqliteResult<()> {
    Ok(())
}

#[cfg(test)]
mod tests;
