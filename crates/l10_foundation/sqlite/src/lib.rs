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

use pplx_space_migration_checksums::AcceptedChecksums;
use pplx_space_sql_locator::sql;
use std::collections::{BTreeSet, HashMap};
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
/// A recorded migration its catalog doesn't bundle is left as it is when it
/// succeeded, records a SHA-384 checksum, and is newer than every migration
/// that catalog bundles: a newer release applied it, and db/README.md's
/// authoring rules keep an older release able to run on its schema. Any other
/// unbundled row is refused, and so is a history with such a row while either
/// history lacks a migration this release bundles: the newer release that
/// recorded the row applied both catalogs in full.
///
/// Daemons call this after acquiring their database ownership mutex.
pub async fn migrate_embedded(
    db_name: &str,
    db_path: &Path,
    migrators: EmbeddedMigrators<'_>,
) -> SqliteResult<()> {
    if std::env::var_os(RETIRED_ACCEPT_NEWER_MIGRATIONS).is_some() {
        tracing::warn!(
            store = db_name,
            "{RETIRED_ACCEPT_NEWER_MIGRATIONS} is set but no longer read: this release accepts \
             newer successful migrations by default"
        );
    }
    let checksums = AcceptedChecksums::embedded().context("invalid migration checksum manifest")?;
    migrate_once(db_name, db_path, migrators, &checksums)
        .await
        .with_context(|| migrate_failure(db_name, db_path))
}

fn migrate_failure(db_name: &str, db_path: &Path) -> String {
    format!(
        "failed to migrate {db_name} SQLite at {}",
        db_path.display()
    )
}

/// Whether [`migrate_embedded`] would refuse the database at `db_path` now,
/// answered without writing anything, so that a passive instance can ask
/// while another instance owns and writes the database. `Ok(None)` means the
/// runner would accept it; a missing database records nothing and is
/// accepted. The answer comes from the decision the runner's startup
/// validation makes, so a refusal here has the runner's text.
///
/// [`MigrateBlock`] also says when this read couldn't tell, because another
/// connection held a lock it needed or the database changed while it was
/// read. That is neither an acceptance nor a refusal: ask again.
pub async fn check_migrate_embedded(
    db_name: &str,
    db_path: &Path,
    migrators: EmbeddedMigrators<'_>,
) -> SqliteResult<Option<MigrateBlock>> {
    let checksums = AcceptedChecksums::embedded().context("invalid migration checksum manifest")?;
    check_once(
        db_name,
        db_path,
        migrators,
        &checksums,
        RusqliteOpenOptions::default().busy_timeout,
    )
    .await
    .with_context(|| check_failure(db_name, db_path))
}

/// [`check_migrate_embedded`] for a store without a shared catalog, as
/// [`migrate`] is for [`migrate_embedded`].
pub async fn check_migrate(
    db_name: &str,
    db_path: &Path,
    migrator: &Migrator,
) -> SqliteResult<Option<MigrateBlock>> {
    check_migrate_embedded(
        db_name,
        db_path,
        EmbeddedMigrators {
            service: migrator,
            common: None,
        },
    )
    .await
}

fn check_failure(db_name: &str, db_path: &Path) -> String {
    format!(
        "failed to check {db_name} SQLite at {} against this release's migrations",
        db_path.display()
    )
}

/// Use the existing shared history without moving or rewriting its rows.
pub fn common_migrator(migrator: &Migrator) -> Migrator {
    Migrator {
        migrations: migrator.migrations.clone(),
        table_name: COMMON_HISTORY_TABLE.into(),
        ..Migrator::DEFAULT
    }
}

/// Earlier releases read this to accept the newer migrations it named. This one
/// accepts them by default and only warns that the setting does nothing, for a
/// chart or an operator still following the earlier rollback procedure.
const RETIRED_ACCEPT_NEWER_MIGRATIONS: &str = "SPACE_SQLITE_ACCEPT_NEWER_MIGRATIONS";

/// SQLx records each migration's SHA-384 digest.
const CHECKSUM_LEN: usize = 48;

const SERVICE_HISTORY_TABLE: &str = "_sqlx_migrations";
const COMMON_HISTORY_TABLE: &str = "_space_common_schema_migrations";

/// What validation found in one public history.
#[derive(Debug)]
struct PublicHistory {
    /// The store whose catalog this history records, for warnings.
    store: String,
    table: &'static str,
    /// The history records at least one migration.
    applied: bool,
    /// Accepted newer migrations this catalog doesn't bundle, as
    /// `(version, recorded checksum)`.
    accepted_newer: Vec<(i64, Vec<u8>)>,
    /// The oldest bundled migration the history doesn't record.
    missing: Option<i64>,
}

impl PublicHistory {
    fn empty(store: &str, table: &'static str) -> Self {
        Self {
            store: store.to_owned(),
            table,
            applied: false,
            accepted_newer: Vec::new(),
            missing: None,
        }
    }
}

#[derive(Debug)]
struct ValidatedHistories {
    service: PublicHistory,
    common: PublicHistory,
}

/// Fails if SQLx ever applies it: a placeholder stands only for a history row
/// already recorded with its checksum, which SQLx compares and leaves.
const NEWER_PLACEHOLDER_SQL: &str =
    "SELECT accepted_newer_migration_recorded_by_a_newer_release_is_never_applied_here;";

/// `migrator` plus a placeholder for each accepted newer row, carrying the
/// recorded checksum. SQLx then leaves those rows alone and still refuses any
/// other recorded version it doesn't bundle, such as one a concurrent runner
/// recorded after validation.
fn with_newer_placeholders(migrator: &Migrator, accepted: &[(i64, Vec<u8>)]) -> Migrator {
    use sqlx::SqlSafeStr as _;
    use sqlx::migrate::MigrationType;

    let mut migrations = migrator.migrations.to_vec();
    for (version, checksum) in accepted {
        let mut placeholder = Migration::new(
            *version,
            "newer migration recorded by a newer release".into(),
            MigrationType::Simple,
            NEWER_PLACEHOLDER_SQL.into_sql_str(),
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

fn history_table(common: bool) -> &'static str {
    if common {
        COMMON_HISTORY_TABLE
    } else {
        SERVICE_HISTORY_TABLE
    }
}

/// One recorded public-history row: `(version, success, checksum)`.
type HistoryRow = (i64, bool, Vec<u8>);

/// What validation reads from a database: each public history, empty when
/// its table is missing, and the application schema outside them.
struct RecordedSchema {
    service: Vec<HistoryRow>,
    common: Vec<HistoryRow>,
    application: Vec<(String, String)>,
}

/// Reads what validation decides on in one read transaction, so that a
/// reader beside a live writer sees one committed state of every table.
fn read_recorded_schema(conn: &Connection, common: bool) -> SqliteResult<RecordedSchema> {
    let transaction = conn
        .unchecked_transaction()
        .context("failed to begin reading public migration history")?;
    let recorded = RecordedSchema {
        service: read_public_history(&transaction, false)?,
        common: if common {
            read_public_history(&transaction, true)?
        } else {
            Vec::new()
        },
        application: public_application_schema(&transaction)?,
    };
    transaction
        .commit()
        .context("failed to finish reading public migration history")?;
    Ok(recorded)
}

fn read_public_history(conn: &Connection, common: bool) -> SqliteResult<Vec<HistoryRow>> {
    let table = history_table(common);
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = ?1)",
            [table],
            |row| row.get(0),
        )
        .context("failed to inspect public migration history")?;
    if !exists {
        return Ok(Vec::new());
    }
    let query = if common {
        "SELECT version, success, checksum FROM _space_common_schema_migrations ORDER BY version"
    } else {
        "SELECT version, success, checksum FROM _sqlx_migrations ORDER BY version"
    };
    conn.prepare(query)
        .context("failed to read public migration history")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .context("failed to query public migration history")?
        .collect::<Result<_, _>>()
        .context("invalid public migration history")
}

/// Why the runner refuses a database. Its text is the refusal
/// [`migrate_embedded`] returns.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
enum Refusal {
    #[error("{table} has failed, modified, or unbundled migration {version}; see db/README.md")]
    Invalid { table: &'static str, version: i64 },
    /// `table` records newer migration `newer` while `gap_table` doesn't
    /// record bundled migration `missing`.
    #[error("{}", gap_refusal(table, *newer, gap_table, *missing))]
    GapBelowNewer {
        table: &'static str,
        newer: i64,
        gap_table: &'static str,
        missing: i64,
    },
    #[error(
        "SQLite service history is present but shared history is missing; explicit repair is required"
    )]
    MissingSharedHistory,
    #[error("SQLite has untracked application schema; see db/README.md")]
    UntrackedSchema,
}

fn gap_refusal(table: &str, newer: i64, gap_table: &str, missing: i64) -> String {
    if table == gap_table {
        format!(
            "{table} records newer migration {newer} but not bundled migration {missing}; \
             see db/README.md"
        )
    } else {
        format!(
            "{table} records newer migration {newer} but {gap_table} does not record bundled \
             migration {missing}; see db/README.md"
        )
    }
}

impl From<Refusal> for SqliteError {
    fn from(refusal: Refusal) -> Self {
        Self {
            context: String::new(),
            source: Box::new(refusal),
        }
    }
}

/// Why [`check_migrate_embedded`] holds takeover back: the runner would
/// refuse the database, or the read couldn't tell whether it would.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrateBlock {
    store: String,
    db_path: PathBuf,
    reason: BlockReason,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum BlockReason {
    /// The runner would refuse the database.
    Refused(Refusal),
    /// Another connection held a lock the read needed (`SQLITE_BUSY` or
    /// `SQLITE_LOCKED`), with the error.
    Locked(String),
    /// The database changed while it was read as immutable.
    Changed,
}

impl Display for MigrateBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (store, path) = (&self.store, self.db_path.display());
        match &self.reason {
            BlockReason::Refused(refusal) => {
                write!(
                    f,
                    "this release refuses {store} SQLite at {path}: {refusal}"
                )
            }
            BlockReason::Locked(error) => write!(
                f,
                "could not tell whether this release accepts {store} SQLite at {path}, because \
                 another connection held a lock the read needed ({error}); checking again"
            ),
            BlockReason::Changed => write!(
                f,
                "could not tell whether this release accepts {store} SQLite at {path}, because \
                 it changed while it was read; checking again"
            ),
        }
    }
}

/// The decision over one public history's recorded rows.
fn validate_public_history(
    store: &str,
    rows: &[HistoryRow],
    migrator: &Migrator,
    common: bool,
    checksums: &AcceptedChecksums,
) -> Result<PublicHistory, Refusal> {
    let table = history_table(common);
    let bundled = migrations_by_version(migrator);
    let newest_bundled = bundled.keys().max().copied();
    let mut recorded = BTreeSet::new();
    let mut history = PublicHistory::empty(store, table);
    for (version, success, checksum) in rows {
        let (version, success) = (*version, *success);
        let valid = match bundled.get(&version) {
            Some(migration) => {
                success && checksums.accepts(store, version, &migration.checksum, checksum)
            }
            // A newer release applied it, and the authoring rules keep its
            // schema usable by this release. A recorded version below the
            // newest bundled one is refused instead: one withdrawn after a
            // newer migration was bundled, one merged out of timestamp order,
            // or an accepted row that a later release's migration overtook.
            // A checksum that isn't a SHA-384 digest is damage SQLx would
            // not notice: the placeholder carries it and compares it with
            // itself.
            None if success
                && checksum.len() == CHECKSUM_LEN
                && newest_bundled.is_none_or(|newest| version > newest) =>
            {
                history.accepted_newer.push((version, checksum.clone()));
                true
            }
            None => false,
        };
        if !valid {
            return Err(Refusal::Invalid { table, version });
        }
        recorded.insert(version);
        history.applied = true;
    }
    history.missing = bundled
        .keys()
        .filter(|v| !recorded.contains(v))
        .min()
        .copied();
    Ok(history)
}

/// Refuses a newer row in either history while either lacks a bundled
/// migration. The newer release that recorded the row applied every migration
/// of both catalogs below it, the shared one first, so one this release would
/// still apply is missing from a damaged history, not pending; applying it
/// would run its SQL on a schema it wasn't written for.
fn refuse_gap_below_newer(histories: [&PublicHistory; 2]) -> Result<(), Refusal> {
    for gap in histories {
        let Some(missing) = gap.missing else {
            continue;
        };
        // Name a newer row of the history with the gap when it has one.
        let newer = std::iter::once(gap)
            .chain(histories)
            .find_map(|history| Some((history, history.accepted_newer.first()?.0)));
        if let Some((history, newer)) = newer {
            return Err(Refusal::GapBelowNewer {
                table: history.table,
                newer,
                gap_table: gap.table,
                missing,
            });
        }
    }
    Ok(())
}

/// Logs each newer row validation accepted in the history.
fn warn_accepted_newer(history: &PublicHistory) {
    let table = history.table;
    for (version, checksum) in &history.accepted_newer {
        tracing::warn!(
            store = history.store,
            migration_version = version,
            recorded_checksum = %hex::encode(checksum),
            "{table} records newer migration {version}, which this release doesn't bundle; \
             leaving it applied"
        );
    }
}

/// The runner's decision on what a database records: what validation
/// accepts, or why it refuses. The runner's startup validation and
/// [`check_migrate_embedded`] both decide here, so they can't disagree. Only
/// a failure to build the shared schema reference is an error.
async fn decide(
    db_name: &str,
    recorded: &RecordedSchema,
    migrators: EmbeddedMigrators<'_>,
    checksums: &AcceptedChecksums,
) -> SqliteResult<Result<ValidatedHistories, Refusal>> {
    let validated = match decide_histories(db_name, recorded, migrators, checksums) {
        Ok(validated) => validated,
        Err(refusal) => return Ok(Err(refusal)),
    };
    if !validated.service.applied {
        let expected = match migrators.common.filter(|_| validated.common.applied) {
            Some(common) => common_schema_reference(common).await?,
            None => Vec::new(),
        };
        if recorded.application != expected {
            return Ok(Err(Refusal::UntrackedSchema));
        }
    }
    Ok(Ok(validated))
}

fn decide_histories(
    db_name: &str,
    recorded: &RecordedSchema,
    migrators: EmbeddedMigrators<'_>,
    checksums: &AcceptedChecksums,
) -> Result<ValidatedHistories, Refusal> {
    let service = validate_public_history(
        db_name,
        &recorded.service,
        migrators.service,
        false,
        checksums,
    )?;
    let common = match migrators.common {
        Some(common) => {
            validate_public_history("common", &recorded.common, common, true, checksums)?
        }
        None => PublicHistory::empty("common", COMMON_HISTORY_TABLE),
    };
    refuse_gap_below_newer([&service, &common])?;
    if service.applied && migrators.common.is_some() && !common.applied {
        return Err(Refusal::MissingSharedHistory);
    }
    Ok(ValidatedHistories { service, common })
}

/// The application schema the shared catalog alone creates.
async fn common_schema_reference(common: &Migrator) -> SqliteResult<Vec<(String, String)>> {
    let reference = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .context("failed to open common schema reference")?;
    let result = async {
        common_migrator(common)
            .run(&reference)
            .await
            .context("failed to migrate common schema reference")?;
        sqlx::query_as::<_, (String, String)>(sql!(
            "SELECT name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'
             AND name NOT IN ('_sqlx_migrations', '_space_common_schema_migrations') ORDER BY name"
        ))
        .fetch_all(&reference)
        .await
        .context("failed to read common schema reference")
    }
    .await;
    reference.close().await;
    result
}

async fn validate_schema(
    db_name: &str,
    db_path: &Path,
    migrators: EmbeddedMigrators<'_>,
    checksums: &AcceptedChecksums,
) -> SqliteResult<ValidatedHistories> {
    let conn = Connection::open(db_path).context("failed to open SQLite database")?;
    let recorded = read_recorded_schema(&conn, migrators.common.is_some())?;
    drop(conn);
    let validated = decide(db_name, &recorded, migrators, checksums).await??;
    warn_accepted_newer(&validated.service);
    warn_accepted_newer(&validated.common);
    Ok(validated)
}

async fn check_once(
    db_name: &str,
    db_path: &Path,
    migrators: EmbeddedMigrators<'_>,
    checksums: &AcceptedChecksums,
    busy_timeout: Duration,
) -> SqliteResult<Option<MigrateBlock>> {
    validate_catalogs(db_name, migrators, checksums)?;
    let read = {
        let db_path = db_path.to_path_buf();
        let common = migrators.common.is_some();
        tokio::task::spawn_blocking(move || read_without_writing(&db_path, common, busy_timeout))
            .await
            .context("the read-only check stopped before it finished")??
    };
    let block = |reason| MigrateBlock {
        store: db_name.to_owned(),
        db_path: db_path.to_path_buf(),
        reason,
    };
    let recorded = match read {
        ReadWithoutWriting::Missing => return Ok(None),
        ReadWithoutWriting::Unsettled(reason) => return Ok(Some(block(reason))),
        ReadWithoutWriting::Recorded(recorded) => recorded,
    };
    Ok(decide(db_name, &recorded, migrators, checksums)
        .await?
        .err()
        .map(|refusal| block(BlockReason::Refused(refusal))))
}

enum ReadWithoutWriting {
    /// No database file is there.
    Missing,
    /// The read couldn't tell; see [`BlockReason`].
    Unsettled(BlockReason),
    Recorded(RecordedSchema),
}

/// What an immutable read relies on staying as it was: the main file's size
/// and modification time, and that no `-wal` exists.
#[derive(Debug, PartialEq, Eq)]
struct DatabaseFiles {
    main: (u64, std::time::SystemTime),
    wal: bool,
}

impl DatabaseFiles {
    /// The files at `db_path`, or `None` when no database file is there.
    fn observe(db_path: &Path) -> SqliteResult<Option<Self>> {
        let [_, wal, _] = database_files(db_path);
        let main = match std::fs::metadata(db_path) {
            Ok(metadata) => (
                metadata.len(),
                metadata.modified().with_context(|| {
                    format!(
                        "failed to read the modification time of {}",
                        db_path.display()
                    )
                })?,
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to stat SQLite file {}", db_path.display()));
            }
        };
        let wal = wal
            .try_exists()
            .with_context(|| format!("failed to stat SQLite file {}", wal.display()))?;
        Ok(Some(Self { main, wal }))
    }
}

/// Reads what validation decides on without creating or changing any file.
fn read_without_writing(
    db_path: &Path,
    common: bool,
    busy_timeout: Duration,
) -> SqliteResult<ReadWithoutWriting> {
    match DatabaseFiles::observe(db_path)? {
        None => Ok(ReadWithoutWriting::Missing),
        Some(before) => read_observed(db_path, &before, common, busy_timeout),
    }
}

/// [`read_without_writing`] of the files `before` describes.
///
/// A read-only connection still creates a missing `-wal` and `-shm` and
/// leaves them behind. Without a `-wal`, no WAL-mode connection has the
/// database open, as each keeps one while it does, and the main file holds
/// every commit, so it is read as immutable, which opens nothing else and
/// takes no lock. A writer can open the database during that read: it
/// creates a `-wal` before it writes, and only a checkpoint, which changes the
/// main file, writes the main file. So when a `-wal` has appeared or the main
/// file has changed by the end of the read, what was read may be torn, and
/// the read is unsettled. With a `-wal`, the database is read through it as
/// any reader does, under SQLite's locks, which sees a live writer's
/// commits; a lock the read can't get within `busy_timeout` leaves it
/// unsettled too, and so does a `-wal` gone by the time the read starts. The
/// runner validates again once it holds the service mutex, so a read that
/// raced a writer can delay takeover but never replaces that validation.
fn read_observed(
    db_path: &Path,
    before: &DatabaseFiles,
    common: bool,
    busy_timeout: Duration,
) -> SqliteResult<ReadWithoutWriting> {
    use rusqlite::OpenFlags;

    let immutable = !before.wal;
    let conn = Connection::open_with_flags(
        read_only_uri(db_path, immutable),
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| {
        format!(
            "failed to open SQLite database {} read-only",
            db_path.display()
        )
    })?;
    conn.busy_timeout(busy_timeout)
        .with_context(|| format!("failed to set busy timeout for {}", db_path.display()))?;
    // The last writer to close removes the `-wal`; reading after that would
    // create it again.
    if !immutable && DatabaseFiles::observe(db_path)?.is_none_or(|now| !now.wal) {
        return Ok(ReadWithoutWriting::Unsettled(BlockReason::Changed));
    }
    let recorded = read_recorded_schema(&conn, common);
    drop(conn);
    if immutable && DatabaseFiles::observe(db_path)?.as_ref() != Some(before) {
        return Ok(ReadWithoutWriting::Unsettled(BlockReason::Changed));
    }
    match recorded {
        Ok(recorded) => Ok(ReadWithoutWriting::Recorded(recorded)),
        Err(error) if is_locked(&error) => Ok(ReadWithoutWriting::Unsettled(BlockReason::Locked(
            error.to_string(),
        ))),
        Err(error) => Err(error),
    }
}

/// Whether `error` is SQLite's `SQLITE_BUSY` or `SQLITE_LOCKED`.
fn is_locked(error: &SqliteError) -> bool {
    error
        .source
        .downcast_ref::<rusqlite::Error>()
        .and_then(rusqlite::Error::sqlite_error_code)
        .is_some_and(|code| {
            matches!(
                code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
        })
}

/// A `file:` URI for `db_path`, escaping what SQLite's URI parser would
/// otherwise read as a query, fragment or escape.
fn read_only_uri(db_path: &Path, immutable: bool) -> String {
    let path = db_path.display().to_string();
    let mut uri = String::from(if path.starts_with('/') {
        "file://"
    } else {
        "file:"
    });
    for c in path.chars() {
        match c {
            '%' => uri.push_str("%25"),
            '?' => uri.push_str("%3f"),
            '#' => uri.push_str("%23"),
            c => uri.push(c),
        }
    }
    uri.push_str(if immutable {
        "?mode=ro&immutable=1"
    } else {
        "?mode=ro"
    });
    uri
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

/// Refuse a bundled migration whose source the checksum manifest doesn't
/// list, whatever the database records.
fn validate_catalogs(
    db_name: &str,
    migrators: EmbeddedMigrators<'_>,
    checksums: &AcceptedChecksums,
) -> SqliteResult<()> {
    checksums
        .validate(db_name, migrators.service)
        .context("unlisted migration checksum")?;
    if let Some(common) = migrators.common {
        checksums
            .validate("common", common)
            .context("unlisted common migration checksum")?;
    }
    Ok(())
}

async fn migrate_once(
    db_name: &str,
    db_path: &Path,
    migrators: EmbeddedMigrators<'_>,
    checksums: &AcceptedChecksums,
) -> SqliteResult<()> {
    validate_catalogs(db_name, migrators, checksums)?;
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
            let validated = validate_schema(db_name, db_path, migrators, checksums).await?;
            let mut conn = pool
                .acquire()
                .await
                .context("failed to acquire SQLite migration connection")?;
            if let Some(common) = migrators.common {
                let common = with_newer_placeholders(
                    &common_migrator(common),
                    &validated.common.accepted_newer,
                );
                checksums
                    .run("common", &common, &mut *conn)
                    .await
                    .context("failed to apply common SQLite migrations")?;
            }
            let service =
                with_newer_placeholders(migrators.service, &validated.service.accepted_newer);
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
