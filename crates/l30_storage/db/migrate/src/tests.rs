use rusqlite::{Connection, params};

use super::*;

static ALL_MIGRATORS: &[(&str, &Migrator)] = &[
    ("common", &COMMON_SCHEMA_MIGRATOR),
    ("postgres", &POSTGRES_MIGRATOR),
    (
        "sandbox-manager",
        pplx_space_sbx_mgr_store::db::migration::target().service,
    ),
    ("template-manager", &TEMPLATE_MANAGER_MIGRATOR),
    ("node-manager", &NODE_MANAGER_MIGRATOR),
    ("node-ctl", &NODE_CTL_MIGRATOR),
    ("runtime-manager", &RUNTIME_MANAGER_MIGRATOR),
    ("volume-manager", &VOLUME_MANAGER_MIGRATOR),
    ("build-manager", &BUILD_MANAGER_MIGRATOR),
];

#[tokio::test]
async fn sandbox_baseline_refuses_down_without_changing_shared_jobs_or_history() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sandbox.sqlite");
    let database = SqliteDatabase::from(pplx_space_sbx_mgr_store::db::migration::target());
    migrate_sqlite_up(database, &path).await.unwrap();
    // Step back to the baseline first: the property is about the baseline's
    // own down migration, not whichever migration happens to be newest.
    let after_baseline = pplx_space_sbx_mgr_store::db::migration::target()
        .service
        .iter()
        .filter(|migration| {
            migration.migration_type.is_up_migration()
                && migration.version > pplx_space_sqlite::baselines::PUBLIC_SQLITE_BASELINE
        })
        .count();
    for _ in 0..after_baseline {
        rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
            .await
            .unwrap();
    }
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "INSERT INTO jobs (job_id, kind, status) VALUES ('cancelled-witness', 'test', 'cancelled')",
        [],
    )
    .unwrap();
    let before = common_history(&conn).unwrap();
    let schema_before: i64 = conn
        .pragma_query_value(None, "schema_version", |row| row.get(0))
        .unwrap();
    let error = rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
        .await
        .unwrap_err();
    assert!(matches!(error, RunDbMigrateError::RollbackSqlite(_)));
    assert_eq!(common_history(&conn).unwrap(), before);
    assert_eq!(
        conn.pragma_query_value(None, "schema_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        schema_before
    );
    assert_eq!(
        conn.query_row(
            "SELECT status FROM jobs WHERE job_id = 'cancelled-witness'",
            [],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        "cancelled"
    );
}

/// Rolling a node back past the capture-time marker keeps its artifact rows,
/// and upgrading again brings every one of them back as not comparable. A pin
/// recorded before a downgrade can't then report a capture time the
/// downgraded binary never guaranteed.
#[tokio::test]
async fn capture_time_marker_round_trips_and_reupgrades_as_not_comparable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sandbox.sqlite");
    let database = SqliteDatabase::from(pplx_space_sbx_mgr_store::db::migration::target());
    let marker_columns = |conn: &Connection| -> Vec<String> {
        let mut statement = conn
            .prepare("SELECT name FROM pragma_table_info('local_artifacts') WHERE name = 'capture_time_comparable'")
            .unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    let marker = |conn: &Connection| -> i64 {
        conn.query_row(
            "SELECT capture_time_comparable FROM local_artifacts WHERE local_artifact_id = 'a'",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };

    migrate_sqlite_up(database, &path).await.unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO local_artifacts (
                 local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                 local_path, created_at, capture_time_comparable
             ) VALUES ('a', 's', 'disk', 'stop', '/cache/a', '1', 1)",
            [],
        )
        .unwrap();
        assert_eq!(marker(&conn), 1);
    }

    // Roll back until the marker's own migration has been undone, however many
    // newer migrations sit above it.
    for _ in 0..pplx_space_sbx_mgr_store::db::migration::target()
        .service
        .iter()
        .count()
    {
        if marker_columns(&Connection::open(&path).unwrap()).is_empty() {
            break;
        }
        rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
            .await
            .unwrap();
    }
    {
        let conn = Connection::open(&path).unwrap();
        assert!(
            marker_columns(&conn).is_empty(),
            "the down migration drops the marker"
        );
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM local_artifacts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 1, "and keeps the artifact row");
    }

    migrate_sqlite_up(database, &path).await.unwrap();
    let conn = Connection::open(&path).unwrap();
    assert_eq!(
        marker_columns(&conn),
        vec!["capture_time_comparable".to_owned()]
    );
    assert_eq!(
        marker(&conn),
        0,
        "a row from before the downgrade isn't comparable"
    );
}

#[test]
fn embedded_migrations_have_unique_versions_and_public_baselines() {
    for (name, migrator) in ALL_MIGRATORS {
        assert_unique_migration_versions(name, migrator);
        let baseline = if *name == "postgres" {
            20261002070625
        } else {
            pplx_space_sqlite::baselines::PUBLIC_SQLITE_BASELINE
        };
        assert_eq!(
            migrator
                .iter()
                .filter(|migration| migration.migration_type.is_up_migration())
                .map(|migration| migration.version)
                .min(),
            Some(baseline),
            "{name} must reach its public baseline before legacy compatibility is removed"
        );
    }
}

/// `-- no-transaction` migrations run outside a transaction so they can
/// hold statements like CREATE INDEX CONCURRENTLY — but sqlx still sends
/// the whole file to Postgres as one simple-query batch, and Postgres
/// wraps a multi-statement batch in an implicit transaction, which
/// rejects CONCURRENTLY at runtime. Enforce one statement per
/// no-transaction file so the mistake fails at PR time, not deploy time.
#[test]
fn no_transaction_migrations_are_single_statement() {
    for (name, migrator) in ALL_MIGRATORS {
        for migration in migrator.iter().filter(|migration| migration.no_tx) {
            let statements = count_sql_statements(migration.sql.as_ref());
            assert_eq!(
                statements, 1,
                "{name} migration {} ({}) is no-transaction but holds {statements} \
                 statements; Postgres runs a multi-statement simple-query batch in an \
                 implicit transaction, so keep no-transaction files to one statement",
                migration.version, migration.description,
            );
        }
    }
}

/// Counts SQL statements after stripping `--` line comments. Good enough
/// for migration files; would miscount a `--` or `;` inside a string
/// literal, which no bundled migration uses in a no-transaction file.
fn count_sql_statements(sql: &str) -> usize {
    sql.lines()
        .map(|line| line.split_once("--").map_or(line, |(code, _)| code))
        .collect::<Vec<_>>()
        .join("\n")
        .split(';')
        .filter(|chunk| !chunk.trim().is_empty())
        .count()
}

#[tokio::test]
async fn common_schema_converges_on_fresh_consumer_databases() {
    for database in sqlite_databases_with_common_schema() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join(format!("{}.db", database.name));

        migrate_sqlite_up(database, &db_path).await.unwrap();
        assert_common_jobs_table_matches_reader(&db_path);
        assert_common_schema_migration_applied(
            &db_path,
            COMMON_SCHEMA_MIGRATOR
                .iter()
                .filter(|migration| migration.migration_type.is_up_migration())
                .map(|migration| migration.version)
                .max()
                .unwrap(),
        );

        migrate_sqlite_up(database, &db_path).await.unwrap();
        assert_common_jobs_table_matches_reader(&db_path);
        if database.name == "build-manager" {
            assert!(!index_exists(&db_path, "idx_build_jobs_active_template"));
        }
    }
}

fn sqlite_databases_with_common_schema() -> [SqliteDatabase; 5] {
    [
        SqliteDatabase::from(pplx_space_sbx_mgr_store::db::migration::target()),
        sqlite_template_manager(),
        sqlite_volume_manager(),
        sqlite_build_manager(),
        sqlite_runtime_manager(),
    ]
}

fn assert_common_jobs_table_matches_reader(db_path: &Path) {
    let expected = pplx_space_common_jobs::db::JOB_TABLE_COLUMNS
        .iter()
        .map(|column| column.to_string())
        .collect::<Vec<_>>();
    assert_eq!(jobs_columns(db_path), expected);

    let actual = expected.iter().map(String::as_str).collect::<BTreeSet<_>>();
    for column in pplx_space_common_jobs::db::selected_job_columns() {
        assert!(
            actual.contains(column),
            "jobs schema is missing selected column {column}"
        );
    }
    assert!(index_exists(db_path, "idx_jobs_lease_expiry"));
    assert!(index_exists(db_path, "idx_jobs_dedup_latest"));
    assert!(index_exists(db_path, "idx_jobs_created_at"));
    assert!(index_exists(db_path, "idx_jobs_status_created_at"));
    assert!(index_exists(db_path, "idx_jobs_subject_created_at"));
    assert!(index_exists(db_path, "idx_jobs_active_kind_status"));
    assert!(index_exists(db_path, "idx_jobs_pending_priority"));
}

fn assert_common_schema_migration_applied(db_path: &Path, version: i64) {
    let conn = Connection::open(db_path).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM _space_common_schema_migrations WHERE version = ?1 AND success = TRUE",
            params![version],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}

fn jobs_columns(db_path: &Path) -> Vec<String> {
    let conn = Connection::open(db_path).unwrap();
    let mut stmt = conn
        .prepare("SELECT name FROM pragma_table_info('jobs') ORDER BY cid")
        .unwrap();
    stmt.query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn index_exists(db_path: &Path, index: &str) -> bool {
    let conn = Connection::open(db_path).unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
        params![index],
        |row| row.get::<_, i64>(0),
    )
    .map(|count| count > 0)
    .unwrap()
}

mod postgres_baseline_tests;
mod public_baseline_rollback_tests;

fn assert_unique_migration_versions(name: &str, migrator: &Migrator) {
    let mut up_versions = BTreeSet::new();
    let mut down_versions = BTreeSet::new();

    for migration in migrator.iter() {
        if migration.migration_type.is_up_migration() {
            assert!(
                up_versions.insert(migration.version),
                "{name} has duplicate up migration version {}",
                migration.version
            );
            continue;
        }

        assert!(
            down_versions.insert(migration.version),
            "{name} has duplicate down migration version {}",
            migration.version
        );
    }
}

#[tokio::test]
async fn migration_creates_a_missing_database_parent() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("new/directory/volume-manager.db");

    migrate_sqlite_up(sqlite_volume_manager(), &db_path)
        .await
        .unwrap();

    assert!(db_path.exists());
    assert_common_jobs_table_matches_reader(&db_path);
}

fn common_history(conn: &Connection) -> rusqlite::Result<std::collections::BTreeMap<i64, Vec<u8>>> {
    conn.prepare("SELECT version, checksum FROM _space_common_schema_migrations ORDER BY version")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect()
}

async fn migrate_common(path: &Path, migrator: &Migrator) {
    let pool = sqlite_pool(path).await.unwrap();
    sqlite::common_migrator(migrator).run(&pool).await.unwrap();
    pool.close().await;
}
