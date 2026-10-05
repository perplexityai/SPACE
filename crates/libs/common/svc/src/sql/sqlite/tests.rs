use std::sync::{Arc, Mutex};

use pplx_space_observability::test_capture::{RecordingVisitor, field_value};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt as _};

use super::*;

#[derive(Clone, Default)]
struct CapturedSqliteStatements {
    statements: Arc<Mutex<Vec<String>>>,
}

impl<S: Subscriber> Layer<S> for CapturedSqliteStatements {
    fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
        if event.metadata().target() != "rusqlite::query" {
            return;
        }

        let mut visitor = RecordingVisitor::default();
        event.record(&mut visitor);
        let fields = visitor.into_fields();
        if let Some(statement) = field_value(&fields, "db.statement") {
            self.statements.lock().unwrap().push(statement.to_string());
        }
    }
}

#[test]
fn sqlite_query_summary_uses_operation_name() {
    let cases = [
        ("select * from sandboxes", "sqlite SELECT"),
        ("  INSERT INTO sandboxes VALUES (1)", "sqlite INSERT"),
        ("pragma journal_mode=WAL", "sqlite PRAGMA"),
        (
            "WITH stale AS (SELECT 1) SELECT * FROM stale",
            "sqlite query",
        ),
        (
            "/*pplx_space_common_svc::sql::sqlite:1*/ SELECT 1",
            "sqlite SELECT",
        ),
    ];

    for (sql, expected) in cases {
        let actual = sqlite_query_summary(sql);

        assert_eq!(actual, expected);
    }
}

#[test]
fn rusqlite_profile_emits_statement_without_bind_values() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("test.db");
    let captured = CapturedSqliteStatements::default();
    let statements = captured.statements.clone();
    let subscriber = tracing_subscriber::registry().with(captured);
    let secret = "secret-sandbox-value";

    tracing::subscriber::with_default(subscriber, || {
        let conn = open_rusqlite(&db_path, RusqliteOpenOptions::default()).unwrap();
        conn.execute_batch("CREATE TABLE secrets (value TEXT);")
            .unwrap();
        conn.execute(
            "INSERT INTO secrets (value) VALUES (?1)",
            rusqlite::params![secret],
        )
        .unwrap();
    });

    let statements = statements.lock().unwrap();
    let insert = statements
        .iter()
        .find(|statement| statement.contains("INSERT INTO secrets"))
        .expect("captured INSERT profile event");

    assert!(
        insert.contains("?1"),
        "expected raw SQL with placeholder, got {insert}"
    );
    assert!(
        !insert.contains(secret),
        "profiled db.statement leaked bind value: {insert}"
    );
}

#[tokio::test]
async fn node_manager_migration_chain_preserves_leases_on_history_failure() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("node-manager.db");
    let migrator =
        Migrator::new(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../../db/node-manager"))
            .await
            .unwrap();

    migrate("node-manager", &db_path, &migrator).await.unwrap();
    let conn = open_rusqlite(&db_path, RusqliteOpenOptions::default()).unwrap();
    conn.execute_batch(
        "INSERT INTO network_slots
        (slot_id, slot_label, generation, schema_version, state, health, sandbox_id, sandbox_uid,
         memory_reservation_mib, created_at, updated_at, lease_kind)
        VALUES (0, 'slot-0', 7, 1, 'leased', 'healthy', 'sandbox-a', 10000, 8320, 1, 1, 'pooled');",
    )
    .unwrap();
    drop(conn);
    // A compatible reopen must preserve the real migrated lease schema.
    migrate("node-manager", &db_path, &migrator).await.unwrap();
    let conn = open_rusqlite(&db_path, RusqliteOpenOptions::default()).unwrap();
    conn.execute(
        "UPDATE _sqlx_migrations SET checksum = X'00' WHERE version = ?1",
        [migrator
            .iter()
            .filter(|migration| migration.migration_type.is_up_migration())
            .map(|migration| migration.version)
            .max()
            .unwrap()],
    )
    .unwrap();
    drop(conn);
    migrate("node-manager", &db_path, &migrator)
        .await
        .unwrap_err();
    let conn = open_rusqlite(&db_path, RusqliteOpenOptions::default()).unwrap();
    let lease = conn.query_row("SELECT sandbox_id, generation, memory_reservation_mib FROM network_slots WHERE slot_id = 0", [], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?))
    }).unwrap();
    assert_eq!(lease, ("sandbox-a".into(), 7, 8320));
}

#[tokio::test]
async fn migrate_preserves_untracked_schema() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("test.db");
    let migrations_dir = temp.path().join("migrations");
    std::fs::create_dir(&migrations_dir).unwrap();
    std::fs::write(
        migrations_dir.join("001_initial.up.sql"),
        "CREATE TABLE current_schema (id INTEGER PRIMARY KEY);\n",
    )
    .unwrap();
    std::fs::write(
        migrations_dir.join("001_initial.down.sql"),
        "DROP TABLE current_schema;\n",
    )
    .unwrap();

    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE stale_schema (id INTEGER PRIMARY KEY); INSERT INTO stale_schema VALUES (42);",
    )
    .unwrap();
    drop(conn);

    let migrator = Migrator::new(migrations_dir).await.unwrap();
    let error = migrate("test", &db_path, &migrator).await.unwrap_err();
    assert!(error.to_string().contains("failed to migrate test SQLite"));

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&db_path)
                .create_if_missing(false),
        )
        .await
        .unwrap();
    assert!(table_exists(&pool, "stale_schema").await.unwrap());
    assert!(!table_exists(&pool, "current_schema").await.unwrap());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(sql!("SELECT id FROM stale_schema"))
            .fetch_one(&pool)
            .await
            .unwrap(),
        42
    );
}

#[tokio::test]
async fn migrate_rejects_unknown_migration_version() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("test.db");
    let migrations_dir = temp.path().join("migrations");
    std::fs::create_dir(&migrations_dir).unwrap();
    write_table_migration(&migrations_dir, 1, "initial");
    write_table_migration(&migrations_dir, 3, "latest");
    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE _sqlx_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            success BOOLEAN NOT NULL,
            checksum BLOB NOT NULL,
            execution_time BIGINT NOT NULL
        );
        INSERT INTO _sqlx_migrations
            (version, description, success, checksum, execution_time)
        VALUES (2, 'unknown', TRUE, x'00', 0);
        "#,
    )
    .unwrap();
    drop(conn);
    let migrator = Migrator::new(migrations_dir).await.unwrap();

    let error = migrate("test", &db_path, &migrator).await.unwrap_err();

    assert!(format!("{error:#}").contains("migration 2"), "{error:#}");
}

#[tokio::test]
async fn migrate_rejects_legacy_history_without_restamping() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("test.db");
    let migrations_dir = temp.path().join("migrations");
    std::fs::create_dir(&migrations_dir).unwrap();
    std::fs::write(
        migrations_dir.join("002_baseline.up.sql"),
        "CREATE TABLE baseline_schema (id INTEGER PRIMARY KEY);\n",
    )
    .unwrap();
    std::fs::write(
        migrations_dir.join("002_baseline.down.sql"),
        "DROP TABLE baseline_schema;\n",
    )
    .unwrap();
    std::fs::write(
        migrations_dir.join("003_next.up.sql"),
        "CREATE TABLE next_schema (id INTEGER PRIMARY KEY);\n",
    )
    .unwrap();
    std::fs::write(
        migrations_dir.join("003_next.down.sql"),
        "DROP TABLE next_schema;\n",
    )
    .unwrap();

    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE baseline_schema (id INTEGER PRIMARY KEY);
        CREATE TABLE _sqlx_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            success BOOLEAN NOT NULL,
            checksum BLOB NOT NULL,
            execution_time BIGINT NOT NULL
        );
        INSERT INTO _sqlx_migrations
            (version, description, success, checksum, execution_time)
        VALUES
            (1, 'old', TRUE, x'00', 0),
            (2, 'old_baseline', TRUE, x'00', 0);
        "#,
    )
    .unwrap();
    drop(conn);

    let migrator = Migrator::new(migrations_dir).await.unwrap();
    migrate("test", &db_path, &migrator).await.unwrap_err();

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&db_path)
                .create_if_missing(false),
        )
        .await
        .unwrap();
    assert!(table_exists(&pool, "baseline_schema").await.unwrap());
    assert!(!table_exists(&pool, "next_schema").await.unwrap());

    let versions: Vec<i64> = sqlx::query_scalar(sql!(
        "SELECT version FROM _sqlx_migrations ORDER BY version"
    ))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(versions, vec![1, 2]);
    let preserved_checksum: Vec<u8> = sqlx::query_scalar(sql!(
        "SELECT checksum FROM _sqlx_migrations WHERE version = 2"
    ))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(preserved_checksum, vec![0]);
}

#[tokio::test]
async fn migrate_rejects_goose_history_without_importing() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("test.db");
    let migrations_dir = temp.path().join("migrations");
    std::fs::create_dir(&migrations_dir).unwrap();
    std::fs::write(
        migrations_dir.join("002_baseline.up.sql"),
        "CREATE TABLE baseline_schema (id INTEGER PRIMARY KEY);\n",
    )
    .unwrap();
    std::fs::write(
        migrations_dir.join("002_baseline.down.sql"),
        "DROP TABLE baseline_schema;\n",
    )
    .unwrap();
    std::fs::write(
        migrations_dir.join("003_next.up.sql"),
        "CREATE TABLE next_schema (id INTEGER PRIMARY KEY);\n",
    )
    .unwrap();
    std::fs::write(
        migrations_dir.join("003_next.down.sql"),
        "DROP TABLE next_schema;\n",
    )
    .unwrap();

    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE baseline_schema (id INTEGER PRIMARY KEY);
        CREATE TABLE goose_db_version (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_id INTEGER NOT NULL,
            is_applied BOOLEAN NOT NULL,
            tstamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO goose_db_version (version_id, is_applied)
        VALUES
            (1, 1),
            (2, 1);
        "#,
    )
    .unwrap();
    drop(conn);

    let migrator = Migrator::new(migrations_dir).await.unwrap();
    migrate("test", &db_path, &migrator).await.unwrap_err();

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&db_path)
                .create_if_missing(false),
        )
        .await
        .unwrap();
    assert!(table_exists(&pool, "baseline_schema").await.unwrap());
    assert!(!table_exists(&pool, "next_schema").await.unwrap());

    assert!(!table_exists(&pool, "_sqlx_migrations").await.unwrap());
    let versions: Vec<i64> = sqlx::query_scalar(sql!(
        "SELECT version_id FROM goose_db_version WHERE is_applied = 1 ORDER BY version_id"
    ))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(versions, vec![1, 2]);
}

fn write_table_migration(migrations_dir: &Path, version: i64, name: &str) {
    let prefix = format!("{version:03}_{name}");
    std::fs::write(
        migrations_dir.join(format!("{prefix}.up.sql")),
        format!("CREATE TABLE {name}_schema (id INTEGER PRIMARY KEY);\n"),
    )
    .unwrap();
    std::fs::write(
        migrations_dir.join(format!("{prefix}.down.sql")),
        format!("DROP TABLE {name}_schema;\n"),
    )
    .unwrap();
}

#[test]
fn common_schema_accepts_registered_revisions_and_preserves_history() {
    use sqlx::SqlSafeStr as _;
    use sqlx::migrate::{Migration, MigrationType};
    use std::borrow::Cow;

    let revisions: Vec<_> = ["original", "intermediate", "current", "unknown"]
        .iter()
        .map(|comment| {
            Migration::new(
                101,
                Cow::Borrowed("checksum probe"),
                MigrationType::Simple,
                sqlx::AssertSqlSafe(format!(
                    "-- {comment}\nCREATE TABLE checksum_probe (id INTEGER);"
                ))
                .into_sql_str(),
                false,
            )
        })
        .collect();
    let digests: Vec<_> = revisions[..3]
        .iter()
        .map(|m| hex::encode(&m.checksum))
        .collect();
    let registry = AcceptedChecksums::from_toml(&format!(
        "[[migrations]]\nstore = 'common'\nversion = 101\naccepted_checksums = {digests:?}"
    ))
    .unwrap();
    let current = Migrator::with_migrations(vec![revisions[2].clone()]);

    for previous in [None, Some(0), Some(1), Some(2), Some(3)] {
        let mut conn = Connection::open_in_memory().unwrap();
        if let Some(previous) = previous {
            let old = Migrator::with_migrations(vec![revisions[previous].clone()]);
            migrate_common_schema_up(&mut conn, &old).unwrap();
        }

        let result = migrate_common_schema_with_checksums(&mut conn, &current, &registry);

        assert_eq!(result.is_ok(), previous != Some(3));
        let applied = applied_common_schema_migrations(&conn).unwrap();
        assert_eq!(
            applied[&101],
            revisions[previous.unwrap_or(2)].checksum.as_ref()
        );
    }
}

#[tokio::test]
async fn public_baseline_preserves_rejected_history() {
    let version = super::super::baselines::PUBLIC_SQLITE_BASELINE;
    for damage in [
        "UPDATE _sqlx_migrations SET checksum = x'00'",
        "UPDATE _sqlx_migrations SET success = FALSE",
        "UPDATE _sqlx_migrations SET version = 1",
        "UPDATE _sqlx_migrations SET version = version + 1",
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time)
         SELECT version + 1, description, success, checksum, execution_time FROM _sqlx_migrations",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("test.db");
        let migrations_dir = temp.path().join("migrations");
        std::fs::create_dir(&migrations_dir).unwrap();
        std::fs::write(
            migrations_dir.join(format!("{version}_baseline.up.sql")),
            "CREATE TABLE evidence (value TEXT NOT NULL);",
        )
        .unwrap();
        let mut migrator = Migrator::new(migrations_dir).await.unwrap();
        migrate("test", &db_path, &migrator).await.unwrap();
        migrate("test", &db_path, &migrator).await.unwrap();
        let conn = Connection::open(&db_path).unwrap();
        conn.execute("INSERT INTO evidence VALUES ('retained')", [])
            .unwrap();
        conn.execute_batch(damage).unwrap();
        let history = |conn: &Connection| {
            conn.prepare("SELECT version, success, checksum FROM _sqlx_migrations ORDER BY version")
                .unwrap()
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, bool>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        let before = history(&conn);
        drop(conn);

        migrator.set_ignore_missing(true);
        let result = migrate("test", &db_path, &migrator).await;

        assert!(result.is_err(), "accepted {damage}");
        let conn = Connection::open(&db_path).unwrap();
        assert_eq!(history(&conn), before);
        assert_eq!(
            conn.query_row("SELECT value FROM evidence", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            "retained"
        );
    }
}

fn public_test_migrators() -> (&'static Migrator, &'static Migrator) {
    static MIGRATORS: std::sync::LazyLock<(Migrator, Migrator)> = std::sync::LazyLock::new(|| {
        use sqlx::SqlSafeStr as _;
        use sqlx::migrate::MigrationType;
        use std::borrow::Cow;

        let migration = |sql: &'static str| {
            Migrator::with_migrations(vec![Migration::new(
                super::super::baselines::PUBLIC_SQLITE_BASELINE,
                Cow::Borrowed("baseline"),
                MigrationType::Simple,
                sql.into_sql_str(),
                false,
            )])
        };
        (
            migration("CREATE INDEX jobs_by_status ON jobs(status);"),
            migration(
                "CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT, lease_owner TEXT, payload TEXT);",
            ),
        )
    });
    (&MIGRATORS.0, &MIGRATORS.1)
}

#[tokio::test]
async fn public_embedded_migrations_reject_both_histories_before_hooks_or_writes() {
    let (service, common) = public_test_migrators();
    let migrators = EmbeddedMigrators {
        service,
        common: Some(common),
    };
    for table in ["_sqlx_migrations", "_space_common_schema_migrations"] {
        for change in ["checksum", "failed", "legacy", "future", "missing"] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("database.sqlite");
            migrate_embedded("test", &path, migrators, None)
                .await
                .unwrap();
            let conn = Connection::open(&path).unwrap();
            conn.execute(
                "INSERT INTO jobs VALUES ('retained', 'running', 'owner', 'intent')",
                [],
            )
            .unwrap();
            let mutation = match change {
                "checksum" => format!("UPDATE {table} SET checksum = x'00'"),
                "failed" => format!("UPDATE {table} SET success = 0"),
                "legacy" => format!("UPDATE {table} SET version = 1"),
                "future" => format!("UPDATE {table} SET version = version + 1"),
                "missing" => format!("DELETE FROM {table}"),
                _ => unreachable!(),
            };
            conn.execute_batch(&mutation).unwrap();
            let before = embedded_history(&conn);
            let schema_before: i64 = conn
                .pragma_query_value(None, "schema_version", |row| row.get(0))
                .unwrap();
            drop(conn);

            let result = migrate_embedded(
                "test",
                &path,
                migrators,
                Some(|_| panic!("compatibility hook ran before history admission")),
            )
            .await;

            assert!(result.is_err(), "{table} {change}");
            let conn = Connection::open(&path).unwrap();
            assert_eq!(embedded_history(&conn), before);
            assert_eq!(
                conn.pragma_query_value(None, "schema_version", |row| row.get::<_, i64>(0))
                    .unwrap(),
                schema_before
            );
            let row: (String, String, String) = conn
                .query_row(
                    "SELECT status, lease_owner, payload FROM jobs WHERE job_id = 'retained'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(row, ("running".into(), "owner".into(), "intent".into()));
        }
    }
}

fn embedded_history(conn: &Connection) -> Vec<(String, i64, bool, Vec<u8>)> {
    conn.prepare("SELECT 'service', version, success, checksum FROM _sqlx_migrations UNION ALL SELECT 'common', version, success, checksum FROM _space_common_schema_migrations ORDER BY 1, 2")
        .unwrap().query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).unwrap().collect::<Result<_, _>>().unwrap()
}

#[tokio::test]
async fn public_common_with_legacy_service_preserves_rejected_store() {
    use sqlx::SqlSafeStr as _;
    use sqlx::migrate::MigrationType;
    use std::{borrow::Cow, sync::LazyLock};

    static SERVICE: LazyLock<Migrator> = LazyLock::new(|| {
        Migrator::with_migrations(vec![Migration::new(
            1,
            Cow::Borrowed("initial"),
            MigrationType::Simple,
            "CREATE TABLE service_state (id TEXT PRIMARY KEY);".into_sql_str(),
            false,
        )])
    });
    static COMMON: LazyLock<Migrator> = LazyLock::new(|| {
        let mut migrations = public_test_migrators()
            .1
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        migrations[0].version = 1;
        Migrator::with_migrations(migrations)
    });
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database.sqlite");
    migrate_embedded(
        "test",
        &path,
        EmbeddedMigrators {
            service: &SERVICE,
            common: Some(&COMMON),
        },
        None,
    )
    .await
    .unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "INSERT INTO jobs VALUES ('retained', 'running', 'owner', 'intent')",
        [],
    )
    .unwrap();
    let before = embedded_history(&conn);
    let schema: i64 = conn
        .pragma_query_value(None, "schema_version", |row| row.get(0))
        .unwrap();
    drop(conn);

    let result = migrate_embedded(
        "test",
        &path,
        EmbeddedMigrators {
            service: &SERVICE,
            common: Some(public_test_migrators().1),
        },
        None,
    )
    .await;

    assert!(result.is_err());
    let conn = Connection::open(&path).unwrap();
    assert_eq!(embedded_history(&conn), before);
    assert_eq!(
        conn.pragma_query_value(None, "schema_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        schema
    );
    assert_eq!(
        conn.query_row("SELECT lease_owner FROM jobs", [], |row| row
            .get::<_, String>(0))
            .unwrap(),
        "owner"
    );
}

#[tokio::test]
async fn public_embedded_migrations_resume_after_common_schema_initialization() {
    let (service, common) = public_test_migrators();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database.sqlite");
    let mut conn = Connection::open(&path).unwrap();
    migrate_common_schema_up(&mut conn, common).unwrap();
    conn.execute(
        "INSERT INTO jobs VALUES ('retained', 'running', 'owner', 'intent')",
        [],
    )
    .unwrap();
    drop(conn);

    for _ in 0..2 {
        migrate_embedded(
            "test",
            &path,
            EmbeddedMigrators {
                service,
                common: Some(common),
            },
            None,
        )
        .await
        .unwrap();
    }

    let conn = Connection::open(&path).unwrap();
    assert_eq!(embedded_history(&conn).len(), 2);
    assert_eq!(
        conn.query_row("SELECT payload FROM jobs", [], |row| row
            .get::<_, String>(0))
            .unwrap(),
        "intent"
    );
}
