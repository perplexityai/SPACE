use std::{borrow::Cow, sync::LazyLock};

use super::*;
use pplx_space_common_svc::sql::baselines;
use sqlx::SqlSafeStr as _;
use sqlx::migrate::{Migration, MigrationType};

static SERVICE: LazyLock<Migrator> = LazyLock::new(|| Migrator {
    migrations: Cow::Owned(vec![
        Migration::new(
            baselines::PUBLIC_SQLITE_BASELINE,
            "baseline".into(),
            MigrationType::ReversibleUp,
            "CREATE TABLE service_state (id TEXT PRIMARY KEY);".into_sql_str(),
            false,
        ),
        Migration::new(
            baselines::PUBLIC_SQLITE_BASELINE,
            "baseline".into(),
            MigrationType::ReversibleDown,
            "CREATE TEMP TABLE rollback_guard (value INTEGER);
             CREATE TEMP TRIGGER refuse_baseline_rollback BEFORE INSERT ON rollback_guard
             BEGIN SELECT RAISE(ABORT, 'baseline rollback refused'); END;
             INSERT INTO rollback_guard VALUES (1);"
                .into_sql_str(),
            false,
        ),
        Migration::new(
            baselines::PUBLIC_SQLITE_BASELINE + 2,
            "service index".into(),
            MigrationType::ReversibleUp,
            "CREATE INDEX service_state_id ON service_state (id);".into_sql_str(),
            false,
        ),
        Migration::new(
            baselines::PUBLIC_SQLITE_BASELINE + 2,
            "service index".into(),
            MigrationType::ReversibleDown,
            "DROP INDEX service_state_id;".into_sql_str(),
            false,
        ),
    ]),
    ..Migrator::DEFAULT
});

#[tokio::test]
async fn service_down_reaches_baseline_without_changing_shared_jobs_or_history() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("service.sqlite");
    let database = SqliteDatabase {
        name: "baseline-rollback-test",
        default_path: "unused",
        migrator: &SERVICE,
        common_schema: true,
    };
    migrate_sqlite_up(database, &path).await.unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "INSERT INTO jobs (job_id, kind, status, lease_owner) VALUES ('retained', 'test', 'running', 'owner')",
        [],
    )
    .unwrap();
    let history = common_history(&conn).unwrap();

    rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
        .await
        .unwrap();

    assert!(
        !conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'service_state_id')",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap()
    );
    assert_eq!(common_history(&conn).unwrap(), history);
    assert_eq!(
        conn.query_row("SELECT MAX(version) FROM _sqlx_migrations", [], |row| row
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        baselines::PUBLIC_SQLITE_BASELINE
    );
    let schema: i64 = conn
        .pragma_query_value(None, "schema_version", |row| row.get(0))
        .unwrap();

    let result = rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR).await;

    assert!(result.is_err());
    assert_eq!(common_history(&conn).unwrap(), history);
    assert_eq!(
        conn.pragma_query_value(None, "schema_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        schema
    );
    assert_eq!(
        conn.query_row(
            "SELECT lease_owner FROM jobs WHERE job_id = 'retained'",
            [],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        "owner"
    );
}

#[tokio::test]
async fn mixed_catalog_down_removes_the_newest_migration_and_reopens_the_previous_release() {
    let baseline = baselines::PUBLIC_SQLITE_BASELINE;
    for common_version in [baseline + 1, baseline + 3] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("mixed.sqlite");
        let database = SqliteDatabase {
            name: "mixed-rollback-test",
            default_path: "unused",
            migrator: &SERVICE,
            common_schema: true,
        };
        let mut common = COMMON_SCHEMA_MIGRATOR.iter().cloned().collect::<Vec<_>>();
        common.extend([
            Migration::new(
                common_version,
                "shared index".into(),
                MigrationType::ReversibleUp,
                "CREATE INDEX shared_rollback_probe ON jobs (lease_owner);".into_sql_str(),
                false,
            ),
            Migration::new(
                common_version,
                "shared index".into(),
                MigrationType::ReversibleDown,
                "DROP INDEX shared_rollback_probe;".into_sql_str(),
                false,
            ),
        ]);
        let common = Migrator::with_migrations(common);
        migrate_sqlite_up(database, &path).await.unwrap();
        let conn = Connection::open(&path).unwrap();
        migrate_common(&path, &common).await;
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status) VALUES ('retained', 'test', 'running')",
            [],
        )
        .unwrap();

        rollback_last_sqlite_database(database, &path, &common)
            .await
            .unwrap();

        let common_was_newest = common_version > baseline + 2;
        let service_version = if common_was_newest {
            baseline + 2
        } else {
            baseline
        };
        let shared_version = if common_was_newest {
            baseline
        } else {
            common_version
        };
        assert_eq!(
            conn.query_row("SELECT MAX(version) FROM _sqlx_migrations", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            service_version
        );
        assert_eq!(
            common_history(&conn).unwrap().keys().copied().max(),
            Some(shared_version)
        );
        let previous_service = Migrator::with_migrations(
            SERVICE
                .iter()
                .filter(|m| m.version <= service_version)
                .cloned()
                .collect(),
        );
        let previous_common = Migrator::with_migrations(
            common
                .iter()
                .filter(|m| m.version <= shared_version)
                .cloned()
                .collect(),
        );
        migrate_common(&path, &previous_common).await;
        let pool = sqlite_pool(&path).await.unwrap();
        previous_service.run(&pool).await.unwrap();
        pool.close().await;
        assert_eq!(
            conn.query_row(
                "SELECT status FROM jobs WHERE job_id = 'retained'",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            "running"
        );
    }
}
