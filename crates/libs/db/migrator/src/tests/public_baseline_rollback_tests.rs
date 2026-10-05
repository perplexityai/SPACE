use std::{borrow::Cow, sync::LazyLock};

use super::*;
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
    ]),
    ..Migrator::DEFAULT
});

#[tokio::test]
async fn shared_baseline_down_preserves_shared_jobs_and_history() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("service.sqlite");
    let database = SqliteDatabase {
        name: "baseline-rollback-test",
        default_path: "unused",
        migrator: &SERVICE,
        store: baselines::VOLUME_MANAGER,
        common_schema: true,
        compatibility_hook: None,
        downgrade_admission: None,
    };
    migrate_sqlite_up(database, &path, MigrationFailurePolicy::Fail)
        .await
        .unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "INSERT INTO jobs (job_id, kind, status, lease_owner) VALUES ('retained', 'test', 'running', 'owner')",
        [],
    )
    .unwrap();
    let history = sqlite::applied_common_schema_migrations(&conn).unwrap();
    let schema: i64 = conn
        .pragma_query_value(None, "schema_version", |row| row.get(0))
        .unwrap();

    let result = rollback_last_sqlite_database(database, &path).await;

    assert!(format!("{:?}", result.unwrap_err()).contains("public baseline is a rollback floor"));
    assert_eq!(
        sqlite::applied_common_schema_migrations(&conn).unwrap(),
        history
    );
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
