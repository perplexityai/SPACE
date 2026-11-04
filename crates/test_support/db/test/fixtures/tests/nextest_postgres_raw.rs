use std::env;

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::raw_pg_test_db;

const POSTGRES_ADMIN_URL_ENV: &str = "SPACE_TEST_POSTGRES_ADMIN_URL";
const TESTCONTAINERS_POSTGRES_IMAGE_ENV: &str = "TESTCONTAINERS_POSTGRES_IMAGE";

#[tokio::test]
async fn raw_pg_test_db_reuses_server_but_skips_the_migrated_template()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if env::var_os("NEXTEST").is_none() {
        return Ok(());
    }

    env::var(POSTGRES_ADMIN_URL_ENV).expect("nextest must provide the shared Postgres URL");

    // The invalid image proves the shared server is used. Configure it
    // before starting the child, without mutating this process's environment.
    if !pplx_space_test_env::enter_test_process(&[(
        TESTCONTAINERS_POSTGRES_IMAGE_ENV,
        Some(std::ffi::OsStr::new("fixture-must-not-start-a-container")),
    )]) {
        return Ok(());
    }

    let first = raw_pg_test_db().await?;
    let second = raw_pg_test_db().await?;

    let first_database: String = sqlx::query_scalar(sql!("SELECT current_database()"))
        .fetch_one(&first.pool)
        .await?;
    let second_database: String = sqlx::query_scalar(sql!("SELECT current_database()"))
        .fetch_one(&second.pool)
        .await?;
    assert_ne!(first_database, second_database);

    // Unlike `pg_test_db`, a raw database carries no migration history: a
    // caller that manually creates `_sqlx_migrations` (to engineer a specific
    // collision-repair scenario) must not find it already present.
    let migrations_table: Option<String> =
        sqlx::query_scalar(sql!("SELECT to_regclass('_sqlx_migrations')::text"))
            .fetch_one(&first.pool)
            .await?;
    assert_eq!(migrations_table, None);

    Ok(())
}
