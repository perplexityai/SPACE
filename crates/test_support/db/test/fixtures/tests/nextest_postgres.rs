use std::env;

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::pg_test_db;

const POSTGRES_ADMIN_URL_ENV: &str = "SPACE_TEST_POSTGRES_ADMIN_URL";
const TESTCONTAINERS_POSTGRES_IMAGE_ENV: &str = "TESTCONTAINERS_POSTGRES_IMAGE";

#[tokio::test]
async fn nextest_postgres_reuses_server_with_isolated_databases()
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

    let first = pg_test_db().await?;
    let second = pg_test_db().await?;
    let first_database: String = sqlx::query_scalar(sql!("SELECT current_database()"))
        .fetch_one(&first.pool)
        .await?;
    let second_database: String = sqlx::query_scalar(sql!("SELECT current_database()"))
        .fetch_one(&second.pool)
        .await?;
    assert_ne!(first_database, second_database);

    sqlx::query(sql!(
        "CREATE TABLE nextest_isolation_marker (id integer PRIMARY KEY)"
    ))
    .execute(&first.pool)
    .await?;
    let marker_in_second: Option<String> =
        sqlx::query_scalar(sql!("SELECT to_regclass('nextest_isolation_marker')::text"))
            .fetch_one(&second.pool)
            .await?;
    assert_eq!(marker_in_second, None);

    Ok(())
}
