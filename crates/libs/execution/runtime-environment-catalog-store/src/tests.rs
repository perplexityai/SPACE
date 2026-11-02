use pplx_space_common_core::sql;
use pplx_space_common_core::types::{Digest, EnvironmentReference};
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;

fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

/// Insert a catalog row as a node publish would, retired or not.
async fn insert(pool: &PgPool, environment: &str, retired: bool) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO runtime_environments (
             environment_ref, runtime, runtime_version, version_sort_key,
             kernel_version, state_format_name, state_format_version, retired_at
         )
         VALUES ($1, 'firecracker', '1.17.0', '{1,17,0}', '6.1.155',
                 'firecracker-snapshot', '12.0.0',
                 CASE WHEN $2 THEN now() END)"
    ))
    .bind(environment)
    .bind(retired)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retired_environments_are_left_out() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    insert(&db.pool, &environment(2).to_string(), false).await?;
    insert(&db.pool, &environment(1).to_string(), true).await?;
    insert(&db.pool, &environment(0).to_string(), false).await?;

    assert_eq!(
        list_unretired(&db.pool).await?,
        vec![environment(0), environment(2)]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_unparseable_reference_fails_the_read() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // The schema refuses a malformed reference, so drop its CHECK in this
    // isolated database to stand in for a row the parser rejects.
    sqlx::query(sql!(
        "ALTER TABLE runtime_environments
         DROP CONSTRAINT runtime_environments_environment_ref_check"
    ))
    .execute(&db.pool)
    .await?;
    insert(&db.pool, &environment(0).to_string(), false).await?;
    insert(&db.pool, "sha256:not-a-digest", false).await?;

    let error = list_unretired(&db.pool)
        .await
        .expect_err("a malformed reference must fail the whole read");
    assert!(
        matches!(
            &error,
            ListUnretiredError::InvalidEnvironmentReference { value, .. }
                if value == "sha256:not-a-digest"
        ),
        "{error:?}"
    );
    Ok(())
}
