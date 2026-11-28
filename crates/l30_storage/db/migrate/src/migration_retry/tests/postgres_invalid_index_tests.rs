//! Live-Postgres coverage for the INVALID index read the migrator logs, both
//! after a `-- no-transaction` migration fails and before a run applies
//! anything.

use std::time::Duration;

use pplx_space_db_test_fixtures::try_raw_pg_test_db_with_max_connections;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;

use super::super::*;
use super::{PROBE_VERSION, fast_policy, probe_migrator};
use crate::pg_test_support::TestResult;

/// `(index, table, ready, in_progress)` for each INVALID index.
async fn listed(pool: &PgPool) -> TestResult<Vec<(String, String, bool, bool)>> {
    Ok(invalid_indexes(pool)
        .await?
        .into_iter()
        .map(|index| (index.index, index.table, index.ready, index.in_progress))
        .collect())
}

/// Leave `invalid_index_probe_unique` INVALID the way a failed concurrent
/// build does: the duplicate rows fail it after Postgres has created the
/// index.
async fn fail_a_unique_concurrent_build(pool: &PgPool) -> TestResult {
    sqlx::query(sql!("CREATE TABLE invalid_index_probe (value INT)"))
        .execute(pool)
        .await?;
    sqlx::query(sql!("INSERT INTO invalid_index_probe VALUES (1), (1)"))
        .execute(pool)
        .await?;
    let error = sqlx::query(sql!(
        "CREATE UNIQUE INDEX CONCURRENTLY invalid_index_probe_unique \
         ON invalid_index_probe (value)"
    ))
    .execute(pool)
    .await
    .expect_err("a unique build over duplicate rows fails");
    assert_eq!(
        sqlstate(&error).as_deref(),
        Some("23505"),
        "expected unique_violation, got {error:?}"
    );
    Ok(())
}

#[tokio::test]
async fn lists_the_index_a_failed_concurrent_build_leaves_and_no_valid_one() -> TestResult {
    let Some(fixture) = try_raw_pg_test_db_with_max_connections(2).await? else {
        return Ok(());
    };
    fail_a_unique_concurrent_build(&fixture.pool).await?;
    sqlx::query(sql!(
        "CREATE INDEX invalid_index_probe_valid ON invalid_index_probe (value)"
    ))
    .execute(&fixture.pool)
    .await?;
    // Another session reading and writing the table holds weaker locks than a
    // build or drop takes.
    let mut writer = fixture.pool.begin().await?;
    sqlx::query(sql!("INSERT INTO invalid_index_probe VALUES (2)"))
        .execute(&mut *writer)
        .await?;
    sqlx::query(sql!("SELECT count(*) FROM invalid_index_probe"))
        .execute(&mut *writer)
        .await?;

    // Nothing is building the failed index any more.
    assert_eq!(
        listed(&fixture.pool).await?,
        [(
            "invalid_index_probe_unique".to_owned(),
            "invalid_index_probe".to_owned(),
            false,
            false
        )]
    );
    writer.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn marks_an_index_whose_concurrent_build_is_still_running() -> TestResult {
    let Some(fixture) = try_raw_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    sqlx::query(sql!("CREATE TABLE invalid_index_probe (value INT)"))
        .execute(&fixture.pool)
        .await?;
    // An open write transaction makes the build wait after it has created the
    // index, which stays INVALID until the transaction ends.
    let mut writer = fixture.pool.begin().await?;
    sqlx::query(sql!("INSERT INTO invalid_index_probe VALUES (1)"))
        .execute(&mut *writer)
        .await?;

    let build = sqlx::query(sql!(
        "CREATE INDEX CONCURRENTLY invalid_index_probe_building \
         ON invalid_index_probe (value)"
    ))
    .execute(&fixture.pool);
    let observe = async {
        let while_building = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let listed = listed(&fixture.pool).await?;
                if !listed.is_empty() {
                    return TestResult::Ok(listed);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the waiting build's index appears in pg_index")?;
        writer.commit().await?;
        TestResult::Ok(while_building)
    };
    let (build, while_building) = tokio::join!(build, observe);
    build?;

    assert_eq!(
        while_building?,
        [(
            "invalid_index_probe_building".to_owned(),
            "invalid_index_probe".to_owned(),
            false,
            true
        )]
    );
    assert_eq!(listed(&fixture.pool).await?, []);
    Ok(())
}

#[tokio::test]
async fn an_invalid_index_does_not_stop_the_run() -> TestResult {
    let Some(fixture) = try_raw_pg_test_db_with_max_connections(2).await? else {
        return Ok(());
    };
    fail_a_unique_concurrent_build(&fixture.pool).await?;
    let migrator = probe_migrator("CREATE TABLE applied_probe (id INT)".to_owned(), false);

    assert_eq!(run_up(&fixture.pool, &migrator, fast_policy(1)).await?, 1);
    let applied: Vec<i64> = sqlx::query_scalar(sql!("SELECT version FROM _sqlx_migrations"))
        .fetch_all(&fixture.pool)
        .await?;
    assert_eq!(applied, [PROBE_VERSION]);
    Ok(())
}
