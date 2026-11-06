use super::pg_test_support::TestResult;
use pplx_space_common_svc::sql;
use pplx_space_db_test_fixtures::try_pg_test_db;
use sqlx::Connection as _;

#[derive(Debug, PartialEq, sqlx::FromRow)]
struct LegacyRow {
    environment_ref: String,
    runtime: String,
    runtime_version: String,
    version_sort_key: Vec<i32>,
    kernel_version: String,
    state_format_name: String,
    state_format_version: String,
    retired_at: String,
}

const UP: &str = include_str!(
    "../../../../../db/postgres/20261106040410_runtime_environment_optional_components.up.sql"
);
const DOWN: &str = include_str!(
    "../../../../../db/postgres/20261106040410_runtime_environment_optional_components.down.sql"
);
const VALIDATE_UP: &str = include_str!(
    "../../../../../db/postgres/20261106040411_validate_runtime_environment_state_format_pair.up.sql"
);
const VALIDATE_DOWN: &str = include_str!(
    "../../../../../db/postgres/20261106040411_validate_runtime_environment_state_format_pair.down.sql"
);

#[tokio::test]
async fn optional_metadata_preserves_old_rows_and_downgrades_fail_closed() -> TestResult {
    let Some(fixture) = try_pg_test_db().await? else {
        return Ok(());
    };
    // The shared fixture supplies the real trigger functions and all other
    // schema dependencies. Return only this empty disposable catalog to the
    // preceding schema, then exercise its upgrade with an old-format row.
    // These are entire immutable checked-in migrations, never interpolated
    // predicates or input strings. sql!'s expression arm adds the locator.
    for migration in [VALIDATE_DOWN, DOWN] {
        let mut setup = fixture.pool.begin().await?;
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql!(migration)))
            .execute(&mut *setup)
            .await?;
        setup.commit().await?;
    }
    let reference = format!("sha256:{}", "11".repeat(32));
    sqlx::query(sql!(
        "INSERT INTO runtime_environments (
             environment_ref, runtime, runtime_version, version_sort_key,
             kernel_version, state_format_name, state_format_version, retired_at
         ) VALUES ($1, 'firecracker', '1.15.1', ARRAY[1,15,1],
                   '6.1.155', 'firecracker.snapshot', '9',
                   '2026-11-01T01:02:03Z'::timestamptz)"
    ))
    .bind(&reference)
    .execute(&fixture.pool)
    .await?;
    // Pin the old row's complete shape, including operator retirement, and
    // retain a prepared statement with the unchanged production read shape.
    let mut connection = fixture.pool.acquire().await?;
    let old: LegacyRow = sqlx::query_as(sql!(
        "SELECT environment_ref, runtime, runtime_version, version_sort_key,
                    kernel_version, state_format_name, state_format_version,
                    retired_at::text FROM runtime_environments"
    ))
    .fetch_one(&mut *connection)
    .await?;
    sqlx::query(sql!(
        "SELECT environment_ref FROM runtime_environments
         WHERE retired_at IS NULL ORDER BY environment_ref"
    ))
    .fetch_all(&mut *connection)
    .await?;
    for (migration, validated) in [(UP, false), (VALIDATE_UP, true)] {
        // Separate transactions are the locking contract of this migration
        // pair, not just an implementation detail of the test.
        let mut tx = connection.begin().await?;
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql!(migration)))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        let actual: (bool,) = sqlx::query_as(sql!(
            "SELECT convalidated FROM pg_constraint
             WHERE conrelid = 'runtime_environments'::regclass
               AND conname = 'runtime_environments_state_format_pair_check'"
        ))
        .fetch_one(&mut *connection)
        .await?;
        assert_eq!(actual.0, validated);
    }
    let preserved: LegacyRow = sqlx::query_as(sql!(
        "SELECT environment_ref, runtime, runtime_version, version_sort_key,
                    kernel_version, state_format_name, state_format_version,
                    retired_at::text FROM runtime_environments"
    ))
    .fetch_one(&mut *connection)
    .await?;
    assert_eq!(old, preserved);
    let generic = format!("sha256:{}", "22".repeat(32));
    sqlx::query(sql!(
        "INSERT INTO runtime_environments (
             environment_ref, runtime, runtime_version
         ) VALUES ($1, 'firecracker', '1.17.0')"
    ))
    .bind(&generic)
    .execute(&mut *connection)
    .await?;
    let visible: Vec<(String,)> = sqlx::query_as(sql!(
        "SELECT environment_ref FROM runtime_environments
         WHERE retired_at IS NULL ORDER BY environment_ref"
    ))
    .fetch_all(&mut *connection)
    .await?;
    assert_eq!(visible, vec![(generic.clone(),)]);
    let partial_format = sqlx::query(sql!(
        "UPDATE runtime_environments SET state_format_name = 'firecracker.snapshot'
         WHERE environment_ref = $1"
    ))
    .bind(&generic)
    .execute(&mut *connection)
    .await
    .unwrap_err();
    assert_eq!(
        partial_format
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("23514"),
        "a partial state-format identity must fail its paired-null constraint"
    );
    let mut tx = connection.begin().await?;
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql!(VALIDATE_DOWN)))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    let mut tx = connection.begin().await?;
    let incompatible_down = sqlx::raw_sql(sqlx::AssertSqlSafe(sql!(DOWN)))
        .execute(&mut *tx)
        .await
        .unwrap_err();
    assert_eq!(
        incompatible_down
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("23502"),
        "downgrade must refuse incompatible rows, not discard them"
    );
    tx.rollback().await?;
    // Remove only the incompatible row in this disposable fixture, then prove
    // a downgrade succeeds while the old row and retirement remain intact.
    sqlx::query(sql!(
        "DELETE FROM runtime_environments WHERE environment_ref = $1"
    ))
    .bind(&generic)
    .execute(&mut *connection)
    .await?;
    let mut tx = connection.begin().await?;
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql!(DOWN)))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    let downgraded: LegacyRow = sqlx::query_as(sql!(
        "SELECT environment_ref, runtime, runtime_version, version_sort_key,
                    kernel_version, state_format_name, state_format_version,
                    retired_at::text FROM runtime_environments"
    ))
    .fetch_one(&mut *connection)
    .await?;
    assert_eq!(old, downgraded);
    Ok(())
}
