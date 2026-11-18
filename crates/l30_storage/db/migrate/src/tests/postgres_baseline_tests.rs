use pplx_space_db_test_fixtures::try_raw_pg_test_db;
use sqlx::SqlSafeStr as _;

use super::super::*;
use crate::pg_test_support::TestResult;

#[tokio::test]
async fn postgres_rejects_legacy_history_without_changing_data() -> TestResult {
    let Some(fixture) = try_raw_pg_test_db().await? else {
        return Ok(());
    };
    Migrator::with_migrations(vec![sqlx::migrate::Migration::new(
        44,
        Cow::Borrowed("legacy schema"),
        MigrationType::Simple,
        "CREATE TABLE retained_state (id integer PRIMARY KEY); INSERT INTO retained_state VALUES (7);"
            .into_sql_str(),
        false,
    )])
    .run(&fixture.pool)
    .await?;
    let history: String = sqlx::query_scalar(sql!(
        "SELECT json_agg(history ORDER BY version)::text FROM _sqlx_migrations AS history"
    ))
    .fetch_one(&fixture.pool)
    .await?;

    let result = run_control_plane(ControlPlaneArgs {
        database_url: fixture.database_url.clone(),
        job_deadline_seconds: 60,
        confirm_data_loss: false,
        command: Command::Up,
    })
    .await;

    assert!(matches!(result, Err(RunDbMigrateError::ApplyMigrations(_))));
    assert_eq!(
        sqlx::query_scalar::<_, String>(sql!(
            "SELECT json_agg(history ORDER BY version)::text FROM _sqlx_migrations AS history"
        ))
        .fetch_one(&fixture.pool)
        .await?,
        history
    );
    assert_eq!(
        sqlx::query_scalar::<_, i32>(sql!("SELECT id FROM retained_state"))
            .fetch_one(&fixture.pool)
            .await?,
        7
    );
    assert!(
        sqlx::query_scalar::<_, bool>(sql!("SELECT to_regclass('public.organizations') IS NULL"))
            .fetch_one(&fixture.pool)
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn all_postgres_migrations_apply_to_fresh_database() -> TestResult {
    let Some(fixture) = try_raw_pg_test_db().await? else {
        return Ok(());
    };

    POSTGRES_MIGRATOR.run(&fixture.pool).await?;
    let history: String = sqlx::query_scalar(sql!(
        "SELECT json_agg(history ORDER BY version)::text FROM _sqlx_migrations AS history"
    ))
    .fetch_one(&fixture.pool)
    .await?;

    // Retired metadata is opaque: startup must not depend on its columns.
    sqlx::raw_sql(sql!(
        "CREATE TABLE goose_db_version (retained_marker integer);
         INSERT INTO goose_db_version VALUES (7);"
    ))
    .execute(&fixture.pool)
    .await?;

    for goose_present in [true, false] {
        if !goose_present {
            sqlx::query(sql!("DROP TABLE goose_db_version"))
                .execute(&fixture.pool)
                .await?;
        }

        apply_control_plane_migrations(
            &fixture.pool,
            migration_retry::LockRetryPolicy::DEFAULT,
            RunDbMigrateError::ApplyMigrations,
        )
        .await?;

        assert_eq!(
            sqlx::query_scalar::<_, String>(sql!(
                "SELECT json_agg(history ORDER BY version)::text FROM _sqlx_migrations AS history"
            ))
            .fetch_one(&fixture.pool)
            .await?,
            history
        );
        if goose_present {
            assert_eq!(
                sqlx::query_scalar::<_, i32>(sql!("SELECT retained_marker FROM goose_db_version"))
                    .fetch_one(&fixture.pool)
                    .await?,
                7
            );
        }
    }

    let latest: i64 = sqlx::query_scalar(sql!(
        "SELECT MAX(version) FROM _sqlx_migrations WHERE success = TRUE"
    ))
    .fetch_one(&fixture.pool)
    .await?;
    let expected_latest = POSTGRES_MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| migration.version)
        .max()
        .expect("Postgres migrator must bundle at least one migration");
    assert_eq!(latest, expected_latest);

    let mut migrations: Vec<_> = POSTGRES_MIGRATOR.iter().cloned().collect();
    migrations.push(sqlx::migrate::Migration::new(
        expected_latest + 1,
        Cow::Borrowed("next migration"),
        MigrationType::Simple,
        "ALTER TABLE organizations ADD COLUMN upgrade_probe boolean NOT NULL DEFAULT true;"
            .into_sql_str(),
        false,
    ));
    Migrator::with_migrations(migrations)
        .run(&fixture.pool)
        .await?;

    let upgraded: bool = sqlx::query_scalar(sql!(
        "SELECT upgrade_probe FROM organizations WHERE slug = 'local'"
    ))
    .fetch_one(&fixture.pool)
    .await?;
    assert!(upgraded);
    Ok(())
}

/// The firewall_rules_changed down keeps its enum label, so applying the up
/// again must succeed without adding a second one.
#[tokio::test]
async fn firewall_rules_changed_reapplies_after_its_down_with_one_label() -> TestResult {
    const VERSION: i64 = 20261118160040;
    let Some(fixture) = try_raw_pg_test_db().await? else {
        return Ok(());
    };
    let migrator = Migrator::with_migrations(
        POSTGRES_MIGRATOR
            .iter()
            .filter(|migration| migration.version <= VERSION)
            .cloned()
            .collect(),
    );
    let previous = migrator
        .iter()
        .map(|migration| migration.version)
        .filter(|version| *version < VERSION)
        .max()
        .expect("a migration precedes firewall_rules_changed");
    let labels = async || {
        sqlx::query_scalar::<_, i64>(sql!(
            "SELECT COUNT(*) FROM pg_enum
             WHERE enumtypid = 'public.state_reconciliation_reason'::regtype
               AND enumlabel = 'firewall_rules_changed'"
        ))
        .fetch_one(&fixture.pool)
        .await
    };

    migrator.run(&fixture.pool).await?;
    assert_eq!(labels().await?, 1);
    migrator.undo(&fixture.pool, previous).await?;
    assert_eq!(labels().await?, 1);
    assert!(
        !sqlx::query_scalar::<_, bool>(sql!(
            "SELECT EXISTS (SELECT 1 FROM _sqlx_migrations WHERE version = $1)"
        ))
        .bind(VERSION)
        .fetch_one(&fixture.pool)
        .await?
    );
    migrator.run(&fixture.pool).await?;
    assert_eq!(labels().await?, 1);
    Ok(())
}
