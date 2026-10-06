use super::*;
use crate::pg_test_support::{TestResult, postgres_fixture};

async fn seed_history(pool: &PgPool) -> TestResult {
    let manifest = CutoverManifest::embedded()?;
    sqlx::query(sql!("DELETE FROM public._sqlx_migrations"))
        .execute(pool)
        .await?;
    for (version, sums) in &manifest.stores["postgres"].migrations {
        sqlx::query(sql!(
            "INSERT INTO public._sqlx_migrations
            (version, description, success, checksum, execution_time)
            VALUES ($1, 'historical', TRUE, decode($2, 'hex'), 123)"
        ))
        .bind(version)
        .bind(&sums[0])
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn history(pool: &PgPool) -> TestResult<Vec<HistoryRow>> {
    Ok(sqlx::query_as(sql!(
        "SELECT version, success, checksum FROM public._sqlx_migrations ORDER BY version"
    ))
    .fetch_all(pool)
    .await?)
}

#[tokio::test]
async fn exact_history_converts_once_and_keeps_application_data() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    crate::bootstrap_roles::run(crate::bootstrap_roles::BootstrapRolesArgs {
        database_url: fixture.database_url("postgres"),
        app_statement_timeout: "10s".into(),
        app_idle_transaction_timeout: "15s".into(),
        app_lock_timeout: None,
        migrator_statement_timeout: "10min".into(),
        migrator_lock_timeout: "5s".into(),
        allow_missing_passwords: true,
    })
    .await?;
    let run = |command: crate::Command| {
        crate::run_control_plane(crate::ControlPlaneArgs {
            database_url: fixture.database_url("postgres"),
            job_deadline_seconds: 60,
            confirm_data_loss: false,
            command,
        })
    };
    let bundled: Vec<i64> = crate::POSTGRES_MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| migration.version)
        .collect();
    run(crate::Command::Up).await?;
    sqlx::query(sql!(
        "UPDATE organizations SET name = 'Retained evidence' WHERE slug = 'local'"
    ))
    .execute(&fixture.pool)
    .await?;
    // A historical database holds only the legacy schema, so roll back every
    // migration after the baseline before replacing the history with the
    // legacy catalog; the cutover must then apply those migrations itself.
    for _ in 1..bundled.len() {
        run(crate::Command::Down).await?;
    }
    seed_history(&fixture.pool).await?;
    let before = history(&fixture.pool).await?;

    run(crate::Command::Up).await?;
    run(crate::Command::Up).await?;

    // The converted history is exactly the bundled catalog: the baseline plus
    // every migration added after it, each recorded once.
    let recorded: Vec<i64> = history(&fixture.pool)
        .await?
        .into_iter()
        .map(|row| row.0)
        .collect();
    assert_eq!(recorded, bundled);
    let archived: Vec<HistoryRow> = sqlx::query_as(sql!("SELECT version, success, checksum FROM space_migration_archive.postgres_history ORDER BY version"))
        .fetch_all(&fixture.pool).await?;
    assert_eq!(archived, before);
    for role in ["space_user", "space_ro", "space_creds"] {
        let accessible: bool = sqlx::query_scalar(sql!(
            "SELECT has_schema_privilege($1, 'space_migration_archive', 'USAGE')"
        ))
        .bind(role)
        .fetch_one(&fixture.pool)
        .await?;
        assert!(!accessible, "{role} must not access recovery history");
    }
    let name: String =
        sqlx::query_scalar(sql!("SELECT name FROM organizations WHERE slug = 'local'"))
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(name, "Retained evidence");
    Ok(())
}

#[tokio::test]
async fn bad_history_is_rejected_without_rewriting_history() -> TestResult {
    use sqlx::SqlSafeStr as _;
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    crate::POSTGRES_MIGRATOR.run(&fixture.pool).await?;
    sqlx::query(sql!("CREATE SCHEMA space_migration_archive"))
        .execute(&fixture.pool)
        .await?;
    for damage in [
        "UPDATE _sqlx_migrations SET checksum = decode('00', 'hex') WHERE version = 44",
        "UPDATE _sqlx_migrations SET success = FALSE WHERE version = 44",
        "DELETE FROM _sqlx_migrations WHERE version = 44",
        "INSERT INTO _sqlx_migrations VALUES (999999, 'unknown', now(), TRUE, decode('00', 'hex'), 0)",
    ] {
        seed_history(&fixture.pool).await?;
        sqlx::query(damage.into_sql_str())
            .execute(&fixture.pool)
            .await?;
        let before = history(&fixture.pool).await?;

        assert!(
            prepare(&fixture.pool, &crate::POSTGRES_MIGRATOR)
                .await
                .is_err(),
            "{damage}"
        );

        assert_eq!(history(&fixture.pool).await?, before);
        let archived: bool = sqlx::query_scalar(sql!(
            "SELECT to_regclass('space_migration_archive.postgres_history') IS NOT NULL"
        ))
        .fetch_one(&fixture.pool)
        .await?;
        assert!(!archived);
    }
    Ok(())
}
