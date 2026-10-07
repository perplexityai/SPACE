use std::borrow::Cow;

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{TestDbResult, try_raw_pg_test_db};
use sqlx::SqlSafeStr as _;
use sqlx::migrate::{Migration, MigrationType};

use super::*;

fn revision(comment: &str) -> Migration {
    Migration::new(
        1,
        Cow::Borrowed("probe"),
        MigrationType::Simple,
        sqlx::AssertSqlSafe(format!(
            "-- {comment}\nCREATE TABLE checksum_probe (id INTEGER); INSERT INTO checksum_probe VALUES (7);"
        )).into_sql_str(),
        false,
    )
}

fn registry(store: &str, revisions: &[Migration]) -> AcceptedChecksums {
    let checksums: Vec<_> = revisions.iter().map(|m| hex::encode(&m.checksum)).collect();
    AcceptedChecksums::from_toml(&format!(
        "[[migrations]]\nstore = {store:?}\nversion = 1\naccepted_checksums = {checksums:?}"
    ))
    .unwrap()
}

async fn upgrade<C: Migrate>(
    conn: &mut C,
    store: &str,
    previous: Option<usize>,
) -> TestDbResult<()> {
    let revisions = [
        revision("original"),
        revision("intermediate"),
        revision("current"),
    ];
    let checksums = registry(store, &revisions);
    let expected = &revisions[previous.unwrap_or(2)].checksum;
    if let Some(previous) = previous {
        Migrator::with_migrations(vec![revisions[previous].clone()])
            .run_direct(None, conn, false)
            .await?;
    }
    let current = Migrator::with_migrations(vec![
        revisions[2].clone(),
        Migration::new(
            2,
            Cow::Borrowed("pending"),
            MigrationType::Simple,
            sql!("CREATE INDEX checksum_probe_id ON checksum_probe (id);").into_sql_str(),
            false,
        ),
    ]);

    if previous.is_some_and(|version| version < 2) {
        assert!(matches!(
            current.run_direct(None, conn, false).await,
            Err(MigrateError::VersionMismatch(1))
        ));
        conn.unlock().await?;
    }

    checksums.run(store, &current, conn).await?;
    checksums.run(store, &current, conn).await?;

    let applied = conn.list_applied_migrations(&current.table_name).await?;
    assert_eq!(applied.len(), 2);
    assert_eq!(applied[0].version, 1);
    assert_eq!(
        &applied[0].checksum, expected,
        "history must not be rewritten"
    );
    assert_eq!(applied[1].checksum, current.migrations[1].checksum);
    Ok(())
}

#[tokio::test]
async fn postgres_upgrades_across_revisions_without_replaying_or_rewriting() -> TestDbResult<()> {
    for previous in [None, Some(0), Some(1), Some(2)] {
        let Some(fixture) = try_raw_pg_test_db().await? else {
            return Ok(());
        };
        let mut conn = fixture.pool.acquire().await?;

        upgrade(&mut *conn, "postgres", previous).await?;

        let rows: Vec<i32> = sqlx::query_scalar(sql!("SELECT id FROM checksum_probe"))
            .fetch_all(&mut *conn)
            .await?;
        assert_eq!(rows, [7]);
        let locks: i64 = sqlx::query_scalar(sql!(
            "SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'advisory'"
        ))
        .fetch_one(&mut *conn)
        .await?;
        assert_eq!(
            locks, 0,
            "successful retries must release every advisory lock"
        );
    }
    Ok(())
}

#[tokio::test]
async fn sqlite_upgrades_across_revisions_without_replaying_or_rewriting() -> TestDbResult<()> {
    use sqlx::Connection as _;
    for previous in [None, Some(0), Some(1), Some(2)] {
        let mut conn = sqlx::SqliteConnection::connect("sqlite::memory:").await?;

        upgrade(&mut conn, "template-manager", previous).await?;

        let rows: Vec<i32> = sqlx::query_scalar(sql!("SELECT id FROM checksum_probe"))
            .fetch_all(&mut conn)
            .await?;
        assert_eq!(rows, [7]);
    }
    Ok(())
}

#[tokio::test]
async fn aliases_do_not_bypass_unknown_dirty_or_missing_history() -> TestDbResult<()> {
    use sqlx::Connection as _;
    let original = revision("original");
    let current = revision("current");
    let checksums = registry("template-manager", &[original.clone(), current.clone()]);
    let migrator = Migrator::with_migrations(vec![current.clone()]);

    for (store, applied) in [
        ("template-manager", revision("unknown")),
        ("node-manager", original.clone()),
    ] {
        let mut conn = sqlx::SqliteConnection::connect("sqlite::memory:").await?;
        Migrator::with_migrations(vec![applied.clone()])
            .run(&mut conn)
            .await?;

        assert!(matches!(
            checksums.run(store, &migrator, &mut conn).await,
            Err(MigrateError::VersionMismatch(1))
        ));
        assert_eq!(
            conn.list_applied_migrations(&migrator.table_name).await?[0].checksum,
            applied.checksum
        );
    }
    let mut conn = sqlx::SqliteConnection::connect("sqlite::memory:").await?;
    Migrator::with_migrations(vec![original])
        .run(&mut conn)
        .await?;
    sqlx::query(sql!("UPDATE _sqlx_migrations SET success = FALSE"))
        .execute(&mut conn)
        .await?;
    assert!(matches!(
        checksums
            .run("template-manager", &migrator, &mut conn)
            .await,
        Err(MigrateError::Dirty(1))
    ));
    sqlx::query(sql!(
        "UPDATE _sqlx_migrations SET success = TRUE, version = 99"
    ))
    .execute(&mut conn)
    .await?;
    assert!(matches!(
        checksums
            .run("template-manager", &migrator, &mut conn)
            .await,
        Err(MigrateError::VersionMissing(99))
    ));

    let mut fresh = sqlx::SqliteConnection::connect("sqlite::memory:").await?;
    let unlisted = Migrator::with_migrations(vec![revision("unreviewed edit")]);
    assert!(matches!(
        checksums
            .run("template-manager", &unlisted, &mut fresh)
            .await,
        Err(MigrateError::VersionMismatch(1))
    ));
    let tables: i64 = sqlx::query_scalar(sql!(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table'"
    ))
    .fetch_one(&mut fresh)
    .await?;
    assert_eq!(
        tables, 0,
        "unlisted source must fail before changing a fresh database"
    );
    Ok(())
}

#[test]
fn malformed_or_ambiguous_manifests_are_rejected() {
    let digest = "ab".repeat(48);
    let valid = format!(
        "[[migrations]]\nstore = 'postgres'\nversion = 1\naccepted_checksums = ['{digest}']"
    );
    for source in [
        "not toml".into(),
        valid.replace(&digest, "abc"),
        valid.replace(&digest, &"zz".repeat(48)),
        format!("{valid}\n{valid}"),
        valid.replace(&format!("'{digest}'"), &format!("'{digest}', '{digest}'")),
        valid.replace(&format!("'{digest}'"), ""),
        valid.replace("version = 1", "version = 0"),
        valid.replace("'postgres'", "''"),
        format!("{valid}\nunknown = true"),
    ] {
        assert!(
            AcceptedChecksums::from_toml(&source).is_err(),
            "accepted {source}"
        );
    }
}
