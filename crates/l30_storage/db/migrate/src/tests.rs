use rusqlite::{Connection, params};

use super::*;

static ALL_MIGRATORS: &[(&str, &Migrator)] = &[
    ("common", &COMMON_SCHEMA_MIGRATOR),
    ("postgres", &POSTGRES_MIGRATOR),
    (
        "sandbox-manager",
        pplx_space_sbx_mgr_store::db::migration::target().service,
    ),
    ("template-manager", &TEMPLATE_MANAGER_MIGRATOR),
    ("node-manager", &NODE_MANAGER_MIGRATOR),
    ("node-ctl", &NODE_CTL_MIGRATOR),
    ("runtime-manager", &RUNTIME_MANAGER_MIGRATOR),
    ("volume-manager", &VOLUME_MANAGER_MIGRATOR),
    ("build-manager", &BUILD_MANAGER_MIGRATOR),
];

#[tokio::test]
async fn suspend_attempt_rollback_preserves_ownership_and_migration_history() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sandbox.sqlite");
    let database = SqliteDatabase::from(pplx_space_sbx_mgr_store::db::migration::target());
    migrate_sqlite_up(database, &path).await.unwrap();
    // Step back past every newer migration first, the legacy_state restore
    // and re-drop among them: the property is about suspend_attempt's own
    // down, not whichever migration happens to be newest.
    let newer = pplx_space_sbx_mgr_store::db::migration::target()
        .service
        .iter()
        .filter(|migration| {
            migration.migration_type.is_up_migration() && migration.version > 20261112160000
        })
        .count();
    for _ in 0..newer {
        rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
            .await
            .unwrap();
    }
    let newest: i64 = Connection::open(&path)
        .unwrap()
        .query_row("SELECT MAX(version) FROM _sqlx_migrations", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(newest, 20261112160000, "suspend_attempt is the newest");
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "INSERT INTO sandboxes (id, uuid8, state, template, created_at, updated_at,
             suspend_attempt_artifact_id, suspend_attempt_started_at)
         VALUES ('sandbox', 'sandbox', 'suspending', 'template', '1', '1', 'attempt', 1)",
        [],
    )
    .unwrap();
    let history = || {
        conn.query_row(
            "SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 20261112160000",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap()
    };
    assert_eq!(history(), 1);
    assert!(
        rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
            .await
            .is_err()
    );
    assert_eq!(history(), 1, "refused rollback must retain applied history");
    assert_eq!(
        conn.query_row(
            "SELECT suspend_attempt_artifact_id FROM sandboxes WHERE id = 'sandbox'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        "attempt"
    );

    conn.execute(
        "UPDATE sandboxes SET state = 'paused' WHERE id = 'sandbox'",
        [],
    )
    .unwrap();
    rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
        .await
        .unwrap();
    assert_eq!(history(), 0);
    migrate_sqlite_up(database, &path).await.unwrap();
    assert_eq!(history(), 1);
    assert_eq!(
        conn.query_row(
            "SELECT suspend_attempt_artifact_id FROM sandboxes WHERE id = 'sandbox'",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .unwrap(),
        None,
        "re-upgrade must not invent an admission identity"
    );
}

#[tokio::test]
async fn sandbox_baseline_refuses_down_without_changing_shared_jobs_or_history() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sandbox.sqlite");
    let database = SqliteDatabase::from(pplx_space_sbx_mgr_store::db::migration::target());
    migrate_sqlite_up(database, &path).await.unwrap();
    // Step back to the baseline first: the property is about the baseline's
    // own down migration, not whichever migration happens to be newest.
    let after_baseline = pplx_space_sbx_mgr_store::db::migration::target()
        .service
        .iter()
        .filter(|migration| {
            migration.migration_type.is_up_migration()
                && migration.version > pplx_space_sqlite::baselines::PUBLIC_SQLITE_BASELINE
        })
        .count();
    for _ in 0..after_baseline {
        rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
            .await
            .unwrap();
    }
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "INSERT INTO jobs (job_id, kind, status) VALUES ('cancelled-witness', 'test', 'cancelled')",
        [],
    )
    .unwrap();
    let before = common_history(&conn).unwrap();
    let schema_before: i64 = conn
        .pragma_query_value(None, "schema_version", |row| row.get(0))
        .unwrap();
    let error = rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
        .await
        .unwrap_err();
    assert!(matches!(error, RunDbMigrateError::RollbackSqlite(_)));
    assert_eq!(common_history(&conn).unwrap(), before);
    assert_eq!(
        conn.pragma_query_value(None, "schema_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        schema_before
    );
    assert_eq!(
        conn.query_row(
            "SELECT status FROM jobs WHERE job_id = 'cancelled-witness'",
            [],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        "cancelled"
    );
}

/// Rolling a node back past the capture-time marker keeps its artifact rows,
/// and upgrading again brings every one of them back as not comparable. A pin
/// recorded before a downgrade can't then report a capture time the
/// downgraded binary never guaranteed.
#[tokio::test]
async fn capture_time_marker_round_trips_and_reupgrades_as_not_comparable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sandbox.sqlite");
    let database = SqliteDatabase::from(pplx_space_sbx_mgr_store::db::migration::target());
    let marker_columns = |conn: &Connection| -> Vec<String> {
        let mut statement = conn
            .prepare("SELECT name FROM pragma_table_info('local_artifacts') WHERE name = 'capture_time_comparable'")
            .unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    let marker = |conn: &Connection| -> i64 {
        conn.query_row(
            "SELECT capture_time_comparable FROM local_artifacts WHERE local_artifact_id = 'a'",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };

    migrate_sqlite_up(database, &path).await.unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO local_artifacts (
                 local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                 local_path, created_at, capture_time_comparable
             ) VALUES ('a', 's', 'disk', 'stop', '/cache/a', '1', 1)",
            [],
        )
        .unwrap();
        assert_eq!(marker(&conn), 1);
    }

    // Roll back until the marker's own migration has been undone, however many
    // newer migrations sit above it.
    for _ in 0..pplx_space_sbx_mgr_store::db::migration::target()
        .service
        .iter()
        .count()
    {
        if marker_columns(&Connection::open(&path).unwrap()).is_empty() {
            break;
        }
        rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
            .await
            .unwrap();
    }
    {
        let conn = Connection::open(&path).unwrap();
        assert!(
            marker_columns(&conn).is_empty(),
            "the down migration drops the marker"
        );
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM local_artifacts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 1, "and keeps the artifact row");
    }

    migrate_sqlite_up(database, &path).await.unwrap();
    let conn = Connection::open(&path).unwrap();
    assert_eq!(
        marker_columns(&conn),
        vec!["capture_time_comparable".to_owned()]
    );
    assert_eq!(
        marker(&conn),
        0,
        "a row from before the downgrade isn't comparable"
    );
}

/// The two runtime-upgrade migrations: the attempt id and the attempt outcomes.
const RUNTIME_UPGRADE_MIGRATIONS: [i64; 2] = [20261114030001, 20261114030002];
/// The newest sandbox-manager migration that shipped before them.
const BEFORE_RUNTIME_UPGRADE_MIGRATIONS: i64 = 20261114020000;
/// The tables 20261114030001 adds `attempt_id` to.
const ATTEMPT_ID_TABLES: [&str; 2] = [
    "sandbox_runtime_upgrades",
    "sandbox_runtime_upgrade_outcomes",
];

/// Whether `table` exists in the SQLite database at `path`.
fn table_exists(path: &Path, table: &str) -> bool {
    Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get::<_, i64>(0),
        )
        .unwrap()
        == 1
}

/// The `ATTEMPT_ID_TABLES` that have an `attempt_id` column.
fn tables_with_attempt_id(path: &Path) -> usize {
    let conn = Connection::open(path).unwrap();
    ATTEMPT_ID_TABLES
        .iter()
        .filter(|table| {
            conn.query_row(
                "SELECT COUNT(*) FROM pragma_table_xinfo(?1) WHERE name = 'attempt_id'",
                [table],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
                == 1
        })
        .count()
}

/// The runtime-upgrade migrations come after every migration that shipped
/// before them, next to each other, so a database at that earlier head takes
/// them as its next two and a `down` reverts them, newest first, before it
/// reaches any earlier migration. Up, down and up again round-trip; newer
/// migrations are rolled back first. The attempt-outcome table has no foreign
/// key to `sandboxes`, and the downs drop that table and both `attempt_id`
/// columns.
#[tokio::test]
async fn runtime_upgrade_migrations_follow_their_predecessors_and_round_trip() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sandbox.sqlite");
    let database = SqliteDatabase::from(pplx_space_sbx_mgr_store::db::migration::target());
    let migrator = pplx_space_sbx_mgr_store::db::migration::target().service;
    let bundled: Vec<i64> = migrator
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| migration.version)
        .collect();
    assert!(RUNTIME_UPGRADE_MIGRATIONS[0] > BEFORE_RUNTIME_UPGRADE_MIGRATIONS);
    assert!(bundled.contains(&BEFORE_RUNTIME_UPGRADE_MIGRATIONS));
    let first = bundled
        .iter()
        .position(|version| *version == RUNTIME_UPGRADE_MIGRATIONS[0])
        .unwrap();
    assert_eq!(
        bundled[first..first + RUNTIME_UPGRADE_MIGRATIONS.len()],
        RUNTIME_UPGRADE_MIGRATIONS,
        "the runtime-upgrade migrations are adjacent, in order"
    );
    let earlier = bundled[..first].to_vec();
    assert_eq!(
        earlier.last(),
        Some(&BEFORE_RUNTIME_UPGRADE_MIGRATIONS),
        "nothing sits between the newest earlier migration and them"
    );
    let newer = bundled.len() - first - RUNTIME_UPGRADE_MIGRATIONS.len();
    let applied = || async {
        let pool = sqlite_pool(&path).await.unwrap();
        applied_versions_sqlite(&pool, migrator).await.unwrap()
    };
    let rollback = || async {
        rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
            .await
            .unwrap();
    };

    // The attempt outcomes' foreign keys, of any target.
    let outcome_foreign_keys = || {
        Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM pragma_foreign_key_list(
                     'sandbox_runtime_upgrade_attempt_outcomes'
                 )",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
    };

    migrate_sqlite_up(database, &path).await.unwrap();
    for round in 0..2 {
        for _ in 0..newer {
            rollback().await;
        }
        assert!(
            table_exists(&path, "sandbox_runtime_upgrade_attempt_outcomes"),
            "round {round}: the attempt-outcome table exists"
        );
        assert_eq!(
            outcome_foreign_keys(),
            0,
            "round {round}: the attempt outcomes don't reference `sandboxes`"
        );
        assert_eq!(
            tables_with_attempt_id(&path),
            ATTEMPT_ID_TABLES.len(),
            "round {round}: both tables have `attempt_id`"
        );
        for reverted in RUNTIME_UPGRADE_MIGRATIONS.iter().rev() {
            rollback().await;
            assert!(
                !applied().await.contains(reverted),
                "round {round}: the down that follows is {reverted}'s"
            );
        }
        assert_eq!(
            applied().await,
            earlier,
            "round {round}: only the runtime-upgrade migrations were reverted"
        );
        let conn = Connection::open(&path).unwrap();
        let tables: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN (
                     'sandbox_runtime_upgrade_attempt_outcomes'
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tables, 0, "round {round}: their tables are gone");
        assert_eq!(
            tables_with_attempt_id(&path),
            0,
            "round {round}: both `attempt_id` columns are gone"
        );
        let unowned_state: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_xinfo('sandboxes') WHERE name = 'unowned_state'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(unowned_state, 1, "round {round}: the earlier schema stays");
        migrate_sqlite_up(database, &path).await.unwrap();
        assert_eq!(applied().await, bundled, "round {round}: up again");
    }
}

/// Each runtime-upgrade down refuses while a row it guards is recorded (an
/// attempt outcome it would drop, or a per-operation outcome an older release
/// can't parse), leaves its migration applied and its schema in place, and
/// goes through once the row is gone. The `rolled_back` and
/// `already_on_target` outcomes, which an older release parses, don't hold
/// back the attempt id's down. The attempt-outcome table keeps one row per
/// operation and attempt id, every row names an attempt, and `recorded_seq`
/// isn't reused after the newest row is deleted.
#[tokio::test]
async fn runtime_upgrade_downs_refuse_while_a_row_an_older_release_misreads_is_recorded() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sandbox.sqlite");
    let database = SqliteDatabase::from(pplx_space_sbx_mgr_store::db::migration::target());
    let migrator = pplx_space_sbx_mgr_store::db::migration::target().service;
    let newer = migrator
        .iter()
        .filter(|migration| {
            migration.migration_type.is_up_migration()
                && migration.version > RUNTIME_UPGRADE_MIGRATIONS[1]
        })
        .count();
    let applied = || async {
        let pool = sqlite_pool(&path).await.unwrap();
        applied_versions_sqlite(&pool, migrator).await.unwrap()
    };
    // The guards count rows, so seed them without a sandbox row behind them.
    let execute = |sql: &str| {
        let conn = Connection::open(&path).unwrap();
        conn.pragma_update(None, "foreign_keys", false).unwrap();
        conn.execute(sql, []).unwrap();
    };

    let attempt_outcome = |operation: &str, attempt: &str| {
        format!(
            "INSERT INTO sandbox_runtime_upgrade_attempt_outcomes
                 (operation_id, attempt_id, sandbox_id, outcome, failure, recorded_at)
             VALUES ('{operation}', {attempt}, 'sandbox', 'rolled_back', NULL, '0')"
        )
    };
    let operation_outcome = |operation: &str, outcome: &str| {
        format!(
            "INSERT INTO sandbox_runtime_upgrade_outcomes
                 (operation_id, sandbox_id, outcome, target_environment, target_state,
                  authorization_id, failure, recorded_at, attempt_id)
             VALUES ('{operation}', 'sandbox', '{outcome}', 'env', 'started', 'auth', NULL,
                     '0', 'attempt-a')"
        )
    };

    migrate_sqlite_up(database, &path).await.unwrap();
    for _ in 0..newer {
        rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
            .await
            .unwrap();
    }
    let conn = Connection::open(&path).unwrap();
    conn.execute(&attempt_outcome("op", "'attempt-a'"), [])
        .unwrap();
    let duplicate = conn
        .execute(&attempt_outcome("op", "'attempt-a'"), [])
        .unwrap_err();
    assert!(
        duplicate.to_string().contains("UNIQUE constraint failed"),
        "one outcome per operation and attempt id: {duplicate}"
    );
    conn.execute(&attempt_outcome("op", "'attempt-b'"), [])
        .unwrap();
    let unnamed = conn
        .execute(&attempt_outcome("op", "NULL"), [])
        .unwrap_err();
    assert!(
        unnamed.to_string().contains("NOT NULL constraint failed"),
        "every attempt outcome names an attempt: {unnamed}"
    );
    let newest = || -> i64 {
        conn.query_row(
            "SELECT MAX(recorded_seq) FROM sandbox_runtime_upgrade_attempt_outcomes",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };
    let deleted = newest();
    conn.execute("DELETE FROM sandbox_runtime_upgrade_attempt_outcomes", [])
        .unwrap();
    conn.execute(&attempt_outcome("op", "'attempt-c'"), [])
        .unwrap();
    assert!(
        newest() > deleted,
        "a deleted row's `recorded_seq` isn't reused, so it still orders by recording"
    );
    conn.execute("DELETE FROM sandbox_runtime_upgrade_attempt_outcomes", [])
        .unwrap();
    drop(conn);

    // Newest first: each down's guarded rows, each tried alone and then
    // cleared.
    let guarded = [
        (
            RUNTIME_UPGRADE_MIGRATIONS[1],
            vec![attempt_outcome("op", "'attempt-a'")],
            "DELETE FROM sandbox_runtime_upgrade_attempt_outcomes",
        ),
        (
            RUNTIME_UPGRADE_MIGRATIONS[0],
            vec![
                operation_outcome("refused-op", "refused"),
                operation_outcome("unknown-op", "activation_refused"),
            ],
            "DELETE FROM sandbox_runtime_upgrade_outcomes
              WHERE outcome NOT IN ('rolled_back', 'already_on_target')",
        ),
    ];
    // An older release reads these outcomes, so they stay through the
    // attempt id's down.
    let parsed = ["rolled_back", "already_on_target"];
    for outcome in parsed {
        execute(&operation_outcome(&format!("{outcome}-op"), outcome));
    }
    for (version, inserts, clear) in guarded {
        for insert in &inserts {
            execute(insert);
            let refused = rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
                .await
                .unwrap_err();
            assert!(
                format!("{refused:?}").contains("CHECK constraint failed"),
                "{version}: {refused:?}"
            );
            assert!(
                applied().await.contains(&version),
                "{version} stays applied while its guarded row is recorded"
            );
            if version == RUNTIME_UPGRADE_MIGRATIONS[1] {
                assert!(
                    table_exists(&path, "sandbox_runtime_upgrade_attempt_outcomes"),
                    "{version}: the refused down keeps the attempt-outcome table"
                );
            }
            assert_eq!(
                tables_with_attempt_id(&path),
                ATTEMPT_ID_TABLES.len(),
                "{version}: the refused down keeps both `attempt_id` columns"
            );
            execute(clear);
        }
        rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
            .await
            .unwrap();
        assert!(!applied().await.contains(&version), "{version} reverted");
    }
    let kept: i64 = Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM sandbox_runtime_upgrade_outcomes
              WHERE outcome IN ('rolled_back', 'already_on_target')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        kept,
        parsed.len() as i64,
        "the outcomes an older release parses are kept"
    );
}

/// Records the request with each final attempt outcome.
const ATTEMPT_IDENTITY_MIGRATION: i64 = 20261120180000;

/// The attempt-identity down refuses while a final attempt outcome (one that
/// records its request) is recorded, since a release without the columns
/// would take it as final for no attempt and run a retry its caller gave up.
/// It leaves its migration and columns in place, and goes through over rows
/// that don't record their request, which it keeps.
#[tokio::test]
async fn attempt_identity_down_refuses_while_a_final_attempt_outcome_is_recorded() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sandbox.sqlite");
    let database = SqliteDatabase::from(pplx_space_sbx_mgr_store::db::migration::target());
    let migrator = pplx_space_sbx_mgr_store::db::migration::target().service;
    let newer = migrator
        .iter()
        .filter(|migration| {
            migration.migration_type.is_up_migration()
                && migration.version > ATTEMPT_IDENTITY_MIGRATION
        })
        .count();
    let applied = || async {
        let pool = sqlite_pool(&path).await.unwrap();
        applied_versions_sqlite(&pool, migrator).await.unwrap()
    };
    let execute = |sql: &str| {
        Connection::open(&path).unwrap().execute(sql, []).unwrap();
    };
    let identity_columns = || -> i64 {
        Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_xinfo('sandbox_runtime_upgrade_attempt_outcomes')
                  WHERE name IN ('target_environment', 'target_state', 'authorization_id')",
                [],
                |row| row.get(0),
            )
            .unwrap()
    };

    migrate_sqlite_up(database, &path).await.unwrap();
    for _ in 0..newer {
        rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
            .await
            .unwrap();
    }
    assert_eq!(identity_columns(), 3);
    execute(
        "INSERT INTO sandbox_runtime_upgrade_attempt_outcomes
             (operation_id, attempt_id, sandbox_id, outcome, failure, recorded_at)
         VALUES ('legacy-op', 'attempt-a', 'sandbox', 'rolled_back', NULL, '0')",
    );
    execute(
        "INSERT INTO sandbox_runtime_upgrade_attempt_outcomes
             (operation_id, attempt_id, sandbox_id, outcome, failure, recorded_at,
              target_environment, target_state, authorization_id)
         VALUES ('final-op', 'attempt-a', 'sandbox', 'rolled_back', NULL, '0',
                 'env', 'started', 'auth')",
    );

    let refused = rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
        .await
        .unwrap_err();
    assert!(
        format!("{refused:?}").contains("CHECK constraint failed"),
        "{refused:?}"
    );
    assert!(applied().await.contains(&ATTEMPT_IDENTITY_MIGRATION));
    assert_eq!(identity_columns(), 3, "the refused down keeps the columns");

    execute(
        "DELETE FROM sandbox_runtime_upgrade_attempt_outcomes
          WHERE target_environment IS NOT NULL",
    );
    rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
        .await
        .unwrap();
    assert!(!applied().await.contains(&ATTEMPT_IDENTITY_MIGRATION));
    assert_eq!(identity_columns(), 0);
    let legacy: i64 = Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM sandbox_runtime_upgrade_attempt_outcomes",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(legacy, 1, "the row without a request is kept");
    migrate_sqlite_up(database, &path).await.unwrap();
    assert_eq!(identity_columns(), 3, "up again");
}

const DIRTY_REF_REVISIONS: i64 = 20261031050126;

/// db/README.md has operators roll template-manager back past the dirty-ref
/// revision migration with `space-db-migrator template-manager down`. That
/// drops the revision column, its counter and the triggers that stamp it,
/// keeps every queued outbox row, and `up` applies it again.
#[tokio::test]
async fn dirty_ref_revisions_down_keeps_outbox_rows_and_reapplies() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("template-manager.sqlite");
    let database = sqlite_template_manager();
    let recorded = |conn: &Connection| -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM _sqlx_migrations WHERE version = ?1",
            [DIRTY_REF_REVISIONS],
            |row| row.get(0),
        )
        .unwrap()
    };
    let revision_objects = |conn: &Connection| -> Vec<String> {
        let mut statement = conn
            .prepare(
                "SELECT name FROM pragma_table_info('node_dirty_refs') WHERE name = 'revision'
                 UNION ALL
                 SELECT name FROM sqlite_schema
                 WHERE name IN ('node_dirty_ref_revision', 'node_dirty_refs_insert_revision',
                                'node_dirty_refs_update_revision')
                 ORDER BY name",
            )
            .unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    let outbox = |conn: &Connection| -> Vec<(i64, String, String, String, String)> {
        let mut statement = conn
            .prepare(
                "SELECT facet, element_kind, element_value, requested_at, updated_at
                 FROM node_dirty_refs ORDER BY facet, element_kind, element_value",
            )
            .unwrap();
        statement
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    let all_revision_objects = vec![
        "node_dirty_ref_revision".to_owned(),
        "node_dirty_refs_insert_revision".to_owned(),
        "node_dirty_refs_update_revision".to_owned(),
        "revision".to_owned(),
    ];

    migrate_sqlite_up(database, &path).await.unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "INSERT INTO node_dirty_refs (facet, element_kind, element_value, requested_at, updated_at)
         VALUES (1, 'template', 'a', '1', '1'), (2, 'pool', 'b', '2', '3');
         UPDATE node_dirty_refs SET requested_at = '4', updated_at = '4' WHERE facet = 1;",
    )
    .unwrap();
    let rows = outbox(&conn);
    assert_eq!(rows.len(), 2);
    assert_eq!(recorded(&conn), 1);
    assert_eq!(revision_objects(&conn), all_revision_objects);

    // Revert any newer migration in either catalog first, as the README's
    // procedure does, so the next `down` reaches the dirty-ref migration.
    let newer = [&TEMPLATE_MANAGER_MIGRATOR, &COMMON_SCHEMA_MIGRATOR]
        .into_iter()
        .flat_map(|migrator| migrator.iter())
        .filter(|migration| {
            migration.migration_type.is_up_migration() && migration.version > DIRTY_REF_REVISIONS
        })
        .count();
    for _ in 0..newer {
        rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
            .await
            .unwrap();
    }
    rollback_last_sqlite_database(database, &path, &COMMON_SCHEMA_MIGRATOR)
        .await
        .unwrap();
    assert_eq!(recorded(&conn), 0);
    assert_eq!(revision_objects(&conn), Vec::<String>::new());
    assert_eq!(outbox(&conn), rows, "the down keeps every outbox row");

    migrate_sqlite_up(database, &path).await.unwrap();
    assert_eq!(recorded(&conn), 1);
    assert_eq!(revision_objects(&conn), all_revision_objects);
    assert_eq!(outbox(&conn), rows);
}

/// Some earlier template-manager releases don't bundle these migrations but
/// reverse them at startup, and only when the recorded checksum is the one
/// pinned here, so rolling back to them stays image-only. Change the schema
/// with a new migration, never by editing one of these. See db/README.md.
#[test]
fn template_manager_migrations_keep_the_checksums_releases_reverse() {
    let pins = [
        (
            20261026220935,
            "bb39d295eee07bfc5ff54e6d2f076efb42a50b43d7cedffd8907e9cbf73e7e9b2837295cb1131306798e1c3a4bf4b820",
        ),
        (
            DIRTY_REF_REVISIONS,
            "c92d51875e4001e0346e759fcf75c45b1f3929e9c5b719eda3a32d6af21651fdb37559a1deea96f86c262efe8c6264dd",
        ),
    ];
    for (version, checksum) in pins {
        let up = TEMPLATE_MANAGER_MIGRATOR
            .iter()
            .find(|migration| {
                migration.version == version && migration.migration_type.is_up_migration()
            })
            .unwrap_or_else(|| panic!("template-manager doesn't bundle {version}"));
        assert_eq!(hex::encode(up.checksum.as_ref()), checksum, "{version}");
    }
}

#[test]
fn embedded_migrations_have_unique_versions_and_public_baselines() {
    for (name, migrator) in ALL_MIGRATORS {
        assert_unique_migration_versions(name, migrator);
        let baseline = if *name == "postgres" {
            20261002070625
        } else {
            pplx_space_sqlite::baselines::PUBLIC_SQLITE_BASELINE
        };
        assert_eq!(
            migrator
                .iter()
                .filter(|migration| migration.migration_type.is_up_migration())
                .map(|migration| migration.version)
                .min(),
            Some(baseline),
            "{name} must reach its public baseline before legacy compatibility is removed"
        );
    }
}

/// `-- no-transaction` migrations run outside a transaction so they can
/// hold statements like CREATE INDEX CONCURRENTLY — but sqlx still sends
/// the whole file to Postgres as one simple-query batch, and Postgres
/// wraps a multi-statement batch in an implicit transaction, which
/// rejects CONCURRENTLY at runtime. Enforce one statement per
/// no-transaction file so the mistake fails at PR time, not deploy time.
#[test]
fn no_transaction_migrations_are_single_statement() {
    for (name, migrator) in ALL_MIGRATORS {
        for migration in migrator.iter().filter(|migration| migration.no_tx) {
            let statements = count_sql_statements(migration.sql.as_ref());
            assert_eq!(
                statements, 1,
                "{name} migration {} ({}) is no-transaction but holds {statements} \
                 statements; Postgres runs a multi-statement simple-query batch in an \
                 implicit transaction, so keep no-transaction files to one statement",
                migration.version, migration.description,
            );
        }
    }
}

/// Counts SQL statements after stripping `--` line comments. Good enough
/// for migration files; would miscount a `--` or `;` inside a string
/// literal, which no bundled migration uses in a no-transaction file.
fn count_sql_statements(sql: &str) -> usize {
    sql.lines()
        .map(|line| line.split_once("--").map_or(line, |(code, _)| code))
        .collect::<Vec<_>>()
        .join("\n")
        .split(';')
        .filter(|chunk| !chunk.trim().is_empty())
        .count()
}

#[tokio::test]
async fn common_schema_converges_on_fresh_consumer_databases() {
    for database in sqlite_databases_with_common_schema() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join(format!("{}.db", database.name));

        migrate_sqlite_up(database, &db_path).await.unwrap();
        assert_common_jobs_table_matches_reader(&db_path);
        assert_common_schema_migration_applied(
            &db_path,
            COMMON_SCHEMA_MIGRATOR
                .iter()
                .filter(|migration| migration.migration_type.is_up_migration())
                .map(|migration| migration.version)
                .max()
                .unwrap(),
        );

        migrate_sqlite_up(database, &db_path).await.unwrap();
        assert_common_jobs_table_matches_reader(&db_path);
        if database.name == "build-manager" {
            assert!(!index_exists(&db_path, "idx_build_jobs_active_template"));
        }
    }
}

fn sqlite_databases_with_common_schema() -> [SqliteDatabase; 5] {
    [
        SqliteDatabase::from(pplx_space_sbx_mgr_store::db::migration::target()),
        sqlite_template_manager(),
        sqlite_volume_manager(),
        sqlite_build_manager(),
        sqlite_runtime_manager(),
    ]
}

fn assert_common_jobs_table_matches_reader(db_path: &Path) {
    let expected = pplx_space_common_jobs::db::JOB_TABLE_COLUMNS
        .iter()
        .map(|column| column.to_string())
        .collect::<Vec<_>>();
    assert_eq!(jobs_columns(db_path), expected);

    let actual = expected.iter().map(String::as_str).collect::<BTreeSet<_>>();
    for column in pplx_space_common_jobs::db::selected_job_columns() {
        assert!(
            actual.contains(column),
            "jobs schema is missing selected column {column}"
        );
    }
    assert!(index_exists(db_path, "idx_jobs_lease_expiry"));
    assert!(index_exists(db_path, "idx_jobs_dedup_latest"));
    assert!(index_exists(db_path, "idx_jobs_created_at"));
    assert!(index_exists(db_path, "idx_jobs_status_created_at"));
    assert!(index_exists(db_path, "idx_jobs_subject_created_at"));
    assert!(index_exists(db_path, "idx_jobs_active_kind_status"));
    assert!(index_exists(db_path, "idx_jobs_pending_priority"));
}

fn assert_common_schema_migration_applied(db_path: &Path, version: i64) {
    let conn = Connection::open(db_path).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM _space_common_schema_migrations WHERE version = ?1 AND success = TRUE",
            params![version],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}

fn jobs_columns(db_path: &Path) -> Vec<String> {
    let conn = Connection::open(db_path).unwrap();
    let mut stmt = conn
        .prepare("SELECT name FROM pragma_table_info('jobs') ORDER BY cid")
        .unwrap();
    stmt.query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn index_exists(db_path: &Path, index: &str) -> bool {
    let conn = Connection::open(db_path).unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
        params![index],
        |row| row.get::<_, i64>(0),
    )
    .map(|count| count > 0)
    .unwrap()
}

mod postgres_baseline_tests;
mod public_baseline_rollback_tests;

fn assert_unique_migration_versions(name: &str, migrator: &Migrator) {
    let mut up_versions = BTreeSet::new();
    let mut down_versions = BTreeSet::new();

    for migration in migrator.iter() {
        if migration.migration_type.is_up_migration() {
            assert!(
                up_versions.insert(migration.version),
                "{name} has duplicate up migration version {}",
                migration.version
            );
            continue;
        }

        assert!(
            down_versions.insert(migration.version),
            "{name} has duplicate down migration version {}",
            migration.version
        );
    }
}

#[tokio::test]
async fn migration_creates_a_missing_database_parent() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("new/directory/volume-manager.db");

    migrate_sqlite_up(sqlite_volume_manager(), &db_path)
        .await
        .unwrap();

    assert!(db_path.exists());
    assert_common_jobs_table_matches_reader(&db_path);
}

fn common_history(conn: &Connection) -> rusqlite::Result<std::collections::BTreeMap<i64, Vec<u8>>> {
    conn.prepare("SELECT version, checksum FROM _space_common_schema_migrations ORDER BY version")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect()
}

async fn migrate_common(path: &Path, migrator: &Migrator) {
    let pool = sqlite_pool(path).await.unwrap();
    sqlite::common_migrator(migrator).run(&pool).await.unwrap();
    pool.close().await;
}
