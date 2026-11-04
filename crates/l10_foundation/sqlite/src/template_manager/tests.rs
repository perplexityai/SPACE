use std::sync::LazyLock;

use super::*;

static BUNDLED: Migrator = sqlx::migrate!("../../../db/template-manager");
static COMMON: Migrator = sqlx::migrate!("../../../db/common");
/// The catalog of a release that bundles the retired migration.
static PARTITIONED: LazyLock<Migrator> = LazyLock::new(|| {
    let mut migrations = BUNDLED.migrations.to_vec();
    migrations.push(retired_migration());
    Migrator::with_migrations(migrations)
});

async fn migrate(path: &Path, service: &Migrator) -> SqliteResult<()> {
    migrate_embedded(
        STORE,
        path,
        EmbeddedMigrators {
            service,
            common: Some(&COMMON),
        },
    )
    .await
}

/// Legacy-partition rows in every rebuilt table, written without naming the
/// partition so the same statements fit either schema. The jobs cover a stamp
/// job without an environment and an other-kind job with an empty payload.
fn seed_legacy(conn: &Connection) {
    conn.execute_batch(
        "INSERT INTO template_builds (name, content_hash, build_date, manifest_json, runtime, arch, created_at)
         VALUES ('seed', 'sha256:aa', '2026-10-01T00:00:00Z', '{}', 'firecracker', 'x86_64', '2026-10-01T00:00:00Z');
         INSERT INTO template_build_components (name, content_hash, kind, role_key, version)
         VALUES ('seed', 'sha256:aa', 'kernel', '', '6.1');
         INSERT INTO template_snapshots (name, content_hash, vcpu, memory_mib, snapshot_hash, built_at)
         VALUES ('seed', 'sha256:aa', 2, 2048, 'sha256:bb', '2026-10-01T00:00:00Z');
         INSERT INTO template_pinned_tags (name, tag, content_hash, updated_at)
         VALUES ('seed', 'latest', 'sha256:aa', '2026-10-01T00:00:00Z');
         INSERT INTO jobs (job_id, kind, payload)
         VALUES ('stamp', 'template_stamp', '{\"name\":\"seed\"}'), ('other', 'template_pull', '');",
    )
    .unwrap();
}

/// One row outside the legacy partition that reversing would lose.
const NON_LEGACY: [&str; 3] = [
    "INSERT INTO template_builds (name, content_hash, cache_partition, build_date, manifest_json, runtime, arch, created_at)
     VALUES ('seed', 'sha256:aa', 'sha256:env', '2026-10-01T00:00:00Z', '{}', 'firecracker', 'x86_64', '2026-10-01T00:00:00Z')",
    "INSERT INTO template_pinned_tags (name, tag, cache_partition, content_hash, updated_at)
     VALUES ('seed', 'latest', 'sha256:env', 'sha256:aa', '2026-10-01T00:00:00Z')",
    "INSERT INTO jobs (job_id, kind, payload)
     VALUES ('twin-stamp', 'template_stamp', '{\"partition_environment\":\"sha256:env\"}')",
];

/// One row outside the legacy partition that the down SQL drops with nothing
/// to keep: a child whose environment build is gone, and a finished stamp job.
const DISPOSABLE: [&str; 3] = [
    "INSERT INTO template_build_components (name, content_hash, cache_partition, kind, role_key)
     VALUES ('seed', 'sha256:aa', 'sha256:env', 'kernel', '')",
    "INSERT INTO template_snapshots (name, content_hash, cache_partition, vcpu, memory_mib, snapshot_hash, built_at)
     VALUES ('seed', 'sha256:aa', 'sha256:env', 2, 2048, 'sha256:bb', '2026-10-01T00:00:00Z')",
    "INSERT INTO jobs (job_id, kind, status, payload)
     VALUES ('twin-stamp', 'template_stamp', 'completed', '{\"partition_environment\":\"sha256:env\"}')",
];

/// Every seeded row, by the columns both schemas share.
fn rows(conn: &Connection) -> Vec<String> {
    [
        "SELECT name || '|' || content_hash || '|' || build_date || '|' || state FROM template_builds",
        "SELECT name || '|' || content_hash || '|' || kind || '|' || role_key FROM template_build_components",
        "SELECT name || '|' || content_hash || '|' || vcpu || '|' || memory_mib || '|' || snapshot_hash FROM template_snapshots",
        "SELECT name || '|' || tag || '|' || content_hash FROM template_pinned_tags",
        "SELECT job_id || '|' || kind || '|' || payload FROM jobs",
    ]
    .iter()
    .flat_map(|query| {
        conn.prepare(&format!("{query} ORDER BY 1"))
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    })
    .collect()
}

/// The application schema, ignoring the quotes `ALTER TABLE ... RENAME`
/// writes around table names.
fn schema(conn: &Connection) -> Vec<(String, String)> {
    public_application_schema(conn)
        .unwrap()
        .into_iter()
        .map(|(name, sql)| (name, sql.replace('"', "")))
        .collect()
}

fn history(conn: &Connection) -> Vec<i64> {
    conn.prepare("SELECT version FROM _sqlx_migrations ORDER BY version")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

async fn fresh_schema(service: &Migrator) -> Vec<(String, String)> {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("template-manager.db");
    migrate(&path, service).await.unwrap();
    schema(&Connection::open(&path).unwrap())
}

#[test]
fn retired_migration_is_the_one_previous_releases_recorded() {
    assert_eq!(
        hex::encode(retired_migration().checksum.as_ref()),
        "bb39d295eee07bfc5ff54e6d2f076efb42a50b43d7cedffd8907e9cbf73e7e9b2837295cb1131306798e1c3a4bf4b820"
    );
}

#[tokio::test]
async fn a_bundled_schema_is_left_untouched() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("template-manager.db");
    migrate(&path, &BUNDLED).await.unwrap();
    seed_legacy(&Connection::open(&path).unwrap());
    let before = rows(&Connection::open(&path).unwrap());

    migrate(&path, &BUNDLED).await.unwrap();

    let conn = Connection::open(&path).unwrap();
    assert_eq!(rows(&conn), before);
    assert_eq!(history(&conn), [crate::baselines::PUBLIC_SQLITE_BASELINE]);
    assert_eq!(schema(&conn), fresh_schema(&BUNDLED).await);
}

#[tokio::test]
async fn a_legacy_only_partitioned_database_reverses_and_reapplies() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("template-manager.db");
    migrate(&path, &PARTITIONED).await.unwrap();
    seed_legacy(&Connection::open(&path).unwrap());
    let seeded = rows(&Connection::open(&path).unwrap());
    let bundled = fresh_schema(&BUNDLED).await;
    let partitioned = fresh_schema(&PARTITIONED).await;

    // Rolling back, forward again, then back once more is image-only each way.
    for (service, expected, versions) in [
        (
            &BUNDLED,
            &bundled,
            vec![crate::baselines::PUBLIC_SQLITE_BASELINE],
        ),
        (
            &*PARTITIONED,
            &partitioned,
            vec![crate::baselines::PUBLIC_SQLITE_BASELINE, VERSION],
        ),
        (
            &BUNDLED,
            &bundled,
            vec![crate::baselines::PUBLIC_SQLITE_BASELINE],
        ),
    ] {
        migrate(&path, service).await.unwrap();

        let conn = Connection::open(&path).unwrap();
        assert_eq!(rows(&conn), seeded);
        assert_eq!(history(&conn), versions);
        assert_eq!(&schema(&conn), expected);
    }
}

#[tokio::test]
async fn a_row_outside_the_legacy_partition_refuses_reversal() {
    for statement in NON_LEGACY {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("template-manager.db");
        migrate(&path, &PARTITIONED).await.unwrap();
        let conn = Connection::open(&path).unwrap();
        seed_legacy(&conn);
        // Each case is the only non-legacy row, so a child has no parent.
        conn.execute_batch("PRAGMA foreign_keys=OFF;").unwrap();
        conn.execute(statement, []).unwrap();
        let before = (rows(&conn), schema(&conn));

        let error = migrate(&path, &BUNDLED).await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("outside the legacy template cache partition"),
            "{statement}: {error}"
        );
        let conn = Connection::open(&path).unwrap();
        assert_eq!((rows(&conn), schema(&conn)), before);
        assert_eq!(
            history(&conn),
            [crate::baselines::PUBLIC_SQLITE_BASELINE, VERSION]
        );
    }
}

#[tokio::test]
async fn orphaned_children_and_finished_stamp_jobs_do_not_block_reversal() {
    for statement in DISPOSABLE {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("template-manager.db");
        migrate(&path, &PARTITIONED).await.unwrap();
        let conn = Connection::open(&path).unwrap();
        seed_legacy(&conn);
        let seeded = rows(&conn);
        conn.execute_batch("PRAGMA foreign_keys=OFF;").unwrap();
        conn.execute(statement, []).unwrap();

        migrate(&path, &BUNDLED).await.unwrap();

        let conn = Connection::open(&path).unwrap();
        assert_eq!(rows(&conn), seeded, "{statement}");
        assert_eq!(history(&conn), [crate::baselines::PUBLIC_SQLITE_BASELINE]);
    }
}

#[tokio::test]
async fn a_failed_or_modified_retired_migration_refuses_reversal() {
    for update in ["checksum = x'00'", "success = 0"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("template-manager.db");
        migrate(&path, &PARTITIONED).await.unwrap();
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            &format!("UPDATE _sqlx_migrations SET {update} WHERE version = ?1"),
            [VERSION],
        )
        .unwrap();

        let error = migrate(&path, &BUNDLED).await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed or modified retired migration"),
            "{update}: {error}"
        );
        assert_eq!(
            schema(&Connection::open(&path).unwrap()),
            fresh_schema(&PARTITIONED).await
        );
    }
}
