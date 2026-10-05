use super::*;
use sqlx::SqlSafeStr as _;

fn catalogs() -> Vec<(&'static str, Migrator, Option<Migrator>)> {
    let common = || sqlx::migrate!("../../../../db/common");
    vec![
        (
            "node-manager",
            sqlx::migrate!("../../../../db/node-manager"),
            None,
        ),
        ("node-ctl", sqlx::migrate!("../../../../db/node-ctl"), None),
        (
            "sandbox-manager",
            sqlx::migrate!("../../../../db/sandbox-manager"),
            Some(common()),
        ),
        (
            "template-manager",
            sqlx::migrate!("../../../../db/template-manager"),
            Some(common()),
        ),
        (
            "volume-manager",
            sqlx::migrate!("../../../../db/volume-manager"),
            Some(common()),
        ),
        (
            "build-manager",
            sqlx::migrate!("../../../../db/build-manager"),
            Some(common()),
        ),
        (
            "runtime-manager",
            sqlx::migrate!("../../../../db/runtime-manager"),
            Some(common()),
        ),
    ]
}

fn public_common() -> Migrator {
    Migrator::with_migrations(vec![Migration::new(
        20260930000000,
        "baseline".into(),
        sqlx::migrate::MigrationType::ReversibleUp,
        include_str!("fixtures/common_baseline.sql").into_sql_str(),
        false,
    )])
}

fn prepared_database(store: &str, service: &Migrator, common: Option<&Migrator>) -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("PRAGMA foreign_keys=ON;
        CREATE TABLE _sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL,
            installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, success BOOLEAN NOT NULL,
            checksum BLOB NOT NULL, execution_time BIGINT NOT NULL);
        CREATE TABLE _space_common_schema_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL,
            installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, success BOOLEAN NOT NULL,
            checksum BLOB NOT NULL, execution_time BIGINT NOT NULL);").unwrap();
    if let Some(common) = common {
        for m in common.iter().filter(|m| m.migration_type.is_up_migration()) {
            conn.execute_batch(m.sql.as_ref()).unwrap();
        }
    }
    conn.execute_batch(first(service).unwrap().sql.as_ref())
        .unwrap();
    let manifest = CutoverManifest::embedded().unwrap();
    for (owner, table) in [(store, SERVICE_HISTORY), ("common", COMMON_HISTORY)] {
        if owner == "common" && common.is_none() {
            continue;
        }
        for (version, sums) in &manifest.stores[owner].migrations {
            conn.execute(
                &format!(
                    "INSERT INTO {table} (version, description, success, checksum, execution_time)
                VALUES (?1, 'historical', TRUE, ?2, 123)"
                ),
                params![version, hex::decode(&sums[0]).unwrap()],
            )
            .unwrap();
        }
    }
    let seed = match store {
        "node-manager" => "INSERT INTO network_pool_meta VALUES ('evidence', 'retained', 1)",
        "node-ctl" => {
            "INSERT INTO dirty_node_state (kind, resource_id, updated_at) VALUES ('evidence', 'retained', 1)"
        }
        _ => "INSERT INTO jobs (job_id, kind, payload) VALUES ('evidence', 'test', 'retained')",
    };
    conn.execute_batch(seed).unwrap();
    conn
}

fn schema(conn: &Connection) -> rusqlite::Result<Vec<(String, String, String, String)>> {
    conn.prepare(
        "SELECT type, name, tbl_name, sql FROM sqlite_schema
        WHERE sql IS NOT NULL AND name != '_space_baseline_archive'
        ORDER BY type, name",
    )?
    .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
    .collect()
}

fn evidence(conn: &Connection, store: &str) -> String {
    let query = match store {
        "node-manager" => "SELECT value FROM network_pool_meta WHERE key = 'evidence'",
        "node-ctl" => "SELECT resource_id FROM dirty_node_state WHERE kind = 'evidence'",
        _ => "SELECT payload FROM jobs WHERE job_id = 'evidence'",
    };
    conn.query_row(query, [], |r| r.get(0)).unwrap()
}

#[test]
fn every_store_cuts_over_once_preserving_data_and_original_history() {
    for (store, service, common) in catalogs() {
        let mut conn = prepared_database(store, &service, common.as_ref());
        let before = history(&conn, SERVICE_HISTORY).unwrap();
        let common_before = history(&conn, COMMON_HISTORY).unwrap();
        let before_schema = schema(&conn).unwrap();

        convert(store, &mut conn, &service, common.as_ref()).unwrap();
        convert(store, &mut conn, &service, common.as_ref()).unwrap();

        assert_eq!(evidence(&conn, store), "retained", "{store}");
        assert_eq!(schema(&conn).unwrap(), before_schema, "{store}");
        let expected_common = match common.as_ref() {
            Some(m) if super::super::super::baselines::is_public(m) => vec![(
                first(m).unwrap().version,
                true,
                first(m).unwrap().checksum.to_vec(),
            )],
            _ => common_before.clone(),
        };
        assert_eq!(
            history(&conn, COMMON_HISTORY).unwrap(),
            expected_common,
            "{store}"
        );
        assert_eq!(
            history(&conn, SERVICE_HISTORY).unwrap(),
            vec![(
                first(&service).unwrap().version,
                true,
                first(&service).unwrap().checksum.to_vec()
            )]
        );
        let archived: Vec<HistoryRow> = conn.prepare("SELECT version, success, checksum FROM _space_baseline_archive WHERE store = ?1 ORDER BY version")
            .unwrap().query_map([store], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(archived, before, "{store}");
        assert_eq!(
            conn.query_row(
                "SELECT MIN(execution_time) FROM _space_baseline_archive",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            123
        );
        if common.is_some() {
            let shared = public_common();
            convert(store, &mut conn, &service, Some(&shared)).unwrap();
            convert(store, &mut conn, &service, Some(&shared)).unwrap();
            assert_eq!(evidence(&conn, store), "retained");
            assert_eq!(schema(&conn).unwrap(), before_schema);
            assert_eq!(history(&conn, COMMON_HISTORY).unwrap().len(), 1);
            let archived: Vec<HistoryRow> = conn.prepare(
                "SELECT version, success, checksum FROM _space_baseline_archive WHERE store = 'common' ORDER BY version"
            ).unwrap().query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap().collect::<Result<_, _>>().unwrap();
            assert_eq!(archived, common_before);

            let mut together = prepared_database(store, &service, common.as_ref());
            convert(store, &mut together, &service, Some(&shared)).unwrap();
            assert_eq!(schema(&together).unwrap(), before_schema);
            assert_eq!(history(&together, SERVICE_HISTORY).unwrap().len(), 1);
            assert_eq!(history(&together, COMMON_HISTORY).unwrap().len(), 1);
            assert_eq!(evidence(&together, store), "retained");
        }
    }
}

#[test]
fn invalid_histories_leave_every_store_untouched() {
    for (store, service, common) in catalogs() {
        for damage in [
            "UPDATE _sqlx_migrations SET checksum = x'00'",
            "UPDATE _sqlx_migrations SET success = FALSE",
            "DELETE FROM _sqlx_migrations WHERE version = (SELECT MAX(version) FROM _sqlx_migrations)",
            "INSERT INTO _sqlx_migrations VALUES (999999, 'unknown', CURRENT_TIMESTAMP, TRUE, x'00', 0)",
        ] {
            let mut conn = prepared_database(store, &service, common.as_ref());
            conn.execute_batch(damage).unwrap();
            let before = history(&conn, SERVICE_HISTORY).unwrap();
            let before_schema = schema(&conn).unwrap();

            // Empty history is left for ordinary public admission, which rejects untracked schema.
            if before.is_empty() {
                continue;
            }
            assert!(
                convert(store, &mut conn, &service, common.as_ref()).is_err(),
                "{store}: {damage}"
            );

            assert_eq!(history(&conn, SERVICE_HISTORY).unwrap(), before, "{store}");
            assert_eq!(schema(&conn).unwrap(), before_schema, "{store}");
            assert_eq!(evidence(&conn, store), "retained", "{store}");
            assert!(!conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = '_space_baseline_archive')", [], |r| r.get::<_, bool>(0)).unwrap());
        }
    }
}

#[tokio::test]
async fn startup_uses_the_transition_and_survives_repeated_open() {
    let (store, service, common) = catalogs().remove(3);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let conn = prepared_database(store, &service, common.as_ref());
    conn.execute_batch(
        "PRAGMA foreign_keys=OFF;
         INSERT INTO template_snapshots VALUES ('missing-build', 'sha256:orphan', 2, 1024, 'snapshot', 'now');",
    )
    .unwrap();
    assert!(
        conn.prepare("PRAGMA foreign_key_check")
            .unwrap()
            .exists([])
            .unwrap()
    );
    conn.execute("VACUUM INTO ?1", [path.to_str().unwrap()])
        .unwrap();
    drop(conn);

    migrate(store, &path, &service).await.unwrap();
    migrate(store, &path, &service).await.unwrap();

    let conn = Connection::open(path).unwrap();
    assert_eq!(evidence(&conn, store), "retained");
    let snapshot: String = conn
        .query_row(
            "SELECT snapshot_hash FROM template_snapshots WHERE name = 'missing-build'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(snapshot, "snapshot");
    assert!(
        conn.prepare("PRAGMA foreign_key_check")
            .unwrap()
            .exists([])
            .unwrap()
    );
}

#[test]
fn failed_shared_cutover_rolls_back_service_cutover() {
    let (store, service, common) = catalogs().remove(2);
    let mut conn = prepared_database(store, &service, common.as_ref());
    conn.execute_batch(
        "CREATE TABLE _space_baseline_archive AS
        SELECT 'common' AS store, * FROM _space_common_schema_migrations",
    )
    .unwrap();
    let service_before = history(&conn, SERVICE_HISTORY).unwrap();
    let common_before = history(&conn, COMMON_HISTORY).unwrap();

    assert!(convert(store, &mut conn, &service, Some(&public_common())).is_err());

    assert_eq!(history(&conn, SERVICE_HISTORY).unwrap(), service_before);
    assert_eq!(history(&conn, COMMON_HISTORY).unwrap(), common_before);
    assert_eq!(evidence(&conn, store), "retained");
    let archives: i64 = conn
        .query_row(
            "SELECT count(*) FROM _space_baseline_archive WHERE store != 'common'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(archives, 0);
}
