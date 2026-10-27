//! One rootfs bound to two environments: each build keeps its own catalog
//! rows, snapshots and tag pins.

use pplx_space_common_core::types::{
    Arch, ArtifactComponentKind, ArtifactComponentMetadata, ArtifactFormat, ArtifactVersion,
    Digest, EnvironmentReference, ExecutionMetadata, TemplateManifest,
};
use rusqlite::Connection;

use super::*;

const NAME: &str = "seed";
const HASH: &str = "sha256:5555555555555555555555555555555555555555555555555555555555555555";

fn initial() -> EnvironmentReference {
    pplx_space_node_runtime_core::catalog::initial_environment_reference().unwrap()
}

fn other() -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([2; 32]))
}

fn twin() -> TemplateCachePartition {
    TemplateCachePartition::Environment(other())
}

fn manifest_json(environment: Option<EnvironmentReference>) -> String {
    let execution = ExecutionMetadata::initial_firecracker(
        Arch::X86_64,
        vec![ArtifactComponentMetadata {
            kind: ArtifactComponentKind::Kernel,
            role: None,
            version: Some(ArtifactVersion::parse("6.1.155").unwrap()),
            digest: Some(Digest::sha256([4; 32])),
            format: Some(ArtifactFormat::parse("vmlinux").unwrap()),
        }],
    );
    let execution = match environment {
        Some(environment) => execution.with_environment(environment),
        None => execution,
    };
    let mut manifest = TemplateManifest {
        runtime: "fc".to_string(),
        arch: "x86_64".to_string(),
        ..Default::default()
    };
    manifest.set_execution_metadata(execution);
    serde_json::to_string(&manifest).unwrap()
}

fn upsert(
    conn: &Connection,
    environment: Option<EnvironmentReference>,
    cache_partition: TemplateCachePartition,
    build_date: &str,
) -> Result<()> {
    upsert_build(
        conn,
        &BuildUpsert {
            name: NAME,
            content_hash: HASH,
            cache_partition,
            build_date,
            manifest_json: &manifest_json(environment),
            runtime: "fc",
            arch: "x86_64",
            artifact_size_bytes: 1,
            state: "ready",
        },
    )
}

fn seed_twins(conn: &Connection) {
    upsert(
        conn,
        Some(initial()),
        TemplateCachePartition::Legacy,
        "2026-10-01T00:00:00Z",
    )
    .unwrap();
    upsert(conn, Some(other()), twin(), "2026-10-01T00:00:00Z").unwrap();
}

fn environment_of(row: &BuildRow) -> Option<EnvironmentReference> {
    serde_json::from_str::<ExecutionMetadata>(row.execution_metadata_json.as_deref()?)
        .unwrap()
        .environment()
}

fn snapshot_key(cache_partition: TemplateCachePartition) -> SnapshotKey<'static> {
    SnapshotKey {
        name: NAME,
        content_hash: HASH,
        cache_partition,
        vcpu: 2,
        memory_mib: 1024,
    }
}

#[test]
fn twins_of_one_rootfs_resolve_their_own_builds() {
    let conn = test_conn();
    seed_twins(&conn);

    let legacy = get_build(&conn, NAME, HASH, TemplateCachePartition::Legacy)
        .unwrap()
        .unwrap();
    let environment = get_build(&conn, NAME, HASH, twin()).unwrap().unwrap();
    assert_eq!(environment_of(&legacy), Some(initial()));
    assert_eq!(environment_of(&environment), Some(other()));

    let partitions: Vec<_> = list_builds_for_hash(&conn, NAME, HASH)
        .unwrap()
        .into_iter()
        .map(|row| row.cache_partition)
        .collect();
    assert_eq!(partitions, [TemplateCachePartition::Legacy, twin()]);
    assert_eq!(
        list_build_partitions(&conn, NAME).unwrap(),
        [TemplateCachePartition::Legacy, twin()]
    );

    // Twins share a build date; environment-blind lookups prefer legacy.
    assert_eq!(
        get_latest_build(&conn, NAME, None)
            .unwrap()
            .unwrap()
            .cache_partition,
        TemplateCachePartition::Legacy
    );
    assert_eq!(
        get_latest_build_in_partition(&conn, NAME, twin())
            .unwrap()
            .unwrap()
            .cache_partition,
        twin()
    );
}

/// Same-date builds of different rootfs go to the larger content hash, the
/// order sandbox-manager's catalog and node-ctl use.
#[test]
fn same_date_builds_resolve_to_the_larger_content_hash() {
    let conn = test_conn();
    let larger = "sha256:7777777777777777777777777777777777777777777777777777777777777777";
    for hash in [HASH, larger] {
        upsert_build(
            &conn,
            &BuildUpsert {
                name: NAME,
                content_hash: hash,
                cache_partition: TemplateCachePartition::Legacy,
                build_date: "2026-10-01T00:00:00Z",
                manifest_json: &manifest_json(Some(initial())),
                runtime: "fc",
                arch: "x86_64",
                artifact_size_bytes: 1,
                state: "ready",
            },
        )
        .unwrap();
    }
    assert_eq!(
        get_latest_build(&conn, NAME, None)
            .unwrap()
            .unwrap()
            .content_hash,
        larger
    );
    assert_eq!(
        get_latest_build_in_partition(&conn, NAME, TemplateCachePartition::Legacy)
            .unwrap()
            .unwrap()
            .content_hash,
        larger
    );
}

/// A newer row the migration filed as legacy for a build bound to another
/// environment doesn't hide an older build that belongs in the partition.
#[test]
fn a_partition_s_latest_build_skips_rows_bound_elsewhere() {
    let conn = test_conn();
    upsert(
        &conn,
        Some(initial()),
        TemplateCachePartition::Legacy,
        "2026-10-01T00:00:00Z",
    )
    .unwrap();
    let misfiled = "sha256:7777777777777777777777777777777777777777777777777777777777777777";
    upsert_build(
        &conn,
        &BuildUpsert {
            name: NAME,
            content_hash: misfiled,
            cache_partition: twin(),
            build_date: "2026-10-20T00:00:00Z",
            manifest_json: &manifest_json(Some(other())),
            runtime: "fc",
            arch: "x86_64",
            artifact_size_bytes: 1,
            state: "ready",
        },
    )
    .unwrap();
    // As the migration files it; the runtime connection enforces no keys.
    conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
    conn.execute(
        "UPDATE template_builds SET cache_partition = '' WHERE content_hash = ?1",
        [misfiled],
    )
    .unwrap();

    for latest in [
        get_latest_build_in_partition(&conn, NAME, TemplateCachePartition::Legacy).unwrap(),
        crate::pool_manager::pool_base_build(&conn, NAME).unwrap(),
    ] {
        assert_eq!(latest.unwrap().content_hash, HASH);
    }
}

/// A pool refill names its legacy build, defers to a pull when nothing is
/// cached, and refuses when only other environments' builds are.
#[test]
fn pool_refills_name_the_legacy_build_or_refuse() {
    let conn = test_conn();
    assert_eq!(
        crate::pool_manager::pool_refill_hash(&conn, NAME).unwrap(),
        None
    );
    upsert(&conn, Some(other()), twin(), "2026-10-01T00:00:00Z").unwrap();
    assert!(matches!(
        crate::pool_manager::pool_refill_hash(&conn, NAME),
        Err(crate::pool_manager::PoolError::NoPoolBuild)
    ));
    upsert(
        &conn,
        Some(initial()),
        TemplateCachePartition::Legacy,
        "2026-09-01T00:00:00Z",
    )
    .unwrap();
    assert_eq!(
        crate::pool_manager::pool_refill_hash(&conn, NAME).unwrap(),
        Some(HASH.to_string())
    );
}

/// A pool whose template has only another environment's build stays empty:
/// its fill refuses before asking sandbox-manager for a donor, so there's no
/// member to clone or adopt.
#[tokio::test(flavor = "current_thread")]
async fn pools_without_a_legacy_build_create_no_donor() {
    let conn = test_conn();
    upsert(&conn, Some(other()), twin(), "2026-10-01T00:00:00Z").unwrap();
    let db = std::sync::Arc::new(std::sync::Mutex::new(conn));
    let created = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        crate::pool_manager::create_from_base(db, NAME.to_string(), true, 2, 512),
    )
    .await
    .expect("the fill refuses without dialing sandbox-manager");
    assert!(
        matches!(created, Err(crate::pool_manager::PoolError::NoPoolBuild)),
        "{created:?}"
    );
}

/// A pool boots its template's newest legacy build, never a newer one bound
/// to another environment.
#[test]
fn pools_boot_the_newest_legacy_build() {
    let conn = test_conn();
    upsert(
        &conn,
        Some(initial()),
        TemplateCachePartition::Legacy,
        "2026-10-01T00:00:00Z",
    )
    .unwrap();
    let newer = "sha256:7777777777777777777777777777777777777777777777777777777777777777";
    upsert_build(
        &conn,
        &BuildUpsert {
            name: NAME,
            content_hash: newer,
            cache_partition: twin(),
            build_date: "2026-10-20T00:00:00Z",
            manifest_json: &manifest_json(Some(other())),
            runtime: "fc",
            arch: "x86_64",
            artifact_size_bytes: 1,
            state: "ready",
        },
    )
    .unwrap();
    assert_eq!(
        get_latest_build(&conn, NAME, None)
            .unwrap()
            .unwrap()
            .content_hash,
        newer
    );
    let pool = crate::pool_manager::pool_base_build(&conn, NAME)
        .unwrap()
        .unwrap();
    assert_eq!(
        (pool.content_hash.as_str(), pool.cache_partition),
        (HASH, TemplateCachePartition::Legacy)
    );
}

#[test]
fn lookups_prefer_the_selected_partition_then_legacy() {
    let conn = test_conn();
    // A newer twin of a different rootfs, bound to another environment.
    upsert(
        &conn,
        Some(initial()),
        TemplateCachePartition::Legacy,
        "2026-10-01T00:00:00Z",
    )
    .unwrap();
    let newer = "sha256:7777777777777777777777777777777777777777777777777777777777777777";
    upsert_build(
        &conn,
        &BuildUpsert {
            name: NAME,
            content_hash: newer,
            cache_partition: twin(),
            build_date: "2026-10-20T00:00:00Z",
            manifest_json: &manifest_json(Some(other())),
            runtime: "fc",
            arch: "x86_64",
            artifact_size_bytes: 1,
            state: "ready",
        },
    )
    .unwrap();
    let latest = |preferred| get_latest_build(&conn, NAME, preferred).unwrap().unwrap();
    // Every lookup gets the newest build, which the caller checks as a
    // compatible cold-boot source.
    assert_eq!(latest(None).content_hash, newer);
    assert_eq!(
        latest(Some(TemplateCachePartition::Legacy)).content_hash,
        newer
    );
    assert_eq!(latest(Some(twin())).content_hash, newer);
    let unknown =
        TemplateCachePartition::Environment(EnvironmentReference::new(Digest::sha256([3; 32])));
    assert_eq!(latest(Some(unknown)).content_hash, newer);
    assert_eq!(
        get_latest_build_in_partition(&conn, NAME, unknown)
            .unwrap()
            .map(|row| row.content_hash),
        None
    );

    // Exact digests: the selected partition, else legacy.
    upsert(&conn, Some(other()), twin(), "2026-10-01T00:00:00Z").unwrap();
    let exact = |preferred| {
        find_ready_build(&conn, NAME, HASH, preferred)
            .unwrap()
            .unwrap()
            .cache_partition
    };
    assert_eq!(exact(None), TemplateCachePartition::Legacy);
    assert_eq!(exact(Some(twin())), twin());
    assert_eq!(exact(Some(unknown)), TemplateCachePartition::Legacy);

    // Newest-date ties, like a release and its twin, prefer the selected
    // partition.
    upsert_build(
        &conn,
        &BuildUpsert {
            name: NAME,
            content_hash: newer,
            cache_partition: TemplateCachePartition::Legacy,
            build_date: "2026-10-20T00:00:00Z",
            manifest_json: &manifest_json(Some(initial())),
            runtime: "fc",
            arch: "x86_64",
            artifact_size_bytes: 1,
            state: "ready",
        },
    )
    .unwrap();
    assert_eq!(latest(Some(twin())).cache_partition, twin());
    assert_eq!(
        latest(Some(TemplateCachePartition::Legacy)).cache_partition,
        TemplateCachePartition::Legacy
    );
    assert_eq!(latest(None).cache_partition, TemplateCachePartition::Legacy);
}

#[test]
fn twins_keep_separate_components_and_base_snapshots() {
    let conn = test_conn();
    seed_twins(&conn);
    let components: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM template_build_components",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(components, 2);
    upsert_snapshot(
        &conn,
        snapshot_key(TemplateCachePartition::Legacy),
        "legacy",
    )
    .unwrap();
    upsert_snapshot(&conn, snapshot_key(twin()), "twin").unwrap();

    let hash_of = |partition| {
        get_snapshot(&conn, snapshot_key(partition))
            .unwrap()
            .map(|row| row.snapshot_hash)
    };
    assert_eq!(
        hash_of(TemplateCachePartition::Legacy).as_deref(),
        Some("legacy")
    );
    assert_eq!(hash_of(twin()).as_deref(), Some("twin"));

    delete_build(&conn, NAME, HASH, twin()).unwrap();
    assert!(get_build(&conn, NAME, HASH, twin()).unwrap().is_none());
    assert_eq!(hash_of(twin()), None);
    assert!(
        get_build(&conn, NAME, HASH, TemplateCachePartition::Legacy)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        hash_of(TemplateCachePartition::Legacy).as_deref(),
        Some("legacy")
    );
    let components: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM template_build_components WHERE cache_partition = ''",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let twin_components: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM template_build_components WHERE cache_partition != ''",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!((components, twin_components), (1, 0));
}

#[test]
fn builds_are_refused_outside_their_binding_partition() {
    let conn = test_conn();
    for (environment, partition) in [
        // Bound to another environment, filed as legacy.
        (Some(other()), TemplateCachePartition::Legacy),
        // The initial environment's builds live in the legacy partition.
        (
            Some(initial()),
            TemplateCachePartition::Environment(initial()),
        ),
        // Unbound builds can't claim an environment.
        (None, twin()),
        // A twin can't be filed under a sibling environment.
        (
            Some(other()),
            TemplateCachePartition::Environment(EnvironmentReference::new(Digest::sha256([3; 32]))),
        ),
    ] {
        assert!(
            upsert(&conn, environment, partition, "2026-10-01T00:00:00Z").is_err(),
            "{environment:?} in {partition}"
        );
    }
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM template_builds", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 0);
    upsert(
        &conn,
        None,
        TemplateCachePartition::Legacy,
        "2026-10-01T00:00:00Z",
    )
    .unwrap();
}

#[test]
fn tag_pins_are_kept_per_partition_and_protect_the_digest() {
    let conn = std::sync::Arc::new(std::sync::Mutex::new(test_conn()));
    seed_twins(&conn.lock().unwrap());
    let pins = crate::pinned_digests::PinnedDigestsStore::new(std::sync::Arc::clone(&conn));
    pins.upsert_tag(NAME, "latest", TemplateCachePartition::Legacy, HASH)
        .unwrap();
    pins.upsert_tag(NAME, "latest", twin(), HASH).unwrap();
    let count: i64 = conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM template_pinned_tags WHERE name = ?1 AND tag = 'latest'",
            [NAME],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);
    assert_eq!(
        list_pinned_build_keys(&conn.lock().unwrap()).unwrap(),
        [(NAME.to_string(), HASH.to_string())]
    );
}

/// Runs one template-manager migration. The shared `db/common` catalog has
/// its own baseline at the same version, so it's applied separately.
fn apply(conn: &Connection, version: i64, up: bool) {
    let service = sqlx::migrate!("../../../../db/template-manager");
    for migration in service.iter().filter(|migration| {
        migration.version == version && migration.migration_type.is_up_migration() == up
    }) {
        // As the production migrator runs it: foreign keys enforced, one
        // transaction per migration. The runtime connection leaves them off.
        conn.execute_batch("PRAGMA foreign_keys = ON; BEGIN;")
            .unwrap();
        conn.execute_batch(migration.sql.as_ref()).unwrap();
        conn.execute_batch("COMMIT; PRAGMA foreign_keys = OFF;")
            .unwrap();
    }
}

fn apply_common_and_baseline(conn: &Connection) {
    let common = sqlx::migrate!("../../../../db/common");
    for migration in common
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        conn.execute_batch(migration.sql.as_ref()).unwrap();
    }
    apply(conn, 20260930000000, true);
}

const PARTITIONS: i64 = 20261026220935;

#[test]
fn partition_migration_keeps_existing_rows_in_the_legacy_partition() {
    let conn = Connection::open_in_memory().unwrap();
    apply_common_and_baseline(&conn);
    conn.execute_batch(&format!(
        "INSERT INTO template_builds (name, content_hash, build_date, manifest_json, runtime, arch, created_at)
         VALUES ('{NAME}', '{HASH}', 'd', '{{}}', 'fc', 'x86_64', '1');
         INSERT INTO template_build_components (name, content_hash, kind, role_key)
         VALUES ('{NAME}', '{HASH}', 'kernel', '');
         INSERT INTO template_snapshots (name, content_hash, vcpu, memory_mib, snapshot_hash, built_at)
         VALUES ('{NAME}', '{HASH}', 2, 1024, 'snap', '1');
         INSERT INTO template_pinned_tags (name, tag, content_hash, updated_at)
         VALUES ('{NAME}', 'latest', '{HASH}', '1');"
    ))
    .unwrap();

    apply(&conn, PARTITIONS, true);
    // The rebuilt child tables reference the renamed parent, not the
    // temporary table the migration built it as.
    for child in ["template_build_components", "template_snapshots"] {
        let parents: Vec<String> = conn
            .prepare(&format!(
                "SELECT DISTINCT \"table\" FROM pragma_foreign_key_list('{child}')"
            ))
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(parents, ["template_builds"], "{child}");
    }

    for table in [
        "template_builds",
        "template_build_components",
        "template_snapshots",
        "template_pinned_tags",
    ] {
        let partitions: Vec<String> = conn
            .prepare(&format!("SELECT cache_partition FROM {table}"))
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(partitions, [""], "{table}");
    }
    let legacy = get_build(&conn, NAME, HASH, TemplateCachePartition::Legacy)
        .unwrap()
        .unwrap();
    assert_eq!(legacy.build_date, "d");
    assert!(
        get_snapshot(&conn, snapshot_key(TemplateCachePartition::Legacy))
            .unwrap()
            .is_some()
    );
    // The rebuilt tables accept a twin next to the migrated row.
    upsert(&conn, Some(other()), twin(), "2026-10-01T00:00:00Z").unwrap();
    upsert_snapshot(&conn, snapshot_key(twin()), "twin").unwrap();
}

/// Without enforced foreign keys, eviction and recovery leave child rows of
/// deleted builds behind; the migrations drop them rather than fail on them.
#[test]
fn partition_migrations_drop_child_rows_of_deleted_builds() {
    let conn = Connection::open_in_memory().unwrap();
    apply_common_and_baseline(&conn);
    let orphans = |conn: &Connection| {
        conn.execute(
            "INSERT INTO template_build_components (name, content_hash, kind, role_key)
             VALUES (?1, ?2, 'kernel', '')",
            [NAME, HASH],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO template_snapshots (name, content_hash, vcpu, memory_mib, snapshot_hash, built_at)
             VALUES (?1, ?2, 2, 1024, 'orphan', '0')",
            [NAME, HASH],
        )
        .unwrap();
    };
    let count = |conn: &Connection, table: &str| -> i64 {
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
    };
    orphans(&conn);

    apply(&conn, PARTITIONS, true);
    assert_eq!(count(&conn, "template_build_components"), 0);
    assert_eq!(count(&conn, "template_snapshots"), 0);

    orphans(&conn);
    apply(&conn, PARTITIONS, false);
    assert_eq!(count(&conn, "template_build_components"), 0);
    assert_eq!(count(&conn, "template_snapshots"), 0);
}

#[test]
fn partition_down_migration_keeps_only_legacy_rows() {
    let conn = Connection::open_in_memory().unwrap();
    apply_common_and_baseline(&conn);
    apply(&conn, PARTITIONS, true);
    seed_twins(&conn);
    upsert_snapshot(
        &conn,
        snapshot_key(TemplateCachePartition::Legacy),
        "legacy",
    )
    .unwrap();
    upsert_snapshot(&conn, snapshot_key(twin()), "twin").unwrap();
    for (job_id, partition) in [
        ("legacy-stamp", TemplateCachePartition::Legacy),
        ("twin-stamp", twin()),
    ] {
        conn.execute(
            "INSERT INTO jobs (job_id, kind, payload) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                job_id,
                crate::background_jobs::STAMP_JOB_KIND,
                serde_json::to_string(&crate::background_jobs::StampPayload::new(
                    NAME, HASH, partition
                ))
                .unwrap(),
            ],
        )
        .unwrap();
    }
    // Jobs whose payload isn't JSON survive untouched.
    for (job_id, kind) in [
        ("empty-stamp", crate::background_jobs::STAMP_JOB_KIND),
        ("empty-other", "other"),
    ] {
        conn.execute(
            "INSERT INTO jobs (job_id, kind, payload) VALUES (?1, ?2, '')",
            [job_id, kind],
        )
        .unwrap();
    }

    apply(&conn, PARTITIONS, false);
    for child in ["template_build_components", "template_snapshots"] {
        let parents: Vec<String> = conn
            .prepare(&format!(
                "SELECT DISTINCT \"table\" FROM pragma_foreign_key_list('{child}')"
            ))
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(parents, ["template_builds"], "{child}");
    }

    let jobs: Vec<String> = conn
        .prepare("SELECT job_id FROM jobs ORDER BY job_id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(jobs, ["empty-other", "empty-stamp", "legacy-stamp"]);

    let builds: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM template_builds WHERE name = ?1 AND content_hash = ?2",
            [NAME, HASH],
            |row| row.get(0),
        )
        .unwrap();
    let snapshot: String = conn
        .query_row(
            "SELECT snapshot_hash FROM template_snapshots WHERE name = ?1",
            [NAME],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!((builds, snapshot.as_str()), (1, "legacy"));
}

#[test]
fn a_twin_mid_eviction_holds_back_pulls_of_its_digest() {
    let conn = test_conn();
    seed_twins(&conn);
    assert!(!is_build_marked_deleting(&conn, NAME, HASH).unwrap());
    conn.execute(
        "UPDATE template_builds SET state = 'deleting' WHERE cache_partition = ?1",
        [twin().key()],
    )
    .unwrap();
    // A pull learns its partition only from the release manifest.
    assert!(is_build_marked_deleting(&conn, NAME, HASH).unwrap());
}
