//! One rootfs bound to two environments: each build keeps its own catalog
//! rows, snapshots and tag pins.

use pplx_space_common_core::types::{
    ArtifactComponentKind, ArtifactComponentMetadata, ArtifactFormat, ArtifactVersion, Digest,
    EnvironmentReference, ExecutionMetadata,
};
use pplx_space_cpu_arch::Arch;
use pplx_space_template_manifest::TemplateManifest;
use rusqlite::Connection;

use super::*;

const NAME: &str = "seed";
const HASH: &str = "sha256:5555555555555555555555555555555555555555555555555555555555555555";

fn initial() -> EnvironmentReference {
    pplx_space_node_runtime_core::catalog::initial_environment_reference()
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

    // A latest row that doesn't decode is an error, not a missing build.
    conn.execute(
        "UPDATE template_builds SET artifact_size_bytes = -1 WHERE content_hash = ?1",
        [larger],
    )
    .unwrap();
    get_latest_build(&conn, NAME, None).unwrap_err();
    get_latest_build_in_partition(&conn, NAME, TemplateCachePartition::Legacy).unwrap_err();
}

/// A newer row filed under an environment's partition but bound to another
/// environment doesn't hide an older build that belongs in the partition.
#[test]
fn an_environment_partition_s_latest_build_skips_rows_bound_elsewhere() {
    let conn = test_conn();
    upsert(&conn, Some(other()), twin(), "2026-10-01T00:00:00Z").unwrap();
    let elsewhere = EnvironmentReference::new(Digest::sha256([3; 32]));
    let misfiled = "sha256:7777777777777777777777777777777777777777777777777777777777777777";
    upsert_build(
        &conn,
        &BuildUpsert {
            name: NAME,
            content_hash: misfiled,
            cache_partition: TemplateCachePartition::Environment(elsewhere),
            build_date: "2026-10-20T00:00:00Z",
            manifest_json: &manifest_json(Some(elsewhere)),
            runtime: "fc",
            arch: "x86_64",
            artifact_size_bytes: 1,
            state: "ready",
        },
    )
    .unwrap();
    // The store refuses this filing; the runtime connection enforces no keys.
    conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
    conn.execute(
        "UPDATE template_builds SET cache_partition = ?1 WHERE content_hash = ?2",
        rusqlite::params![twin().key(), misfiled],
    )
    .unwrap();

    assert_eq!(
        get_latest_build_in_partition(&conn, NAME, twin())
            .unwrap()
            .unwrap()
            .content_hash,
        HASH
    );
}

/// Startup recovery adopts a legacy-layout build bound to another environment
/// in place, so it's the legacy partition's newest build.
#[test]
fn the_legacy_partition_s_latest_build_takes_an_adopted_build() {
    let conn = test_conn();
    upsert(
        &conn,
        Some(initial()),
        TemplateCachePartition::Legacy,
        "2026-10-01T00:00:00Z",
    )
    .unwrap();
    let adopted = "sha256:7777777777777777777777777777777777777777777777777777777777777777";
    upsert_build(
        &conn,
        &BuildUpsert {
            name: NAME,
            content_hash: adopted,
            cache_partition: TemplateCachePartition::Legacy,
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
        get_latest_build_in_partition(&conn, NAME, TemplateCachePartition::Legacy)
            .unwrap()
            .unwrap()
            .content_hash,
        adopted
    );
}

/// A pool-capable Cloud Hypervisor environment from the node catalog.
fn cloud_hypervisor_manifest() -> String {
    let catalog =
        pplx_space_execution_catalog::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap();
    let linux = catalog
        .iter()
        .find(|entry| entry.name() == "ch-54.0-linux-7.2.8")
        .unwrap();
    let mut manifest = TemplateManifest::default();
    manifest.set_execution_metadata(linux.definition().execution_requirements().unwrap());
    serde_json::to_string(&manifest).unwrap()
}

/// Files a build of `runtime` under the partition its manifest binds.
fn upsert_pool_candidate(
    conn: &Connection,
    content_hash: &str,
    build_date: &str,
    runtime: &str,
    manifest_json: &str,
) -> TemplateCachePartition {
    let manifest: TemplateManifest = serde_json::from_str(manifest_json).unwrap();
    let cache_partition = pplx_space_node_runtime_core::catalog::template_cache_partition(
        manifest.environment().unwrap(),
    );
    upsert_build(
        conn,
        &BuildUpsert {
            name: NAME,
            content_hash,
            cache_partition,
            build_date,
            manifest_json,
            runtime,
            arch: "x86_64",
            artifact_size_bytes: 1,
            state: "ready",
        },
    )
    .unwrap();
    cache_partition
}

const CH_HASH: &str = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

/// A Cloud Hypervisor build bound to its environment is filed in that
/// environment's partition and still fills its template's pool: a refill
/// names it, with its partition, and records the use there.
#[test]
fn a_cloud_hypervisor_environment_build_fills_a_pool_from_its_partition() {
    let conn = test_conn();
    let partition = upsert_pool_candidate(
        &conn,
        CH_HASH,
        "2026-10-01T00:00:00Z",
        "ch",
        &cloud_hypervisor_manifest(),
    );
    assert_ne!(partition, TemplateCachePartition::Legacy);
    conn.execute("UPDATE template_builds SET last_used_at = '0'", [])
        .unwrap();

    let refill = crate::pool_manager::pool_refill_build(&conn, NAME)
        .unwrap()
        .unwrap();

    assert_eq!(
        (refill.content_hash.as_str(), refill.cache_partition),
        (CH_HASH, partition)
    );
    let last_used: String = conn
        .query_row(
            "SELECT last_used_at FROM template_builds WHERE content_hash = ?1 AND cache_partition = ?2",
            rusqlite::params![CH_HASH, partition.key()],
            |row| row.get(0),
        )
        .unwrap();
    assert_ne!(last_used, "0");
}

/// Pool creates and clones reach sandbox-manager without an environment, and
/// its lookup of a digest prefers the legacy build. So an environment build
/// whose digest also has a legacy row is never a pool's build: the lookup
/// would boot the legacy row instead.
#[test]
fn pools_skip_an_environment_build_whose_digest_has_a_legacy_row() {
    let conn = test_conn();
    upsert_pool_candidate(
        &conn,
        CH_HASH,
        "2026-10-01T00:00:00Z",
        "ch",
        &cloud_hypervisor_manifest(),
    );
    // Not pool-capable, but what an environment-less lookup of the digest
    // resolves.
    upsert_pool_candidate(&conn, CH_HASH, "2026-09-01T00:00:00Z", "gvisor", "{}");

    assert!(
        crate::pool_manager::pool_base_build(&conn, NAME)
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        crate::pool_manager::pool_refill_build(&conn, NAME),
        Err(crate::pool_manager::PoolError::NoPoolBuild)
    ));
}

/// The environment-less lookup returns a digest's row with the lowest
/// partition key even when none is legacy, so a pool skips an eligible build
/// that another environment's row of the same digest outranks there.
#[test]
fn pools_skip_a_build_the_environment_less_lookup_would_not_resolve() {
    let conn = test_conn();
    let partition = upsert_pool_candidate(
        &conn,
        CH_HASH,
        "2026-10-01T00:00:00Z",
        "ch",
        &cloud_hypervisor_manifest(),
    );
    let lower = EnvironmentReference::new(Digest::sha256([0; 32]));
    let lower_partition = upsert_pool_candidate(
        &conn,
        CH_HASH,
        "2026-09-01T00:00:00Z",
        "fc",
        &manifest_json(Some(lower)),
    );
    assert!(lower_partition.key() < partition.key());
    assert_eq!(
        find_ready_build(&conn, NAME, CH_HASH, None)
            .unwrap()
            .unwrap()
            .cache_partition,
        lower_partition
    );

    assert!(
        crate::pool_manager::pool_base_build(&conn, NAME)
            .unwrap()
            .is_none()
    );
}

/// An environment partition's row whose binding names another partition is
/// never a pool's build: one filed under another environment, and one whose
/// binding can't be read, which reads as unbound.
#[rstest::rstest]
#[case::filed_under_another_environment(true)]
#[case::unreadable_binding(false)]
fn pools_skip_environment_rows_filed_outside_their_binding_partition(#[case] misfiled: bool) {
    let conn = test_conn();
    upsert_pool_candidate(
        &conn,
        CH_HASH,
        "2026-10-01T00:00:00Z",
        "ch",
        &cloud_hypervisor_manifest(),
    );
    if misfiled {
        // The store refuses this filing; the runtime connection enforces no
        // keys.
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        conn.execute(
            "UPDATE template_builds SET cache_partition = ?1 WHERE content_hash = ?2",
            rusqlite::params![twin().key(), CH_HASH],
        )
        .unwrap();
    } else {
        conn.execute(
            "UPDATE template_builds SET execution_metadata_json = '{' WHERE content_hash = ?1",
            [CH_HASH],
        )
        .unwrap();
    }
    assert!(
        crate::pool_manager::pool_base_build(&conn, NAME)
            .unwrap()
            .is_none()
    );
}

/// Startup recovery adopts a legacy-layout build bound to a non-initial
/// environment in place, so a legacy row bound to a pool-capable environment
/// fills its template's pool.
#[test]
fn pool_refills_take_an_adopted_legacy_build() {
    let conn = test_conn();
    let catalog =
        pplx_space_execution_catalog::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap();
    let mut manifest = TemplateManifest {
        runtime: "fc".to_string(),
        arch: "x86_64".to_string(),
        ..Default::default()
    };
    manifest.set_execution_metadata(
        catalog
            .iter()
            .find(|entry| entry.name() == "fc-1.17.0-linux-6.1.155")
            .unwrap()
            .definition()
            .execution_requirements()
            .unwrap(),
    );
    assert_ne!(manifest.environment().unwrap(), Some(initial()));
    upsert_build(
        &conn,
        &BuildUpsert {
            name: NAME,
            content_hash: HASH,
            cache_partition: TemplateCachePartition::Legacy,
            build_date: "2026-10-01T00:00:00Z",
            manifest_json: &serde_json::to_string(&manifest).unwrap(),
            runtime: "fc",
            arch: "x86_64",
            artifact_size_bytes: 1,
            state: "ready",
        },
    )
    .unwrap();

    let refill = crate::pool_manager::pool_refill_build(&conn, NAME)
        .unwrap()
        .unwrap();

    assert_eq!(
        (refill.content_hash.as_str(), refill.cache_partition),
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
    // A legacy-layout build bound to another environment, as startup
    // recovery adopts it in place, is the one exception.
    let adopted = upsert(
        &conn,
        Some(other()),
        TemplateCachePartition::Legacy,
        "2026-10-02T00:00:00Z",
    );
    assert!(adopted.is_ok(), "{adopted:?}");
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
    let service = sqlx::migrate!("../../../../../db/template-manager");
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
    let common = sqlx::migrate!("../../../../../db/common");
    for migration in common
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        conn.execute_batch(migration.sql.as_ref()).unwrap();
    }
    apply(conn, 20260930000000, true);
}

const PARTITIONS: i64 = 20261026220935;

/// Releases that don't bundle the partition migration reverse it at startup
/// only when the recorded checksum is this one, so rolling back to them stays
/// image-only. See db/README.md.
#[test]
fn partition_migration_keeps_the_checksum_releases_reverse() {
    let service = sqlx::migrate!("../../../../../db/template-manager");
    let up = service
        .iter()
        .find(|migration| {
            migration.version == PARTITIONS && migration.migration_type.is_up_migration()
        })
        .unwrap();
    assert_eq!(
        hex::encode(up.checksum.as_ref()),
        "bb39d295eee07bfc5ff54e6d2f076efb42a50b43d7cedffd8907e9cbf73e7e9b2837295cb1131306798e1c3a4bf4b820"
    );
}

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
