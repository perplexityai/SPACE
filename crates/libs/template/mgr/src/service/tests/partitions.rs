//! An E0 build and its E1 twin of one rootfs on one node: lookups, deletes and
//! base-snapshot capture each act on their own build.

use super::*;
use crate::db::TemplateCachePartition;
use pplx_space_common_core::types::{
    Arch, EnvironmentReference, ExecutionMetadata, TemplateManifest,
};

const NAME: &str = "seed";

fn hash() -> String {
    format!("sha256:{}", "6".repeat(64))
}

fn initial() -> EnvironmentReference {
    pplx_space_node_runtime_core::catalog::initial_environment_reference().unwrap()
}

fn twin_environment() -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([2; 32]))
}

fn twin() -> TemplateCachePartition {
    TemplateCachePartition::Environment(twin_environment())
}

fn insert_bound_build(conn: &rusqlite::Connection, environment: EnvironmentReference) {
    let mut manifest = TemplateManifest {
        runtime: "fc".to_string(),
        arch: "x86_64".to_string(),
        ..Default::default()
    };
    manifest.set_execution_metadata(
        ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]).with_environment(environment),
    );
    let hash = hash();
    let cache_partition =
        pplx_space_node_runtime_core::catalog::template_cache_partition(Some(environment));
    crate::db::upsert_build(
        conn,
        &crate::db::BuildUpsert {
            name: NAME,
            content_hash: &hash,
            cache_partition,
            build_date: "2026-10-01T00:00:00Z",
            manifest_json: &serde_json::to_string(&manifest).unwrap(),
            runtime: RUNTIME_FIRECRACKER_ALIAS,
            arch: "x86_64",
            artifact_size_bytes: 1,
            state: "ready",
        },
    )
    .unwrap();
    crate::db::upsert_snapshot(
        conn,
        crate::db::SnapshotKey {
            name: NAME,
            content_hash: &hash,
            cache_partition,
            vcpu: 2,
            memory_mib: 1024,
        },
        &cache_partition.to_string(),
    )
    .unwrap();
}

fn twin_fixture() -> (tempfile::TempDir, TemplateManagerService) {
    let root = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    insert_bound_build(&conn, initial());
    insert_bound_build(&conn, twin_environment());
    let service = TemplateManagerService::new_with_template_root(conn, root.path()).unwrap();
    (root, service)
}

fn lookup(environment: Option<EnvironmentReference>) -> Request<crate::proto::GetTemplateRequest> {
    internal_request(crate::proto::GetTemplateRequest {
        template: Some(crate::proto::TemplateRef {
            name: NAME.into(),
            content_hash: Some(hash()),
            tag: None,
            environment: environment.map(|environment| environment.to_string()),
        }),
        caller_org_id: None,
        vcpu: Some(2),
        memory_mib: Some(1024),
    })
}

#[rstest::rstest]
#[case::unselected(None, TemplateCachePartition::Legacy, initial())]
#[case::initial(Some(initial()), TemplateCachePartition::Legacy, initial())]
#[case::twin(Some(twin_environment()), twin(), twin_environment())]
#[tokio::test]
async fn get_template_resolves_each_environment_build_of_one_rootfs(
    #[case] selected: Option<EnvironmentReference>,
    #[case] partition: TemplateCachePartition,
    #[case] bound: EnvironmentReference,
) {
    let (root, service) = twin_fixture();
    let response = service
        .get_template_resolving(
            lookup(selected),
            async |_, _, _, _| panic!("cached exact digests resolve offline"),
            async |_, _, _| panic!("cached builds are not pulled"),
        )
        .await
        .unwrap()
        .into_inner();

    let build_dir = partition.build_dir_name(&hash());
    assert_eq!(
        response.template_dir,
        root.path().join(NAME).join(&build_dir).to_string_lossy()
    );
    assert_eq!(
        response.snapshot_dir.as_deref(),
        Some(
            root.path()
                .join(NAME)
                .join("snapshots")
                .join(&build_dir)
                .join("2vcpu-1024mib")
                .to_string_lossy()
                .as_ref()
        )
    );
    assert_eq!(response.snapshot_hash, Some(partition.to_string()));
    assert_eq!(
        response.template.unwrap().environment,
        Some(bound.to_string())
    );
}

/// A lookup's LRU touches land on the build it serves, so a twin's traffic
/// doesn't keep its legacy sibling from eviction.
#[tokio::test]
async fn a_lookup_touches_only_the_build_it_serves() {
    let (_root, service) = twin_fixture();
    let last_used = |partition: TemplateCachePartition| -> String {
        service
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT last_used_at FROM template_builds WHERE cache_partition = ?1",
                [partition.key()],
                |row| row.get(0),
            )
            .unwrap()
    };
    service
        .db
        .lock()
        .unwrap()
        .execute("UPDATE template_builds SET last_used_at = '0'", [])
        .unwrap();
    let mut request = lookup(Some(twin_environment()));
    // Without snapshot dimensions, the lookup also reads the template's
    // base-snapshot policy.
    request.get_mut().vcpu = None;
    request.get_mut().memory_mib = None;
    service
        .get_template_resolving(
            request,
            async |_, _, _, _| panic!("cached exact digests resolve offline"),
            async |_, _, _| panic!("cached builds are not pulled"),
        )
        .await
        .unwrap();

    assert_eq!(last_used(TemplateCachePartition::Legacy), "0");
    assert_ne!(last_used(twin()), "0");
}

#[tokio::test]
async fn an_environment_without_its_own_build_falls_back_to_the_legacy_build() {
    let (root, service) = twin_fixture();
    let other = EnvironmentReference::new(Digest::sha256([3; 32]));
    let response = service
        .get_template_resolving(
            lookup(Some(other)),
            async |_, _, _, _| panic!("cached exact digests resolve offline"),
            async |_, _, _| panic!("a cached build of the digest is not pulled"),
        )
        .await
        .unwrap()
        .into_inner();
    // The caller checks it as a compatible cold-boot source.
    assert_eq!(
        response.template_dir,
        root.path().join(NAME).join(hash()).to_string_lossy()
    );
    assert_eq!(
        response.template.unwrap().environment,
        Some(initial().to_string())
    );
}

#[tokio::test]
async fn an_uncached_digest_pulls_without_a_partition_selector() {
    use crate::volume::{PullOutcome, ResolvedRelease};
    let root = tempfile::tempdir().unwrap();
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    let mut request = lookup(Some(twin_environment()));
    request.get_mut().caller_org_id = Some(OrgId::new().to_string());
    let materialized = std::cell::Cell::new(false);
    let response = service
        .get_template_resolving(
            request,
            async |_, _, _, _| {
                Ok(ResolvedRelease {
                    hash: hash(),
                    storage_uris: vec![],
                    visibility: pplx_space_node_api_client::proto::TemplateVisibility::Generic,
                })
            },
            async |_, _, _| {
                // The release's manifest decides the partition; here it's a
                // twin bound to the selected environment.
                insert_bound_build(&service.db.lock().unwrap(), twin_environment());
                materialized.set(true);
                PullOutcome::Materialized
            },
        )
        .await
        .unwrap()
        .into_inner();
    assert!(materialized.get());
    assert_eq!(
        response.template_dir,
        root.path()
            .join(NAME)
            .join(twin().build_dir_name(&hash()))
            .to_string_lossy()
    );
}

#[test]
fn requested_environments_select_their_partition() {
    use super::lookup::requested_cache_partition;
    assert_eq!(requested_cache_partition(None).unwrap(), None);
    assert_eq!(
        requested_cache_partition(Some(&initial().to_string())).unwrap(),
        Some(TemplateCachePartition::Legacy)
    );
    assert_eq!(
        requested_cache_partition(Some(&twin_environment().to_string())).unwrap(),
        Some(twin())
    );
    assert_eq!(
        requested_cache_partition(Some("not-a-digest"))
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[tokio::test]
async fn deleting_a_digest_removes_every_cached_partition() {
    let (root, service) = twin_fixture();
    for partition in [TemplateCachePartition::Legacy, twin()] {
        let build_dir = root
            .path()
            .join(NAME)
            .join(partition.build_dir_name(&hash()));
        std::fs::create_dir_all(&build_dir).unwrap();
        std::fs::write(build_dir.join(TEMPLATE_ROOTFS_FILE), b"rootfs").unwrap();
        std::fs::create_dir_all(
            root.path()
                .join(NAME)
                .join("snapshots")
                .join(partition.build_dir_name(&hash()))
                .join("2vcpu-1024mib"),
        )
        .unwrap();
    }

    service
        .delete_template_build_after_dependency_checks(NAME, &hash())
        .await
        .unwrap();

    for partition in [TemplateCachePartition::Legacy, twin()] {
        let build_dir_name = partition.build_dir_name(&hash());
        assert!(!root.path().join(NAME).join(&build_dir_name).exists());
        assert!(
            !root
                .path()
                .join(NAME)
                .join("snapshots")
                .join(&build_dir_name)
                .exists()
        );
    }
    assert!(
        crate::db::list_builds_for_hash(&service.db.lock().unwrap(), NAME, &hash())
            .unwrap()
            .is_empty()
    );
}

/// A complete twin dir an interrupted pull left without a row goes too, so a
/// later pull can't recover the deleted build from it.
#[tokio::test]
async fn deleting_a_digest_removes_a_partition_without_a_row() {
    let (root, service) = twin_fixture();
    crate::db::delete_build(&service.db.lock().unwrap(), NAME, &hash(), twin()).unwrap();
    let twin_dir = root.path().join(NAME).join(twin().build_dir_name(&hash()));
    std::fs::create_dir_all(&twin_dir).unwrap();
    std::fs::write(twin_dir.join(TEMPLATE_ROOTFS_FILE), b"rootfs").unwrap();

    service
        .delete_template_build_after_dependency_checks(NAME, &hash())
        .await
        .unwrap();

    assert!(!twin_dir.exists());
}

/// A twin's base-snapshot dir left without its build dir or row goes too, so
/// startup recovery doesn't re-register its snapshot.
#[tokio::test]
async fn deleting_a_digest_removes_a_partition_s_leftover_snapshots() {
    let (root, service) = twin_fixture();
    crate::db::delete_build(&service.db.lock().unwrap(), NAME, &hash(), twin()).unwrap();
    let snapshot_dir = root
        .path()
        .join(NAME)
        .join("snapshots")
        .join(twin().build_dir_name(&hash()));
    std::fs::create_dir_all(snapshot_dir.join("2vcpu-1024mib")).unwrap();

    service
        .delete_template_build_after_dependency_checks(NAME, &hash())
        .await
        .unwrap();

    assert!(!snapshot_dir.exists());
}

/// A name whose only build is cached for another environment is already in
/// the digest layout.
#[tokio::test]
async fn registering_a_flat_layout_finds_partitioned_builds() {
    let (root, service) = twin_fixture();
    let twin_dir = root.path().join(NAME).join(twin().build_dir_name(&hash()));
    std::fs::create_dir_all(&twin_dir).unwrap();
    std::fs::write(twin_dir.join(TEMPLATE_ROOTFS_FILE), b"rootfs").unwrap();

    let error = service
        .register_template(internal_request(crate::proto::RegisterTemplateRequest {
            template: Some(crate::proto::TemplateRef {
                name: NAME.into(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::AlreadyExists, "{error}");
}

#[tokio::test]
async fn delete_template_build_rejects_an_environment_selector() {
    let (_root, service) = twin_fixture();
    let error = service
        .delete_template_build(internal_request(crate::proto::DeleteTemplateBuildRequest {
            template: Some(crate::proto::TemplateRef {
                name: NAME.into(),
                content_hash: Some(hash()),
                tag: None,
                environment: Some(twin_environment().to_string()),
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        crate::db::list_builds_for_hash(&service.db.lock().unwrap(), NAME, &hash())
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn base_snapshot_capture_boots_a_twin_under_its_environment() {
    let legacy = snapshots::base_snapshot_create_request(
        NAME,
        &hash(),
        TemplateCachePartition::Legacy,
        Some(4),
        pplx_space_common_core::types::RuntimeKind::Firecracker,
    );
    assert_eq!(legacy.environment, None);
    assert_eq!(legacy.template, NAME);
    assert_eq!(legacy.runtime, RUNTIME_FIRECRACKER_ALIAS);

    let twin = snapshots::base_snapshot_create_request(
        NAME,
        &hash(),
        twin(),
        Some(4),
        pplx_space_common_core::types::RuntimeKind::Firecracker,
    );
    let input = twin
        .environment
        .expect("twin capture selects its environment");
    assert_eq!(input.reference, twin_environment().to_string());
    assert_eq!(input.template, NAME);
    // Environment creates carry the template inside the input only.
    assert!(twin.template.is_empty() && twin.runtime.is_empty());
    assert_eq!(twin.content_hash, Some(hash()));
    assert_eq!(
        twin.boot_mode,
        pplx_space_sbx_mgr_client::proto::BootMode::Cold as i32
    );
    assert_eq!(twin.rootfs_size_mb, Some(4));
}

#[test]
fn only_a_legacy_snapshot_rebuild_invalidates_pools() {
    assert!(snapshots::snapshot_invalidates_pools(
        TemplateCachePartition::Legacy
    ));
    assert!(!snapshots::snapshot_invalidates_pools(twin()));
}

#[test]
fn partition_reconciles_report_the_legacy_hash_and_label_twin_errors() {
    use snapshots::PartitionReconcile;
    let mut combined = PartitionReconcile::default();
    combined.merge(
        TemplateCachePartition::Legacy,
        PartitionReconcile::recorded(SnapshotReconcileOutcome {
            reconciled: true,
            content_hash: Some("legacy".into()),
            ..Default::default()
        }),
    );
    combined.merge(
        twin(),
        PartitionReconcile::failed(Some("twin".into()), "capture refused".into()),
    );
    assert!(combined.outcome.reconciled);
    assert_eq!(combined.outcome.content_hash.as_deref(), Some("legacy"));
    assert_eq!(
        combined.outcome.error,
        Some(format!("{}: capture refused", twin()))
    );
    assert_eq!(
        combined.recorded_result(),
        Some(combined.outcome.error.as_deref())
    );

    // A deferred retry records nothing, and its error never reaches what the
    // other partitions record, whichever order they merge in.
    let deferred = || {
        PartitionReconcile::unrecorded(SnapshotReconcileOutcome {
            error: Some("deferred".into()),
            ..Default::default()
        })
    };
    let recorded = || {
        PartitionReconcile::recorded(SnapshotReconcileOutcome {
            error: Some("legacy failed".into()),
            ..Default::default()
        })
    };
    let mut alone = PartitionReconcile::default();
    alone.merge(twin(), deferred());
    assert_eq!(alone.recorded_result(), None);
    let mut after = PartitionReconcile::default();
    after.merge(TemplateCachePartition::Legacy, recorded());
    after.merge(twin(), deferred());
    let mut before = PartitionReconcile::default();
    before.merge(twin(), deferred());
    before.merge(TemplateCachePartition::Legacy, recorded());
    for merged in [after, before] {
        assert_eq!(merged.recorded_result(), Some(Some("legacy failed")));
        assert!(merged.outcome.error.unwrap().contains("deferred"));
    }
}

fn import_header(
    manifest_environment: Option<Option<EnvironmentReference>>,
    selected: Option<EnvironmentReference>,
) -> crate::proto::ImportTemplateHeader {
    let manifest_json = manifest_environment
        .map(|environment| {
            let execution = ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]);
            let mut manifest = TemplateManifest::default();
            manifest.set_execution_metadata(match environment {
                Some(environment) => execution.with_environment(environment),
                None => execution,
            });
            serde_json::to_vec(&manifest).unwrap()
        })
        .unwrap_or_default();
    crate::proto::ImportTemplateHeader {
        template: Some(crate::proto::TemplateRef {
            name: NAME.into(),
            content_hash: Some(hash()),
            tag: None,
            environment: selected.map(|environment| environment.to_string()),
        }),
        manifest_json,
        ..Default::default()
    }
}

#[test]
fn imports_target_the_partition_their_manifest_binds() {
    let hash = hash();
    for (manifest, selected, partition) in [
        (Some(Some(twin_environment())), None, twin()),
        (
            Some(Some(twin_environment())),
            Some(twin_environment()),
            twin(),
        ),
        (
            Some(Some(initial())),
            Some(initial()),
            TemplateCachePartition::Legacy,
        ),
        (Some(None), None, TemplateCachePartition::Legacy),
        // No manifest: the existing validators decide.
        (None, None, TemplateCachePartition::Legacy),
    ] {
        let header = import_header(manifest, selected);
        let target = ImportTarget::from_header(NAME, &hash, &header).unwrap();
        assert_eq!(target.partition, partition);
        assert_eq!(
            target.lock_key(),
            format!("{NAME}@{}", partition.build_dir_name(&hash))
        );
    }
    for (manifest, selected) in [
        (Some(Some(initial())), Some(twin_environment())),
        (Some(Some(twin_environment())), Some(initial())),
        (Some(None), Some(twin_environment())),
    ] {
        let header = import_header(manifest, selected);
        assert_eq!(
            ImportTarget::from_header(NAME, &hash, &header)
                .err()
                .map(|error| error.code()),
            Some(tonic::Code::InvalidArgument)
        );
    }

    // A manifest without execution metadata takes the header metadata's
    // binding, which registration copies into it.
    let mut metadata = pplx_space_common_core::types::TemplateMetadata::default();
    metadata.set_execution_metadata(
        ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![])
            .with_environment(twin_environment()),
    );
    let header = crate::proto::ImportTemplateHeader {
        manifest_json: serde_json::to_vec(&TemplateManifest::default()).unwrap(),
        metadata_json: serde_json::to_vec(&metadata).unwrap(),
        ..import_header(None, None)
    };
    assert_eq!(
        ImportTarget::from_header(NAME, &hash, &header)
            .unwrap()
            .partition,
        twin()
    );
}

/// A row the migration left under the legacy partition for a build bound to
/// another environment isn't the legacy partition's to capture before
/// startup recovery drops it.
#[tokio::test]
async fn base_snapshot_reconcile_skips_a_build_filed_outside_its_partition() {
    let (_root, service) = twin_fixture();
    {
        let conn = service.db.lock().unwrap();
        crate::db::delete_build(&conn, NAME, &hash(), TemplateCachePartition::Legacy).unwrap();
        conn.execute("DELETE FROM template_snapshots WHERE name = ?1", [NAME])
            .unwrap();
        conn.execute(
            "UPDATE template_builds SET cache_partition = '' WHERE name = ?1",
            [NAME],
        )
        .unwrap();
    }
    let service = service.without_sbx_connect_retries();

    let reconciled = service
        .reconcile_partition_base_snapshot(NAME, TemplateCachePartition::Legacy, 2, 1024)
        .await;

    let error = reconciled.outcome.error.unwrap();
    assert!(error.contains("no builds found"), "{error}");
    assert!(!reconciled.outcome.snapshot_built);
}

#[tokio::test]
async fn base_snapshot_reconcile_resolves_the_newest_build_of_its_own_partition() {
    let (_root, service) = twin_fixture();
    // A newer legacy build of a different rootfs: an environment-blind
    // resolution would pick it for the twin's partition.
    let newer = format!("sha256:{}", "9".repeat(64));
    crate::db::upsert_build(
        &service.db.lock().unwrap(),
        &crate::db::BuildUpsert {
            name: NAME,
            content_hash: &newer,
            cache_partition: TemplateCachePartition::Legacy,
            build_date: "2026-10-20T00:00:00Z",
            manifest_json: "{}",
            runtime: RUNTIME_FIRECRACKER_ALIAS,
            arch: "x86_64",
            artifact_size_bytes: 1,
            state: "ready",
        },
    )
    .unwrap();
    let service = service.without_sbx_connect_retries();
    for (partition, expected) in [(twin(), hash()), (TemplateCachePartition::Legacy, newer)] {
        let reconciled = service
            .reconcile_partition_base_snapshot(NAME, partition, 2, 1024)
            .await;
        // Snapshot files live under the host template root, so this
        // partition's capture starts and fails on the missing rootfs.
        assert_eq!(
            reconciled.outcome.content_hash.as_deref(),
            Some(expected.as_str()),
            "{partition}"
        );
    }
}
