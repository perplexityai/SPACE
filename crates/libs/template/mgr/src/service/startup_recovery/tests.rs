use super::*;
use pplx_space_common_core::types::{
    TemplateManifest, TemplateManifestCompression, TemplateManifestFile,
};
use sha2::{Digest as _, Sha256};

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn sha256_content_hash(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex(bytes))
}

struct DigestFixture {
    dir: tempfile::TempDir,
    rootfs_hash: String,
    rootfs_size: u64,
    zst_size: u64,
}

fn write_digest_fixture(rootfs: &[u8], zst: &[u8]) -> DigestFixture {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(TEMPLATE_ROOTFS_FILE), rootfs).unwrap();
    std::fs::write(dir.path().join(TEMPLATE_ROOTFS_ZSTD_FILE), zst).unwrap();
    DigestFixture {
        dir,
        rootfs_hash: sha256_content_hash(rootfs),
        rootfs_size: rootfs.len() as u64,
        zst_size: zst.len() as u64,
    }
}

fn canonical_manifest(fixture: &DigestFixture) -> TemplateManifest {
    let mut manifest = TemplateManifest {
        content_hash: fixture.rootfs_hash.clone(),
        bootable_artifact: TEMPLATE_ROOTFS_FILE.to_string(),
        ..Default::default()
    };
    manifest.files.insert(
        TEMPLATE_ROOTFS_FILE.to_string(),
        TemplateManifestFile {
            digest: fixture.rootfs_hash.clone(),
            size: Some(fixture.rootfs_size),
            compression: None,
        },
    );
    manifest.files.insert(
        TEMPLATE_ROOTFS_ZSTD_FILE.to_string(),
        TemplateManifestFile {
            digest: sha256_content_hash(b"zst-bytes-marker"),
            size: Some(fixture.zst_size),
            compression: Some(TemplateManifestCompression {
                codec: TEMPLATE_ROOTFS_ZSTD_CODEC.to_string(),
                level: TEMPLATE_ROOTFS_ZSTD_LEVEL,
            }),
        },
    );
    manifest
}

fn write_manifest_json(dir: &Path, manifest: &TemplateManifest) {
    let json = serde_json::to_string(manifest).unwrap();
    std::fs::write(dir.join("manifest.json"), json).unwrap();
}

#[test]
fn parse_manifest_rejects_bootable_artifact_with_path_traversal() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = TemplateManifest {
        bootable_artifact: "../escape".to_string(),
        ..Default::default()
    };
    write_manifest_json(dir.path(), &manifest);

    let err = parse_manifest_from_digest_dir(dir.path()).unwrap_err();
    assert!(
        matches!(err, DigestDirRejection::ManifestFilenamesUnsafe { .. }),
        "expected ManifestFilenamesUnsafe, got {err:?}"
    );
}

#[test]
fn parse_manifest_rejects_files_map_key_with_path_separator() {
    let dir = tempfile::tempdir().unwrap();
    let mut manifest = TemplateManifest {
        bootable_artifact: TEMPLATE_ROOTFS_FILE.to_string(),
        ..Default::default()
    };
    manifest
        .files
        .insert("subdir/escape".to_string(), TemplateManifestFile::default());
    write_manifest_json(dir.path(), &manifest);

    let err = parse_manifest_from_digest_dir(dir.path()).unwrap_err();
    assert!(
        matches!(err, DigestDirRejection::ManifestFilenamesUnsafe { .. }),
        "expected ManifestFilenamesUnsafe, got {err:?}"
    );
}

#[test]
fn parse_manifest_rejects_files_map_key_with_dot_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let mut manifest = TemplateManifest {
        bootable_artifact: TEMPLATE_ROOTFS_FILE.to_string(),
        ..Default::default()
    };
    manifest
        .files
        .insert("..".to_string(), TemplateManifestFile::default());
    write_manifest_json(dir.path(), &manifest);

    let err = parse_manifest_from_digest_dir(dir.path()).unwrap_err();
    assert!(
        matches!(err, DigestDirRejection::ManifestFilenamesUnsafe { .. }),
        "expected ManifestFilenamesUnsafe, got {err:?}"
    );
}

#[test]
fn manifest_self_consistency_accepts_canonical_manifest() {
    let fixture = write_digest_fixture(b"rootfs-bytes", b"zst-bytes");
    let manifest = canonical_manifest(&fixture);
    validate_manifest_self_consistency(
        &fixture.rootfs_hash,
        crate::db::TemplateCachePartition::Legacy,
        &manifest,
        &TemplateMetadata::default(),
    )
    .unwrap();
}

#[test]
fn manifest_self_consistency_rejects_content_hash_mismatch() {
    let fixture = write_digest_fixture(b"rootfs-bytes", b"zst-bytes");
    let mut manifest = canonical_manifest(&fixture);
    manifest.content_hash = sha256_content_hash(b"different");
    let err = validate_manifest_self_consistency(
        &fixture.rootfs_hash,
        crate::db::TemplateCachePartition::Legacy,
        &manifest,
        &TemplateMetadata::default(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        DigestDirRejection::ContentHashDisagreesWithDirName { .. }
    ));
}

#[test]
fn manifest_self_consistency_rejects_empty_bootable_artifact() {
    let fixture = write_digest_fixture(b"rootfs-bytes", b"zst-bytes");
    let mut manifest = canonical_manifest(&fixture);
    manifest.bootable_artifact.clear();
    let err = validate_manifest_self_consistency(
        &fixture.rootfs_hash,
        crate::db::TemplateCachePartition::Legacy,
        &manifest,
        &TemplateMetadata::default(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        DigestDirRejection::BootableArtifactNotDeclared
    ));
}

#[test]
fn manifest_self_consistency_rejects_missing_bootable_entry() {
    let fixture = write_digest_fixture(b"rootfs-bytes", b"zst-bytes");
    let mut manifest = canonical_manifest(&fixture);
    manifest.files.remove(TEMPLATE_ROOTFS_FILE);
    let err = validate_manifest_self_consistency(
        &fixture.rootfs_hash,
        crate::db::TemplateCachePartition::Legacy,
        &manifest,
        &TemplateMetadata::default(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        DigestDirRejection::BootableEntryAbsentFromFilesMap(_)
    ));
}

#[test]
fn manifest_self_consistency_rejects_bootable_digest_mismatch() {
    let fixture = write_digest_fixture(b"rootfs-bytes", b"zst-bytes");
    let mut manifest = canonical_manifest(&fixture);
    manifest.files.get_mut(TEMPLATE_ROOTFS_FILE).unwrap().digest =
        sha256_content_hash(b"different");
    let err = validate_manifest_self_consistency(
        &fixture.rootfs_hash,
        crate::db::TemplateCachePartition::Legacy,
        &manifest,
        &TemplateMetadata::default(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        DigestDirRejection::BootableEntryDigestDisagreesWithDirName { .. }
    ));
}

#[test]
fn local_file_verify_skips_compressed_entries_treated_as_s3_only() {
    let fixture = write_digest_fixture(b"rootfs-bytes", b"zst-bytes");
    let mut manifest = canonical_manifest(&fixture);
    // Lie about the compressed size; should still pass because the
    // compressed entry is treated as S3-only.
    manifest
        .files
        .get_mut(TEMPLATE_ROOTFS_ZSTD_FILE)
        .unwrap()
        .size = Some(fixture.zst_size + 999);
    verify_local_files_against_manifest(fixture.dir.path(), &manifest).unwrap();
}

#[test]
fn local_file_verify_accepts_manifest_listing_only_bootable_artifact() {
    let fixture = write_digest_fixture(b"rootfs-bytes", b"zst-bytes");
    let mut manifest = canonical_manifest(&fixture);
    manifest.files.remove(TEMPLATE_ROOTFS_ZSTD_FILE);
    verify_local_files_against_manifest(fixture.dir.path(), &manifest).unwrap();
}

#[test]
fn local_file_verify_rejects_bootable_size_drift() {
    let fixture = write_digest_fixture(b"rootfs-bytes", b"zst-bytes");
    let mut manifest = canonical_manifest(&fixture);
    manifest.files.get_mut(TEMPLATE_ROOTFS_FILE).unwrap().size = Some(fixture.rootfs_size + 1);
    let err = verify_local_files_against_manifest(fixture.dir.path(), &manifest).unwrap_err();
    assert!(matches!(
        err,
        DigestDirRejection::DeclaredFileSizeMismatch { .. }
    ));
}

#[test]
fn local_file_verify_rejects_missing_bootable_file() {
    let fixture = write_digest_fixture(b"rootfs-bytes", b"zst-bytes");
    std::fs::remove_file(fixture.dir.path().join(TEMPLATE_ROOTFS_FILE)).unwrap();
    let manifest = canonical_manifest(&fixture);
    let err = verify_local_files_against_manifest(fixture.dir.path(), &manifest).unwrap_err();
    assert!(matches!(
        err,
        DigestDirRejection::DeclaredFileMissing { .. }
    ));
}

#[test]
fn local_file_verify_rejects_uncompressed_sibling_missing_size() {
    let fixture = write_digest_fixture(b"rootfs-bytes", b"zst-bytes");
    let mut manifest = canonical_manifest(&fixture);
    manifest.files.get_mut(TEMPLATE_ROOTFS_FILE).unwrap().size = None;
    let err = verify_local_files_against_manifest(fixture.dir.path(), &manifest).unwrap_err();
    assert!(matches!(
        err,
        DigestDirRejection::FileSizeNotDeclared { .. }
    ));
}

#[test]
fn local_file_verify_honors_nonstandard_bootable_artifact_filename() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = b"alt-bootable-bytes";
    let filename = "alt-rootfs.img";
    std::fs::write(dir.path().join(filename), bytes).unwrap();
    let mut manifest = TemplateManifest {
        content_hash: sha256_content_hash(bytes),
        bootable_artifact: filename.to_string(),
        ..Default::default()
    };
    manifest.files.insert(
        filename.to_string(),
        TemplateManifestFile {
            digest: sha256_content_hash(bytes),
            size: Some(bytes.len() as u64),
            compression: None,
        },
    );
    verify_local_files_against_manifest(dir.path(), &manifest).unwrap();
}

#[tokio::test]
async fn bootable_artifact_hash_matches_dir_name_when_bytes_agree() {
    let fixture = write_digest_fixture(b"abc", b"zst-bytes");
    for hash in [
        fixture.rootfs_hash.as_str(),
        "blake3:6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85",
    ] {
        verify_bootable_artifact_hash(fixture.dir.path(), TEMPLATE_ROOTFS_FILE, hash)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn bootable_artifact_hash_rejects_when_bytes_disagree() {
    let fixture = write_digest_fixture(b"rootfs-bytes", b"zst-bytes");
    let err = verify_bootable_artifact_hash(
        fixture.dir.path(),
        TEMPLATE_ROOTFS_FILE,
        &sha256_content_hash(b"not-these-bytes"),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        err,
        DigestDirRejection::BootableArtifactHashMismatch { .. }
    ));
}

/// Recovery test for the GC-vs-sandbox-create race:
/// a stuck `state='deleting'` row whose artifacts are gone from
/// disk (Phase 2 finished, Phase 3 didn't) is fully deleted by
/// the startup sweep.
#[tokio::test]
async fn prune_drops_deleting_row_when_artifacts_missing() {
    let root = tempfile::tempdir().unwrap();
    let store = TemplateArtifactStore::new(root.path());
    let conn = crate::db::test_conn();

    let hash = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    crate::db::upsert_build(
        &conn,
        &crate::db::BuildUpsert {
            name: "seed",
            content_hash: hash,
            cache_partition: crate::db::TemplateCachePartition::Legacy,
            build_date: "2026-01-01T00:00:00Z",
            manifest_json: "{}",
            runtime: "fc",
            arch: "aarch64",
            artifact_size_bytes: 4096,
            state: "ready",
        },
    )
    .unwrap();
    conn.execute(
        "UPDATE template_builds SET state = 'deleting'
         WHERE name = ?1 AND content_hash = ?2",
        rusqlite::params!["seed", hash],
    )
    .unwrap();
    // Artifacts intentionally missing.

    let db = Arc::new(Mutex::new(conn));
    prune_catalog_rows_missing_on_disk(&db, &store).await;

    let count: i64 = db
        .lock()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM template_builds", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        count, 0,
        "stuck 'deleting' row with missing artifacts must be dropped"
    );
}

/// Recovery test (companion): a stuck `state='deleting'` row
/// whose artifacts are STILL on disk (Phase 2 hadn't started
/// before the crash) is restored to `state='ready'` so the
/// build remains usable. The alternative — dropping the row —
/// would leak a digest dir nothing in the DB references.
#[tokio::test]
async fn prune_restores_deleting_row_when_artifacts_intact() {
    let root = tempfile::tempdir().unwrap();
    let store = TemplateArtifactStore::new(root.path());
    let conn = crate::db::test_conn();

    let hash = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    crate::db::upsert_build(
        &conn,
        &crate::db::BuildUpsert {
            name: "seed",
            content_hash: hash,
            cache_partition: crate::db::TemplateCachePartition::Legacy,
            build_date: "2026-01-01T00:00:00Z",
            manifest_json: "{}",
            runtime: "fc",
            arch: "aarch64",
            artifact_size_bytes: 4096,
            state: "ready",
        },
    )
    .unwrap();

    // Plant digest dir + rootfs so the artifact_store sees the
    // build as present.
    let digest_dir = root.path().join("seed").join(hash);
    std::fs::create_dir_all(&digest_dir).unwrap();
    std::fs::write(digest_dir.join(TEMPLATE_ROOTFS_FILE), b"x").unwrap();

    conn.execute(
        "UPDATE template_builds SET state = 'deleting'
         WHERE name = ?1 AND content_hash = ?2",
        rusqlite::params!["seed", hash],
    )
    .unwrap();

    let db = Arc::new(Mutex::new(conn));
    prune_catalog_rows_missing_on_disk(&db, &store).await;

    let state: String = db
        .lock()
        .unwrap()
        .query_row(
            "SELECT state FROM template_builds WHERE name = ?1 AND content_hash = ?2",
            rusqlite::params!["seed", hash],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        state, "ready",
        "stuck 'deleting' row with intact artifacts must be restored to 'ready'"
    );
}

#[tokio::test]
async fn recovery_adopts_blake3_bytes_and_rejects_corruption_without_a_catalog_row() {
    let fixture = write_digest_fixture(b"abc", b"zst-bytes");
    let hash = "blake3:6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85";
    let mut manifest = canonical_manifest(&fixture);
    manifest.content_hash = hash.to_string();
    manifest.files.get_mut(TEMPLATE_ROOTFS_FILE).unwrap().digest = hash.to_string();
    write_manifest_json(fixture.dir.path(), &manifest);
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), fixture.dir.path())
            .unwrap();
    reconcile_digest_dir_against_db(
        &service,
        DigestDirBuild {
            name: "seed",
            hash_name: hash,
            partition: crate::db::TemplateCachePartition::Legacy,
            digest_dir: fixture.dir.path(),
        },
    )
    .await;
    assert!(
        crate::db::get_build(
            &service.db.lock().unwrap(),
            "seed",
            hash,
            crate::db::TemplateCachePartition::Legacy
        )
        .unwrap()
        .is_some()
    );
    crate::db::delete_build(
        &service.db.lock().unwrap(),
        "seed",
        hash,
        crate::db::TemplateCachePartition::Legacy,
    )
    .unwrap();
    std::fs::write(fixture.dir.path().join(TEMPLATE_ROOTFS_FILE), b"bad").unwrap();
    reconcile_digest_dir_against_db(
        &service,
        DigestDirBuild {
            name: "seed",
            hash_name: hash,
            partition: crate::db::TemplateCachePartition::Legacy,
            digest_dir: fixture.dir.path(),
        },
    )
    .await;
    assert!(
        crate::db::get_build(
            &service.db.lock().unwrap(),
            "seed",
            hash,
            crate::db::TemplateCachePartition::Legacy
        )
        .unwrap()
        .is_none()
    );
    assert!(!fixture.dir.path().exists());
}

fn twin_partition() -> crate::db::TemplateCachePartition {
    crate::db::TemplateCachePartition::Environment(
        pplx_space_common_core::types::EnvironmentReference::new(
            pplx_space_common_core::types::Digest::sha256([2; 32]),
        ),
    )
}

fn bind_manifest(manifest: &mut TemplateManifest, partition: crate::db::TemplateCachePartition) {
    let execution = pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
        pplx_space_common_core::types::Arch::X86_64,
        vec![],
    );
    manifest.set_execution_metadata(match partition.environment() {
        Some(environment) => execution.with_environment(environment),
        None => execution,
    });
}

#[test]
fn self_consistency_rejects_a_dir_bound_elsewhere() {
    let fixture = write_digest_fixture(b"rootfs", b"zst-bytes");
    let mut manifest = canonical_manifest(&fixture);
    bind_manifest(&mut manifest, crate::db::TemplateCachePartition::Legacy);
    let err = validate_manifest_self_consistency(
        &fixture.rootfs_hash,
        twin_partition(),
        &manifest,
        &TemplateMetadata::default(),
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            DigestDirRejection::PartitionDisagreesWithDirName { .. }
        ),
        "{err}"
    );
    let mut manifest = canonical_manifest(&fixture);
    bind_manifest(&mut manifest, twin_partition());
    validate_manifest_self_consistency(
        &fixture.rootfs_hash,
        twin_partition(),
        &manifest,
        &TemplateMetadata::default(),
    )
    .unwrap();
    // A legacy-layout dir bound to another environment is no exception.
    let err = validate_manifest_self_consistency(
        &fixture.rootfs_hash,
        crate::db::TemplateCachePartition::Legacy,
        &manifest,
        &TemplateMetadata::default(),
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            DigestDirRejection::PartitionDisagreesWithDirName { .. }
        ),
        "{err}"
    );

    // A manifest without execution metadata takes metadata.json's binding,
    // as registration does.
    let unbound = canonical_manifest(&fixture);
    assert!(unbound.execution.is_none());
    let mut metadata = TemplateMetadata::default();
    metadata.set_execution_metadata(
        pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
            pplx_space_common_core::types::Arch::X86_64,
            vec![],
        )
        .with_environment(twin_partition().environment().unwrap()),
    );
    validate_manifest_self_consistency(&fixture.rootfs_hash, twin_partition(), &unbound, &metadata)
        .unwrap();
    let err = validate_manifest_self_consistency(
        &fixture.rootfs_hash,
        crate::db::TemplateCachePartition::Legacy,
        &unbound,
        &metadata,
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            DigestDirRejection::PartitionDisagreesWithDirName { .. }
        ),
        "{err}"
    );

    // Unreadable execution metadata keeps a legacy dir's pre-partition
    // handling, but a partitioned dir can't prove its binding.
    manifest.schema_version = Some(u32::MAX);
    validate_manifest_self_consistency(
        &fixture.rootfs_hash,
        crate::db::TemplateCachePartition::Legacy,
        &manifest,
        &TemplateMetadata::default(),
    )
    .unwrap();
    let err = validate_manifest_self_consistency(
        &fixture.rootfs_hash,
        twin_partition(),
        &manifest,
        &TemplateMetadata::default(),
    )
    .unwrap_err();
    assert!(
        matches!(err, DigestDirRejection::ExecutionMetadataInvalid(_)),
        "{err}"
    );
}

/// The binding may sit only in metadata.json, as a pull interrupted before
/// normalizing its manifest leaves it.
#[rstest::rstest]
#[case::in_manifest(false)]
#[case::in_metadata(true)]
#[tokio::test]
async fn recovery_registers_a_twin_dir_in_its_own_partition(#[case] in_metadata: bool) {
    let fixture = write_digest_fixture(b"twin-rootfs", b"zst-bytes");
    let mut manifest = canonical_manifest(&fixture);
    if in_metadata {
        let mut bound = manifest.clone();
        bind_manifest(&mut bound, twin_partition());
        let mut metadata = TemplateMetadata::default();
        metadata.set_execution_metadata(bound.execution.unwrap());
        std::fs::write(
            fixture.dir.path().join("metadata.json"),
            serde_json::to_vec(&metadata).unwrap(),
        )
        .unwrap();
    } else {
        bind_manifest(&mut manifest, twin_partition());
    }
    write_manifest_json(fixture.dir.path(), &manifest);
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), fixture.dir.path())
            .unwrap();
    reconcile_digest_dir_against_db(
        &service,
        DigestDirBuild {
            name: "seed",
            hash_name: &fixture.rootfs_hash,
            partition: twin_partition(),
            digest_dir: fixture.dir.path(),
        },
    )
    .await;
    let conn = service.db.lock().unwrap();
    assert!(
        crate::db::get_build(&conn, "seed", &fixture.rootfs_hash, twin_partition())
            .unwrap()
            .is_some()
    );
    assert!(
        crate::db::get_build(
            &conn,
            "seed",
            &fixture.rootfs_hash,
            crate::db::TemplateCachePartition::Legacy
        )
        .unwrap()
        .is_none()
    );
}

/// A node that predates partitions can hold a build bound to another
/// environment in the legacy layout, under a legacy row. Recovery drops it
/// with its base snapshots, so a pull fetches it into its own partition.
#[tokio::test]
async fn recovery_drops_a_legacy_dir_bound_to_another_environment() {
    let root = tempfile::tempdir().unwrap();
    let fixture = write_digest_fixture(b"legacy-dir-rootfs", b"zst-bytes");
    let mut manifest = canonical_manifest(&fixture);
    bind_manifest(&mut manifest, twin_partition());
    write_manifest_json(fixture.dir.path(), &manifest);
    let conn = crate::db::test_conn();
    conn.execute(
        "INSERT INTO template_builds (name, content_hash, build_date, manifest_json, runtime, arch, created_at)
         VALUES ('seed', ?1, 'd', ?2, 'fc', 'x86_64', '0')",
        rusqlite::params![fixture.rootfs_hash, serde_json::to_string(&manifest).unwrap()],
    )
    .unwrap();
    let service = TemplateManagerService::new_with_template_root(conn, root.path()).unwrap();
    let snapshot = root
        .path()
        .join("seed/snapshots")
        .join(&fixture.rootfs_hash)
        .join("vcpu1_mem256");
    std::fs::create_dir_all(&snapshot).unwrap();
    std::fs::write(snapshot.join("snapshot.base"), b"memory").unwrap();

    reconcile_digest_dir_against_db(
        &service,
        DigestDirBuild {
            name: "seed",
            hash_name: &fixture.rootfs_hash,
            partition: crate::db::TemplateCachePartition::Legacy,
            digest_dir: fixture.dir.path(),
        },
    )
    .await;

    assert!(!fixture.dir.path().exists());
    assert!(!snapshot.exists());
    assert!(
        crate::db::get_build(
            &service.db.lock().unwrap(),
            "seed",
            &fixture.rootfs_hash,
            crate::db::TemplateCachePartition::Legacy,
        )
        .unwrap()
        .is_none()
    );
}

/// A partitioned dir only this code writes must hold its own environment's
/// build; one that doesn't is dropped.
#[tokio::test]
async fn recovery_drops_a_partitioned_dir_bound_elsewhere() {
    let root = tempfile::tempdir().unwrap();
    let fixture = write_digest_fixture(b"twin-dir-rootfs", b"zst-bytes");
    let mut manifest = canonical_manifest(&fixture);
    bind_manifest(&mut manifest, crate::db::TemplateCachePartition::Legacy);
    write_manifest_json(fixture.dir.path(), &manifest);
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();

    reconcile_digest_dir_against_db(
        &service,
        DigestDirBuild {
            name: "seed",
            hash_name: &fixture.rootfs_hash,
            partition: twin_partition(),
            digest_dir: fixture.dir.path(),
        },
    )
    .await;

    assert!(!fixture.dir.path().exists());
}
