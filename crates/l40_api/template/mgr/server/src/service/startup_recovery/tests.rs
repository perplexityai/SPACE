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
    validate_manifest_self_consistency(&fixture.rootfs_hash, &manifest).unwrap();
}

#[test]
fn manifest_self_consistency_rejects_content_hash_mismatch() {
    let fixture = write_digest_fixture(b"rootfs-bytes", b"zst-bytes");
    let mut manifest = canonical_manifest(&fixture);
    manifest.content_hash = sha256_content_hash(b"different");
    let err = validate_manifest_self_consistency(&fixture.rootfs_hash, &manifest).unwrap_err();
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
    let err = validate_manifest_self_consistency(&fixture.rootfs_hash, &manifest).unwrap_err();
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
    let err = validate_manifest_self_consistency(&fixture.rootfs_hash, &manifest).unwrap_err();
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
    let err = validate_manifest_self_consistency(&fixture.rootfs_hash, &manifest).unwrap_err();
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
        verify_bootable_artifact_hash(
            fixture.dir.path(),
            TEMPLATE_ROOTFS_FILE,
            hash,
            TemplateReadMode::Sparse,
        )
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
        TemplateReadMode::Sparse,
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
    reconcile_digest_dir_against_db(&service, "seed", hash, fixture.dir.path()).await;
    assert!(
        crate::db::get_build(&service.db.lock().unwrap(), "seed", hash)
            .unwrap()
            .is_some()
    );
    crate::db::delete_build(&service.db.lock().unwrap(), "seed", hash).unwrap();
    std::fs::write(fixture.dir.path().join(TEMPLATE_ROOTFS_FILE), b"bad").unwrap();
    reconcile_digest_dir_against_db(&service, "seed", hash, fixture.dir.path()).await;
    assert!(
        crate::db::get_build(&service.db.lock().unwrap(), "seed", hash)
            .unwrap()
            .is_none()
    );
    assert!(!fixture.dir.path().exists());
}

fn place_build_dir(root: &Path, template: &str, rootfs: &[u8]) -> (String, std::path::PathBuf) {
    let fixture = write_digest_fixture(rootfs, b"zst-bytes");
    write_manifest_json(fixture.dir.path(), &canonical_manifest(&fixture));
    let dir = root.join(template).join(&fixture.rootfs_hash);
    std::fs::create_dir_all(dir.parent().unwrap()).unwrap();
    std::fs::rename(fixture.dir.path(), &dir).unwrap();
    (fixture.rootfs_hash.clone(), dir)
}

/// A base snapshot of `{template}/{hash}` under `root`, as a finished
/// snapshot capture leaves it; returns its config dir.
fn place_base_snapshot(root: &Path, template: &str, hash: &str) -> std::path::PathBuf {
    let config_dir = root
        .join(template)
        .join("snapshots")
        .join(hash)
        .join("2vcpu-1024mib");
    std::fs::create_dir_all(&config_dir).unwrap();
    for file in ["snapshot.base", "vmstate.base"] {
        std::fs::write(config_dir.join(file), b"base").unwrap();
    }
    std::fs::write(config_dir.join("snapshot_hash"), "snap-hash").unwrap();
    config_dir
}

/// The GC tick's wait for one rowless-dir check, longer than any check
/// here takes.
const CHECK_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

fn row_exists(service: &TemplateManagerService, template: &str, hash: &str) -> bool {
    crate::db::get_build(&service.db.lock().unwrap(), template, hash)
        .unwrap()
        .is_some()
}

/// DeleteTemplateBuild takes the digest's pull claim first: while a pull
/// holds it, the delete refuses retryably and the dir, and the row the pull
/// then writes, survive. Once the pull lets go, the delete goes through.
#[tokio::test]
async fn delete_template_build_waits_for_a_pull_of_the_digest() {
    let root = tempfile::tempdir().unwrap();
    let (hash, dir) = place_build_dir(root.path(), "seed", b"claimed-rootfs");
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    let pull = service
        .volume_puller
        .try_claim_digest("seed", &hash)
        .unwrap();

    let error = service
        .delete_template_build(crate::service::internal_request(
            crate::proto::DeleteTemplateBuildRequest {
                template: Some(crate::proto::TemplateRef {
                    name: "seed".into(),
                    content_hash: Some(hash.clone()),
                    tag: None,
                    environment: None,
                }),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Aborted, "{error}");
    // The pull finishes and registers its build.
    crate::db::upsert_build(
        &service.db.lock().unwrap(),
        &crate::db::BuildUpsert {
            name: "seed",
            content_hash: &hash,
            build_date: "2026-05-13T00:00:00Z",
            manifest_json: "{}",
            runtime: RUNTIME_FIRECRACKER_ALIAS,
            arch: "x86_64",
            artifact_size_bytes: 0,
            state: "ready",
        },
    )
    .unwrap();
    assert!(dir.join(TEMPLATE_ROOTFS_FILE).exists());
    assert!(row_exists(&service, "seed", &hash));
    drop(pull);

    let claim = service.claim_digest_for_delete("seed", &hash).unwrap();
    service
        .delete_template_build_after_dependency_checks("seed", &hash)
        .await
        .unwrap();
    drop(claim);

    assert!(!dir.exists());
    assert!(!row_exists(&service, "seed", &hash));
}

/// Startup recovery runs once requests flow, so it doesn't verify a build
/// whose digest a pull, import, registration or delete holds alongside them.
/// It waits for the holder after its scan and then verifies the build in the
/// same pass, since a pull refuses a dir without a row while it holds the
/// digest and no later pass would check the dir.
#[tokio::test]
async fn startup_recovery_verifies_a_held_build_once_its_holder_lets_go() {
    let root = tempfile::tempdir().unwrap();
    let (hash, dir) = place_build_dir(root.path(), "seed", b"claimed-at-startup-rootfs");
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    *service.pool_handle.lock().unwrap() = Some(tokio::spawn(async {}));
    let held = service
        .volume_puller
        .try_claim_digest("seed", &hash)
        .unwrap();

    let recovery = tokio::spawn({
        let service = service.clone();
        async move { service.run_startup_recovery().await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert!(!recovery.is_finished());
    assert!(!row_exists(&service, "seed", &hash));
    assert!(dir.join(TEMPLATE_ROOTFS_FILE).exists());
    drop(held);
    tokio::time::timeout(std::time::Duration::from_secs(1), recovery)
        .await
        .expect("recovery finishes once the claim goes")
        .unwrap();
    assert!(row_exists(&service, "seed", &hash));
}

fn registered(service: &TemplateManagerService, fixture: &DigestFixture) -> bool {
    row_exists(service, "seed", &fixture.rootfs_hash)
}

fn insert_row(conn: &rusqlite::Connection, fixture: &DigestFixture, manifest: &TemplateManifest) {
    crate::db::upsert_build(
        conn,
        &crate::db::BuildUpsert {
            name: "seed",
            content_hash: &fixture.rootfs_hash,
            build_date: "2026-01-01T00:00:00Z",
            manifest_json: &serde_json::to_string(manifest).unwrap(),
            runtime: "fc",
            arch: "x86_64",
            artifact_size_bytes: fixture.rootfs_size,
            state: "ready",
        },
    )
    .unwrap();
}

async fn reconcile(service: &TemplateManagerService, fixture: &DigestFixture) {
    reconcile_digest_dir_against_db(service, "seed", &fixture.rootfs_hash, fixture.dir.path())
        .await;
}

/// A build whose metadata.json read fails with EIO, here a link to an
/// unmapped address of `/proc/self/mem`, keeps its dir: the failure is a
/// transient rejection, never the default metadata, which would register the
/// build without its arch and execution metadata. Its row goes, and once
/// metadata.json reads again the next pass registers it.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_unreadable_metadata_json_keeps_the_dir() {
    let fixture = write_digest_fixture(b"metadata-eio-rootfs", b"zst-bytes");
    write_manifest_json(fixture.dir.path(), &canonical_manifest(&fixture));
    let metadata_path = fixture.dir.path().join("metadata.json");
    let metadata_json = br#"{"arch":"aarch64"}"#;
    std::fs::write(&metadata_path, metadata_json).unwrap();
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), fixture.dir.path())
            .unwrap();
    reconcile(&service, &fixture).await;
    assert!(registered(&service, &fixture));
    // As after a pull that stopped before its row: only a build without a
    // row reads its metadata.
    crate::db::delete_build(&service.db.lock().unwrap(), "seed", &fixture.rootfs_hash).unwrap();

    std::fs::remove_file(&metadata_path).unwrap();
    std::os::unix::fs::symlink("/proc/self/mem", &metadata_path).unwrap();
    let error = std::fs::read_to_string(&metadata_path).unwrap_err();
    assert_eq!(
        error.raw_os_error(),
        Some(rustix::io::Errno::IO.raw_os_error()),
        "{error}"
    );

    reconcile(&service, &fixture).await;

    assert_eq!(
        std::fs::read(fixture.dir.path().join(TEMPLATE_ROOTFS_FILE)).unwrap(),
        b"metadata-eio-rootfs"
    );
    assert!(fixture.dir.path().join("manifest.json").exists());
    assert!(!registered(&service, &fixture));

    std::fs::remove_file(&metadata_path).unwrap();
    std::fs::write(&metadata_path, metadata_json).unwrap();
    reconcile(&service, &fixture).await;

    assert!(registered(&service, &fixture));
    let arch = crate::db::get_build(&service.db.lock().unwrap(), "seed", &fixture.rootfs_hash)
        .unwrap()
        .unwrap()
        .arch;
    assert_eq!(arch, "aarch64");
}

/// A metadata.json that reads but doesn't parse can't say what the build's
/// arch and execution metadata are, so it's a permanent rejection, as an
/// unparseable manifest is. An absent or empty one is a build without
/// metadata, as registration installs it.
#[rstest::rstest]
#[case::unparseable(Some(&b"{not json"[..]), false)]
#[case::empty(Some(&b""[..]), true)]
#[case::absent(None, true)]
fn only_unparseable_metadata_json_is_a_rejection(
    #[case] contents: Option<&[u8]>,
    #[case] accepted: bool,
) {
    let fixture = write_digest_fixture(b"metadata-parse-rootfs", b"zst-bytes");
    if let Some(contents) = contents {
        std::fs::write(fixture.dir.path().join("metadata.json"), contents).unwrap();
    }
    match parse_metadata_from_digest_dir(fixture.dir.path()) {
        Ok(metadata) => {
            assert!(accepted, "{contents:?} must be rejected");
            assert_eq!(metadata, TemplateMetadata::default());
        }
        Err(rejection) => {
            assert!(!accepted, "{contents:?} must be accepted: {rejection}");
            assert!(
                matches!(rejection, DigestDirRejection::MetadataUnparseable(_)),
                "{rejection}"
            );
            assert!(!rejection.is_transient(), "{rejection}");
        }
    }
}

#[test]
fn io_errors_that_can_clear_are_transient() {
    use std::io::{Error, ErrorKind};
    for kind in [
        ErrorKind::NotFound,
        ErrorKind::InvalidData,
        ErrorKind::InvalidInput,
        ErrorKind::InvalidFilename,
        ErrorKind::IsADirectory,
        ErrorKind::NotADirectory,
    ] {
        assert!(!io_error_is_transient(&Error::from(kind)), "{kind:?}");
    }
    let looped = Error::from_raw_os_error(rustix::io::Errno::LOOP.raw_os_error());
    assert!(!io_error_is_transient(&looped), "{looped}");
    for error in [
        Error::from_raw_os_error(rustix::io::Errno::IO.raw_os_error()),
        Error::from(ErrorKind::PermissionDenied),
        Error::from(ErrorKind::Interrupted),
        Error::from(ErrorKind::TimedOut),
    ] {
        assert!(io_error_is_transient(&error), "{error}");
    }
}

/// A manifest that fails to read with EIO, here a link to an unmapped
/// address of `/proc/self/mem`, never deletes the installed build: its row
/// goes, so it can't be selected, and its dir stays. Once the manifest reads
/// again, the next startup verifies the dir from scratch and registers it.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_transient_rejection_keeps_the_dir_and_the_next_startup_restores_it() {
    let fixture = write_digest_fixture(b"transient-rootfs", b"zst-bytes");
    let manifest = canonical_manifest(&fixture);
    let manifest_path = fixture.dir.path().join("manifest.json");
    std::os::unix::fs::symlink("/proc/self/mem", &manifest_path).unwrap();
    let error = std::fs::read_to_string(&manifest_path).unwrap_err();
    assert_eq!(
        error.raw_os_error(),
        Some(rustix::io::Errno::IO.raw_os_error()),
        "{error}"
    );
    let conn = crate::db::test_conn();
    insert_row(&conn, &fixture, &manifest);
    let service = TemplateManagerService::new_with_template_root(conn, fixture.dir.path()).unwrap();

    reconcile(&service, &fixture).await;

    assert!(fixture.dir.path().join(TEMPLATE_ROOTFS_FILE).exists());
    assert!(!registered(&service, &fixture));
    assert!(
        crate::db::get_latest_build(&service.db.lock().unwrap(), "seed")
            .unwrap()
            .is_none()
    );

    // The next startup, with the manifest readable again.
    std::fs::remove_file(&manifest_path).unwrap();
    write_manifest_json(fixture.dir.path(), &manifest);
    reconcile(&service, &fixture).await;

    assert!(registered(&service, &fixture));
}

/// A bootable artifact whose read fails with EIO while it's hashed is a
/// transient rejection. The ordinary reader reads from offset 0, which
/// `/proc/self/mem` fails with EIO; the sparse one sizes the file first.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn hashing_a_rootfs_that_fails_with_eio_is_transient() {
    let fixture = write_digest_fixture(b"unhashable-rootfs", b"zst-bytes");
    let rootfs = fixture.dir.path().join(TEMPLATE_ROOTFS_FILE);
    std::fs::remove_file(&rootfs).unwrap();
    std::os::unix::fs::symlink("/proc/self/mem", &rootfs).unwrap();

    let rejection = verify_bootable_artifact_hash(
        fixture.dir.path(),
        TEMPLATE_ROOTFS_FILE,
        &fixture.rootfs_hash,
        crate::file_reader::TemplateReadMode::Ordinary,
    )
    .await
    .unwrap_err();

    assert!(
        matches!(
            rejection,
            DigestDirRejection::BootableArtifactUnreadable { .. }
        ),
        "{rejection}"
    );
    assert!(rejection.is_transient(), "{rejection}");
}

/// A manifest that can never read, because it's missing or its path is a
/// directory, is a permanent rejection: the build and its row go, as before.
#[rstest::rstest]
#[case::missing(false)]
#[case::directory(true)]
#[tokio::test]
async fn a_permanent_rejection_removes_the_build(#[case] directory: bool) {
    let fixture = write_digest_fixture(b"no-manifest-rootfs", b"zst-bytes");
    if directory {
        std::fs::create_dir(fixture.dir.path().join("manifest.json")).unwrap();
    }
    let conn = crate::db::test_conn();
    insert_row(&conn, &fixture, &canonical_manifest(&fixture));
    let service = TemplateManagerService::new_with_template_root(conn, fixture.dir.path()).unwrap();

    reconcile(&service, &fixture).await;

    assert!(!fixture.dir.path().exists());
    assert!(!registered(&service, &fixture));
}

/// Startup recovery runs once requests flow, so an import that starts first
/// can own a staging dir by the time it scans. It leaves the dir alone; the
/// same dirs left by a crashed process go before the socket binds.
#[tokio::test]
async fn startup_recovery_leaves_a_live_import_s_staging_dirs() {
    let root = tempfile::tempdir().unwrap();
    let hash = sha256_content_hash(b"staging-rootfs");
    let name_dir = root.path().join("seed");
    let live = [
        name_dir.join(format!("{hash}.importing.{}", uuid::Uuid::now_v7())),
        name_dir.join(format!("{hash}.republishing.{}", uuid::Uuid::now_v7())),
    ];
    for dir in &live {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("manifest.json"), b"{}").unwrap();
    }
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    *service.pool_handle.lock().unwrap() = Some(tokio::spawn(async {}));

    service.run_startup_recovery().await;

    for dir in &live {
        assert!(dir.join("manifest.json").exists(), "{}", dir.display());
    }

    service.clean_transient_dirs_before_serving();

    for dir in &live {
        assert!(!dir.exists(), "{}", dir.display());
    }
}

/// A build with a row is never registered from its metadata, so a
/// metadata.json that no longer parses, as one a newer release wrote might
/// not, leaves the build and its row alone.
#[tokio::test]
async fn a_build_with_a_row_keeps_it_whatever_its_metadata_json_holds() {
    let fixture = write_digest_fixture(b"row-backed-rootfs", b"zst-bytes");
    let manifest = canonical_manifest(&fixture);
    write_manifest_json(fixture.dir.path(), &manifest);
    std::fs::write(fixture.dir.path().join("metadata.json"), b"{not json").unwrap();
    let conn = crate::db::test_conn();
    insert_row(&conn, &fixture, &manifest);
    let service = TemplateManagerService::new_with_template_root(conn, fixture.dir.path()).unwrap();

    reconcile(&service, &fixture).await;

    assert!(registered(&service, &fixture));
    assert!(fixture.dir.path().join(TEMPLATE_ROOTFS_FILE).exists());
}

/// A dir the stuck-eviction reaper left without a row is verified by the
/// next GC tick: an intact one gets its row back, and one whose contents the
/// eviction already wiped is removed, so neither blocks pulls of its build.
#[tokio::test]
async fn the_gc_tick_restores_or_removes_a_dir_the_reaper_left_rowless() {
    let root = tempfile::tempdir().unwrap();
    let (intact_hash, intact_dir) = place_build_dir(root.path(), "seed", b"reaped-intact-rootfs");
    let (wiped_hash, wiped_dir) = place_build_dir(root.path(), "seed", b"reaped-wiped-rootfs");
    for entry in std::fs::read_dir(&wiped_dir).unwrap() {
        std::fs::remove_file(entry.unwrap().path()).unwrap();
    }
    let conn = crate::db::test_conn();
    for hash in [&intact_hash, &wiped_hash] {
        crate::db::upsert_build(
            &conn,
            &crate::db::BuildUpsert {
                name: "seed",
                content_hash: hash,
                build_date: "2026-01-01T00:00:00Z",
                manifest_json: "{}",
                runtime: "fc",
                arch: "x86_64",
                artifact_size_bytes: 0,
                state: "deleting",
            },
        )
        .unwrap();
    }
    let service = TemplateManagerService::new_with_template_root(conn, root.path()).unwrap();
    let missing: crate::eviction::ReaperDiskProbeFn =
        std::sync::Arc::new(|_, _| Box::pin(async { crate::eviction::ReaperDiskState::Missing }));
    crate::eviction::reap_stuck_deleting(std::sync::Arc::clone(&service.db), &missing).await;
    assert!(!row_exists(&service, "seed", &intact_hash));
    assert!(intact_dir.exists() && wiped_dir.exists());

    assert_eq!(service.reconcile_rowless_build_dirs(4, CHECK_WAIT).await, 2);

    assert!(row_exists(&service, "seed", &intact_hash));
    assert!(intact_dir.join(TEMPLATE_ROOTFS_FILE).exists());
    assert!(!wiped_dir.exists());
    assert!(!row_exists(&service, "seed", &wiped_hash));
}

/// The tick verifies at most `limit` dirs, and dirs that have a row neither
/// get checked nor take its slots.
#[tokio::test]
async fn the_gc_tick_bounds_its_rowless_work() {
    let root = tempfile::tempdir().unwrap();
    let mut dirs = Vec::new();
    for rootfs in [b"bound-a".as_slice(), b"bound-b", b"bound-c"] {
        dirs.push(place_build_dir(root.path(), "seed", rootfs));
    }
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();

    assert_eq!(service.reconcile_rowless_build_dirs(2, CHECK_WAIT).await, 2);
    assert_eq!(service.reconcile_rowless_build_dirs(2, CHECK_WAIT).await, 1);
    assert_eq!(service.reconcile_rowless_build_dirs(2, CHECK_WAIT).await, 0);
    for (hash, _) in &dirs {
        assert!(row_exists(&service, "seed", hash));
    }

    // Dirs with a row don't take the tick's slots: with two of them first in
    // name order, a fresh tick still reaches the rowless dir after them.
    let root = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    for (template, rootfs) in [("a-row", b"slot-a".as_slice()), ("b-row", b"slot-b")] {
        let (hash, _) = place_build_dir(root.path(), template, rootfs);
        crate::db::upsert_build(
            &conn,
            &crate::db::BuildUpsert {
                name: template,
                content_hash: &hash,
                build_date: "2026-01-01T00:00:00Z",
                manifest_json: "{}",
                runtime: "fc",
                arch: "x86_64",
                artifact_size_bytes: 0,
                state: "ready",
            },
        )
        .unwrap();
    }
    let (late_hash, _) = place_build_dir(root.path(), "c-rowless", b"slot-c");
    let service = TemplateManagerService::new_with_template_root(conn, root.path()).unwrap();
    assert_eq!(service.reconcile_rowless_build_dirs(2, CHECK_WAIT).await, 1);
    assert!(row_exists(&service, "c-rowless", &late_hash));
}

/// A read on the tick that fails in a way that can clear leaves the dir for
/// a later tick, without a row.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_transient_error_on_the_gc_tick_leaves_the_dir() {
    let root = tempfile::tempdir().unwrap();
    let (hash, dir) = place_build_dir(root.path(), "seed", b"tick-transient-rootfs");
    let snapshot = place_base_snapshot(root.path(), "seed", &hash);
    let manifest_path = dir.join("manifest.json");
    std::fs::remove_file(&manifest_path).unwrap();
    std::os::unix::fs::symlink("/proc/self/mem", &manifest_path).unwrap();
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();

    service.reconcile_rowless_build_dirs(4, CHECK_WAIT).await;

    assert!(dir.join(TEMPLATE_ROOTFS_FILE).exists());
    assert!(snapshot.join("snapshot.base").exists());
    assert!(!row_exists(&service, "seed", &hash));
}

/// The tick takes rowless dirs from just after the last one it examined, so
/// dirs that keep failing don't hold the rest back: with the first two
/// failing on every tick, the next tick reaches and restores the third.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_gc_tick_rotates_past_dirs_that_keep_failing() {
    let root = tempfile::tempdir().unwrap();
    let mut failing = Vec::new();
    for template in ["a-fail", "b-fail"] {
        let (_, dir) = place_build_dir(root.path(), template, template.as_bytes());
        let manifest = dir.join("manifest.json");
        std::fs::remove_file(&manifest).unwrap();
        std::os::unix::fs::symlink("/proc/self/mem", &manifest).unwrap();
        failing.push(dir);
    }
    let (good_hash, _) = place_build_dir(root.path(), "c-good", b"rotated-to-rootfs");
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();

    assert_eq!(service.reconcile_rowless_build_dirs(2, CHECK_WAIT).await, 2);
    assert!(!row_exists(&service, "c-good", &good_hash));

    service.reconcile_rowless_build_dirs(2, CHECK_WAIT).await;

    assert!(row_exists(&service, "c-good", &good_hash));
    for dir in failing {
        assert!(dir.join(TEMPLATE_ROOTFS_FILE).exists());
    }
}

/// While someone else holds a build's claim, the tick leaves its dir alone
/// and writes no row; once the claim goes, the next tick restores it.
#[tokio::test]
async fn the_gc_tick_skips_a_build_someone_else_claims() {
    let root = tempfile::tempdir().unwrap();
    let (hash, dir) = place_build_dir(root.path(), "seed", b"claimed-by-a-pull-rootfs");
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    let pull = service
        .volume_puller
        .try_claim_digest("seed", &hash)
        .unwrap();

    assert_eq!(service.reconcile_rowless_build_dirs(4, CHECK_WAIT).await, 0);
    assert!(!row_exists(&service, "seed", &hash));
    assert!(dir.join(TEMPLATE_ROOTFS_FILE).exists());

    drop(pull);
    assert_eq!(service.reconcile_rowless_build_dirs(4, CHECK_WAIT).await, 1);
    assert!(row_exists(&service, "seed", &hash));
}

/// A delete that empties a dir and then fails holds the claim while it
/// works, so the tick can't register the emptied dir as a ready build; after
/// the delete lets go, the tick removes the empty dir instead.
#[tokio::test]
async fn a_delete_failing_midway_never_leaves_a_ready_row_over_an_empty_dir() {
    let root = tempfile::tempdir().unwrap();
    let (hash, dir) = place_build_dir(root.path(), "seed", b"half-deleted-rootfs");
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    let delete = service.claim_digest_for_delete("seed", &hash).unwrap();
    // The delete removed the contents, then failed before the dir and rows.
    for entry in std::fs::read_dir(&dir).unwrap() {
        std::fs::remove_file(entry.unwrap().path()).unwrap();
    }

    assert_eq!(service.reconcile_rowless_build_dirs(4, CHECK_WAIT).await, 0);
    assert!(!row_exists(&service, "seed", &hash));
    // The half-deleted dir is the delete's to finish while it holds the
    // claim; the tick doesn't touch it.
    assert!(dir.exists());
    drop(delete);
    service.reconcile_rowless_build_dirs(4, CHECK_WAIT).await;

    assert!(!row_exists(&service, "seed", &hash));
    assert!(!dir.exists());
}

/// A build the GC tick restores is advertised: the tick enqueues the
/// template's inventory for the cluster, as every catalog mutation does.
#[tokio::test]
async fn the_gc_tick_enqueues_the_inventory_of_a_build_it_restores() {
    let root = tempfile::tempdir().unwrap();
    let (hash, _dir) = place_build_dir(root.path(), "seed", b"advertised-rootfs");
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    let dirty = || {
        crate::db::list_node_dirty_refs(&service.db.lock().unwrap(), 100)
            .unwrap()
            .into_iter()
            .map(|row| row.element_value)
            .collect::<Vec<_>>()
    };
    assert!(!dirty().contains(&"seed".to_string()));

    assert_eq!(service.reconcile_rowless_build_dirs(4, CHECK_WAIT).await, 1);

    assert!(row_exists(&service, "seed", &hash));
    assert!(dirty().contains(&"seed".to_string()), "{:?}", dirty());
}

/// DeleteTemplateBuild accepts a digest whose only trace is a build dir
/// without a row, checks sandboxes by the digest, and removes the dir; a
/// digest with neither is still not found.
#[tokio::test]
async fn delete_template_build_clears_a_rowless_only_digest() {
    let root = tempfile::tempdir().unwrap();
    let (hash, dir) = place_build_dir(root.path(), "seed", b"rowless-delete-rootfs");
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    let missing = format!("sha256:{}", "e".repeat(64));
    {
        let conn = &service.db;
        assert!(
            crate::service::lookup::cached_builds_to_delete(conn, root.path(), "seed", &hash)
                .is_ok_and(|builds| builds.is_empty())
        );
        assert_eq!(
            crate::service::lookup::cached_builds_to_delete(conn, root.path(), "seed", &missing)
                .err()
                .map(|status| status.code()),
            Some(tonic::Code::NotFound)
        );
    }
    let sandboxes = [pplx_space_sbx_mgr_client::proto::SandboxListElement {
        sandbox_id: "suspended-child".to_string(),
        template: Some("seed".to_string()),
        image_hash: Some(hash.clone()),
        ..Default::default()
    }];
    assert_eq!(
        crate::service::dependent_sandbox_ids_for_template_delete_target(
            &sandboxes,
            crate::service::TemplateBuildDeleteTarget::Digest {
                name: "seed",
                content_hash: &hash,
            },
            crate::service::DependentSandboxScope::All,
        ),
        [(
            "seed".to_string(),
            hash.clone(),
            vec!["suspended-child".to_string()]
        )]
    );

    service
        .delete_template_build_after_dependency_checks("seed", &hash)
        .await
        .unwrap();

    assert!(!dir.exists());

    // A base-snapshot dir a removed build left behind counts too.
    let snapshot = root.path().join("seed/snapshots").join(&missing);
    std::fs::create_dir_all(snapshot.join("1vcpu-256mib")).unwrap();
    assert!(
        crate::service::lookup::cached_builds_to_delete(
            &service.db,
            root.path(),
            "seed",
            &missing,
        )
        .is_ok_and(|builds| builds.is_empty())
    );
    service
        .delete_template_build_after_dependency_checks("seed", &missing)
        .await
        .unwrap();
    assert!(!snapshot.exists());
}

/// A delete of a digest with no row, whose dirs' presence can't be read,
/// fails retryably instead of reporting the digest as not found while a dir
/// may still be cached.
#[tokio::test]
async fn deleting_a_digest_whose_dirs_cannot_be_listed_fails_retryably() {
    let root = tempfile::tempdir().unwrap();
    let hash = format!("sha256:{}", "d".repeat(64));
    // A file where the template's snapshots dir belongs can't be listed.
    std::fs::create_dir_all(root.path().join("seed")).unwrap();
    std::fs::write(root.path().join("seed/snapshots"), b"not a dir").unwrap();
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();

    let status =
        crate::service::lookup::cached_builds_to_delete(&service.db, root.path(), "seed", &hash)
            .err()
            .unwrap();
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status}");

    let status = service
        .delete_template_build(crate::service::internal_request(
            crate::proto::DeleteTemplateBuildRequest {
                template: Some(crate::proto::TemplateRef {
                    name: "seed".into(),
                    content_hash: Some(hash.clone()),
                    tag: None,
                    environment: None,
                }),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status}");
}

/// Dropping a build's row drops its base-snapshot rows, which can't be
/// registered again without it. A build the GC tick restores gets the base
/// snapshots on disk back with its row, so they're served again.
#[tokio::test]
async fn the_gc_tick_restores_the_base_snapshots_of_a_build_it_restores() {
    let root = tempfile::tempdir().unwrap();
    let (hash, _dir) = place_build_dir(root.path(), "seed", b"snapshotted-rootfs");
    place_base_snapshot(root.path(), "seed", &hash);
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    // Without the build's row, a snapshot row is refused.
    assert!(
        crate::db::upsert_snapshot(
            &service.db.lock().unwrap(),
            "seed",
            &hash,
            2,
            1024,
            "snap-hash"
        )
        .is_err()
    );

    assert_eq!(service.reconcile_rowless_build_dirs(4, CHECK_WAIT).await, 1);

    assert!(row_exists(&service, "seed", &hash));
    let snapshots = crate::db::list_snapshots(&service.db.lock().unwrap(), "seed", &hash).unwrap();
    assert_eq!(
        snapshots
            .iter()
            .map(|row| (row.vcpu, row.memory_mib, row.snapshot_hash.as_str()))
            .collect::<Vec<_>>(),
        [(2, 1024, "snap-hash")]
    );
}

/// The tick scans for dirs without a row before it claims each one, so a
/// registration or import can finish a build in between. Once it holds the
/// claim, a build that has a row by then is left alone: a read of it that
/// fails in a way that can clear doesn't drop the row it just got.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_gc_tick_leaves_a_build_that_gained_a_row_after_its_scan() {
    let root = tempfile::tempdir().unwrap();
    let (hash, dir) = place_build_dir(root.path(), "seed", b"registered-meanwhile-rootfs");
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    // The registration lands its row after the scan picked the dir.
    crate::db::upsert_build(
        &service.db.lock().unwrap(),
        &crate::db::BuildUpsert {
            name: "seed",
            content_hash: &hash,
            build_date: "2026-01-01T00:00:00Z",
            manifest_json: "{}",
            runtime: "fc",
            arch: "x86_64",
            artifact_size_bytes: 0,
            state: "ready",
        },
    )
    .unwrap();
    let manifest = dir.join("manifest.json");
    std::fs::remove_file(&manifest).unwrap();
    std::os::unix::fs::symlink("/proc/self/mem", &manifest).unwrap();

    assert!(!service.verify_rowless_build_dir("seed", &hash, &dir).await);

    assert!(row_exists(&service, "seed", &hash));
    assert!(dir.join(TEMPLATE_ROOTFS_FILE).exists());
}

/// A FIFO at `path` with no writer, so a reader parks opening it. Releasing
/// it, or dropping it, opens it read-write, which never blocks and wakes a
/// parked reader, then swaps the path for an empty regular file before
/// closing it, so a reader that arrives later doesn't park forever. Nothing it does can
/// block, so a failing test can't hang on it.
#[cfg(target_os = "linux")]
struct ParkedFifo {
    path: std::path::PathBuf,
    released: bool,
}

#[cfg(target_os = "linux")]
impl ParkedFifo {
    fn new(path: std::path::PathBuf) -> Self {
        let _ = std::fs::remove_file(&path);
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&path)
                .status()
                .unwrap()
                .success()
        );
        Self {
            path,
            released: false,
        }
    }

    fn release(&mut self) {
        if std::mem::replace(&mut self.released, true) {
            return;
        }
        let fifo = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path);
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::write(&self.path, b"");
        drop(fifo);
    }
}

#[cfg(target_os = "linux")]
impl Drop for ParkedFifo {
    fn drop(&mut self) {
        self.release();
    }
}

/// A tick cancelled mid-check, as shutdown cancels it, doesn't let go of the
/// digest's claim while the check still runs: the check finishes in its own
/// task, and only then lets go.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_gc_tick_keeps_the_claim_until_its_check_finishes() {
    let root = tempfile::tempdir().unwrap();
    let (hash, dir) = place_build_dir(root.path(), "seed", b"");
    // The check's hash parks opening the rootfs until it's released.
    let mut rootfs = ParkedFifo::new(dir.join(TEMPLATE_ROOTFS_FILE));
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    let tick = tokio::spawn({
        let service = service.clone();
        async move { service.reconcile_rowless_build_dirs(4, CHECK_WAIT).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !service.volume_puller.is_claimed("seed", &hash) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the check claims the digest");
    // Cancelled while the check holds the claim: before the hash or parked
    // in it, the claim must outlive the tick.
    tick.abort();
    assert!(tick.await.unwrap_err().is_cancelled());

    assert!(service.volume_puller.is_claimed("seed", &hash));

    rootfs.release();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while service.volume_puller.is_claimed("seed", &hash) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the check lets go once it finishes");
}

/// A dir a delete removes between the scan and the claim is skipped: no
/// verification, no inventory publish.
#[tokio::test]
async fn the_gc_tick_skips_a_dir_removed_after_its_scan() {
    let root = tempfile::tempdir().unwrap();
    let hash = format!("sha256:{}", "c".repeat(64));
    let dir = root.path().join("seed").join(&hash);
    std::fs::create_dir_all(root.path().join("seed")).unwrap();
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();

    assert!(!service.verify_rowless_build_dir("seed", &hash, &dir).await);

    assert!(
        crate::db::list_node_dirty_refs(&service.db.lock().unwrap(), 100)
            .unwrap()
            .is_empty()
    );
}

/// A check that hangs, here on a FIFO in place of the rootfs, doesn't stall
/// the GC loop: the tick stops waiting for it and returns, and the check
/// keeps its claim until it finishes.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_gc_tick_moves_on_from_a_check_that_hangs() {
    let root = tempfile::tempdir().unwrap();
    let (hash, dir) = place_build_dir(root.path(), "seed", b"");
    let mut rootfs = ParkedFifo::new(dir.join(TEMPLATE_ROOTFS_FILE));
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();

    let checked = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        service.reconcile_rowless_build_dirs(4, std::time::Duration::from_millis(200)),
    )
    .await
    .expect("the tick stops waiting for a check that hangs");

    assert_eq!(checked, 1);
    assert!(service.volume_puller.is_claimed("seed", &hash));
    rootfs.release();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while service.volume_puller.is_claimed("seed", &hash) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the check lets go once it finishes");
}

/// The check's blocking reads run off the runtime's workers: with the only
/// worker of a current-thread runtime, a manifest read parked on a FIFO
/// still lets the tick's wait expire on time.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn a_hung_read_on_the_gc_tick_doesnt_block_the_runtime() {
    let root = tempfile::tempdir().unwrap();
    let (_hash, dir) = place_build_dir(root.path(), "seed", b"hung-manifest-rootfs");
    let mut manifest = ParkedFifo::new(dir.join("manifest.json"));
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    // Released as soon as the tick returns, or after a second from another
    // thread if a read on the runtime's worker keeps the tick from returning.
    let (returned, wait_for_return) = std::sync::mpsc::channel::<()>();
    let started = std::time::Instant::now();
    let releaser = std::thread::spawn(move || {
        let _ = wait_for_return.recv_timeout(std::time::Duration::from_secs(1));
        manifest.release();
    });

    service
        .reconcile_rowless_build_dirs(4, std::time::Duration::from_millis(200))
        .await;
    let elapsed = started.elapsed();
    let _ = returned.send(());

    releaser.join().unwrap();
    assert!(
        elapsed < std::time::Duration::from_millis(800),
        "{elapsed:?}"
    );
}

/// A permanently invalid build the GC tick removes takes its base snapshots
/// with it, so the next build installed under the hash doesn't get them
/// registered as its own.
#[tokio::test]
async fn the_gc_tick_removes_the_base_snapshots_of_a_build_it_rejects() {
    let root = tempfile::tempdir().unwrap();
    let (hash, dir) = place_build_dir(root.path(), "seed", b"rejected-with-snapshots-rootfs");
    let snapshot = place_base_snapshot(root.path(), "seed", &hash);
    // A rootfs whose bytes no longer match the hash is a permanent rejection.
    std::fs::write(
        dir.join(TEMPLATE_ROOTFS_FILE),
        b"rejected-with-snapshots-rootfx",
    )
    .unwrap();
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();

    assert_eq!(service.reconcile_rowless_build_dirs(4, CHECK_WAIT).await, 1);

    assert!(!dir.exists());
    assert!(!snapshot.exists());
    assert!(!row_exists(&service, "seed", &hash));
}

/// A permanently invalid build whose base snapshots can't be removed still
/// loses its row, so it isn't served; its dirs stay, now without a row, for
/// the GC tick to retry.
#[tokio::test]
async fn an_invalid_build_whose_snapshots_cant_be_removed_loses_its_row() {
    let root = tempfile::tempdir().unwrap();
    let (hash, dir) = place_build_dir(root.path(), "seed", b"snapshots-stuck-rootfs");
    let conn = crate::db::test_conn();
    crate::db::upsert_build(
        &conn,
        &crate::db::BuildUpsert {
            name: "seed",
            content_hash: &hash,
            build_date: "2026-01-01T00:00:00Z",
            manifest_json: "{}",
            runtime: "fc",
            arch: "x86_64",
            artifact_size_bytes: 0,
            state: "ready",
        },
    )
    .unwrap();
    // A rootfs of another size is a permanent rejection.
    std::fs::write(dir.join(TEMPLATE_ROOTFS_FILE), b"grown").unwrap();
    // A file where the snapshots dir belongs can't be removed as one.
    let snapshots = root.path().join("seed/snapshots").join(&hash);
    std::fs::create_dir_all(snapshots.parent().unwrap()).unwrap();
    std::fs::write(&snapshots, b"not a dir").unwrap();
    let service = TemplateManagerService::new_with_template_root(conn, root.path()).unwrap();

    reconcile_digest_dir_against_db(&service, "seed", &hash, &dir).await;

    assert!(!row_exists(&service, "seed", &hash));
    assert!(dir.join(TEMPLATE_ROOTFS_FILE).exists());
    assert!(snapshots.exists());
}

/// Checks that outlive their tick keep their permits, so on a hung disk the
/// next tick starts none until they finish, instead of parking more.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checks_left_running_by_a_tick_hold_off_the_next_ticks() {
    let root = tempfile::tempdir().unwrap();
    let mut parked = Vec::new();
    for template in ["a-hung", "b-hung"] {
        let (hash, dir) = place_build_dir(root.path(), template, b"");
        parked.push((
            template,
            hash,
            ParkedFifo::new(dir.join(TEMPLATE_ROOTFS_FILE)),
        ));
    }
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    let wait = std::time::Duration::from_millis(100);
    assert_eq!(service.reconcile_rowless_build_dirs(4, wait).await, 2);

    let (good_hash, _) = place_build_dir(root.path(), "c-good", b"held-off-rootfs");
    assert_eq!(service.reconcile_rowless_build_dirs(4, wait).await, 0);
    assert!(!row_exists(&service, "c-good", &good_hash));

    for (template, hash, mut fifo) in parked {
        fifo.release();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while service.volume_puller.is_claimed(template, &hash) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("a parked check finishes once released");
    }
    service.reconcile_rowless_build_dirs(4, CHECK_WAIT).await;
    assert!(row_exists(&service, "c-good", &good_hash));
}

/// A dir the tick picks but can't start, because earlier checks hold the
/// permits, comes first on the next tick rather than being passed over.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dir_skipped_for_want_of_a_permit_comes_first_next_tick() {
    let root = tempfile::tempdir().unwrap();
    let (hung_hash, hung_dir) = place_build_dir(root.path(), "a-hung", b"");
    let mut hung = ParkedFifo::new(hung_dir.join(TEMPLATE_ROOTFS_FILE));
    let (skipped_hash, _) = place_build_dir(root.path(), "b-skipped", b"permit-b-rootfs");
    place_build_dir(root.path(), "c-next", b"permit-c-rootfs");
    place_build_dir(root.path(), "d-last", b"permit-d-rootfs");
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    // An earlier check that's still running holds one of the two permits.
    let _running = Arc::clone(&service.rowless_checks)
        .try_acquire_owned()
        .unwrap();

    // The hung check outlives the tick with the other permit, so the tick
    // can't start the next dir it picked.
    service
        .reconcile_rowless_build_dirs(2, std::time::Duration::from_millis(100))
        .await;
    assert!(!row_exists(&service, "b-skipped", &skipped_hash));
    hung.release();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while service.volume_puller.is_claimed("a-hung", &hung_hash) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the hung check finishes once released");

    service.reconcile_rowless_build_dirs(2, CHECK_WAIT).await;
    assert!(row_exists(&service, "b-skipped", &skipped_hash));
}
