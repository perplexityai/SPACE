use super::*;

#[test]
fn source_identity_is_required_before_touching_retained_subvolumes() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    std::fs::create_dir(root.join(DIRECTORY)).unwrap();
    let expected = Uuid::now_v7();
    for (version, identity) in [(3, expected), (1, Uuid::now_v7()), (2, Uuid::now_v7())] {
        let manifest = Manifest {
            version,
            source_id: identity,
            source_uuid: Uuid::now_v7(),
            parent_uuid: Uuid::now_v7(),
            received_uuid: Uuid::now_v7(),
        };
        std::fs::write(
            root.join(DIRECTORY).join(MANIFEST),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        let artifact = trusted_paths::open_directory(&root).unwrap();
        assert!(matches!(
            SendSource::open(&artifact, expected),
            Err(SourceError::IdentityChanged)
        ));
        assert_eq!(
            inspect(&root, expected).unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
    }
}

#[test]
fn unstamped_parents_require_an_explicit_opt_in() {
    for value in ["true", "TRUE", " 1 "] {
        assert_eq!(ParentSeal::from_setting(Some(value)), ParentSeal::Unstamped);
    }
    for value in [
        None,
        Some(""),
        Some("false"),
        Some("0"),
        Some("yes"),
        Some("unstamped"),
    ] {
        assert_eq!(ParentSeal::from_setting(value), ParentSeal::Stamped);
    }
}

#[test]
fn inspection_reports_absence_only_for_a_missing_artifact_or_bundle() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let expected = Uuid::now_v7();
    let code = |path: &Path| inspect(path, expected).unwrap_err().code();
    assert_eq!(code(&root.join("missing")), tonic::Code::NotFound);
    assert_eq!(code(&root), tonic::Code::NotFound);
    // The caller learned this identity only after capture synced the
    // manifest, so a bundle without one lost it: unusable, not absent.
    std::fs::create_dir(root.join(DIRECTORY)).unwrap();
    assert_eq!(code(&root), tonic::Code::FailedPrecondition);
    std::fs::write(root.join(DIRECTORY).join(MANIFEST), b"{").unwrap();
    assert_eq!(code(&root), tonic::Code::FailedPrecondition);
    std::fs::write(root.join(DIRECTORY).join(MANIFEST), vec![b' '; 4097]).unwrap();
    assert_eq!(code(&root), tonic::Code::FailedPrecondition);
    // A matching manifest without its subvolumes is a broken bundle.
    let manifest = Manifest {
        version: 1,
        source_id: expected,
        source_uuid: Uuid::now_v7(),
        parent_uuid: Uuid::now_v7(),
        received_uuid: Uuid::now_v7(),
    };
    std::fs::write(
        root.join(DIRECTORY).join(MANIFEST),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    assert_eq!(code(&root), tonic::Code::FailedPrecondition);
}

#[test]
fn inspection_status_keeps_io_failures_out_of_artifact_evidence() {
    let io = SourceError::Io(io::Error::from_raw_os_error(libc::EACCES));
    assert_eq!(io.inspection_status().code(), tonic::Code::Internal);
    assert_eq!(
        SourceError::io(
            io::ErrorKind::NotFound.into(),
            SourceError::RootfsNotRegular
        )
        .inspection_status()
        .code(),
        tonic::Code::FailedPrecondition
    );
    // Staging, reading, and materialization keep their contract.
    assert_eq!(Status::from(io).code(), tonic::Code::FailedPrecondition);
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires root and an explicitly supplied disposable btrfs fixture directory"]
fn retained_source_regenerates_after_producer_deletion_and_receives_on_same_filesystem() {
    let Some(root) = std::env::var_os("SPACE_NODEMGR_TEST_BTRFS_DIR") else {
        eprintln!("not opted in: SPACE_NODEMGR_TEST_BTRFS_DIR is unset");
        return;
    };
    assert!(nix::unistd::geteuid().is_root());
    let receiver = tempfile::tempdir_in(&root).unwrap();
    let temp = tempfile::tempdir_in(root).unwrap();
    let root = temp.path().canonicalize().unwrap();
    let directory = trusted_paths::open_directory(&root).unwrap();
    btrfs::create(&directory, "template".as_ref()).unwrap();
    let template = artifact_fs::directory(&directory, "template".as_ref()).unwrap();
    std::fs::write(root.join("template/rootfs.ext4"), b"original template").unwrap();
    let received_uuid = Uuid::now_v7();
    btrfs::stamp(&template, received_uuid).unwrap();
    btrfs::set_readonly(&template, true).unwrap();
    btrfs::snapshot(&template, &directory, "instance".as_ref(), false).unwrap();
    let instance = artifact_fs::directory(&directory, "instance".as_ref()).unwrap();
    let expected = b"frozen snapshot data: longer than the original";
    std::fs::write(root.join("instance/rootfs.ext4"), expected).unwrap();
    let old_uid = 100_042;
    nix::unistd::chown(
        &root.join("instance/rootfs.ext4"),
        Some(nix::unistd::Uid::from_raw(old_uid)),
        Some(nix::unistd::Gid::from_raw(old_uid)),
    )
    .unwrap();
    std::fs::create_dir(root.join("snapshot")).unwrap();
    let snapshot = root.join("snapshot");
    let id = capture(
        &snapshot,
        &root.join("template"),
        &root.join("instance"),
        ParentSeal::Unstamped,
    )
    .unwrap();
    // The receiving node's own install of the same template build, outside
    // the cache tree that slot retirement rewrites below.
    let receiver = trusted_paths::open_directory(receiver.path()).unwrap();
    stamped_template_copy(&template, &receiver, "template", received_uuid);
    artifact_fs::remove_at(&directory, "instance".as_ref()).unwrap();
    artifact_fs::remove_at(&directory, "template".as_ref()).unwrap();
    drop((template, instance));
    // Capture writes a version 2 manifest and seals the private parent
    // without the template's received UUID; inspection reads the retained
    // image length without the producer or template.
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(snapshot.join(DIRECTORY).join(MANIFEST)).unwrap())
            .unwrap();
    assert_eq!(manifest["version"], UNSTAMPED_PARENT_VERSION);
    assert_eq!(manifest["received_uuid"], received_uuid.to_string());
    assert_eq!(received_uuid_holders(&root, received_uuid), 1);
    assert_eq!(inspect(&snapshot, id).unwrap(), expected.len() as u64);
    assert_eq!(
        inspect(&snapshot, Uuid::now_v7()).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    // Slot retirement must leave immutable send inputs intact, including
    // staging artifacts that the ordinary snapshots sweep visits separately.
    let staged = root.join("snapshots/.suspend-publish-test");
    std::fs::create_dir(root.join("snapshots")).unwrap();
    for artifact in [&snapshot, &staged] {
        if artifact == &staged {
            std::fs::rename(&snapshot, &staged).unwrap();
        }
        for scoped in [true, false] {
            let mutable = root.join("retired-slot-data");
            std::fs::write(&mutable, b"retire this owner").unwrap();
            nix::unistd::chown(
                &mutable,
                Some(nix::unistd::Uid::from_raw(old_uid)),
                Some(nix::unistd::Gid::from_raw(old_uid)),
            )
            .unwrap();
            assert!(if scoped {
                super::super::cleanup_helpers::neutralize_retained_cache_dir_owned_by_uid(
                    &root, old_uid,
                )
                .unwrap()
            } else {
                super::super::cleanup_helpers::neutralize_retained_cache_dir(&root).unwrap()
            });
            assert_eq!(
                crate::cache_ownership::first_path_owned_by_uid(&root, old_uid).unwrap(),
                None
            );
            let bundle = artifact.join(DIRECTORY);
            assert_eq!(std::fs::metadata(&bundle).unwrap().mode() & 0o7777, 0o750);
            assert_eq!(
                std::fs::metadata(bundle.join("source/rootfs.ext4"))
                    .unwrap()
                    .uid(),
                old_uid
            );
            assert_eq!(inspect(artifact, id).unwrap(), expected.len() as u64);
        }
    }
    std::fs::rename(&staged, &snapshot).unwrap();
    let mut lengths = Vec::new();
    // Reopen all persisted state for every attempt; no in-memory pin survives.
    for _ in 0..2 {
        lengths.push(materialize(&snapshot, id, None).unwrap());
        let received = root.join("received");
        std::fs::create_dir(&received).unwrap();
        let stream = File::open(snapshot.join("rootfs.btrfs-send")).unwrap();
        let output = std::process::Command::new("btrfs")
            .arg("receive")
            .arg(&received)
            .stdin(stream)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            std::fs::read(received.join("instance.ro/rootfs.ext4")).unwrap(),
            expected
        );
        artifact_fs::remove_at(&directory, "received".as_ref()).unwrap();
        std::fs::remove_file(snapshot.join("rootfs.btrfs-send")).unwrap();
    }
    assert!(lengths.iter().all(|bytes| *bytes > 16));
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    assert_eq!(
        materialize(&snapshot, id, Some(&cancel))
            .unwrap_err()
            .code(),
        tonic::Code::Cancelled
    );
    assert!(!snapshot.join("rootfs.btrfs-send").exists());
    // A replaced source subvolume is unusable, not absent.
    let bundle = trusted_paths::open_directory(&snapshot.join(DIRECTORY)).unwrap();
    artifact_fs::remove_at(&bundle, "source".as_ref()).unwrap();
    btrfs::create(&bundle, "source".as_ref()).unwrap();
    assert_eq!(
        inspect(&snapshot, id).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    artifact_fs::remove_at(&bundle, "source".as_ref()).unwrap();
    assert_eq!(
        inspect(&snapshot, id).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    drop(bundle);
    // Existing recursive artifact deletion reclaims nested RO subvolumes.
    artifact_fs::remove_at(&directory, "snapshot".as_ref()).unwrap();
    assert_eq!(
        inspect(&snapshot, id).unwrap_err().code(),
        tonic::Code::NotFound
    );
    artifact_fs::remove_at(&receiver, "template".as_ref()).unwrap();
}

#[cfg(target_os = "linux")]
fn stamped_template_copy(template: &File, directory: &File, name: &str, received_uuid: Uuid) {
    btrfs::snapshot(template, directory, name.as_ref(), false).unwrap();
    let copy = artifact_fs::directory(directory, name.as_ref()).unwrap();
    btrfs::stamp(&copy, received_uuid).unwrap();
    btrfs::set_readonly(&copy, true).unwrap();
}

/// Subvolumes on `root`'s filesystem listed under `received_uuid` in the UUID
/// tree item that `btrfs receive` searches for an incremental parent.
#[cfg(target_os = "linux")]
fn received_uuid_holders(root: &Path, received_uuid: Uuid) -> usize {
    let output = std::process::Command::new("btrfs")
        .args(["subvolume", "list", "-a", "-R"])
        .arg(root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let needle = format!("received_uuid {received_uuid} ");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| line.contains(&needle))
        .count()
}

#[cfg(target_os = "linux")]
fn receive_materialized(root: &Path, directory: &File, artifact: &Path, id: Uuid) -> Vec<u8> {
    materialize(artifact, id, None).unwrap();
    let received = root.join("received");
    std::fs::create_dir(&received).unwrap();
    let output = std::process::Command::new("btrfs")
        .arg("receive")
        .arg(&received)
        .stdin(File::open(artifact.join("rootfs.btrfs-send")).unwrap())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let bytes = std::fs::read(received.join("instance.ro/rootfs.ext4")).unwrap();
    artifact_fs::remove_at(directory, "received".as_ref()).unwrap();
    std::fs::remove_file(artifact.join("rootfs.btrfs-send")).unwrap();
    bytes
}

/// `btrfs receive` reads the template's received-UUID item through a fixed
/// 3992-byte search buffer, so it finds no parent once more than 495
/// subvolumes carry that UUID. Retained sources must not add holders.
#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires root and an explicitly supplied disposable btrfs fixture directory"]
fn retained_sources_keep_the_template_receivable_past_the_uuid_item_search_limit() {
    let Some(root) = std::env::var_os("SPACE_NODEMGR_TEST_BTRFS_DIR") else {
        eprintln!("not opted in: SPACE_NODEMGR_TEST_BTRFS_DIR is unset");
        return;
    };
    assert!(nix::unistd::geteuid().is_root());
    let temp = tempfile::tempdir_in(root).unwrap();
    let root = temp.path().canonicalize().unwrap();
    let directory = trusted_paths::open_directory(&root).unwrap();
    btrfs::create(&directory, "template".as_ref()).unwrap();
    let template = artifact_fs::directory(&directory, "template".as_ref()).unwrap();
    std::fs::write(root.join("template/rootfs.ext4"), b"original template").unwrap();
    let received_uuid = Uuid::now_v7();
    btrfs::stamp(&template, received_uuid).unwrap();
    btrfs::set_readonly(&template, true).unwrap();
    btrfs::snapshot(&template, &directory, "instance".as_ref(), false).unwrap();
    let expected = b"suspended rootfs";
    std::fs::write(root.join("instance/rootfs.ext4"), expected).unwrap();
    let mut captured = Vec::new();
    for index in 0..500 {
        let artifact = root.join(format!("snapshot-{index}"));
        std::fs::create_dir(&artifact).unwrap();
        let id = capture(
            &artifact,
            &root.join("template"),
            &root.join("instance"),
            ParentSeal::Unstamped,
        )
        .unwrap();
        captured.push((artifact, id));
    }
    assert_eq!(received_uuid_holders(&root, received_uuid), 1);
    // The same-filesystem receive resolves the template, the only holder; a
    // stamped private parent per capture would make it 501 and fail here.
    let (artifact, id) = &captured[0];
    assert_eq!(
        receive_materialized(&root, &directory, artifact, *id),
        expected
    );
    assert_eq!(received_uuid_holders(&root, received_uuid), 1);
}

/// The default seal stays readable by releases that predate it. Its bundles
/// keep working after the stamp is cleared in place, and restaging one under
/// the unstamped seal writes a copy without the received UUID.
#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires root and an explicitly supplied disposable btrfs fixture directory"]
fn stamped_parent_bundles_stay_sendable_and_restage_unstamped() {
    let Some(root) = std::env::var_os("SPACE_NODEMGR_TEST_BTRFS_DIR") else {
        eprintln!("not opted in: SPACE_NODEMGR_TEST_BTRFS_DIR is unset");
        return;
    };
    assert!(nix::unistd::geteuid().is_root());
    let temp = tempfile::tempdir_in(root).unwrap();
    let root = temp.path().canonicalize().unwrap();
    let directory = trusted_paths::open_directory(&root).unwrap();
    btrfs::create(&directory, "template".as_ref()).unwrap();
    let template = artifact_fs::directory(&directory, "template".as_ref()).unwrap();
    std::fs::write(root.join("template/rootfs.ext4"), b"original template").unwrap();
    let received_uuid = Uuid::now_v7();
    btrfs::stamp(&template, received_uuid).unwrap();
    btrfs::set_readonly(&template, true).unwrap();
    btrfs::snapshot(&template, &directory, "instance".as_ref(), false).unwrap();
    let expected = b"suspended rootfs";
    std::fs::write(root.join("instance/rootfs.ext4"), expected).unwrap();
    let legacy = root.join("legacy");
    std::fs::create_dir(&legacy).unwrap();
    let id = capture(
        &legacy,
        &root.join("template"),
        &root.join("instance"),
        ParentSeal::Stamped,
    )
    .unwrap();
    let manifest_path = legacy.join(DIRECTORY).join(MANIFEST);
    let manifest: Manifest =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    assert_eq!(manifest.version, STAMPED_PARENT_VERSION);
    assert_eq!(manifest.received_uuid, received_uuid);
    assert_eq!(received_uuid_holders(&root, received_uuid), 2);
    assert_eq!(inspect(&legacy, id).unwrap(), expected.len() as u64);
    assert_eq!(
        receive_materialized(&root, &directory, &legacy, id),
        expected
    );
    // A version 2 manifest over the stamped parent is a lineage mismatch.
    let write_version = |version| {
        std::fs::write(
            &manifest_path,
            serde_json::to_vec(&Manifest {
                version,
                ..manifest
            })
            .unwrap(),
        )
        .unwrap();
    };
    write_version(UNSTAMPED_PARENT_VERSION);
    let artifact = trusted_paths::open_directory(&legacy).unwrap();
    assert!(matches!(
        SendSource::open(&artifact, id),
        Err(SourceError::LineageMismatch)
    ));
    write_version(STAMPED_PARENT_VERSION);
    // Clearing the stamp in place keeps the version 1 bundle usable and
    // drops its holder.
    let bundle = trusted_paths::open_directory(&legacy.join(DIRECTORY)).unwrap();
    let parent = artifact_fs::directory(&bundle, "parent".as_ref()).unwrap();
    btrfs::set_readonly(&parent, false).unwrap();
    btrfs::set_readonly(&parent, true).unwrap();
    drop((parent, bundle));
    assert_eq!(received_uuid_holders(&root, received_uuid), 1);
    assert_eq!(inspect(&legacy, id).unwrap(), expected.len() as u64);
    assert_eq!(
        receive_materialized(&root, &directory, &legacy, id),
        expected
    );
    // Restaging, as a send lease does, under the unstamped seal.
    let source = SendSource::open(&artifact, id).unwrap();
    let restaged = root.join("restaged");
    std::fs::create_dir(&restaged).unwrap();
    let restaged_dir = trusted_paths::open_directory(&restaged).unwrap();
    let lease = Uuid::now_v7();
    capture_open(
        &restaged_dir,
        &source.parent,
        &source.source,
        lease,
        source.received_uuid,
        ParentSeal::Unstamped,
    )
    .unwrap();
    drop(source);
    let restaged_manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(restaged.join(DIRECTORY).join(MANIFEST)).unwrap())
            .unwrap();
    assert_eq!(restaged_manifest["version"], UNSTAMPED_PARENT_VERSION);
    assert_eq!(received_uuid_holders(&root, received_uuid), 1);
    assert_eq!(
        receive_materialized(&root, &directory, &restaged, lease),
        expected
    );
}
