use super::*;

#[test]
fn source_identity_is_required_before_touching_retained_subvolumes() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    std::fs::create_dir(root.join(DIRECTORY)).unwrap();
    let expected = Uuid::now_v7();
    for (version, identity) in [(2, expected), (1, Uuid::now_v7())] {
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
    let temp = tempfile::tempdir_in(root).unwrap();
    let root = temp.path().canonicalize().unwrap();
    let directory = trusted_paths::open_directory(&root).unwrap();
    btrfs::create(&directory, "template".as_ref()).unwrap();
    let template = artifact_fs::directory(&directory, "template".as_ref()).unwrap();
    std::fs::write(root.join("template/rootfs.ext4"), b"original template").unwrap();
    btrfs::stamp(&template, Uuid::now_v7()).unwrap();
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
    let id = capture(&snapshot, &root.join("template"), &root.join("instance")).unwrap();
    artifact_fs::remove_at(&directory, "instance".as_ref()).unwrap();
    artifact_fs::remove_at(&directory, "template".as_ref()).unwrap();
    drop((template, instance));
    // Capture writes a version 1 manifest; inspection reads the retained
    // image length without the producer or template.
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(snapshot.join(DIRECTORY).join(MANIFEST)).unwrap())
            .unwrap();
    assert_eq!(manifest["version"], 1);
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
}
