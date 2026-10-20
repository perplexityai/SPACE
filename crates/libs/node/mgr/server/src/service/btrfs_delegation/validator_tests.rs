use super::*;

#[test]
fn instance_artifact_cleanup_requires_complete_removal() {
    for failed_targets in [
        &["instance"][..],
        &["instance.ro"][..],
        &["snapshot/instance.ro"][..],
        &["instance", "instance.ro", "snapshot/instance.ro"][..],
    ] {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("snapshot")).unwrap();
        let metadata = root.path().join("snapshot/snapshot.json");
        std::fs::write(&metadata, b"preserve until acknowledgement").unwrap();
        for target in ["instance", "instance.ro", "snapshot/instance.ro"] {
            let path = root.path().join(target);
            std::fs::create_dir(path).unwrap();
        }
        let failing = std::cell::Cell::new(true);
        let mut delete = |path: &Path| {
            if failing.get()
                && failed_targets
                    .iter()
                    .any(|target| path == root.path().join(target))
            {
                return Err(Status::unavailable("device busy"));
            }
            match std::fs::remove_dir_all(path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(Status::internal(error.to_string())),
            }
        };
        let error =
            delete_sandbox_instance_artifacts_with("fixture", root.path().to_owned(), &mut delete)
                .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(metadata.exists());
        for target in ["instance", "instance.ro", "snapshot/instance.ro"] {
            let path = root.path().join(target);
            assert_eq!(
                path.exists(),
                failed_targets.contains(&target),
                "independent cleanup obligation: {target}"
            );
        }
        for target in failed_targets {
            assert!(
                error
                    .message()
                    .contains(root.path().join(target).to_str().unwrap())
            );
        }
        failing.set(false);
        // Retry finishes the remaining work; already-absent targets succeed.
        for _ in 0..2 {
            delete_sandbox_instance_artifacts_with("fixture", root.path().to_owned(), &mut delete)
                .unwrap();
        }
        for target in ["instance", "instance.ro", "snapshot/instance.ro"] {
            assert!(!root.path().join(target).exists());
        }
        assert!(metadata.exists());
    }
}

#[test]
fn template_name_accepts_canonical_shapes() {
    for ok in &[
        "seed",
        "app-runtime",
        "code-runtime",
        "ecw-copy-file-dest-smoke-115630",
        "code-runtime-eks-smoke-20260511-192921",
        "a",
        "A_B",
        "x.y",
    ] {
        assert!(validate_template_name(ok).is_ok(), "expected ok: {ok}");
    }
}

#[test]
fn template_name_rejects_path_traversal() {
    for bad in &[
        "", "..", ".", "../etc", ".hidden", "a/b", "a\\b", "a\0b", "a b", "a:b",
        "семя", // non-ASCII
    ] {
        assert!(
            validate_template_name(bad).is_err(),
            "expected rejection: {bad:?}",
        );
    }
}

#[test]
fn content_hash_accepts_canonical_sha256() {
    for ok in &[
        "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "sha256:6dfaca783bd73dbb42fe5833b46e575a7c9555e56801e79cda7f01f573f2ebb1",
    ] {
        assert!(validate_content_hash(ok).is_ok(), "expected ok: {ok}");
    }
}

#[test]
fn content_hash_rejects_malformed_inputs() {
    for bad in &[
        "",
        "6dfaca78",        // no prefix
        "sha256:",         // empty hex
        "sha256:6dfaca78", // too short
        "sha256:6DFACA783BD73DBB42FE5833B46E575A7C9555E56801E79CDA7F01F573F2EBB1", // uppercase
        "sha256:6dfaca783bd73dbb42fe5833b46e575a7c9555e56801e79cda7f01f573f2ebb1XX",
        "sha1:6dfaca783bd73dbb42fe5833b46e575a7c9555e56801e79cda7f01f573f2ebb1",
        "sha256:../",
    ] {
        assert!(
            validate_content_hash(bad).is_err(),
            "expected rejection: {bad:?}",
        );
    }
}

#[test]
fn paths_compose_under_expected_roots() {
    let tdir = template_digest_dir(
        "seed",
        "sha256:6dfaca783bd73dbb42fe5833b46e575a7c9555e56801e79cda7f01f573f2ebb1",
    );
    assert!(
        tdir.starts_with(TEMPLATE_DIR),
        "{tdir:?} should be under TEMPLATE_DIR={TEMPLATE_DIR}"
    );
    let sid = "019e57e9-8827-7771-b598-8e270d4a3d5e";
    let canonical_snapshot_dir = snapshot_dir(sid);
    for p in [
        sandbox_dir(sid),
        instance_dir(sid),
        canonical_snapshot_dir.clone(),
        instance_ro_in_snapshot_dir(&canonical_snapshot_dir),
        rootfs_btrfs_send_path(&canonical_snapshot_dir),
        rootfs_ext4_in_snapshot_dir(&canonical_snapshot_dir),
    ] {
        assert!(
            p.starts_with(CACHE_DIR),
            "{p:?} should be under CACHE_DIR={CACHE_DIR}"
        );
    }
}
