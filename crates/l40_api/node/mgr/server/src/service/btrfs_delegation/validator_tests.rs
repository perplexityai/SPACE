use super::super::TEMPLATE_DIR;
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
fn content_hash_accepts_canonical_digests() {
    for ok in &[
        "blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
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
    let tdir = TemplateBuildTarget::parse(
        "seed",
        "sha256:6dfaca783bd73dbb42fe5833b46e575a7c9555e56801e79cda7f01f573f2ebb1",
        None,
    )
    .unwrap()
    .digest_dir();
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

fn fixture_hash() -> String {
    pplx_space_common_core::types::Digest::blake3([7; 32]).to_string()
}

fn other_environment() -> pplx_space_common_core::types::EnvironmentReference {
    pplx_space_common_core::types::EnvironmentReference::new(
        pplx_space_common_core::types::Digest::sha256([9; 32]),
    )
}

#[test]
fn legacy_template_targets_keep_their_directory_and_received_uuid() {
    let hash = fixture_hash();
    let target = TemplateBuildTarget::parse("seed", &hash, None).unwrap();
    assert_eq!(
        target.digest_dir(),
        PathBuf::from(format!("{TEMPLATE_DIR}/seed/{hash}"))
    );
    assert_eq!(
        target.received_uuid(),
        pplx_space_common_core::host::btrfs::deterministic_template_received_uuid("seed", &hash)
    );
}

#[test]
fn environment_template_targets_use_their_own_directory_and_received_uuid() {
    let hash = fixture_hash();
    let environment = other_environment();
    let build = EnvironmentTemplateBuild {
        content_hash: hash.clone(),
        environment: environment.to_string(),
    };
    let target = TemplateBuildTarget::parse("seed", "", Some(&build)).unwrap();
    let legacy = TemplateBuildTarget::parse("seed", &hash, None).unwrap();
    assert_eq!(
        target.digest_dir(),
        PathBuf::from(format!("{TEMPLATE_DIR}/seed/{hash}@{environment}"))
    );
    assert_ne!(target.received_uuid(), legacy.received_uuid());

    // The stamp is wire format between nodes; pinned beside the btrfs crate's
    // own pin for the same build.
    let pinned = TemplateBuildTarget::parse(
        "seed",
        "",
        Some(&EnvironmentTemplateBuild {
            content_hash: "sha256:6dfaca783bd73dbb42fe5833b46e575a7c9555e56801e79cda7f01f573f2ebb1"
                .to_string(),
            environment: environment.to_string(),
        }),
    )
    .unwrap();
    assert_eq!(
        pinned.received_uuid().to_string(),
        "19f18f52-6ca4-5e7f-93bf-e4ba9d8a2118"
    );
}

#[test]
fn environment_template_targets_reject_ambiguous_or_legacy_identities() {
    let hash = fixture_hash();
    let initial = pplx_space_node_runtime_core::catalog::initial_environment_reference();
    for (content_hash, build) in [
        // Both identities: an old caller's field and a new one.
        (
            hash.clone(),
            EnvironmentTemplateBuild {
                content_hash: hash.clone(),
                environment: other_environment().to_string(),
            },
        ),
        // The initial environment lives in the legacy layout.
        (
            String::new(),
            EnvironmentTemplateBuild {
                content_hash: hash.clone(),
                environment: initial.to_string(),
            },
        ),
        (
            String::new(),
            EnvironmentTemplateBuild {
                content_hash: hash.clone(),
                environment: "not-a-digest".to_string(),
            },
        ),
        (
            String::new(),
            EnvironmentTemplateBuild {
                content_hash: "../escape".to_string(),
                environment: other_environment().to_string(),
            },
        ),
    ] {
        let error = TemplateBuildTarget::parse("seed", &content_hash, Some(&build)).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument, "{build:?}");
    }
    // An empty legacy hash is what an old server sees; it stays invalid.
    assert_eq!(
        TemplateBuildTarget::parse("seed", "", None)
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn absent_template_build_is_a_lineage_precondition_only_for_suspend() {
    let content_hash = "sha256:6dfaca783bd73dbb42fe5833b46e575a7c9555e56801e79cda7f01f573f2ebb1";
    let temp = tempfile::tempdir().unwrap();
    let templates = temp.path().canonicalize().unwrap().join("templates");
    // An absent digest directory, then an absent template directory.
    std::fs::create_dir_all(templates.join("present")).unwrap();
    for name in ["present", "absent"] {
        let path = templates.join(name).join(content_hash);
        let template = TemplateBuildTarget::parse(name, content_hash, None).unwrap();
        let Err(status) = validated_suspend_send_parent_at(path.clone(), &template) else {
            panic!("{name}: an absent build cannot be a send parent");
        };
        assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{name}");
        assert_eq!(
            status.metadata().get(LINEAGE_CAUSE_METADATA).unwrap(),
            "template_missing",
            "{name}"
        );
        assert!(status.message().contains("format=full"), "{name}");

        // Operation snapshots have no Full codec, so the named-snapshot
        // encode keeps a terminal NotFound rather than a fork refreeze.
        let Err(status) = validated_send_parent_at(path, &template) else {
            panic!("{name}: an absent build cannot be a send parent");
        };
        assert_eq!(status.code(), tonic::Code::NotFound, "{name}");
    }
}

/// Each sandbox codec request resolves its parent in the partition its
/// `environment_build` names, and an old-shape request resolves the legacy
/// build. The two hashes must agree, and as for subvolume requests an
/// `environment_build` naming the initial environment is rejected.
#[test]
fn sandbox_codec_requests_resolve_their_parent_in_its_partition() {
    use crate::proto::{
        BtrfsEncodeRootfsSendRequest, BtrfsEncodeSnapshotRootfsSendRequest,
        BtrfsReceiveInstanceRequest,
    };
    let hash = fixture_hash();
    let environment = other_environment();
    let build = |environment: String| EnvironmentTemplateBuild {
        content_hash: hash.clone(),
        environment,
    };
    let twin_dir = PathBuf::from(format!("{TEMPLATE_DIR}/seed/{hash}@{environment}"));
    let legacy_dir = PathBuf::from(format!("{TEMPLATE_DIR}/seed/{hash}"));
    let initial = pplx_space_node_runtime_core::catalog::initial_environment_reference();
    let targets = |environment_build: Option<EnvironmentTemplateBuild>| {
        [
            BtrfsEncodeRootfsSendRequest {
                template_name: "seed".into(),
                content_hash: hash.clone(),
                environment_build: environment_build.clone(),
                ..Default::default()
            }
            .parent_target(),
            BtrfsEncodeSnapshotRootfsSendRequest {
                template_name: "seed".into(),
                content_hash: hash.clone(),
                environment_build: environment_build.clone(),
                ..Default::default()
            }
            .parent_target(),
            BtrfsReceiveInstanceRequest {
                template_name: "seed".into(),
                content_hash: hash.clone(),
                environment_build,
                ..Default::default()
            }
            .parent_target(),
        ]
    };

    for target in targets(Some(build(environment.to_string()))) {
        assert_eq!(target.unwrap().digest_dir(), twin_dir);
    }
    for target in targets(None) {
        assert_eq!(target.unwrap().digest_dir(), legacy_dir);
    }
    for target in targets(Some(build(initial.to_string()))) {
        assert_eq!(target.unwrap_err().code(), tonic::Code::InvalidArgument);
    }
    let mismatched = EnvironmentTemplateBuild {
        content_hash: pplx_space_common_core::types::Digest::blake3([8; 32]).to_string(),
        environment: environment.to_string(),
    };
    for target in targets(Some(mismatched)) {
        assert_eq!(target.unwrap_err().code(), tonic::Code::InvalidArgument);
    }
}

/// A codec request naming another environment tries that partition's build
/// first and the legacy build second, since a sandbox bound there can descend
/// from the legacy build; an old-shape request has the legacy build only.
#[test]
fn sandbox_codec_parent_candidates_fall_back_to_the_legacy_build() {
    use crate::proto::BtrfsEncodeSnapshotRootfsSendRequest;
    let hash = fixture_hash();
    let environment = other_environment();
    let twin_dir = PathBuf::from(format!("{TEMPLATE_DIR}/seed/{hash}@{environment}"));
    let legacy_dir = PathBuf::from(format!("{TEMPLATE_DIR}/seed/{hash}"));
    let request = |environment_build: Option<EnvironmentTemplateBuild>| {
        BtrfsEncodeSnapshotRootfsSendRequest {
            template_name: "seed".into(),
            content_hash: hash.clone(),
            environment_build,
            ..Default::default()
        }
    };
    let dirs = |request: BtrfsEncodeSnapshotRootfsSendRequest| {
        request
            .parent_candidates()
            .unwrap()
            .iter()
            .map(TemplateBuildTarget::digest_dir)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        dirs(request(Some(EnvironmentTemplateBuild {
            content_hash: hash.clone(),
            environment: environment.to_string(),
        }))),
        [twin_dir, legacy_dir.clone()]
    );
    assert_eq!(dirs(request(None)), std::slice::from_ref(&legacy_dir));
}

/// A request naming a partition ends its candidates with the digest's other
/// partitions found on disk, after the binding's own and the legacy one;
/// other digests, staging siblings and partitions already listed are left
/// out. A request naming none keeps the legacy build alone.
#[test]
fn sandbox_codec_parent_candidates_include_the_digest_s_other_cached_partitions() {
    let hash = fixture_hash();
    let other_hash = pplx_space_common_core::types::Digest::blake3([9; 32]).to_string();
    let environment = other_environment();
    let legacy = TemplateBuildTarget::parse("seed", &hash, None).unwrap();
    let dirs = |targets: Vec<TemplateBuildTarget>| {
        targets
            .iter()
            .map(TemplateBuildTarget::dir_name)
            .collect::<Vec<_>>()
    };
    let listing = [
        format!("{other_hash}@{environment}"),
        format!("{hash}@{environment}"),
        hash.clone(),
        format!(".{hash}.staging"),
        format!("{hash}@not-an-environment"),
    ];
    // A request without `environment_build` keeps the legacy build alone,
    // as before partitions, however many partitions are on disk.
    assert_eq!(
        dirs(append_partitions(vec![legacy.clone()], listing.clone())),
        std::slice::from_ref(&hash)
    );
    let twin = TemplateBuildTarget::parse(
        "seed",
        "",
        Some(&EnvironmentTemplateBuild {
            content_hash: hash.clone(),
            environment: environment.to_string(),
        }),
    )
    .unwrap();
    // The digest's other partitions follow, in directory-name order.
    let third = pplx_space_common_core::types::EnvironmentReference::new(
        pplx_space_common_core::types::Digest::sha256([3; 32]),
    );
    let fourth = pplx_space_common_core::types::EnvironmentReference::new(
        pplx_space_common_core::types::Digest::sha256([1; 32]),
    );
    let mut listing = listing.to_vec();
    listing.extend([format!("{hash}@{third}"), format!("{hash}@{fourth}")]);
    assert_eq!(
        dirs(append_partitions(vec![twin, legacy], listing)),
        [
            format!("{hash}@{environment}"),
            hash.clone(),
            format!("{hash}@{fourth}"),
            format!("{hash}@{third}"),
        ]
    );
}

/// The first candidate that passes its check and is accepted wins, even after
/// an earlier one failed or was rejected; with none accepted, the first that
/// passed, so the handler's lineage check reports the mismatch; with none
/// passing, the first error.
#[test]
fn the_first_accepted_parent_candidate_wins() {
    let hash = fixture_hash();
    let twin = TemplateBuildTarget::parse(
        "seed",
        "",
        Some(&EnvironmentTemplateBuild {
            content_hash: hash.clone(),
            environment: other_environment().to_string(),
        }),
    )
    .unwrap();
    let legacy = TemplateBuildTarget::parse("seed", &hash, None).unwrap();
    let candidates = || vec![twin.clone(), legacy.clone()];
    let is_legacy = |target: &TemplateBuildTarget| target.digest_dir() == legacy.digest_dir();

    // The binding's partition is missing: the legacy build wins.
    let (chosen, ()) = first_parent_candidate(
        candidates(),
        |target| {
            if is_legacy(target) {
                Ok(())
            } else {
                Err(Status::not_found("absent"))
            }
        },
        |()| true,
    )
    .unwrap();
    assert!(is_legacy(&chosen));

    // Both pass; only the legacy build is the instance's parent.
    let (chosen, _) = first_parent_candidate(
        candidates(),
        |target| Ok(is_legacy(target)),
        |parent| *parent,
    )
    .unwrap();
    assert!(is_legacy(&chosen));

    // Both pass, neither is accepted: the first, for the lineage error.
    let (chosen, _) =
        first_parent_candidate(candidates(), |target| Ok(is_legacy(target)), |_| false).unwrap();
    assert!(!is_legacy(&chosen));

    // Neither passes: the first error.
    let error = first_parent_candidate(
        candidates(),
        |target| -> Result<(), Status> {
            Err(Status::not_found(if is_legacy(target) {
                "legacy"
            } else {
                "twin"
            }))
        },
        |()| true,
    )
    .unwrap_err();
    assert_eq!(error.message(), "twin");
}
